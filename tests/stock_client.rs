use std::{collections::BTreeMap, sync::Arc};

use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory};
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, OffsetAt, UnknownTopicHandling},
    },
    record::Record,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use walstream::{
    coordinator::GroupCoordinator,
    group::GroupStore,
    log::LogEngine,
    protocol::BrokerIdentity,
    server::{DEFAULT_MAX_FRAME_BYTES, serve},
};

struct RunningServer {
    address: std::net::SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

async fn start(
    store: Arc<dyn ObjectStore>,
    address: &str,
    default_topic_partitions: i32,
) -> RunningServer {
    let listener = TcpListener::bind(address).await.unwrap();
    let address = listener.local_addr().unwrap();
    let engine = LogEngine::with_default_topic_partitions(
        store.clone(),
        "walstream/clusters/stock",
        default_topic_partitions,
    )
    .unwrap();
    let groups =
        GroupCoordinator::new(GroupStore::new(store.clone(), "walstream/clusters/stock").unwrap());
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(serve(
        listener,
        engine,
        groups,
        BrokerIdentity {
            host: "127.0.0.1".into(),
            port: address.port(),
            cluster_id: "stock".into(),
        },
        DEFAULT_MAX_FRAME_BYTES,
        async move {
            let _ = receiver.await;
        },
    ));
    RunningServer {
        address,
        shutdown,
        task,
    }
}

fn record(value: &str, timestamp: i64) -> Record {
    Record {
        key: None,
        value: Some(value.as_bytes().to_vec()),
        headers: BTreeMap::new(),
        timestamp: Utc.timestamp_millis_opt(timestamp).unwrap(),
    }
}

#[tokio::test]
async fn rskafka_discovers_produces_lists_fetches_and_recovers_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = start(store.clone(), "127.0.0.1:0", 1).await;
    let bootstrap = first.address.to_string();

    let client = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .unwrap();
    let partition = client
        .partition_client("events", 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    assert_eq!(
        partition
            .produce(
                vec![record("first", 1_000), record("second", 2_000)],
                Compression::NoCompression,
            )
            .await
            .unwrap(),
        vec![0, 1]
    );
    assert_eq!(partition.get_offset(OffsetAt::Earliest).await.unwrap(), 0);
    assert_eq!(partition.get_offset(OffsetAt::Latest).await.unwrap(), 2);
    let (records, high_watermark) = partition
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .unwrap();
    assert_eq!(high_watermark, 2);
    assert_eq!(
        records
            .iter()
            .map(|entry| (entry.offset, entry.record.value.as_deref().unwrap()))
            .collect::<Vec<_>>(),
        vec![(0, b"first".as_slice()), (1, b"second".as_slice())]
    );

    drop(partition);
    drop(client);
    let address = first.address;
    first.stop().await;

    let second = start(store, &address.to_string(), 1).await;
    let client = ClientBuilder::new(vec![bootstrap]).build().await.unwrap();
    let partition = client
        .partition_client("events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let (records, high_watermark) = partition
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .unwrap();
    assert_eq!(high_watermark, 2);
    assert_eq!(
        records
            .iter()
            .map(|entry| entry.record.value.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
    second.stop().await;
}

#[tokio::test]
async fn rskafka_multi_partition_logs_recover_the_persisted_count() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = start(store.clone(), "127.0.0.1:0", 3).await;
    let bootstrap = first.address.to_string();
    let client = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .unwrap();

    for (partition_index, value) in [(0, "zero"), (1, "one"), (2, "two")] {
        let partition = client
            .partition_client("parallel", partition_index, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        assert_eq!(
            partition
                .produce(
                    vec![record(value, 1_000 + i64::from(partition_index))],
                    Compression::NoCompression,
                )
                .await
                .unwrap(),
            vec![0]
        );
        assert_eq!(partition.get_offset(OffsetAt::Latest).await.unwrap(), 1);
    }

    drop(client);
    let address = first.address;
    first.stop().await;

    let second = start(store, &address.to_string(), 1).await;
    let client = ClientBuilder::new(vec![bootstrap]).build().await.unwrap();
    for (partition_index, expected) in [(0, "zero"), (1, "one"), (2, "two")] {
        let partition = client
            .partition_client("parallel", partition_index, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let (records, high_watermark) = partition
            .fetch_records(0, 1..1_000_000, 1_000)
            .await
            .unwrap();
        assert_eq!(high_watermark, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].record.value.as_deref(),
            Some(expected.as_bytes())
        );
    }
    second.stop().await;
}
