use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};

use chrono::{TimeZone, Utc};
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, OffsetAt, UnknownTopicHandling},
    },
    record::Record,
};
use tokio::net::TcpStream;
use uuid::Uuid;

struct BrokerProcess {
    child: Child,
    address: SocketAddr,
}

impl BrokerProcess {
    async fn start(bucket: &str, endpoint: &str, prefix: &str, cluster: &str) -> Self {
        let address = free_address();
        let mut child = Command::new(env!("CARGO_BIN_EXE_walstream"))
            .args(store_args("serve", bucket, endpoint, prefix, cluster))
            .args([
                "--listen",
                &address.to_string(),
                "--advertised-host",
                "127.0.0.1",
                "--advertised-port",
                &address.port().to_string(),
            ])
            .env("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn walstream broker process");

        for _ in 0..200 {
            if TcpStream::connect(address).await.is_ok() {
                return Self { child, address };
            }
            if let Some(status) = child.try_wait().expect("poll broker process") {
                panic!("walstream exited before becoming ready: {status}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        panic!("walstream did not listen on {address}");
    }

    fn stop(mut self) {
        self.child.kill().expect("replace walstream process");
        self.child.wait().expect("reap walstream process");
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read loopback port")
}

fn store_args(
    command: &str,
    bucket: &str,
    endpoint: &str,
    prefix: &str,
    cluster: &str,
) -> Vec<String> {
    vec![
        command.into(),
        "--bucket".into(),
        bucket.into(),
        "--region".into(),
        "us-east-1".into(),
        "--endpoint".into(),
        endpoint.into(),
        "--allow-http".into(),
        "--prefix".into(),
        prefix.into(),
        "--cluster-id".into(),
        cluster.into(),
    ]
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
#[ignore = "requires scripts/test-s3-e2e.sh and an Apple Container S3-compatible service"]
async fn binary_recovers_from_s3_and_serializes_independent_writers() {
    let bucket = std::env::var("WALSTREAM_E2E_BUCKET").expect("test bucket");
    let endpoint = std::env::var("WALSTREAM_E2E_ENDPOINT").expect("test endpoint");
    let run_id = Uuid::new_v4();
    let prefix = format!("walstream-e2e/{run_id}");
    let cluster = "cluster";

    let verify = Command::new(env!("CARGO_BIN_EXE_walstream"))
        .args(store_args(
            "verify-store",
            &bucket,
            &endpoint,
            &prefix,
            cluster,
        ))
        .env("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1")
        .output()
        .expect("run conditional-write capability probe");
    assert!(
        verify.status.success(),
        "verify-store failed: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    assert!(
        String::from_utf8_lossy(&verify.stdout)
            .contains("supports required create and update preconditions")
    );

    let first = BrokerProcess::start(&bucket, &endpoint, &prefix, cluster).await;
    let bootstrap = first.address.to_string();
    let client = ClientBuilder::new(vec![bootstrap]).build().await.unwrap();
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
    drop(partition);
    drop(client);
    first.stop();

    let second = BrokerProcess::start(&bucket, &endpoint, &prefix, cluster).await;
    let bootstrap = second.address.to_string();
    let recovered_client = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .unwrap();
    let recovered = recovered_client
        .partition_client("events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    assert_eq!(recovered.get_offset(OffsetAt::Earliest).await.unwrap(), 0);
    assert_eq!(recovered.get_offset(OffsetAt::Latest).await.unwrap(), 2);
    let (records, high_watermark) = recovered
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

    let left_client = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .unwrap();
    let right_client = ClientBuilder::new(vec![bootstrap]).build().await.unwrap();
    let left = left_client
        .partition_client("events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let right = right_client
        .partition_client("events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let (left_offsets, right_offsets) = tokio::join!(
        left.produce(vec![record("left", 3_000)], Compression::NoCompression),
        right.produce(vec![record("right", 4_000)], Compression::NoCompression),
    );
    let mut published = vec![left_offsets.unwrap()[0], right_offsets.unwrap()[0]];
    published.sort_unstable();
    assert_eq!(published, vec![2, 3]);

    let (records, high_watermark) = recovered
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .unwrap();
    assert_eq!(high_watermark, 4);
    assert_eq!(
        records.iter().map(|entry| entry.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        records
            .iter()
            .map(|entry| entry.record.value.as_deref().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            b"first".as_slice(),
            b"second".as_slice(),
            b"left".as_slice(),
            b"right".as_slice(),
        ])
    );
    second.stop();
}
