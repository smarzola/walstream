use std::{sync::Arc, time::Duration};

use bytes::{Buf, Bytes};
use kafka_protocol::{messages::api_versions_response::ApiVersionsResponse, protocol::Decodable};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use walstream::{
    coordinator::GroupCoordinator,
    group::GroupStore,
    log::LogEngine,
    protocol::{BrokerIdentity, SUPPORTED_APIS},
    server::serve,
};

fn request(api_key: i16, version: i16, correlation_id: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&api_key.to_be_bytes());
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&correlation_id.to_be_bytes());
    body.extend_from_slice(&(-1_i16).to_be_bytes()); // null client ID

    let mut framed = Vec::new();
    framed.extend_from_slice(&(body.len() as i32).to_be_bytes());
    framed.extend_from_slice(&body);
    framed
}

async fn read_response(stream: &mut TcpStream) -> Bytes {
    let length = stream.read_i32().await.unwrap();
    assert!(length > 4);
    let mut body = vec![0_u8; length as usize];
    stream.read_exact(&mut body).await.unwrap();
    Bytes::from(body)
}

async fn assert_connection_closed(stream: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn handles_fragmented_and_pipelined_frames_with_correlation_ids() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        LogEngine::in_memory("walstream/clusters/protocol").unwrap(),
        GroupCoordinator::new(
            GroupStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                "walstream/clusters/protocol-groups",
            )
            .unwrap(),
        ),
        BrokerIdentity {
            host: "127.0.0.1".into(),
            port: address.port(),
            cluster_id: "protocol".into(),
        },
        1024,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let first = request(18, 0, 41);
    let second = request(18, 0, 42);
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(&first[..3]).await.unwrap();
    tokio::task::yield_now().await;
    let mut remainder = first[3..].to_vec();
    remainder.extend_from_slice(&second);
    stream.write_all(&remainder).await.unwrap();

    for correlation_id in [41, 42] {
        let mut response = read_response(&mut stream).await;
        assert_eq!(response.get_i32(), correlation_id);
        let decoded = ApiVersionsResponse::decode(&mut response, 0).unwrap();
        assert_eq!(decoded.error_code, 0);
        assert_eq!(decoded.api_keys.len(), SUPPORTED_APIS.len());
        assert_eq!(
            decoded
                .api_keys
                .iter()
                .map(|version| (version.api_key, version.min_version, version.max_version))
                .collect::<Vec<_>>(),
            SUPPORTED_APIS
                .iter()
                .map(|(key, min, max)| (*key as i16, *min, *max))
                .collect::<Vec<_>>()
        );
        assert!(!response.has_remaining());
    }

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn closes_connections_for_unsupported_apis_and_oversized_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        LogEngine::in_memory("walstream/clusters/bounds").unwrap(),
        GroupCoordinator::new(
            GroupStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                "walstream/clusters/bound-groups",
            )
            .unwrap(),
        ),
        BrokerIdentity {
            host: "127.0.0.1".into(),
            port: address.port(),
            cluster_id: "bounds".into(),
        },
        64,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let mut unsupported = TcpStream::connect(address).await.unwrap();
    unsupported.write_all(&request(8, 0, 1)).await.unwrap();
    assert_connection_closed(&mut unsupported).await;

    let mut unsupported_version = TcpStream::connect(address).await.unwrap();
    unsupported_version
        .write_all(&request(0, 8, 2))
        .await
        .unwrap();
    assert_connection_closed(&mut unsupported_version).await;

    let mut malformed = TcpStream::connect(address).await.unwrap();
    let mut with_trailing_byte = request(18, 0, 3);
    with_trailing_byte[0..4].copy_from_slice(&11_i32.to_be_bytes());
    with_trailing_byte.push(0);
    malformed.write_all(&with_trailing_byte).await.unwrap();
    assert_connection_closed(&mut malformed).await;

    let mut oversized = TcpStream::connect(address).await.unwrap();
    oversized.write_all(&65_i32.to_be_bytes()).await.unwrap();
    assert_connection_closed(&mut oversized).await;

    let mut count_bomb = TcpStream::connect(address).await.unwrap();
    let mut metadata = request(3, 4, 4);
    metadata.extend_from_slice(&i32::MAX.to_be_bytes());
    metadata.push(1);
    let body_length = i32::try_from(metadata.len() - 4).unwrap();
    metadata[0..4].copy_from_slice(&body_length.to_be_bytes());
    count_bomb.write_all(&metadata).await.unwrap();
    assert_connection_closed(&mut count_bomb).await;

    let mut group_count_bomb = TcpStream::connect(address).await.unwrap();
    let mut join = request(11, 2, 5);
    for value in ["g", "", "consumer"] {
        join.extend_from_slice(&(value.len() as i16).to_be_bytes());
        join.extend_from_slice(value.as_bytes());
        if value == "g" {
            join.extend_from_slice(&30_000_i32.to_be_bytes());
            join.extend_from_slice(&30_000_i32.to_be_bytes());
        }
    }
    join.extend_from_slice(&i32::MAX.to_be_bytes());
    let body_length = i32::try_from(join.len() - 4).unwrap();
    join[0..4].copy_from_slice(&body_length.to_be_bytes());
    group_count_bomb.write_all(&join).await.unwrap();
    assert_connection_closed(&mut group_count_bomb).await;

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn metadata_creation_is_durable_and_listed() {
    let engine = LogEngine::in_memory_with_partitions("walstream/clusters/topics", 3).unwrap();
    engine.ensure_topic("beta", 0).await.unwrap();
    engine.ensure_topic("alpha", 0).await.unwrap();
    engine.ensure_topic("alpha", 0).await.unwrap();
    assert_eq!(engine.topics().await.unwrap(), vec!["alpha", "beta"]);
    assert_eq!(
        engine.topic_partition_count("alpha").await.unwrap(),
        Some(3)
    );
    assert_eq!(engine.offsets("alpha", 0).await.unwrap().latest, 0);
    assert_eq!(engine.offsets("alpha", 2).await.unwrap().latest, 0);
}
