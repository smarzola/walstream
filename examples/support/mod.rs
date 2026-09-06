//! Shared setup for the real broker walkthroughs.
use anyhow::{Result, ensure};
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, PartitionClient, UnknownTopicHandling},
    },
    record::Record,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};
use walstream::config::S3Settings;

pub struct Process(pub Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub fn free_address() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

pub async fn start(
    settings: &S3Settings,
    endpoint: &str,
    executable: &std::path::Path,
) -> Result<(Process, SocketAddr)> {
    let address = free_address()?;
    let process = Process(
        Command::new(executable)
            .args([
                "serve",
                "--bucket",
                &settings.bucket,
                "--region",
                &settings.region,
                "--endpoint",
                endpoint,
                "--allow-http",
                "--prefix",
                &settings.prefix,
                "--cluster-id",
                &settings.cluster_id,
                "--listen",
                &address.to_string(),
                "--advertised-host",
                "127.0.0.1",
                "--advertised-port",
                &address.port().to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()?,
    );
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            println!(
                "launched {} pid={} address={address}",
                executable.display(),
                process.0.id()
            );
            return Ok((process, address));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("broker failed to listen")
}

pub async fn client(address: SocketAddr, topic: &str) -> Result<PartitionClient> {
    Ok(ClientBuilder::new(vec![address.to_string()])
        .build()
        .await?
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await?)
}

pub fn record(offset: usize) -> Record {
    Record {
        key: None,
        value: Some(format!("record-{offset}").into_bytes()),
        headers: BTreeMap::new(),
        timestamp: Utc.timestamp_millis_opt(1_777_000_000_000).unwrap(),
    }
}

pub async fn append(client: &PartitionClient, offset: usize) -> Result<()> {
    ensure!(
        client
            .produce(vec![record(offset)], Compression::NoCompression)
            .await?
            == vec![offset as i64],
        "wrong assigned offset {offset}"
    );
    Ok(())
}

pub async fn check_range(client: &PartitionClient, start: usize, end: usize) -> Result<()> {
    let mut next = start;
    while next < end {
        let (records, watermark) = client.fetch_records(next as i64, 1..1_000_000, 100).await?;
        ensure!(
            watermark >= end as i64 && !records.is_empty(),
            "missing committed records at {next}"
        );
        for got in records {
            if got.offset < next as i64 {
                continue;
            }
            if next == end {
                break;
            }
            ensure!(
                got.offset == next as i64 && got.record.value == record(next).value,
                "record mismatch at {next}"
            );
            next += 1;
        }
    }
    Ok(())
}

pub fn manifest(settings: &S3Settings, topic: &str) -> Path {
    Path::from(format!(
        "{}/topics/{topic}/0/manifest.json",
        settings.cluster_prefix()
    ))
}

pub async fn read_json(store: &dyn ObjectStore, path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(
        &store.get(path).await?.bytes().await?,
    )?)
}
