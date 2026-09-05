//! Hands-on broker/storage walkthrough. Run through scripts/test-log-index.sh.
//! Uses real Kafka requests and inspects only its unique disposable prefix.

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use clap::Parser;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    record::Record,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use walstream::{config::S3Settings, storage::build_s3_store};

#[derive(Parser)]
struct Args {
    #[command(flatten)]
    store: S3Settings,
    #[arg(long, default_value = "25000")]
    appends: usize,
    #[arg(long, default_value = "target/release/walstream")]
    broker: PathBuf,
    /// Optional baseline executable for a real downgrade-rejection check.
    #[arg(long)]
    baseline_broker: Option<PathBuf>,
    /// Python S3 forwarding proxy for the controlled pre-publication crash.
    #[arg(long, default_value = "scripts/index-fault-proxy.py")]
    proxy: PathBuf,
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_address() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

async fn start(
    args: &Args,
    endpoint: &str,
    executable: &std::path::Path,
) -> Result<(Process, SocketAddr)> {
    let address = free_address()?;
    let process = Process(
        Command::new(executable)
            .args([
                "serve",
                "--bucket",
                &args.store.bucket,
                "--region",
                &args.store.region,
                "--endpoint",
                endpoint,
                "--allow-http",
                "--prefix",
                &args.store.prefix,
                "--cluster-id",
                &args.store.cluster_id,
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

async fn client(address: SocketAddr, topic: &str) -> Result<PartitionClient> {
    Ok(ClientBuilder::new(vec![address.to_string()])
        .build()
        .await?
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await?)
}

fn record(offset: usize) -> Record {
    Record {
        key: None,
        value: Some(format!("record-{offset}").into_bytes()),
        headers: BTreeMap::new(),
        timestamp: Utc.timestamp_millis_opt(1_777_000_000_000).unwrap(),
    }
}

async fn append(client: &PartitionClient, offset: usize) -> Result<()> {
    ensure!(
        client
            .produce(vec![record(offset)], Compression::NoCompression)
            .await?
            == vec![offset as i64],
        "wrong assigned offset {offset}"
    );
    Ok(())
}

async fn check_range(client: &PartitionClient, start: usize, end: usize) -> Result<()> {
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

fn manifest(args: &Args, topic: &str) -> Path {
    Path::from(format!(
        "{}/topics/{topic}/0/manifest.json",
        args.store.cluster_prefix()
    ))
}

async fn read_json(store: &dyn ObjectStore, path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(
        &store.get(path).await?.bytes().await?,
    )?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.appends >= 129,
        "use at least 129 appends to exercise index branching"
    );
    let endpoint = args
        .store
        .endpoint
        .as_deref()
        .context("local endpoint required")?;
    let store = build_s3_store(&args.store)?;
    let (broker, address) = start(&args, endpoint, &args.broker).await?;
    let events = client(address, "events").await?;
    for offset in 0..args.appends {
        append(&events, offset).await?;
        if (offset + 1) % 5000 == 0 {
            println!("separate appends acknowledged: {}", offset + 1);
        }
    }
    check_range(&events, 0, args.appends).await?;
    for offset in [0, 63, 64, 127, 128, args.appends / 2, args.appends - 1] {
        check_range(&events, offset, offset + 1).await?;
    }
    let root = read_json(store.as_ref(), &manifest(&args, "events")).await?;
    ensure!(root["schema"] == 2 && root["tail"].as_array().unwrap().len() <= 64);
    let root_len = serde_json::to_vec(&root)?.len();
    let page = Path::from(root["tree"]["object"].as_str().context("missing tree")?);
    let page_bytes = store.get(&page).await?.bytes().await?;
    ensure!(root_len <= 4 * 1024 * 1024 && page_bytes.len() <= 4 * 1024 * 1024);
    println!(
        "growth verified: appends={} root_bytes={root_len} index_level={} page_bytes={}",
        args.appends,
        root["tree"]["level"],
        page_bytes.len()
    );
    drop(events);
    drop(broker);
    let (broker, address) = start(&args, endpoint, &args.broker).await?;
    let events = client(address, "events").await?;
    ensure!(events.get_offset(OffsetAt::Latest).await? == args.appends as i64);
    check_range(&events, 0, args.appends).await?;
    append(&events, args.appends).await?;
    println!(
        "replacement verified: complete history and next append offset {}",
        args.appends
    );

    // A faithful legacy fixture: real broker-written canonical records, with
    // their original descriptors published in the baseline v1 shape.
    let legacy = client(address, "legacy").await?;
    for offset in 0..16 {
        append(&legacy, offset).await?;
    }
    let legacy_path = manifest(&args, "legacy");
    let original = read_json(store.as_ref(), &legacy_path).await?;
    let segments = original["tail"].clone();
    let mut record_bytes = Vec::new();
    for segment in segments.as_array().unwrap() {
        let path = Path::from(segment["object"].as_str().unwrap());
        record_bytes.push((path.clone(), store.get(&path).await?.bytes().await?));
    }
    let old = json!({"schema":1,"revision":16,"next_offset":16,"segments":segments});
    store
        .put(&legacy_path, Bytes::from(serde_json::to_vec(&old)?).into())
        .await?;
    check_range(&legacy, 0, 16).await?;
    ensure!(read_json(store.as_ref(), &legacy_path).await?["schema"] == 1);
    append(&legacy, 16).await?;
    ensure!(read_json(store.as_ref(), &legacy_path).await?["schema"] == 2);
    for (path, bytes) in record_bytes {
        ensure!(
            store.get(&path).await?.bytes().await? == bytes,
            "legacy record object changed"
        );
    }
    drop(legacy);
    drop(events);
    drop(broker);
    let (broker, address) = start(&args, endpoint, &args.broker).await?;
    let legacy = client(address, "legacy").await?;
    check_range(&legacy, 0, 17).await?;
    println!(
        "legacy upgrade verified: schema 1 read unchanged, schema 2 append, original objects unchanged, replacement readback"
    );
    if let Some(baseline) = &args.baseline_broker {
        let (old, address) = start(&args, endpoint, baseline).await?;
        let old_client = client(address, "legacy").await?;
        ensure!(
            old_client.fetch_records(0, 1..1000, 100).await.is_err(),
            "old binary accepted upgraded log"
        );
        drop(old_client);
        drop(old);
        println!("baseline executable rejected upgraded partition");
    }

    // Start a forwarding proxy that blocks a single topic's root PUT before
    // sending it upstream. The marker proves the exact interruption boundary.
    let scratch = std::env::temp_dir().join(format!("walstream-index-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&scratch)?;
    let marker = scratch.join("blocked");
    let proxy_address = free_address()?;
    let proxy = Process(
        Command::new("python3")
            .arg(&args.proxy)
            .args([
                "--listen",
                &proxy_address.to_string(),
                "--upstream",
                endpoint,
                "--marker",
                marker.to_str().unwrap(),
                "--suffix",
                "/topics/interrupted/0/manifest.json",
            ])
            .stdin(Stdio::null())
            .spawn()?,
    );
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(proxy_address).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let interrupted = client(address, "interrupted").await?;
    for offset in 0..64 {
        append(&interrupted, offset).await?;
    }
    drop(interrupted);
    let (crashing, crash_address) =
        start(&args, &format!("http://{proxy_address}"), &args.broker).await?;
    let interrupted = client(crash_address, "interrupted").await?;
    let attempt = tokio::spawn(async move { append(&interrupted, 64).await });
    let mut blocked = false;
    for _ in 0..400 {
        if marker.exists() {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    ensure!(blocked, "publication proxy did not intercept root PUT");
    // At this boundary the new record and sealed leaf already exist in S3.
    ensure!(read_json(store.as_ref(), &manifest(&args, "interrupted")).await?["next_offset"] == 64);
    drop(crashing);
    drop(proxy);
    attempt.abort();
    let _ = attempt.await;
    let (fresh, fresh_address) = start(&args, endpoint, &args.broker).await?;
    let recovered = client(fresh_address, "interrupted").await?;
    ensure!(recovered.get_offset(OffsetAt::Latest).await? == 64);
    check_range(&recovered, 0, 64).await?;
    append(&recovered, 64).await?;
    println!(
        "controlled crash verified: root PUT intercepted before upstream publication, fresh process resumed at offset 64"
    );
    drop(recovered);
    drop(fresh);
    std::fs::remove_dir_all(&scratch)?;

    // Destructive fixture mutation is confined to this disposable prefix.
    // Re-read the current pointer because the post-restart append may have rolled over.
    let current = read_json(store.as_ref(), &manifest(&args, "events")).await?;
    let page = Path::from(
        current["tree"]["object"]
            .as_str()
            .context("missing current tree")?,
    );
    store
        .put(&page, Bytes::from_static(b"corrupt-index-page").into())
        .await?;
    let events = client(address, "events").await?;
    ensure!(
        events.fetch_records(0, 1..1000, 100).await.is_err(),
        "corrupt page produced a successful fetch"
    );
    println!("corrupt referenced page rejected through Kafka Fetch");
    drop(events);
    drop(legacy);
    drop(broker);
    println!(
        "all runtime scenarios passed; prefix={}",
        args.store.cluster_prefix()
    );
    Ok(())
}
