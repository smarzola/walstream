//! Hands-on broker/storage walkthrough. Run through scripts/test-log-index.sh.
//! Uses real Kafka requests and inspects only its unique disposable prefix.

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use clap::Parser;
use object_store::{ObjectStoreExt, path::Path};
use rskafka::client::partition::OffsetAt;
use serde_json::json;
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};
use walstream::{config::S3Settings, storage::build_s3_store};
mod support;
use support::*;

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
    let (broker, address) = start(&args.store, endpoint, &args.broker).await?;
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
    let root = read_json(store.as_ref(), &manifest(&args.store, "events")).await?;
    ensure!(root["schema"] == 3 && root["tail"].as_array().unwrap().len() <= 64);
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
    let (broker, address) = start(&args.store, endpoint, &args.broker).await?;
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
    let legacy_path = manifest(&args.store, "legacy");
    let original = read_json(store.as_ref(), &legacy_path).await?;
    let mut segments = original["tail"].clone();
    for segment in segments.as_array_mut().unwrap() {
        segment.as_object_mut().unwrap().remove("received_at_ms");
    }
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
    ensure!(read_json(store.as_ref(), &legacy_path).await?["schema"] == 3);
    for (path, bytes) in record_bytes {
        ensure!(
            store.get(&path).await?.bytes().await? == bytes,
            "legacy record object changed"
        );
    }
    drop(legacy);
    drop(events);
    drop(broker);
    let (broker, address) = start(&args.store, endpoint, &args.broker).await?;
    let legacy = client(address, "legacy").await?;
    check_range(&legacy, 0, 17).await?;
    println!(
        "legacy upgrade verified: schema 1 read unchanged, schema 3 append, original objects unchanged, replacement readback"
    );
    if let Some(baseline) = &args.baseline_broker {
        let (old, address) = start(&args.store, endpoint, baseline).await?;
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
    let (crashing, crash_address) = start(
        &args.store,
        &format!("http://{proxy_address}"),
        &args.broker,
    )
    .await?;
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
    ensure!(
        read_json(store.as_ref(), &manifest(&args.store, "interrupted")).await?["next_offset"]
            == 64
    );
    drop(crashing);
    drop(proxy);
    attempt.abort();
    let _ = attempt.await;
    let (fresh, fresh_address) = start(&args.store, endpoint, &args.broker).await?;
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
    let current = read_json(store.as_ref(), &manifest(&args.store, "events")).await?;
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
