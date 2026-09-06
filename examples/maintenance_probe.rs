//! Real Kafka, CLI, and S3 maintenance walkthrough on an owned disposable prefix.
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use rskafka::client::partition::OffsetAt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
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
    #[arg(long, default_value = "target/release/walstream")]
    broker: PathBuf,
    #[arg(long)]
    baseline_broker: Option<PathBuf>,
    #[arg(long, default_value = "scripts/index-fault-proxy.py")]
    proxy: PathBuf,
}

fn maintenance_command(args: &Args, endpoint: &str, topic: &str, extra: &[&str]) -> Command {
    let mut command = Command::new(&args.broker);
    command
        .args([
            "maintain",
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
            "--topic",
            topic,
            "--partition",
            "0",
            "--json",
        ])
        .args(extra)
        .stdin(Stdio::null());
    command
}

fn maintain(args: &Args, endpoint: &str, topic: &str, extra: &[&str]) -> Result<Value> {
    let output = maintenance_command(args, endpoint, topic, extra).output()?;
    ensure!(
        output.status.success(),
        "maintenance failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    println!("maintenance topic={topic}: {report}");
    Ok(report)
}

async fn inventory(
    store: &dyn ObjectStore,
    prefix: &str,
) -> Result<BTreeMap<String, (u64, Option<String>)>> {
    let mut listed = store.list(Some(&Path::from(prefix)));
    let mut output = BTreeMap::new();
    while let Some(meta) = listed.next().await {
        let meta = meta?;
        output.insert(meta.location.to_string(), (meta.size, meta.e_tag));
    }
    Ok(output)
}

struct BarrierProxy {
    _process: Process,
    endpoint: String,
    marker: PathBuf,
    release: PathBuf,
}
impl BarrierProxy {
    async fn start(
        args: &Args,
        scratch: &std::path::Path,
        method: &str,
        suffix: &str,
        skip: usize,
    ) -> Result<Self> {
        let address = free_address()?;
        let marker = scratch.join(format!("blocked-{}", uuid::Uuid::new_v4()));
        let release = marker.with_extension("release");
        let process = Process(
            Command::new("python3")
                .arg(&args.proxy)
                .args([
                    "--listen",
                    &address.to_string(),
                    "--upstream",
                    args.store.endpoint.as_deref().unwrap(),
                    "--marker",
                    marker.to_str().unwrap(),
                    "--release",
                    release.to_str().unwrap(),
                    "--method",
                    method,
                    "--suffix",
                    suffix,
                    "--skip",
                    &skip.to_string(),
                ])
                .stdin(Stdio::null())
                .spawn()?,
        );
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return Ok(Self {
                    _process: process,
                    endpoint: format!("http://{address}"),
                    marker,
                    release,
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!("proxy did not listen")
    }
    async fn blocked(&self) -> Result<()> {
        for _ in 0..800 {
            if self.marker.exists() {
                println!(
                    "observed barrier: {}",
                    std::fs::read_to_string(&self.marker)?.trim()
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!("proxy did not reach barrier")
    }
    fn resume(&self) -> Result<()> {
        std::fs::write(&self.release, "resume")?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let endpoint = args
        .store
        .endpoint
        .as_deref()
        .context("local endpoint required")?;
    let store = build_s3_store(&args.store)?;
    let prefix = args.store.cluster_prefix();
    let scratch =
        std::env::temp_dir().join(format!("walstream-maintenance-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&scratch)?;
    let (broker, address) = start(&args.store, endpoint, &args.broker).await?;
    let events = client(address, "events").await?;
    for n in 0..260 {
        append(&events, n).await?;
    }
    let before = inventory(store.as_ref(), &prefix).await?;
    let preview = maintain(&args, endpoint, "events", &["--max-bytes", "12000"])?;
    ensure!(
        before == inventory(store.as_ref(), &prefix).await?,
        "preview changed objects or ETags"
    );
    ensure!(preview["start_offset"].as_i64().unwrap() > 64);
    let applied = maintain(
        &args,
        endpoint,
        "events",
        &["--max-bytes", "12000", "--apply"],
    )?;
    ensure!(preview["start_offset"] == applied["start_offset"]);
    let after = inventory(store.as_ref(), &prefix).await?;
    let before_bytes: u64 = before.values().map(|(size, _)| size).sum();
    let after_bytes: u64 = after.values().map(|(size, _)| size).sum();
    ensure!(after.len() < before.len() && after_bytes < before_bytes);
    println!(
        "visible storage: {} objects / {before_bytes} bytes -> {} objects / {after_bytes} bytes",
        before.len(),
        after.len()
    );
    let start_offset = applied["start_offset"].as_u64().unwrap() as usize;
    ensure!(events.get_offset(OffsetAt::Earliest).await? == start_offset as i64);
    check_range(&events, start_offset, 260).await?;
    ensure!(
        format!(
            "{:?}",
            events.fetch_records(0, 1..1000, 100).await.unwrap_err()
        )
        .contains("OffsetOutOfRange")
    );
    for cycle in 0..2 {
        for n in 260 + cycle * 65..325 + cycle * 65 {
            append(&events, n).await?;
        }
        let report = maintain(
            &args,
            endpoint,
            "events",
            &["--max-bytes", "12000", "--apply"],
        )?;
        check_range(
            &events,
            report["start_offset"].as_u64().unwrap() as usize,
            325 + cycle * 65,
        )
        .await?;
    }
    println!("repeated append/retain/read cycles passed across page boundaries");

    // Pause a real writer after its tentative objects exist but before its CAS.
    let proxy =
        BarrierProxy::start(&args, &scratch, "PUT", "/topics/events/0/manifest.json", 0).await?;
    let (writer_broker, writer_address) = start(&args.store, &proxy.endpoint, &args.broker).await?;
    let writer = client(writer_address, "events").await?;
    let attempt = tokio::spawn(async move { append(&writer, 390).await });
    proxy.blocked().await?;
    let collected = maintain(&args, endpoint, "events", &["--apply"])?;
    ensure!(collected["collected_objects"].as_u64().unwrap() >= 1);
    proxy.resume()?;
    attempt.await??;
    check_range(&events, 390, 391).await?;
    drop(writer_broker);
    drop(proxy);
    println!("paused Kafka writer retried after its tentative segment was collected");

    // Pause a Fetch before an old index GET, expire everything, and resume it.
    let root = read_json(store.as_ref(), &manifest(&args.store, "events")).await?;
    let proxy = BarrierProxy::start(
        &args,
        &scratch,
        "GET",
        root["tree"]["object"].as_str().unwrap(),
        0,
    )
    .await?;
    let (reader_broker, reader_address) = start(&args.store, &proxy.endpoint, &args.broker).await?;
    let reader = client(reader_address, "events").await?;
    let old_start = root["start_offset"].as_i64().unwrap();
    let attempt = tokio::spawn(async move { reader.fetch_records(old_start, 1..1000, 100).await });
    proxy.blocked().await?;
    maintain(&args, endpoint, "events", &["--max-bytes", "0", "--apply"])?;
    proxy.resume()?;
    ensure!(format!("{:?}", attempt.await?.unwrap_err()).contains("OffsetOutOfRange"));
    drop(reader_broker);
    drop(proxy);
    ensure!(events.get_offset(OffsetAt::Earliest).await? == 391);
    ensure!(events.get_offset(OffsetAt::Latest).await? == 391);
    drop(events);
    drop(broker);
    let (broker, address) = start(&args.store, endpoint, &args.broker).await?;
    let events = client(address, "events").await?;
    append(&events, 391).await?;
    check_range(&events, 391, 392).await?;
    println!("stale Fetch reported expiry; replacement of empty log preserved next offset 391");

    // A writer can lose a page while preparing rollover, before reaching CAS.
    let preparation = client(address, "preparation").await?;
    for n in 0..192 {
        append(&preparation, n).await?;
    }
    let root = read_json(store.as_ref(), &manifest(&args.store, "preparation")).await?;
    let proxy = BarrierProxy::start(
        &args,
        &scratch,
        "GET",
        root["tree"]["object"].as_str().unwrap(),
        0,
    )
    .await?;
    let (writer_broker, writer_address) = start(&args.store, &proxy.endpoint, &args.broker).await?;
    let writer = client(writer_address, "preparation").await?;
    let attempt = tokio::spawn(async move { append(&writer, 192).await });
    proxy.blocked().await?;
    maintain(
        &args,
        endpoint,
        "preparation",
        &["--max-bytes", "0", "--apply"],
    )?;
    proxy.resume()?;
    attempt.await??;
    check_range(&preparation, 192, 193).await?;
    drop(writer_broker);
    drop(proxy);
    println!("writer recovered from a removed index page during rollover preparation");

    let interrupted = client(address, "interrupted").await?;
    for n in 0..80 {
        append(&interrupted, n).await?;
    }
    let proxy = BarrierProxy::start(
        &args,
        &scratch,
        "PUT",
        "/topics/interrupted/0/manifest.json",
        0,
    )
    .await?;
    let before = inventory(store.as_ref(), &format!("{prefix}/topics/interrupted/0/")).await?;
    let child = Process(
        maintenance_command(
            &args,
            &proxy.endpoint,
            "interrupted",
            &["--max-bytes", "0", "--apply"],
        )
        .stdout(Stdio::null())
        .spawn()?,
    );
    proxy.blocked().await?;
    ensure!(before == inventory(store.as_ref(), &format!("{prefix}/topics/interrupted/0/")).await?);
    drop(child);
    drop(proxy);
    check_range(&interrupted, 0, 80).await?;
    // object_store sends one DeleteObjects POST for each explicit delete.
    let proxy = BarrierProxy::start(
        &args,
        &scratch,
        "POST",
        &format!("/{}", args.store.bucket),
        3,
    )
    .await?;
    let child = Process(
        maintenance_command(
            &args,
            &proxy.endpoint,
            "interrupted",
            &["--max-bytes", "0", "--apply"],
        )
        .stdout(Stdio::null())
        .spawn()?,
    );
    proxy.blocked().await?;
    ensure!(interrupted.get_offset(OffsetAt::Earliest).await? == 80);
    let partial = inventory(store.as_ref(), &format!("{prefix}/topics/interrupted/0/")).await?;
    ensure!(
        before.len() - partial.len() == 3,
        "expected exactly three completed deletions"
    );
    drop(child);
    drop(proxy);
    let resumed = maintain(&args, endpoint, "interrupted", &["--apply"])?;
    ensure!(resumed["collected_objects"] == 78);
    println!(
        "killed maintenance before publication and after three deletes; rerun reclaimed remaining 78 objects"
    );

    for schema in [1, 2, 3] {
        let topic = format!("legacy{schema}");
        let legacy = client(address, &topic).await?;
        for n in 0..16 {
            append(&legacy, n).await?;
        }
        let path = manifest(&args.store, &topic);
        let mut original = read_json(store.as_ref(), &path).await?;
        let mut segments = original["tail"].clone();
        let mut bytes = Vec::new();
        for segment in segments.as_array_mut().unwrap() {
            segment.as_object_mut().unwrap().remove("received_at_ms");
            let object = Path::from(segment["object"].as_str().unwrap());
            bytes.push((object.clone(), store.get(&object).await?.bytes().await?));
        }
        let old = if schema == 1 {
            json!({"schema":1,"revision":16,"next_offset":16,"segments":segments})
        } else if schema == 2 {
            original["schema"] = json!(2);
            original["tail"] = segments;
            original.as_object_mut().unwrap().remove("start_offset");
            original.as_object_mut().unwrap().remove("adopted_at_ms");
            original
        } else {
            original["schema"] = json!(3);
            original
        };
        store
            .put(&path, Bytes::from(serde_json::to_vec(&old)?).into())
            .await?;
        check_range(&legacy, 0, 16).await?;
        if schema == 3 {
            if let Some(baseline) = &args.baseline_broker {
                let (old_broker, old_address) = start(&args.store, endpoint, baseline).await?;
                let old_client = client(old_address, &topic).await?;
                check_range(&old_client, 0, 16).await?;
                drop(old_broker);
            }
        }
        let adopted = maintain(
            &args,
            endpoint,
            &topic,
            &["--max-age-ms", "60000", "--apply"],
        )?;
        ensure!(adopted["format_adoption"] == true && adopted["expired_batches"] == 0);
        if schema == 3 {
            ensure!(
                read_json(store.as_ref(), &path).await?["adopted_at_ms"] == old["adopted_at_ms"]
            );
        }
        for (object, original) in bytes {
            ensure!(store.get(&object).await?.bytes().await? == original);
        }
        if let Some(baseline) = &args.baseline_broker {
            let (old_broker, old_address) = start(&args.store, endpoint, baseline).await?;
            let old = client(old_address, &topic).await?;
            ensure!(old.fetch_records(0, 1..1000, 100).await.is_err());
            drop(old_broker);
        }
        let expired = maintain(&args, endpoint, &topic, &["--max-age-ms", "0", "--apply"])?;
        ensure!(expired["start_offset"] == 16);
        println!(
            "schema {schema} adoption preserved record bytes and full age window; explicit zero-age expiry passed"
        );
    }

    let corrupt = client(address, "corrupt").await?;
    for n in 0..80 {
        append(&corrupt, n).await?;
    }
    let root = read_json(store.as_ref(), &manifest(&args.store, "corrupt")).await?;
    store
        .put(
            &Path::from(root["tree"]["object"].as_str().unwrap()),
            Bytes::from_static(b"corrupt-page").into(),
        )
        .await?;
    let before = inventory(store.as_ref(), &prefix).await?;
    let output = maintenance_command(&args, endpoint, "corrupt", &["--max-bytes", "0", "--apply"])
        .output()?;
    ensure!(!output.status.success());
    ensure!(before == inventory(store.as_ref(), &prefix).await?);
    println!(
        "corrupt live graph prevented publication and deletion; CLI exit={}",
        output.status
    );
    drop(broker);
    std::fs::remove_dir_all(scratch)?;
    println!("all maintenance runtime scenarios passed; prefix={prefix}");
    Ok(())
}
