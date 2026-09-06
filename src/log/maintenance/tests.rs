use super::*;
use crate::log::tests::record;

#[tokio::test]
async fn preview_trim_empty_restart_and_append() {
    let store = Arc::new(InMemory::new());
    let engine = LogEngine::new(store.clone(), "retention").unwrap();
    for n in 0..140 {
        engine
            .append("events", 0, vec![record(&n.to_string())])
            .await
            .unwrap();
    }
    let before = engine.fetch("events", 0, 0).await.unwrap();
    let mut options = MaintenanceOptions {
        max_bytes: Some(1500),
        ..MaintenanceOptions::default()
    };
    let root_before = store
        .get(&engine.manifest_path("events", 0))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let preview = engine.maintain("events", 0, &options).await.unwrap();
    assert!(!preview.applied);
    assert!(preview.start_offset > 64);
    assert_eq!(
        root_before,
        store
            .get(&engine.manifest_path("events", 0))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    );
    options.apply = true;
    let applied = engine.maintain("events", 0, &options).await.unwrap();
    assert_eq!(preview.start_offset, applied.start_offset);
    assert_eq!(applied.collected_objects, applied.removable_objects);
    assert!(applied.collected_objects >= applied.expired_batches);
    let restarted = LogEngine::new(store, "retention").unwrap();
    let retained = restarted
        .fetch("events", 0, applied.start_offset)
        .await
        .unwrap();
    assert_eq!(retained, before[applied.start_offset as usize..]);
    assert!(matches!(
        restarted.fetch("events", 0, 0).await,
        Err(LogError::OffsetOutOfRange { .. })
    ));
    options.max_bytes = Some(0);
    let empty = restarted.maintain("events", 0, &options).await.unwrap();
    assert_eq!((empty.start_offset, empty.next_offset), (140, 140));
    assert_eq!(
        restarted.offsets("events", 0).await.unwrap(),
        OffsetRange {
            earliest: 140,
            latest: 140
        }
    );
    assert!(restarted.fetch("events", 0, 140).await.unwrap().is_empty());
    assert_eq!(
        restarted
            .append("events", 0, vec![record("new")])
            .await
            .unwrap()
            .base_offset,
        140
    );
    assert_eq!(
        restarted.fetch("events", 0, 140).await.unwrap()[0].value,
        Some(Bytes::from_static(b"new"))
    );
}

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use tokio::sync::Notify;

#[derive(Debug)]
struct Gate {
    operation: &'static str,
    contains: String,
    entered: Notify,
    resume: Notify,
}
impl Gate {
    async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(20), self.entered.notified())
            .await
            .unwrap();
    }
}

#[derive(Debug, Default)]
struct Controls {
    gate: Mutex<Option<Arc<Gate>>>,
    root_fault: AtomicUsize,
    allocator_fault: AtomicUsize,
    delete_fault_after: AtomicUsize,
    deletes: AtomicUsize,
    puts: AtomicUsize,
    received: Mutex<Vec<(i64, u64)>>,
}

#[derive(Clone, Debug, Default)]
struct Store {
    inner: Arc<InMemory>,
    controls: Arc<Controls>,
}

impl Store {
    fn gate(&self, operation: &'static str, contains: impl Into<String>) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            operation,
            contains: contains.into(),
            entered: Notify::new(),
            resume: Notify::new(),
        });
        *self.controls.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    async fn pause(&self, operation: &str, path: &Path) {
        let gate = {
            let mut slot = self.controls.gate.lock().unwrap();
            if slot.as_ref().is_some_and(|gate| {
                gate.operation == operation && path.as_ref().contains(&gate.contains)
            }) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.resume.notified().await;
        }
    }
    async fn delete_one(&self, path: Path) -> StoreResult<Path> {
        self.pause("delete", &path).await;
        let attempt = self.controls.deletes.fetch_add(1, SeqCst) + 1;
        if self.controls.delete_fault_after.load(SeqCst) == attempt {
            return Err(interrupted());
        }
        self.inner.delete(&path).await?;
        Ok(path)
    }
}
fn interrupted() -> StoreError {
    StoreError::Generic {
        store: "maintenance-test",
        source: std::io::Error::other("injected interruption").into(),
    }
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("maintenance-test-store")
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        if path.as_ref().ends_with("/manifest.json") {
            let bytes: Vec<u8> = payload
                .iter()
                .flat_map(|bytes| bytes.iter().copied())
                .collect();
            let root: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if let Some(received) = root["tail"]
                .as_array()
                .and_then(|tail| tail.last())
                .and_then(|segment| segment["received_at_ms"].as_u64())
            {
                self.controls
                    .received
                    .lock()
                    .unwrap()
                    .push((root["next_offset"].as_i64().unwrap(), received));
            }
        }
        self.pause("put", path).await;
        self.controls.puts.fetch_add(1, SeqCst);
        let fault = if path.as_ref().ends_with("/manifest.json") {
            self.controls.root_fault.swap(0, SeqCst)
        } else if path.as_ref().ends_with("/producer-ids.json") {
            self.controls.allocator_fault.swap(0, SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(interrupted());
        }
        if fault == 3 {
            return Err(StoreError::Precondition {
                path: path.to_string(),
                source: std::io::Error::other("injected stale fence").into(),
            });
        }
        let result = self.inner.put_opts(path, payload, options).await;
        self.pause("published", path).await;
        if fault == 2 && result.is_ok() {
            return Err(interrupted());
        }
        result
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.pause(if options.head { "head" } else { "get" }, path)
            .await;
        self.inner.get_opts(path, options).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        let store = self.clone();
        Box::pin(paths.then(move |path| {
            let store = store.clone();
            async move { store.delete_one(path?).await }
        }))
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

async fn fixture(count: usize) -> (LogEngine, Arc<Store>) {
    let store = Arc::new(Store::default());
    let engine = LogEngine::new(store.clone(), "retention").unwrap();
    for _ in 0..count {
        engine
            .append("events", 0, vec![record("fixed")])
            .await
            .unwrap();
    }
    (engine, store)
}

fn trim_to_bytes(bytes: u64) -> MaintenanceOptions {
    MaintenanceOptions {
        apply: true,
        max_bytes: Some(bytes),
        ..MaintenanceOptions::default()
    }
}

async fn keys(store: &dyn ObjectStore) -> BTreeMap<String, u64> {
    store
        .list(None)
        .map(|meta| {
            let meta = meta.unwrap();
            (meta.location.to_string(), meta.size)
        })
        .collect()
        .await
}

#[tokio::test]
async fn preview_unknown_invalid_and_budget_exhaustion_never_write() {
    let (engine, store) = fixture(80).await;
    let before = keys(store.as_ref()).await;
    let puts = store.controls.puts.load(SeqCst);
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                max_bytes: Some(0),
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(report.expired_batches, 80);
    assert_eq!(store.controls.puts.load(SeqCst), puts);
    assert_eq!(store.controls.deletes.load(SeqCst), 0);
    assert!(matches!(
        engine
            .maintain("absent", 0, &MaintenanceOptions::default())
            .await,
        Err(LogError::UnknownTopic { .. })
    ));
    assert!(matches!(
        engine
            .maintain(
                "events",
                0,
                &MaintenanceOptions {
                    max_objects: 0,
                    apply: true,
                    ..MaintenanceOptions::default()
                }
            )
            .await,
        Err(LogError::InvalidMaintenance { .. })
    ));
    assert!(matches!(
        engine
            .maintain(
                "events",
                0,
                &MaintenanceOptions {
                    max_objects: 5,
                    apply: true,
                    ..MaintenanceOptions::default()
                }
            )
            .await,
        Err(LogError::MaintenanceBudget { .. })
    ));
    assert_eq!(keys(store.as_ref()).await, before);
    assert_eq!(store.controls.puts.load(SeqCst), puts);
}

#[tokio::test]
async fn fence_collects_orphans_but_preserves_scoped_and_live_keys() {
    let (engine, store) = fixture(140).await;
    let orphan = engine.segment_path("events", 0, Uuid::new_v4());
    store
        .inner
        .put(&orphan, Bytes::from_static(b"orphan").into())
        .await
        .unwrap();
    let protected = [
        "retention/topics/events/0/segments/notes.batch",
        "retention/topics/events/0/segments/nested/00000000-0000-0000-0000-000000000000.batch",
        "retention/topics/events/1/segments/00000000-0000-0000-0000-000000000000.batch",
        "retention/topics/events/00/segments/00000000-0000-0000-0000-000000000000.batch",
        "retention/topics/other/0/index/00000000-0000-0000-0000-000000000000.json",
        "retention/groups/consumer/offsets.json",
    ];
    for key in protected {
        store
            .inner
            .put(&Path::from(key), Bytes::from_static(b"protected").into())
            .await
            .unwrap();
    }
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(report.collected_objects >= 1);
    assert!(matches!(
        store.inner.head(&orphan).await,
        Err(StoreError::NotFound { .. })
    ));
    for key in protected {
        assert!(store.inner.head(&Path::from(key)).await.is_ok(), "{key}");
    }
    assert_eq!(engine.fetch("events", 0, 0).await.unwrap().len(), 140);
    let before_version = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .version;
    let second = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second.collected_objects, 0);
    assert_ne!(
        before_version,
        engine
            .load_manifest("events", 0)
            .await
            .unwrap()
            .unwrap()
            .version
    );
}

#[tokio::test]
async fn paused_writer_cannot_publish_a_collected_tentative_segment() {
    let (engine, store) = fixture(2).await;
    store.controls.received.lock().unwrap().clear();
    let gate = store.gate("put", "/manifest.json");
    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.append("events", 0, vec![record("later")]).await })
    };
    gate.entered().await;
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        report.collected_objects, 1,
        "paused writer's tentative segment was inventoried"
    );
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    gate.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap().base_offset, 2);
    assert_eq!(engine.fetch("events", 0, 0).await.unwrap().len(), 3);
    let received = store.controls.received.lock().unwrap();
    let attempts: Vec<_> = received.iter().filter(|(end, _)| *end == 3).collect();
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].1, attempts[1].1,
        "receive time must survive a CAS retry"
    );
}

#[tokio::test]
async fn append_preparation_retries_when_maintenance_deletes_its_index() {
    let (engine, store) = fixture(192).await;
    let gate = store.gate("get", "/index/");
    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.append("events", 0, vec![record("later")]).await })
    };
    gate.entered().await;
    let report = engine
        .maintain("events", 0, &trim_to_bytes(1000))
        .await
        .unwrap();
    assert!(report.start_offset > 128);
    gate.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap().base_offset, 192);
    let records = engine
        .fetch("events", 0, report.start_offset)
        .await
        .unwrap();
    assert_eq!(records.last().unwrap().offset, 192);
}

#[tokio::test]
async fn stale_readers_retry_from_scratch_or_report_expiry() {
    for expire_all in [false, true] {
        let (engine, store) = fixture(192).await;
        let offset = if expire_all { 0 } else { 100 };
        let contains = if expire_all {
            let loaded = engine.load_manifest("events", 0).await.unwrap().unwrap();
            engine
                .trace_manifest("events", 0, &loaded.manifest, 1000)
                .await
                .unwrap()
                .segments[64]
                .object
                .clone()
        } else {
            "/index/".into()
        };
        let gate = store.gate("get", contains);
        let reader = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.fetch("events", 0, offset).await })
        };
        gate.entered().await;
        let size = engine
            .load_manifest("events", 0)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .tail()[0]
            .byte_length;
        let report = engine
            .maintain(
                "events",
                0,
                &trim_to_bytes(if expire_all { 0 } else { size * 100 }),
            )
            .await
            .unwrap();
        gate.resume.notify_one();
        let result = reader.await.unwrap();
        if expire_all {
            assert!(matches!(result, Err(LogError::OffsetOutOfRange { .. })));
        } else {
            assert_eq!(report.start_offset, 92);
            let records = result.unwrap();
            assert_eq!(records.len(), 92);
            assert_eq!(
                (records[0].offset, records.last().unwrap().offset),
                (100, 191)
            );
        }
    }
}

#[tokio::test]
async fn collector_restarts_a_trace_superseded_by_another_collector() {
    let (engine, store) = fixture(192).await;
    let gate = store.gate("published", "/manifest.json");
    let first = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .maintain(
                    "events",
                    0,
                    &MaintenanceOptions {
                        apply: true,
                        ..MaintenanceOptions::default()
                    },
                )
                .await
        })
    };
    gate.entered().await;
    let trimmed = engine
        .maintain("events", 0, &trim_to_bytes(1000))
        .await
        .unwrap();
    gate.resume.notify_one();
    let report = first.await.unwrap().unwrap();
    assert_eq!(report.start_offset, trimmed.start_offset);
    assert_eq!(
        engine.offsets("events", 0).await.unwrap().earliest,
        trimmed.start_offset
    );
    assert!(
        !engine
            .fetch("events", 0, report.start_offset)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn failed_ambiguous_and_stale_fences_authorize_no_early_deletion() {
    for fault in [1, 2, 3] {
        let (engine, store) = fixture(80).await;
        store.controls.root_fault.store(fault, SeqCst);
        let result = engine.maintain("events", 0, &trim_to_bytes(0)).await;
        if fault == 3 {
            assert_eq!(result.unwrap().collected_objects, 81);
            continue;
        }
        let Err(LogError::MaintenanceIncomplete { report, .. }) = result else {
            panic!("missing uncertainty report")
        };
        assert!(report.publication_uncertain);
        assert_eq!(report.collected_objects, 0);
        assert_eq!(store.controls.deletes.load(SeqCst), 0);
        let range = engine.offsets("events", 0).await.unwrap();
        assert_eq!(range.earliest, if fault == 1 { 0 } else { 80 });
        let recovered = engine
            .maintain("events", 0, &trim_to_bytes(0))
            .await
            .unwrap();
        assert_eq!(recovered.collected_objects, 81);
    }
}

#[tokio::test]
async fn partial_deletion_reports_committed_range_and_resumes() {
    let (engine, store) = fixture(80).await;
    store.controls.delete_fault_after.store(4, SeqCst);
    let result = engine.maintain("events", 0, &trim_to_bytes(0)).await;
    let Err(LogError::MaintenanceIncomplete { report, .. }) = result else {
        panic!("missing partial report")
    };
    assert!(report.applied);
    assert_eq!(report.collected_objects, 3);
    assert_eq!(engine.offsets("events", 0).await.unwrap().earliest, 80);
    let resumed = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        resumed.collected_objects + report.collected_objects,
        report.removable_objects
    );
}

#[tokio::test]
async fn malformed_and_missing_live_graphs_prevent_deletion() {
    for missing in [false, true] {
        let (engine, store) = fixture(80).await;
        let loaded = engine.load_manifest("events", 0).await.unwrap().unwrap();
        let LogManifest::Indexed(root) = loaded.manifest else {
            unreachable!()
        };
        let path = Path::from(root.tree.unwrap().object);
        if missing {
            store.inner.delete(&path).await.unwrap();
        } else {
            store
                .inner
                .put(&path, Bytes::from_static(b"broken").into())
                .await
                .unwrap();
        }
        assert!(
            engine
                .maintain("events", 0, &trim_to_bytes(0))
                .await
                .is_err()
        );
        assert_eq!(store.controls.deletes.load(SeqCst), 0);
        assert!(engine.fetch("events", 0, 0).await.is_err());
    }
}

async fn write_manifest(engine: &LogEngine, value: &impl Serialize) {
    engine
        .store
        .put(
            &engine.manifest_path("events", 0),
            Bytes::from(serde_json::to_vec(value).unwrap()).into(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn age_boundaries_whole_batches_and_nonmonotonic_receive_times() {
    let engine = LogEngine::in_memory("ages").unwrap();
    for count in [2, 3, 1] {
        engine
            .append(
                "events",
                0,
                (0..count).map(|_| record("old client timestamp")).collect(),
            )
            .await
            .unwrap();
    }
    let LogManifest::Indexed(mut root) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        unreachable!()
    };
    // A backward clock step cannot cause a hole: only an expired prefix is removed.
    for (segment, received) in root.tail.iter_mut().zip([1000, 2000, 500]) {
        segment.received_at_ms = Some(received);
    }
    write_manifest(&engine, &root).await;
    let age = MaintenanceOptions {
        apply: true,
        max_age_ms: Some(1000),
        ..MaintenanceOptions::default()
    };
    assert_eq!(
        engine
            .maintain_at("events", 0, &age, 1999)
            .await
            .unwrap()
            .expired_batches,
        0
    );
    let trimmed = engine.maintain_at("events", 0, &age, 2000).await.unwrap();
    assert_eq!((trimmed.expired_batches, trimmed.start_offset), (1, 2));
    assert_eq!(engine.fetch("events", 0, 2).await.unwrap().len(), 4);
    assert_eq!(
        engine
            .maintain_at("events", 0, &age, 3000)
            .await
            .unwrap()
            .start_offset,
        6
    );

    // A byte cap smaller than one remaining batch removes that entire batch.
    engine
        .append("events", 0, vec![record("a"), record("b")])
        .await
        .unwrap();
    let empty = engine
        .maintain("events", 0, &trim_to_bytes(1))
        .await
        .unwrap();
    assert_eq!(
        (empty.expired_batches, empty.start_offset, empty.next_offset),
        (1, 8, 8)
    );
}

#[tokio::test]
async fn both_legacy_schemas_adopt_a_full_age_window_without_record_rewrites() {
    for schema in [1, 2] {
        let (engine, store) = fixture(16).await;
        let LogManifest::Indexed(mut root) = engine
            .load_manifest("events", 0)
            .await
            .unwrap()
            .unwrap()
            .manifest
        else {
            unreachable!()
        };
        for segment in &mut root.tail {
            segment.received_at_ms = None;
        }
        root.schema = 2;
        root.adopted_at_ms = None;
        if schema == 1 {
            write_manifest(
                &engine,
                &Manifest {
                    schema,
                    revision: 16,
                    next_offset: 16,
                    segments: root.tail.clone(),
                },
            )
            .await;
        } else {
            let mut value = serde_json::to_value(&root).unwrap();
            value.as_object_mut().unwrap().remove("start_offset");
            write_manifest(&engine, &value).await;
        }
        let original = engine.fetch("events", 0, 0).await.unwrap();
        let record_objects = keys(store.as_ref())
            .await
            .into_iter()
            .filter(|(key, _)| key.ends_with(".batch"))
            .collect::<BTreeMap<_, _>>();
        let preview_options = MaintenanceOptions {
            max_age_ms: Some(100),
            ..MaintenanceOptions::default()
        };
        let preview = engine
            .maintain_at("events", 0, &preview_options, 10_000)
            .await
            .unwrap();
        assert!(preview.format_adoption);
        assert_eq!(preview.expired_batches, 0);
        let options = MaintenanceOptions {
            apply: true,
            ..preview_options
        };
        let adopted = engine
            .maintain_at("events", 0, &options, 10_000)
            .await
            .unwrap();
        assert!(adopted.format_adoption);
        assert_eq!(adopted.start_offset, 0);
        assert_eq!(engine.fetch("events", 0, 0).await.unwrap(), original);
        for (key, size) in record_objects {
            assert_eq!(store.inner.head(&Path::from(key)).await.unwrap().size, size);
        }
        assert_eq!(
            engine
                .maintain_at("events", 0, &options, 10_099)
                .await
                .unwrap()
                .start_offset,
            0
        );
        assert_eq!(
            engine
                .maintain_at("events", 0, &options, 10_100)
                .await
                .unwrap()
                .start_offset,
            16
        );
    }
}

#[tokio::test]
async fn schema_two_append_adopts_old_pages_and_records_receive_time() {
    let (engine, _) = fixture(192).await;
    let loaded = engine.load_manifest("events", 0).await.unwrap().unwrap();
    let mut graph = engine
        .trace_manifest("events", 0, &loaded.manifest, 1000)
        .await
        .unwrap();
    for segment in &mut graph.segments {
        segment.received_at_ms = None;
    }
    let mut root = engine
        .rebuild_index("events", 0, &graph.segments, 0, 192, 1000)
        .await
        .unwrap();
    root.schema = 2;
    root.revision = 192;
    root.adopted_at_ms = None;
    write_manifest(&engine, &root).await;
    let before = unix_millis().unwrap();
    engine
        .append("events", 0, vec![record("new")])
        .await
        .unwrap();
    let LogManifest::Indexed(upgraded) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        unreachable!()
    };
    assert_eq!(upgraded.schema, INDEX_SCHEMA);
    assert!(upgraded.adopted_at_ms.unwrap() >= before);
    assert!(upgraded.tail.last().unwrap().received_at_ms.unwrap() >= before);
    let age = MaintenanceOptions {
        max_age_ms: Some(1000),
        apply: true,
        ..MaintenanceOptions::default()
    };
    assert_eq!(
        engine
            .maintain_at("events", 0, &age, before)
            .await
            .unwrap()
            .expired_batches,
        0
    );
    assert_eq!(engine.fetch("events", 0, 0).await.unwrap().len(), 193);
}

#[tokio::test]
async fn repeated_retention_rebuilds_and_appends_across_tree_levels() {
    let (engine, _) = fixture(4300).await;
    let length = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
        .tail()[0]
        .byte_length;
    for cycle in 0..3 {
        let report = engine
            .maintain("events", 0, &trim_to_bytes(length * 4100))
            .await
            .unwrap();
        assert_eq!(report.retained_batches, 4100);
        let records = engine
            .fetch("events", 0, report.start_offset)
            .await
            .unwrap();
        assert_eq!(records.len(), 4100);
        for (n, record) in records.iter().enumerate() {
            assert_eq!(record.offset, report.start_offset + n as i64);
        }
        for _ in 0..65 {
            engine
                .append("events", 0, vec![record("fixed")])
                .await
                .unwrap();
        }
        assert_eq!(
            engine.offsets("events", 0).await.unwrap().latest,
            4300 + (cycle + 1) * 65
        );
    }
}

#[tokio::test]
async fn combined_limits_remove_age_and_size_prefix_without_splitting_batches() {
    let (engine, _) = fixture(3).await;
    let LogManifest::Indexed(mut root) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        unreachable!()
    };
    for (segment, received) in root.tail.iter_mut().zip([1000, 2000, 3000]) {
        segment.received_at_ms = Some(received);
    }
    let length = root.tail[0].byte_length;
    write_manifest(&engine, &root).await;
    let options = MaintenanceOptions {
        apply: true,
        max_age_ms: Some(1000),
        max_bytes: Some(length),
        ..MaintenanceOptions::default()
    };
    let report = engine
        .maintain_at("events", 0, &options, 2000)
        .await
        .unwrap();
    assert_eq!(report.start_offset, 2);
    assert_eq!(report.retained_record_bytes, length);
    assert_eq!(engine.fetch("events", 0, 2).await.unwrap()[0].offset, 2);
}

#[tokio::test]
async fn a_failure_after_retry_preserves_the_last_confirmed_publication_report() {
    let (engine, store) = fixture(80).await;
    let gate = store.gate("published", "/manifest.json");
    let first = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .maintain(
                    "events",
                    0,
                    &MaintenanceOptions {
                        apply: true,
                        max_objects: 100,
                        ..MaintenanceOptions::default()
                    },
                )
                .await
        })
    };
    gate.entered().await;
    engine
        .maintain("events", 0, &trim_to_bytes(0))
        .await
        .unwrap();
    for _ in 0..101 {
        store
            .inner
            .put(
                &engine.segment_path("events", 0, Uuid::new_v4()),
                Bytes::from_static(b"orphan").into(),
            )
            .await
            .unwrap();
    }
    let deletes = store.controls.deletes.load(SeqCst);
    gate.resume.notify_one();
    let Err(LogError::MaintenanceIncomplete { report, source }) = first.await.unwrap() else {
        panic!("lost committed report")
    };
    assert!(report.applied);
    assert_eq!(report.next_offset, 80);
    assert_eq!(report.collected_objects, 0);
    assert!(matches!(
        *source,
        LogError::MaintenanceBudget { maximum: 100 }
    ));
    assert_eq!(store.controls.deletes.load(SeqCst), deletes);
}

#[tokio::test]
async fn producer_request_validation_is_atomic_and_duplicate_retries_never_write() {
    use crate::log::producer::tests::batch;
    let (engine, store) = fixture(0).await;
    engine.ensure_topic("events", 0).await.unwrap();
    let puts = store.controls.puts.load(SeqCst);
    assert!(matches!(
        engine
            .append_batches("events", 0, vec![batch(7, 0, 0, 1), batch(7, 0, 2, 1)])
            .await,
        Err(LogError::OutOfOrderSequence)
    ));
    assert_eq!(store.controls.puts.load(SeqCst), puts);
    engine
        .append_batches("events", 0, (0..5).map(|s| batch(7, 0, s, 1)).collect())
        .await
        .unwrap();
    let puts = store.controls.puts.load(SeqCst);
    assert_eq!(
        engine
            .append_batches("events", 0, (0..5).map(|s| batch(7, 0, s, 1)).collect())
            .await
            .unwrap(),
        AppendResult {
            base_offset: 0,
            last_offset: 4
        }
    );
    let mut conflicting = batch(7, 0, 4, 1);
    conflicting[0].value = Some(Bytes::from_static(b"conflict"));
    assert!(matches!(
        engine
            .append_batches("events", 0, vec![batch(7, 0, 5, 1), conflicting])
            .await,
        Err(LogError::ConflictingSequence)
    ));
    assert_eq!(store.controls.puts.load(SeqCst), puts);
    assert_eq!(
        engine
            .append_batches("events", 0, vec![batch(7, 0, 4, 1), batch(7, 0, 5, 1)])
            .await
            .unwrap(),
        AppendResult {
            base_offset: 4,
            last_offset: 5
        }
    );
    assert!(matches!(
        engine.append("events", 0, batch(7, 0, 0, 1)).await,
        Err(LogError::OutOfOrderSequence)
    ));
    engine.append("events", 0, batch(7, 1, 0, 1)).await.unwrap();
    let puts = store.controls.puts.load(SeqCst);
    assert!(matches!(
        engine.append("events", 0, batch(7, 0, 6, 1)).await,
        Err(LogError::InvalidProducerEpoch)
    ));
    assert!(matches!(
        engine.append("events", 0, batch(7, 2, 1, 1)).await,
        Err(LogError::OutOfOrderSequence)
    ));
    assert_eq!(store.controls.puts.load(SeqCst), puts);
    engine
        .append("events", 0, vec![record("ordinary")])
        .await
        .unwrap();
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                max_bytes: Some(0),
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!((report.start_offset, report.next_offset), (8, 8));
    let puts = store.controls.puts.load(SeqCst);
    assert_eq!(
        engine
            .append("events", 0, batch(7, 1, 0, 1))
            .await
            .unwrap()
            .base_offset,
        6
    );
    assert_eq!(store.controls.puts.load(SeqCst), puts);
    assert!(engine.fetch("events", 0, 8).await.unwrap().is_empty());
    assert_eq!(
        engine
            .append("events", 0, batch(7, 1, 1, 1))
            .await
            .unwrap()
            .base_offset,
        8
    );
}

#[tokio::test]
async fn producer_publication_failures_and_collection_retry_whole_snapshot() {
    use crate::log::producer::tests::batch;
    let (engine, store) = fixture(0).await;
    engine.ensure_topic("events", 0).await.unwrap();
    for fault in 1..=3 {
        store.controls.root_fault.store(fault, SeqCst);
        let result = engine
            .append("events", 0, batch(fault as i64, 0, 0, 1))
            .await;
        if fault < 3 {
            assert!(result.is_err());
        }
        let retried = engine
            .append("events", 0, batch(fault as i64, 0, 0, 1))
            .await
            .unwrap();
        assert_eq!(retried.base_offset, fault as i64 - 1);
    }
    let gate = store.gate("put", "/manifest.json");
    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.append("events", 0, batch(1, 0, 1, 1)).await })
    };
    gate.entered().await;
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(
        report.collected_objects >= 2,
        "collect tentative record and producer page before resuming the losing writer"
    );
    gate.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap().base_offset, 3);
    let gate = store.gate("get", "/producer-state/");
    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.append("events", 0, batch(1, 0, 2, 1)).await })
    };
    gate.entered().await;
    engine.append("events", 0, batch(2, 0, 1, 1)).await.unwrap();
    engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    gate.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap().base_offset, 5);
    assert_eq!(engine.fetch("events", 0, 0).await.unwrap().len(), 6);
    let LogManifest::Indexed(root) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        panic!()
    };
    let object = Path::from(root.producers.unwrap().object);
    let bytes = store
        .inner
        .get(&object)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    store
        .inner
        .put(&object, Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();
    assert!(matches!(
        engine.append("events", 0, batch(1, 0, 3, 1)).await,
        Err(LogError::InvalidManifest { .. })
    ));
    assert!(
        engine
            .maintain(
                "events",
                0,
                &MaintenanceOptions {
                    apply: true,
                    max_bytes: Some(0),
                    ..MaintenanceOptions::default()
                }
            )
            .await
            .is_err()
    );
    store.inner.put(&object, bytes.into()).await.unwrap();
    store.inner.delete(&object).await.unwrap();
    assert!(matches!(
        engine.append("events", 0, batch(1, 0, 3, 1)).await,
        Err(LogError::ObjectStore(StoreError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn producer_allocator_never_reuses_an_ambiguously_committed_id() {
    let (engine, store) = fixture(0).await;
    store.controls.allocator_fault.store(2, SeqCst);
    assert!(engine.allocate_producer_id().await.is_err());
    let restarted = LogEngine::new(store.clone(), "retention").unwrap();
    assert_eq!(restarted.allocate_producer_id().await.unwrap(), 1);
    store.controls.allocator_fault.store(3, SeqCst);
    assert_eq!(restarted.allocate_producer_id().await.unwrap(), 2);
    store.controls.allocator_fault.store(1, SeqCst);
    assert!(restarted.allocate_producer_id().await.is_err());
    assert_eq!(restarted.allocate_producer_id().await.unwrap(), 3);
}
