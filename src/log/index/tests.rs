use super::*;
use crate::log::tests::record;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use tokio::sync::Barrier;

#[derive(Debug, Default)]
struct Store {
    inner: InMemory,
    gets: AtomicUsize,
    bytes: AtomicUsize,
    puts: AtomicUsize,
    fault: AtomicUsize,
    races: AtomicUsize,
    gate: Option<Barrier>,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("index-test-store")
    }
}

fn interrupted() -> StoreError {
    StoreError::Generic {
        store: "fault-injection",
        source: std::io::Error::other("interrupted publication").into(),
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
        let name = path.to_string();
        if !name.ends_with(".batch") {
            self.puts.fetch_add(1, SeqCst);
        }
        let fault = self.fault.load(SeqCst);
        if (fault == 1 && name.ends_with("/manifest.json"))
            || (fault == 2 && name.contains("/index/"))
        {
            return Err(interrupted());
        }
        if fault == 4 && name.ends_with("/manifest.json") && self.races.fetch_add(1, SeqCst) < 2 {
            self.gate.as_ref().unwrap().wait().await;
        }
        let result = self.inner.put_opts(path, payload, options).await;
        if fault == 3 && name.ends_with("/manifest.json") && result.is_ok() {
            return Err(interrupted());
        }
        result
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let result = self.inner.get_opts(path, options).await?;
        if !path.to_string().ends_with(".batch") {
            self.gets.fetch_add(1, SeqCst);
            self.bytes.fetch_add(result.meta.size as usize, SeqCst);
        }
        Ok(result)
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
        p: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        panic!("log access must not list the bucket")
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> StoreResult<ListResult> {
        panic!("explicit topic access must not list")
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

impl Store {
    fn reset(&self) {
        self.gets.store(0, SeqCst);
        self.puts.store(0, SeqCst);
        self.bytes.store(0, SeqCst);
    }
}

async fn root(engine: &LogEngine) -> Root {
    match engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    {
        LogManifest::Indexed(root) => root,
        _ => panic!("expected upgraded root"),
    }
}

async fn put_root(engine: &LogEngine, root: &Root) {
    engine
        .store
        .put(
            &engine.manifest_path("events", 0),
            Bytes::from(serde_json::to_vec(root).unwrap()).into(),
        )
        .await
        .unwrap();
}

async fn legacy(engine: &LogEngine, count: usize) -> Manifest {
    let mut manifest = Manifest::default();
    for offset in 0..count {
        let mut record = record(&offset.to_string());
        record.offset = offset as i64;
        let bytes = encode_records(&[record]).unwrap();
        let path = engine.segment_path("events", 0, Uuid::new_v4());
        engine
            .store
            .put_opts(&path, bytes.clone().into(), PutMode::Create.into())
            .await
            .unwrap();
        manifest.segments.push(Segment {
            object: path.to_string(),
            base_offset: offset as i64,
            record_count: 1,
            byte_length: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        });
    }
    manifest.next_offset = count as i64;
    manifest.revision = count as u64;
    engine
        .store
        .put(
            &engine.manifest_path("events", 0),
            Bytes::from(serde_json::to_vec(&manifest).unwrap()).into(),
        )
        .await
        .unwrap();
    manifest
}

#[tokio::test]
async fn grows_beyond_legacy_limit_and_seeks_without_reading_history() {
    let store = Arc::new(Store::default());
    let engine = LogEngine::new(store.clone(), "growth").unwrap();
    for offset in 0..25_000 {
        assert_eq!(
            engine
                .append("events", 0, vec![record(&offset.to_string())])
                .await
                .unwrap()
                .base_offset,
            offset
        );
        if [128, 8192, 24999].contains(&offset) {
            store.reset();
            let fetched = engine.fetch_bounded("events", 0, 1, 1, true).await.unwrap();
            assert_eq!(
                decode_record_batches(fetched.records).unwrap().1[0].offset,
                1
            );
            let gets = store.gets.load(SeqCst);
            let bytes = store.bytes.load(SeqCst);
            println!(
                "history={} seek metadata_gets={gets} metadata_bytes={bytes}",
                offset + 1
            );
            assert!(gets <= 5 && bytes < 100_000);
            store.reset();
            assert_eq!(
                engine.offsets("events", 0).await.unwrap().latest,
                offset + 1
            );
            assert_eq!(store.gets.load(SeqCst), 2);
        }
    }
    let tree = root(&engine).await.tree.unwrap();
    assert_eq!(tree.level, 2);
    let recovered = LogEngine::new(store.clone(), "growth").unwrap();
    let all = recovered.fetch("events", 0, 0).await.unwrap();
    assert_eq!(all.len(), 25_000);
    for (offset, value) in all.iter().enumerate() {
        assert_eq!(value.offset, offset as i64);
        assert_eq!(value.value.as_deref(), Some(offset.to_string().as_bytes()));
    }
    for offset in [0, 63, 64, 65, 4095, 4096, 4097, 12_500, 24_999] {
        let fetched = recovered
            .fetch_bounded("events", 0, offset, 1, true)
            .await
            .unwrap();
        assert_eq!(
            decode_record_batches(fetched.records).unwrap().1[0].offset,
            offset
        );
    }
    // Force a rollover at a large history and measure the real update path.
    for offset in 25_000..25_025 {
        if offset == 25_024 {
            store.reset();
        }
        recovered
            .append("events", 0, vec![record(&offset.to_string())])
            .await
            .unwrap();
    }
    println!(
        "rollover append metadata_gets={} metadata_puts={} bytes={}",
        store.gets.load(SeqCst),
        store.puts.load(SeqCst),
        store.bytes.load(SeqCst)
    );
    assert!(store.gets.load(SeqCst) <= 4 && store.puts.load(SeqCst) <= 4);
    // 25,024 is a multiple of 64; the preceding final append rolled over.
    assert_eq!(root(&recovered).await.tail.len(), 1);
    store.reset();
    recovered
        .append("events", 0, vec![record("after")])
        .await
        .unwrap();
    println!(
        "ordinary append metadata_gets={} metadata_puts={} bytes={}",
        store.gets.load(SeqCst),
        store.puts.load(SeqCst),
        store.bytes.load(SeqCst)
    );
    assert_eq!(store.gets.load(SeqCst), 2);
    assert_eq!(store.puts.load(SeqCst), 1);
}

#[tokio::test]
async fn upgrades_full_legacy_log_without_rewriting_records() {
    let store = Arc::new(Store::default());
    let engine = LogEngine::new(store.clone(), "legacy").unwrap();
    let original = legacy(&engine, MAX_MANIFEST_SEGMENTS).await;
    let before = engine
        .store
        .get(&engine.manifest_path("events", 0))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        engine
            .fetch_bounded("events", 0, 9999, 1, true)
            .await
            .unwrap()
            .high_watermark,
        10_000
    );
    assert_eq!(
        engine
            .store
            .get(&engine.manifest_path("events", 0))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        engine
            .append("events", 0, vec![record("10000")])
            .await
            .unwrap()
            .base_offset,
        10_000
    );
    assert_eq!(root(&engine).await.revision, 10_001);
    for segment in &original.segments {
        let bytes = engine
            .store
            .get(&Path::from(segment.object.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(sha256_hex(&bytes), segment.sha256);
    }
    let fresh = LogEngine::new(store, "legacy").unwrap();
    assert_eq!(fresh.fetch("events", 0, 0).await.unwrap().len(), 10_001);
    let bytes = fresh
        .store
        .get(&fresh.manifest_path("events", 0))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        serde_json::from_slice::<Manifest>(&bytes).is_err(),
        "old reader must reject v2"
    );
}

#[tokio::test]
async fn interrupted_upgrade_and_rollover_do_not_publish_orphans() {
    for initial in [64, 128] {
        for fault in [1, 2, 3] {
            let store = Arc::new(Store::default());
            let engine = LogEngine::new(store.clone(), "faults").unwrap();
            if initial == 128 {
                legacy(&engine, initial).await;
            } else {
                for i in 0..initial {
                    engine
                        .append("events", 0, vec![record(&i.to_string())])
                        .await
                        .unwrap();
                }
            }
            engine.ensure_topic("events", 0).await.unwrap();
            store.fault.store(fault, SeqCst);
            assert!(
                engine
                    .append("events", 0, vec![record("new")])
                    .await
                    .is_err()
            );
            store.fault.store(0, SeqCst);
            let fresh = LogEngine::new(store.clone(), "faults").unwrap();
            let expected = initial + usize::from(fault == 3);
            assert_eq!(
                fresh.offsets("events", 0).await.unwrap().latest,
                expected as i64
            );
            assert_eq!(fresh.fetch("events", 0, 0).await.unwrap().len(), expected);
            assert_eq!(
                fresh
                    .append("events", 0, vec![record("retry")])
                    .await
                    .unwrap()
                    .base_offset,
                expected as i64
            );
        }
    }
}

#[tokio::test]
async fn concurrent_upgrade_and_rollover_serialize_at_root() {
    for migrate in [false, true] {
        let store = Arc::new(Store {
            gate: Some(Barrier::new(2)),
            ..Store::default()
        });
        let engine = LogEngine::new(store.clone(), "race").unwrap();
        if migrate {
            legacy(&engine, 128).await;
        } else {
            for _ in 0..128 {
                engine
                    .append("events", 0, vec![record("seed")])
                    .await
                    .unwrap();
            }
        }
        engine.ensure_topic("events", 0).await.unwrap();
        store.fault.store(4, SeqCst);
        let other = LogEngine::new(store.clone(), "race").unwrap();
        let (a, b) = tokio::join!(
            engine.append("events", 0, vec![record("a")]),
            other.append("events", 0, vec![record("b")])
        );
        let mut offsets = [a.unwrap().base_offset, b.unwrap().base_offset];
        offsets.sort();
        assert_eq!(offsets, [128, 129]);
        assert_eq!(engine.fetch("events", 0, 0).await.unwrap().len(), 130);
        assert!(store.races.load(SeqCst) >= 3);
    }
}

#[tokio::test]
async fn corrupt_index_pages_fail_closed_and_short_fetch_skips_later_pages() {
    let engine = LogEngine::in_memory("corruption").unwrap();
    for _ in 0..129 {
        engine
            .append("events", 0, vec![record("value")])
            .await
            .unwrap();
    }
    let original = root(&engine).await;
    let reference = original.tree.clone().unwrap();
    let Page::Branch { children } = engine.read_page("events", 0, &reference).await.unwrap() else {
        panic!()
    };
    let second = children[1].clone();
    engine
        .store
        .put(
            &Path::from(second.object.clone()),
            Bytes::from_static(b"corrupt").into(),
        )
        .await
        .unwrap();
    assert!(
        engine
            .fetch_bounded("events", 0, 64, 1, true)
            .await
            .is_err()
    );
    assert!(engine.fetch_bounded("events", 0, 0, 1, true).await.is_ok());
    // Re-authenticate malformed page contents to exercise structural checks,
    // independently of the checksum barrier.
    let original_bytes = engine
        .store
        .get(&Path::from(reference.object.clone()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    for mutation in 0..8 {
        let mut page: serde_json::Value = serde_json::from_slice(&original_bytes).unwrap();
        let kids = page["branch"]["children"].as_array_mut().unwrap();
        match mutation {
            0 => kids[0]["first_offset"] = 1.into(),
            1 => kids[0]["level"] = 11.into(),
            2 => kids[0]["object"] = "elsewhere/index/page.json".into(),
            3 => kids[0]["next_offset"] = 9999.into(),
            4 => kids[0]["object"] = reference.object.clone().into(),
            5 => {
                let child = kids[0].clone();
                *kids = vec![child; 65];
            }
            6 => {
                kids[0]["segment_count"] = 0.into();
            }
            _ => {
                kids[0]["object"] =
                    format!("corruption/topics/events/0/index/{}.json", Uuid::new_v4()).into();
            }
        }
        let bytes = Bytes::from(serde_json::to_vec(&page).unwrap());
        engine
            .store
            .put(&Path::from(reference.object.clone()), bytes.clone().into())
            .await
            .unwrap();
        let mut changed = original.clone();
        let pointer = changed.tree.as_mut().unwrap();
        pointer.sha256 = sha256_hex(&bytes);
        pointer.byte_length = bytes.len() as u64;
        put_root(&engine, &changed).await;
        assert!(
            engine.fetch_bounded("events", 0, 0, 1, true).await.is_err(),
            "mutation {mutation}"
        );
    }
    let mut changed = original.clone();
    changed.tree.as_mut().unwrap().byte_length = MAX_MANIFEST_BYTES as u64 + 1;
    put_root(&engine, &changed).await;
    assert!(engine.offsets("events", 0).await.is_err());
    changed = original;
    changed.tail = vec![changed.tail[0].clone(); 65];
    put_root(&engine, &changed).await;
    assert!(engine.offsets("events", 0).await.is_err());
}
