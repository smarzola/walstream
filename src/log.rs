//! Object-store-backed ordered log.
//!
//! Each topic has one partition and one versioned `manifest.json`. Appends first
//! create an immutable Kafka record-batch object, then make it visible by
//! conditionally creating or updating the manifest with its last-read ETag.
//! Losing a manifest race leaves an invisible orphan and retries against fresh
//! state; no acknowledged record depends on local process state.

use std::{collections::HashSet, sync::Arc};

use bytes::{Bytes, BytesMut};
use kafka_protocol::records::{
    Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record, RecordBatchDecoder,
    RecordBatchEncoder, RecordEncodeOptions,
};
use object_store::{
    Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, memory::InMemory,
    path::Path,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MANIFEST_SCHEMA: u32 = 1;
const MAX_CAS_ATTEMPTS: usize = 128;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// Durable partition log backed by an object store.
#[derive(Clone)]
pub struct LogEngine {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl std::fmt::Debug for LogEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogEngine")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Successful append result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    /// Offset assigned to the first appended record.
    pub base_offset: i64,
    /// Offset assigned to the final appended record.
    pub last_offset: i64,
}

/// Earliest inclusive and latest exclusive offsets for a partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetRange {
    /// Earliest readable offset.
    pub earliest: i64,
    /// Offset immediately after the final committed record.
    pub latest: i64,
}

impl LogEngine {
    /// Create an engine over any object-store implementation.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, LogError> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        Ok(Self { store, prefix })
    }

    /// Create an engine backed by a fresh in-memory store.
    pub fn in_memory(prefix: impl Into<String>) -> Result<Self, LogError> {
        Self::new(Arc::new(InMemory::new()), prefix)
    }

    /// Append a non-empty record batch to the single supported partition.
    ///
    /// The immutable segment is uploaded first. The append becomes visible only
    /// when a create-or-update precondition commits the new manifest version.
    pub async fn append(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<AppendResult, LogError> {
        validate_topic(topic)?;
        validate_partition(partition)?;
        validate_records(&records)?;

        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self.load_manifest(topic).await?;
            let base_offset = loaded.manifest.next_offset;
            let mut assigned = records.clone();
            for (delta, record) in assigned.iter_mut().enumerate() {
                let delta_i32 = i32::try_from(delta).map_err(|_| LogError::OffsetOverflow)?;
                record.offset = base_offset
                    .checked_add(i64::try_from(delta).map_err(|_| LogError::OffsetOverflow)?)
                    .ok_or(LogError::OffsetOverflow)?;
                // A normal Kafka v2 batch derives per-record sequence values
                // from the batch's -1 sentinel. Producer identity, not those
                // derived values, determines whether idempotence is enabled.
                record.sequence = NO_SEQUENCE.wrapping_add(delta_i32);
            }

            let encoded = encode_records(&assigned)?;
            if encoded.len() > MAX_BATCH_BYTES {
                return Err(LogError::BatchTooLarge {
                    actual: encoded.len(),
                    maximum: MAX_BATCH_BYTES,
                });
            }

            let object = self.segment_path(topic, Uuid::new_v4());
            let checksum = sha256_hex(&encoded);
            self.store
                .put_opts(&object, encoded.clone().into(), PutMode::Create.into())
                .await?;

            let record_count =
                i64::try_from(assigned.len()).map_err(|_| LogError::OffsetOverflow)?;
            let next_offset = base_offset
                .checked_add(record_count)
                .ok_or(LogError::OffsetOverflow)?;
            let mut next = loaded.manifest.clone();
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or(LogError::RevisionOverflow)?;
            next.next_offset = next_offset;
            next.segments.push(Segment {
                object: object.to_string(),
                base_offset,
                record_count: u32::try_from(assigned.len())
                    .map_err(|_| LogError::OffsetOverflow)?,
                byte_length: u64::try_from(encoded.len()).map_err(|_| LogError::OffsetOverflow)?,
                sha256: checksum,
            });

            let bytes = Bytes::from(serde_json::to_vec(&next)?);
            let mode = loaded.version.map_or(PutMode::Create, PutMode::Update);
            match self
                .store
                .put_opts(&self.manifest_path(topic), bytes.into(), mode.into())
                .await
            {
                Ok(_) => {
                    return Ok(AppendResult {
                        base_offset,
                        last_offset: next_offset - 1,
                    });
                }
                Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => {
                    // The segment is an unreferenced immutable orphan. It is
                    // intentionally invisible and can be collected later.
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(LogError::ContentionExhausted {
            attempts: MAX_CAS_ATTEMPTS,
        })
    }

    /// Fetch committed records beginning at an inclusive offset.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<Record>, LogError> {
        validate_topic(topic)?;
        validate_partition(partition)?;
        if offset < 0 {
            return Err(LogError::InvalidOffset { offset });
        }

        let loaded = self.load_manifest(topic).await?;
        if offset >= loaded.manifest.next_offset {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for segment in loaded.manifest.segments {
            let last_offset = segment.last_offset()?;
            if last_offset < offset {
                continue;
            }

            let path =
                Path::parse(&segment.object).map_err(|source| LogError::InvalidManifest {
                    detail: format!("invalid segment object path: {source}"),
                })?;
            let bytes = self
                .store
                .get(&path)
                .await
                .map_err(|source| LogError::MissingSegment {
                    object: segment.object.clone(),
                    source,
                })?
                .bytes()
                .await
                .map_err(|source| LogError::MissingSegment {
                    object: segment.object.clone(),
                    source,
                })?;

            if u64::try_from(bytes.len()).map_err(|_| LogError::OffsetOverflow)?
                != segment.byte_length
                || sha256_hex(&bytes) != segment.sha256
            {
                return Err(LogError::CorruptSegment {
                    object: segment.object,
                    detail: "length or SHA-256 mismatch".into(),
                });
            }

            let mut input = bytes;
            let decoded = RecordBatchDecoder::decode_all(&mut input).map_err(|source| {
                LogError::CorruptSegment {
                    object: segment.object.clone(),
                    detail: source.to_string(),
                }
            })?;
            let segment_records: Vec<_> = decoded.into_iter().flat_map(|set| set.records).collect();
            validate_segment_records(&segment, &segment_records)?;
            records.extend(
                segment_records
                    .into_iter()
                    .filter(|record| record.offset >= offset),
            );
        }

        Ok(records)
    }

    /// Return earliest inclusive and latest exclusive offsets.
    pub async fn offsets(&self, topic: &str, partition: i32) -> Result<OffsetRange, LogError> {
        validate_topic(topic)?;
        validate_partition(partition)?;
        let manifest = self.load_manifest(topic).await?.manifest;
        Ok(OffsetRange {
            earliest: 0,
            latest: manifest.next_offset,
        })
    }

    fn manifest_path(&self, topic: &str) -> Path {
        Path::from(format!("{}/topics/{topic}/0/manifest.json", self.prefix))
    }

    fn segment_path(&self, topic: &str, id: Uuid) -> Path {
        Path::from(format!(
            "{}/topics/{topic}/0/segments/{id}.batch",
            self.prefix
        ))
    }

    async fn load_manifest(&self, topic: &str) -> Result<LoadedManifest, LogError> {
        let path = self.manifest_path(topic);
        match self.store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await?;
                let manifest: Manifest =
                    serde_json::from_slice(&bytes).map_err(|source| LogError::InvalidManifest {
                        detail: source.to_string(),
                    })?;
                manifest.validate(&self.prefix, topic)?;
                Ok(LoadedManifest {
                    manifest,
                    version: Some(version),
                })
            }
            Err(StoreError::NotFound { .. }) => Ok(LoadedManifest {
                manifest: Manifest::default(),
                version: None,
            }),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u32,
    revision: u64,
    next_offset: i64,
    segments: Vec<Segment>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            schema: MANIFEST_SCHEMA,
            revision: 0,
            next_offset: 0,
            segments: Vec::new(),
        }
    }
}

impl Manifest {
    fn validate(&self, prefix: &str, topic: &str) -> Result<(), LogError> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(LogError::InvalidManifest {
                detail: format!("unsupported schema {}", self.schema),
            });
        }
        if self.revision != self.segments.len() as u64 {
            return Err(LogError::InvalidManifest {
                detail: "revision does not match committed segment count".into(),
            });
        }

        let expected_prefix = format!("{prefix}/topics/{topic}/0/segments/");
        let mut next_offset = 0_i64;
        let mut objects = HashSet::new();
        for segment in &self.segments {
            if segment.base_offset != next_offset {
                return Err(LogError::InvalidManifest {
                    detail: "segment offsets are not contiguous".into(),
                });
            }
            if segment.record_count == 0 || segment.byte_length == 0 {
                return Err(LogError::InvalidManifest {
                    detail: "segment has an empty record or byte count".into(),
                });
            }
            if !segment.object.starts_with(&expected_prefix)
                || !segment.object.ends_with(".batch")
                || !objects.insert(&segment.object)
            {
                return Err(LogError::InvalidManifest {
                    detail: "segment object is outside the partition or duplicated".into(),
                });
            }
            if segment.sha256.len() != 64
                || !segment.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(LogError::InvalidManifest {
                    detail: "segment checksum is not a SHA-256 hex digest".into(),
                });
            }
            next_offset = segment
                .last_offset()?
                .checked_add(1)
                .ok_or(LogError::OffsetOverflow)?;
        }
        if self.next_offset != next_offset {
            return Err(LogError::InvalidManifest {
                detail: "next offset does not match committed segments".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Segment {
    object: String,
    base_offset: i64,
    record_count: u32,
    byte_length: u64,
    sha256: String,
}

impl Segment {
    fn last_offset(&self) -> Result<i64, LogError> {
        self.base_offset
            .checked_add(i64::from(self.record_count) - 1)
            .ok_or(LogError::OffsetOverflow)
    }
}

#[derive(Debug)]
struct LoadedManifest {
    manifest: Manifest,
    version: Option<UpdateVersion>,
}

/// Log validation, persistence, or decoding failure.
#[derive(Debug, Error)]
pub enum LogError {
    /// Topic is not a safe Kafka/object-store path component.
    #[error("invalid topic name {topic:?}")]
    InvalidTopic { topic: String },
    /// The MVP only exposes partition zero.
    #[error("partition {partition} is unsupported; the MVP supports partition 0 only")]
    UnsupportedPartition { partition: i32 },
    /// Append input was empty.
    #[error("append batch must contain at least one record")]
    EmptyBatch,
    /// Record metadata requires unsupported Kafka semantics.
    #[error("idempotent, transactional, and control records are unsupported in the MVP")]
    UnsupportedRecordSemantics,
    /// Fetch offset was negative.
    #[error("offset {offset} must not be negative")]
    InvalidOffset { offset: i64 },
    /// Encoded batch exceeded the bounded request size.
    #[error("encoded batch is {actual} bytes; maximum is {maximum}")]
    BatchTooLarge { actual: usize, maximum: usize },
    /// Offset arithmetic overflowed.
    #[error("offset arithmetic overflow")]
    OffsetOverflow,
    /// Manifest revision overflowed.
    #[error("manifest revision overflow")]
    RevisionOverflow,
    /// Manifest bytes or invariants are invalid.
    #[error("invalid manifest: {detail}")]
    InvalidManifest { detail: String },
    /// A committed segment cannot be loaded.
    #[error("missing committed segment {object}: {source}")]
    MissingSegment { object: String, source: StoreError },
    /// A committed segment failed integrity or codec checks.
    #[error("corrupt committed segment {object}: {detail}")]
    CorruptSegment { object: String, detail: String },
    /// Kafka record-batch codec failure.
    #[error("record batch codec failed: {detail}")]
    Codec { detail: String },
    /// Manifest JSON serialization failure.
    #[error("manifest serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// Object-store operation failure.
    #[error("object store operation failed")]
    ObjectStore(#[from] StoreError),
    /// Repeated concurrent commits did not converge.
    #[error("manifest contention did not converge after {attempts} attempts")]
    ContentionExhausted { attempts: usize },
}

fn validate_prefix(prefix: &str) -> Result<(), LogError> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(LogError::InvalidManifest {
            detail: "engine prefix must be a safe relative object path".into(),
        });
    }
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), LogError> {
    let valid = !topic.is_empty()
        && topic.len() <= 249
        && topic != "."
        && topic != ".."
        && topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    valid.then_some(()).ok_or_else(|| LogError::InvalidTopic {
        topic: topic.to_owned(),
    })
}

fn validate_partition(partition: i32) -> Result<(), LogError> {
    (partition == 0)
        .then_some(())
        .ok_or(LogError::UnsupportedPartition { partition })
}

fn validate_records(records: &[Record]) -> Result<(), LogError> {
    if records.is_empty() {
        return Err(LogError::EmptyBatch);
    }
    if records.iter().any(|record| {
        record.transactional
            || record.control
            || record.delete_horizon
            || record.producer_id != NO_PRODUCER_ID
            || record.producer_epoch != NO_PRODUCER_EPOCH
    }) {
        return Err(LogError::UnsupportedRecordSemantics);
    }
    Ok(())
}

fn encode_records(records: &[Record]) -> Result<Bytes, LogError> {
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|source| LogError::Codec {
        detail: source.to_string(),
    })?;
    Ok(encoded.freeze())
}

fn validate_segment_records(segment: &Segment, records: &[Record]) -> Result<(), LogError> {
    if records.len() != segment.record_count as usize
        || records
            .iter()
            .enumerate()
            .any(|(delta, record)| record.offset != segment.base_offset + delta as i64)
    {
        return Err(LogError::CorruptSegment {
            object: segment.object.clone(),
            detail: "decoded record offsets or count do not match manifest".into(),
        });
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fmt,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use futures::{StreamExt, stream::BoxStream};
    use kafka_protocol::{indexmap::IndexMap, records::TimestampType};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    };
    use tokio::sync::Barrier;

    use super::*;

    fn record(value: &str) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp: 1_777_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: IndexMap::new(),
        }
    }

    #[derive(Debug)]
    struct ContentionStore {
        inner: InMemory,
        first_manifest_writes: Barrier,
        manifest_write_count: AtomicUsize,
        conflict_count: AtomicUsize,
    }

    impl ContentionStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                first_manifest_writes: Barrier::new(2),
                manifest_write_count: AtomicUsize::new(0),
                conflict_count: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for ContentionStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("contention-test-store")
        }
    }

    #[async_trait]
    impl ObjectStore for ContentionStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            let is_manifest = location.to_string().ends_with("/manifest.json");
            if is_manifest {
                let write = self.manifest_write_count.fetch_add(1, Ordering::SeqCst);
                if write < 2 {
                    self.first_manifest_writes.wait().await;
                }
            }

            let result = self.inner.put_opts(location, payload, options).await;
            if is_manifest
                && matches!(
                    result,
                    Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. })
                )
            {
                self.conflict_count.fetch_add(1, Ordering::SeqCst);
            }
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn appends_fetches_and_recovers_from_fresh_engine() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = LogEngine::new(store.clone(), "walstream/clusters/test").unwrap();

        assert_eq!(
            engine
                .append("events", 0, vec![record("a"), record("b")])
                .await
                .unwrap(),
            AppendResult {
                base_offset: 0,
                last_offset: 1
            }
        );
        assert_eq!(
            engine.append("events", 0, vec![record("c")]).await.unwrap(),
            AppendResult {
                base_offset: 2,
                last_offset: 2
            }
        );
        assert_eq!(
            engine.offsets("events", 0).await.unwrap(),
            OffsetRange {
                earliest: 0,
                latest: 3
            }
        );

        let fresh = LogEngine::new(store, "walstream/clusters/test").unwrap();
        let fetched = fresh.fetch("events", 0, 1).await.unwrap();
        assert_eq!(
            fetched
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            fetched
                .iter()
                .map(|record| record.value.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec![b"b".as_slice(), b"c".as_slice()]
        );
    }

    #[tokio::test]
    async fn competing_writers_publish_unique_contiguous_offsets() {
        let store = Arc::new(ContentionStore::new());
        let engine = LogEngine::new(store.clone(), "walstream/clusters/test").unwrap();
        let left = {
            let writer = engine.clone();
            tokio::spawn(async move {
                writer
                    .append("races", 0, vec![record("left")])
                    .await
                    .unwrap()
                    .base_offset
            })
        };
        let right = {
            let writer = engine.clone();
            tokio::spawn(async move {
                writer
                    .append("races", 0, vec![record("right")])
                    .await
                    .unwrap()
                    .base_offset
            })
        };

        let mut offsets = vec![left.await.unwrap(), right.await.unwrap()];
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, 1]);
        assert_eq!(store.conflict_count.load(Ordering::SeqCst), 1);

        let fetched = engine.fetch("races", 0, 0).await.unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(
            fetched
                .iter()
                .map(|record| record.offset)
                .collect::<HashSet<_>>()
                .len(),
            2
        );

        let segment_prefix = Path::from("walstream/clusters/test/topics/races/0/segments");
        let segment_objects = store.list(Some(&segment_prefix)).collect::<Vec<_>>().await;
        assert_eq!(
            segment_objects.len(),
            3,
            "one losing segment must be orphaned"
        );
    }

    #[tokio::test]
    async fn accepts_decoded_normal_multi_record_batch() {
        let mut source = vec![record("a"), record("b")];
        source[1].offset = 1;
        source[1].sequence = 0;
        let mut encoded = encode_records(&source).unwrap();
        let decoded = RecordBatchDecoder::decode(&mut encoded).unwrap();
        assert_eq!(
            decoded
                .records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![-1, 0]
        );

        let engine = LogEngine::in_memory("walstream/clusters/test").unwrap();
        engine.append("events", 0, decoded.records).await.unwrap();
        let fetched = engine.fetch("events", 0, 0).await.unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].offset, 0);
        assert_eq!(fetched[1].offset, 1);
    }

    #[tokio::test]
    async fn unreferenced_segment_is_invisible() {
        let engine = LogEngine::in_memory("walstream/clusters/test").unwrap();
        let orphan = engine.segment_path("events", Uuid::new_v4());
        engine
            .store
            .put_opts(
                &orphan,
                PutPayload::from_static(b"orphan"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();

        assert!(engine.fetch("events", 0, 0).await.unwrap().is_empty());
        assert_eq!(engine.offsets("events", 0).await.unwrap().latest, 0);
    }

    #[tokio::test]
    async fn rejects_invalid_inputs_and_unsupported_semantics() {
        let engine = LogEngine::in_memory("walstream/clusters/test").unwrap();
        assert!(matches!(
            engine.append("../events", 0, vec![record("a")]).await,
            Err(LogError::InvalidTopic { .. })
        ));
        assert!(matches!(
            engine.append("events", 1, vec![record("a")]).await,
            Err(LogError::UnsupportedPartition { partition: 1 })
        ));
        assert!(matches!(
            engine.append("events", 0, Vec::new()).await,
            Err(LogError::EmptyBatch)
        ));

        let mut idempotent = record("a");
        idempotent.producer_id = 7;
        idempotent.producer_epoch = 1;
        idempotent.sequence = 0;
        assert!(matches!(
            engine.append("events", 0, vec![idempotent]).await,
            Err(LogError::UnsupportedRecordSemantics)
        ));

        let mut transactional = record("b");
        transactional.transactional = true;
        assert!(matches!(
            engine.append("events", 0, vec![transactional]).await,
            Err(LogError::UnsupportedRecordSemantics)
        ));
    }

    #[tokio::test]
    async fn rejects_malformed_manifest() {
        let engine = LogEngine::in_memory("walstream/clusters/test").unwrap();
        engine
            .store
            .put(
                &engine.manifest_path("events"),
                PutPayload::from_static(
                    br#"{"schema":1,"revision":9,"next_offset":0,"segments":[]}"#,
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            engine.fetch("events", 0, 0).await,
            Err(LogError::InvalidManifest { .. })
        ));
    }

    #[tokio::test]
    async fn detects_corrupt_committed_segment() {
        let engine = LogEngine::in_memory("walstream/clusters/test").unwrap();
        engine.append("events", 0, vec![record("a")]).await.unwrap();
        let manifest = engine.load_manifest("events").await.unwrap().manifest;
        let path = Path::parse(&manifest.segments[0].object).unwrap();
        engine
            .store
            .put(&path, PutPayload::from_static(b"corrupt"))
            .await
            .unwrap();

        assert!(matches!(
            engine.fetch("events", 0, 0).await,
            Err(LogError::CorruptSegment { .. })
        ));
    }
}
