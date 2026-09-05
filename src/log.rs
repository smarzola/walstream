//! Object-store-backed ordered log.
//!
//! Each topic has durable metadata and one versioned index root per partition.
//! Appends first create immutable record and index objects, then make them visible by
//! conditionally creating or updating the manifest with its last-read ETag.
//! Losing a manifest race leaves an invisible orphan and retries against fresh
//! state; no acknowledged record depends on local process state.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use kafka_protocol::records::{
    Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record, RecordBatchEncoder,
    RecordEncodeOptions,
};
use object_store::{
    Error as StoreError, GetResult, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    memory::InMemory, path::Path,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as DeserializeError, SeqAccess, Visitor},
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::codec::{MAX_BATCH_RECORDS, decode_record_batches, inspect_record_batches};

mod index;
use index::{INDEX_SCHEMA, PAGE_ENTRIES, Root, Selection};

const MANIFEST_SCHEMA: u32 = 1;
const MAX_CAS_ATTEMPTS: usize = 128;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_MANIFEST_SEGMENTS: usize = 10_000;
const TOPIC_METADATA_SCHEMA: u32 = 1;
const MAX_TOPIC_METADATA_BYTES: usize = 4 * 1024;
/// Default partition count for newly auto-created topics.
pub const DEFAULT_TOPIC_PARTITIONS: i32 = 1;
/// Maximum partition count accepted from local configuration or durable metadata.
pub const MAX_TOPIC_PARTITIONS: i32 = 1024;

/// Durable partition log backed by an object store.
#[derive(Clone)]
pub struct LogEngine {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    default_topic_partitions: i32,
}

impl std::fmt::Debug for LogEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogEngine")
            .field("prefix", &self.prefix)
            .field("default_topic_partitions", &self.default_topic_partitions)
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

/// Bounded encoded record batches plus the partition high watermark.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedFetch {
    /// Complete Kafka v2 record batches selected from committed segments.
    pub records: Bytes,
    /// Offset immediately after the final committed record.
    pub high_watermark: i64,
    /// Whether the first-batch exception exceeded the requested byte budget.
    pub oversized_first_batch: bool,
}

impl LogEngine {
    /// Create an engine over any object-store implementation.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, LogError> {
        Self::with_default_topic_partitions(store, prefix, DEFAULT_TOPIC_PARTITIONS)
    }

    /// Create an engine with a bounded creation-time default for new topics.
    pub fn with_default_topic_partitions(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        default_topic_partitions: i32,
    ) -> Result<Self, LogError> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        validate_partition_count(default_topic_partitions)?;
        Ok(Self {
            store,
            prefix,
            default_topic_partitions,
        })
    }

    /// Create an engine backed by a fresh in-memory store.
    pub fn in_memory(prefix: impl Into<String>) -> Result<Self, LogError> {
        Self::new(Arc::new(InMemory::new()), prefix)
    }

    /// Create an in-memory engine with a non-default topic partition count.
    pub fn in_memory_with_partitions(
        prefix: impl Into<String>,
        default_topic_partitions: i32,
    ) -> Result<Self, LogError> {
        Self::with_default_topic_partitions(
            Arc::new(InMemory::new()),
            prefix,
            default_topic_partitions,
        )
    }

    /// Ensure a topic exists durably and contains the requested partition.
    ///
    /// Concurrent creators converge through the same conditional-create
    /// primitive used by first append.
    pub async fn ensure_topic(&self, topic: &str, partition: i32) -> Result<i32, LogError> {
        validate_topic(topic)?;
        let partition_count = match self.load_topic_metadata(topic).await? {
            Some(metadata) => metadata.partition_count,
            None => {
                let partition_count = if self.load_manifest(topic, 0).await?.is_some() {
                    DEFAULT_TOPIC_PARTITIONS
                } else {
                    self.default_topic_partitions
                };
                validate_partition(partition, partition_count)?;
                self.create_topic_metadata(topic, partition_count).await?
            }
        };
        validate_partition(partition, partition_count)?;
        Ok(partition_count)
    }

    async fn create_topic_metadata(
        &self,
        topic: &str,
        partition_count: i32,
    ) -> Result<i32, LogError> {
        let metadata = TopicMetadata {
            schema: TOPIC_METADATA_SCHEMA,
            partition_count,
        };
        let bytes = Bytes::from(serde_json::to_vec(&metadata)?);
        match self
            .store
            .put_opts(
                &self.topic_metadata_path(topic),
                bytes.into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(partition_count),
            Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => self
                .load_topic_metadata(topic)
                .await?
                .ok_or_else(|| LogError::UnknownTopic {
                    topic: topic.to_owned(),
                })
                .map(|metadata| metadata.partition_count),
            Err(error) => Err(error.into()),
        }
    }

    /// Return a topic's durable partition count, including legacy inference.
    pub async fn topic_partition_count(&self, topic: &str) -> Result<Option<i32>, LogError> {
        validate_topic(topic)?;
        if let Some(metadata) = self.load_topic_metadata(topic).await? {
            return Ok(Some(metadata.partition_count));
        }
        self.load_manifest(topic, 0)
            .await
            .map(|manifest| manifest.map(|_| DEFAULT_TOPIC_PARTITIONS))
    }

    /// List topics that have a committed manifest in object storage.
    pub async fn topics(&self) -> Result<Vec<String>, LogError> {
        let prefix = format!("{}/topics", self.prefix);
        let path = Path::from(prefix.clone());
        let expected = format!("{prefix}/");
        let listed = self.store.list_with_delimiter(Some(&path)).await?;
        let mut topics = BTreeSet::new();

        for common_prefix in listed.common_prefixes {
            let object = common_prefix.to_string();
            let Some(topic) = object.strip_prefix(&expected) else {
                continue;
            };
            if topic.contains('/') {
                continue;
            }
            validate_topic(topic)?;
            if self.topic_partition_count(topic).await?.is_some() {
                topics.insert(topic.to_owned());
            }
        }

        Ok(topics.into_iter().collect())
    }

    /// Return whether a valid committed topic manifest exists.
    pub async fn topic_exists(&self, topic: &str, partition: i32) -> Result<bool, LogError> {
        validate_topic(topic)?;
        let Some(partition_count) = self.topic_partition_count(topic).await? else {
            return Ok(false);
        };
        validate_partition(partition, partition_count)?;
        Ok(true)
    }

    /// Append a non-empty record batch to a durable partition.
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
        validate_partition(partition, MAX_TOPIC_PARTITIONS)?;
        validate_records(&records)?;
        if records.len() > MAX_BATCH_RECORDS {
            return Err(LogError::TooManyRecords {
                actual: records.len(),
                maximum: MAX_BATCH_RECORDS,
            });
        }
        validate_timestamp_span(&records)?;
        self.ensure_topic(topic, partition).await?;

        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self
                .load_manifest(topic, partition)
                .await?
                .unwrap_or_else(LoadedManifest::empty);
            let base_offset = loaded.manifest.next_offset();
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
                // Segment objects are one canonical batch. Incoming batch
                // leader epochs have no meaning in this single virtual broker.
                record.partition_leader_epoch = -1;
            }

            validate_timestamp_span(&assigned)?;
            let encoded = encode_records(&assigned)?;
            if encoded.len() > MAX_BATCH_BYTES {
                return Err(LogError::BatchTooLarge {
                    actual: encoded.len(),
                    maximum: MAX_BATCH_BYTES,
                });
            }
            let inspection =
                inspect_record_batches(&encoded).map_err(|source| LogError::Codec {
                    detail: source.to_string(),
                })?;
            if inspection.batch_count != 1 || inspection.record_count != assigned.len() {
                return Err(LogError::Codec {
                    detail: "canonical segment did not encode as exactly one batch".into(),
                });
            }

            let object = self.segment_path(topic, partition, Uuid::new_v4());
            let checksum = sha256_hex(&encoded);
            let record_count =
                i64::try_from(assigned.len()).map_err(|_| LogError::OffsetOverflow)?;
            let next_offset = base_offset
                .checked_add(record_count)
                .ok_or(LogError::OffsetOverflow)?;
            let mut next = match loaded.manifest {
                LogManifest::Legacy(legacy) => {
                    self.migrate_manifest(topic, partition, legacy).await?
                }
                LogManifest::Indexed(root) => root,
            };
            if next.tail.len() == PAGE_ENTRIES {
                self.seal_tail(topic, partition, &mut next).await?;
            }
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or(LogError::RevisionOverflow)?;
            next.next_offset = next_offset;
            next.tail.push(Segment {
                object: object.to_string(),
                base_offset,
                record_count: u32::try_from(assigned.len())
                    .map_err(|_| LogError::OffsetOverflow)?,
                byte_length: encoded.len() as u64,
                sha256: checksum,
            });
            next.validate(&self.prefix, topic, partition)?;

            let bytes = Bytes::from(serde_json::to_vec(&next)?);
            if bytes.len() > MAX_MANIFEST_BYTES {
                return Err(LogError::ManifestTooLarge {
                    actual: bytes.len(),
                    maximum: MAX_MANIFEST_BYTES,
                });
            }
            self.store
                .put_opts(&object, encoded.clone().into(), PutMode::Create.into())
                .await?;
            let mode = loaded.version.map_or(PutMode::Create, PutMode::Update);
            match self
                .store
                .put_opts(
                    &self.manifest_path(topic, partition),
                    bytes.into(),
                    mode.into(),
                )
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

    /// Fetch all committed records beginning at an inclusive offset.
    ///
    /// Protocol-serving code should use [`Self::fetch_bounded`] so request
    /// limits are applied before segment objects are downloaded.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<Record>, LogError> {
        let fetched = self
            .fetch_bounded(topic, partition, offset, usize::MAX, false)
            .await?;
        if fetched.records.is_empty() {
            return Ok(Vec::new());
        }
        let (_, decoded) =
            decode_record_batches(fetched.records).map_err(|source| LogError::Codec {
                detail: source.to_string(),
            })?;
        let records = decoded
            .into_iter()
            .filter(|record| record.offset >= offset)
            .collect();
        Ok(records)
    }

    /// Fetch complete committed segment batches within an object-read budget.
    ///
    /// Manifest byte lengths select segments before any segment GET. When
    /// `allow_oversized_first_batch` is true, at most the first eligible
    /// segment may exceed `maximum_bytes`, matching Kafka's first-batch rule.
    pub async fn fetch_bounded(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        maximum_bytes: usize,
        allow_oversized_first_batch: bool,
    ) -> Result<BoundedFetch, LogError> {
        validate_topic(topic)?;
        let partition_count =
            self.topic_partition_count(topic)
                .await?
                .ok_or_else(|| LogError::UnknownTopic {
                    topic: topic.to_owned(),
                })?;
        validate_partition(partition, partition_count)?;
        if offset < 0 {
            return Err(LogError::InvalidOffset { offset });
        }

        let loaded = self
            .load_manifest(topic, partition)
            .await?
            .unwrap_or_else(LoadedManifest::empty);
        let high_watermark = loaded.manifest.next_offset();
        if offset > high_watermark {
            return Err(LogError::OffsetOutOfRange {
                offset,
                latest: high_watermark,
            });
        }
        if offset == high_watermark {
            return Ok(BoundedFetch {
                records: Bytes::new(),
                high_watermark,
                oversized_first_batch: false,
            });
        }

        let mut selection = Selection::new(offset, maximum_bytes, allow_oversized_first_batch);
        match loaded.manifest {
            LogManifest::Legacy(manifest) => {
                for segment in manifest.segments {
                    if !selection.push(segment)? {
                        break;
                    }
                }
            }
            LogManifest::Indexed(root) => {
                self.indexed_segments(topic, partition, root, &mut selection)
                    .await?
            }
        }

        // Manifest lengths are validated and the protocol clamps selection,
        // but avoid one eager allocation from durable metadata regardless.
        let mut encoded = BytesMut::new();
        for segment in selection.segments {
            let bytes = self.read_segment(&segment).await?;
            encoded.extend_from_slice(&bytes);
        }
        Ok(BoundedFetch {
            records: encoded.freeze(),
            high_watermark,
            oversized_first_batch: selection.oversized_first_batch,
        })
    }

    async fn read_segment(&self, segment: &Segment) -> Result<Bytes, LogError> {
        let path = Path::parse(&segment.object).map_err(|source| LogError::InvalidManifest {
            detail: format!("invalid segment object path: {source}"),
        })?;
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|source| LogError::MissingSegment {
                object: segment.object.clone(),
                source,
            })?;
        if result.meta.size != segment.byte_length {
            return Err(LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: "object metadata length does not match the manifest".into(),
            });
        }
        let maximum = usize::try_from(segment.byte_length).map_err(|_| LogError::OffsetOverflow)?;
        let bytes = collect_bounded(result, maximum)
            .await
            .map_err(|error| match error {
                BoundedReadError::Store(source) => LogError::MissingSegment {
                    object: segment.object.clone(),
                    source,
                },
                BoundedReadError::TooLarge => LogError::CorruptSegment {
                    object: segment.object.clone(),
                    detail: "object body exceeds the manifest length".into(),
                },
            })?;

        if bytes.len() != maximum || sha256_hex(&bytes) != segment.sha256 {
            return Err(LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: "length or SHA-256 mismatch".into(),
            });
        }

        let (inspection, segment_records) =
            decode_record_batches(bytes.clone()).map_err(|source| LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: source.to_string(),
            })?;
        if inspection.batch_count != 1 || inspection.record_count != segment.record_count as usize {
            return Err(LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: "invalid canonical record-batch headers".into(),
            });
        }
        validate_segment_records(segment, &segment_records)?;
        validate_canonical_segment_records(segment, &segment_records)?;
        let canonical =
            encode_records(&segment_records).map_err(|source| LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: format!("cannot safely re-encode committed records: {source}"),
            })?;
        if canonical != bytes {
            return Err(LogError::CorruptSegment {
                object: segment.object.clone(),
                detail: "record batch is not the writer's canonical encoding".into(),
            });
        }
        Ok(bytes)
    }

    /// Return earliest inclusive and latest exclusive offsets.
    pub async fn offsets(&self, topic: &str, partition: i32) -> Result<OffsetRange, LogError> {
        validate_topic(topic)?;
        let partition_count =
            self.topic_partition_count(topic)
                .await?
                .ok_or_else(|| LogError::UnknownTopic {
                    topic: topic.to_owned(),
                })?;
        validate_partition(partition, partition_count)?;
        let manifest = self
            .load_manifest(topic, partition)
            .await?
            .unwrap_or_else(LoadedManifest::empty)
            .manifest;
        Ok(OffsetRange {
            earliest: 0,
            latest: manifest.next_offset(),
        })
    }

    fn topic_metadata_path(&self, topic: &str) -> Path {
        Path::from(format!("{}/topics/{topic}/metadata.json", self.prefix))
    }

    fn manifest_path(&self, topic: &str, partition: i32) -> Path {
        Path::from(format!(
            "{}/topics/{topic}/{partition}/manifest.json",
            self.prefix
        ))
    }

    fn segment_path(&self, topic: &str, partition: i32, id: Uuid) -> Path {
        Path::from(format!(
            "{}/topics/{topic}/{partition}/segments/{id}.batch",
            self.prefix
        ))
    }

    async fn load_topic_metadata(&self, topic: &str) -> Result<Option<TopicMetadata>, LogError> {
        let path = self.topic_metadata_path(topic);
        match self.store.get(&path).await {
            Ok(result) => {
                if result.meta.size > MAX_TOPIC_METADATA_BYTES as u64 {
                    return Err(LogError::InvalidTopicMetadata {
                        detail: format!(
                            "topic metadata is {} bytes; maximum is {MAX_TOPIC_METADATA_BYTES}",
                            result.meta.size
                        ),
                    });
                }
                let bytes = collect_bounded(result, MAX_TOPIC_METADATA_BYTES)
                    .await
                    .map_err(|error| match error {
                        BoundedReadError::Store(source) => LogError::ObjectStore(source),
                        BoundedReadError::TooLarge => LogError::InvalidTopicMetadata {
                            detail: format!(
                                "topic metadata body exceeds {MAX_TOPIC_METADATA_BYTES} bytes"
                            ),
                        },
                    })?;
                let metadata: TopicMetadata = serde_json::from_slice(&bytes).map_err(|source| {
                    LogError::InvalidTopicMetadata {
                        detail: source.to_string(),
                    }
                })?;
                metadata.validate()?;
                Ok(Some(metadata))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn load_manifest(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<LoadedManifest>, LogError> {
        let path = self.manifest_path(topic, partition);
        match self.store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                if result.meta.size > MAX_MANIFEST_BYTES as u64 {
                    return Err(LogError::InvalidManifest {
                        detail: format!(
                            "manifest is {} bytes; maximum is {MAX_MANIFEST_BYTES}",
                            result.meta.size
                        ),
                    });
                }
                let bytes = collect_bounded(result, MAX_MANIFEST_BYTES)
                    .await
                    .map_err(|error| match error {
                        BoundedReadError::Store(source) => LogError::ObjectStore(source),
                        BoundedReadError::TooLarge => LogError::InvalidManifest {
                            detail: format!("manifest body exceeds {MAX_MANIFEST_BYTES} bytes"),
                        },
                    })?;
                #[derive(Deserialize)]
                struct Schema {
                    schema: u32,
                }
                let Schema { schema } =
                    serde_json::from_slice(&bytes).map_err(|e| LogError::InvalidManifest {
                        detail: e.to_string(),
                    })?;
                let manifest = match schema {
                    MANIFEST_SCHEMA => {
                        let legacy: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                            LogError::InvalidManifest {
                                detail: e.to_string(),
                            }
                        })?;
                        legacy.validate(&self.prefix, topic, partition)?;
                        LogManifest::Legacy(legacy)
                    }
                    INDEX_SCHEMA => {
                        let root: Root = serde_json::from_slice(&bytes).map_err(|e| {
                            LogError::InvalidManifest {
                                detail: e.to_string(),
                            }
                        })?;
                        root.validate(&self.prefix, topic, partition)?;
                        LogManifest::Indexed(root)
                    }
                    _ => {
                        return Err(LogError::InvalidManifest {
                            detail: format!("unsupported manifest schema {schema}"),
                        });
                    }
                };
                Ok(Some(LoadedManifest {
                    manifest,
                    version: Some(version),
                }))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

async fn collect_bounded(
    result: GetResult,
    maximum_bytes: usize,
) -> Result<Bytes, BoundedReadError> {
    let mut stream = result.into_stream();
    let mut output = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(BoundedReadError::Store)?;
        if output
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > maximum_bytes)
        {
            return Err(BoundedReadError::TooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output.freeze())
}

enum BoundedReadError {
    Store(StoreError),
    TooLarge,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TopicMetadata {
    schema: u32,
    partition_count: i32,
}

impl TopicMetadata {
    fn validate(&self) -> Result<(), LogError> {
        if self.schema != TOPIC_METADATA_SCHEMA {
            return Err(LogError::InvalidTopicMetadata {
                detail: format!("unsupported schema {}", self.schema),
            });
        }
        validate_partition_count(self.partition_count)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u32,
    revision: u64,
    next_offset: i64,
    #[serde(deserialize_with = "deserialize_segments")]
    segments: Vec<Segment>,
}

fn deserialize_segments<'de, D>(deserializer: D) -> Result<Vec<Segment>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded::<D, Segment, MAX_MANIFEST_SEGMENTS>(deserializer)
}

fn deserialize_bounded<'de, D, T, const N: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for Bounded<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at most {N} metadata entries")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            if sequence.size_hint().is_some_and(|size| size > N) {
                return Err(A::Error::custom("metadata entry limit exceeded"));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default().min(N));
            while values.len() < N {
                match sequence.next_element()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(A::Error::custom("metadata entry limit exceeded"));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Bounded::<T, N>(std::marker::PhantomData))
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
    fn validate(&self, prefix: &str, topic: &str, partition: i32) -> Result<(), LogError> {
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

        let next_offset = validate_segments(&self.segments, 0, prefix, topic, partition)?;
        if self.next_offset != next_offset {
            return Err(LogError::InvalidManifest {
                detail: "next offset does not match committed segments".into(),
            });
        }
        Ok(())
    }
}

fn valid_checksum(checksum: &str) -> bool {
    checksum.len() == 64 && checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_segments(
    segments: &[Segment],
    start: i64,
    prefix: &str,
    topic: &str,
    partition: i32,
) -> Result<i64, LogError> {
    let expected_prefix = format!("{prefix}/topics/{topic}/{partition}/segments/");
    let mut next_offset = start;
    let mut objects = HashSet::new();
    for segment in segments {
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
        if segment.record_count as usize > MAX_BATCH_RECORDS
            || segment.byte_length > MAX_BATCH_BYTES as u64
        {
            return Err(LogError::InvalidManifest {
                detail: "segment exceeds the writer's record or byte bound".into(),
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
        if !valid_checksum(&segment.sha256) {
            return Err(LogError::InvalidManifest {
                detail: "segment checksum is not a SHA-256 hex digest".into(),
            });
        }
        next_offset = segment
            .last_offset()?
            .checked_add(1)
            .ok_or(LogError::OffsetOverflow)?;
    }
    Ok(next_offset)
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

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum LogManifest {
    Legacy(Manifest),
    Indexed(Root),
}

impl LogManifest {
    fn next_offset(&self) -> i64 {
        match self {
            Self::Legacy(m) => m.next_offset,
            Self::Indexed(m) => m.next_offset,
        }
    }

    #[cfg(test)]
    fn tail(&self) -> &[Segment] {
        match self {
            Self::Legacy(m) => &m.segments,
            Self::Indexed(m) => &m.tail,
        }
    }

    #[cfg(test)]
    fn tail_mut(&mut self) -> &mut [Segment] {
        match self {
            Self::Legacy(m) => &mut m.segments,
            Self::Indexed(m) => &mut m.tail,
        }
    }
}

#[derive(Debug)]
struct LoadedManifest {
    manifest: LogManifest,
    version: Option<UpdateVersion>,
}

impl LoadedManifest {
    fn empty() -> Self {
        Self {
            manifest: LogManifest::Indexed(Root::default()),
            version: None,
        }
    }
}

/// Log validation, persistence, or decoding failure.
#[derive(Debug, Error)]
pub enum LogError {
    /// Topic is not a safe Kafka/object-store path component.
    #[error("invalid topic name {topic:?}")]
    InvalidTopic { topic: String },
    /// Durable topic metadata is corrupt or outside supported bounds.
    #[error("invalid topic metadata: {detail}")]
    InvalidTopicMetadata { detail: String },
    /// No committed manifest exists for the requested topic.
    #[error("topic {topic:?} does not exist")]
    UnknownTopic { topic: String },
    /// The requested partition is outside the topic's durable partition range.
    #[error("partition {partition} is unsupported for this topic")]
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
    /// Fetch offset is beyond the partition high watermark.
    #[error("offset {offset} is beyond latest offset {latest}")]
    OffsetOutOfRange { offset: i64, latest: i64 },
    /// Encoded batch exceeded the bounded request size.
    #[error("encoded batch is {actual} bytes; maximum is {maximum}")]
    BatchTooLarge { actual: usize, maximum: usize },
    /// Declared record count exceeded the bounded decoder limit.
    #[error("batch contains {actual} records; maximum is {maximum}")]
    TooManyRecords { actual: usize, maximum: usize },
    /// The serialized manifest exceeded its bounded object size.
    #[error("manifest is {actual} bytes; maximum is {maximum}")]
    ManifestTooLarge { actual: usize, maximum: usize },
    /// Offset arithmetic overflowed.
    #[error("offset arithmetic overflow")]
    OffsetOverflow,
    /// Canonicalizing the batch would overflow timestamp delta arithmetic.
    #[error("record timestamps span a range that Kafka v2 cannot encode")]
    InvalidTimestampRange,
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

fn validate_partition_count(partition_count: i32) -> Result<(), LogError> {
    (DEFAULT_TOPIC_PARTITIONS..=MAX_TOPIC_PARTITIONS)
        .contains(&partition_count)
        .then_some(())
        .ok_or_else(|| LogError::InvalidTopicMetadata {
            detail: format!(
                "partition count {partition_count} is outside {DEFAULT_TOPIC_PARTITIONS}..={MAX_TOPIC_PARTITIONS}"
            ),
        })
}

fn validate_partition(partition: i32, partition_count: i32) -> Result<(), LogError> {
    (0..partition_count)
        .contains(&partition)
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

fn validate_timestamp_span(records: &[Record]) -> Result<(), LogError> {
    let minimum = records.iter().map(|record| record.timestamp).min();
    let maximum = records.iter().map(|record| record.timestamp).max();
    match (minimum, maximum) {
        (Some(minimum), Some(maximum)) if maximum.checked_sub(minimum).is_none() => {
            Err(LogError::InvalidTimestampRange)
        }
        _ => Ok(()),
    }
}

pub(crate) fn encode_records(records: &[Record]) -> Result<Bytes, LogError> {
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

fn validate_canonical_segment_records(
    segment: &Segment,
    records: &[Record],
) -> Result<(), LogError> {
    validate_records(records).map_err(|source| LogError::CorruptSegment {
        object: segment.object.clone(),
        detail: format!("committed records use unsupported semantics: {source}"),
    })?;
    validate_timestamp_span(records).map_err(|source| LogError::CorruptSegment {
        object: segment.object.clone(),
        detail: format!("committed timestamps are not canonical: {source}"),
    })?;
    if records.iter().enumerate().any(|(delta, record)| {
        record.partition_leader_epoch != -1
            || i32::try_from(delta)
                .ok()
                .is_none_or(|delta| record.sequence != NO_SEQUENCE.wrapping_add(delta))
    }) {
        return Err(LogError::CorruptSegment {
            object: segment.object.clone(),
            detail: "record leader epoch or sequence is not canonical".into(),
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
        fmt::{self, Write},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use futures::{StreamExt, stream::BoxStream};
    use kafka_protocol::{
        indexmap::IndexMap,
        records::{RecordBatchDecoder, TimestampType},
    };
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    };
    use tokio::sync::Barrier;

    use super::*;

    pub(super) fn record(value: &str) -> Record {
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

    async fn forge_committed_segment(
        engine: &LogEngine,
        topic: &str,
        mutate: impl FnOnce(&mut [u8]),
    ) {
        let mut manifest = engine
            .load_manifest(topic, 0)
            .await
            .unwrap()
            .unwrap()
            .manifest;
        let segment_path = Path::from(manifest.tail()[0].object.clone());
        let mut forged = engine
            .store
            .get(&segment_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        mutate(&mut forged);
        let checksum = crc32c::crc32c(&forged[21..]);
        forged[17..21].copy_from_slice(&checksum.to_be_bytes());
        manifest.tail_mut()[0].sha256 = sha256_hex(&forged);
        engine
            .store
            .put(&segment_path, Bytes::from(forged).into())
            .await
            .unwrap();
        engine
            .store
            .put(
                &engine.manifest_path(topic, 0),
                Bytes::from(serde_json::to_vec(&manifest).unwrap()).into(),
            )
            .await
            .unwrap();
    }

    #[derive(Debug)]
    struct ContentionStore {
        inner: InMemory,
        first_topic_metadata_writes: Barrier,
        first_manifest_writes: Barrier,
        topic_metadata_write_count: AtomicUsize,
        topic_metadata_conflict_count: AtomicUsize,
        manifest_write_count: AtomicUsize,
        conflict_count: AtomicUsize,
    }

    impl ContentionStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                first_topic_metadata_writes: Barrier::new(2),
                first_manifest_writes: Barrier::new(2),
                topic_metadata_write_count: AtomicUsize::new(0),
                topic_metadata_conflict_count: AtomicUsize::new(0),
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

    #[derive(Debug)]
    struct ReadCountingStore {
        inner: InMemory,
        segment_gets: AtomicUsize,
    }

    impl ReadCountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                segment_gets: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for ReadCountingStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("read-counting-test-store")
        }
    }

    #[async_trait]
    impl ObjectStore for ReadCountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            if location.to_string().ends_with(".batch") {
                self.segment_gets.fetch_add(1, Ordering::SeqCst);
            }
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

    #[async_trait]
    impl ObjectStore for ContentionStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            let is_topic_metadata = location.to_string().ends_with("/metadata.json");
            let is_manifest = location.to_string().ends_with("/manifest.json");
            if is_topic_metadata {
                let write = self
                    .topic_metadata_write_count
                    .fetch_add(1, Ordering::SeqCst);
                if write < 2 {
                    self.first_topic_metadata_writes.wait().await;
                }
            }
            if is_manifest {
                let write = self.manifest_write_count.fetch_add(1, Ordering::SeqCst);
                if write < 2 {
                    self.first_manifest_writes.wait().await;
                }
            }

            let result = self.inner.put_opts(location, payload, options).await;
            if is_topic_metadata
                && matches!(
                    result,
                    Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. })
                )
            {
                self.topic_metadata_conflict_count
                    .fetch_add(1, Ordering::SeqCst);
            }
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
    async fn multi_partition_topics_persist_counts_and_isolate_logs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = LogEngine::with_default_topic_partitions(
            store.clone(),
            "walstream/clusters/multi-partition",
            3,
        )
        .unwrap();

        assert_eq!(engine.ensure_topic("events", 2).await.unwrap(), 3);
        engine
            .append("events", 0, vec![record("zero-a"), record("zero-b")])
            .await
            .unwrap();
        engine
            .append("events", 2, vec![record("two-a")])
            .await
            .unwrap();

        assert_eq!(engine.offsets("events", 0).await.unwrap().latest, 2);
        assert_eq!(engine.offsets("events", 1).await.unwrap().latest, 0);
        assert_eq!(engine.offsets("events", 2).await.unwrap().latest, 1);
        assert_eq!(
            engine.fetch("events", 2, 0).await.unwrap()[0]
                .value
                .as_deref(),
            Some(b"two-a".as_slice())
        );
        assert!(matches!(
            engine.offsets("events", 3).await,
            Err(LogError::UnsupportedPartition { partition: 3 })
        ));

        let fresh = LogEngine::with_default_topic_partitions(
            store,
            "walstream/clusters/multi-partition",
            1,
        )
        .unwrap();
        assert_eq!(
            fresh.topic_partition_count("events").await.unwrap(),
            Some(3)
        );
        assert_eq!(fresh.offsets("events", 2).await.unwrap().latest, 1);
    }

    #[tokio::test]
    async fn concurrent_multi_partition_topic_creators_converge_on_one_durable_count() {
        let store = Arc::new(ContentionStore::new());
        let three = LogEngine::with_default_topic_partitions(
            store.clone(),
            "walstream/clusters/topic-race",
            3,
        )
        .unwrap();
        let five = LogEngine::with_default_topic_partitions(
            store.clone(),
            "walstream/clusters/topic-race",
            5,
        )
        .unwrap();

        let (left, right) = tokio::join!(
            three.ensure_topic("events", 0),
            five.ensure_topic("events", 0)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert!(matches!(left, 3 | 5));
        assert_eq!(store.topic_metadata_write_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.topic_metadata_conflict_count.load(Ordering::SeqCst),
            1
        );

        let durable_store: Arc<dyn ObjectStore> = store;
        let fresh = LogEngine::new(durable_store, "walstream/clusters/topic-race").unwrap();
        assert_eq!(
            fresh.topic_partition_count("events").await.unwrap(),
            Some(left)
        );
    }

    #[tokio::test]
    async fn legacy_multi_partition_upgrade_infers_partition_zero_without_rewrite() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine =
            LogEngine::with_default_topic_partitions(store.clone(), "walstream/clusters/legacy", 3)
                .unwrap();
        let legacy_records = vec![record("legacy")];
        let legacy_batch = encode_records(&legacy_records).unwrap();
        let legacy_segment = engine.segment_path("events", 0, Uuid::new_v4());
        store
            .put(&legacy_segment, legacy_batch.clone().into())
            .await
            .unwrap();
        let legacy_manifest = Manifest {
            schema: MANIFEST_SCHEMA,
            revision: 1,
            next_offset: 1,
            segments: vec![Segment {
                object: legacy_segment.to_string(),
                base_offset: 0,
                record_count: 1,
                byte_length: legacy_batch.len() as u64,
                sha256: sha256_hex(&legacy_batch),
            }],
        };
        let legacy_manifest = Bytes::from(serde_json::to_vec(&legacy_manifest).unwrap());
        store
            .put(
                &engine.manifest_path("events", 0),
                legacy_manifest.clone().into(),
            )
            .await
            .unwrap();

        assert_eq!(
            engine.topic_partition_count("events").await.unwrap(),
            Some(1)
        );
        assert_eq!(engine.ensure_topic("events", 0).await.unwrap(), 1);
        assert_eq!(
            store
                .get(&engine.manifest_path("events", 0))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            legacy_manifest
        );
        assert!(
            engine
                .load_topic_metadata("events")
                .await
                .unwrap()
                .is_some()
        );
        let fresh = LogEngine::with_default_topic_partitions(
            store,
            "walstream/clusters/legacy",
            MAX_TOPIC_PARTITIONS,
        )
        .unwrap();
        assert_eq!(
            fresh.topic_partition_count("events").await.unwrap(),
            Some(1)
        );
        assert_eq!(fresh.offsets("events", 0).await.unwrap().latest, 1);
        let fetched = fresh.fetch("events", 0, 0).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].value.as_deref(), Some(b"legacy".as_slice()));
        assert!(matches!(
            fresh.ensure_topic("events", 1).await,
            Err(LogError::UnsupportedPartition { partition: 1 })
        ));
    }

    #[test]
    fn multi_partition_configuration_is_bounded() {
        for partition_count in [0, MAX_TOPIC_PARTITIONS + 1] {
            assert!(matches!(
                LogEngine::in_memory_with_partitions(
                    "walstream/clusters/invalid-partitions",
                    partition_count
                ),
                Err(LogError::InvalidTopicMetadata { .. })
            ));
        }
        LogEngine::in_memory_with_partitions(
            "walstream/clusters/maximum-partitions",
            MAX_TOPIC_PARTITIONS,
        )
        .unwrap();
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
    async fn bounded_fetch_selects_segments_before_object_gets() {
        let store = Arc::new(ReadCountingStore::new());
        let engine = LogEngine::new(store.clone(), "walstream/clusters/bounded").unwrap();
        for value in ["one", "two", "three"] {
            engine
                .append("events", 0, vec![record(value)])
                .await
                .unwrap();
        }

        store.segment_gets.store(0, Ordering::SeqCst);
        let fetched = engine.fetch_bounded("events", 0, 0, 1, true).await.unwrap();
        assert!(fetched.oversized_first_batch);
        assert_eq!(store.segment_gets.load(Ordering::SeqCst), 1);
        let decoded = RecordBatchDecoder::decode_all(&mut fetched.records.clone()).unwrap();
        assert_eq!(
            decoded.into_iter().flat_map(|batch| batch.records).count(),
            1
        );

        store.segment_gets.store(0, Ordering::SeqCst);
        let fetched = engine
            .fetch_bounded("events", 0, 0, 1, false)
            .await
            .unwrap();
        assert!(fetched.records.is_empty());
        assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reads_reject_missing_topics_and_future_offsets() {
        let engine = LogEngine::in_memory("walstream/clusters/read-errors").unwrap();
        assert!(matches!(
            engine.offsets("missing", 0).await,
            Err(LogError::UnknownTopic { .. })
        ));
        assert!(matches!(
            engine.fetch_bounded("missing", 0, 0, 1024, true).await,
            Err(LogError::UnknownTopic { .. })
        ));

        engine.ensure_topic("events", 0).await.unwrap();
        assert!(
            engine
                .fetch_bounded("events", 0, 0, 1024, true)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        assert!(matches!(
            engine.fetch_bounded("events", 0, 1, 1024, true).await,
            Err(LogError::OffsetOutOfRange {
                offset: 1,
                latest: 0
            })
        ));
    }

    #[tokio::test]
    async fn manifest_rejects_segment_lengths_beyond_writer_bounds() {
        let engine = LogEngine::in_memory("walstream/clusters/manifest-bounds").unwrap();
        for (topic, record_count, byte_length) in [
            ("too-many-records", MAX_BATCH_RECORDS as u32 + 1, 1),
            ("too-many-bytes", 1, MAX_BATCH_BYTES as u64 + 1),
        ] {
            let manifest = Manifest {
                schema: MANIFEST_SCHEMA,
                revision: 1,
                next_offset: i64::from(record_count),
                segments: vec![Segment {
                    object: engine.segment_path(topic, 0, Uuid::new_v4()).to_string(),
                    base_offset: 0,
                    record_count,
                    byte_length,
                    sha256: "0".repeat(64),
                }],
            };
            engine
                .store
                .put_opts(
                    &engine.manifest_path(topic, 0),
                    Bytes::from(serde_json::to_vec(&manifest).unwrap()).into(),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
            assert!(matches!(
                engine.offsets(topic, 0).await,
                Err(LogError::InvalidManifest { .. })
            ));
        }
    }

    #[tokio::test]
    async fn object_and_manifest_bodies_are_bounded_before_collection() {
        let store = InMemory::new();
        let path = Path::from("oversized-object");
        store
            .put(&path, Bytes::from(vec![0; 32]).into())
            .await
            .unwrap();
        let result = store.get(&path).await.unwrap();
        assert!(matches!(
            collect_bounded(result, 16).await,
            Err(BoundedReadError::TooLarge)
        ));

        let engine = LogEngine::in_memory("walstream/clusters/manifest-body-limit").unwrap();
        engine
            .store
            .put(
                &engine.manifest_path("events", 0),
                Bytes::from(vec![b' '; MAX_MANIFEST_BYTES + 1]).into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            engine.offsets("events", 0).await,
            Err(LogError::InvalidManifest { .. })
        ));
    }

    #[test]
    fn manifest_deserializer_stops_at_the_segment_limit() {
        let mut json = String::from(r#"{"schema":1,"revision":0,"next_offset":0,"segments":["#);
        for index in 0..=MAX_MANIFEST_SEGMENTS {
            if index != 0 {
                json.push(',');
            }
            write!(
                json,
                r#"{{"object":"segment-{index}","base_offset":0,"record_count":1,"byte_length":1,"sha256":"{}"}}"#,
                "0".repeat(64)
            )
            .unwrap();
        }
        json.push_str("]}");
        let error = serde_json::from_str::<Manifest>(&json).unwrap_err();
        assert!(error.to_string().contains("metadata entry limit exceeded"));
    }

    #[tokio::test]
    async fn durable_records_are_raw_validated_before_decoding() {
        let engine = LogEngine::in_memory("walstream/clusters/durable-validation").unwrap();
        engine.append("events", 0, vec![record("a")]).await.unwrap();

        forge_committed_segment(&engine, "events", |forged| {
            forged[0..8].copy_from_slice(&i64::MAX.to_be_bytes());
            assert_eq!(forged[64], 0, "fixture offset delta changed");
            forged[64] = 2; // zig-zag encoding of +1, which overflows base offset
        })
        .await;

        assert!(matches!(
            engine.fetch("events", 0, 0).await,
            Err(LogError::CorruptSegment { .. })
        ));
    }

    #[tokio::test]
    async fn durable_reads_reject_noncanonical_semantics_and_batch_headers() {
        fn make_transactional(bytes: &mut [u8]) {
            let attributes = i16::from_be_bytes(bytes[21..23].try_into().unwrap()) | (1 << 4);
            bytes[21..23].copy_from_slice(&attributes.to_be_bytes());
        }
        fn make_last_offset_delta_inconsistent(bytes: &mut [u8]) {
            bytes[23..27].copy_from_slice(&1_i32.to_be_bytes());
        }

        for (cluster, mutate) in [
            ("transactional", make_transactional as fn(&mut [u8])),
            (
                "last-offset-delta",
                make_last_offset_delta_inconsistent as fn(&mut [u8]),
            ),
        ] {
            let engine =
                LogEngine::in_memory(format!("walstream/clusters/durable-canonical-{cluster}"))
                    .unwrap();
            engine.append("events", 0, vec![record("a")]).await.unwrap();
            forge_committed_segment(&engine, "events", mutate).await;
            assert!(matches!(
                engine.fetch("events", 0, 0).await,
                Err(LogError::CorruptSegment { .. })
            ));
        }
    }

    #[tokio::test]
    async fn append_rejects_unencodable_timestamp_span_without_panicking() {
        let engine = LogEngine::in_memory("walstream/clusters/timestamp-span").unwrap();
        let mut first = record("first");
        first.timestamp = i64::MIN;
        let mut second = record("second");
        second.timestamp = i64::MAX;
        assert!(matches!(
            engine.append("events", 0, vec![first, second]).await,
            Err(LogError::InvalidTimestampRange)
        ));
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
        engine.ensure_topic("events", 0).await.unwrap();
        let orphan = engine.segment_path("events", 0, Uuid::new_v4());
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
                &engine.manifest_path("events", 0),
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
        let manifest = engine
            .load_manifest("events", 0)
            .await
            .unwrap()
            .unwrap()
            .manifest;
        let path = Path::parse(&manifest.tail()[0].object).unwrap();
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
