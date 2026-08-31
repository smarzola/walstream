//! Durable Kafka consumer-group offsets backed by object-store CAS.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::{
    Error as StoreError, GetResult, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as DeserializeError, SeqAccess, Visitor},
};
use thiserror::Error;

use crate::log::MAX_TOPIC_PARTITIONS;

const OFFSET_SCHEMA: u32 = 1;
const MAX_CAS_ATTEMPTS: usize = 128;
const MAX_OFFSET_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_GROUP_OFFSETS: usize = 10_000;
const MAX_COMPONENT_BYTES: usize = 249;
const MAX_COMMIT_METADATA_BYTES: usize = 4 * 1024;

/// Topic-partition key for one committed consumer position.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// One offset supplied by an OffsetCommit request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OffsetCommit {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

/// Durable consumer position returned to OffsetFetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: Option<String>,
}

/// Deterministic selected/all-offset result, including an explicit absence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedOffset {
    pub topic_partition: TopicPartition,
    pub committed: Option<CommittedOffset>,
}

/// Bounded durable offset store for independent Kafka consumer groups.
#[derive(Clone)]
pub struct GroupStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl std::fmt::Debug for GroupStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl GroupStore {
    /// Create a group store beneath an already validated cluster prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, GroupError> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        Ok(Self { store, prefix })
    }

    /// Validate a Kafka group ID without performing object-store I/O.
    pub fn validate_group_id(group: &str) -> Result<(), GroupError> {
        validate_component(group, IdentifierKind::Group)
    }

    /// Validate one topic-partition without performing object-store I/O.
    pub fn validate_topic_partition(key: &TopicPartition) -> Result<(), GroupError> {
        validate_component(&key.topic, IdentifierKind::Topic)?;
        validate_partition(key.partition)
    }

    /// Atomically apply every valid entry in one OffsetCommit request.
    pub async fn commit(&self, group: &str, commits: &[OffsetCommit]) -> Result<(), GroupError> {
        validate_component(group, IdentifierKind::Group)?;
        validate_commits(commits)?;
        if commits.is_empty() {
            return Ok(());
        }

        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self
                .load(group)
                .await?
                .unwrap_or_else(LoadedOffsetManifest::empty);
            let mut next = loaded.manifest;
            let mut offsets = next.offset_map()?;
            for commit in commits {
                offsets.insert(
                    TopicPartition::new(commit.topic.clone(), commit.partition),
                    CommittedOffset {
                        offset: commit.offset,
                        metadata: commit.metadata.clone(),
                    },
                );
            }
            if offsets.len() > MAX_GROUP_OFFSETS {
                return Err(GroupError::OffsetLimit {
                    maximum: MAX_GROUP_OFFSETS,
                });
            }
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or(GroupError::RevisionOverflow)?;
            next.offsets = offsets
                .into_iter()
                .map(|(key, value)| OffsetEntry {
                    topic: key.topic,
                    partition: key.partition,
                    offset: value.offset,
                    metadata: value.metadata,
                })
                .collect();
            let bytes = Bytes::from(serde_json::to_vec(&next)?);
            if bytes.len() > MAX_OFFSET_MANIFEST_BYTES {
                return Err(GroupError::ManifestTooLarge {
                    actual: bytes.len(),
                    maximum: MAX_OFFSET_MANIFEST_BYTES,
                });
            }
            let mode = loaded.version.map_or(PutMode::Create, PutMode::Update);
            match self
                .store
                .put_opts(&self.offset_path(group), bytes.into(), mode.into())
                .await
            {
                Ok(_) => return Ok(()),
                Err(StoreError::AlreadyExists { .. } | StoreError::Precondition { .. }) => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(GroupError::ContentionExhausted {
            attempts: MAX_CAS_ATTEMPTS,
        })
    }

    /// Load every committed position for a group in deterministic key order.
    pub async fn offsets(
        &self,
        group: &str,
    ) -> Result<BTreeMap<TopicPartition, CommittedOffset>, GroupError> {
        validate_component(group, IdentifierKind::Group)?;
        self.load(group).await?.map_or_else(
            || Ok(BTreeMap::new()),
            |loaded| loaded.manifest.offset_map(),
        )
    }

    /// Fetch selected positions in request order, or every position in key order.
    pub async fn fetch(
        &self,
        group: &str,
        selection: Option<&[TopicPartition]>,
    ) -> Result<Vec<FetchedOffset>, GroupError> {
        validate_component(group, IdentifierKind::Group)?;
        if let Some(selection) = selection {
            validate_selection(selection)?;
        }
        let offsets = self.load(group).await?.map_or_else(
            || Ok(BTreeMap::new()),
            |loaded| loaded.manifest.offset_map(),
        )?;
        Ok(match selection {
            Some(selection) => selection
                .iter()
                .map(|key| FetchedOffset {
                    topic_partition: key.clone(),
                    committed: offsets.get(key).cloned(),
                })
                .collect(),
            None => offsets
                .into_iter()
                .map(|(topic_partition, committed)| FetchedOffset {
                    topic_partition,
                    committed: Some(committed),
                })
                .collect(),
        })
    }

    fn offset_path(&self, group: &str) -> Path {
        Path::from(format!("{}/groups/{group}/offsets.json", self.prefix))
    }

    async fn load(&self, group: &str) -> Result<Option<LoadedOffsetManifest>, GroupError> {
        match self.store.get(&self.offset_path(group)).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                if result.meta.size > MAX_OFFSET_MANIFEST_BYTES as u64 {
                    return Err(GroupError::InvalidManifest {
                        detail: format!(
                            "offset manifest is {} bytes; maximum is {MAX_OFFSET_MANIFEST_BYTES}",
                            result.meta.size
                        ),
                    });
                }
                let bytes = collect_bounded(result, MAX_OFFSET_MANIFEST_BYTES)
                    .await
                    .map_err(|error| match error {
                        BoundedReadError::Store(source) => GroupError::ObjectStore(source),
                        BoundedReadError::TooLarge => GroupError::InvalidManifest {
                            detail: format!(
                                "offset manifest body exceeds {MAX_OFFSET_MANIFEST_BYTES} bytes"
                            ),
                        },
                    })?;
                let manifest: OffsetManifest =
                    serde_json::from_slice(&bytes).map_err(|source| {
                        GroupError::InvalidManifest {
                            detail: source.to_string(),
                        }
                    })?;
                manifest.validate()?;
                Ok(Some(LoadedOffsetManifest {
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OffsetManifest {
    schema: u32,
    revision: u64,
    #[serde(deserialize_with = "deserialize_offsets")]
    offsets: Vec<OffsetEntry>,
}

impl OffsetManifest {
    fn validate(&self) -> Result<(), GroupError> {
        if self.schema != OFFSET_SCHEMA {
            return Err(GroupError::InvalidManifest {
                detail: format!("unsupported schema {}", self.schema),
            });
        }
        let _ = self.offset_map()?;
        Ok(())
    }

    fn offset_map(&self) -> Result<BTreeMap<TopicPartition, CommittedOffset>, GroupError> {
        let mut offsets = BTreeMap::new();
        for entry in &self.offsets {
            validate_component(&entry.topic, IdentifierKind::Topic)
                .and_then(|()| validate_partition(entry.partition))
                .and_then(|()| validate_offset(entry.offset))
                .and_then(|()| validate_metadata(entry.metadata.as_deref()))
                .map_err(|error| GroupError::InvalidManifest {
                    detail: error.to_string(),
                })?;
            let key = TopicPartition::new(entry.topic.clone(), entry.partition);
            if offsets
                .insert(
                    key,
                    CommittedOffset {
                        offset: entry.offset,
                        metadata: entry.metadata.clone(),
                    },
                )
                .is_some()
            {
                return Err(GroupError::InvalidManifest {
                    detail: "offset manifest contains duplicate topic-partitions".into(),
                });
            }
        }
        Ok(offsets)
    }
}

impl Default for OffsetManifest {
    fn default() -> Self {
        Self {
            schema: OFFSET_SCHEMA,
            revision: 0,
            offsets: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OffsetEntry {
    topic: String,
    partition: i32,
    offset: i64,
    metadata: Option<String>,
}

fn deserialize_offsets<'de, D>(deserializer: D) -> Result<Vec<OffsetEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedOffsets;

    impl<'de> Visitor<'de> for BoundedOffsets {
        type Value = Vec<OffsetEntry>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "at most {MAX_GROUP_OFFSETS} committed offsets")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|size| size > MAX_GROUP_OFFSETS)
            {
                return Err(A::Error::custom("committed offset limit exceeded"));
            }
            let mut offsets = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_GROUP_OFFSETS),
            );
            while let Some(offset) = sequence.next_element()? {
                if offsets.len() == MAX_GROUP_OFFSETS {
                    return Err(A::Error::custom("committed offset limit exceeded"));
                }
                offsets.push(offset);
            }
            Ok(offsets)
        }
    }

    deserializer.deserialize_seq(BoundedOffsets)
}

struct LoadedOffsetManifest {
    manifest: OffsetManifest,
    version: Option<UpdateVersion>,
}

impl LoadedOffsetManifest {
    fn empty() -> Self {
        Self {
            manifest: OffsetManifest::default(),
            version: None,
        }
    }
}

fn validate_commits(commits: &[OffsetCommit]) -> Result<(), GroupError> {
    if commits.len() > MAX_GROUP_OFFSETS {
        return Err(GroupError::OffsetLimit {
            maximum: MAX_GROUP_OFFSETS,
        });
    }
    let mut keys = BTreeSet::new();
    for commit in commits {
        validate_component(&commit.topic, IdentifierKind::Topic)?;
        validate_partition(commit.partition)?;
        validate_offset(commit.offset)?;
        validate_metadata(commit.metadata.as_deref())?;
        if !keys.insert((&commit.topic, commit.partition)) {
            return Err(GroupError::DuplicateCommit {
                topic: commit.topic.clone(),
                partition: commit.partition,
            });
        }
    }
    Ok(())
}

fn validate_selection(selection: &[TopicPartition]) -> Result<(), GroupError> {
    if selection.len() > MAX_GROUP_OFFSETS {
        return Err(GroupError::OffsetLimit {
            maximum: MAX_GROUP_OFFSETS,
        });
    }
    let mut keys = BTreeSet::new();
    for key in selection {
        validate_component(&key.topic, IdentifierKind::Topic)?;
        validate_partition(key.partition)?;
        if !keys.insert(key) {
            return Err(GroupError::DuplicateFetch {
                topic: key.topic.clone(),
                partition: key.partition,
            });
        }
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), GroupError> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(GroupError::InvalidPrefix);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum IdentifierKind {
    Group,
    Topic,
}

fn validate_component(value: &str, kind: IdentifierKind) -> Result<(), GroupError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_COMPONENT_BYTES
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        return Ok(());
    }
    match kind {
        IdentifierKind::Group => Err(GroupError::InvalidGroupId {
            group: value.to_owned(),
        }),
        IdentifierKind::Topic => Err(GroupError::InvalidTopic {
            topic: value.to_owned(),
        }),
    }
}

fn validate_partition(partition: i32) -> Result<(), GroupError> {
    (0..MAX_TOPIC_PARTITIONS)
        .contains(&partition)
        .then_some(())
        .ok_or(GroupError::UnsupportedPartition { partition })
}

fn validate_offset(offset: i64) -> Result<(), GroupError> {
    (offset >= 0)
        .then_some(())
        .ok_or(GroupError::InvalidOffset { offset })
}

fn validate_metadata(metadata: Option<&str>) -> Result<(), GroupError> {
    if metadata.is_some_and(|value| value.len() > MAX_COMMIT_METADATA_BYTES) {
        return Err(GroupError::MetadataTooLarge {
            maximum: MAX_COMMIT_METADATA_BYTES,
        });
    }
    Ok(())
}

/// Durable group-offset validation or persistence failure.
#[derive(Debug, Error)]
pub enum GroupError {
    #[error("group store prefix is not a safe relative object path")]
    InvalidPrefix,
    #[error("invalid group id {group:?}")]
    InvalidGroupId { group: String },
    #[error("invalid topic name {topic:?}")]
    InvalidTopic { topic: String },
    #[error("partition {partition} is outside Walstream's supported range")]
    UnsupportedPartition { partition: i32 },
    #[error("committed offset {offset} must not be negative")]
    InvalidOffset { offset: i64 },
    #[error("committed offset metadata exceeds {maximum} bytes")]
    MetadataTooLarge { maximum: usize },
    #[error("duplicate offset commit for {topic:?} partition {partition}")]
    DuplicateCommit { topic: String, partition: i32 },
    #[error("duplicate offset fetch for {topic:?} partition {partition}")]
    DuplicateFetch { topic: String, partition: i32 },
    #[error("group offset manifest reached its limit of {maximum} entries")]
    OffsetLimit { maximum: usize },
    #[error("group offset manifest is {actual} bytes; maximum is {maximum}")]
    ManifestTooLarge { actual: usize, maximum: usize },
    #[error("group offset revision overflow")]
    RevisionOverflow,
    #[error("invalid group offset manifest: {detail}")]
    InvalidManifest { detail: String },
    #[error("group offset serialization failed")]
    Serialization(#[from] serde_json::Error),
    #[error("group offset object-store operation failed")]
    ObjectStore(#[from] StoreError),
    #[error("group offset contention did not converge after {attempts} attempts")]
    ContentionExhausted { attempts: usize },
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as StoreResult, memory::InMemory,
    };
    use tokio::sync::Barrier;

    use super::*;

    fn commit(topic: &str, offset: i64) -> OffsetCommit {
        OffsetCommit {
            topic: topic.into(),
            partition: 0,
            offset,
            metadata: Some(format!("at-{offset}")),
        }
    }

    #[derive(Debug)]
    struct ContentionStore {
        inner: InMemory,
        first_writes: Barrier,
        offset_writes: AtomicUsize,
        conflicts: AtomicUsize,
    }

    impl ContentionStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                first_writes: Barrier::new(2),
                offset_writes: AtomicUsize::new(0),
                conflicts: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for ContentionStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("group-contention-test-store")
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
            if location.to_string().ends_with("/offsets.json")
                && self.offset_writes.fetch_add(1, Ordering::SeqCst) < 2
            {
                self.first_writes.wait().await;
            }
            let result = self.inner.put_opts(location, payload, options).await;
            if matches!(
                result,
                Err(StoreError::AlreadyExists { .. } | StoreError::Precondition { .. })
            ) {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
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
    async fn commits_overwrites_and_recovers_offsets() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = GroupStore::new(store.clone(), "walstream/clusters/groups").unwrap();
        assert!(first.offsets("workers").await.unwrap().is_empty());

        first
            .commit("workers", &[commit("events", 4), commit("audit", 2)])
            .await
            .unwrap();
        first
            .commit("workers", &[commit("events", 7)])
            .await
            .unwrap();

        let recovered = GroupStore::new(store, "walstream/clusters/groups").unwrap();
        assert_eq!(
            recovered.offsets("workers").await.unwrap(),
            BTreeMap::from([
                (
                    TopicPartition::new("audit", 0),
                    CommittedOffset {
                        offset: 2,
                        metadata: Some("at-2".into()),
                    },
                ),
                (
                    TopicPartition::new("events", 0),
                    CommittedOffset {
                        offset: 7,
                        metadata: Some("at-7".into()),
                    },
                ),
            ])
        );
    }

    #[tokio::test]
    async fn multi_partition_offsets_are_durable_and_independent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = GroupStore::new(store.clone(), "walstream/clusters/partition-offsets").unwrap();
        first
            .commit(
                "workers",
                &[
                    OffsetCommit {
                        partition: 0,
                        ..commit("events", 4)
                    },
                    OffsetCommit {
                        partition: 2,
                        ..commit("events", 9)
                    },
                ],
            )
            .await
            .unwrap();

        let recovered = GroupStore::new(store, "walstream/clusters/partition-offsets").unwrap();
        let offsets = recovered.offsets("workers").await.unwrap();
        assert_eq!(offsets[&TopicPartition::new("events", 0)].offset, 4);
        assert_eq!(offsets[&TopicPartition::new("events", 2)].offset, 9);
    }

    #[tokio::test]
    async fn concurrent_commits_preserve_non_conflicting_offsets() {
        let store = Arc::new(ContentionStore::new());
        let left = GroupStore::new(store.clone(), "walstream/clusters/concurrent").unwrap();
        let right = left.clone();
        let left_commit = [commit("left", 1)];
        let right_commit = [commit("right", 2)];
        let (left_result, right_result) = tokio::join!(
            left.commit("workers", &left_commit),
            right.commit("workers", &right_commit),
        );
        left_result.unwrap();
        right_result.unwrap();

        let offsets = left.offsets("workers").await.unwrap();
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[&TopicPartition::new("left", 0)].offset, 1);
        assert_eq!(offsets[&TopicPartition::new("right", 0)].offset, 2);
        assert_eq!(store.conflicts.load(Ordering::SeqCst), 1);
        assert_eq!(store.offset_writes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn fetches_selected_absent_and_all_offsets_deterministically() {
        let groups =
            GroupStore::new(Arc::new(InMemory::new()), "walstream/clusters/selection").unwrap();
        groups
            .commit("workers", &[commit("zeta", 9), commit("alpha", 1)])
            .await
            .unwrap();

        let selected = [
            TopicPartition::new("zeta", 0),
            TopicPartition::new("absent", 0),
        ];
        assert_eq!(
            groups.fetch("workers", Some(&selected)).await.unwrap(),
            vec![
                FetchedOffset {
                    topic_partition: selected[0].clone(),
                    committed: Some(CommittedOffset {
                        offset: 9,
                        metadata: Some("at-9".into()),
                    }),
                },
                FetchedOffset {
                    topic_partition: selected[1].clone(),
                    committed: None,
                },
            ]
        );
        assert_eq!(
            groups
                .fetch("workers", None)
                .await
                .unwrap()
                .into_iter()
                .map(|entry| entry.topic_partition.topic)
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );

        for invalid in [
            vec![TopicPartition::new("bad/topic", 0)],
            vec![TopicPartition::new("events", -1)],
            vec![TopicPartition::new("events", MAX_TOPIC_PARTITIONS)],
            vec![
                TopicPartition::new("events", 0),
                TopicPartition::new("events", 0),
            ],
        ] {
            assert!(groups.fetch("workers", Some(&invalid)).await.is_err());
        }
    }

    #[tokio::test]
    async fn rejects_invalid_requests_without_persisting_partial_state() {
        let groups =
            GroupStore::new(Arc::new(InMemory::new()), "walstream/clusters/validation").unwrap();

        for commits in [
            vec![commit("events", -1)],
            vec![OffsetCommit {
                partition: MAX_TOPIC_PARTITIONS,
                ..commit("events", 1)
            }],
            vec![commit("events/escape", 1)],
            vec![commit("events", 1), commit("events", 2)],
            vec![OffsetCommit {
                metadata: Some("x".repeat(MAX_COMMIT_METADATA_BYTES + 1)),
                ..commit("events", 1)
            }],
        ] {
            assert!(groups.commit("workers", &commits).await.is_err());
        }
        assert!(
            groups
                .commit("../workers", &[commit("events", 1)])
                .await
                .is_err()
        );
        let too_many = (0..=MAX_GROUP_OFFSETS)
            .map(|index| commit(&format!("topic-{index}"), 1))
            .collect::<Vec<_>>();
        assert!(matches!(
            groups.commit("workers", &too_many).await,
            Err(GroupError::OffsetLimit { .. })
        ));
        assert!(groups.offsets("workers").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn corrupt_future_duplicate_and_oversized_manifests_fail_closed() {
        let store = Arc::new(InMemory::new());
        let groups = GroupStore::new(store.clone(), "walstream/clusters/corrupt").unwrap();
        let path = groups.offset_path("workers");

        for bytes in [
            Bytes::from_static(br#"{"schema":2,"revision":0,"offsets":[]}"#),
            Bytes::from_static(br#"{"schema":1"#),
            Bytes::from_static(br#"{"schema":1,"revision":1,"offsets":[{"topic":"events","partition":0,"offset":1,"metadata":null},{"topic":"events","partition":0,"offset":2,"metadata":null}]}"#),
            Bytes::from_static(br#"{"schema":1,"revision":0,"offsets":[{"topic":"../events","partition":0,"offset":1,"metadata":null}]}"#),
            Bytes::from(format!(
                "{{\"schema\":1,\"revision\":0,\"offsets\":[{{\"topic\":\"events\",\"partition\":{MAX_TOPIC_PARTITIONS},\"offset\":1,\"metadata\":null}}]}}"
            )),
            Bytes::from_static(br#"{"schema":1,"revision":0,"offsets":[{"topic":"events","partition":0,"offset":-1,"metadata":null}]}"#),
            Bytes::from(format!(
                "{{\"schema\":1,\"revision\":0,\"offsets\":[{{\"topic\":\"events\",\"partition\":0,\"offset\":1,\"metadata\":\"{}\"}}]}}",
                "x".repeat(MAX_COMMIT_METADATA_BYTES + 1)
            )),
        ] {
            store
                .put(&path, bytes.clone().into())
                .await
                .unwrap();
            assert!(matches!(
                groups.offsets("workers").await,
                Err(GroupError::InvalidManifest { .. })
            ));
            assert!(matches!(
                groups.commit("workers", &[commit("safe", 1)]).await,
                Err(GroupError::InvalidManifest { .. })
            ));
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                bytes
            );
        }

        store
            .put(
                &path,
                Bytes::from(vec![b' '; MAX_OFFSET_MANIFEST_BYTES + 1]).into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            groups.offsets("workers").await,
            Err(GroupError::InvalidManifest { .. })
        ));
    }
}
