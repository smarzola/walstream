//! Process-local classic group membership over durable object-store offsets.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::group::{FetchedOffset, GroupError, GroupStore, OffsetCommit, TopicPartition};

const CONSUMER_PROTOCOL_TYPE: &str = "consumer";
const MAX_PROTOCOLS: usize = 32;
const MAX_GROUP_SLOTS: usize = 10_000;
const MAX_IDENTIFIER_BYTES: usize = 249;
const MAX_GROUP_BLOB_BYTES: usize = 1024 * 1024;
const MAX_SESSION_TIMEOUT_MS: i32 = 300_000;

/// One assignor offered in JoinGroup, with opaque client-owned metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinProtocol {
    pub name: String,
    pub metadata: Bytes,
}

/// Successful JoinGroup state returned to the only active member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinOutcome {
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader_id: String,
    pub member_id: String,
    pub member_metadata: Bytes,
}

/// One opaque leader assignment supplied through SyncGroup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncAssignment {
    pub member_id: String,
    pub assignment: Bytes,
}

/// Ephemeral single-active-member coordinator with durable offset commits.
#[derive(Clone, Debug)]
pub struct GroupCoordinator {
    offsets: GroupStore,
    groups: Arc<Mutex<HashMap<String, GroupSlot>>>,
}

type GroupSlot = Arc<Mutex<Option<MemberState>>>;

#[derive(Clone, Debug)]
struct MemberState {
    generation_id: i32,
    member_id: String,
    protocol_type: String,
    protocol_name: String,
    metadata: Bytes,
    assignment: Option<Bytes>,
    session_timeout: Duration,
    deadline: Instant,
}

impl GroupCoordinator {
    pub fn new(offsets: GroupStore) -> Self {
        Self {
            offsets,
            groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn validate_group_id(&self, group: &str) -> Result<(), CoordinatorError> {
        GroupStore::validate_group_id(group).map_err(CoordinatorError::Storage)
    }

    pub fn validate_topic_partition(&self, key: &TopicPartition) -> Result<(), CoordinatorError> {
        GroupStore::validate_topic_partition(key).map_err(CoordinatorError::Storage)
    }

    async fn existing_group_slot(&self, group: &str) -> Option<GroupSlot> {
        self.groups.lock().await.get(group).cloned()
    }

    async fn join_group_slot(
        &self,
        group: &str,
        member_id: &str,
    ) -> Result<GroupSlot, CoordinatorError> {
        let mut groups = self.groups.lock().await;
        if let Some(slot) = groups.get(group) {
            return Ok(slot.clone());
        }
        if !member_id.is_empty() {
            return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId));
        }
        sweep_reclaimable_slots(&mut groups, Instant::now());
        if groups.len() >= MAX_GROUP_SLOTS {
            return Err(CoordinatorError::kafka(ResponseError::GroupMaxSizeReached));
        }
        let slot = Arc::new(Mutex::new(None));
        groups.insert(group.to_owned(), slot.clone());
        Ok(slot)
    }

    async fn reclaim_slot(&self, group: &str, slot: &GroupSlot) {
        let mut groups = self.groups.lock().await;
        let is_same_idle_slot = groups.get(group).is_some_and(|current| {
            Arc::ptr_eq(current, slot)
                && Arc::strong_count(current) == 2
                && current.try_lock().is_ok_and(|state| state.is_none())
        });
        if is_same_idle_slot {
            groups.remove(group);
        }
    }

    pub async fn join(
        &self,
        group: &str,
        session_timeout_ms: i32,
        member_id: &str,
        protocol_type: &str,
        protocols: &[JoinProtocol],
    ) -> Result<JoinOutcome, CoordinatorError> {
        self.validate_group_id(group)?;
        validate_session_timeout(session_timeout_ms)?;
        validate_member_id(member_id, true)?;
        validate_protocols(protocol_type, protocols)?;

        let slot = self.join_group_slot(group, member_id).await?;
        let mut state = slot.lock().await;
        let now = Instant::now();
        expire_state(&mut state, now);

        let selected = protocols[0].clone();
        let session_timeout = Duration::from_millis(session_timeout_ms as u64);
        let result = match state.as_mut() {
            None if member_id.is_empty() => {
                let assigned = Uuid::new_v4().to_string();
                *state = Some(MemberState {
                    generation_id: 1,
                    member_id: assigned,
                    protocol_type: protocol_type.to_owned(),
                    protocol_name: selected.name,
                    metadata: selected.metadata,
                    assignment: None,
                    session_timeout,
                    deadline: now + session_timeout,
                });
                Ok(join_outcome(state.as_ref().expect("inserted group state")))
            }
            None => Err(CoordinatorError::kafka(ResponseError::UnknownMemberId)),
            Some(_) if member_id.is_empty() => {
                Err(CoordinatorError::kafka(ResponseError::GroupMaxSizeReached))
            }
            Some(state) if state.member_id != member_id => {
                Err(CoordinatorError::kafka(ResponseError::UnknownMemberId))
            }
            Some(state) if state.protocol_type != protocol_type => Err(CoordinatorError::kafka(
                ResponseError::InconsistentGroupProtocol,
            )),
            Some(state) => {
                let Some(generation_id) = state.generation_id.checked_add(1) else {
                    return Err(CoordinatorError::kafka(ResponseError::UnknownServerError));
                };
                state.generation_id = generation_id;
                state.protocol_name = selected.name;
                state.metadata = selected.metadata;
                state.assignment = None;
                state.session_timeout = session_timeout;
                state.deadline = now + session_timeout;
                Ok(join_outcome(state))
            }
        };
        let reclaim = state.is_none();
        drop(state);
        if reclaim {
            self.reclaim_slot(group, &slot).await;
        }
        result
    }

    pub async fn sync(
        &self,
        group: &str,
        generation_id: i32,
        member_id: &str,
        assignments: &[SyncAssignment],
    ) -> Result<Bytes, CoordinatorError> {
        self.validate_group_id(group)?;
        validate_member_id(member_id, false)?;
        validate_assignments(assignments)?;
        let slot = self
            .existing_group_slot(group)
            .await
            .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
        let mut state = slot.lock().await;
        let now = Instant::now();
        let result = active_member(&mut state, generation_id, member_id, now).and_then(|member| {
            let assignment = assignments
                .iter()
                .find(|assignment| assignment.member_id == member_id)
                .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?
                .assignment
                .clone();
            member.assignment = Some(assignment.clone());
            member.deadline = now + member.session_timeout;
            Ok(assignment)
        });
        let reclaim = state.is_none();
        drop(state);
        if reclaim {
            self.reclaim_slot(group, &slot).await;
        }
        result
    }

    pub async fn heartbeat(
        &self,
        group: &str,
        generation_id: i32,
        member_id: &str,
    ) -> Result<(), CoordinatorError> {
        self.validate_group_id(group)?;
        validate_member_id(member_id, false)?;
        let slot = self
            .existing_group_slot(group)
            .await
            .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
        let mut state = slot.lock().await;
        let now = Instant::now();
        let result = active_member(&mut state, generation_id, member_id, now).and_then(|member| {
            if member.assignment.is_none() {
                return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress));
            }
            member.deadline = now + member.session_timeout;
            Ok(())
        });
        let reclaim = state.is_none();
        drop(state);
        if reclaim {
            self.reclaim_slot(group, &slot).await;
        }
        result
    }

    pub async fn leave(&self, group: &str, member_id: &str) -> Result<(), CoordinatorError> {
        self.validate_group_id(group)?;
        validate_member_id(member_id, false)?;
        let slot = self
            .existing_group_slot(group)
            .await
            .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
        let mut state = slot.lock().await;
        let now = Instant::now();
        expire_state(&mut state, now);
        let result = match state.as_ref() {
            Some(member) if member.member_id == member_id => {
                *state = None;
                Ok(())
            }
            _ => Err(CoordinatorError::kafka(ResponseError::UnknownMemberId)),
        };
        let reclaim = state.is_none();
        drop(state);
        if reclaim {
            self.reclaim_slot(group, &slot).await;
        }
        result
    }

    pub async fn commit(
        &self,
        group: &str,
        generation_id: i32,
        member_id: &str,
        commits: &[OffsetCommit],
    ) -> Result<(), CoordinatorError> {
        self.validate_group_id(group)?;
        validate_member_id(member_id, false)?;
        let slot = self
            .existing_group_slot(group)
            .await
            .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
        let mut state = slot.lock().await;
        let now = Instant::now();
        match active_member(&mut state, generation_id, member_id, now) {
            Ok(member) if member.assignment.is_some() => {}
            Ok(_) => return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress)),
            Err(error) => {
                let reclaim = state.is_none();
                drop(state);
                if reclaim {
                    self.reclaim_slot(group, &slot).await;
                }
                return Err(error);
            }
        }
        if commits.is_empty() {
            return Ok(());
        }
        self.offsets
            .commit(group, commits)
            .await
            .map_err(CoordinatorError::Storage)
    }

    pub async fn fetch(
        &self,
        group: &str,
        selection: Option<&[TopicPartition]>,
    ) -> Result<Vec<FetchedOffset>, CoordinatorError> {
        self.offsets
            .fetch(group, selection)
            .await
            .map_err(CoordinatorError::Storage)
    }
}

fn join_outcome(state: &MemberState) -> JoinOutcome {
    JoinOutcome {
        generation_id: state.generation_id,
        protocol_name: state.protocol_name.clone(),
        leader_id: state.member_id.clone(),
        member_id: state.member_id.clone(),
        member_metadata: state.metadata.clone(),
    }
}

fn sweep_reclaimable_slots(groups: &mut HashMap<String, GroupSlot>, now: Instant) {
    groups.retain(|_, slot| {
        if Arc::strong_count(slot) != 1 {
            return true;
        }
        let Ok(mut state) = slot.try_lock() else {
            return true;
        };
        expire_state(&mut state, now);
        state.is_some()
    });
}

fn active_member<'a>(
    state: &'a mut Option<MemberState>,
    generation_id: i32,
    member_id: &str,
    now: Instant,
) -> Result<&'a mut MemberState, CoordinatorError> {
    expire_state(state, now);
    let state = state
        .as_mut()
        .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
    if state.member_id != member_id {
        return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId));
    }
    if state.generation_id != generation_id {
        return Err(CoordinatorError::kafka(ResponseError::IllegalGeneration));
    }
    Ok(state)
}

fn expire_state(state: &mut Option<MemberState>, now: Instant) {
    if state.as_ref().is_some_and(|state| state.deadline <= now) {
        *state = None;
    }
}

fn validate_session_timeout(session_timeout_ms: i32) -> Result<(), CoordinatorError> {
    if !(1..=MAX_SESSION_TIMEOUT_MS).contains(&session_timeout_ms) {
        return Err(CoordinatorError::kafka(
            ResponseError::InvalidSessionTimeout,
        ));
    }
    Ok(())
}

fn validate_member_id(member_id: &str, allow_empty: bool) -> Result<(), CoordinatorError> {
    if allow_empty && member_id.is_empty() {
        return Ok(());
    }
    validate_token(member_id).then_some(()).ok_or_else(|| {
        CoordinatorError::kafka(if member_id.is_empty() {
            ResponseError::UnknownMemberId
        } else {
            ResponseError::InvalidRequest
        })
    })
}

fn validate_protocols(
    protocol_type: &str,
    protocols: &[JoinProtocol],
) -> Result<(), CoordinatorError> {
    if protocol_type != CONSUMER_PROTOCOL_TYPE
        || protocols.is_empty()
        || protocols.len() > MAX_PROTOCOLS
    {
        return Err(CoordinatorError::kafka(
            ResponseError::InconsistentGroupProtocol,
        ));
    }
    let mut names = BTreeSet::new();
    for protocol in protocols {
        if !validate_token(&protocol.name)
            || protocol.metadata.len() > MAX_GROUP_BLOB_BYTES
            || !names.insert(&protocol.name)
        {
            return Err(CoordinatorError::kafka(
                ResponseError::InconsistentGroupProtocol,
            ));
        }
    }
    Ok(())
}

fn validate_assignments(assignments: &[SyncAssignment]) -> Result<(), CoordinatorError> {
    if assignments.len() != 1 {
        return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
    }
    let assignment = &assignments[0];
    validate_member_id(&assignment.member_id, false)?;
    if assignment.assignment.len() > MAX_GROUP_BLOB_BYTES {
        return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
    }
    Ok(())
}

fn validate_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Coordinator semantic or durable-state failure.
#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("Kafka coordinator error: {0:?}")]
    Kafka(ResponseError),
    #[error(transparent)]
    Storage(#[from] GroupError),
}

impl CoordinatorError {
    fn kafka(error: ResponseError) -> Self {
        Self::Kafka(error)
    }

    pub fn response_error(&self) -> ResponseError {
        match self {
            Self::Kafka(error) => *error,
            Self::Storage(GroupError::InvalidGroupId { .. }) => ResponseError::InvalidGroupId,
            Self::Storage(
                GroupError::InvalidTopic { .. } | GroupError::UnsupportedPartition { .. },
            ) => ResponseError::UnknownTopicOrPartition,
            Self::Storage(GroupError::InvalidOffset { .. }) => {
                ResponseError::InvalidCommitOffsetSize
            }
            Self::Storage(GroupError::MetadataTooLarge { .. }) => {
                ResponseError::OffsetMetadataTooLarge
            }
            Self::Storage(
                GroupError::DuplicateCommit { .. }
                | GroupError::DuplicateFetch { .. }
                | GroupError::OffsetLimit { .. }
                | GroupError::ManifestTooLarge { .. },
            ) => ResponseError::InvalidRequest,
            Self::Storage(
                GroupError::InvalidPrefix
                | GroupError::RevisionOverflow
                | GroupError::InvalidManifest { .. }
                | GroupError::Serialization(_)
                | GroupError::ObjectStore(_)
                | GroupError::ContentionExhausted { .. },
            ) => ResponseError::KafkaStorageError,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
        memory::InMemory, path::Path,
    };
    use tokio::sync::Notify;

    use super::*;

    fn coordinator(store: Arc<dyn ObjectStore>) -> GroupCoordinator {
        GroupCoordinator::new(GroupStore::new(store, "walstream/clusters/coordinator").unwrap())
    }

    fn protocols() -> Vec<JoinProtocol> {
        vec![JoinProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(b"subscription"),
        }]
    }

    async fn stable_member_in(coordinator: &GroupCoordinator, group: &str) -> JoinOutcome {
        let joined = coordinator
            .join(group, 30_000, "", "consumer", &protocols())
            .await
            .unwrap();
        coordinator
            .sync(
                group,
                joined.generation_id,
                &joined.member_id,
                &[SyncAssignment {
                    member_id: joined.member_id.clone(),
                    assignment: Bytes::from_static(b"partition-0"),
                }],
            )
            .await
            .unwrap();
        joined
    }

    async fn stable_member(coordinator: &GroupCoordinator) -> JoinOutcome {
        stable_member_in(coordinator, "workers").await
    }

    #[derive(Debug)]
    struct BlockingCommitStore {
        inner: InMemory,
        block_workers_once: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl BlockingCommitStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                block_workers_once: AtomicBool::new(true),
                entered: Notify::new(),
                release: Notify::new(),
            }
        }
    }

    impl fmt::Display for BlockingCommitStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("blocking-group-commit-test-store")
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingCommitStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            if location
                .to_string()
                .ends_with("/groups/workers/offsets.json")
                && self.block_workers_once.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
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
    async fn joins_syncs_heartbeats_and_leaves_one_member() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let joined = stable_member(&groups).await;
        assert_eq!(joined.generation_id, 1);
        assert_eq!(joined.protocol_name, "range");
        groups
            .heartbeat("workers", joined.generation_id, &joined.member_id)
            .await
            .unwrap();

        assert_eq!(
            groups
                .join("workers", 30_000, "", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::GroupMaxSizeReached
        );
        assert_eq!(
            groups
                .heartbeat("workers", joined.generation_id + 1, &joined.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::IllegalGeneration
        );
        groups.leave("workers", &joined.member_id).await.unwrap();
        assert!(groups.groups.lock().await.is_empty());
        assert_eq!(
            groups
                .heartbeat("workers", joined.generation_id, &joined.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
        assert!(groups.groups.lock().await.is_empty());
    }

    #[tokio::test]
    async fn expires_members_and_requires_rejoin() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let joined = groups
            .join("workers", 1, "", "consumer", &protocols())
            .await
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(
            groups
                .sync(
                    "workers",
                    joined.generation_id,
                    &joined.member_id,
                    &[SyncAssignment {
                        member_id: joined.member_id.clone(),
                        assignment: Bytes::new(),
                    }],
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
        assert!(groups.groups.lock().await.is_empty());
        assert!(
            groups
                .join("workers", 30_000, "", "consumer", &protocols())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_invalid_protocols_and_heartbeat_before_sync() {
        let groups = coordinator(Arc::new(InMemory::new()));
        assert_eq!(
            groups
                .join("workers", 30_000, "", "connect", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InconsistentGroupProtocol
        );
        assert_eq!(
            groups
                .join("workers", 30_000, "", "consumer", &[])
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InconsistentGroupProtocol
        );

        let joined = groups
            .join("workers", 30_000, "", "consumer", &protocols())
            .await
            .unwrap();
        assert_eq!(
            groups
                .heartbeat("workers", joined.generation_id, &joined.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::RebalanceInProgress
        );
    }

    #[tokio::test]
    async fn bounds_and_reclaims_group_slots() {
        let groups = coordinator(Arc::new(InMemory::new()));
        assert_eq!(
            groups
                .join("unknown", 30_000, "member", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
        assert!(groups.groups.lock().await.is_empty());

        let mut first_member = None;
        for index in 0..MAX_GROUP_SLOTS {
            let joined = groups
                .join(
                    &format!("group-{index}"),
                    30_000,
                    "",
                    "consumer",
                    &protocols(),
                )
                .await
                .unwrap();
            if index == 0 {
                first_member = Some(joined.member_id);
            }
        }
        assert_eq!(groups.groups.lock().await.len(), MAX_GROUP_SLOTS);
        assert_eq!(
            groups
                .join("one-too-many", 30_000, "", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::GroupMaxSizeReached
        );
        assert_eq!(groups.groups.lock().await.len(), MAX_GROUP_SLOTS);

        groups
            .leave("group-0", first_member.as_deref().unwrap())
            .await
            .unwrap();
        assert_eq!(groups.groups.lock().await.len(), MAX_GROUP_SLOTS - 1);
        groups
            .join("replacement", 30_000, "", "consumer", &protocols())
            .await
            .unwrap();
        assert_eq!(groups.groups.lock().await.len(), MAX_GROUP_SLOTS);
    }

    #[tokio::test]
    async fn commits_offsets_only_for_a_stable_member_and_recovers_them() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = coordinator(store.clone());
        let joined = stable_member(&first).await;
        first
            .commit(
                "workers",
                joined.generation_id,
                &joined.member_id,
                &[OffsetCommit {
                    topic: "events".into(),
                    partition: 0,
                    offset: 4,
                    metadata: Some("done".into()),
                }],
            )
            .await
            .unwrap();

        let recovered = coordinator(store);
        let fetched = recovered
            .fetch(
                "workers",
                Some(&[
                    TopicPartition::new("events", 0),
                    TopicPartition::new("absent", 0),
                ]),
            )
            .await
            .unwrap();
        assert_eq!(fetched[0].committed.as_ref().unwrap().offset, 4);
        assert!(fetched[1].committed.is_none());
        assert_eq!(
            recovered
                .commit("workers", joined.generation_id, &joined.member_id, &[])
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
    }

    #[tokio::test]
    async fn serializes_membership_and_commits_per_group_without_cross_group_blocking() {
        let store = Arc::new(BlockingCommitStore::new());
        let groups = coordinator(store.clone());
        let old = stable_member(&groups).await;

        let old_commit = tokio::spawn({
            let groups = groups.clone();
            let member_id = old.member_id.clone();
            async move {
                groups
                    .commit(
                        "workers",
                        old.generation_id,
                        &member_id,
                        &[OffsetCommit {
                            topic: "events".into(),
                            partition: 0,
                            offset: 4,
                            metadata: None,
                        }],
                    )
                    .await
            }
        });
        store.entered.notified().await;

        let replacement = tokio::spawn({
            let groups = groups.clone();
            let member_id = old.member_id.clone();
            async move {
                groups.leave("workers", &member_id).await.unwrap();
                let joined = stable_member(&groups).await;
                groups
                    .commit(
                        "workers",
                        joined.generation_id,
                        &joined.member_id,
                        &[OffsetCommit {
                            topic: "events".into(),
                            partition: 0,
                            offset: 9,
                            metadata: None,
                        }],
                    )
                    .await
                    .unwrap();
            }
        });

        let independent = stable_member_in(&groups, "independent").await;
        groups
            .commit(
                "independent",
                independent.generation_id,
                &independent.member_id,
                &[OffsetCommit {
                    topic: "events".into(),
                    partition: 0,
                    offset: 7,
                    metadata: None,
                }],
            )
            .await
            .unwrap();
        assert!(!old_commit.is_finished());
        assert!(!replacement.is_finished());

        store.release.notify_one();
        old_commit.await.unwrap().unwrap();
        replacement.await.unwrap();

        let workers = groups
            .fetch("workers", Some(&[TopicPartition::new("events", 0)]))
            .await
            .unwrap();
        assert_eq!(workers[0].committed.as_ref().unwrap().offset, 9);
        let independent = groups
            .fetch("independent", Some(&[TopicPartition::new("events", 0)]))
            .await
            .unwrap();
        assert_eq!(independent[0].committed.as_ref().unwrap().offset, 7);
    }
}
