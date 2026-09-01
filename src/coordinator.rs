//! Process-local classic group membership over durable object-store offsets.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use thiserror::Error;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::group::{FetchedOffset, GroupError, GroupStore, OffsetCommit, TopicPartition};

const CONSUMER_PROTOCOL_TYPE: &str = "consumer";
const MAX_PROTOCOLS: usize = 32;
const MAX_GROUP_SLOTS: usize = 10_000;
const MAX_GROUP_MEMBERS: usize = 1024;
const MAX_IDENTIFIER_BYTES: usize = 249;
const MAX_GROUP_BLOB_BYTES: usize = 1024 * 1024;
const MAX_MEMBER_PROTOCOL_BYTES: usize = 1024 * 1024;
const MAX_GROUP_PROTOCOL_BYTES: usize = 16 * 1024 * 1024;
const MAX_GROUP_ASSIGNMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_SESSION_TIMEOUT_MS: i32 = 300_000;

/// One assignor offered in JoinGroup, with opaque client-owned metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinProtocol {
    pub name: String,
    pub metadata: Bytes,
}

/// One member and its selected-protocol metadata exposed to the group leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinMember {
    pub member_id: String,
    pub metadata: Bytes,
}

/// Successful JoinGroup state returned to one member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinOutcome {
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader_id: String,
    pub member_id: String,
    pub members: Vec<JoinMember>,
}

/// One opaque leader assignment supplied through SyncGroup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncAssignment {
    pub member_id: String,
    pub assignment: Bytes,
}

/// Ephemeral multi-member classic coordinator with durable offset commits.
#[derive(Clone, Debug)]
pub struct GroupCoordinator {
    offsets: GroupStore,
    groups: Arc<Mutex<HashMap<String, GroupSlot>>>,
}

type GroupSlot = Arc<GroupSlotState>;

#[derive(Debug)]
struct GroupSlotState {
    state: Mutex<Option<GroupState>>,
    changed: Notify,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GroupPhase {
    Joining,
    AwaitingSync,
    Stable,
}

#[derive(Clone, Debug)]
struct GroupState {
    generation_id: i32,
    rebalance_id: u64,
    phase: GroupPhase,
    phase_deadline: Instant,
    protocol_type: String,
    protocol_name: Option<String>,
    leader_id: String,
    members: BTreeMap<String, MemberState>,
}

#[derive(Clone, Debug)]
struct MemberState {
    protocols: Vec<JoinProtocol>,
    assignment: Option<Bytes>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    deadline: Instant,
    joined_rebalance: u64,
    join_result: Option<JoinOutcome>,
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
        let slot = Arc::new(GroupSlotState {
            state: Mutex::new(None),
            changed: Notify::new(),
        });
        groups.insert(group.to_owned(), slot.clone());
        Ok(slot)
    }

    async fn reclaim_slot(&self, group: &str, slot: &GroupSlot) {
        let mut groups = self.groups.lock().await;
        let is_same_idle_slot = groups.get(group).is_some_and(|current| {
            Arc::ptr_eq(current, slot)
                && Arc::strong_count(current) == 2
                && current.state.try_lock().is_ok_and(|state| state.is_none())
        });
        if is_same_idle_slot {
            groups.remove(group);
        }
    }

    pub async fn join(
        &self,
        group: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: &str,
        protocol_type: &str,
        protocols: &[JoinProtocol],
    ) -> Result<JoinOutcome, CoordinatorError> {
        self.validate_group_id(group)?;
        validate_session_timeout(session_timeout_ms)?;
        validate_rebalance_timeout(rebalance_timeout_ms)?;
        validate_member_id(member_id, true)?;
        validate_protocols(protocol_type, protocols)?;

        let slot = self.join_group_slot(group, member_id).await?;
        let now = Instant::now();
        let session_timeout = Duration::from_millis(session_timeout_ms as u64);
        let rebalance_timeout = Duration::from_millis(rebalance_timeout_ms as u64);
        let (assigned_member_id, target_rebalance_id) = {
            let mut state = slot.state.lock().await;
            let changed = advance_group(&mut state, now)?;
            if changed {
                slot.changed.notify_waiters();
            }

            match state.as_mut() {
                None if member_id.is_empty() => {
                    let assigned = Uuid::new_v4().to_string();
                    let mut members = BTreeMap::new();
                    members.insert(
                        assigned.clone(),
                        MemberState {
                            protocols: protocols.to_vec(),
                            assignment: None,
                            session_timeout,
                            rebalance_timeout,
                            deadline: now + session_timeout,
                            joined_rebalance: 1,
                            join_result: None,
                        },
                    );
                    *state = Some(GroupState {
                        generation_id: 0,
                        rebalance_id: 1,
                        phase: GroupPhase::Joining,
                        phase_deadline: now + rebalance_timeout,
                        protocol_type: protocol_type.to_owned(),
                        protocol_name: None,
                        leader_id: assigned.clone(),
                        members,
                    });
                    (assigned, 1)
                }
                None => return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId)),
                Some(group_state) => {
                    if !member_id.is_empty() && !group_state.members.contains_key(member_id) {
                        return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId));
                    }
                    if group_state.protocol_type != protocol_type
                        || !protocols_compatible(group_state, member_id, protocols)
                    {
                        return Err(CoordinatorError::kafka(
                            ResponseError::InconsistentGroupProtocol,
                        ));
                    }
                    validate_group_protocol_budget(group_state, member_id, protocols)?;

                    let assigned = if member_id.is_empty() {
                        if group_state.members.len() >= MAX_GROUP_MEMBERS {
                            return Err(CoordinatorError::kafka(
                                ResponseError::GroupMaxSizeReached,
                            ));
                        }
                        let assigned = Uuid::new_v4().to_string();
                        if group_state.phase != GroupPhase::Joining {
                            start_rebalance(group_state, now)?;
                        }
                        group_state.members.insert(
                            assigned.clone(),
                            MemberState {
                                protocols: protocols.to_vec(),
                                assignment: None,
                                session_timeout,
                                rebalance_timeout,
                                deadline: now + session_timeout,
                                joined_rebalance: group_state.rebalance_id,
                                join_result: None,
                            },
                        );
                        assigned
                    } else {
                        if group_state.phase != GroupPhase::Joining {
                            start_rebalance(group_state, now)?;
                        }
                        let current_rebalance = group_state.rebalance_id;
                        let member = group_state
                            .members
                            .get_mut(member_id)
                            .expect("member existence checked");
                        member.protocols = protocols.to_vec();
                        member.assignment = None;
                        member.session_timeout = session_timeout;
                        member.rebalance_timeout = rebalance_timeout;
                        member.deadline = now + session_timeout;
                        member.joined_rebalance = current_rebalance;
                        member.join_result = None;
                        member_id.to_owned()
                    };
                    group_state.phase_deadline = group_state
                        .members
                        .values()
                        .map(|member| now + member.rebalance_timeout)
                        .max()
                        .unwrap_or(now + rebalance_timeout);
                    (assigned, group_state.rebalance_id)
                }
            }
        };

        {
            let mut state = slot.state.lock().await;
            let changed = advance_group(&mut state, Instant::now())?;
            if changed {
                slot.changed.notify_waiters();
            }
        }
        slot.changed.notify_waiters();

        loop {
            let notified = slot.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = {
                let mut state = slot.state.lock().await;
                let changed = advance_group(&mut state, Instant::now())?;
                if changed {
                    slot.changed.notify_waiters();
                }
                let group_state = state
                    .as_mut()
                    .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
                if group_state.rebalance_id != target_rebalance_id {
                    return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress));
                }
                let member = group_state
                    .members
                    .get_mut(&assigned_member_id)
                    .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
                if let Some(outcome) = member.join_result.clone() {
                    return Ok(outcome);
                }
                group_state.phase_deadline.min(member.deadline)
            };
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = tokio::time::sleep_until(deadline.into()) => {}
            }
        }
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
        let mut submitted = false;

        loop {
            let notified = slot.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = {
                let mut state = slot.state.lock().await;
                let now = Instant::now();
                let changed = advance_group(&mut state, now)?;
                if changed {
                    slot.changed.notify_waiters();
                }
                if state.is_none() {
                    drop(state);
                    self.reclaim_slot(group, &slot).await;
                    return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId));
                }
                let group_state = active_group(&mut state, generation_id, member_id)?;
                if group_state.phase == GroupPhase::Joining {
                    return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress));
                }

                let is_leader = group_state.leader_id == member_id;
                if !submitted {
                    match group_state.phase {
                        GroupPhase::Joining => unreachable!("joining phase returned above"),
                        GroupPhase::AwaitingSync if is_leader => {
                            let installed = exact_assignments(group_state, assignments)?;
                            for (id, assignment) in installed {
                                let member = group_state
                                    .members
                                    .get_mut(&id)
                                    .expect("validated assignment member");
                                member.assignment = Some(assignment);
                                member.deadline = now + member.session_timeout;
                            }
                            group_state.phase = GroupPhase::Stable;
                            slot.changed.notify_waiters();
                        }
                        GroupPhase::AwaitingSync => {
                            if !assignments.is_empty() {
                                return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
                            }
                        }
                        GroupPhase::Stable if is_leader && !assignments.is_empty() => {
                            let proposed = exact_assignments(group_state, assignments)?;
                            if proposed.iter().any(|(id, assignment)| {
                                group_state
                                    .members
                                    .get(id)
                                    .and_then(|member| member.assignment.as_ref())
                                    != Some(assignment)
                            }) {
                                return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
                            }
                        }
                        GroupPhase::Stable if !is_leader && !assignments.is_empty() => {
                            return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
                        }
                        GroupPhase::Stable => {}
                    }
                    let member = group_state
                        .members
                        .get_mut(member_id)
                        .expect("active group validated member");
                    member.deadline = now + member.session_timeout;
                    submitted = true;
                }

                let member = group_state
                    .members
                    .get(member_id)
                    .expect("active group validated member");
                if group_state.phase == GroupPhase::Stable {
                    return member.assignment.clone().ok_or_else(|| {
                        CoordinatorError::kafka(ResponseError::RebalanceInProgress)
                    });
                }
                group_state.phase_deadline.min(member.deadline)
            };
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = tokio::time::sleep_until(deadline.into()) => {}
            }
        }
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
        let mut state = slot.state.lock().await;
        let now = Instant::now();
        let changed = advance_group(&mut state, now)?;
        let result = active_group(&mut state, generation_id, member_id).and_then(|group_state| {
            let member = group_state
                .members
                .get_mut(member_id)
                .expect("active group validated member");
            member.deadline = now + member.session_timeout;
            if group_state.phase != GroupPhase::Stable || member.assignment.is_none() {
                return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress));
            }
            Ok(())
        });
        let reclaim = state.is_none();
        drop(state);
        if changed {
            slot.changed.notify_waiters();
        }
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
        let mut state = slot.state.lock().await;
        let now = Instant::now();
        let mut changed = advance_group(&mut state, now)?;
        let result = if let Some(group_state) = state.as_mut() {
            if group_state.members.remove(member_id).is_none() {
                Err(CoordinatorError::kafka(ResponseError::UnknownMemberId))
            } else {
                changed = true;
                if group_state.members.is_empty() {
                    *state = None;
                } else {
                    if group_state.leader_id == member_id {
                        group_state.leader_id = group_state
                            .members
                            .keys()
                            .next()
                            .expect("nonempty group")
                            .clone();
                    }
                    if group_state.phase == GroupPhase::Joining {
                        if group_state
                            .members
                            .values()
                            .all(|member| member.joined_rebalance == group_state.rebalance_id)
                        {
                            finalize_rebalance(group_state, now)?;
                        }
                    } else {
                        start_rebalance(group_state, now)?;
                    }
                }
                Ok(())
            }
        } else {
            Err(CoordinatorError::kafka(ResponseError::UnknownMemberId))
        };
        let reclaim = state.is_none();
        drop(state);
        if changed {
            slot.changed.notify_waiters();
        }
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
        let mut state = slot.state.lock().await;
        let now = Instant::now();
        let changed = advance_group(&mut state, now)?;
        let group_state = match active_group(&mut state, generation_id, member_id) {
            Ok(group_state) => group_state,
            Err(error) => {
                let reclaim = state.is_none();
                drop(state);
                if changed {
                    slot.changed.notify_waiters();
                }
                if reclaim {
                    self.reclaim_slot(group, &slot).await;
                }
                return Err(error);
            }
        };
        let member = group_state
            .members
            .get(member_id)
            .expect("active group validated member");
        if group_state.phase != GroupPhase::Stable || member.assignment.is_none() {
            let reclaim = state.is_none();
            drop(state);
            if changed {
                slot.changed.notify_waiters();
            }
            if reclaim {
                self.reclaim_slot(group, &slot).await;
            }
            return Err(CoordinatorError::kafka(ResponseError::RebalanceInProgress));
        }
        if commits.is_empty() {
            drop(state);
            if changed {
                slot.changed.notify_waiters();
            }
            return Ok(());
        }
        let result = self
            .offsets
            .commit(group, commits)
            .await
            .map_err(CoordinatorError::Storage);
        drop(state);
        if changed {
            slot.changed.notify_waiters();
        }
        result
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

fn sweep_reclaimable_slots(groups: &mut HashMap<String, GroupSlot>, now: Instant) {
    groups.retain(|_, slot| {
        if Arc::strong_count(slot) != 1 {
            return true;
        }
        let Ok(mut state) = slot.state.try_lock() else {
            return true;
        };
        if advance_group(&mut state, now).is_err() {
            return true;
        }
        state.is_some()
    });
}

fn active_group<'a>(
    state: &'a mut Option<GroupState>,
    generation_id: i32,
    member_id: &str,
) -> Result<&'a mut GroupState, CoordinatorError> {
    let state = state
        .as_mut()
        .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownMemberId))?;
    if !state.members.contains_key(member_id) {
        return Err(CoordinatorError::kafka(ResponseError::UnknownMemberId));
    }
    if state.generation_id != generation_id {
        return Err(CoordinatorError::kafka(ResponseError::IllegalGeneration));
    }
    Ok(state)
}

fn advance_group(state: &mut Option<GroupState>, now: Instant) -> Result<bool, CoordinatorError> {
    let Some(group_state) = state.as_mut() else {
        return Ok(false);
    };
    let mut changed = false;
    let expired = group_state
        .members
        .iter()
        .filter_map(|(id, member)| (member.deadline <= now).then_some(id.clone()))
        .collect::<Vec<_>>();
    if !expired.is_empty() {
        changed = true;
        for id in expired {
            group_state.members.remove(&id);
        }
        if group_state.members.is_empty() {
            *state = None;
            return Ok(true);
        }
        ensure_leader(group_state);
        if group_state.phase != GroupPhase::Joining {
            start_rebalance(group_state, now)?;
        }
    }

    let Some(group_state) = state.as_mut() else {
        return Ok(changed);
    };
    match group_state.phase {
        GroupPhase::Joining => {
            if now >= group_state.phase_deadline {
                let rebalance_id = group_state.rebalance_id;
                group_state
                    .members
                    .retain(|_, member| member.joined_rebalance == rebalance_id);
                changed = true;
                if group_state.members.is_empty() {
                    *state = None;
                    return Ok(true);
                }
                ensure_leader(group_state);
            }
            if group_state
                .members
                .values()
                .all(|member| member.joined_rebalance == group_state.rebalance_id)
            {
                finalize_rebalance(group_state, now)?;
                changed = true;
            }
        }
        GroupPhase::AwaitingSync if now >= group_state.phase_deadline => {
            start_rebalance(group_state, now)?;
            changed = true;
        }
        GroupPhase::AwaitingSync | GroupPhase::Stable => {}
    }
    Ok(changed)
}

fn ensure_leader(state: &mut GroupState) {
    if !state.members.contains_key(&state.leader_id) {
        state.leader_id = state.members.keys().next().expect("nonempty group").clone();
    }
}

fn start_rebalance(state: &mut GroupState, now: Instant) -> Result<(), CoordinatorError> {
    state.rebalance_id = state
        .rebalance_id
        .checked_add(1)
        .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownServerError))?;
    state.phase = GroupPhase::Joining;
    state.protocol_name = None;
    state.phase_deadline = state
        .members
        .values()
        .map(|member| now + member.rebalance_timeout)
        .max()
        .unwrap_or(now);
    for member in state.members.values_mut() {
        member.assignment = None;
        member.join_result = None;
    }
    Ok(())
}

fn finalize_rebalance(state: &mut GroupState, now: Instant) -> Result<(), CoordinatorError> {
    ensure_leader(state);
    let protocol_name = select_protocol(state)
        .ok_or_else(|| CoordinatorError::kafka(ResponseError::InconsistentGroupProtocol))?;
    state.generation_id = state
        .generation_id
        .checked_add(1)
        .ok_or_else(|| CoordinatorError::kafka(ResponseError::UnknownServerError))?;
    state.phase = GroupPhase::AwaitingSync;
    state.protocol_name = Some(protocol_name.clone());
    state.phase_deadline = state
        .members
        .values()
        .map(|member| now + member.rebalance_timeout)
        .max()
        .unwrap_or(now);

    let leader_members = state
        .members
        .iter()
        .map(|(id, member)| JoinMember {
            member_id: id.clone(),
            metadata: member
                .protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
                .expect("selected protocol is common")
                .metadata
                .clone(),
        })
        .collect::<Vec<_>>();
    for (id, member) in &mut state.members {
        member.assignment = None;
        member.deadline = now + member.session_timeout;
        member.join_result = Some(JoinOutcome {
            generation_id: state.generation_id,
            protocol_name: protocol_name.clone(),
            leader_id: state.leader_id.clone(),
            member_id: id.clone(),
            members: if *id == state.leader_id {
                leader_members.clone()
            } else {
                Vec::new()
            },
        });
    }
    Ok(())
}

fn protocols_compatible(
    state: &GroupState,
    candidate_id: &str,
    candidate_protocols: &[JoinProtocol],
) -> bool {
    let choices = if state.leader_id == candidate_id {
        candidate_protocols
    } else {
        state
            .members
            .get(&state.leader_id)
            .map_or(candidate_protocols, |member| member.protocols.as_slice())
    };
    choices.iter().any(|choice| {
        candidate_protocols
            .iter()
            .any(|protocol| protocol.name == choice.name)
            && state.members.iter().all(|(id, member)| {
                let protocols = if id == candidate_id {
                    candidate_protocols
                } else {
                    member.protocols.as_slice()
                };
                protocols
                    .iter()
                    .any(|protocol| protocol.name == choice.name)
            })
    })
}

fn select_protocol(state: &GroupState) -> Option<String> {
    let leader = state.members.get(&state.leader_id)?;
    leader
        .protocols
        .iter()
        .find(|choice| {
            state.members.values().all(|member| {
                member
                    .protocols
                    .iter()
                    .any(|protocol| protocol.name == choice.name)
            })
        })
        .map(|protocol| protocol.name.clone())
}

fn exact_assignments(
    state: &GroupState,
    assignments: &[SyncAssignment],
) -> Result<BTreeMap<String, Bytes>, CoordinatorError> {
    if assignments.len() != state.members.len() {
        return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
    }
    let installed = assignments
        .iter()
        .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
        .collect::<BTreeMap<_, _>>();
    if installed.len() != assignments.len()
        || installed
            .keys()
            .any(|member_id| !state.members.contains_key(member_id))
    {
        return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
    }
    Ok(installed)
}

fn validate_session_timeout(session_timeout_ms: i32) -> Result<(), CoordinatorError> {
    if !(1..=MAX_SESSION_TIMEOUT_MS).contains(&session_timeout_ms) {
        return Err(CoordinatorError::kafka(
            ResponseError::InvalidSessionTimeout,
        ));
    }
    Ok(())
}

fn validate_rebalance_timeout(rebalance_timeout_ms: i32) -> Result<(), CoordinatorError> {
    if !(1..=MAX_SESSION_TIMEOUT_MS).contains(&rebalance_timeout_ms) {
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
    let mut retained_bytes = 0usize;
    for protocol in protocols {
        if !validate_token(&protocol.name)
            || protocol.metadata.len() > MAX_GROUP_BLOB_BYTES
            || !names.insert(&protocol.name)
        {
            return Err(CoordinatorError::kafka(
                ResponseError::InconsistentGroupProtocol,
            ));
        }
        retained_bytes = retained_bytes
            .checked_add(protocol.name.len())
            .and_then(|total| total.checked_add(protocol.metadata.len()))
            .ok_or_else(retained_bytes_error)?;
        if retained_bytes > MAX_MEMBER_PROTOCOL_BYTES {
            return Err(retained_bytes_error());
        }
    }
    Ok(())
}

fn validate_group_protocol_budget(
    state: &GroupState,
    candidate_id: &str,
    candidate_protocols: &[JoinProtocol],
) -> Result<(), CoordinatorError> {
    let mut retained_bytes =
        protocol_bytes(candidate_protocols).ok_or_else(retained_bytes_error)?;
    for (member_id, member) in &state.members {
        if member_id == candidate_id {
            continue;
        }
        retained_bytes = retained_bytes
            .checked_add(protocol_bytes(&member.protocols).ok_or_else(retained_bytes_error)?)
            .ok_or_else(retained_bytes_error)?;
        if retained_bytes > MAX_GROUP_PROTOCOL_BYTES {
            return Err(retained_bytes_error());
        }
    }
    Ok(())
}

fn protocol_bytes(protocols: &[JoinProtocol]) -> Option<usize> {
    protocols.iter().try_fold(0usize, |total, protocol| {
        total
            .checked_add(protocol.name.len())?
            .checked_add(protocol.metadata.len())
    })
}

fn validate_assignments(assignments: &[SyncAssignment]) -> Result<(), CoordinatorError> {
    if assignments.len() > MAX_GROUP_MEMBERS {
        return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
    }
    let mut member_ids = BTreeSet::new();
    let mut retained_bytes = 0usize;
    for assignment in assignments {
        validate_member_id(&assignment.member_id, false)?;
        if assignment.assignment.len() > MAX_GROUP_BLOB_BYTES
            || !member_ids.insert(&assignment.member_id)
        {
            return Err(CoordinatorError::kafka(ResponseError::InvalidRequest));
        }
        retained_bytes = retained_bytes
            .checked_add(assignment.member_id.len())
            .and_then(|total| total.checked_add(assignment.assignment.len()))
            .ok_or_else(retained_bytes_error)?;
        if retained_bytes > MAX_GROUP_ASSIGNMENT_BYTES {
            return Err(retained_bytes_error());
        }
    }
    Ok(())
}

fn retained_bytes_error() -> CoordinatorError {
    CoordinatorError::kafka(ResponseError::InvalidRequest)
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
            .join(group, 30_000, 30_000, "", "consumer", &protocols())
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

    async fn wait_for_rebalance(coordinator: &GroupCoordinator, group: &str, member: &JoinOutcome) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match coordinator
                    .heartbeat(group, member.generation_id, &member.member_id)
                    .await
                {
                    Err(error) if error.response_error() == ResponseError::RebalanceInProgress => {
                        break;
                    }
                    Ok(()) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected heartbeat error: {error}"),
                }
            }
        })
        .await
        .expect("rebalance did not start");
    }

    async fn joined_two_members(
        coordinator: &GroupCoordinator,
        group: &str,
        second_session_timeout_ms: i32,
    ) -> (JoinOutcome, JoinOutcome) {
        let first = stable_member_in(coordinator, group).await;
        let second_join = tokio::spawn({
            let coordinator = coordinator.clone();
            let group = group.to_owned();
            async move {
                coordinator
                    .join(
                        &group,
                        second_session_timeout_ms,
                        30_000,
                        "",
                        "consumer",
                        &protocols(),
                    )
                    .await
            }
        });
        wait_for_rebalance(coordinator, group, &first).await;
        let first = coordinator
            .join(
                group,
                30_000,
                30_000,
                &first.member_id,
                "consumer",
                &protocols(),
            )
            .await
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), second_join)
            .await
            .expect("second member join timed out")
            .unwrap()
            .unwrap();
        assert_eq!(first.generation_id, second.generation_id);
        assert_eq!(first.leader_id, first.member_id);
        assert_eq!(second.leader_id, first.member_id);
        assert_eq!(first.members.len(), 2);
        assert!(second.members.is_empty());
        (first, second)
    }

    async fn stable_two_members(
        coordinator: &GroupCoordinator,
        group: &str,
    ) -> (JoinOutcome, JoinOutcome) {
        let (first, second) = joined_two_members(coordinator, group, 30_000).await;

        let follower_sync = tokio::spawn({
            let coordinator = coordinator.clone();
            let group = group.to_owned();
            let second = second.clone();
            async move {
                coordinator
                    .sync(&group, second.generation_id, &second.member_id, &[])
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!follower_sync.is_finished());
        let first_assignment = Bytes::from_static(b"partitions-0-2");
        let second_assignment = Bytes::from_static(b"partition-1");
        assert_eq!(
            coordinator
                .sync(
                    group,
                    first.generation_id,
                    &first.member_id,
                    &[
                        SyncAssignment {
                            member_id: first.member_id.clone(),
                            assignment: first_assignment.clone(),
                        },
                        SyncAssignment {
                            member_id: second.member_id.clone(),
                            assignment: second_assignment.clone(),
                        },
                    ],
                )
                .await
                .unwrap(),
            first_assignment
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), follower_sync)
                .await
                .expect("follower sync timed out")
                .unwrap()
                .unwrap(),
            second_assignment
        );
        (first, second)
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
        assert_eq!(joined.members.len(), 1);
        groups
            .heartbeat("workers", joined.generation_id, &joined.member_id)
            .await
            .unwrap();

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
    async fn multi_member_consumer_group_rebalances_after_leave() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let (first, second) = stable_two_members(&groups, "workers").await;
        groups
            .heartbeat("workers", first.generation_id, &first.member_id)
            .await
            .unwrap();
        groups
            .heartbeat("workers", second.generation_id, &second.member_id)
            .await
            .unwrap();

        groups.leave("workers", &second.member_id).await.unwrap();
        assert_eq!(
            groups
                .heartbeat("workers", first.generation_id, &first.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::RebalanceInProgress
        );
        let rejoined = groups
            .join(
                "workers",
                30_000,
                30_000,
                &first.member_id,
                "consumer",
                &protocols(),
            )
            .await
            .unwrap();
        assert!(rejoined.generation_id > first.generation_id);
        assert_eq!(rejoined.members.len(), 1);
        let assignment = Bytes::from_static(b"all-partitions");
        assert_eq!(
            groups
                .sync(
                    "workers",
                    rejoined.generation_id,
                    &rejoined.member_id,
                    &[SyncAssignment {
                        member_id: rejoined.member_id.clone(),
                        assignment: assignment.clone(),
                    }],
                )
                .await
                .unwrap(),
            assignment
        );
        groups
            .heartbeat("workers", rejoined.generation_id, &rejoined.member_id)
            .await
            .unwrap();
        assert_eq!(
            groups
                .heartbeat("workers", first.generation_id, &first.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::IllegalGeneration
        );
        assert_eq!(
            groups
                .heartbeat("workers", rejoined.generation_id, &second.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
    }

    #[tokio::test]
    async fn negotiates_one_common_protocol_and_exposes_selected_metadata_to_leader() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let leader_protocols = vec![
            JoinProtocol {
                name: "range".into(),
                metadata: Bytes::from_static(b"leader-range"),
            },
            JoinProtocol {
                name: "roundrobin".into(),
                metadata: Bytes::from_static(b"leader-roundrobin"),
            },
        ];
        let follower_protocols = vec![JoinProtocol {
            name: "roundrobin".into(),
            metadata: Bytes::from_static(b"follower-roundrobin"),
        }];
        let first = groups
            .join("workers", 30_000, 30_000, "", "consumer", &leader_protocols)
            .await
            .unwrap();
        groups
            .sync(
                "workers",
                first.generation_id,
                &first.member_id,
                &[SyncAssignment {
                    member_id: first.member_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();

        let follower_join = tokio::spawn({
            let groups = groups.clone();
            let follower_protocols = follower_protocols.clone();
            async move {
                groups
                    .join(
                        "workers",
                        30_000,
                        30_000,
                        "",
                        "consumer",
                        &follower_protocols,
                    )
                    .await
            }
        });
        wait_for_rebalance(&groups, "workers", &first).await;
        let first = groups
            .join(
                "workers",
                30_000,
                30_000,
                &first.member_id,
                "consumer",
                &leader_protocols,
            )
            .await
            .unwrap();
        let second = follower_join.await.unwrap().unwrap();
        assert_eq!(first.protocol_name, "roundrobin");
        assert_eq!(second.protocol_name, "roundrobin");
        assert!(second.members.is_empty());
        let metadata = first
            .members
            .iter()
            .map(|member| (member.member_id.as_str(), member.metadata.as_ref()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            metadata.get(first.member_id.as_str()),
            Some(&b"leader-roundrobin".as_slice())
        );
        assert_eq!(
            metadata.get(second.member_id.as_str()),
            Some(&b"follower-roundrobin".as_slice())
        );
    }

    #[tokio::test]
    async fn rejects_incompatible_members_without_destabilizing_the_group() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let first = stable_member(&groups).await;
        let incompatible = vec![JoinProtocol {
            name: "roundrobin".into(),
            metadata: Bytes::new(),
        }];
        assert_eq!(
            groups
                .join("workers", 30_000, 30_000, "", "consumer", &incompatible,)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InconsistentGroupProtocol
        );
        assert_eq!(
            groups
                .join(
                    "workers",
                    30_000,
                    30_000,
                    "unknown-member",
                    "consumer",
                    &incompatible,
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
        groups
            .heartbeat("workers", first.generation_id, &first.member_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn requires_exact_leader_assignments_while_followers_wait() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let (first, second) = joined_two_members(&groups, "workers", 30_000).await;
        let follower_assignment = [SyncAssignment {
            member_id: second.member_id.clone(),
            assignment: Bytes::from_static(b"invalid-follower-write"),
        }];
        assert_eq!(
            groups
                .sync(
                    "workers",
                    second.generation_id,
                    &second.member_id,
                    &follower_assignment,
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );

        let incomplete = [SyncAssignment {
            member_id: first.member_id.clone(),
            assignment: Bytes::new(),
        }];
        assert_eq!(
            groups
                .sync(
                    "workers",
                    first.generation_id,
                    &first.member_id,
                    &incomplete,
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        let duplicate = [
            SyncAssignment {
                member_id: first.member_id.clone(),
                assignment: Bytes::new(),
            },
            SyncAssignment {
                member_id: first.member_id.clone(),
                assignment: Bytes::new(),
            },
        ];
        assert_eq!(
            groups
                .sync("workers", first.generation_id, &first.member_id, &duplicate,)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        let unknown = [
            SyncAssignment {
                member_id: first.member_id.clone(),
                assignment: Bytes::new(),
            },
            SyncAssignment {
                member_id: "unknown".into(),
                assignment: Bytes::new(),
            },
        ];
        assert_eq!(
            groups
                .sync("workers", first.generation_id, &first.member_id, &unknown,)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );

        let follower_sync = tokio::spawn({
            let groups = groups.clone();
            let second = second.clone();
            async move {
                groups
                    .sync("workers", second.generation_id, &second.member_id, &[])
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!follower_sync.is_finished());
        let assignments = [
            SyncAssignment {
                member_id: first.member_id.clone(),
                assignment: Bytes::from_static(b"leader"),
            },
            SyncAssignment {
                member_id: second.member_id.clone(),
                assignment: Bytes::from_static(b"follower"),
            },
        ];
        assert_eq!(
            groups
                .sync(
                    "workers",
                    first.generation_id,
                    &first.member_id,
                    &assignments,
                )
                .await
                .unwrap(),
            Bytes::from_static(b"leader")
        );
        assert_eq!(
            follower_sync.await.unwrap().unwrap(),
            Bytes::from_static(b"follower")
        );
    }

    #[tokio::test]
    async fn stable_generation_assignments_are_immutable() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let (first, second) = stable_two_members(&groups, "workers").await;
        let changed = [
            SyncAssignment {
                member_id: first.member_id.clone(),
                assignment: Bytes::from_static(b"partition-1"),
            },
            SyncAssignment {
                member_id: second.member_id.clone(),
                assignment: Bytes::from_static(b"partitions-0-2"),
            },
        ];
        assert_eq!(
            groups
                .sync("workers", first.generation_id, &first.member_id, &changed,)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        assert_eq!(
            groups
                .sync("workers", first.generation_id, &first.member_id, &[])
                .await
                .unwrap(),
            Bytes::from_static(b"partitions-0-2")
        );
        assert_eq!(
            groups
                .sync("workers", second.generation_id, &second.member_id, &[])
                .await
                .unwrap(),
            Bytes::from_static(b"partition-1")
        );
    }

    #[tokio::test]
    async fn leave_during_joining_completes_waiting_survivors() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let (first, second) = stable_two_members(&groups, "workers").await;
        let third_join = tokio::spawn({
            let groups = groups.clone();
            async move {
                groups
                    .join("workers", 30_000, 30_000, "", "consumer", &protocols())
                    .await
            }
        });
        wait_for_rebalance(&groups, "workers", &first).await;
        let first_rejoin = tokio::spawn({
            let groups = groups.clone();
            let first_id = first.member_id.clone();
            async move {
                groups
                    .join(
                        "workers",
                        30_000,
                        30_000,
                        &first_id,
                        "consumer",
                        &protocols(),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let slot = groups.existing_group_slot("workers").await.unwrap();
                let state = slot.state.lock().await;
                let group = state.as_ref().unwrap();
                let joined = group
                    .members
                    .values()
                    .filter(|member| member.joined_rebalance == group.rebalance_id)
                    .count();
                if group.phase == GroupPhase::Joining && group.members.len() == 3 && joined == 2 {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("surviving members did not enter the in-flight rebalance");

        groups.leave("workers", &second.member_id).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), first_rejoin)
            .await
            .expect("leader remained stranded after member leave")
            .unwrap()
            .unwrap();
        let third = tokio::time::timeout(Duration::from_secs(2), third_join)
            .await
            .expect("new member remained stranded after member leave")
            .unwrap()
            .unwrap();
        assert_eq!(first.generation_id, third.generation_id);
        assert_eq!(first.members.len(), 2);
        assert!(third.members.is_empty());
    }

    #[tokio::test]
    async fn same_member_join_retry_cannot_consume_another_waiters_completion() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let (first, second) = stable_two_members(&groups, "workers").await;
        let third_join = tokio::spawn({
            let groups = groups.clone();
            async move {
                groups
                    .join("workers", 30_000, 30_000, "", "consumer", &protocols())
                    .await
            }
        });
        wait_for_rebalance(&groups, "workers", &first).await;

        let first_retry_protocols = vec![JoinProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(b"first-waiter"),
        }];
        let abandoned_request = tokio::spawn({
            let groups = groups.clone();
            let member_id = first.member_id.clone();
            let retry_protocols = first_retry_protocols.clone();
            async move {
                groups
                    .join(
                        "workers",
                        30_000,
                        30_000,
                        &member_id,
                        "consumer",
                        &retry_protocols,
                    )
                    .await
            }
        });
        wait_for_member_metadata(&groups, "workers", &first.member_id, b"first-waiter").await;

        let second_retry_protocols = vec![JoinProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(b"retained-retry"),
        }];
        let retained_request = tokio::spawn({
            let groups = groups.clone();
            let member_id = first.member_id.clone();
            let retry_protocols = second_retry_protocols.clone();
            async move {
                groups
                    .join(
                        "workers",
                        30_000,
                        30_000,
                        &member_id,
                        "consumer",
                        &retry_protocols,
                    )
                    .await
            }
        });
        wait_for_member_metadata(&groups, "workers", &first.member_id, b"retained-retry").await;

        let second = groups
            .join(
                "workers",
                30_000,
                30_000,
                &second.member_id,
                "consumer",
                &protocols(),
            )
            .await
            .unwrap();
        let retained = tokio::time::timeout(Duration::from_secs(2), retained_request)
            .await
            .expect("retained JoinGroup retry was stranded")
            .unwrap()
            .unwrap();
        let abandoned = tokio::time::timeout(Duration::from_secs(2), abandoned_request)
            .await
            .expect("original JoinGroup waiter was stranded")
            .unwrap()
            .unwrap();
        let third = tokio::time::timeout(Duration::from_secs(2), third_join)
            .await
            .expect("new member JoinGroup was stranded")
            .unwrap()
            .unwrap();

        assert_eq!(retained.member_id, first.member_id);
        assert_eq!(retained, abandoned);
        assert_eq!(retained.generation_id, second.generation_id);
        assert_eq!(retained.generation_id, third.generation_id);
        assert_eq!(retained.members.len(), 3);
        assert_eq!(
            retained
                .members
                .iter()
                .find(|member| member.member_id == retained.member_id)
                .unwrap()
                .metadata,
            Bytes::from_static(b"retained-retry")
        );
    }

    async fn wait_for_member_metadata(
        groups: &GroupCoordinator,
        group_id: &str,
        member_id: &str,
        expected: &[u8],
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let slot = groups.existing_group_slot(group_id).await.unwrap();
                let state = slot.state.lock().await;
                let group = state.as_ref().unwrap();
                let member = group.members.get(member_id).unwrap();
                if group.phase == GroupPhase::Joining
                    && member.joined_rebalance == group.rebalance_id
                    && member.protocols[0].metadata.as_ref() == expected
                {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("JoinGroup waiter did not register its member metadata");
    }

    #[tokio::test]
    async fn member_expiry_rebalances_only_its_group() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let independent = stable_member_in(&groups, "independent").await;
        let (first, second) = joined_two_members(&groups, "workers", 50).await;
        groups
            .sync(
                "workers",
                first.generation_id,
                &first.member_id,
                &[
                    SyncAssignment {
                        member_id: first.member_id.clone(),
                        assignment: Bytes::from_static(b"survivor"),
                    },
                    SyncAssignment {
                        member_id: second.member_id.clone(),
                        assignment: Bytes::from_static(b"expiring"),
                    },
                ],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_eq!(
            groups
                .heartbeat("workers", first.generation_id, &first.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::RebalanceInProgress
        );
        groups
            .heartbeat(
                "independent",
                independent.generation_id,
                &independent.member_id,
            )
            .await
            .unwrap();
        let rejoined = groups
            .join(
                "workers",
                30_000,
                30_000,
                &first.member_id,
                "consumer",
                &protocols(),
            )
            .await
            .unwrap();
        assert_eq!(rejoined.members.len(), 1);
        assert_eq!(
            groups
                .heartbeat("workers", rejoined.generation_id, &second.member_id)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::UnknownMemberId
        );
    }

    #[tokio::test]
    async fn expires_members_and_requires_rejoin() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let joined = groups
            .join("workers", 1, 30_000, "", "consumer", &protocols())
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
                .join("workers", 30_000, 30_000, "", "consumer", &protocols())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_invalid_protocols_and_heartbeat_before_sync() {
        let groups = coordinator(Arc::new(InMemory::new()));
        assert_eq!(
            groups
                .join("workers", 0, 30_000, "", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidSessionTimeout
        );
        assert_eq!(
            groups
                .join("workers", 30_000, 0, "", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidSessionTimeout
        );
        assert_eq!(
            groups
                .join("workers", 30_000, 30_000, "", "connect", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InconsistentGroupProtocol
        );
        assert_eq!(
            groups
                .join("workers", 30_000, 30_000, "", "consumer", &[])
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InconsistentGroupProtocol
        );

        let joined = groups
            .join("workers", 30_000, 30_000, "", "consumer", &protocols())
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
                .join(
                    "unknown",
                    30_000,
                    30_000,
                    "member",
                    "consumer",
                    &protocols(),
                )
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
                .join("one-too-many", 30_000, 30_000, "", "consumer", &protocols(),)
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
            .join("replacement", 30_000, 30_000, "", "consumer", &protocols())
            .await
            .unwrap();
        assert_eq!(groups.groups.lock().await.len(), MAX_GROUP_SLOTS);
    }

    #[tokio::test]
    async fn bounds_group_members_before_allocating_another_member() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let joined = stable_member(&groups).await;
        let slot = groups.existing_group_slot("workers").await.unwrap();
        {
            let mut state = slot.state.lock().await;
            let group = state.as_mut().unwrap();
            let template = group.members.get(&joined.member_id).unwrap().clone();
            for index in 1..MAX_GROUP_MEMBERS {
                group
                    .members
                    .insert(format!("bounded-member-{index}"), template.clone());
            }
            assert_eq!(group.members.len(), MAX_GROUP_MEMBERS);
        }
        assert_eq!(
            groups
                .join("workers", 30_000, 30_000, "", "consumer", &protocols())
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::GroupMaxSizeReached
        );
    }

    #[tokio::test]
    async fn bounds_aggregate_protocol_and_assignment_bytes_without_mutation() {
        let groups = coordinator(Arc::new(InMemory::new()));
        let joined = stable_member(&groups).await;
        let oversized_member_protocols = vec![
            JoinProtocol {
                name: "range".into(),
                metadata: Bytes::from(vec![0; MAX_MEMBER_PROTOCOL_BYTES / 2]),
            },
            JoinProtocol {
                name: "roundrobin".into(),
                metadata: Bytes::from(vec![0; MAX_MEMBER_PROTOCOL_BYTES / 2]),
            },
        ];
        assert_eq!(
            groups
                .join(
                    "workers",
                    30_000,
                    30_000,
                    "",
                    "consumer",
                    &oversized_member_protocols,
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        groups
            .heartbeat("workers", joined.generation_id, &joined.member_id)
            .await
            .unwrap();

        let slot = groups.existing_group_slot("workers").await.unwrap();
        {
            let mut state = slot.state.lock().await;
            let group = state.as_mut().unwrap();
            let large_protocols = vec![JoinProtocol {
                name: "range".into(),
                metadata: Bytes::from(vec![0; MAX_MEMBER_PROTOCOL_BYTES - 128]),
            }];
            let mut template = group.members.get(&joined.member_id).unwrap().clone();
            template.protocols = large_protocols;
            group.members.get_mut(&joined.member_id).unwrap().protocols =
                template.protocols.clone();
            for index in 1..16 {
                group
                    .members
                    .insert(format!("bounded-bytes-{index}"), template.clone());
            }
        }
        let candidate = [JoinProtocol {
            name: "range".into(),
            metadata: Bytes::from(vec![0; 4096]),
        }];
        assert_eq!(
            groups
                .join("workers", 30_000, 30_000, "", "consumer", &candidate,)
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        {
            let state = slot.state.lock().await;
            let group = state.as_ref().unwrap();
            assert_eq!(group.phase, GroupPhase::Stable);
            assert_eq!(group.members.len(), 16);
            assert!(
                group
                    .members
                    .values()
                    .all(|member| member.assignment.is_some())
            );
        }

        let assignment_groups = coordinator(Arc::new(InMemory::new()));
        let leader = stable_member(&assignment_groups).await;
        let slot = assignment_groups
            .existing_group_slot("workers")
            .await
            .unwrap();
        {
            let mut state = slot.state.lock().await;
            let group = state.as_mut().unwrap();
            let mut template = group.members.get(&leader.member_id).unwrap().clone();
            template.assignment = None;
            group.members.get_mut(&leader.member_id).unwrap().assignment = None;
            for index in 1..17 {
                group
                    .members
                    .insert(format!("assignment-member-{index}"), template.clone());
            }
            group.phase = GroupPhase::AwaitingSync;
        }
        let assignment_blob = Bytes::from(vec![0; MAX_GROUP_BLOB_BYTES]);
        let assignments = {
            let state = slot.state.lock().await;
            state
                .as_ref()
                .unwrap()
                .members
                .keys()
                .map(|member_id| SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: assignment_blob.clone(),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            assignment_groups
                .sync(
                    "workers",
                    leader.generation_id,
                    &leader.member_id,
                    &assignments,
                )
                .await
                .unwrap_err()
                .response_error(),
            ResponseError::InvalidRequest
        );
        let state = slot.state.lock().await;
        let group = state.as_ref().unwrap();
        assert_eq!(group.phase, GroupPhase::AwaitingSync);
        assert!(
            group
                .members
                .values()
                .all(|member| member.assignment.is_none())
        );
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
