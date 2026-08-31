# Goal: Parallel Topics And Consumer Groups

Work in `/Users/smarzola/projects/walstream`.

Make Walstream useful for horizontally parallel Kafka consumption: topics can durably expose multiple independent partitions, and multiple dynamic classic-protocol consumers in one group divide those partitions, rebalance on membership changes, and resume from durable offsets after complete broker replacement.

Source of truth: the outcome agreed in the 2026-09-01 Codex thread and this contract.

Starting branch: `main` at `bda9ce2f8c5aa8a8e87ad12398df602571fc3ba1`.
Goal branch: `feat/multi-partition-consumer-groups`.

## Target State

When this goal is complete:

- New auto-created topics use a bounded operator-configured partition count, defaulting to one, and persist that count in object storage so later brokers cannot reinterpret it.
- Metadata, Produce, Fetch, ListOffsets, OffsetCommit, and OffsetFetch operate independently across every valid partition; existing partition-0-only topic data remains readable as a one-partition topic.
- Multiple dynamic members using the classic consumer protocol can join one group, receive a complete non-overlapping client-computed assignment, heartbeat, commit, leave or expire, and rebalance without affecting other groups.
- Pinned librdkafka and Kafka Java clients prove three partitions split across two live consumers, reassignment after one member leaves, and durable-offset recovery by retained clients across complete broker replacement.

## Current-State Evidence

Verified before this prompt was written:

- `src/log.rs`: object paths and manifest validation hard-code partition `0`; `validate_partition` rejects every other index.
- `src/protocol.rs`: Metadata v4 advertises exactly partition `0`, while Produce, Fetch, and ListOffsets already iterate requested partitions.
- `src/group.rs`: durable offset keys already include a partition index, but validation currently permits only partition `0`.
- `src/coordinator.rs`: one `MemberState` is stored per group, a second empty member is rejected with `GROUP_MAX_SIZE_REACHED`, and SyncGroup requires exactly one assignment.
- `scripts/test-consumer-group-clients.sh`: pinned librdkafka `2.12.1` and Kafka Java `4.2.0` each prove one retained consumer across broker replacement against RustFS.
- `cargo test --all-targets`, `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `./scripts/test-consumer-group-clients.sh`, and the three `WALSTREAM_E2E_BACKEND=... ./scripts/test-s3-e2e.sh` variants passed for the preceding single-partition/single-member implementation.

Unknowns that may affect implementation details, but not the target state:

- Exact internal coordinator state representation and wait/notification structure.
- Whether the real-client proof is clearest as extensions of the existing probes or a focused new probe.

## Constraints And Non-Goals

- Preserve the one-broker, S3-only durable-dependency architecture and current object-store conditional-write contract.
- Preserve existing partition-0 manifests. A topic without new topic metadata but with a valid partition-0 manifest must be interpreted as a legacy one-partition topic and may be upgraded without rewriting its log.
- Default to one partition. Bound configured topic partitions to `1..=1024`; changing the default affects only newly created topics.
- Persist schema-versioned topic metadata under the existing cluster/topic namespace. Per-partition log ordering and offsets remain independent.
- Keep classic dynamic membership on the currently advertised API versions. Assignment remains leader/client computed from opaque subscription metadata; the broker must not implement a server-side assignor.
- Bound identifiers, group members, protocols, metadata, assignments, topic partitions, waiting time, and resident groups before untrusted input can cause unbounded work or allocation.
- Keep durable offsets CAS-backed. Membership, generations, assignments, heartbeats, and session deadlines remain process-local and must be re-established after replacement.
- Do not add CreateTopics or other admin APIs, static membership, the newer consumer protocol, transactions, idempotent producers, replication, retention, compaction, authentication, TLS, quotas, or multi-region behavior.
- Preserve unrelated user changes and repository-supported Apple Container workflows. Do not introduce Docker commands or dependencies.
- Implement the smallest coherent design that completely satisfies this goal. Prefer direct code and current repository patterns; do not add speculative layers, dependencies, configuration, or adjacent features.
- Simplicity must not omit required error handling, concurrency correctness, backward compatibility, real-client tests, or documentation.

## Authorization And Decisions

This goal authorizes repository inspection, in-scope local edits, focused Conventional Commit checkpoints, branch-local history, non-destructive verification, and required read-only reviewer agents.

It does not authorize pushing, opening or merging a pull request, publishing a release, destructive actions, secrets or permission changes, or material scope expansion. Ask before any such action unless the user separately authorizes it.

Continue through routine implementation choices using repository evidence. Ask only when an ambiguity materially changes user-visible behavior, architecture, durable-data compatibility, security posture, or authorization. Otherwise choose the least-surprising in-scope interpretation and record material decisions below.

Before declaring a blocker, exhaust safe in-scope alternatives and preserve goal state. Report the evidence and smallest external decision or change required; never claim completion without proof.

## Success Criteria

The goal is complete only when:

1. A serve-time setting configures `1..=1024` partitions for newly auto-created topics, defaults to one, fails preflight outside the bound, and persisted metadata wins over later setting changes.
2. Legacy partition-0 topics remain readable, while new multi-partition topics advertise every partition and isolate manifests, segments, offsets, and errors by partition.
3. Multiple dynamic classic members negotiate one common protocol, expose all current member metadata to the elected leader, wait for complete leader assignments, and reject stale generations, unknown members, incomplete/duplicate assignments, or invalid protocol changes with appropriate Kafka errors.
4. Join, leave, and session expiry trigger bounded rebalances; surviving members receive `REBALANCE_IN_PROGRESS`, rejoin, and settle on a new generation without disrupting independent groups.
5. Offset commits remain generation-fenced and durable for every valid partition, including after complete broker replacement.
6. Real pinned librdkafka and Kafka Java clients each demonstrate a three-partition/two-member disjoint complete assignment, reassignment after one member leaves, and retained-client rejoin after broker replacement without replaying committed records.
7. README and architecture documentation accurately describe configuration, object layout, classic-group behavior, failure semantics, bounds, compatibility, and remaining non-goals.
8. Every milestone is checked off with verification evidence and a focused Conventional Commit checkpoint.
9. Final regression verification and a fresh independent audit pass without blocking findings.

## Milestones

- [ ] Milestone 1: Durable multi-partition topics and partition-aware data APIs
- [ ] Milestone 2: Bounded multi-member classic group coordination
- [ ] Milestone 3: Real-client rebalance/recovery proof and operator documentation

### Checkpoint Protocol

At the end of each milestone:

1. Satisfy its acceptance criteria.
2. Run its verification commands and inspect the results.
3. Freeze implementation writes and obtain a clean adversarial milestone review; repair and re-review blocking findings.
4. Mark its checkbox `[x]` and add a dated status note with outcome, exact commands, and results.
5. Commit implementation, tests, docs, and this contract update together with a focused Conventional Commit message.
6. Report the resulting commit hash before starting the next milestone.

If verification fails, leave the milestone unchecked and do not make its checkpoint commit. Diagnose and repair in-scope failures rather than weakening tests. A commit cannot contain its own hash, so report it after committing.

## Milestone 1: Durable Multi-Partition Topics

Why this matters:

- Parallel consumers require independently ordered logs discoverable through ordinary Kafka metadata, and the partition count must survive stateless broker replacement.

Acceptance criteria:

- Serve preflight validates a bounded default partition count; new topic metadata persists the creation-time count and concurrent creators converge safely.
- Existing legacy topic objects infer one partition without log rewrite.
- Metadata advertises all valid partitions, all data APIs operate on them, invalid partitions return `UNKNOWN_TOPIC_OR_PARTITION`, and independent partitions maintain independent offsets and objects.
- Focused unit/protocol/stock-client tests cover persistence, races, compatibility, bounds, and multi-partition restart recovery.

Likely touchpoints (non-exhaustive):

- `src/config.rs`, `src/main.rs`, `src/log.rs`, `src/protocol.rs`
- `tests/kafka_protocol.rs`, `tests/stock_client.rs`, `tests/s3_e2e.rs`

Verification:

```bash
cargo test --all-targets multi_partition
cargo test --all-targets metadata
cargo test --test stock_client
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Status: Not started.

## Milestone 2: Multi-Member Classic Groups

Why this matters:

- Multiple partitions provide no horizontal consumption unless classic clients can coordinate a complete, exclusive assignment and recover from membership changes.

Acceptance criteria:

- Multiple members join a bounded group through JoinGroup v2, negotiate a common protocol, and receive correct leader/follower views.
- SyncGroup v1 accepts only the elected leader's exact one-per-member assignment and makes followers wait for their own assignment within a bounded session.
- Heartbeats, commits, leaves, and expiry enforce member identity and generation fencing and drive correct rebalance transitions.
- Concurrent coordinator tests cover join/sync waiting, incompatible protocols, duplicate/incomplete/unknown assignments, stale generations, leave/expiry, group isolation, durable commits, and bounded group/member state.

Likely touchpoints (non-exhaustive):

- `src/coordinator.rs`, `src/group.rs`, `src/protocol.rs`, `src/wire.rs`
- `tests/kafka_protocol.rs`

Verification:

```bash
cargo test --all-targets coordinator
cargo test --all-targets consumer_group
cargo test --test kafka_protocol
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Status: Not started.

## Milestone 3: Real-Client Parallel Recovery

Why this matters:

- Internal request tests cannot prove compatibility with real assignors, callbacks, retry timing, and replacement behavior.

Acceptance criteria:

- Pinned librdkafka and Kafka Java probes each prove a complete non-overlapping three-partition assignment across two live members.
- Each probe proves all partitions move to the survivor after one member leaves, then retains the surviving client through broker shutdown/restart, observes a new member identity and later exact assignment, and resumes every partition from committed next offsets without replay.
- RustFS runs the complete client proof; the existing RustFS, SeaweedFS, and MinIO object-store matrix still proves replacement and concurrent publication.
- Documentation matches the delivered behavior and no longer lists multi-partition or multi-member classic groups as non-goals.

Likely touchpoints (non-exhaustive):

- `scripts/test-consumer-group-clients.sh`
- `tests/clients/librdkafka_probe.py`
- `tests/clients/java/src/main/java/dev/walstream/ConsumerGroupProbe.java`
- `scripts/test-s3-e2e.sh`, `README.md`, `docs/architecture.md`

Verification:

```bash
sh -n scripts/test-consumer-group-clients.sh scripts/test-s3-e2e.sh
container system status
./scripts/test-consumer-group-clients.sh
WALSTREAM_E2E_BACKEND=rustfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=seaweedfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=minio ./scripts/test-s3-e2e.sh
```

Status: Not started.

## Final Verification

Run from `/Users/smarzola/projects/walstream`:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
sh -n scripts/test-consumer-group-clients.sh scripts/test-s3-e2e.sh
./scripts/test-consumer-group-clients.sh
WALSTREAM_E2E_BACKEND=rustfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=seaweedfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=minio ./scripts/test-s3-e2e.sh
```

Inspect every failure. Fix in-scope regressions rather than weakening tests. Document an unrelated pre-existing failure only with the exact command, result, and evidence that this branch did not cause it.

## Decision And Status Notes

- 2026-09-01: Use persisted per-topic metadata with a creation-time partition count and a serve-time default of one. This provides configurable auto-creation without adding an admin API.
- 2026-09-01: Interpret valid legacy partition-0 manifests without topic metadata as one-partition topics to preserve the current durable format.
- 2026-09-01: Keep assignment client-computed. The broker coordinates membership and distributes opaque leader-provided assignments, matching the classic protocol boundary.

## Resume Protocol

On resume, read this contract, repository instructions, `git status`, status notes, and recent commits. Verify completed checkpoints and continue from the first unchecked milestone without redoing completed work. New evidence may refine implementation details but must not silently weaken target behavior or success criteria.

## Final Report

Lead with `Achieved` or `Not achieved`, then report target-state and success-criteria status, milestone commits, files changed, exact verification results, reviewer rounds and disposition, residual risks, and any unauthorized external delivery step that remains.
