# Goal: Durable Idempotent Producers

Repository: `/Users/smarzola/projects/walstream`
Revision: 1, 2026-09-06.
Approval: **Approved.** The user said “Go for it” after the three-step proposal: producer identities/epochs, durable sequence tracking, and recovery/retention integration. The decisive acceptance scenario commits a batch, loses its acknowledgement, replaces the broker, and lets the same producer retry without appending a second copy.
Execution: implement the approved outcome using the reloaded pursue-goal and goal-loop-prompt skills. Routine implementation choices below carry out that outcome; material changes require fresh approval.
Goal-file policy: commit this goal and its updates, following the three comparable tracked contracts in `docs/goals`.
Starting base: merged `main` at `9675a35eac3f5a9a51cfe8a1c7e3228a73b54e2f`. Branch: `feat/durable-idempotent-producers`. Worktree clean before goal preparation. No applicable repository AGENTS.md found.
PR delivery: GitHub `smarzola/walstream`, base `main`; open draft immediately after the goal commit, keep draft through verification/review, then verify ready status and final checks. No merge or release authorization.

## Outcome

A nontransactional Kafka producer can enable idempotence and retry a committed batch after an acknowledgement is lost or the broker is replaced. The broker returns that batch's original offsets without another append. Producer state survives maintenance even if the corresponding records have expired, so a recognized old retry cannot resurrect them.

| Step | Deliverable | Success condition |
|---|---|---|
| 1 | Durable producer identities and batch metadata | Standard Java and librdkafka producers initialize IDs and send uncompressed idempotent batches; ordinary non-idempotent clients remain compatible |
| 2 | Atomic durable sequence state and maintenance integration | Records and sequence state publish together; duplicate retries, stale epochs, sequence gaps, concurrent writers, retention, and replacement preserve the agreed invariants |
| 3 | Runtime proof and ready PR | Both implementer and fresh reviewer exercise real producer recovery; relevant checks pass and the reviewed PR becomes ready |

## Decisions and bounds

| Decision | Choice and reason | Tradeoff or alternative |
|---|---|---|
| Producer identity | Cluster-wide monotonically allocated IDs in an S3 CAS object. Nontransactional InitProducerId returns a fresh ID and epoch zero; newer batch epochs fence older epochs per partition | A lost initialization response can consume an unused ID; IDs must never be reused. This follows Kafka's nontransactional allocation behavior |
| Atomicity | Publish producer state through the same partition root CAS as record/index references | A separate mutable producer-state object would leave a two-object failure window |
| State layout | Immutable bounded pages keyed by producer ID, with bounded traversal and a small root reference. Keep the latest five batch identities, content fingerprints, sequence ranges, and original offsets per producer/epoch | Adds object requests to idempotent appends. An inline lifetime-limited producer map would recreate a hard partition capacity limit |
| Retention | Preserve producer sequence state independently of record expiry, including after complete expiry. GC traces live producer pages and collects only superseded/tentative pages | Metadata grows with distinct producer IDs. Automatic producer-state expiry is outside this milestone because forgetting identities can allow old retries to appear new |
| Durable format | Schema 4 adds the producer-state reference; read schemas 1–3 and adopt the new format without rewriting existing record objects | Continue the existing one-way upgrade policy: older binaries reject upgraded roots, and mixed-version operation/downgrade remain unsupported |
| Retry boundary | Kafka's five-batch retry window and sequence/epoch rules, with exact fingerprints for recognized repeats; no promise of deduplication across a new application producer identity | Very old retries outside the remembered window fail explicitly instead of being treated as new records |

Exact protocol versions, page fanout, response/error handling, and batch-frame handling must be verified against the pinned Kafka clients and existing allocation-safe wire boundary. Preserve native batch identities when requests contain multiple batches; do not flatten away producer sequence boundaries. Keep metadata bodies within the existing 4 MiB limit and ordinary append/fetch paths free of bucket listings.

Primary reference: [Kafka 4.2 producer configuration](https://kafka.apache.org/42/configuration/producer-configs/#enable.idempotence). Pinned source inspection confirms null-transactional-id initialization always returns a fresh producer ID at epoch zero in [TransactionCoordinator](https://github.com/apache/kafka/blob/4.2.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala). Use source/reference behavior to resolve implementation details; do not claim transactions or end-to-end exactly-once processing.

## Inspected baseline and scope

`src/protocol.rs` advertises Produce v7 but no InitProducerId. `src/codec.rs` validates allocation bounds then flattens decoded batches. `src/log.rs` rejects producer IDs/epochs and resets non-idempotent record sequences before canonical encoding. Each append publishes only by exact-root CAS. `src/log/index.rs` supplies bounded immutable log pages and schema-3 retained ranges. Maintenance closes its inventory before publication and fully traces the committed graph before deletion. The librdkafka runtime probe currently disables idempotence explicitly.

Scope: native nontransactional producer initialization, producer batch validation and deduplication, durable sequence state, format compatibility, GC/retry integration, operator documentation, and real stock-client proof. Preserve existing groups, retained offsets, immutable record bytes, single-broker interface, and S3-only durable dependency.

Non-goals: transactions, producer application restart identity recovery, compression, batching/throughput optimization, automatic state expiry, additional brokers, authentication, TLS, or unrelated cleanup. Keep current limits and meaningful tests; do not weaken assertions to hide defects.

## Success criteria

1. Native InitProducerId works for the exercised advertised versions, allocations remain unique across independent processes/restart/ambiguous responses, and malformed or transactional requests fail explicitly without accidental allocation or publication.
2. Idempotent batches retain their identity and sequence boundaries. A new producer/epoch begins at sequence zero; valid sequences advance with Kafka wrap semantics. Stale epochs, gaps, invalid records, and conflicting repeated batch content fail without publishing data. Ordinary non-idempotent production remains supported.
3. An acknowledged batch's record references and deduplication state share one root CAS. The last five recognized batches return original offsets on retry without record/index/state writes; failed and ambiguous publication, concurrent same/different producers, and retries after process replacement remain correct.
4. Producer-state pages are immutable, bounded, namespace-checked, and integrity-checked. Lookup/update uses a bounded path rather than scanning all producers. Many producer IDs cross page/split boundaries without a fixed small lifetime producer limit.
5. Retention preserves sequence state and never pins expired record objects merely for deduplication. GC inventories/traces the new page namespace under its existing fence protocol. Paused writers and collectors cannot resurrect collected pages; missing referenced state from a changed root retries completely, while corruption/unchanged-root absence fails closed. Empty-log replacement plus duplicate/new appends preserve original offsets and the exclusive next offset.
6. Schemas 1–3 remain readable and migrate without rewriting record bodies. Old executables reject schema 4. Existing indexed lookup, maintenance, consumer recovery, and three-backend checks remain valid.
7. Both implementer and fresh independent reviewer personally build, launch, and exercise the changed software. Real Java 4.2.0 and librdkafka 2.12.1 producers retain the same instances through a controlled lost-ack broker outage and complete retries without duplicate records. Retention between commit and retry must not resurrect expired batches. Capture actual request/commit/response boundaries, IDs, sequences, offsets, and persisted effects.
8. Formatting, Clippy, meaningful tests, required regression checks, both runtime records, and a fresh independent review pass. No material correctness, completeness, simplicity, or test-quality findings remain. Push coherent checkpoints to an early draft PR, verify the published head/checks, update the description to actual behavior/evidence, mark ready, and verify readiness plus any newly triggered checks.

## Verification and review

Use existing commands: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, shell syntax checks, `scripts/test-maintenance.sh`, `scripts/test-log-index.sh`, `scripts/test-consumer-group-clients.sh`, and `WALSTREAM_E2E_BACKEND={rustfs,seaweedfs,minio} scripts/test-s3-e2e.sh`. The S3 integration test is ignored in the default suite and must run through its backend harness. Reuse unchanged valid evidence and rerun affected checks after repairs.

Add only meaningful coverage for the new identity/sequence/atomicity and retention boundaries. New producer runtime scenarios are proposed additions, not commands already implemented. Use disposable Apple Container storage and owned process/proxy resources; clean up only owned artifacts. Preserve old-format fixtures and record bytes; adapt schema assertions without weakening their safety checks.

Each role runs `cargo build --release`, launches the actual broker against disposable RustFS, drives native Kafka producers, forces a lost response after an observed root commit, replaces the process while the same producer stays alive, then verifies the delivery result and full consumer readback. Repeat the relevant retention/empty-log and failure scenarios. Tests or another role's report do not substitute for either runtime record.

One fresh final reviewer: `gpt-5.6-sol`, `fork_turns: none`; read the pursue-goal reviewer contract and supply approved goal, full diff, raw evidence, and runtime setup. Source edits freeze during review. A bounded earlier design review is justified for the new producer-state publication/GC contract; it does not replace final review. Both roles challenge material needless complexity and ineffective tests. Keep documentation concise and organized around operator behavior and verified compatibility.

## Status

- [x] Native producer initialization and validated batch handling.
- [x] Atomic sequence tracking and maintenance/recovery integration.
- [x] Complete verification and independent runtime review.
- PR delivery: the linked GitHub PR records the current published head, checks, and readiness. Remove draft status only after the final evidence commit passes CI.

Implementer runtime: passed on final implementation commit `6603e08479fd3053e4c6f4668885ac3760f04dcb`, including a rerun after final batch-header validation. `scripts/test-idempotent-producers.sh` built and launched the broker against disposable RustFS, ran the pinned clients in Apple Container, withheld each initial successful Produce response, replaced the broker, and observed the same producer retry. IDs 0–3 stayed at epoch/sequence zero with byte-identical payloads. All returned offset zero without changing the partition root; subsequent records used offset one. Normal readback was `[0:first, 1:next]`; retention-before-retry readback was `[1:next]`, with earliest/latest 1/2. Raw output: `/tmp/walstream-producer-runtime-final.log`; final snapshots and request traces: `/var/folders/ww/y0g67mbx2mn0s9rt57gy9c3r0000gn/T/walstream-producers.WnvGPV`.

Verification so far: formatting and Clippy pass. `cargo test --all-targets` passes 92 library, 1 CLI, 3 protocol-boundary, and 2 stock-client tests; the separate S3 test passed on RustFS, SeaweedFS, and MinIO through the existing harness. Maintenance passed with the preserved schema-3 baseline executable `/tmp/walstream-producers-schema3`, including schema 1/2/3 conversion, record-byte preservation, old-binary rejection, and unchanged schema-3 adoption time. Retained Java/librdkafka consumer-group recovery passed. Logs: `/tmp/walstream-producer-{tests,clippy,maintenance,backends,groups}.log`. Full 25,000-append index walkthrough passed: root 11,825 bytes, level-2 index, complete replacement readback and next offset 25,000, legacy-byte preservation, pre-publication crash, and corrupt-page rejection (`/tmp/walstream-producer-index.log`).

Implementation decisions: InitProducerId v0–4 supports null transactional IDs; transactions fail explicitly. Producer pages use 64-entry copy-on-write B+tree pages, levels 0–12, and five-entry histories with normalized-record SHA-256 fingerprints. Epochs increase without wrapping; sequence advancement uses modulo 2^31. A complete partition request is decided before writes and publishes once, preserving native batches and allowing duplicate prefixes followed by new data. More-than-five-batch replay can fail once its earliest identity leaves the window, as documented. Maintenance traces all producer pages, preserves the directory through trims, and counts it under the existing object budget.

Meaningful new tests cover concurrent allocation and same/different producers, ambiguous initialization/publication, exact duplicate no-write behavior, whole-request validation, epoch fencing, sequence wrap, five-batch eviction, 2,100 producer IDs through an internal split, full expiry, stale paused writers/readers versus GC, and corrupt/missing referenced state. Initial failures were obsolete no-idempotence/schema assertions and a generated default request fixture using an empty transactional ID; valid nontransactional fixtures now set it to null explicitly. No criteria were weakened.

Early bounded design review: completed read-only by Sol; publication, schema preservation, and complete producer-graph tracing invariants incorporated. It does not substitute for final review.
Independent runtime: passed on `6603e08`. Fresh Sol reviewer ran `container system status && scripts/test-idempotent-producers.sh`, including the release build and all four native lost-ack replacement/retention cases. It independently observed IDs 0–3, epoch/sequence zero, identical payload hashes, original offset zero, subsequent offset one, exact retained readback, and matching pre/post-retry root hashes. Independent evidence: `/var/folders/ww/y0g67mbx2mn0s9rt57gy9c3r0000gn/T/walstream-producers.zJVf57`. Owned containers/processes were cleaned up.
Final review: **no material blocking findings**, at implementation `6603e08479fd3053e4c6f4668885ac3760f04dcb`. Reviewer inspected the full diff, state/append/protocol/maintenance paths, migration, docs, tests, and runtime evidence. Simplicity and test quality passed: the tree and single-root CAS were judged proportionate; new tests protect consequential invariants. Git state after review contained only the previously prepared goal evidence update. No repairs were required.
PR: [#6](https://github.com/smarzola/walstream/pull/6), opened after goal commit `2128fae`.
Goal status: implementation and all local verification/review criteria achieved. Both push and PR CI checks passed on `6603e08`; publish this final evidence record and verify its checks before the ready transition. GitHub is the authoritative delivery-status record; no merge or release is authorized.

On resume, reconcile this contract with current git/PR state and actual evidence. Continue unfinished work without weakening criteria or repeating valid checks. Record completed behavior, material decisions, commands/results, runtime records, and review disposition here; commit only explicit in-scope paths. Completion requires all success criteria and verified ready-PR delivery.
