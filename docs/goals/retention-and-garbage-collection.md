# Goal: Retention and Safe Garbage Collection

Repository: `/Users/smarzola/projects/walstream`
Source of truth: this contract and the request to pursue the next milestone after PR #4.
Revision: 1, prepared 2026-09-06.
Approval: **Approved** revision 1 on 2026-09-06. The user replied “Approved” to the concrete retention and garbage collection plan and its defaults.
Execution: approved; implementation in progress.
Goal-file commit policy: **Commit after approval.** `git ls-files docs/goals` and merged history show two comparable committed goal contracts, including `scalable-partition-log-metadata.md` in PR #4.
PR delivery: GitHub `smarzola/walstream`, base `main`, proposed branch `feat/retention-garbage-collection`. Open a draft after approval and keep it draft through implementation and independent review; finish with a verified ready PR.

## Outcome and delivery plan

Walstream currently retains every record and every abandoned or superseded object. Add an operator-run maintenance command that previews or applies age/size retention and reclaims unreachable record/index objects while the broker remains available.

For example, if maintenance removes complete batches covering offsets 0–99, the earliest readable offset becomes 100. Existing offsets 100 onward keep their values, the next append continues from the previous log end, and a consumer requesting offset 50 receives `OFFSET_OUT_OF_RANGE`. Retention does not rewrite committed consumer offsets or wait for slow consumers.

| Step | Deliverable | Success condition |
|---|---|---|
| 1 | Retained ranges and compatible storage metadata | A root can represent a nonzero earliest offset or an empty retained log; reads, appends, ListOffsets, and existing data remain correct after replacement |
| 2 | Explicit maintenance with safe reclamation | Preview writes nothing; apply publishes retention atomically and deletes only proven unreachable objects from the selected partition; interrupted and competing operations cannot resurrect or lose retained data |
| 3 | Runtime proof and ready PR | Both implementer and fresh reviewer exercise live maintenance, reader/writer races, and restart recovery; required local/backend/client checks and published CI pass before the PR becomes ready |

## Decisions for approval

| Decision | Proposed choice | Reason and tradeoff | Alternative |
|---|---|---|---|
| How maintenance runs | `walstream maintain` targets one explicit topic/partition. Preview is the default; `--apply` permits retention and collection. Limits are supplied per invocation; omitting both limits collects only unreachable objects | Operators control when data expires. The command can run alongside the broker, but limits are enforced only when invoked | A periodic broker task would enforce policy automatically, with persistent policy settings and an additional lifecycle |
| Retention units | Remove the oldest contiguous prefix of whole record batches. Apply age and retained-record-byte limits together; all records may expire. Byte limits count encoded record objects, not index overhead or orphan storage | Preserves canonical batches and offset continuity. A single batch larger than the byte limit can be removed entirely; this is a retention target at publication, not a quota on future writes | Preserve a newest batch even when it exceeds the limit, or rewrite batches for record-level trimming |
| Age meaning | Use broker receive time recorded for new batches, independent of client record timestamps. Existing batches without that time use a single persisted adoption time when their partition first upgrades | Avoids unexpectedly expiring historical data whose producer timestamps are old. Existing data receives a full age window after upgrade; size retention can still remove it immediately | Use the newest client record timestamp in each batch, which allows late data to expire soon after arrival |
| Concurrency | Publish retention with an exact-root conditional update that also changes a unique fence revision. Trace the published root completely before deleting from a pre-fence inventory. Restart stale reads, writes, and collector traces on a missing object only when their exact root version changed | Protects publication without durable reader pins, leases, or a maintenance outage. Overlapping reads can restart or return an out-of-range result; repeated interference can exhaust a bounded retry budget | Require an offline maintenance window, or persist reader pins and coordinate their expiry |
| Durable format | Introduce schema 3 for the retained start, publication revision, and age metadata. Read schemas 1 and 2; upgrade on the next successful append or maintenance apply without rewriting record objects | Preserves existing data but creates another one-way format boundary. Old binaries cannot serve upgraded partitions; stop them before deploying the new writer | Keep a separate retention sidecar, which would split authoritative state across objects |

The user approved revision 1 with the defaults above, including explicit maintenance and broker receive time.

## Baseline, scope, and constraints

Inspected baseline: clean `main` at `c0647b9748b8b1d641aab7b9026f242cf4d01ad3`, matching GitHub. PR #4 is merged, both previous goals are complete, and there is no open PR for this work. No repository `AGENTS.md` was found in the applicable ancestry.

- `src/log/index.rs::Root` uses schema 2, starts at offset zero, and equates root revision to retained segment count. Pages and active tails are bounded to 64 entries; bodies are bounded to 4 MiB, with 11 page levels.
- `src/log.rs::append` creates immutable UUID objects and commits only by root CAS. A lost race retries; a missing page during preparation currently returns an error directly.
- `fetch_bounded` reads one root snapshot and then referenced pages/records. It has no recovery path for objects removed after that snapshot. `offsets` hard-codes earliest zero.
- Segment descriptors contain lengths and checksums but no broker receive time. Index traversal validates complete-batch ranges and requires full subtrees except the right edge.
- `src/main.rs` exposes only `serve` and `verify-store`. There is no maintenance scheduler, retention policy, or object collector.
- ListOffsets already reads `OffsetRange.earliest`; Fetch has an out-of-range error path. Durable group offsets live separately and must retain their current semantics.

Scope: retained-range metadata, age metadata, affected append/fetch/error handling, maintenance CLI/API, exact partition-scoped reclamation, compatibility documentation, and focused proofs. Preserve the one-broker interface, S3-only durable dependency, independent-writer CAS correctness, existing wire versions, immutable record objects, and bounded ordinary request paths.

The maintenance process may scan the selected partition and use disposable local scratch. Bound inventory size, graph traversal, memory, retries, and deletion batches. Inventory or planning budget exhaustion must stop before root publication. A failure during post-publication validation must stop deletion and report the committed state accurately. Ordinary append/fetch/offset operations must keep their bounded index access and must not gain a listing requirement.

Maintenance requires listing and deletion privileges on the selected partition's data/index prefixes in addition to normal reads/writes. Never delete the partition root, topic metadata, consumer offsets, unknown filenames, unrelated topics/partitions/clusters, or objects reachable from the published maintenance root. Use ordinary object deletion; purging noncurrent versions in versioned buckets is outside this feature and must not be claimed as reclaimed storage.

No automatic background scheduling, topic administration API, record compaction, compression, producer idempotence, multi-broker coordination, authentication, or TLS changes. This goal does not change slow-consumer retention into a promise to preserve their committed positions.

Protected work: none observed. Preserve unrelated changes that appear later.
Implementation choices still open: exact schema fields and CLI spelling, scan-budget defaults, and index trimming/rebuilding strategy. A bounded streaming rebuild during explicit maintenance is acceptable; ordinary append/fetch must retain the previous milestone's scaling properties. These choices must preserve the approved behavior above.

## Publication and deletion contract

The following ordering is required; an object that looks old or unreferenced during a live scan is not sufficient evidence for deletion.

1. Complete a bounded inventory of recognized immutable UUID keys under the selected partition's `segments/` and `index/` prefixes. Deduplicate it. Keys omitted by listing remain for a later pass; never add newly discovered keys after the fence.
2. Load and validate the latest root and complete the bounded planning traversal. Compute the retained whole-batch prefix boundary and prepare any replacement index pages. Retained ranges must be contiguous, `0 <= start_offset <= next_offset`, and `start_offset` must never decrease. Preserve the exclusive next offset even when retention empties the log. Separate publication revision semantics from retained segment count.
3. CAS the complete desired root, including a fresh fence revision, against the exact loaded root version. The fence must change the stored root even for collection without retention. Every writer and maintenance operation participates in this root version protocol; never delete or recreate the root. A failed or ambiguous attempt authorizes no deletion; reload and restart safely.
4. Trace the exact root that won the CAS, including every reachable index page and retained record key. Validate the complete live graph before deleting anything. A malformed or incomplete trace aborts deletion. If another collector made the trace stale, restart from a new bounded inventory/root attempt.
5. Delete only inventory keys outside that complete live set. Check every deletion result; an already absent candidate is harmless, while other errors produce a nonzero result with accurate partial-progress reporting. A crash after publication or during deletion is safe to resume by running maintenance again.

All immutable keys are permanently unique and created conditionally. After a root-CAS failure or a fence change, an operation must not publish an earlier unreferenced tentative object; create fresh tentative objects for the new attempt. Fresh operations may reference only objects reachable from their loaded root or fresh objects created for that attempt. These rules prevent a paused writer or collector from resurrecting a key already selected for deletion.

Fetch, append preparation, and collector tracing retain the exact root version for each attempt. On a missing referenced page/record, reload the root. A changed version permits a complete bounded retry, discarding partial results; an unchanged version is a storage integrity failure. An expired requested offset returns `OFFSET_OUT_OF_RANGE`. Do not turn checksum failures, malformed data, or arbitrary transport errors into retention retries. A successful Fetch must describe one coherent root snapshot.

The fence relies on the existing conditional-write contract: [S3 rejects an If-Match update when the ETag has changed](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html). This protocol is the proposed application-level use of that guarantee, not an S3 multi-object transaction.

## Success criteria

1. Preview reports the selected partition, old/proposed readable ranges, effective age/byte limits, retained and removable object counts/bytes, and whether format adoption is needed. It creates, changes, and deletes no objects, including when the topic is absent or metadata is invalid.
2. Applying age, byte, or combined retention advances the earliest offset only across complete batches. Remaining record bytes/offsets are unchanged; the log end never goes backward. Empty retained logs, subsequent appends, ListOffsets, expired Fetch positions, and process replacement behave correctly.
3. Age retention uses the approved clock semantics, including legacy adoption, stable per-batch receive time across a write attempt's retries, and explicit time-boundary tests. Inputs are validated before writes. Clock-dependent retention is separate from GC safety, which must not rely on a grace period or clock synchronization.
4. Collection removes unreachable record objects, superseded pages, and eligible expired objects while preserving every retained record, live page, and unrelated key. A pass that encounters malformed live metadata deletes nothing. Retry and partial-delete behavior is observable and idempotent.
5. Deterministic barriers prove safety with paused writers before CAS, writers reading a deleted old index page, readers paused before page/record GET, failed and ambiguous fence CAS, concurrent collectors, and crashes before/after publication and during deletion. Under quiescence, repeated maintenance completes the work; under sustained interference it returns a bounded retry/error result without corrupting data.
6. Schema-1/2 logs remain readable and upgrade without record rewriting. Older executables reject schema 3. Age-based retention does not immediately expire legacy batches on adoption; size-based retention applies as requested. Normal operations preserve bounded page/body limits and targeted lookup without bucket scans.
7. Run repeated append–maintain–read cycles across index boundaries on disposable S3 storage. Prove the retained range and values after replacement, inspect actual removed/preserved keys, and measure visible object counts/bytes before and after cleanup. Include the former 25,000-append growth regression. Do not describe logical deletion as purging historical bucket versions.
8. Required Rust, shell, backend, and retained-client checks pass. Both implementer and fresh final reviewer personally build, launch, and exercise the changed broker/maintenance behavior and record separate evidence. Independent review reports no material blocking findings, including avoidable complexity or ineffective tests.
9. Push coherent checkpoints to an early draft PR. After verification and independent review, verify the published head and available required checks, update the PR description with actual results, mark it ready, and confirm readiness and any newly triggered required checks.

## Authority, review, and delivery

Approval evidence: the user replied “Approved” on 2026-09-06 after the revision 1 presentation, authorizing implementation with its deletion, clock, migration, and draft-to-ready PR choices.

Once approved, proceed with in-scope implementation, disposable runtime mutations, focused Conventional Commits, the goal-file commits allowed by the inspected precedent, and the updated draft-to-ready PR workflow. Stage explicit paths and inspect staged changes. The earlier instruction to merge concerned PR #4; no merge or release of this next PR is authorized yet.

Use the reloaded `$pursue-goal` execution mechanics and `$goal-loop-prompt` writing policy. Keep code direct, reuse existing dependencies when sufficient, preserve meaningful tests, and write documentation around operator tasks and verified behavior. Do not copy the workflow into each checkpoint.

Early review: a bounded Sol design review was justified by the irreversible deletion boundary. It found the protocol viable with explicit corrections now included above: publish the retained root before tracing/deletion, retry stale writer preparations as well as reads, compare exact root versions, restart failed fences safely, trace the full live graph, prohibit key reuse/resurrection, and preserve monotonic retained ranges. This design review is not final acceptance.

Final review: one new independent Sol reviewer (`gpt-5.6-sol`) with no inherited conversation, after implementation and verification. Provide the approved goal, real diff, raw evidence, and runtime setup. Freeze tracked source during review; allow reviewer-owned scratch and disposable data. Review correctness, completeness, simplicity, and test quality together. Disclose a model substitution if required; missing independent review remains an unmet criterion.

## Milestones and verification

Starting branch/base: `main` at `c0647b9748b8b1d641aab7b9026f242cf4d01ad3`.

- [ ] Retained-range metadata, age semantics, migration, and stale-snapshot retry behavior verified.
- [ ] Preview/apply maintenance and deletion/failure invariants verified, including measured storage cleanup.
- [ ] Full runtime/regression evidence, independent final review, and ready PR completed.

Commit and push coherent verified milestones, recording concise evidence and material choices. Reuse valid evidence until a code/input/environment change or finding invalidates it; do not rerun passing checks merely at a checkpoint boundary.

Existing regression commands, from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
sh -n scripts/test-log-index.sh scripts/test-s3-e2e.sh scripts/test-consumer-group-clients.sh
./scripts/test-log-index.sh
./scripts/test-consumer-group-clients.sh
WALSTREAM_E2E_BACKEND=rustfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=seaweedfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=minio ./scripts/test-s3-e2e.sh
```

Planned additions: focused retained-range, age/byte boundary, CLI preview, inventory, fence, live-trace, retry, and partial-delete tests; a real maintenance walkthrough. These commands do not exist yet. Existing index probes may need schema-neutral assertions while preserving their behavior and migration protections; do not weaken assertions that expose defects.

## Hands-on runtime acceptance

Each role independently builds with `cargo build --release`, starts the real broker against an owned disposable RustFS bucket/prefix, sends Kafka requests, invokes the actual maintenance executable, and inspects object state. Use the existing Apple Container workflow; confirm/start the service as needed. Fixtures, clocks, barriers, and proxy fault points must make timing claims observable. Clean up only owned processes, containers, and scratch.

| Scenario | Actions | Required observation |
|---|---|---|
| Preview and apply | Produce distinguishable batches across index boundaries; preview and apply age/size limits | Preview has zero writes; apply reports and preserves the exact retained range and removes only authorized keys |
| Empty log and replacement | Expire all batches, replace the process, append again | Earliest equals latest after expiry; offsets are never reused; the new record is readable |
| Live read/write races | Pause a reader or writer at an observed object/root boundary while maintenance applies | Retained data stays correct; stale attempts restart coherently or report an expired offset; a paused stale writer cannot resurrect deleted objects |
| Failure and resume | Interrupt maintenance before root publication and during deletion; repeat it | No pre-publication deletion; durable range remains valid; rerun completes partial reclamation |
| Legacy data and corruption | Adopt schema-1/2 fixtures; test clock behavior and old-binary rejection; corrupt a separate live graph | Legacy bytes stay unchanged, age adoption is conservative, old binaries fail closed, and corrupt live metadata prevents deletion |

Implementer runtime: pending.
Independent reviewer runtime: pending.
Final review: pending.
PR: not created; draft creation follows approval.
Goal status: revision 1 approved; implementation in progress.

On resume, reconcile this goal, actual approval and preference answers, applicable instructions, git state/history, and PR status. Continue unfinished work without weakening criteria. Completion requires the verified outcomes, both runtime records, a clean material review, and the ready-PR delivery state.
