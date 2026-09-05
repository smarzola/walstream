# Goal: Scalable Partition Log Metadata

Repository: `/Users/smarzola/projects/walstream`
Source of truth: this contract and the 2026-09-05 conversation recommending scalable log metadata as the next implementation milestone.
Revision: 1, prepared 2026-09-05.
Approval: **Approved** revision 1 on 2026-09-05. The user replied “Approved” to the revision-1 presentation, including automatic migration on the first successful append and the old-binary downgrade boundary.
Execution: approved local implementation, verification, both runtime walkthroughs, independent review, and focused commits completed on 2026-09-06.

## Outcome And Delivery Plan

A partition can continue accepting appends beyond the current 10,000-segment ceiling while the metadata used for each append or bounded fetch stays proportional to the index depth and requested data, rather than the partition's entire history. All acknowledged data remains recoverable from object storage alone.

Before: every append reads and rewrites a growing JSON list and eventually fails at the segment or manifest-byte limit. After: bounded immutable index pages locate segment ranges, and a small versioned root is the sole conditional-update commit point.

| Step | Deliverable | Success condition |
|---|---|---|
| 1 | Versioned root, bounded index pages, and legacy migration integrated into the log engine | Existing schema-v1 logs remain readable; their next successful append atomically publishes the new index without rewriting record objects; new logs append and fetch across page and tree-level boundaries |
| 2 | Proven growth, concurrency, and failure behavior | At least 25,000 separate appends succeed on one partition; targeted reads avoid whole-history scans; concurrent publication, interrupted upgrades, rollover failures, and corrupt metadata preserve the required invariants |
| 3 | Real-broker recovery, compatibility documentation, and final verification | Actual broker/client runs prove growth and recovery against disposable S3 storage; existing client/backend regressions pass; implementer and fresh reviewer independently complete runtime acceptance |

## Decisions For Human Review

| Decision and origin | Proposed choice | Why and tradeoff | Alternative |
|---|---|---|---|
| Index shape: agent-selected implementation of the recommended bounded pages | A bounded-fanout tree of immutable pages indexed by offset ranges, published through the existing partition `manifest.json` as a small schema-v2 root; allow a bounded active tail if it simplifies append costs | Supports targeted reads and bounded path updates. Adds metadata objects and page validation; page capacity and encoding remain routine implementation choices | A linked page chain is simpler to append but makes historical lookup linear; a flat root directory eventually recreates the growing-manifest limit |
| Upgrade behavior: agent-selected compatibility choice | Read schema v1 and v2; use v2 for new partitions; upgrade a legacy partition on its next successful append by one root CAS, referencing unchanged record objects | Preserves readable records and offsets without an offline conversion step. Once upgraded, the partition cannot be served by old binaries; a downgrade or mixed-version rolling deployment is not supported | A separate explicit migration command provides operator control but adds an operational step and a second write workflow |
| Publication: existing architectural constraint | Create record and index objects before publishing their root reference; acknowledge only after the root CAS succeeds | Preserves independent-writer serialization and S3-only recovery. Failed attempts can leave invisible objects, as today | A separate database, durable local state, or writer lease would change the project's architecture |
| Delivery scope: interpretation of the recommended next milestone | Scalable metadata, its migration, meaningful proofs, and documentation | Keeps the change focused. Stored data and orphan objects still grow until a later retention/collection milestone | Combining retention and deletion now adds a separate lifecycle and concurrency contract |

## Baseline, Scope, And Constraints

Inspected on 2026-09-05:

- Starting branch `main`, commit `3f881b34408a54efc00649768d162e32d87a383c` (`feat: add parallel consumer groups (#3)`). Working tree was clean before this goal file was added. Remote: `git@github.com:smarzola/walstream.git`.
- The existing `docs/goals/multi-partition-consumer-groups.md` is complete. This is a separate next goal.
- `src/log.rs::append` loads and clones the full manifest, creates one immutable record-batch object, and conditionally publishes the next manifest. It rejects 10,000 existing segment descriptors or a serialized manifest above 4 MiB.
- `src/log.rs::fetch_bounded` loads the full manifest and walks segment descriptors to locate a requested offset. It uses descriptor lengths before segment downloads and preserves the oversized-first-batch rule.
- Schema-v1 validation enforces revision/segment-count agreement, contiguous offsets, partition-scoped objects, bounded record and byte counts, and checksums. `offsets` returns earliest zero and the manifest's exclusive next offset.
- Legacy topic inference also reads partition-0 manifests. Consumer offsets are separately stored in `groups/<group-id>/offsets.json`; group membership remains process-local.
- `.github/workflows/ci.yml` runs formatting, Clippy, and all-target tests. `scripts/test-s3-e2e.sh` covers real binary replacement and independent writers on RustFS, SeaweedFS, and MinIO. `scripts/test-consumer-group-clients.sh` covers retained Java/librdkafka consumers across replacement.

Scope: the durable log metadata/read/write path, existing-data migration, affected topic discovery and protocol integration, tests/runtime tooling needed to prove it, and accurate documentation.

Constraints:

- Preserve one virtual broker, the object-store conditional-write contract, current supported Kafka versions and semantics, per-partition independent offsets, durable topic counts, and committed consumer offsets.
- Keep local memory and disk disposable. Ordinary explicit-topic append/fetch/offset operations must not gain a ListBucket requirement.
- Bound every root/page body, decoded collection, traversal depth, and selected-data allocation. Preserve existing record validation and fetch payload bounds. A future schema, malformed range, missing referenced page, checksum mismatch, or invalid namespace must fail closed when accessed.
- Preserve schema-v1 reads, including legacy partition-0 topics without topic metadata. Validate the legacy manifest before conversion. A read alone does not rewrite the partition manifest. Old record objects remain unchanged.
- Ordinary post-upgrade append and bounded fetch must not enumerate or validate every historical page. Validate loaded pages and parent/child range and integrity relationships as they are traversed; this is not a whole-bucket integrity audit.
- No retention, orphan deletion, record compaction, batching/caching/metrics project, compression, idempotent producers, additional brokers, admin APIs, authentication, or TLS changes in this goal.
- Use existing dependencies and direct code where sufficient. Add an abstraction, dependency, or operator setting only for a demonstrated need in this outcome.

Protected work: none observed. Preserve any unrelated changes introduced after this baseline.
Bounded unknowns: exact page capacity, tree growth representation, and runtime probe packaging. Resolve with a small implementation and measured metadata bounds, record the choices, and retain the agreed outcomes and migration behavior.

## Success Criteria

1. One partition accepts at least 25,000 separate successful appends, with unique contiguous acknowledged offset ranges, and a fresh engine/broker reads the exact expected record sequence. The proof counts append operations, not merely records inside a few batches.
2. Root and index pages have documented fixed byte/entry bounds and traversal limits. Post-upgrade append reads/writes only a bounded update path; bounded fetch seeks by offset and reads only index paths and relevant leaf pages. Recorded object-operation/byte evidence at different history sizes demonstrates that neither operation loads the full history. ListOffsets obtains the range from the root.
3. Tests exercise leaf rollover and at least one internal tree-level growth boundary through real append/fetch behavior. Reads at the start, middle, page boundaries, tail, empty log, and invalid offsets preserve existing semantics and fetch budgets.
4. Valid schema-v1 logs, including one at the old segment ceiling, can be read and then appended through atomic upgrade without segment rewrites, offset changes, or lost topic/group state. Old binaries reject the resulting schema-v2 state, and documentation explains the downgrade boundary.
5. Concurrent independent writers and fresh-process recovery preserve every acknowledged append. Deterministic boundary failures cover object creation before root publication, a lost root-CAS race, interrupted migration, and rollover. Unpublished objects are invisible. A lost response after commit retains the existing non-idempotent retry semantics.
6. Corrupt, oversized, out-of-scope, cyclic/depth-invalid, or inconsistent accessed metadata cannot produce successful fabricated reads, silent history truncation, or unbounded allocation/traversal. Existing batch integrity protection remains in force.
7. The real-client retained-consumer proof and all three existing S3-compatible backend variants pass on the changed implementation. Existing logs, durable offsets, and multi-partition behavior remain compatible.
8. Documentation describes the committed layout, limits, migration, failure semantics, and ongoing orphan/storage growth accurately. Required automated verification passes.
9. Both implementer and independent reviewer personally build, launch, and exercise the changed software using disposable data and record their own runtime evidence. Final independent review reports no material blocking correctness, completeness, simplicity, or test-quality findings.

## Approval And Authority

Approval evidence: the user replied “Approved” immediately after the revision-1 presentation and explicit migration approval question on 2026-09-05.

Approval of this plan and execution covers in-scope local implementation, the required format-upgrade scenarios on disposable data, verification, independent review, and focused Conventional Commits. Routine page encoding, test arrangement, helper names, and similar choices do not require renewed approval.

No external publication has been authorized: pushing, opening/merging a PR, or releasing is outside the current delivery scope. Do not operate on production buckets or unrelated user data.

A material change to the target, durable-data compatibility, architecture, or scope requires approval before dependent work. Continue unaffected authorized work.

## Engineering, Review, And Checkpoints

Use `$pursue-goal` for execution mechanics. Keep the main agent as the source-code writer. Both implementer and reviewer should challenge unnecessary concepts, branches, dependencies, and ineffective tests while preserving required behavior and error handling.

Review policy: one fresh final reviewer using Sol (`gpt-5.6-sol`) with no inherited conversation. Provide this approved goal, base/current diff, raw verification evidence, and runtime instructions. The reviewer independently launches the software and protects tracked source and git state. Disclose any available independent-model substitution; unavailable independent review remains an unmet criterion. No early review is currently planned.

Starting branch/base: `main` at `3f881b34408a54efc00649768d162e32d87a383c`.
Planned implementation branch: `feat/scalable-partition-log-metadata`.

- [x] Milestone 1: integrated bounded index and schema-v1 compatibility; focused log/protocol/stock-client checks and initial runtime use pass.
- [x] Milestone 2: growth, targeted lookup, migration ceiling, concurrency, and failure criteria pass; record observed page bounds and metadata operation evidence.
- [x] Milestone 3: real-broker scenarios, relevant full regressions, and documentation pass.

After a coherent milestone passes its checks and applicable runtime acceptance, record evidence and decisions here and commit it with a focused Conventional Commit. Record the hash in a later note/report. Final independent review and both runtime records remain separate completion gates.

Checkpoint evidence, 2026-09-05:

- Milestones 1–2: schema-2 root, 64-entry active tail/leaves/branches, 11-level traversal ceiling, shared bounded deserialization and legacy validation implemented. Kept the existing 4 MiB metadata-body cap; normal roots remain much smaller. Index pages and data objects remain immutable, and the root ETag update is still the sole publication point.
- `cargo test --lib log:: -- --nocapture`: 24 passed. After adding rollover operation accounting and a missing-page case, `cargo test --lib log::index:: -- --nocapture`: 5 passed. Growth fixture completed 25,000 appends and exact complete readback. Historical seek used 4 GETs/14,838 bytes at 129 appends, 5 GETs/30,714 bytes at 8,193, and 5 GETs/40,479 bytes at 25,000. A rollover at 25,024 prior appends used 4 metadata GETs, 4 metadata PUTs, and 17,601 bytes read; an ordinary append used 2 GETs and 1 PUT. ListOffsets used 2 metadata GETs (topic and root).
- The full 10,000-segment legacy fixture upgraded and all original record bytes remained unchanged. Deterministic tests covered concurrent migration/rollover, failure before page/root publication, committed-but-lost response, malformed references, corrupt and missing pages, and bounds.
- `cargo test --test kafka_protocol --test stock_client`: 3 protocol and 2 stock-client tests passed outside the sandbox. The initial sandbox run could not bind sockets; no code repair was needed. Initial dependency fetch also required network access outside the sandbox.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed for the initial integrated implementation. Full final checks remain below.
- Initial implementer runtime: `./scripts/test-log-index.sh --appends 129` built release code and launched five real broker processes against owned RustFS container `walstream-index-1788645415-63366`. All scenarios passed: root 594 bytes, root index page 587 bytes, full replacement readback, schema-1 read unchanged then schema-2 append with unchanged record objects, proxy-intercepted root PUT followed by hard process loss and offset-64 recovery, and corrupt-page error through Kafka Fetch. The helper removed its container; output is in `/tmp/walstream-index-smoke.log`. The full-size walkthrough and independent runtime are still pending.

## Automated Verification

Work in `/Users/smarzola/projects/walstream`. Existing commands:

```bash
cargo test --lib log::tests
cargo test --test kafka_protocol --test stock_client
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
sh -n scripts/test-consumer-group-clients.sh scripts/test-s3-e2e.sh
container system status
./scripts/test-consumer-group-clients.sh
WALSTREAM_E2E_BACKEND=rustfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=seaweedfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=minio ./scripts/test-s3-e2e.sh
```

Proposed additions, not existing commands: focused tests for criteria 1–6 and a runnable real-broker growth/migration/recovery probe. Record exact invocation and evidence once implemented. Reuse existing pinned clients and runtime setup where practical; do not add a general benchmarking framework.

Prerequisites: Rust toolchain, Apple Container, `container-compose`, and `jq`; the existing pinned client harness provisions its client dependencies. Check service availability before runtime use and start it if needed. Give every new runtime exercise unique owned processes/containers and disposable bucket/prefix state, and clean up only its own resources.

Rerun passed checks only when changed code, inputs, environment, findings, or stale evidence invalidate them. Repair in-scope failures; evidence of an unrelated pre-existing failure cannot excuse a failed goal criterion.

## Hands-On Runtime Acceptance

Applicable because the durable storage and running broker behavior change. Each role independently builds the code under review with `cargo build --release`, records the commit and relevant working diff, starts the actual `target/release/walstream serve` against a disposable RustFS bucket/prefix using the existing development flags, and sends real Kafka requests with a compatible client. The executable's `verify-store` and observable object state supplement wire results.

The planned runtime helper may provision data and drive requests, but running a test suite, merely starting the server, or reading another role's output is not acceptance. Each role personally invokes the broker/probe workflow, inspects returned offsets and data, and inspects representative persisted roots/pages and record objects.

| Scenario | Actions | Expected observation |
|---|---|---|
| Long-running partition | Make 25,000 separate appends to one partition, read the complete sequence, and issue bounded reads around rollover boundaries and near the tail | Every acknowledged range is contiguous; contents match; root/page sizes remain within declared bounds and targeted reads avoid historical scans |
| Complete broker replacement | Stop the real broker process after committed growth, start a fresh process against the same prefix, fetch old data and append again | Stored history and latest offset survive; next append uses the next exclusive offset |
| Legacy upgrade | Generate disposable v1 data using the baseline binary or a faithful fixture with real record objects, read it with the new broker, append, replace the broker, and read across the old/new boundary | Reads alone leave the partition manifest at v1; the append publishes v2; record-object bytes and old offsets stay intact; new records remain readable after restart |
| Failed publication or corrupt page | Use disposable data to exercise a controlled pre-publication interruption and corrupt one referenced index page on a separate prefix | Unpublished records stay invisible; a fresh process recovers the committed root; fetching the corrupted referenced range returns an error rather than fabricated/truncated success |

Use deterministic storage-boundary fault tests for exact commit interleavings; a process kill without evidence of its timing is not proof of a particular pre/post-CAS boundary. The retained Java/librdkafka group workflow remains a separate required regression.

Implementer runtime record: passed on 2026-09-06. Ran `./scripts/test-log-index.sh --baseline-broker /tmp/walstream-index-baseline-src/target/release/walstream` against the release build of implementation commit `fd5bfb3`. The concurrent working diff changed only the group-capacity test fixture and subsequent documentation; executable behavior was unchanged. The script created owned RustFS container `walstream-index-1788645651-68693` and prefix `walkthrough-1788645651-68693/clusters/index`, launched actual brokers (PIDs 68936, 69426, 69442, 69445, 69446), sent separate Kafka Produce/Fetch requests, and inspected S3 objects. All 25,000 appends and exact full readbacks passed before and after process replacement; root size was 10,550 bytes, tree level 2, and its page 2,038 bytes. Appending after replacement returned offset 25,000. The v1 fixture stayed v1 on read, converted on append, preserved original record bytes, and survived another replacement. An actual baseline executable built from `3f881b3` (PID 69443) rejected the upgraded partition. The proxy intercepted a root PUT after record/index objects were uploaded but before upstream publication; the broker was killed, and a fresh process recovered offset 64 and appended there. Corrupt referenced metadata returned a Kafka Fetch error. The helper cleaned up its owned resources. Raw output: `/tmp/walstream-index-full.log`.
Reviewer runtime record: passed independently on 2026-09-06 at `a7e94262597dcf8fb1dde8f577f30779d6ba89eb`. Fresh Sol reviewer invoked `script -q /tmp/walstream-review-runtime.log ./scripts/test-log-index.sh --baseline-broker /tmp/walstream-index-baseline-src/target/release/walstream`, built release code, and launched actual brokers (PIDs 70139, 70412, 70415, 70418, 70419) against its own RustFS container `walstream-index-1788645971-70006`. It observed 25,000 separate acknowledged appends, exact full readback, root 10,550 bytes/tree level 2/page 2,038 bytes, replacement recovery and next offset 25,000, unchanged v1 reads followed by migration preserving record objects and replacement readback, actual old-binary rejection, pre-publication interception followed by hard process loss and offset-64 recovery, and a Kafka Fetch error for corrupt referenced metadata. The reviewer independently compared the baseline source byte-for-byte with a git archive of base `3f881b3`. Runtime exited 0; owned containers and broker/proxy processes were gone afterward. It also ran `cargo test --lib log::` (24 passed in 12.23 seconds), recording `/tmp/walstream-review-focused-tests.log`. Git remained clean at the exact reviewed head.

## Final Regression Evidence

- Implementation checkpoint: `fd5bfb3` (`feat: index partition logs with bounded immutable pages`).
- `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, and shell syntax checks passed. Counts: 69 library, 1 binary, 3 protocol, and 2 stock-client tests; the script-gated S3 test is ignored only in the hermetic suite. Raw output: `/tmp/walstream-final-checks-2.log`.
- The initial full suite failed the unchanged `bounds_and_reclaims_group_slots` fixture: filling 10,000 slots exceeded its 30-second session duration under concurrent load, so early slots expired (7,023 retained after a 32.82-second run). The fixture now uses the existing 300-second maximum for its population phase; every capacity, rejection, leave, and reclamation assertion is preserved, and production coordinator code is unchanged. The rerun passed. Original output: `/tmp/walstream-final-checks.log`.
- `./scripts/test-consumer-group-clients.sh`: both pinned librdkafka 2.12.1 and Kafka Java 4.2.0 passed the three-partition/two-member split, survivor reassignment, retained-client replacement, and exact offset resume. Raw output: `/tmp/walstream-client-recovery.log`.
- `WALSTREAM_E2E_BACKEND={rustfs,seaweedfs,minio} ./scripts/test-s3-e2e.sh`: each passed the live binary test, now including two leaf rollovers, exact indexed-history readback, and a further replacement process. Raw outputs: `/tmp/walstream-s3-rustfs.log`, `/tmp/walstream-s3-seaweedfs.log`, `/tmp/walstream-s3-minio.log`.
- Final independent review and reviewer-owned runtime acceptance both passed; see their separate records.

## Decision And Status Notes

- 2026-09-05: Inspected current source, protocol integration, goal history, scripts, CI, branch, and clean baseline. Prepared revision 1; implementation has not started.
- 2026-09-05: Verified `cargo`, `container`, `container-compose`, and `jq` are installed. The sandbox cannot query the container service; an outside-sandbox `container system status` reports that the API server is not running or registered with launchd. Start the service for approved runtime work. No runtime acceptance has run. Read back the goal artifact; its whitespace check emitted no diagnostics.
- 2026-09-05: User approved revision 1, including automatic schema-v2 migration. Created implementation branch `feat/scalable-partition-log-metadata`.

## Resume And Final Status

On resume, read this goal, actual approval/decision evidence, applicable instructions, git state, and relevant commits. Continue unfinished work without repeating valid checkpoints or silently weakening criteria.

Final independent review: passed on 2026-09-06. One fresh Sol (`gpt-5.6-sol`) reviewer inspected the entire base `3f881b3` through head `a7e9426` diff, affected execution paths, tests, documentation, and raw evidence. Verdict: **no material blocking findings**. Correctness, completeness, simplicity, and test quality were assessed together; no material unnecessary complexity or ineffective tests were found. No review repair round was required. The only subsequent change is this completion/evidence record, so executable and runtime evidence remain valid.
Goal status: **achieved on 2026-09-06**. All nine success criteria, milestone checks, regression checks, and both independent runtime records are satisfied. The implementation remains on `feat/scalable-partition-log-metadata`; external publication was not authorized.

Delivery commits before this final status note: `276b217` (approved goal), `fd5bfb3` (bounded index implementation and focused proofs), and `a7e9426` (final runtime/regression evidence and group-capacity test fixture repair).

Completion requires all success criteria, the automated gate, both runtime records, no material blocking final-review findings, and understood git state. Report behavior, commit(s), concise verification, each role's runtime results, review disposition, and remaining authorized work or blockers.

## Subsequent Delivery Authorization

- 2026-09-06: After local completion and independent review, the user instructed: “Pr it and when green merge”. This authorizes pushing this branch, opening the pull request, waiting for green CI on its exact head, and merging it. The implementation and verification scope is unchanged.
