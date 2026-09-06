# Architecture

Walstream maps each Kafka topic to schema-versioned metadata plus one independent log per partition in an S3-compatible bucket. The broker has no local recovery state and treats object storage as both the write-ahead log and the coordination substrate.

## Commit protocol

Each partition has one schema-4 JSON manifest containing a monotonic publication revision, the earliest retained and next exclusive offsets, a legacy adoption time, at most 64 active segment descriptors, and optional immutable offset-index and producer-directory references. A segment descriptor fixes the object path, base offset, record count, encoded byte length, SHA-256 digest, and optional broker receive time.

Sealed index leaves contain exactly 64 ordered descriptors. Branches contain 1–64 child references with contiguous offset ranges, equal child levels, segment counts, lengths, and SHA-256 digests. Every subtree except the rightmost is full. Pages live under the partition's `index/<uuid>.json` namespace. When a full active tail rolls over, a writer creates its immutable leaf and replaces only the rightmost branch path, growing a new tree level when necessary. Non-idempotent appends without rollover update only the root; ListOffsets reads the root; Fetch seeks through offset ranges and stops when its response budget is exhausted. Reads take their snapshot from one root and its immutable descendants, without listing objects or consulting local recovery state.

New topics persist their creation-time partition count in `<prefix>/clusters/<cluster-id>/topics/<topic>/metadata.json`. The operator default is bounded to `1..=1024`, and a later setting change cannot reinterpret an existing topic. A valid legacy partition-0 manifest without metadata is inferred and upgraded as a one-partition topic without log rewrite. Partition manifests and segment namespaces are otherwise independent.

For every append, a writer reads the manifest and its ETag, assigns the next contiguous offsets, validates every native batch, creates any required immutable offset/producer pages, and writes UUID-named record objects. Native producer batch boundaries remain intact. Ordinary non-idempotent requests retain the existing single-batch canonicalization. It then either conditionally creates the first manifest or updates the existing manifest with `If-Match` semantics.

The manifest write is the only commit point. A precondition failure means another writer committed first; the losing segment is an invisible orphan and the writer retries from the new manifest. An acknowledged append therefore has a unique contiguous range, while failed or crashed attempts cannot become visible without a committed manifest reference.

## Producer state

`InitProducerId` with a null transactional ID allocates a fresh nonnegative ID at epoch zero from a cluster-wide conditional-update object. IDs are never reused; an ambiguous initialization response can consume an unused ID. Transactional initialization is rejected. Nontransactional clients can advance their epoch locally; epoch ordering does not wrap. An exhausted epoch requires a new producer ID.

The partition root references an immutable B+tree keyed by producer ID. Leaves contain up to 64 strictly sorted states; branches contain 2–64 ordered, nonoverlapping references with equal child levels. References include ID bounds, count, level, byte length, and SHA-256. Levels 0–12 and strictly decreasing child levels bound traversal. Inserting or updating a producer rewrites its search path, splitting full pages. Each page body is bounded to 4 MiB and its arrays stop at 64 entries during deserialization.

A state holds the current epoch and the latest five batch identities: base sequence, record count, original base offset, and SHA-256 of validated canonical records with normalized offsets and leader epoch. It retains producer identity, timestamps, keys, values, and headers. Matching epoch/sequence/count/content returns the original range. A lower epoch, gap, or conflicting retry fails. A higher epoch requires sequence zero and clears the old retry window. Sequence advancement wraps modulo 2^31; canonical record encoding preserves the codec's derived per-record signed wrap within a batch.

All batches in a partition request are decided against one private snapshot before object writes. Only new batches advance the log. Their records and final producer states share one root CAS; losing that CAS discards the whole prepared update. Requests containing only recognized duplicates perform no writes. The protocol requires `acks=all` for idempotent records. A request may contain multiple native batches, but retries older than the retained five-batch window fail explicitly.

Retention preserves the producer-directory reference even when it rebuilds or empties the offset index. Batch identities have no record-object references, so expired record objects remain collectible. Maintenance inventories the producer namespace before fencing and fully validates the directory after publication before deleting any candidate. Producer entries do not expire automatically: metadata and maintenance work grow with distinct producer IDs, and producer pages count against `--max-objects` just like other live objects. Raising that bound can accommodate a larger graph up to its documented hard limit.

## Crash cases

| Failure point | Result |
| --- | --- |
| Before any object create | No durable change |
| After record/index/producer object create, before manifest CAS | Invisible orphan; never fetched |
| Manifest CAS precondition failure | Invisible orphan; retry from current ETag |
| After successful manifest CAS, before response | Data and producer state are committed; a recognized idempotent retry receives its original offsets without another append |
| After acknowledgement | A fresh process reconstructs the log from the bucket |

Unreachable record, index, and producer-state objects remain invisible until an explicit maintenance pass collects them. Successful rollovers can leave superseded branch pages; failed attempts can leave tentative objects. Their age alone never authorizes deletion.

## Durable-format upgrade

The reader accepts schema-1 flat manifests and schema-2/3/4 index roots. A schema-1 manifest retains its 10,000-descriptor/4 MiB read limits. The next successful append or maintenance apply publishes schema 4 with one CAS against the exact old root version. Existing record objects and offsets are unchanged. Schema-2 pages can remain reachable without rewriting them.

New batches carry broker receive time captured once before append retries. Descriptors without receive times use the root's persisted adoption time. Schema-3 adoption time is preserved when upgrading to schema 4. Reads and previews do not upgrade the format. Older binaries reject schema 4; stop old processes before upgrading. Mixed-version operation and downgrade after conversion are unsupported. Topic metadata and durable consumer-offset objects retain their existing behavior.

## Retention and collection

`maintain` previews one topic/partition. `--apply` permits writes and deletions; optional age and encoded-record-byte limits remove an oldest prefix of complete batches. The retained start never decreases and the next offset never changes during maintenance, including when all records expire. Future appends continue at that next offset. Consumer offsets do not pin data.

An apply attempt follows this order:

1. Finish a bounded inventory of recognized UUID record/index/producer-state keys in the selected partition. Never add later keys to this attempt's deletion set.
2. Load the current root and traverse its complete graph, validating every page, record descriptor, and record-object length. Calculate retention and, if needed, build a new bounded-page index referencing retained records.
3. Publish the complete retained root with a CAS against the loaded version. Increment the revision even for GC-only maintenance, so identical logical ranges cannot recreate an old ETag. Failed or ambiguous publication authorizes no deletion.
4. Traverse the exact published root completely. Any validation failure stops deletion; a missing object from a superseded root restarts the attempt. Check graph membership before deleting each candidate from the closed inventory.
5. Report successful deletions, including already absent candidates. Other deletion errors stop the pass and report the committed range and partial progress. A later pass completes remaining reclamation.

Fresh immutable keys are conditionally created and never reused. After a failed CAS, writers and collectors discard tentative objects and create new ones for the next attempt. They may publish references only to objects reachable from their loaded root or objects created in that attempt. These rules prevent an older paused operation from publishing an object that maintenance selected for deletion. Listings may omit candidates safely: omissions delay collection. Unknown filenames and objects outside the selected data/index/producer-state namespaces are never candidates.

Readers, append preparation, and collectors retain their exact root version. If a referenced GET or HEAD returns NotFound, they reload the root: a changed version permits a full retry; an unchanged version is an integrity failure. Partial Fetch buffers are discarded on retry. Corruption and general transport errors are not treated as expiry. A read may finish from its original coherent snapshot if all required objects remain available, or retry and discover that its requested offset expired. No durable reader pins, grace period, or synchronized clocks are needed for GC safety.

Inventory and graph limits default to 100,000 objects and have a hard maximum of 1,000,000. Each operation permits at most 128 attempts. Inventory/planning budget exhaustion stops before publication; post-publication validation errors stop deletion and report committed state. Maintenance may rebuild the retained index, while ordinary append and Fetch retain bounded path access without listings. Record bodies are not rewritten or downloaded by maintenance; full content integrity checks remain in Fetch.

A crash before publication leaves only tentative objects. A crash after publication leaves retention effective, with some or all eligible objects still present. Retrying is safe. Ordinary deletion removes visible keys; purging historical versions in versioned buckets is outside this feature.

## Consumer-group state

Classic group membership is intentionally process-local. One coordinator instance is shared by every connection, while each group moves through bounded `Joining`, `AwaitingSync`, and `Stable` phases. Up to 1,024 dynamic members negotiate one common protocol. The elected leader alone receives every member's opaque metadata for that protocol and must install exactly one assignment per current member; followers wait for their own assignment. Assignments are immutable inside a generation. Join, leave, and session expiry start or complete a bounded rebalance, while independent groups retain separate locks and notifications.

Heartbeats and commits require a current member and generation in `Stable`. A durable commit holds the affected group's operation gate across its object-store update, preventing a stale generation from overwriting a replacement member's newer offset without blocking unrelated groups. Inactive slots are identity-safely reclaimed, and the broker caps resident group slots at 10,000. Protocol metadata is capped at 1 MiB per member and 16 MiB per group; leader assignments are capped at 16 MiB per group, with checked aggregate accounting before state mutation.

Only committed offsets survive replacement. Each group stores a bounded schema-v1 object at `<prefix>/clusters/<cluster-id>/groups/<group-id>/offsets.json`. OffsetCommit validates request-wide retention and coordinator semantics, excludes invalid topic-partitions with partition-local errors, and atomically applies the remaining valid subset in one conditional create or ETag update. A precondition conflict retries from fresh state. OffsetFetch returns an explicit absence and preserves the distinction between null and empty metadata. Corrupt, future-schema, duplicate, truncated, oversized, or out-of-scope state fails closed.

A broker replacement therefore preserves every partition's next committed offset but deliberately loses membership. Retained clients must rediscover the coordinator, receive new dynamic member identities, rejoin, and obtain fresh assignments before they can heartbeat or commit again.

## Read path and bounds

Root and page bodies are streamed under a 4 MiB cap. Indexed tail and page collections stop at 64 entries, while the legacy schema-1 reader retains its 10,000-entry limit. Traversal accepts page levels 0–10 and requires each child level to decrease, bounding depth even for cyclic or forged references. Per-page namespace, range, count, length, checksum, and tree-shape checks precede use. Schema-2 revisions equal the segment count; schema-3/4 revisions advance on publications and may exceed the retained segment count.

Only pages on the requested read or update path are loaded and validated. This avoids a whole-history scan; it is not a full-log integrity audit. A missing accessed page is retried only when the loaded root has changed; corruption fails the operation. Fetch selects complete segments using descriptor lengths before downloading record objects, including across leaf/tail boundaries. Empty tail-of-log reads and ListOffsets need only the root's validated range.

Every selected segment's object metadata must match its bounded manifest length, and its body is streamed only up to that length before its SHA-256 is checked. Its Kafka CRC and raw record boundaries are checked before the upstream decoder may allocate. Record/header counts, reserved attributes, and delta arithmetic are validated, duplicate header keys are rejected, decoded offsets and unsupported semantics are checked, and safe deterministic re-encoding must reproduce the original bytes exactly.

Kafka request frames default to 16 MiB and an allocation-free structural pass limits generated-decoder collections to 10,000 aggregate items per request. Topic partition counts are capped at 1,024. Classic groups cap members at 1,024, protocols per member at 32, identifiers at 249 bytes, and protocol/assignment blobs plus retained aggregates as described above. Within each produced topic-partition batch, record counts are limited to 100,000, headers to 1,024 per record, and aggregate headers to 32,768. Fetch targets at most 1 MiB of record payload per response, but may exceed it for the single complete oversized-first-batch exception.

## Object-store contract

Correctness requires:

- strong read-after-write behavior for new and overwritten objects;
- conditional create equivalent to `If-None-Match: *`;
- conditional update against the exact last-read ETag;
- failed stale conditions reported as failures, never silent overwrites.

`walstream verify-store` tests duplicate create, current update, and stale update behavior with a unique temporary object. Server startup runs the same probe before binding its Kafka listener.

The CAS protocol serializes writers independently at each partition manifest. This is simple and correct for the MVP, but its latency and same-partition contention are the principal throughput limit. A future batching or preferred-writer layer can optimize above the same commit protocol without changing the durable format.
