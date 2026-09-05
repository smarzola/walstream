# Architecture

Walstream maps each Kafka topic to schema-versioned metadata plus one independent log per partition in an S3-compatible bucket. The broker has no local recovery state and treats object storage as both the write-ahead log and the coordination substrate.

## Commit protocol

Each partition has one schema-2 JSON manifest containing a revision (the committed segment count), the next exclusive offset, at most 64 active segment descriptors, and an optional immutable index-tree reference. A segment descriptor fixes the object path, base offset, record count, encoded byte length, and SHA-256 digest.

Sealed index leaves contain exactly 64 ordered descriptors. Branches contain 1–64 child references with contiguous offset ranges, equal child levels, segment counts, lengths, and SHA-256 digests. Every subtree except the rightmost is full. Pages live under the partition's `index/<uuid>.json` namespace. When a full active tail rolls over, a writer creates its immutable leaf and replaces only the rightmost branch path, growing a new tree level when necessary. Ordinary appends update only the root; ListOffsets reads the root; Fetch seeks through offset ranges and stops when its response budget is exhausted. Reads take their snapshot from one root and its immutable descendants, without listing objects or consulting local recovery state.

New topics persist their creation-time partition count in `<prefix>/clusters/<cluster-id>/topics/<topic>/metadata.json`. The operator default is bounded to `1..=1024`, and a later setting change cannot reinterpret an existing topic. A valid legacy partition-0 manifest without metadata is inferred and upgraded as a one-partition topic without log rewrite. Partition manifests and segment namespaces are otherwise independent.

For every append, a writer reads the manifest and its ETag, assigns the next contiguous offsets, canonicalizes the accepted records into one uncompressed Kafka v2 record batch, and creates an immutable UUID-named object. It then either conditionally creates the first manifest or updates the existing manifest with `If-Match` semantics.

The manifest write is the only commit point. A precondition failure means another writer committed first; the losing segment is an invisible orphan and the writer retries from the new manifest. An acknowledged append therefore has a unique contiguous range, while failed or crashed attempts cannot become visible without a committed manifest reference.

## Crash cases

| Failure point | Result |
| --- | --- |
| Before segment create | No durable change |
| After segment create, before manifest CAS | Invisible orphan; never fetched |
| Manifest CAS precondition failure | Invisible orphan; retry from current ETag |
| After successful manifest CAS, before response | Data is committed; client may retry because the MVP has no idempotent producer support |
| After acknowledgement | A fresh process reconstructs the log from the bucket |

Walstream does not yet collect orphan segments, unpublished index pages, or superseded immutable branch pages. They cost storage but cannot change the readable log. Successful rollovers and migrations can also leave superseded pages. No deletion or retention policy is implemented.

## Durable-format upgrade

The reader accepts schema-1 flat manifests and schema-2 index roots. A schema-1 manifest is fully bounded and validated with the existing 10,000-descriptor/4 MiB read limits. On its next append, the writer builds an index referencing the original record objects, adds the new batch, and publishes schema 2 with one CAS against the exact schema-1 ETag. Losing that race reloads current state and retries, including when another writer completed the migration. A crash before publication leaves the legacy manifest authoritative; a successful publication makes the complete indexed history recoverable.

Reads do not convert partition manifests, and original record objects and offsets are unchanged. Older binaries reject schema 2. Operators must stop old processes before adopting the new writer; mixed-version operation and downgrade after conversion are unsupported. Legacy topic-metadata inference and durable consumer-offset objects keep their existing behavior.

## Consumer-group state

Classic group membership is intentionally process-local. One coordinator instance is shared by every connection, while each group moves through bounded `Joining`, `AwaitingSync`, and `Stable` phases. Up to 1,024 dynamic members negotiate one common protocol. The elected leader alone receives every member's opaque metadata for that protocol and must install exactly one assignment per current member; followers wait for their own assignment. Assignments are immutable inside a generation. Join, leave, and session expiry start or complete a bounded rebalance, while independent groups retain separate locks and notifications.

Heartbeats and commits require a current member and generation in `Stable`. A durable commit holds the affected group's operation gate across its object-store update, preventing a stale generation from overwriting a replacement member's newer offset without blocking unrelated groups. Inactive slots are identity-safely reclaimed, and the broker caps resident group slots at 10,000. Protocol metadata is capped at 1 MiB per member and 16 MiB per group; leader assignments are capped at 16 MiB per group, with checked aggregate accounting before state mutation.

Only committed offsets survive replacement. Each group stores a bounded schema-v1 object at `<prefix>/clusters/<cluster-id>/groups/<group-id>/offsets.json`. OffsetCommit validates request-wide retention and coordinator semantics, excludes invalid topic-partitions with partition-local errors, and atomically applies the remaining valid subset in one conditional create or ETag update. A precondition conflict retries from fresh state. OffsetFetch returns an explicit absence and preserves the distinction between null and empty metadata. Corrupt, future-schema, duplicate, truncated, oversized, or out-of-scope state fails closed.

A broker replacement therefore preserves every partition's next committed offset but deliberately loses membership. Retained clients must rediscover the coordinator, receive new dynamic member identities, rejoin, and obtain fresh assignments before they can heartbeat or commit again.

## Read path and bounds

Root and page bodies are streamed under a 4 MiB cap. Schema-2 tail and page collections stop at 64 entries, while the legacy schema-1 reader retains its 10,000-entry limit. Traversal accepts page levels 0–10 and requires each child level to decrease, bounding depth even for cyclic or forged references. Per-page namespace, range, count, length, checksum, and tree-shape checks precede use. Root revisions agree with the indexed segment count plus the active tail.

Only pages on the requested read or update path are loaded and validated. This avoids a whole-history scan; it is not a full-log integrity audit. A missing or corrupt accessed page fails the operation. Fetch selects complete segments using descriptor lengths before downloading record objects, including across leaf/tail boundaries. Empty tail-of-log reads and ListOffsets need only the root's validated range.

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
