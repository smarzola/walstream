# Architecture

Walstream maps each Kafka topic to one partition log in an S3-compatible bucket. The broker has no local recovery state and treats object storage as both the write-ahead log and the coordination substrate.

## Commit protocol

Each partition has one JSON manifest containing a revision, the next exclusive offset, and an ordered list of immutable segment descriptors. A descriptor fixes the object path, base offset, record count, encoded byte length, and SHA-256 digest.

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

Walstream does not yet collect orphan segments. They cost storage but cannot change the readable log.

## Read path and bounds

The manifest is validated before it controls allocations or object reads: revisions and offsets must be contiguous, object paths must stay inside the partition, and segment counts and lengths must stay within writer limits. Fetch chooses complete segments from manifest lengths before downloading objects.

Every selected segment must match its recorded length and SHA-256. Its Kafka CRC and raw record boundaries are checked before the upstream decoder may allocate. Record/header counts and delta arithmetic are bounded, duplicate header keys are rejected, decoded offsets and unsupported semantics are checked, and safe deterministic re-encoding must reproduce the original bytes exactly.

Kafka request frames default to 16 MiB. Within each produced topic-partition batch, record counts are limited to 100,000, headers to 1,024 per record, and aggregate headers to 32,768. Fetch targets at most 1 MiB of record payload per response, but may exceed it for the single complete oversized-first-batch exception.

## Object-store contract

Correctness requires:

- strong read-after-write behavior for new and overwritten objects;
- conditional create equivalent to `If-None-Match: *`;
- conditional update against the exact last-read ETag;
- failed stale conditions reported as failures, never silent overwrites.

`walstream verify-store` tests duplicate create, current update, and stale update behavior with a unique temporary object. Server startup runs the same probe before binding its Kafka listener.

The CAS protocol serializes all writers to a partition manifest. This is simple and correct for the MVP, but its latency and contention are the principal throughput limit. A future batching or preferred-writer layer can optimize above the same commit protocol without changing the durable format.
