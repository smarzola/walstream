# Walstream

Walstream is an experimental, single-binary Kafka-compatible broker whose only durable dependency is S3-compatible object storage. Process memory and local disk are disposable: immutable Kafka record batches and a conditionally updated manifest are the log.

This MVP is deliberately narrow. It provides one virtual broker, durable multi-partition topics, explicit-partition produce/fetch, and bounded dynamic classic consumer groups whose clients divide partitions through ordinary leader-computed assignments. It is not a drop-in replacement for a general Kafka cluster.

## Run it

Build the binary:

```bash
cargo build --release
```

Provide credentials through AWS environment variables or a supported workload/instance credential provider, then verify the bucket contract before serving. Walstream does not read shared `~/.aws/credentials` profiles:

```bash
export AWS_ACCESS_KEY_ID=example
export AWS_SECRET_ACCESS_KEY=example-secret

./target/release/walstream verify-store \
  --bucket my-walstream-bucket \
  --region eu-north-1 \
  --prefix walstream \
  --cluster-id production

./target/release/walstream serve \
  --bucket my-walstream-bucket \
  --region eu-north-1 \
  --prefix walstream \
  --cluster-id production \
  --default-topic-partitions 3 \
  --listen 0.0.0.0:9092 \
  --advertised-host broker.example.internal
```

The bucket must already exist. It must provide strong read-after-write behavior plus conditional object creates and ETag-matched updates. `verify-store` proves those preconditions with a unique temporary object and fails closed if the contract is absent.

Explicit-topic operation and `verify-store` need GetObject, PutObject, and DeleteObject access within the configured cluster prefix. A client Metadata request that lists every topic additionally needs ListBucket access for that prefix; startup and explicit-topic requests do not.

For an S3-compatible endpoint, add `--endpoint https://s3.example.internal`. Plain HTTP requires the explicit `--allow-http` development flag. See [.env.example](.env.example) for every environment variable and `walstream serve --help` for flags.

Metadata or produce access auto-creates a valid topic; there is no administrative topic API. New topics use `--default-topic-partitions` / `WALSTREAM_DEFAULT_TOPIC_PARTITIONS`, which defaults to `1` and is bounded to `1..=1024`. The creation-time count is persisted, so changing the setting affects only later topics. Clients may address any partition advertised in metadata; a partition outside that durable range returns `UNKNOWN_TOPIC_OR_PARTITION`.

## Compatibility

Walstream advertises only this exercised wire surface:

| API | Versions | MVP behavior |
| --- | --- | --- |
| ApiVersions | 0–3 | Returns this exact matrix |
| Metadata | 4 | One broker, all durable partitions; optional topic auto-create |
| Produce | 7 | Uncompressed nontransactional Kafka v2 batches, including idempotent production |
| InitProducerId | 0–4 | Fresh durable nontransactional producer ID at epoch zero |
| Fetch | 4 | Explicit offset, complete batches, 1 MiB broker payload cap |
| ListOffsets | 3 | Earliest and latest offsets |
| FindCoordinator | 2 | Group coordinator is this broker |
| JoinGroup | 2 | Bounded dynamic classic members; common protocol negotiation |
| SyncGroup | 1 | Exact leader-provided opaque assignment; followers wait |
| Heartbeat | 1 | Process-local session deadline |
| LeaveGroup | 1 | Releases ephemeral membership |
| OffsetCommit | 2 | One CAS for the valid subset; partition-local errors; default retention only |
| OffsetFetch | 3 | Selected or all durable group offsets |

Unsupported APIs and adjacent versions close the connection or return an explicit Kafka error. Out-of-range partitions, follower reads, invalid offsets, transactions, control batches, compression, duplicate header keys, and malformed data are never acknowledged as successful.

The default maximum request frame is 16 MiB. Before generated decoding, an allocation-free structural pass limits aggregate request collection items to 10,000. Fetch returns complete segment batches and applies Kafka's oversized-first-batch exception at most once per response.

## Idempotent producers

Java 4.2.0 and librdkafka 2.12.1 can enable `enable.idempotence=true` with `acks=all` and no compression. Keep the client’s maximum in-flight requests at five or fewer. Producer IDs are allocated durably across broker replacements; failed initialization responses may consume unused IDs.

Each partition remembers the current epoch and last five batch identities for every producer ID. A matching retry returns its original offsets without writing records or metadata, including after its records have expired. A new epoch starts at sequence zero and fences older epochs. Sequence gaps and retries outside the window fail; reusing a remembered sequence with different content fails. Sequences wrap at Kafka’s positive 32-bit boundary.

Every native batch in a partition Produce request is validated before any records publish. Multiple new batches commit together, and a duplicate prefix can precede new batches. The five-batch history is a retry window, so replaying a request with more than five batches may fail once its first batches have left that window. Starting a new application producer allocates a different ID and does not deduplicate the previous instance’s work. Transactions and automatic producer-state expiry are unsupported.

Producer state uses immutable pages with up to 64 entries or child references, a bounded search path, and the same 4 MiB metadata cap. State does not reference record objects, so it cannot pin expired data. It persists for each distinct producer ID; metadata storage and full maintenance scans grow with that count.

## Durability model

An append:

1. reads the committed partition manifest;
2. checks producer epochs/sequences and returns original offsets for recognized duplicate batches;
3. assigns contiguous offsets and encodes each new native batch;
4. seals a full 64-segment tail into immutable index pages when necessary;
5. writes immutable records and updated producer-state pages;
6. publishes the bounded root, including producer state, by conditionally creating or ETag-updating the manifest;
7. retries from fresh state if another writer wins the manifest race.

Only the manifest CAS is the commit point. A crash before it can leave an invisible orphan object; a crash after an acknowledged CAS leaves all required state in the bucket. Reads validate manifest invariants, object length, SHA-256, Kafka CRC and raw allocation bounds, logical offsets, unsupported semantics, and exact canonical re-encoding.

Objects live under:

```text
<prefix>/clusters/<cluster-id>/producer-ids.json
<prefix>/clusters/<cluster-id>/topics/<topic>/metadata.json
<prefix>/clusters/<cluster-id>/topics/<topic>/<partition>/manifest.json
<prefix>/clusters/<cluster-id>/topics/<topic>/<partition>/index/<uuid>.json
<prefix>/clusters/<cluster-id>/topics/<topic>/<partition>/segments/<uuid>.batch
<prefix>/clusters/<cluster-id>/topics/<topic>/<partition>/producer-state/<uuid>.json
<prefix>/clusters/<cluster-id>/groups/<group-id>/offsets.json
```

Topic metadata is schema-versioned and conditionally created. Existing installations that have a valid partition-0 manifest but no topic metadata are read as one-partition topics and upgraded without rewriting their log.

New partition manifests use schema 4: a root with at most 64 active segment descriptors and references to immutable offset and producer-state indexes. Sealed leaves contain 64 descriptors, branches contain at most 64 child references, and each metadata body retains the 4 MiB safety cap. The index supports up to 11 page levels, covering the positive Kafka offset space. Appends update the bounded root and, on rollover, the rightmost index path. Fetch locates requested offsets through the index; it does not load the full partition history. There is no longer a 10,000-append limit.

Schemas 1–3 remain readable. Their next successful append or maintenance apply publishes schema 4 using existing record objects and offsets. Reads and maintenance previews do not convert the partition manifest. Schema 4 adds producer state while preserving schema 3 retained offsets, receive times, and publication revisions. Upgrading schema 3 preserves its original adoption time. **Older Walstream binaries cannot serve an upgraded partition.** Stop old processes before upgrading; mixed-version operation and downgrade after conversion are unsupported.

Committed group offsets and optional metadata use a bounded, schema-versioned object with the same conditional-create/ETag-update discipline. They survive complete broker replacement. Membership, generations, assignments, heartbeats, and session deadlines exist only in the broker process, so retained consumers must rediscover the coordinator and rejoin with new member identities after replacement. Join, leave, and session expiry rebalance only the affected group; the leader receives every member's selected-protocol metadata and must submit exactly one immutable assignment per member for the new generation.

See [docs/architecture.md](docs/architecture.md) for the failure model and invariants.

## Retain data and reclaim storage

Retention is explicit: the broker keeps data until you run `maintain --apply`. First verify the store contract as shown above. Maintenance also needs ListBucket access to the selected partition's `segments/`, `index/`, and `producer-state/` prefixes and DeleteObject access to their contents. It does not create topics.

Preview a partition with a one-day age limit and a 1 GiB encoded-record limit:

```bash
./target/release/walstream maintain \
  --bucket my-walstream-bucket --region eu-north-1 \
  --prefix walstream --cluster-id production \
  --topic events --partition 0 \
  --max-age-ms 86400000 --max-bytes 1073741824
```

The report shows the previous and proposed readable offsets, retained/expired batches and bytes, collectible objects, and any format adoption. Preview performs no writes or deletes. Repeat the command with `--apply` to publish retention and collect objects. Omit both limits to collect only unreachable objects. Add `--json` for a structured report.

Both limits remove the oldest contiguous prefix of **whole batches**. Age uses the time the broker received the batch, independently of producer timestamps. Batches without a stored receive time use the partition's persisted adoption time, giving existing data a full age window after upgrade. A byte limit can expire that data immediately. Byte limits count encoded record objects; they exclude index overhead, orphan objects, and historical bucket versions. Limits apply when maintenance runs; they do not cap later appends.

A batch larger than the byte limit can expire completely. Zero age or zero bytes can expire all records. If offsets 0–99 expire, the earliest offset becomes 100 and requests for an expired offset receive `OFFSET_OUT_OF_RANGE`. Retained offsets never change, and even an empty log keeps its next append offset across process replacement. Committed consumer offsets remain unchanged; retention does not wait for slow consumers.

Maintenance runs alongside readers and writers. It inventories candidates, conditionally publishes the retained range, validates the complete committed metadata graph and record-object lengths, and then deletes proven unreachable candidates. Missing objects from a superseded snapshot cause a bounded retry. Corruption and missing objects from an unchanged root fail the operation. Fetch continues to validate record contents and checksums; maintenance is not a full record-body integrity audit.

If maintenance exits unsuccessfully after publication, its report identifies the committed range and completed collection count. An uncertain publication response authorizes no deletion. Rerunning is safe: a committed range remains in effect and a later pass collects remaining objects. A listing that misses a candidate leaves it for a later pass. Collection counts include already absent candidates; collected byte counts use their inventory sizes. Ordinary deletion does not purge noncurrent versions in a versioned bucket.

`--max-objects` defaults to 100,000 and accepts 1–1,000,000. It bounds listed entries and each complete live graph, including permanent producer-state pages; exceeding an inventory/planning bound prevents publication. A failure during validation after publication prevents deletion and reports the committed state. Operations retry stale snapshots or failed conditions at most 128 times. Under sustained interference, maintenance can return a contention error; previously committed retention remains effective.

## Verify it

Hermetic checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

The consumer-group proof is credential-free and uses Apple Container rather than Docker. It runs pinned confluent-kafka/librdkafka `2.12.1` and Apache Kafka Java client `4.2.0` against a local broker and pinned RustFS store. A one-member seed phase initializes durable offsets, after which the measured clients disable offset reset. Each client then forms a two-member classic group over three partitions, proves a disjoint complete split, synchronously commits every partition, closes one member, and proves all partitions move to the retained survivor. The broker process is replaced while that survivor stays alive; the probe requires a changed member identity, the exact three-partition reassignment, and resume from every committed next offset without replay. Missing replacement offsets are therefore an error rather than an accidental reset to the expected position. The harness also pins Python `3.13.5-slim` and Maven `3.9.11` with Eclipse Temurin 21, creates unique container names and bucket/prefix state, and removes its exact processes, containers, and temporary data on exit.

```bash
container system start
./scripts/test-consumer-group-clients.sh
```

The real-S3-compatible proof uses Apple Container, a unique disposable bucket and prefix, and the compiled broker process. It defaults to pinned RustFS `1.0.0-beta.12`:

```bash
container system start
./scripts/test-s3-e2e.sh
```

Run the same proof against pinned SeaweedFS `4.40` or the existing pinned MinIO target:

```bash
WALSTREAM_E2E_BACKEND=seaweedfs ./scripts/test-s3-e2e.sh
WALSTREAM_E2E_BACKEND=minio ./scripts/test-s3-e2e.sh
```

Every backend selection proves Walstream's required conditional create/update behavior, stock-client discovery and produce/fetch, recovery after hard process replacement, and concurrent independent writers publishing unique contiguous offsets. It does not establish general S3 compatibility, backend production readiness, or comparative performance; RustFS remains a beta release. The script removes its selected test container and bucket data when it exits. It requires `container`, `container-compose`, and `jq`; it does not use Docker.

The index walkthrough launches the actual broker against its own disposable RustFS container, makes 25,000 separate appends, reads all records after replacement, upgrades a legacy fixture, kills a broker with a root update intercepted before publication, and verifies corrupt-page rejection:

```bash
./scripts/test-log-index.sh
```

It requires Python 3 for the local fault proxy in addition to the existing container tools. `--appends 129` runs a short rollover smoke walkthrough. `--baseline-broker /path/to/old/walstream` also exercises rejection of an upgraded partition by an actual older binary. This is a correctness walkthrough, not a throughput benchmark.

The maintenance walkthrough exercises the actual CLI and Kafka service, including preview, repeated trimming, storage reclamation, empty-log replacement, paused readers/writers, interrupted deletion, legacy adoption, and corrupt-metadata rejection:

```bash
./scripts/test-maintenance.sh
```

It uses an owned disposable RustFS container and the same Python fault proxy. An optional `--baseline-broker /path/to/schema-2/walstream` proves old-binary rejection after schema-4 adoption.

## Explicit non-goals

- static membership, the newer consumer group protocol, offset retention, transactions, and idempotent producers;
- more than one broker, replication-factor semantics, or follower reads;
- automatic retention scheduling, compaction, quotas, or multi-region operation;
- Kafka authentication/authorization or TLS termination;
- compressed record batches or duplicate Kafka header keys;
- throughput comparable to Kafka: every append uploads an object and contends on one per-partition manifest CAS.

Run Walstream behind appropriate network and TLS controls. The MVP has no client authentication and has not been production-hardened or scale-tested.

## License

MIT

The native producer recovery walkthrough requires Apple Container, Python 3, Ruby, and jq:

```bash
scripts/test-idempotent-producers.sh
```

It runs Java 4.2.0 and librdkafka 2.12.1 with the same producer instances through a withheld successful Produce response and broker replacement. It also expires the committed batch before retry, checks identical producer IDs/epochs/sequences/payloads, compares the root before and after retry, and consumes the exact retained range. The script prints its retained scratch directory containing request traces, root snapshots, and client logs; it removes its containers and processes.
