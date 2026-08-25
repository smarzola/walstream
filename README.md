# Walstream

Walstream is an experimental, single-binary Kafka-compatible broker whose only durable dependency is S3-compatible object storage. Process memory and local disk are disposable: immutable Kafka record batches and a conditionally updated manifest are the log.

This MVP is deliberately narrow. It provides one virtual broker, one partition per topic, explicit-partition produce/fetch, and classic consumer groups with one active dynamic member per group. It is not a drop-in replacement for a general Kafka cluster.

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
  --listen 0.0.0.0:9092 \
  --advertised-host broker.example.internal
```

The bucket must already exist. It must provide strong read-after-write behavior plus conditional object creates and ETag-matched updates. `verify-store` proves those preconditions with a unique temporary object and fails closed if the contract is absent.

Explicit-topic operation and `verify-store` need GetObject, PutObject, and DeleteObject access within the configured cluster prefix. A client Metadata request that lists every topic additionally needs ListBucket access for that prefix; startup and explicit-topic requests do not.

For an S3-compatible endpoint, add `--endpoint https://s3.example.internal`. Plain HTTP requires the explicit `--allow-http` development flag. See [.env.example](.env.example) for every environment variable and `walstream serve --help` for flags.

Clients must use explicit topic partition `0`. Metadata or produce access auto-creates a valid topic; there is no administrative topic API.

## Compatibility

Walstream advertises only this exercised wire surface:

| API | Versions | MVP behavior |
| --- | --- | --- |
| ApiVersions | 0–3 | Returns this exact matrix |
| Metadata | 4 | One broker, one partition; optional topic auto-create |
| Produce | 7 | Uncompressed, non-idempotent Kafka v2 batches |
| Fetch | 4 | Explicit offset, complete batches, 1 MiB broker payload cap |
| ListOffsets | 3 | Earliest and latest offsets |
| FindCoordinator | 2 | Group coordinator is this broker |
| JoinGroup | 2 | Classic consumer protocol; one active dynamic member |
| SyncGroup | 1 | Leader-provided opaque assignment |
| Heartbeat | 1 | Process-local session deadline |
| LeaveGroup | 1 | Releases ephemeral membership |
| OffsetCommit | 2 | Atomic durable next-offset commit; default retention only |
| OffsetFetch | 3 | Selected or all durable group offsets |

Unsupported APIs and adjacent versions close the connection or return an explicit Kafka error. Unsupported partitions, follower reads, invalid offsets, transactions, idempotent/control batches, compression, duplicate header keys, and malformed data are never acknowledged as successful.

The default maximum request frame is 16 MiB. Before generated decoding, an allocation-free structural pass limits aggregate request collection items to 10,000. Fetch returns complete segment batches and applies Kafka's oversized-first-batch exception at most once per response.

## Durability model

An append:

1. reads the committed partition manifest;
2. assigns contiguous offsets and writes one immutable record-batch object;
3. publishes it by conditionally creating or ETag-updating the manifest;
4. retries from fresh state if another writer wins the manifest race.

Only the manifest CAS is the commit point. A crash before it can leave an invisible orphan object; a crash after an acknowledged CAS leaves all required state in the bucket. Reads validate manifest invariants, object length, SHA-256, Kafka CRC and raw allocation bounds, logical offsets, unsupported semantics, and exact canonical re-encoding.

Objects live under:

```text
<prefix>/clusters/<cluster-id>/topics/<topic>/0/manifest.json
<prefix>/clusters/<cluster-id>/topics/<topic>/0/segments/<uuid>.batch
<prefix>/clusters/<cluster-id>/groups/<group-id>/offsets.json
```

Committed group offsets and optional metadata use a bounded, schema-versioned object with the same conditional-create/ETag-update discipline. They survive complete broker replacement. Membership, generations, assignments, heartbeats, and session deadlines exist only in the broker process, so a replacement requires the consumer to rejoin. Different group IDs are independent; a second live member in the same group is rejected explicitly.

See [docs/architecture.md](docs/architecture.md) for the failure model and invariants.

## Verify it

Hermetic checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

The consumer-group proof is credential-free and uses Apple Container rather than Docker. It runs pinned confluent-kafka/librdkafka `2.12.1` and Apache Kafka Java client `4.2.0` against a local broker and pinned RustFS store. Each client subscribes, consumes, commits synchronously, then reconnects after the broker process is replaced and must resume at the next record without replay. The harness also pins Python `3.13.5-slim` and Maven `3.9.11` with Eclipse Temurin 21, creates unique container names and bucket/prefix state, and removes its exact processes, containers, and temporary data on exit.

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

## Explicit non-goals

- multi-member rebalancing, static membership, the newer consumer group protocol, offset retention, transactions, and idempotent producers;
- more than one broker or partition, replication-factor semantics, or follower reads;
- retention, compaction, orphan collection, quotas, or multi-region operation;
- Kafka authentication/authorization or TLS termination;
- compressed record batches or duplicate Kafka header keys;
- throughput comparable to Kafka: every append uploads an object and contends on one manifest CAS.
- more than 10,000 committed segment objects per topic in the current manifest format.

Run Walstream behind appropriate network and TLS controls. The MVP has no client authentication and has not been production-hardened or scale-tested.

## License

MIT
