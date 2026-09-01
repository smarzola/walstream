#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_id="$(date +%s)-$$"
bucket="walstream-clients-$run_id"
prefix="walstream-client-e2e/$run_id"
cluster="clients"
store_name="walstream-client-store-$run_id"
librdkafka_name="walstream-librdkafka-$run_id"
java_name="walstream-java-$run_id"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/walstream-clients.XXXXXX")
broker_pid=
broker_run=0
librdkafka_pid=
java_pid=

cleanup() {
    if [ -n "$broker_pid" ]; then
        kill "$broker_pid" >/dev/null 2>&1 || true
        wait "$broker_pid" >/dev/null 2>&1 || true
    fi
    for pid in "$librdkafka_pid" "$java_pid"; do
        if [ -n "$pid" ]; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    for name in "$librdkafka_name" "$java_name" "$store_name"; do
        container stop "$name" >/dev/null 2>&1 || true
        container delete "$name" >/dev/null 2>&1 || true
    done
    rm -rf "$tmp_dir"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_exec() {
    name=$1
    marker=$2
    attempt=0
    until container exec "$name" test -f "$marker" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 120 ]; then
            container logs "$name" >&2 || true
            echo "$name did not become ready" >&2
            exit 1
        fi
        sleep 1
    done
}

wait_for_phase() {
    marker=$1
    pid=$2
    log=$3
    name=$4
    phase=$5
    attempt=0
    until [ -f "$marker" ]; do
        attempt=$((attempt + 1))
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            wait "$pid" 2>/dev/null || true
            cat "$log" >&2
            cat "$tmp_dir/broker-$broker_run.log" >&2 || true
            echo "$name exited before $phase" >&2
            exit 1
        fi
        if [ "$attempt" -ge 240 ]; then
            cat "$log" >&2
            echo "$name did not $phase" >&2
            exit 1
        fi
        sleep 0.25
    done
}

stop_broker() {
    if [ -n "$broker_pid" ]; then
        kill "$broker_pid"
        wait "$broker_pid" 2>/dev/null || true
        broker_pid=
    fi
}

start_broker() {
    broker_run=$((broker_run + 1))
    AWS_ACCESS_KEY_ID=walstream \
    AWS_SECRET_ACCESS_KEY=walstream-secret \
    AWS_REGION=us-east-1 \
    "$project_dir/target/release/walstream" serve \
        --bucket "$bucket" \
        --region us-east-1 \
        --endpoint "http://$store_ip:9000" \
        --allow-http \
        --prefix "$prefix" \
        --cluster-id "$cluster" \
        --default-topic-partitions 3 \
        --listen "0.0.0.0:$broker_port" \
        --advertised-host "$host_gateway" \
        --advertised-port "$broker_port" \
        >"$tmp_dir/broker-$broker_run.log" 2>&1 &
    broker_pid=$!

    attempt=0
    until ruby -rsocket -e \
        'socket = TCPSocket.new(ARGV[0], Integer(ARGV[1])); socket.close' \
        127.0.0.1 "$broker_port" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if ! kill -0 "$broker_pid" >/dev/null 2>&1; then
            cat "$tmp_dir/broker-$broker_run.log" >&2
            echo "walstream exited before becoming ready" >&2
            exit 1
        fi
        if [ "$attempt" -ge 200 ]; then
            cat "$tmp_dir/broker-$broker_run.log" >&2
            echo "walstream did not become ready" >&2
            exit 1
        fi
        sleep 0.05
    done
}

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to inspect Apple Container addresses" >&2
    exit 1
fi
if ! command -v ruby >/dev/null 2>&1; then
    echo "ruby is required to select and probe the broker port" >&2
    exit 1
fi
if ! container system status >/dev/null 2>&1; then
    container system start
fi

container run --detach --name "$store_name" \
    --env RUSTFS_ACCESS_KEY=walstream \
    --env RUSTFS_SECRET_KEY=walstream-secret \
    --env RUSTFS_ADDRESS=:9000 \
    --env RUSTFS_CONSOLE_ENABLE=false \
    rustfs/rustfs:1.0.0-beta.12 /data >/dev/null

attempt=0
until store_address=$(container inspect "$store_name" 2>/dev/null \
    | jq -r '.[0].status.networks[0].ipv4Address // empty') \
    && [ -n "$store_address" ] \
    && store_ip=${store_address%/*} \
    && container run --rm \
        --env "MC_HOST_local=http://walstream:walstream-secret@$store_ip:9000" \
        quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z \
        mb --ignore-existing "local/$bucket" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        container logs "$store_name" >&2 || true
        echo "RustFS did not become ready" >&2
        exit 1
    fi
    sleep 1
done

host_gateway=$(container run --rm alpine:3.22 \
    sh -c "ip route | awk '/default/ { print \$3; exit }'")
if [ -z "$host_gateway" ]; then
    echo "could not discover the Apple Container host gateway" >&2
    exit 1
fi
broker_port=$(ruby -rsocket -e \
    'server = TCPServer.new("127.0.0.1", 0); puts server.addr[1]; server.close')
bootstrap="$host_gateway:$broker_port"

cd "$project_dir"
cargo build --release
start_broker

container run --detach --name "$librdkafka_name" \
    --mount "type=bind,source=$project_dir/tests/clients,target=/clients,readonly" \
    --mount "type=bind,source=$tmp_dir,target=/state" \
    --entrypoint sh \
    python:3.13.5-slim \
    -c 'pip install --no-cache-dir confluent-kafka==2.12.1 && touch /tmp/ready && sleep 3600' \
    >/dev/null
container run --detach --name "$java_name" \
    --mount "type=bind,source=$project_dir/tests/clients/java,target=/source,readonly" \
    --mount "type=bind,source=$tmp_dir,target=/state" \
    --entrypoint sh \
    maven:3.9.11-eclipse-temurin-21 \
    -c 'cp -R /source /work && mvn -q -f /work/pom.xml compile && touch /tmp/ready && sleep 3600' \
    >/dev/null

wait_for_exec "$librdkafka_name" /tmp/ready
wait_for_exec "$java_name" /tmp/ready

container exec "$librdkafka_name" python /clients/librdkafka_probe.py \
    "$bootstrap" librdkafka-events librdkafka-group \
    librdkafka-first librdkafka-second /state \
    >"$tmp_dir/librdkafka.log" 2>&1 &
librdkafka_pid=$!
container exec "$java_name" mvn -q -f /work/pom.xml exec:java \
    "-Dexec.args=$bootstrap java-events java-group java-first java-second /state" \
    >"$tmp_dir/java.log" 2>&1 &
java_pid=$!

wait_for_phase "$tmp_dir/librdkafka.ready" "$librdkafka_pid" \
    "$tmp_dir/librdkafka.log" librdkafka "split and commit all three partitions"
wait_for_phase "$tmp_dir/java.ready" "$java_pid" "$tmp_dir/java.log" \
    java "split and commit all three partitions"
wait_for_phase "$tmp_dir/librdkafka.survivor" "$librdkafka_pid" \
    "$tmp_dir/librdkafka.log" librdkafka "reassign all partitions to its survivor"
wait_for_phase "$tmp_dir/java.survivor" "$java_pid" "$tmp_dir/java.log" \
    java "reassign all partitions to its survivor"

stop_broker
sleep 2
for marker in "$tmp_dir/librdkafka.rejoined" "$tmp_dir/java.rejoined"; do
    if [ -e "$marker" ]; then
        echo "client reported replacement rejoin before the original broker stopped" >&2
        exit 1
    fi
done
touch "$tmp_dir/arm"
wait_for_phase "$tmp_dir/librdkafka.armed" "$librdkafka_pid" \
    "$tmp_dir/librdkafka.log" librdkafka "arm its broker-epoch boundary"
wait_for_phase "$tmp_dir/java.armed" "$java_pid" "$tmp_dir/java.log" \
    java "arm its broker-epoch boundary"
for marker in "$tmp_dir/librdkafka.rejoined" "$tmp_dir/java.rejoined"; do
    if [ -e "$marker" ]; then
        echo "client reported replacement rejoin while the broker was down" >&2
        exit 1
    fi
done
start_broker

wait_for_phase "$tmp_dir/librdkafka.rejoined" "$librdkafka_pid" \
    "$tmp_dir/librdkafka.log" librdkafka "rejoin the replacement broker"
wait_for_phase "$tmp_dir/java.rejoined" "$java_pid" "$tmp_dir/java.log" \
    java "rejoin the replacement broker"

if ! wait "$librdkafka_pid"; then
    cat "$tmp_dir/librdkafka.log" >&2
    echo "librdkafka client did not recover from broker replacement" >&2
    exit 1
fi
librdkafka_pid=
cat "$tmp_dir/librdkafka.log"
if ! wait "$java_pid"; then
    cat "$tmp_dir/java.log" >&2
    echo "Java client did not recover from broker replacement" >&2
    exit 1
fi
java_pid=
cat "$tmp_dir/java.log"

echo "pinned librdkafka 2.12.1 and Apache Kafka Java 4.2.0 proved two-member three-partition rebalancing and retained-survivor recovery"
