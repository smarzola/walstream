#!/bin/sh
# Real broker walkthrough, using only uniquely owned disposable resources.
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_id="$(date +%s)-$$"
store_name="walstream-index-$run_id"
bucket="walstream-index-$run_id"
cleanup() {
    container stop "$store_name" >/dev/null 2>&1 || true
    container delete "$store_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
container system status >/dev/null 2>&1 || container system start
container run -d --name "$store_name" \
    --env RUSTFS_ACCESS_KEY=walstream --env RUSTFS_SECRET_KEY=walstream-secret \
    --env RUSTFS_ADDRESS=:9000 --env RUSTFS_CONSOLE_ENABLE=false \
    rustfs/rustfs:1.0.0-beta.12 /data
store_ip=$(container inspect "$store_name" | jq -r '.[0].status.networks[0].ipv4Address | split("/")[0]')
attempt=0
until container run --rm --env "MC_HOST_local=http://walstream:walstream-secret@$store_ip:9000" \
    quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z mb --ignore-existing "local/$bucket" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then container logs "$store_name"; exit 1; fi
    sleep 1
done
cd "$project_dir"
cargo build --release --bin walstream --example log_index_probe
AWS_ACCESS_KEY_ID=walstream AWS_SECRET_ACCESS_KEY=walstream-secret \
    ./target/release/examples/log_index_probe \
    --bucket "$bucket" --region us-east-1 --endpoint "http://$store_ip:9000" --allow-http \
    --prefix "walkthrough-$run_id" --cluster-id index "$@"
