#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$project_dir/compose.yaml"
bucket="walstream-e2e-$(date +%s)-$$"

cleanup() {
    container-compose --file "$compose_file" down >/dev/null 2>&1 || true
    container delete walstream-e2e-minio >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

if ! container system status >/dev/null 2>&1; then
    container system start
fi

WALSTREAM_E2E_BUCKET="$bucket" container-compose --file "$compose_file" up -d minio

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to read the private Apple Container address" >&2
    exit 1
fi

attempt=0
until minio_address=$(container inspect walstream-e2e-minio 2>/dev/null \
    | jq -r '.[0].status.networks[0].ipv4Address // empty') \
    && [ -n "$minio_address" ] \
    && minio_ip=${minio_address%/*} \
    && curl --fail --silent --show-error "http://$minio_ip:9000/minio/health/ready" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo "MinIO did not become ready" >&2
        exit 1
    fi
    sleep 1
done

container run --rm \
    --env "MC_HOST_local=http://walstream:walstream-secret@$minio_ip:9000" \
    quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z \
    mb --ignore-existing "local/$bucket"

cd "$project_dir"
AWS_ACCESS_KEY_ID=walstream \
AWS_SECRET_ACCESS_KEY=walstream-secret \
AWS_REGION=us-east-1 \
WALSTREAM_E2E_BUCKET="$bucket" \
WALSTREAM_E2E_ENDPOINT="http://$minio_ip:9000" \
cargo test --test s3_e2e -- --ignored --nocapture
