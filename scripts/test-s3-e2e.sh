#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$project_dir/compose.yaml"
bucket="walstream-e2e-$(date +%s)-$$"
backend=${WALSTREAM_E2E_BACKEND-rustfs}

case "$backend" in
    rustfs)
        store_port=9000
        ;;
    seaweedfs)
        store_port=8333
        ;;
    minio)
        store_port=9000
        ;;
    *)
        echo "unsupported WALSTREAM_E2E_BACKEND '$backend' (expected rustfs, seaweedfs, or minio)" >&2
        exit 2
        ;;
esac

container_name="walstream-e2e-$backend"

cleanup() {
    container stop "$container_name" >/dev/null 2>&1 || true
    container delete "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if ! container system status >/dev/null 2>&1; then
    container system start
fi

WALSTREAM_E2E_BUCKET="$bucket" container-compose --file "$compose_file" up -d "$backend"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to read the private Apple Container address" >&2
    exit 1
fi

attempt=0
until store_address=$(container inspect "$container_name" 2>/dev/null \
    | jq -r '.[0].status.networks[0].ipv4Address // empty') \
    && [ -n "$store_address" ] \
    && store_ip=${store_address%/*} \
    && container run --rm \
        --env "MC_HOST_local=http://walstream:walstream-secret@$store_ip:$store_port" \
        quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z \
        mb --ignore-existing "local/$bucket" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo "$backend did not become ready" >&2
        exit 1
    fi
    echo "waiting for $backend S3 endpoint ($attempt/60)" >&2
    sleep 1
done

cd "$project_dir"
AWS_ACCESS_KEY_ID=walstream \
AWS_SECRET_ACCESS_KEY=walstream-secret \
AWS_REGION=us-east-1 \
WALSTREAM_E2E_BUCKET="$bucket" \
WALSTREAM_E2E_ENDPOINT="http://$store_ip:$store_port" \
cargo test --test s3_e2e -- --ignored --nocapture
