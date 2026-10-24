#!/usr/bin/env bash
# Runs the storage conformance suite and a mover round trip against a real
# MinIO server in Docker. Skips (exit 0) when Docker is not available.
set -euo pipefail
IMAGE=${MINIO_IMAGE:-chainguard/minio@sha256:ae73835075ee168f1745cc28d4cf5bbd6a5973e5609b2b2593280aaa495ea113}

if ! docker info >/dev/null 2>&1; then
  echo "integration: Docker is not available; skipping"
  exit 0
fi

name="keeper-minio-it-$$"
docker run -d --rm --name "$name" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=keeperadmin -e MINIO_ROOT_PASSWORD=keeper-secret-key \
  "$IMAGE" server /tmp/data >/dev/null
trap 'docker stop "$name" >/dev/null 2>&1 || true' EXIT

port=$(docker port "$name" 9000/tcp | head -1 | sed 's/.*://')
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${port}/minio/health/live" >/dev/null 2>&1; then break; fi
  sleep 0.5
done

KEEPER_IT_S3_ENDPOINT="http://127.0.0.1:${port}" KEEPER_IT_S3_HOST="127.0.0.1:${port}" \
  go test -tags integration -count=1 -run TestRealMinIO -v ./internal/storage/ 2>&1 | grep -E '^(=== RUN|--- |ok|FAIL|PASS)|_test.go'
