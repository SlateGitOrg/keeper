#!/usr/bin/env bash
# Counts passing tests (top-level and subtests) across the module, envtest included.
set -euo pipefail
cd "$(dirname "$0")/.."
assets=$(go tool setup-envtest use -i "${ENVTEST_K8S_VERSION:-1.37.0}" -p path)
KUBEBUILDER_ASSETS="$assets" KEEPER_REQUIRE_ENVTEST=1 go test -count=1 -json ./... 2>/dev/null | python3 -c '
import json, sys
top, sub, failed, skipped = set(), set(), 0, 0
for line in sys.stdin:
    try:
        e = json.loads(line)
    except ValueError:
        continue
    t = e.get("Test")
    if not t:
        continue
    if e["Action"] == "pass":
        (sub if "/" in t else top).add((e["Package"], t))
    elif e["Action"] == "fail":
        failed += 1
    elif e["Action"] == "skip":
        skipped += 1
envtest = sum(1 for p, _ in top if p.endswith("/internal/controller"))
print(f"{len(top)} tests ({envtest} envtest) + {len(sub)} subtests passed, {failed} failed, {skipped} skipped")
sys.exit(1 if failed else 0)
'
