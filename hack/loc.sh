#!/usr/bin/env bash
# Line counts (wc -l, all lines) by category, excluding generated code.
set -euo pipefail
cd "$(dirname "$0")/.."
count() { if [ $# -eq 0 ]; then echo 0; else cat "$@" | wc -l | tr -d ' '; fi; }
files() { git ls-files "$@" | grep -v zz_generated || true; }

operator=$(files 'api/*.go' 'cmd/keeper/*.go' 'internal/*.go' | grep -v '_test.go$' | grep -vE 'internal/(simcluster|storage/s3test|storage/storagetest)/')
infra=$(files 'internal/simcluster/*.go' 'internal/storage/s3test/*.go' 'internal/storage/storagetest/*.go' | grep -v '_test.go$')
demo=$(files 'cmd/keeper-demo/*.go')
tests=$(files '*_test.go')

printf "%-44s %6s\n" "operator source (api, cmd/keeper, internal)" "$(count $operator)"
printf "%-44s %6s\n" "test infrastructure (simcluster, fake S3)" "$(count $infra)"
printf "%-44s %6s\n" "demo (cmd/keeper-demo)" "$(count $demo)"
printf "%-44s %6s\n" "tests (*_test.go)" "$(count $tests)"
printf "%-44s %6s\n" "generated (zz_generated.deepcopy.go)" "$(count api/v1alpha1/zz_generated.deepcopy.go)"
