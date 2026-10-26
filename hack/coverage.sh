#!/usr/bin/env bash
# Usage: hack/coverage.sh <coverprofile> <min-percent>
#
# Computes statement coverage over the core packages (the operator's own
# logic) and fails below the threshold. Test infrastructure that only exists
# to exercise the operator (simulated cluster, fake S3 server, conformance
# suite) and declarations-only packages (metrics) are excluded.
set -euo pipefail
profile=${1:?coverprofile}
min=${2:?min percent}
mod=github.com/SlateGitOrg/keeper/internal
core="retention|schedule|storage|mover|hooks|snapshot|controller|operator"
excluded="storage/s3test|storage/storagetest|simcluster"

filtered=$(mktemp)
trap 'rm -f "$filtered"' EXIT
head -1 "$profile" >"$filtered"
grep -E "^${mod}/(${core})/" "$profile" | grep -vE "^${mod}/(${excluded})/" >>"$filtered"

# Per-package and total statement coverage from the raw profile blocks.
awk -v min="$min" -v mod="$mod/" '
  NR == 1 { next }
  {
    split($1, loc, ":"); file = loc[1]; sub(mod, "", file)
    n = split(file, parts, "/"); pkg = parts[1]
    if (pkg == "storage" && n > 2) pkg = pkg "/" parts[2]
    key = $1
    stmts[key] = $2
    if ($3 > 0) hit[key] = 1
    owner[key] = pkg
  }
  END {
    for (k in stmts) {
      p = owner[k]; total[p] += stmts[k]; all += stmts[k]
      if (k in hit) { cov[p] += stmts[k]; allcov += stmts[k] }
    }
    for (p in total) printf "  %-12s %6.1f%%  (%d/%d statements)\n", p, 100 * cov[p] / total[p], cov[p], total[p] | "sort"
    close("sort")
    pct = 100 * allcov / all
    printf "core coverage: %.1f%% (%d/%d statements), threshold %d%%\n", pct, allcov, all, min
    if (pct < min) { print "FAIL: coverage below threshold"; exit 1 }
  }
' "$filtered"
