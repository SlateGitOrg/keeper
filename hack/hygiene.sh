#!/usr/bin/env bash
# Repository hygiene gate (part of `make check`): no build artefacts or
# executables, no oversized files, and no absolute home-directory paths in
# tracked files. Run from the repository root.
set -euo pipefail

max_bytes=${HYGIENE_MAX_BYTES:-1048576} # 1 MiB per tracked file
fail=0

while IFS= read -r -d '' f; do
	[ -f "$f" ] || continue
	size=$(wc -c <"$f" | tr -d ' ')
	if [ "$size" -gt "$max_bytes" ]; then
		echo "hygiene: $f is $size bytes (limit $max_bytes)"
		fail=1
	fi
	magic=$(head -c 4 "$f" | od -An -tx1 | tr -d ' \n')
	case "$magic" in
	cffaedfe | cefaedfe | cafebabe | 7f454c46 | 4d5a*)
		echo "hygiene: $f is a compiled executable (magic $magic)"
		fail=1
		;;
	esac
done < <(git ls-files -z)

# Home-directory prefixes are assembled at runtime so this script does not
# match itself.
home_re="/(Use""rs|ho""me)/[A-Za-z0-9._-]+/"
if git grep -nIE "$home_re" -- . ; then
	echo "hygiene: absolute home-directory paths found in tracked files"
	fail=1
fi

if [ "$fail" -ne 0 ]; then
	exit 1
fi
echo "hygiene: $(git ls-files | wc -l | tr -d ' ') tracked files OK"
