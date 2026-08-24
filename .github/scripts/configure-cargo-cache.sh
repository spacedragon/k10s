#!/usr/bin/env bash
set -euo pipefail

: "${RUNNER_TOOL_CACHE:?RUNNER_TOOL_CACHE is required}"
: "${GITHUB_ENV:?GITHUB_ENV is required}"

cache_root="${RUNNER_TOOL_CACHE}/k10s/cargo-target-weekly"
generation="$(date -u +%G-W%V)"
target_dir="${cache_root}/${generation}"

mkdir -p "$target_dir"

# Keep the current and previous weekly generations. Cargo target directories
# otherwise grow indefinitely as toolchains, features, and source revisions
# change on a persistent self-hosted runner.
while IFS= read -r stale_generation; do
  [[ "$stale_generation" =~ ^[0-9]{4}-W[0-9]{2}$ ]]
  rm -rf -- "${cache_root}/${stale_generation}"
done < <(
  find "$cache_root" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
    | sed -n '/^[0-9]\{4\}-W[0-9]\{2\}$/p' \
    | sort -r \
    | tail -n +3
)

echo "CARGO_TARGET_DIR=$target_dir" >> "$GITHUB_ENV"
