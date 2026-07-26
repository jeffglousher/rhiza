#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_root"

if (( $# != 0 )); then
  echo "this release guard has a fixed package set and accepts no arguments" >&2
  exit 64
fi

# Package the dependency tiers that do not rely on an unpublished workspace
# version. The protected crates.io workflow publishes these first, then packages
# and publishes node, facade, and client after each dependency reaches the index.
cargo package --locked --allow-dirty --no-verify \
  -p rhiza-core \
  -p rhiza-log \
  -p rhiza-obj-store \
  -p rhiza-quepaxa \
  -p rhiza-archive \
  -p rhiza-sql \
  -p rhiza-graph \
  -p rhiza-kv
