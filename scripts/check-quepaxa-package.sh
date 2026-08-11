#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
consumer_target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"

cd "$repo_root"
rm -rf target/package
cargo package \
  -p rhiza-core \
  -p rhiza-log \
  -p rhiza-obj-store \
  -p rhiza-archive \
  -p rhiza-quepaxa \
  --no-verify "$@"

shopt -s nullglob

find_package_archive() {
  local package="$1"
  local archives=("$repo_root"/target/package/"$package"-*.crate)
  if ((${#archives[@]} != 1)); then
    echo "expected exactly one $package package archive, found ${#archives[@]}" >&2
    return 1
  fi
  printf '%s\n' "${archives[0]}"
}

archive_root() {
  local archive="$1"
  local root
  if ! root="$({ tar -tzf "$archive"; } | awk -F/ '
    /^\// { exit 1 }
    {
      for (i = 1; i <= NF; i++) {
        if ($i == "" || $i == "." || $i == "..") exit 1
      }
      if (!($1 in roots)) {
        roots[$1] = 1
        root = $1
        count++
      }
    }
    END {
      if (count != 1) exit 1
      print root
    }
  ')"; then
    echo "package archive has an unsafe or ambiguous root: $archive" >&2
    return 1
  fi
  printf '%s\n' "$root"
}

core_archive="$(find_package_archive rhiza-core)"
log_archive="$(find_package_archive rhiza-log)"
obj_store_archive="$(find_package_archive rhiza-obj-store)"
archive_archive="$(find_package_archive rhiza-archive)"
quepaxa_archive="$(find_package_archive rhiza-quepaxa)"
core_root="$(archive_root "$core_archive")"
log_root="$(archive_root "$log_archive")"
obj_store_root="$(archive_root "$obj_store_archive")"
archive_root_dir="$(archive_root "$archive_archive")"
quepaxa_root="$(archive_root "$quepaxa_archive")"

tar -xzf "$core_archive" -C "$work_dir"
tar -xzf "$log_archive" -C "$work_dir"
tar -xzf "$obj_store_archive" -C "$work_dir"
tar -xzf "$archive_archive" -C "$work_dir"
tar -xzf "$quepaxa_archive" -C "$work_dir"

mkdir -p "$work_dir/consumer/src"
cat >"$work_dir/consumer/Cargo.toml" <<EOF
[package]
name = "quepaxa-package-smoke"
version = "0.0.0"
edition = "2021"

[dependencies]
rhiza-quepaxa = { path = "$work_dir/$quepaxa_root" }

[patch.crates-io]
rhiza-core = { path = "$work_dir/$core_root" }
rhiza-log = { path = "$work_dir/$log_root" }
rhiza-obj-store = { path = "$work_dir/$obj_store_root" }
rhiza-archive = { path = "$work_dir/$archive_root_dir" }
EOF

cat >"$work_dir/consumer/src/main.rs" <<'EOF'
use rhiza_quepaxa::{Command, CommandKind, Membership};

fn main() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = Command::new(CommandKind::Deterministic, b"smoke".to_vec());
    assert_eq!(membership.quorum_size(), 2);
    assert_eq!(command.payload(), b"smoke");
}
EOF

# The extracted package sources must remain outside the workspace, but their
# build artifacts need not be duplicated in the system temporary volume. Reuse
# the caller's Cargo target so the full local CI gate has one bounded build
# cache instead of compiling the dependency graph into a second multi-GiB tree.
CARGO_TARGET_DIR="$consumer_target_dir" \
  cargo run --quiet --manifest-path "$work_dir/consumer/Cargo.toml"
