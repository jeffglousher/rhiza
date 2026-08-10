#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
contract_file="$repo_root/docs/storage-format-compatibility.md"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

extract_contract() {
  awk '/^```json storage-format-contract$/ { found=1; next } found && /^```$/ { exit } found { print }' "$1"
}

validate_contract() {
  jq -e --argjson versions "$1" '
    def required: [
      "qlog-frame", "qlog-truncate-intent", "qlog-compact-intent", "qlog-recovery-anchor",
      "recovery-anchor-payload", "recorder-wal", "recorder-head", "recorder-configuration",
      "recorder-storage-generation",
      "recorder-slot-state", "recorder-transition-intent", "recorder-configuration-head-intent",
      "recorder-command-cache", "sql-qwal", "sql-control-sidecar", "checkpoint-manifest",
      "checkpoint-snapshot-sql", "checkpoint-snapshot-graph", "checkpoint-snapshot-kv",
      "checkpoint-segment", "archive-record", "archive-manifest", "archive-snapshot-sql",
      "archive-snapshot-graph", "archive-snapshot-kv", "gc-control"
    ];
    .contract_version == 1 and (.artifacts | type == "array") and
    ([.artifacts[].id] | length == (unique | length)) and
    ([.artifacts[].id] | sort) == (required | sort) and
    (.artifacts | all(.[];
      ([keys[] | select(. != "profile" and . != "fingerprint")] | sort) == ["activation_sync_boundary","artifact","fixture_migrator_status","id","mixed_binary_policy","n_minus_1_read_policy","offline_migration_required","owner","path","reader_behavior","rollback","version","write_n_policy","writer_behavior"] and
      (.id | type == "string" and test("^[a-z0-9-]+$")) and
      (.artifact | type == "string" and length > 0) and (.path | type == "string" and length > 0) and
      (. as $artifact | ($artifact.version | type == "number" and floor == . and . > 0) and $artifact.version == $versions[$artifact.id]) and
      (if .id == "recorder-storage-generation" then
         .reader_behavior == "exact-bounded-read-through-anchored-directory" and
         .writer_behavior == "fresh-root-only-atomic-publication" and
         .n_minus_1_read_policy == "reject-missing-or-mismatched" and
         .offline_migration_required == false and .rollback == "operator-reset-required" and
         (.fingerprint | type == "string" and length > 0)
       else
         .reader_behavior == "reject-noncurrent" and .writer_behavior == "writes-current-only" and
         .n_minus_1_read_policy == "unsupported" and .offline_migration_required == true and
         .rollback == "no-format-rollback-support" and (has("fingerprint") | not)
       end) and .write_n_policy == "current-only" and
      .mixed_binary_policy == "not-supported-across-format-change" and
      (.owner | IN("rhiza-log", "rhiza-core", "rhiza-quepaxa", "rhiza-sql", "rhiza-graph", "rhiza-kv", "rhiza-archive")) and
      (.activation_sync_boundary | type == "string" and length > 0) and
      .fixture_migrator_status == "not-provided" and
      (if (.id | endswith("-snapshot-sql") or endswith("-snapshot-graph") or endswith("-snapshot-kv")) then
        (.profile | IN("sql", "graph", "kv")) and .owner == ("rhiza-" + .profile)
       else has("profile") | not end)
    )) and
    ([.artifacts[] | select(.id == "recorder-wal") | .path] == ["<recorder-root>/recorder.wal"]) and
    ([.artifacts[] | select(.id == "sql-qwal") | .path] == ["QEFX effect bundle referenced by the replicated log entry"]) and
    ([.artifacts[] | select(.id == "checkpoint-manifest") | .path] == ["rhiza/{cluster}/checkpoints/epoch-{epoch}/config-{config}/generation-{generation}/manifest.json"]) and
    ([.artifacts[] | select(.id | startswith("checkpoint-snapshot-")) | .path] | unique) == ["rhiza/{cluster}/checkpoints/epoch-{epoch}/config-{config}/generation-{generation}/snapshots/{index}-{hash}-{digest}-{fingerprint}.snapshot"] and
    ([.artifacts[] | select(.id == "archive-manifest") | .path] == ["rhiza/{cluster}/archive/manifest.json"]) and
    ([.artifacts[] | select(.id | startswith("archive-snapshot-")) | .path] | unique) == ["rhiza/{cluster}/archive/snapshots/epoch-{epoch}/snapshot-{index}-{executor_fingerprint}.snapshot"]
  ' "$2" >/dev/null
}

const_version() {
  local file="$1" constant="$2"
  rg -o "(pub )?const ${constant}: [^=]+ = [0-9]+" "$repo_root/$file" | head -n1 | awk '{print $NF}'
}

magic_version() {
  local file="$1" constant="$2" line
  line="$(rg -F "${constant}" "$repo_root/$file" | head -n1)"
  [[ "$line" =~ \\x([0-9A-Fa-f]{2}) ]] || return 1
  printf '%d\n' "$((16#${BASH_REMATCH[1]}))"
}

function_version() {
  local file="$1" function_name="$2"
  awk -v function_name="$function_name" '
    $0 ~ ("fn " function_name "\\(") { inside=1 }
    inside && /put_u16/ && /, [0-9]+\);/ {
      value=$0
      sub(/^.*,[[:space:]]*/, "", value)
      sub(/\);.*$/, "", value)
      print value
      exit
    }
  ' "$repo_root/$file"
}

require_literal() {
  local file="$1" literal="$2"
  rg -F "$literal" "$repo_root/$file" >/dev/null || {
    echo "missing authoritative storage parser marker: $file: $literal" >&2
    exit 1
  }
}

source_versions="$(jq -n \
  --argjson qlog "$(const_version crates/rhiza-log/src/lib.rs QLOG_FORMAT_VERSION)" \
  --argjson truncate "$(const_version crates/rhiza-log/src/lib.rs TRUNCATE_INTENT_VERSION)" \
  --argjson compact "$(const_version crates/rhiza-log/src/lib.rs COMPACT_INTENT_VERSION)" \
  --argjson anchor "$(const_version crates/rhiza-log/src/lib.rs ANCHOR_VERSION)" \
  --argjson recovery "$(const_version crates/rhiza-core/src/lib.rs RECOVERY_ANCHOR_FORMAT_VERSION)" \
  --argjson recorder_wal "$(const_version crates/rhiza-quepaxa/src/lib.rs RECORDER_WAL_VERSION)" \
  --argjson recorder_generation 1 \
  --argjson recorder_head "$(const_version crates/rhiza-quepaxa/src/lib.rs RECORDED_HEAD_VERSION)" \
  --argjson configuration "$(const_version crates/rhiza-quepaxa/src/lib.rs CONFIGURATION_STATE_VERSION)" \
  --argjson slot "$(const_version crates/rhiza-quepaxa/src/lib.rs RECORDER_STATE_VERSION)" \
  --argjson transition "$(function_version crates/rhiza-quepaxa/src/lib.rs encode_transition_intent)" \
  --argjson configuration_head "$(function_version crates/rhiza-quepaxa/src/lib.rs encode_configuration_head_intent)" \
  --argjson command_cache "$(function_version crates/rhiza-quepaxa/src/lib.rs encode_stored_command)" \
  --argjson sql_qwal "$(magic_version crates/rhiza-sql/src/qwal.rs QWAL_V3_MAGIC)" \
  --argjson sql_control "$(const_version crates/rhiza-sql/src/control.rs CONTROL_SCHEMA_VERSION)" \
  --argjson checkpoint "$(const_version crates/rhiza-archive/src/lib.rs CHECKPOINT_FORMAT_VERSION)" \
  --argjson segment "$(const_version crates/rhiza-archive/src/lib.rs CHECKPOINT_SEGMENT_FORMAT_VERSION)" \
  --argjson archive "$(const_version crates/rhiza-archive/src/lib.rs ARCHIVE_FORMAT_VERSION)" \
  --argjson gc "$(const_version crates/rhiza-archive/src/lib.rs GC_FORMAT_VERSION)" \
  --argjson sql_snapshot "$(magic_version crates/rhiza-sql/src/lib.rs QWAL_SNAPSHOT_V3_MAGIC)" \
  --argjson graph_snapshot "$(const_version crates/rhiza-graph/src/lib.rs SNAPSHOT_WIRE_VERSION)" \
  --argjson kv_snapshot "$(const_version crates/rhiza-kv/src/lib.rs SNAPSHOT_WIRE_VERSION)" \
  '{"qlog-frame":$qlog,"qlog-truncate-intent":$truncate,"qlog-compact-intent":$compact,"qlog-recovery-anchor":$anchor,"recovery-anchor-payload":$recovery,"recorder-wal":$recorder_wal,"recorder-storage-generation":$recorder_generation,"recorder-head":$recorder_head,"recorder-configuration":$configuration,"recorder-slot-state":$slot,"recorder-transition-intent":$transition,"recorder-configuration-head-intent":$configuration_head,"recorder-command-cache":$command_cache,"sql-qwal":$sql_qwal,"sql-control-sidecar":$sql_control,"checkpoint-manifest":$checkpoint,"checkpoint-snapshot-sql":$sql_snapshot,"checkpoint-snapshot-graph":$graph_snapshot,"checkpoint-snapshot-kv":$kv_snapshot,"checkpoint-segment":$segment,"archive-record":$archive,"archive-manifest":$archive,"archive-snapshot-sql":$sql_snapshot,"archive-snapshot-graph":$graph_snapshot,"archive-snapshot-kv":$kv_snapshot,"gc-control":$gc}')"

require_literal crates/rhiza-sql/src/qwal.rs 'b"QWAL\0\x04"'
require_literal crates/rhiza-sql/src/qwal.rs 'strip_prefix(QWAL_V3_MAGIC)'
require_literal crates/rhiza-quepaxa/src/lib.rs 'const RECORDER_WAL_MAGIC: &[u8; 4] = b"QWAL";'
require_literal crates/rhiza-quepaxa/src/lib.rs 'read_u16(bytes, &mut cursor)? != RECORDER_WAL_VERSION'
require_literal crates/rhiza-archive/src/lib.rs 'format!("rhiza/{}/archive/manifest.json", self.cluster_id)'
require_literal crates/rhiza-archive/src/lib.rs 'fn snapshot_object_key(manifest: &SnapshotManifest) -> String'
require_literal crates/rhiza-archive/src/lib.rs '"rhiza/{}/archive/snapshots/epoch-{:020}/snapshot-{:020}"'
require_literal crates/rhiza-archive/src/lib.rs '"{prefix}-{}.snapshot"'
require_literal crates/rhiza-archive/src/lib.rs 'fn checkpoint_snapshot_key'
require_literal crates/rhiza-archive/src/lib.rs 'manifest.format_version != CHECKPOINT_FORMAT_VERSION'
require_literal crates/rhiza-sql/src/lib.rs 'strip_prefix(QWAL_SNAPSHOT_V3_MAGIC)'
require_literal crates/rhiza-graph/src/lib.rs 'snapshot envelope magic does not match RHGS'
require_literal crates/rhiza-graph/src/lib.rs 'version != SNAPSHOT_WIRE_VERSION'
require_literal crates/rhiza-kv/src/lib.rs 'snapshot envelope magic does not match RHKS'
require_literal crates/rhiza-kv/src/lib.rs 'version != SNAPSHOT_WIRE_VERSION'

extract_contract "$contract_file" > "$tmp/contract.json"
validate_contract "$source_versions" "$tmp/contract.json" || { echo "invalid storage-format compatibility contract" >&2; exit 1; }

# Fixtures exercise this same validator: a valid inventory must pass, while
# missing roles, duplicate QWAL identities, invalid policies, and unsupported
# migration/power-loss claims must fail.
cp "$tmp/contract.json" "$tmp/valid.json"
validate_contract "$source_versions" "$tmp/valid.json" || { echo "valid storage-format fixture rejected" >&2; exit 1; }
for mutation in \
  'del(.artifacts[] | select(.id == "gc-control"))' \
  'del(.artifacts[] | select(.id == "archive-snapshot-kv"))' \
  '.artifacts += [(.artifacts[] | select(.id == "sql-qwal") | .id = "recorder-wal")]' \
  '(.artifacts[] | select(.id == "checkpoint-manifest")).path = "manifest.json"' \
  '(.artifacts[] | select(.id == "archive-manifest")).path = "rhiza/{cluster}/archive/other.json"' \
  '(.artifacts[] | select(.id == "archive-snapshot-sql")).profile = "graph"' \
  '(.artifacts[] | select(.id == "sql-qwal")).n_minus_1_read_policy = "supported"' \
  '(.artifacts[] | select(.id == "sql-qwal")).fixture_migrator_status = "implemented"'; do
  jq "$mutation" "$tmp/contract.json" > "$tmp/invalid.json"
  if validate_contract "$source_versions" "$tmp/invalid.json"; then
    echo "invalid storage-format fixture accepted: $mutation" >&2
    exit 1
  fi
done

echo "storage-format compatibility contract: ok"
