#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
contract_file="$repo_root/docs/storage-format-compatibility.md"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

fail() {
    printf '%s\n' "storage-format compatibility contract: $*" >&2
    exit 1
}

count_exact() {
    awk -v line="$2" '$0 == line { count++ } END { print count + 0 }' "$1"
}

require_exact_once() {
    [ "$(count_exact "$1" "$2")" = 1 ] || fail "expected one exact line: $2"
}

row_count() {
    awk '
        $0 == "## Canonical persisted-artifact matrix" { table = 1; next }
        table && /^## / { exit }
        table && /^\| / &&
            $0 != "| Artifact | Owner / path or key | Authority or reconstruction | Envelope / current reader → writer | Validation, failure, and rebuild |" &&
            $0 != "| --- | --- | --- | --- | --- |" { count++ }
        END { print count + 0 }
    ' "$1"
}

require_row() {
    file=$1
    row=$2
    shift 2
    line=$(awk -v prefix="| $row |" 'index($0, prefix) == 1 { print; count++ } END { if (count != 1) exit 1 }' "$file") || fail "expected one row: $row"
    for token in "$@"; do
        case $line in
            *"$token"*) ;;
            *) fail "row $row is missing token: $token" ;;
        esac
    done
}

const_version() {
    awk -v name="$2" '
        {
            field = $1 == "pub" ? $3 : $2
            sub(/:$/, "", field)
            if (($1 == "const" || ($1 == "pub" && $2 == "const")) && field == name && $NF ~ /^[0-9]+;$/) {
                sub(/;$/, "", $NF)
                print $NF
                count++
            }
        }
        END { if (count != 1) exit 1 }
    ' "$repo_root/$1" || fail "missing source constant: $1:$2"
}

const_product() {
    awk -v name="$2" '
        {
            field = $1 == "pub" ? $3 : $2
            sub(/:$/, "", field)
            if (($1 == "const" || ($1 == "pub" && $2 == "const")) && field == name) {
                expression = $0
                sub(/^[^=]*=[[:space:]]*/, "", expression)
                sub(/;[[:space:]]*$/, "", expression)
                gsub(/[[:space:]_]/, "", expression)
                count++
            }
        }
        END {
            if (count != 1 || expression !~ /^[0-9]+(\*[0-9]+)*$/) exit 1
            factors = split(expression, factor, "\\*")
            value = 1
            for (i = 1; i <= factors; i++) value *= factor[i]
            printf "%.0f\n", value
        }
    ' "$repo_root/$1" || fail "missing source constant product: $1:$2"
}

byte_magic() {
    awk -v name="$2" '
        {
            field = $1 == "pub" ? $3 : $2
            sub(/:$/, "", field)
            if (($1 == "const" || ($1 == "pub" && $2 == "const")) && field == name && match($0, /b"[^"]*"/)) {
                print substr($0, RSTART + 2, RLENGTH - 3)
                count++
            }
        }
        END { if (count != 1) exit 1 }
    ' "$repo_root/$1" || fail "missing source magic: $1:$2"
}

code() {
    printf '%s%s%s' "\`" "$1" "\`"
}

version_token() {
    printf '%s v%s' "$(code "$1")" "$2"
}

source_contains() {
    grep -F -- "$2" "$repo_root/$1" >/dev/null 2>&1 || fail "source anchor changed: $1:$2"
}

source_absent() {
    ! grep -F -- "$2" "$repo_root/$1" >/dev/null 2>&1 || fail "obsolete source token remains: $1:$2"
}

require_matching_successor_restore_versions() {
    [ "$1" = "$2" ] || fail "successor restore version drift: node v$1, HA v$2"
}

validate() {
    file=$1
    [ -f "$file" ] || fail "missing contract: $file"
    require_exact_once "$file" '# Persisted-format compatibility baseline'
    require_exact_once "$file" '## Canonical persisted-artifact matrix'
    require_exact_once "$file" '| Artifact | Owner / path or key | Authority or reconstruction | Envelope / current reader → writer | Validation, failure, and rebuild |'
    require_exact_once "$file" '| --- | --- | --- | --- | --- |'
    [ "$(row_count "$file")" = 19 ] || fail "expected 19 canonical rows"

    require_row "$file" 'Qlog segments' 'rhiza-log' "$(code '<qlog>/{start}-{end}.qlog')" "$qlog_token" 'reject mismatch before replay'
    require_row "$file" 'Qlog compaction controls' 'rhiza-log' "$(code '.truncate-intent')" "$truncate_token" "$compact_token" "$anchor_token" "$(code 'CONTROL_INTENT_MAX_BYTES') = $control_intent_max_bytes bytes (8 MiB)" 'fails closed'
    require_row "$file" 'Replicated command/effect payloads' 'rhiza-core' "$(code 'qlog/recorder/checkpoint payload')" "$qefx_token" 'Canonical bounded decode'
    require_row "$file" 'Recorder generation and lock' 'rhiza-quepaxa' "$(code '.rhiza-storage-generation')" 'clean-v1' 'reject open/install'
    require_row "$file" 'Recorder decision state' 'rhiza-quepaxa' "$(code 'recorder.wal')" "\`QWAL\` v$recorder_wal_version" 'conflict fails closed'
    require_row "$file" 'Recorder configuration/commands' 'rhiza-quepaxa' "$(code 'configuration.rec')" "\`QCON\` v$configuration_version" 'content-hash checks'
    require_row "$file" 'Recorder effects and GC fence' 'rhiza-quepaxa' "$(code '.effect-bundle-gc-anchor.rec')" "$qegc_token" 'staged chunk ACKs are process-local' 'restage every chunk after restart' 'unsafe deletion fails closed'
    require_row "$file" 'SQL materialization and control' 'rhiza-sql' "$(code '.rhiza-control.sqlite')" "\`QCTL\` schema v$sql_control_version" 'install snapshot rather than auto-migrate'
    require_row "$file" 'KV materialization' 'rhiza-kv' "$(code '<data>/kv/data.redb')" "$kv_snapshot_token" 'replay continuity'
    require_row "$file" 'Graph materialization' 'rhiza-graph' "$(code '<data>/ladybug/graph.lbug')" "$graph_snapshot_token" 'checked before use'
    require_row "$file" 'Archive history' 'rhiza-archive' "$(code 'rhiza/{cluster}/archive/manifest.json')" "Archive v$archive_version" 'CAS publication'
    require_row "$file" 'Checkpoint generation' 'rhiza-archive' "$(code 'rhiza/{cluster}/checkpoints/epoch-{e}/config-{c}-digest-{digest}/generation-{g}/manifest.json')" "Checkpoint v$checkpoint_version" 'configuration digest' 'before install'
    require_row "$file" 'Checkpoint publication receipts' 'rhiza-archive' "$(code 'receipts/{holder-hash}/{manifest-digest}.json')" 'same-slot evidence conflicts'
    require_row "$file" 'Archive control and leases' 'rhiza-archive' "$(code 'gc/control.json')" "GC v$gc_version" 'fence deletion'
    require_row "$file" 'Restore/install state' 'rhiza-node' "$(code '.rhiza-checkpoint-install.json')" "restore intent v$restore_intent_version" "install receipt v$restore_install_version" "local marker v$local_marker_version" 'configuration-digest-bound' 'partial activation'
    require_row "$file" 'Restore QEFX and recovery ownership' 'rhiza-node' "$(code 'consensus/pending-qefx-gc.json')" 'aggregate limits' 'digest-bound owner identity' 'fail closed'
    require_row "$file" 'Successor/prestage activation' 'rhiza-node' "$(code '.successor-prestage.{lock,intent,ready,published,finalized}')" "successor restore receipt v$successor_restore_version" "prestage identity v$successor_prestage_version" 'membership digests' 'not activated'
    require_row "$file" 'Completion markers' 'rhiza-node' "$(code '<data-dir>/<portable-marker-name>')" 'caller-supplied validated portable relative name' 'receipt hash bind marker'
    require_row "$file" 'Admin operation ledger' 'rhiza-node' "$(code '<data>/admin-operations-v2.json')" "version $admin_ledger_version" "$(code 'ADMIN_OPERATION_LEDGER_MAX_BYTES') = $admin_ledger_max_bytes" "$(code 'ADMIN_OPERATION_LEDGER_MAX_RECORDS') = $admin_ledger_max_records" "$(code 'ADMIN_OPERATION_RESULT_MAX_BYTES') = $admin_result_max_bytes" "$(code 'ADMIN_OPERATION_RETENTION_SECS') = $admin_retention_secs" 'SHA-256 request fingerprint' '503 unavailable'

    source_contains crates/rhiza-log/src/lib.rs 'pub const QLOG_FORMAT_VERSION'
    source_contains crates/rhiza-quepaxa/src/lib.rs 'const RECORDER_WAL_MAGIC: &[u8; 4] = b"QWAL";'
    source_contains crates/rhiza-quepaxa/src/lib.rs 'const STAGED_EFFECT_RESTAGE_REQUIRED: &str ='
    source_contains crates/rhiza-archive/src/lib.rs 'pub const CHECKPOINT_FORMAT_VERSION'
    source_contains crates/rhiza-node/src/durability.rs 'const RESTORE_RECEIPT_FILE: &str = ".rhiza-checkpoint-install.json";'
    source_contains crates/rhiza-node/src/admin.rs 'const ADMIN_OPERATION_LEDGER_FILE: &str = "admin-operations-v2.json";'
    source_absent crates/rhiza-node/src/admin.rs 'admin-operations-v1'
}

qlog_version=$(const_version crates/rhiza-log/src/lib.rs QLOG_FORMAT_VERSION)
qlog_magic=$(byte_magic crates/rhiza-log/src/lib.rs QLOG_MAGIC)
qlog_token=$(version_token "$qlog_magic" "$qlog_version")
truncate_magic=$(byte_magic crates/rhiza-log/src/lib.rs TRUNCATE_INTENT_MAGIC)
truncate_version=$(const_version crates/rhiza-log/src/lib.rs TRUNCATE_INTENT_VERSION)
truncate_token=$(version_token "$truncate_magic" "$truncate_version")
compact_magic=$(byte_magic crates/rhiza-log/src/lib.rs COMPACT_INTENT_MAGIC)
compact_version=$(const_version crates/rhiza-log/src/lib.rs COMPACT_INTENT_VERSION)
compact_token=$(version_token "$compact_magic" "$compact_version")
anchor_magic=$(byte_magic crates/rhiza-log/src/lib.rs ANCHOR_MAGIC)
anchor_version=$(const_version crates/rhiza-log/src/lib.rs ANCHOR_VERSION)
anchor_token=$(version_token "$anchor_magic" "$anchor_version")
control_intent_max_bytes=$(const_product crates/rhiza-log/src/lib.rs CONTROL_INTENT_MAX_BYTES)
qefx_magic=$(byte_magic crates/rhiza-core/src/lib.rs EXTERNAL_EFFECT_COMMAND_MAGIC)
qefx_token=$(code "$qefx_magic")
recorder_wal_version=$(const_version crates/rhiza-quepaxa/src/lib.rs RECORDER_WAL_VERSION)
configuration_version=$(const_version crates/rhiza-quepaxa/src/lib.rs CONFIGURATION_STATE_VERSION)
qegc_magic=$(byte_magic crates/rhiza-quepaxa/src/lib.rs EFFECT_BUNDLE_GC_ANCHOR_MAGIC)
qegc_version=$(const_version crates/rhiza-quepaxa/src/lib.rs EFFECT_BUNDLE_GC_ANCHOR_VERSION)
qegc_token=$(version_token "$qegc_magic" "$qegc_version")
sql_control_version=$(const_version crates/rhiza-sql/src/control.rs CONTROL_SCHEMA_VERSION)
kv_snapshot_magic=$(byte_magic crates/rhiza-kv/src/lib.rs SNAPSHOT_WIRE_MAGIC)
kv_snapshot_version=$(const_version crates/rhiza-kv/src/lib.rs SNAPSHOT_WIRE_VERSION)
kv_snapshot_token=$(version_token "$kv_snapshot_magic" "$kv_snapshot_version")
graph_snapshot_magic=$(byte_magic crates/rhiza-graph/src/lib.rs SNAPSHOT_WIRE_MAGIC)
graph_snapshot_version=$(const_version crates/rhiza-graph/src/lib.rs SNAPSHOT_WIRE_VERSION)
graph_snapshot_token=$(version_token "$graph_snapshot_magic" "$graph_snapshot_version")
archive_version=$(const_version crates/rhiza-archive/src/lib.rs ARCHIVE_FORMAT_VERSION)
checkpoint_version=$(const_version crates/rhiza-archive/src/lib.rs CHECKPOINT_FORMAT_VERSION)
gc_version=$(const_version crates/rhiza-archive/src/lib.rs GC_FORMAT_VERSION)
restore_intent_version=$(const_version crates/rhiza-node/src/durability.rs RESTORE_INTENT_FORMAT_VERSION)
restore_install_version=$(const_version crates/rhiza-node/src/durability.rs RESTORE_INSTALL_FORMAT_VERSION)
successor_prestage_version=$(const_version crates/rhiza-node/src/durability.rs SUCCESSOR_PRESTAGE_FORMAT_VERSION)
successor_restore_node_version=$(const_version crates/rhiza-node/src/durability.rs SUCCESSOR_RESTORE_FORMAT_VERSION)
successor_restore_ha_version=$(const_version crates/rhiza/src/ha.rs SUCCESSOR_RESTORE_RECEIPT_FORMAT_VERSION)
require_matching_successor_restore_versions "$successor_restore_node_version" "$successor_restore_ha_version"
successor_restore_version=$successor_restore_node_version
local_marker_version=$(const_version crates/rhiza/src/ha.rs LOCAL_CHECKPOINT_IDENTITY_FORMAT_VERSION)
admin_ledger_version=$(const_version crates/rhiza-node/src/admin.rs ADMIN_OPERATION_LEDGER_VERSION)
admin_ledger_max_bytes=$(const_product crates/rhiza-node/src/admin.rs ADMIN_OPERATION_LEDGER_MAX_BYTES)
admin_ledger_max_records=$(const_version crates/rhiza-node/src/admin.rs ADMIN_OPERATION_LEDGER_MAX_RECORDS)
admin_result_max_bytes=$(const_product crates/rhiza-node/src/admin.rs ADMIN_OPERATION_RESULT_MAX_BYTES)
admin_retention_secs=$(const_product crates/rhiza-node/src/admin.rs ADMIN_OPERATION_RETENTION_SECS)

validate "$contract_file"

missing_row="$tmp/missing-row.md"
awk 'index($0, "| Qlog segments |") != 1' "$contract_file" > "$missing_row"
if (validate "$missing_row") >/dev/null 2>&1; then
    fail 'negative test accepted a missing required row'
fi

negative_token() {
    name=$1
    old=$2
    changed="$tmp/$name.md"
    awk '
        BEGIN {
            old = ARGV[1]
            ARGV[1] = ""
        }
        !replaced && index($0, old) {
            print substr($0, 1, index($0, old) - 1) "`BROKEN` v999" substr($0, index($0, old) + length(old))
            replaced = 1
            next
        }
        { print }
        END { if (!replaced) exit 1 }
    ' "$old" "$contract_file" > "$changed" || fail "negative fixture is missing source-backed token: $name"
    if (validate "$changed") >/dev/null 2>&1; then
        fail "negative test accepted changed source-backed token: $name"
    fi
}

negative_token qlog "$qlog_token"
negative_token truncate "$truncate_token"
negative_token compact "$compact_token"
negative_token anchor "$anchor_token"
negative_token control-intent-bound "$(code 'CONTROL_INTENT_MAX_BYTES') = $control_intent_max_bytes bytes (8 MiB)"
negative_token qefx "$qefx_token"
negative_token qegc "$qegc_token"
negative_token staged-effect-process-local 'staged chunk ACKs are process-local'
negative_token staged-effect-restage 'restage every chunk after restart'
negative_token kv-snapshot "$kv_snapshot_token"
negative_token graph-snapshot "$graph_snapshot_token"
negative_token checkpoint "Checkpoint v$checkpoint_version"
negative_token restore-intent "restore intent v$restore_intent_version"
negative_token restore-install "install receipt v$restore_install_version"
negative_token successor-prestage "prestage identity v$successor_prestage_version"
negative_token successor-restore "successor restore receipt v$successor_restore_version"
negative_token local-marker "local marker v$local_marker_version"
negative_token admin-ledger-version "version $admin_ledger_version"
negative_token admin-ledger-bytes "$(code 'ADMIN_OPERATION_LEDGER_MAX_BYTES') = $admin_ledger_max_bytes"
negative_token admin-ledger-records "$(code 'ADMIN_OPERATION_LEDGER_MAX_RECORDS') = $admin_ledger_max_records"
negative_token admin-result-bytes "$(code 'ADMIN_OPERATION_RESULT_MAX_BYTES') = $admin_result_max_bytes"
negative_token admin-retention "$(code 'ADMIN_OPERATION_RETENTION_SECS') = $admin_retention_secs"

if (require_matching_successor_restore_versions "$((successor_restore_node_version + 1))" "$successor_restore_ha_version") >/dev/null 2>&1; then
    fail 'negative test accepted node-side successor restore version drift'
fi
if (require_matching_successor_restore_versions "$successor_restore_node_version" "$((successor_restore_ha_version + 1))") >/dev/null 2>&1; then
    fail 'negative test accepted HA-side successor restore version drift'
fi

printf '%s\n' 'storage-format compatibility contract: ok (missing-row and source-token negative tests passed)'
