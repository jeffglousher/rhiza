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
    rg -Fxc -- "$2" "$1" 2>/dev/null || true
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
    line=$(rg -m1 -o "(pub )?const $2: [^=]+ = [0-9]+" "$repo_root/$1") || fail "missing source constant: $1:$2"
    printf '%s\n' "$line" | awk '{ print $NF }'
}

code() {
    printf '%s%s%s' "\`" "$1" "\`"
}

validate() {
    file=$1
    [ -f "$file" ] || fail "missing contract: $file"
    require_exact_once "$file" '# Persisted-format compatibility baseline'
    require_exact_once "$file" '## Canonical persisted-artifact matrix'
    require_exact_once "$file" '| Artifact | Owner / path or key | Authority or reconstruction | Envelope / current reader → writer | Validation, failure, and rebuild |'
    require_exact_once "$file" '| --- | --- | --- | --- | --- |'
    [ "$(row_count "$file")" = 19 ] || fail "expected 19 canonical rows"

    require_row "$file" 'Qlog segments' 'rhiza-log' "$(code '<qlog>/{start}-{end}.qlog')" "\`QLOG\` v$qlog_version" 'reject mismatch before replay'
    require_row "$file" 'Qlog compaction controls' 'rhiza-log' "$(code '.truncate-intent')" "\`QANC\` v$anchor_version" 'fails closed'
    require_row "$file" 'Replicated command/effect payloads' 'rhiza-core' "$(code 'qlog/recorder/checkpoint payload')" 'QEFX\0\x01' 'Canonical bounded decode'
    require_row "$file" 'Recorder generation and lock' 'rhiza-quepaxa' "$(code '.rhiza-storage-generation')" 'clean-v1' 'reject open/install'
    require_row "$file" 'Recorder decision state' 'rhiza-quepaxa' "$(code 'recorder.wal')" "\`QWAL\` v$recorder_wal_version" 'conflict fails closed'
    require_row "$file" 'Recorder configuration/commands' 'rhiza-quepaxa' "$(code 'configuration.rec')" "\`QCON\` v$configuration_version" 'content-hash checks'
    require_row "$file" 'Recorder effects and GC fence' 'rhiza-quepaxa' "$(code '.effect-bundle-gc-anchor.rec')" "\`QEGC\` v1" 'unsafe deletion fails closed'
    require_row "$file" 'SQL materialization and control' 'rhiza-sql' "$(code '.rhiza-control.sqlite')" "\`QCTL\` schema v$sql_control_version" 'install snapshot rather than auto-migrate'
    require_row "$file" 'KV materialization' 'rhiza-kv' "$(code '<data>/kv/data.redb')" "\`RHKS\` v1" 'replay continuity'
    require_row "$file" 'Graph materialization' 'rhiza-graph' "$(code '<data>/ladybug/graph.lbug')" "\`RHGS\` v1" 'checked before use'
    require_row "$file" 'Archive history' 'rhiza-archive' "$(code 'rhiza/{cluster}/archive/manifest.json')" "Archive v$archive_version" 'CAS publication'
    require_row "$file" 'Checkpoint generation' 'rhiza-archive' "$(code 'rhiza/{cluster}/checkpoints/epoch-{e}/config-{c}/generation-{g}/manifest.json')" "Checkpoint v$checkpoint_version" 'before install'
    require_row "$file" 'Checkpoint publication receipts' 'rhiza-archive' "$(code 'receipts/{holder-hash}/{manifest-digest}.json')" 'same-slot evidence conflicts'
    require_row "$file" 'Archive control and leases' 'rhiza-archive' "$(code 'gc/control.json')" "GC v$gc_version" 'fence deletion'
    require_row "$file" 'Restore/install state' 'rhiza-node' "$(code '.rhiza-checkpoint-install.json')" 'exact expected path identity' 'partial activation'
    require_row "$file" 'Restore QEFX and recovery ownership' 'rhiza-node' "$(code 'consensus/pending-qefx-gc.json')" 'aggregate limits' 'fail closed'
    require_row "$file" 'Successor/prestage activation' 'rhiza-node' "$(code '.successor-prestage.{lock,intent,ready,published,finalized}')" 'state-transition checks' 'not activated'
    require_row "$file" 'Completion markers' 'rhiza-node' "$(code '<data-dir>/<portable-marker-name>')" 'caller-supplied validated portable relative name' 'receipt hash bind marker'
    require_row "$file" 'Admin operation ledger' 'rhiza-node' "$(code '<data>/admin-operations-v1.json')" 'deny_unknown_fields' '503 unavailable'

    rg -F -- 'pub const QLOG_FORMAT_VERSION' "$repo_root/crates/rhiza-log/src/lib.rs" >/dev/null || fail 'qlog source anchor changed'
    rg -F -- 'const RECORDER_WAL_MAGIC: &[u8; 4] = b"QWAL";' "$repo_root/crates/rhiza-quepaxa/src/lib.rs" >/dev/null || fail 'recorder WAL source anchor changed'
    rg -F -- 'pub const CHECKPOINT_FORMAT_VERSION' "$repo_root/crates/rhiza-archive/src/lib.rs" >/dev/null || fail 'checkpoint source anchor changed'
    rg -F -- 'const RESTORE_RECEIPT_FILE: &str = ".rhiza-checkpoint-install.json";' "$repo_root/crates/rhiza-node/src/durability.rs" >/dev/null || fail 'restore source anchor changed'
    rg -F -- 'join("admin-operations-v1.json")' "$repo_root/crates/rhiza-node/src/admin.rs" >/dev/null || fail 'admin ledger source anchor changed'
}

qlog_version=$(const_version crates/rhiza-log/src/lib.rs QLOG_FORMAT_VERSION)
anchor_version=$(const_version crates/rhiza-log/src/lib.rs ANCHOR_VERSION)
recorder_wal_version=$(const_version crates/rhiza-quepaxa/src/lib.rs RECORDER_WAL_VERSION)
configuration_version=$(const_version crates/rhiza-quepaxa/src/lib.rs CONFIGURATION_STATE_VERSION)
sql_control_version=$(const_version crates/rhiza-sql/src/control.rs CONTROL_SCHEMA_VERSION)
archive_version=$(const_version crates/rhiza-archive/src/lib.rs ARCHIVE_FORMAT_VERSION)
checkpoint_version=$(const_version crates/rhiza-archive/src/lib.rs CHECKPOINT_FORMAT_VERSION)
gc_version=$(const_version crates/rhiza-archive/src/lib.rs GC_FORMAT_VERSION)

validate "$contract_file"

missing_row="$tmp/missing-row.md"
awk 'index($0, "| Qlog segments |") != 1' "$contract_file" > "$missing_row"
if (validate "$missing_row") >/dev/null 2>&1; then
    fail 'negative test accepted a missing required row'
fi

wrong_version="$tmp/wrong-version.md"
old_qlog=$(printf '%s v%s' "\`QLOG\`" "$qlog_version")
awk -v old="$old_qlog" -v new='`QLOG` v999' '{ sub(old, new); print }' "$contract_file" > "$wrong_version"
if (validate "$wrong_version") >/dev/null 2>&1; then
    fail 'negative test accepted a changed source-backed version'
fi

printf '%s\n' 'storage-format compatibility contract: ok (missing-row and version negative tests passed)'
