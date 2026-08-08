#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

scripts/bench-rhiza-hiqlite.sh plan "$tmp/plan.json"
jq -e '
  .schema_version == 1 and .safety.cluster_mutation == false and
  .executable_coverage.recovery == "implemented" and
  .executable_coverage.comparable_workload_resource == "pending" and
  .executable_coverage.publishable_performance_comparison == false and
  (.independent_scorecards | length == 5) and
  (.matrix.recovery_cells | length == 9) and
  ([.contract_tiers[] | select(.comparable == false) | .id] | sort) == ["D3","D4"] and
  ([.non_comparable[] | select(.dimension == "durability") | .labels[]] | sort) == ["D3","D4"]
' "$tmp/plan.json" >/dev/null

rhiza_cell() {
  local failed="$1" hold="$2"
  jq -cn --arg cell "f${failed}-h${hold}" --argjson failed "$failed" --argjson hold "$hold" '
    {record_type:"cell",run_id:"rhiza-fixture",profile:"sql",cell_id:$cell,status:"passed",
     failed_peers:$failed,hold_requested_seconds:$hold,hold_actual_seconds:($hold + 1),
     pvc_count:0,old_pod_uids:[{pod:"p0",uid:"old0"},{pod:"p1",uid:"old1"},{pod:"p2",uid:"old2"}],
     new_pod_uids:(if $failed == 1 then [{pod:"p0",uid:"old0"},{pod:"p1",uid:"old1"},{pod:"p2",uid:"new2"}]
       elif $failed == 2 then [{pod:"p0",uid:"old0"},{pod:"p1",uid:"new1"},{pod:"p2",uid:"new2"}]
       else [{pod:"p0",uid:"new0"},{pod:"p1",uid:"new1"},{pod:"p2",uid:"new2"}] end),
     ack_sentinel_preserved:true,idempotency_boundary_verified:true,markers_lost:true,tip_hashes_equal:true,
     service_rto_seconds:1,full_rto_seconds:2,rpo_boundary:"zero",operator_dr:false}'
}
hiqlite_phase() {
  local failed="$1" hold="$2"
  jq -cn --arg cell "f${failed}-h${hold}" --argjson failed "$failed" --argjson hold "$hold" '
    {cell_id:$cell,phase:("f" + ($failed | tostring)),failure_count:$failed,hold_seconds:$hold,
     failure_held_seconds:($hold + 1),service_rto_seconds:1,full_rto_seconds:2,
     expected_vs_observed:{expected:{write:"expected"},observed:{write:"observed"}}}'
}
: > "$tmp/rhiza.jsonl"
for failed in 1 2 3; do
  for hold in 60 180 300; do rhiza_cell "$failed" "$hold" >> "$tmp/rhiza.jsonl"; done
done
jq -cn '{record_type:"summary",run_id:"rhiza-fixture",profile:"sql",status:"passed"}' >> "$tmp/rhiza.jsonl"
: > "$tmp/hiqlite-phases.jsonl"
for failed in 1 2 3; do
  for hold in 60 180 300; do hiqlite_phase "$failed" "$hold" >> "$tmp/hiqlite-phases.jsonl"; done
done
jq -s '{system:"hiqlite",voters:3,storage:"emptyDir",zero_pvc:true,phases:.}' \
  "$tmp/hiqlite-phases.jsonl" > "$tmp/hiqlite.json"

scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite.json" "$tmp/normalized.json"
jq -e '.cells | length == 9 and all(.[]; .rhiza.throughput == "not_measured" and .hiqlite.resource == "not_measured")' "$tmp/normalized.json" >/dev/null
jq -e '.durability_comparison.status == "non_comparable"' "$tmp/normalized.json" >/dev/null
jq -e '.source_artifacts.rhiza_jsonl.sha256 | test("^[0-9a-f]{64}$")' "$tmp/normalized.json" >/dev/null
jq -e '.source_provenance.rhiza_cells_common == [{"run_id":"rhiza-fixture","profile":"sql"}]' "$tmp/normalized.json" >/dev/null

jq -c 'if .record_type == "cell" and .cell_id == "f2-h60" then
  .operator_dr = true | .rpo_boundary = "last_sync_checkpoint" |
  .new_pod_uids = [{pod:"p0",uid:"dr0"},{pod:"p1",uid:"dr1"},{pod:"p2",uid:"dr2"}]
  else . end' "$tmp/rhiza.jsonl" > "$tmp/rhiza-operator-dr.jsonl"
scripts/bench-rhiza-hiqlite.sh normalize-recovery \
  "$tmp/rhiza-operator-dr.jsonl" "$tmp/hiqlite.json" "$tmp/operator-dr.json"
jq -e '.cells[] | select(.cell_id == "f2-h60") |
  .rhiza.operator_dr == true and .rhiza.rpo_boundary == "last_sync_checkpoint"' \
  "$tmp/operator-dr.json" >/dev/null

sed -n '1p' "$tmp/rhiza.jsonl" > "$tmp/missing.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/missing.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing cells were accepted" >&2; exit 1
fi
cat "$tmp/rhiza.jsonl" "$tmp/rhiza.jsonl" > "$tmp/duplicate.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/duplicate.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "duplicate cells were accepted" >&2; exit 1
fi
jq -c 'if .record_type == "cell" and .cell_id == "f3-h300" then .run_id = "other-run" else . end' \
  "$tmp/rhiza.jsonl" > "$tmp/mixed-run.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/mixed-run.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mixed Rhiza runs were accepted" >&2; exit 1
fi
jq -c 'select(.record_type != "summary")' "$tmp/rhiza.jsonl" > "$tmp/no-summary.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/no-summary.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing Rhiza summary was accepted" >&2; exit 1
fi
jq -c 'del(.run_id) | if .record_type == "summary" then del(.profile) else . end' \
  "$tmp/rhiza.jsonl" > "$tmp/missing-identity.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/missing-identity.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing Rhiza identity was accepted" >&2; exit 1
fi
jq '(.phases[0].cell_id) = "f1-h999"' "$tmp/hiqlite.json" > "$tmp/mismatched.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/mismatched.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mismatched cells were accepted" >&2; exit 1
fi
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/no-rhiza.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing source file was accepted" >&2; exit 1
fi
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite.json" "$tmp/rhiza.jsonl" >/dev/null 2>&1; then
  echo "source/output alias was accepted" >&2; exit 1
fi
jq -s '.[0].hold_actual_seconds = 0 | .[]' "$tmp/rhiza.jsonl" > "$tmp/rhiza-short-hold.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza-short-hold.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "short Rhiza hold was accepted" >&2; exit 1
fi
jq '.phases[0].failure_held_seconds = 0' "$tmp/hiqlite.json" > "$tmp/hiqlite-short-hold.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-short-hold.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "short Hiqlite hold was accepted" >&2; exit 1
fi
jq '.storage = "pvc"' "$tmp/hiqlite.json" > "$tmp/hiqlite-pvc.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-pvc.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "non-emptyDir Hiqlite evidence was accepted" >&2; exit 1
fi
