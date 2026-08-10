#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$repo_root"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
isolation='{"mode":"fresh-vcluster","process_generation_new":true,"storage_generation_new":true,"restore_env_absent":true,"prior_sentinel_absent":true,"exact_membership":true,"object_provenance_current":true,"cleanup_verified":true}'
fixture_raw="$tmp/raw"
mkdir -p "$fixture_raw"

scripts/bench-rhiza-hiqlite.sh plan "$tmp/plan.json"
if rg -n '\beval\b|RECOVERY_.*HOOK|\$\{.*COMMAND' scripts/bench-rhiza-hiqlite.sh >/dev/null; then
  echo "recovery coordinator permits an arbitrary hook" >&2; exit 1
fi
# shellcheck disable=SC2016 # Fixed source fragments are deliberately literal.
for required in 'mkdir -p "$cell_root/rhiza" "$cell_root/hiqlite"' \
  'scripts/e2e-vind-rustfs.sh' 'scripts/e2e-hiqlite-recovery.sh' \
  'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1' 'RHIZA_RECOVERY_FORBIDDEN_SENTINEL=' \
  'current_run_sentinel.key' 'RHIZA_VIND_CLEANUP=1' 'HIQLITE_RECOVERY_CLEANUP=1' \
  'HIQLITE_RECOVERY_REUSE_EXISTING=0' 'fresh_vcluster_created == true' \
  'HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1' 'image_provenance_verified == true' \
  'image_provenance_publishable == true' \
  'RHIZA_VIND_SKIP_BUILD=0' 'cleanup_exact_vcluster' 'canonical_source_fingerprint' \
  'git diff --binary HEAD' 'event == "run_started" and .run_id == $run_id' \
  'event == "phase_summary" and .cell_id == $cell' \
  'select(has("phase") and .event != "phase_summary")' \
  'git -C "$hiqlite_source_dir" rev-parse --is-inside-work-tree' 'cleanup proof failed'; do
  rg -F "$required" scripts/bench-rhiza-hiqlite.sh >/dev/null || {
    echo "missing fixed prepare/run/cleanup coordinator step: $required" >&2; exit 1; }
done
# shellcheck disable=SC2016 # jq program intentionally contains $cell.
failure_proof_contract='(.valid == true and .proven == true and .cell_id == $cell and
  [.sequence[].kind] == ["pods","endpoints","metrics","consistent"] and
  (.sequence | all(.[]; (.rc|type)=="number" and (.raw|type)=="string")) and
  .sequence[0].rc == 0 and .sequence[1].rc == 0 and .sequence[2].rc == 0 and .sequence[3].rc != 0 and
  ((.sequence[0].raw|fromjson).items | length) == 1 and
  ((.sequence[1].raw|fromjson).items[0].endpoints | length) == 1 and
  ((.sequence[2].raw|fromjson).running == true and (.sequence[2].raw|fromjson).voter_ids == [1,2,3] and (.sequence[2].raw|fromjson).node_ids == [1,2,3]) and
  (.sequence[3].raw | test("QuorumNotEnough") and test("got: \\{1\\}")) and
  .proxy_ping.rc == 0 and .proof_end_epoch <= .write.started_epoch and (.write.started_epoch - .proof_end_epoch) <= 2 and
  .write.timeout_seconds <= .write.remaining_seconds and
  (if .outcome == "application-no-quorum-rejection" then .write.rc != 0 and (.write.raw|test("QuorumNotEnough"))
   elif .outcome == "no_ack_unknown" then .write.rc != 0 else false end))'
failure_base='{"valid":true,"proven":true,"cell_id":"f2-h60","sequence":[{"kind":"pods","rc":0,"raw":"{\"items\":[{\"metadata\":{\"name\":\"hiqlite-recovery-0\",\"uid\":\"u0\"},\"status\":{\"phase\":\"Running\",\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\"}]}}]}"},{"kind":"endpoints","rc":0,"raw":"{\"items\":[{\"endpoints\":[{\"targetRef\":{\"name\":\"hiqlite-recovery-0\",\"uid\":\"u0\"},\"conditions\":{\"ready\":true}}]}]}"},{"kind":"metrics","rc":0,"raw":"{\"running\":true,\"voter_ids\":[1,2,3],\"node_ids\":[1,2,3]}"},{"kind":"consistent","rc":1,"raw":"QuorumNotEnough got: {1}"}],"proof_end_epoch":10,"proxy_ping":{"rc":0,"raw":"pong"},"write":{"id":"f2-h60-transition-x","value":"v","rc":1,"raw":"QuorumNotEnough","started_epoch":11,"ended_epoch":11,"timeout_seconds":2,"remaining_seconds":3}}'
jq -e --arg cell f2-h60 "$failure_proof_contract" <<< "$(jq -c '. + {outcome:"application-no-quorum-rejection"}' <<< "$failure_base")" >/dev/null
jq -e --arg cell f2-h60 "$failure_proof_contract" <<< "$(jq -c '. + {outcome:"no_ack_unknown"}' <<< "$failure_base")" >/dev/null
for mutation in 'del(.sequence[0].raw)' '.sequence[1].raw = "{\"items\":[]}"' '.sequence[2].raw = "{\"running\":false}"' '.sequence[3].raw = "ok"' '.proxy_ping.rc = 1' '.write.started_epoch = 14' '.write.timeout_seconds = 4' '.write.raw = "transport" | .outcome = "application-no-quorum-rejection"'; do
  if jq -e --arg cell f2-h60 "$failure_proof_contract" <<< "$(jq -c "$mutation" <<< "$(jq -c '. + {outcome:"application-no-quorum-rejection"}' <<< "$failure_base")")" >/dev/null; then
    echo "failure establishment fixture accepted forged raw evidence" >&2; exit 1
  fi
done
# shellcheck disable=SC2016 # This is the executable post-ACK semantic replay.
post_ack_contract='(.valid == true and .cell_id == $cell and .id == $id and .value == $value and
  (.order|type) == "number" and .order >= 1 and (.captured_epoch|type) == "number" and .captured_epoch >= $write_ended and
  (.classification == "unilateral_state_machine_apply" or .classification == "ack_without_local_apply" or .classification == "ack_post_state_unknown") and
  [.sequence[].kind] == ["pods","endpoints","direct_metrics","direct_consistent","direct_local","proxy_ping","voter0_current_logs","proxy_current_logs","proxy_previous_logs","proxy_describe"] and
  ([.sequence[].order] == [range(1;11)]) and
  (.sequence | all(.[]; (.rc|type) == "number" and (.raw|type) == "string" and (.started_epoch|type) == "number" and (.ended_epoch|type) == "number" and .ended_epoch >= .started_epoch and .started_epoch >= $write_ended)) and
  (.sequence[0] as $pods | .sequence[1] as $endpoints | .sequence[2] as $metrics | .sequence[4] as $local |
  ($pods.rc == 0 and $endpoints.rc == 0 and $metrics.rc == 0) and
  ((try ($pods.raw|fromjson) catch null).items | length) == 1 and
  ((try ($endpoints.raw|fromjson) catch null).items[0].endpoints | length) == 1 and
  ((try ($metrics.raw|fromjson) catch null).running == true and (try ($metrics.raw|fromjson) catch null).voter_ids == [1,2,3] and (try ($metrics.raw|fromjson) catch null).node_ids == [1,2,3]) and
  (if .classification == "unilateral_state_machine_apply" then $local.rc == 0 and (try ($local.raw|fromjson | . as $query | $query.found == true and $query.id == $id and $query.value == $value) catch false)
   elif .classification == "ack_without_local_apply" then $local.rc == 0 and (try ($local.raw|fromjson|.found == false) catch false)
   else (($local.rc == 0 and (try ($local.raw|fromjson | . as $query | $query.found == true and $query.id == $id and $query.value == $value) catch false)) | not) and (($local.rc == 0 and (try ($local.raw|fromjson|.found == false) catch false)) | not) end)))'
post_ack_base="$(jq -cn --argjson base "$failure_base" '
  {valid:true,cell_id:"f2-h60",id:"f2-h60-transition-x",value:"v",order:1,captured_epoch:12,
   classification:"unilateral_state_machine_apply",
   sequence:[
     {kind:"pods",rc:0,raw:$base.sequence[0].raw},{kind:"endpoints",rc:0,raw:$base.sequence[1].raw},
     {kind:"direct_metrics",rc:0,raw:$base.sequence[2].raw},{kind:"direct_consistent",rc:1,raw:"QuorumNotEnough got: {1}"},
     {kind:"direct_local",rc:0,raw:"{\"found\":true,\"id\":\"f2-h60-transition-x\",\"value\":\"v\"}"},
     {kind:"proxy_ping",rc:0,raw:"pong"},{kind:"voter0_current_logs",rc:0,raw:"logs"},{kind:"proxy_current_logs",rc:0,raw:"logs"},
     {kind:"proxy_previous_logs",rc:1,raw:"none"},{kind:"proxy_describe",rc:0,raw:"describe"}]
   } | .sequence |= (to_entries | map(.value + {order:(.key + 1),started_epoch:12,ended_epoch:12}))
')"
for classification in unilateral_state_machine_apply ack_without_local_apply ack_post_state_unknown; do
  case "$classification" in
    unilateral_state_machine_apply) candidate="$post_ack_base" ;;
    ack_without_local_apply) candidate="$(jq '.classification = "ack_without_local_apply" | .sequence[4].raw = "{\"found\":false}"' <<< "$post_ack_base")" ;;
    ack_post_state_unknown) candidate="$(jq '.classification = "ack_post_state_unknown" | .sequence[4].rc = 1 | .sequence[4].raw = "timeout"' <<< "$post_ack_base")" ;;
  esac
  jq -e --arg cell f2-h60 --arg id f2-h60-transition-x --arg value v --argjson write_ended 11 "$post_ack_contract" <<< "$candidate" >/dev/null || {
    echo "valid post-ACK fixture rejected: $classification" >&2; exit 1; }
done
for mutation in 'del(.sequence[4])' '.cell_id = "f2-h180"' '.id = "other"' '.captured_epoch = 10' '.sequence[0].order = 2' '.sequence[0].started_epoch = 10' '.classification = "unilateral_state_machine_apply" | .sequence[4].raw = "{\"found\":false}"' '.classification = "ack_without_local_apply" | .sequence[4].raw = "{\"found\":true,\"id\":\"f2-h60-transition-x\",\"value\":\"v\"}"' '.classification = "ack_post_state_unknown" | .sequence[4].raw = "{\"found\":false}"' '.sequence[0].raw = "{\"items\":[]}"'; do
  if jq -e --arg cell f2-h60 --arg id f2-h60-transition-x --arg value v --argjson write_ended 11 "$post_ack_contract" <<< "$(jq "$mutation" <<< "$post_ack_base")" >/dev/null; then
    echo "forged post-ACK fixture was accepted: $mutation" >&2; exit 1
  fi
done
# An ACK outcome is evidence to retain for triage, never a publishable F2 cell.
ack_failure_contract='if (.outcome|startswith("write-ack-violation-")) then (.post_ack.path|type) == "string" and (.post_ack.sha256|test("^[0-9a-f]{64}$")) and (.post_ack.classification == (.outcome | sub("^write-ack-violation-"; ""))) else (.post_ack? == null) end'
ack_failure="$(jq -cn --argjson base "$failure_base" '$base + {outcome:"write-ack-violation-unilateral_state_machine_apply",write:($base.write + {rc:0,raw:"{\"acknowledged\":true,\"id\":\"f2-h60-transition-x\",\"value\":\"v\"}"}),post_ack:{path:"/private/post-ack.json",sha256:("a" * 64),classification:"unilateral_state_machine_apply"}}')"
jq -e "$ack_failure_contract" <<< "$ack_failure" >/dev/null
for mutation in 'del(.post_ack)' '.post_ack.classification = "ack_without_local_apply"' '.post_ack.sha256 = "bad"'; do
  if jq -e "$ack_failure_contract" <<< "$(jq "$mutation" <<< "$ack_failure")" >/dev/null; then
    echo "ACK failure proof accepted missing or forged post-ACK binding" >&2; exit 1
  fi
done
# shellcheck disable=SC2016 # Fixed jq source fragments deliberately keep $variables literal.
for required in '.cell_id == $cell and .stage == $stage' \
  '.expected_config_ids == $expected and .expected_cri_ids == $expected' \
  '["hiqlite-recovery-0","hiqlite-recovery-1","hiqlite-recovery-2"]' \
  '.valid == true and .cell_id == $cell' \
  'transition ledger source mismatch'; do
  rg -F "$required" scripts/bench-rhiza-hiqlite.sh >/dev/null || {
    echo "missing image/ledger binding guard: $required" >&2; exit 1; }
done
# shellcheck disable=SC2016 # Literal coordinator source contracts.
for required in 'snapshot_failure_establishment_proof "$hiqlite_phase" "$cell_root"' \
  'snapshot_failure_establishment_resolution "$hiqlite_phase" "$cell_root"' \
  'failure_establishment_proof:$failure_proof' \
  'failure_establishment_resolution:$failure_resolution' \
  'proof_end_epoch' 'application-no-quorum-rejection' 'no_ack_unknown' \
  'failure_establishment_post_ack' 'write-ack-violation-' \
  'HIQLITE_RECOVERY_EXPECTED_LOCKFILE_PATH'; do
  rg -F "$required" scripts/bench-rhiza-hiqlite.sh >/dev/null || {
    echo "missing failure proof integration: $required" >&2; exit 1; }
done
# shellcheck disable=SC2016 # Literal coordinator source contracts.
for required in 'openraft_version_source == "generated-cargo-lock"' \
  'openraft_version_source:$first.openraft_version_source' \
  'resolved from the generated Cargo.lock'; do
  rg -F "$required" scripts/bench-rhiza-hiqlite.sh >/dev/null || {
    echo "missing lock-derived OpenRaft provenance guard: $required" >&2; exit 1; }
done
reuse_line="$(rg -F 'HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES=1' scripts/bench-rhiza-hiqlite.sh)"
if [[ "$reuse_line" != *'HIQLITE_BUILD_IMAGE=1'* ]] || [[ "$reuse_line" == *'HIQLITE_BUILD_IMAGE=0'* ]]; then
  echo "Hiqlite exact local reuse must stay in the build-image reuse branch" >&2
  exit 1
fi

# A phase summary uses the failure-axis label (`f1`), while probe events use
# the complete cell ID (`f1-h60`). Only non-summary events are cell-bound here;
# the coordinator separately requires exact summary-object equality.
# shellcheck disable=SC2016 # jq expands $cell, not the shell.
event_phase_contract='[.[] | select(has("phase") and .event != "phase_summary") | .phase] | all(. == $cell)'
event_phase_fixture='[{"event":"probe","phase":"f1-h60"},{"event":"phase_summary","phase":"f1","cell_id":"f1-h60"}]'
jq -e --arg cell f1-h60 "$event_phase_contract" <<< "$event_phase_fixture" >/dev/null
if jq -e --arg cell f1-h60 "$event_phase_contract" \
  <<< '[{"event":"probe","phase":"f1-h180"},{"event":"phase_summary","phase":"f1","cell_id":"f1-h60"}]' >/dev/null; then
  echo "Hiqlite raw event contract accepted an event from another cell" >&2
  exit 1
fi
jq -e '
  .schema_version == 1 and .safety.cluster_mutation == false and
  .executable_coverage.recovery == "implemented" and
  .executable_coverage.comparable_workload_resource == "pending" and
  .executable_coverage.publishable_performance_comparison == false and
  .recovery_execution.kind == "single_diagnostic_trial" and
  .recovery_execution.publishable == false and
  (.matrix.workloads | index("locks")) != null and
  (.matrix.workloads | index("notifications")) != null and
  (.matrix.external_baselines | index("ladybugdb_standalone")) != null and
  (.independent_scorecards | length == 5) and
  (.matrix.recovery_cells | length == 9) and
  ([.contract_tiers[] | select(.comparable == false) | .id] | sort) == ["D3","D4"] and
  ([.non_comparable[] | select(.dimension == "durability") | .labels[]] | sort) == ["D3","D4"]
' "$tmp/plan.json" >/dev/null

rhiza_cell() {
  local failed="$1" hold="$2" cell raw source sha
  cell="f${failed}-h${hold}"; raw="$fixture_raw/rhiza-${cell}"; source="rhiza-source-${cell}"
  jq -cn --arg cell "$cell" --arg run "$source" '{record_type:"cell",run_id:$run,cell_id:$cell}' > "$raw"
  jq -cn --arg run "$source" '{record_type:"summary",run_id:$run}' >> "$raw"
  sha="$(shasum -a 256 "$raw" | awk '{print $1}')"
  jq -cn --arg cell "$cell" --arg raw "$raw" --arg sha "$sha" --arg source "$source" --argjson failed "$failed" --argjson hold "$hold" '
    {record_type:"cell",run_id:"rhiza-fixture",profile:"sql",
     rhiza_commit:"951c2f3b56595a93d4418ce8042a24ad75a57bfe",rhiza_dirty:false,
     resolved_image:"sha256:rhiza-fixture",cell_id:$cell,status:"passed",
     failed_peers:$failed,hold_requested_seconds:$hold,hold_actual_seconds:($hold + 1),
     pvc_count:0,old_pod_uids:[{pod:"p0",uid:"old0"},{pod:"p1",uid:"old1"},{pod:"p2",uid:"old2"}],
     new_pod_uids:(if $failed == 1 then [{pod:"p0",uid:"old0"},{pod:"p1",uid:"old1"},{pod:"p2",uid:"new2"}]
       elif $failed == 2 then [{pod:"p0",uid:"old0"},{pod:"p1",uid:"new1"},{pod:"p2",uid:"new2"}]
       else [{pod:"p0",uid:"new0"},{pod:"p1",uid:"new1"},{pod:"p2",uid:"new2"}] end),
     ack_sentinel_preserved:true,idempotency_boundary_verified:true,markers_lost:true,tip_hashes_equal:true,
     source_run_id:$source,source_artifact:{path:$raw,sha256:$sha},adapter_cell_isolation:{native:true},cell_isolation:$isolation,
     service_rto_seconds:1,full_rto_seconds:2,rpo_boundary:"zero",operator_dr:false}' \
    --argjson isolation "$isolation"
}
hiqlite_phase() {
  local failed="$1" hold="$2" cell raw events source sha events_sha base_phase raw_phase proof proof_sha proofs stage stages auto_path manifest manifest_sha ledger ledger_sha baseline baseline_sha write write_sha failure failure_sha resolution resolution_sha outcome
  cell="f${failed}-h${hold}"; raw="$fixture_raw/hiqlite-${cell}"; events="$fixture_raw/events-${cell}"; source="hiqlite-source-${cell}"
  auto_path=false
  if [ "$failed" = 1 ] || [ "$cell" = "f2-h60" ]; then auto_path=true; fi
  base_phase="$(jq -cn --arg cell "$cell" --argjson failed "$failed" --argjson hold "$hold" --argjson auto "$auto_path" '
    {schema_version:1,system:"hiqlite",event:"phase_summary",cell_id:$cell,
     phase:("f" + ($failed | tostring)),failure_count:$failed,hold_seconds:$hold,
     failure_held_seconds:($hold + 1),service_rto_seconds:1,full_rto_seconds:2,
     hiqlite_reference_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
     hiqlite_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
     hiqlite_reference_release:"0.14.0",hiqlite_release:"0.14.0",
     openraft_version:"0.9.99+fixture-lock",openraft_version_source:"generated-cargo-lock",
     log_sync:"Immediate",image_source:"exact-source-build",
     source_commit_basis:"exact-commit",image_source_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
     cargo_lock_origin:"generated-from-exact-source",cargo_lock_sha256:("a" * 64),
     resolved_image:"sha256:hiqlite-fixture",resolved_proxy_image:"proxy:fixture",
     resolved_proxy_image_id:"sha256:proxy-fixture",
     ingress:{kind:"hiqlite-application-proxy",version:"0.14.0+fixture",image:"proxy:fixture",patch_sha256:("b" * 64)},
     upstream_proxy_incompatibility:"fixture incompatibility",
     cell_isolation:{native:true,fresh_vcluster_created:true,
       vcluster:{name:("vcluster-" + $cell),node_uid:("node-" + $cell)},namespace:("namespace-" + $cell)},
     expected_vs_observed:{expected:{write:"expected"},observed:{write:"observed",
       auto_recovery:$auto,operator_dr:($auto|not)}}}')"
  if [ "$cell" != "f1-h60" ]; then
    base_phase="$(jq -c '.image_source = "verified-local-exact-source-reuse" | .cargo_lock_origin = "reused-generated-from-exact-source"' <<< "$base_phase")"
  fi
  if [ "$auto_path" = true ]; then
    stages='pre-fault post-recovery'
  else
    stages='pre-fault post-operator-dr post-restore-clear'
  fi
  proofs='[]'
  for stage in $stages; do
    proof="$fixture_raw/image-proof-${cell}-${stage}.json"
    jq -n --arg stage "$stage" --argjson failed "$failed" '
      def uid($ordinal):
        if $stage == "pre-fault" then "pre" + ($ordinal|tostring)
        elif $stage == "post-recovery" then
          if $ordinal >= (3 - $failed) then "recovered" + ($ordinal|tostring) else "pre" + ($ordinal|tostring) end
        elif $stage == "post-operator-dr" then "operator" + ($ordinal|tostring)
        else "restore" + ($ordinal|tostring) end;
      {valid:true,stage:$stage,voters:[range(0;3) | {name:("hiqlite-recovery-" + tostring),uid:uid(.),image_id:("sha256:" + ("a" * 64))}],proxy:[{uid:"proxy",image_id:("sha256:" + ("b" * 64))}],expected_cri_ids:{voter:("sha256:" + ("a" * 64)),proxy:("sha256:" + ("b" * 64))}}' > "$proof"
    if [ "${fixture_reuse_uid:-0}" = 1 ] && [ "$stage" = "post-recovery" ]; then
      jq '.voters[2].uid = "pre2"' "$proof" > "$proof.reused" && mv "$proof.reused" "$proof"
    fi
    proof_sha="$(shasum -a 256 "$proof" | awk '{print $1}')"
    proofs="$(jq -cn --argjson existing "$proofs" --arg stage "$stage" --arg path "$proof" --arg sha "$proof_sha" '$existing + [{stage:$stage,path:$path,sha256:$sha,snapshot:{path:$path,sha256:$sha}}]')"
  done
  ledger="$fixture_raw/transition-ledger-${cell}.jsonl"; : > "$ledger"; ledger_sha="$(shasum -a 256 "$ledger" | awk '{print $1}')"
  baseline="$fixture_raw/baseline-proof-${cell}.json"
  jq -n --arg cell "$cell" '{cell_id:$cell,pre:{records:[range(0;3) | {ordinal:.,rc:1,raw:"no such table",classification:"absent-table"}]},reset:{rc:0,acknowledged:true,raw:"{}"},post:{records:[range(0;3) | {ordinal:.,attempts:1,rc:0,raw:"{\"found\":false}",classification:"empty"}]},valid:true}' > "$baseline"
  baseline_sha="$(shasum -a 256 "$baseline" | awk '{print $1}')"
  write="$fixture_raw/idempotent-write-${cell}.json"
  jq -n --arg cell "$cell" '{contract:"idempotent-final-state-single-key-not-exactly-once",cell_id:$cell,stage:"post-restore-clear",id:($cell + "-restore-idempotent-fixture"),value:"value",attempts:[{attempt:1,rc:0,raw:("{\"acknowledged\":true,\"id\":\"" + $cell + "-restore-idempotent-fixture\",\"value\":\"value\"}"),classification:"acknowledged"}],final:{found:true,id:($cell + "-restore-idempotent-fixture"),value:"value",rc:0,raw:("{\"found\":true,\"id\":\"" + $cell + "-restore-idempotent-fixture\",\"value\":\"value\"}"),single_logical_key:true,single_key_basis:"PRIMARY KEY(id)"},valid:true}' > "$write"
  if [ "$cell" = "f2-h60" ]; then
    jq '.attempts = [{attempt:1,rc:1,raw:"transport unavailable",classification:"ambiguous-retryable"}, (.attempts[0] + {attempt:2})]' "$write" > "$write.two" && mv "$write.two" "$write"
  fi
  write_sha="$(shasum -a 256 "$write" | awk '{print $1}')"
  outcome=application-no-quorum-rejection
  [ "$cell" != "f2-h60" ] || outcome=no_ack_unknown
  failure="$fixture_raw/failure-proof-${cell}.json"
  jq -cn --argjson base "$failure_base" --arg cell "$cell" --arg outcome "$outcome" '$base + {cell_id:$cell,outcome:$outcome}' > "$failure"
  failure_sha="$(shasum -a 256 "$failure" | awk '{print $1}')"
  resolution=""; resolution_sha=""
  if [ "$outcome" = no_ack_unknown ]; then
    resolution="$fixture_raw/failure-resolution-${cell}.json"
    jq -n --arg cell "$cell" '{cell_id:$cell,mode:"auto-recovery",outcome:"no_ack_unknown",id:"f2-h60-transition-x",value:"v",raw:"{\"found\":false}",valid:true}' > "$resolution"
    resolution_sha="$(shasum -a 256 "$resolution" | awk '{print $1}')"
  fi
  manifest="$fixture_raw/image-manifest-${cell}.json"
  jq -n --arg cell "$cell" --argjson proofs "$proofs" '{valid:true,cell_id:$cell,canonical_tags:{voter:"voter:fixture",proxy:"proxy:fixture"},expected_config_ids:{voter:("sha256:" + ("a" * 64)),proxy:("sha256:" + ("b" * 64))},references:{stage_proofs:$proofs}}' > "$manifest"
  manifest_sha="$(shasum -a 256 "$manifest" | awk '{print $1}')"
  base_phase="$(jq -cn --argjson base "$base_phase" --argjson proofs "$proofs" --arg manifest "$manifest" --arg manifest_sha "$manifest_sha" --arg ledger "$ledger" --arg ledger_sha "$ledger_sha" --arg baseline "$baseline" --arg baseline_sha "$baseline_sha" --arg write "$write" --arg write_sha "$write_sha" --arg failure "$failure" --arg failure_sha "$failure_sha" --arg resolution "$resolution" --arg resolution_sha "$resolution_sha" '$base + {cell_isolation:($base.cell_isolation + {image_proofs:$proofs,expected_image_proof_stages:[$proofs[].stage],image_provenance_manifest:{path:$manifest,sha256:$manifest_sha},transition_ledger:{path:$ledger,sha256:$ledger_sha,count:0},baseline_proof:{path:$baseline,sha256:$baseline_sha},idempotent_recovery_write:{path:$write,sha256:$write_sha,contract:"idempotent-final-state-single-key-not-exactly-once"},failure_establishment_proof:{path:$failure,sha256:$failure_sha},failure_establishment_resolution:{path:(if $resolution=="" then null else $resolution end),sha256:(if $resolution_sha=="" then null else $resolution_sha end)}})}')"
  raw_phase="$base_phase"
  base_phase="$(jq -cn --argjson base "$base_phase" --argjson failed "$failed" --arg outcome "$outcome" --argjson proofs "$proofs" --arg manifest "$manifest" --arg manifest_sha "$manifest_sha" --arg ledger "$ledger" --arg ledger_sha "$ledger_sha" --arg baseline "$baseline" --arg baseline_sha "$baseline_sha" --arg write "$write" --arg write_sha "$write_sha" --arg failure "$failure" --arg failure_sha "$failure_sha" --arg resolution "$resolution" --arg resolution_sha "$resolution_sha" '$base + {image_proofs:$proofs,image_provenance_manifest:{path:$manifest,sha256:$manifest_sha,snapshot:{path:$manifest,sha256:$manifest_sha}},transition_ledger:{path:$ledger,sha256:$ledger_sha,count:0,snapshot:{path:$ledger,sha256:$ledger_sha}},baseline_proof:{path:$baseline,sha256:$baseline_sha,snapshot:{path:$baseline,sha256:$baseline_sha}},idempotent_recovery_write:{path:$write,sha256:$write_sha,snapshot:{path:$write,sha256:$write_sha}},failure_establishment_proof:(if $failed==2 then {path:$failure,sha256:$failure_sha,outcome:$outcome,snapshot:{path:$failure,sha256:$failure_sha}} else null end),failure_establishment_resolution:(if $failed==2 and $resolution!="" then {path:$resolution,sha256:$resolution_sha,snapshot:{path:$resolution,sha256:$resolution_sha}} else null end)}')"
  jq -cn --arg run "$source" --argjson phase "$raw_phase" '{run_id:$run,phases:[$phase]}' > "$raw"
  jq -cn --arg cell "$cell" --arg run "$source" '{event:"run_started",run_id:$run}' > "$events"
  printf '%s\n' "$raw_phase" >> "$events"
  sha="$(shasum -a 256 "$raw" | awk '{print $1}')"
  events_sha="$(shasum -a 256 "$events" | awk '{print $1}')"
  jq -cn --argjson base "$base_phase" --arg raw "$raw" --arg events "$events" \
    --arg sha "$sha" --arg events_sha "$events_sha" --arg source "$source" \
    --argjson isolation "$isolation" '
    $base + {source_run_id:$source,source_artifact:{path:$raw,sha256:$sha},
      source_events:{path:$events,sha256:$events_sha},
      adapter_cell_isolation:$base.cell_isolation,cell_isolation:$isolation}'
}
: > "$tmp/rhiza.jsonl"
for failed in 1 2 3; do
  for hold in 60 180 300; do rhiza_cell "$failed" "$hold" >> "$tmp/rhiza.jsonl"; done
done
jq -cn '{record_type:"summary",run_id:"rhiza-fixture",profile:"sql",
  rhiza_commit:"951c2f3b56595a93d4418ce8042a24ad75a57bfe",rhiza_dirty:false,
  resolved_image:"sha256:rhiza-fixture",status:"passed"}' >> "$tmp/rhiza.jsonl"
: > "$tmp/hiqlite-phases.jsonl"
for failed in 1 2 3; do
  for hold in 60 180 300; do hiqlite_phase "$failed" "$hold" >> "$tmp/hiqlite-phases.jsonl"; done
done
jq -s '{system:"hiqlite",run_id:"hiqlite-fixture",
  hiqlite_reference_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
  hiqlite_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
  hiqlite_reference_release:"0.14.0",hiqlite_release:"0.14.0",
  openraft_version:"0.9.99+fixture-lock",openraft_version_source:"generated-cargo-lock",
  log_sync:"Immediate",image_source:"exact-source-build-with-verified-local-reuse",
  source_commit_basis:"exact-commit",image_source_commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
  cargo_lock_origin:"generated-once-then-verified-reuse",cargo_lock_sha256:("a" * 64),
  resolved_image:"sha256:hiqlite-fixture",
  resolved_proxy_image:"proxy:fixture",resolved_proxy_image_id:"sha256:proxy-fixture",
  ingress:{kind:"hiqlite-application-proxy",version:"0.14.0+fixture",image:"proxy:fixture",patch_sha256:("b" * 64)},
  upstream_proxy_incompatibility:"fixture incompatibility",
  voters:3,storage:"emptyDir",zero_pvc:true,
  failure_counts:[1,2,3],hold_seconds:[60,180,300],
  cell_isolation:{all_cells_proven:true,
    vclusters:[.[] | .adapter_cell_isolation.vcluster.name],
    node_uids:[.[] | .adapter_cell_isolation.vcluster.node_uid],
    namespaces:[.[] | .adapter_cell_isolation.namespace]},phases:.}' \
  "$tmp/hiqlite-phases.jsonl" > "$tmp/hiqlite.json"

scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite.json" "$tmp/normalized.json"
jq -e '.cells | length == 9 and all(.[]; .rhiza.throughput == "not_measured" and .hiqlite.resource == "not_measured")' "$tmp/normalized.json" >/dev/null
jq -e '.durability_comparison.status == "non_comparable"' "$tmp/normalized.json" >/dev/null
jq '.phases[0].openraft_version = "0.0.0-forged"' "$tmp/hiqlite.json" > "$tmp/hiqlite-forged-openraft-version.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-forged-openraft-version.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "forged per-phase OpenRaft version was accepted" >&2; exit 1
fi
jq '.phases[0].openraft_version_source = "handwritten"' "$tmp/hiqlite.json" > "$tmp/hiqlite-forged-openraft-source.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-forged-openraft-source.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "forged OpenRaft version source was accepted" >&2; exit 1
fi
jq -e '.source_artifacts.rhiza_jsonl.sha256 | test("^[0-9a-f]{64}$")' "$tmp/normalized.json" >/dev/null
jq -e '.publication.eligible == false and
  .source_provenance.rhiza_cells_common == [{"run_id":"rhiza-fixture","profile":"sql",
    "rhiza_commit":"951c2f3b56595a93d4418ce8042a24ad75a57bfe","rhiza_dirty":false,
    "resolved_image":"sha256:rhiza-fixture"}]' "$tmp/normalized.json" >/dev/null

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
jq '.log_sync = "interval_200"' "$tmp/hiqlite.json" > "$tmp/hiqlite-wrong-sync.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-wrong-sync.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "wrong Hiqlite durability mode was accepted" >&2; exit 1
fi
jq '.phases[0].resolved_proxy_image_id = "sha256:mixed"' "$tmp/hiqlite.json" > "$tmp/hiqlite-mixed-phase-provenance.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-mixed-phase-provenance.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mixed Hiqlite phase provenance was accepted" >&2; exit 1
fi
jq '.phases[0].image_source = "unverified"' "$tmp/hiqlite.json" > "$tmp/hiqlite-mixed-source-basis.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-mixed-source-basis.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mixed Hiqlite source basis was accepted" >&2; exit 1
fi
jq '.phases[1].image_source = "exact-source-build"' "$tmp/hiqlite.json" > "$tmp/hiqlite-wrong-build-reuse-count.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-wrong-build-reuse-count.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "wrong Hiqlite build/reuse count was accepted" >&2; exit 1
fi
jq 'del(.cargo_lock_sha256)' "$tmp/hiqlite.json" > "$tmp/hiqlite-no-lock.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-no-lock.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "Hiqlite evidence without lockfile provenance was accepted" >&2; exit 1
fi
jq -c 'if .record_type == "cell" and .cell_id == "f1-h60" then
  .rhiza_commit = "0000000000000000000000000000000000000000" else . end' \
  "$tmp/rhiza.jsonl" > "$tmp/rhiza-mixed-commit.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza-mixed-commit.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mixed Rhiza source provenance was accepted" >&2; exit 1
fi
jq -c 'if .record_type == "cell" and .cell_id == "f1-h60" then .cell_isolation.mode = "stateful" else . end' \
  "$tmp/rhiza.jsonl" > "$tmp/rhiza-mixed-isolation.jsonl"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza-mixed-isolation.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mixed isolation was accepted" >&2; exit 1
fi
jq '.phases[0].cell_isolation.cleanup_verified = false' "$tmp/hiqlite.json" > "$tmp/hiqlite-bad-isolation.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-bad-isolation.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing isolation proof was accepted" >&2; exit 1
fi
jq 'del(.phases[0].source_events)' "$tmp/hiqlite.json" > "$tmp/hiqlite-no-events.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-no-events.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "Hiqlite evidence without raw events was accepted" >&2; exit 1
fi
jq '.phases[0].service_rto_seconds = 99' "$tmp/hiqlite.json" > "$tmp/hiqlite-forged-rto.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-forged-rto.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "Hiqlite aggregate timing not bound to raw phase was accepted" >&2; exit 1
fi
jq '.phases[0].source_events = .phases[0].source_artifact' \
  "$tmp/hiqlite.json" > "$tmp/hiqlite-summary-as-events.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-summary-as-events.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "Hiqlite summary artifact was accepted as an event stream" >&2; exit 1
fi
proof_to_mutate="$(jq -r '.phases[0].image_proofs[0].path' "$tmp/hiqlite.json")"
printf 'mutated\n' >> "$proof_to_mutate"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "mutated image proof was accepted" >&2; exit 1
fi
hiqlite_phase 1 60 > "$tmp/repair-phase.json"
jq -s '.[0] as $replacement | .[1] | .phases[0] = $replacement' "$tmp/repair-phase.json" "$tmp/hiqlite.json" > "$tmp/repaired-hiqlite.json"
proof_to_delete="$(jq -r '.phases[0].image_proofs[0].path' "$tmp/repaired-hiqlite.json")"
rm -f "$proof_to_delete"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/repaired-hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "deleted image proof was accepted" >&2; exit 1
fi
hiqlite_phase 1 60 > "$tmp/repair-phase.json"
jq -s '.[0] as $replacement | .[1] | .phases[0] = $replacement' "$tmp/repair-phase.json" "$tmp/hiqlite.json" > "$tmp/repaired-hiqlite.json"
jq 'del(.phases[0].image_proofs[1])' "$tmp/repaired-hiqlite.json" > "$tmp/hiqlite-missing-proof-stage.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-missing-proof-stage.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "missing image proof stage was accepted" >&2; exit 1
fi
jq '.phases[1].image_proofs[0] = .phases[0].image_proofs[0]' "$tmp/repaired-hiqlite.json" > "$tmp/hiqlite-cross-cell-proof.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-cross-cell-proof.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "cross-cell image proof was accepted" >&2; exit 1
fi
jq '.phases[1].baseline_proof = .phases[0].baseline_proof' "$tmp/repaired-hiqlite.json" > "$tmp/hiqlite-cross-cell-baseline-path.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-cross-cell-baseline-path.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "cross-cell baseline proof path was accepted" >&2; exit 1
fi
fixture_reuse_uid=1 hiqlite_phase 1 60 > "$tmp/reused-uid-phase.json"
unset fixture_reuse_uid
jq -s '.[0] as $replacement | .[1] | .phases[0] = $replacement' "$tmp/reused-uid-phase.json" "$tmp/hiqlite.json" > "$tmp/hiqlite-reused-voter-uid.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-reused-voter-uid.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "reused voter UID across image proof stages was accepted" >&2; exit 1
fi
baseline_path="$(jq -r '.phases[0].baseline_proof.path' "$tmp/repaired-hiqlite.json")"
cp "$baseline_path" "$tmp/baseline-original.json"
for forged in foreign-cell missing-voter found-true zero-attempts reset-unacknowledged reset-ack-missing; do
  case "$forged" in
    foreign-cell) jq '.cell_id = "f9-h999"' "$tmp/baseline-original.json" > "$baseline_path" ;;
    missing-voter) jq 'del(.pre.records[2])' "$tmp/baseline-original.json" > "$baseline_path" ;;
    found-true) jq '.post.records[0].raw = "{\"found\":true}"' "$tmp/baseline-original.json" > "$baseline_path" ;;
    zero-attempts) jq '.post.records[0].attempts = 0' "$tmp/baseline-original.json" > "$baseline_path" ;;
    reset-unacknowledged) jq '.reset.acknowledged = false' "$tmp/baseline-original.json" > "$baseline_path" ;;
    reset-ack-missing) jq 'del(.reset.acknowledged)' "$tmp/baseline-original.json" > "$baseline_path" ;;
  esac
  if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/repaired-hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
    echo "forged baseline proof ($forged) was accepted" >&2; exit 1
  fi
done
cp "$tmp/baseline-original.json" "$baseline_path"
jq '.phases[0].baseline_proof.sha256 = ("0" * 64)' "$tmp/repaired-hiqlite.json" > "$tmp/hiqlite-wrong-baseline-sha.json"
if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/hiqlite-wrong-baseline-sha.json" "$tmp/no.json" >/dev/null 2>&1; then
  echo "wrong baseline proof SHA was accepted" >&2; exit 1
fi
write_path="$(jq -r '.phases[0].idempotent_recovery_write.path' "$tmp/repaired-hiqlite.json")"
cp "$write_path" "$tmp/idempotent-write-original.json"
for forged in idempotent-foreign-id idempotent-foreign-cell idempotent-wrong-stage idempotent-cross-hold idempotent-mixed-value idempotent-malformed-ack idempotent-no-ack idempotent-final-wrong-value idempotent-skipped-attempt idempotent-duplicate-attempt; do
  case "$forged" in
    idempotent-foreign-id) jq '.id = "f9-restore-idempotent-foreign"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-foreign-cell) jq '.cell_id = "f9-h999"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-wrong-stage) jq '.stage = "post-recovery"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-cross-hold) jq '.cell_id = "f1-h180" | .id = "f1-h180-restore-idempotent-fixture" | .final.id = "f1-h180-restore-idempotent-fixture"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-mixed-value) jq '.attempts[0].raw = "{\"acknowledged\":true,\"id\":\"f1-restore-idempotent-fixture\",\"value\":\"other\"}"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-malformed-ack) jq '.attempts[0].raw = "{}"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-no-ack) jq '.attempts[0].classification = "ambiguous-retryable" | .attempts[0].rc = 1' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-final-wrong-value) jq '.final.value = "other"' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-skipped-attempt) jq '.attempts = [{attempt:2,rc:0,raw:.attempts[0].raw,classification:"acknowledged"}]' "$tmp/idempotent-write-original.json" > "$write_path" ;;
    idempotent-duplicate-attempt) jq '.attempts = [.attempts[0], (.attempts[0] + {attempt:1,rc:0,classification:"acknowledged"})]' "$tmp/idempotent-write-original.json" > "$write_path" ;;
  esac
  if scripts/bench-rhiza-hiqlite.sh normalize-recovery "$tmp/rhiza.jsonl" "$tmp/repaired-hiqlite.json" "$tmp/no.json" >/dev/null 2>&1; then
    echo "forged idempotent recovery proof ($forged) was accepted" >&2; exit 1
  fi
done
cp "$tmp/idempotent-write-original.json" "$write_path"
