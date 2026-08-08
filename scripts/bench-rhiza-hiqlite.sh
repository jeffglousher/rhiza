#!/usr/bin/env bash
# Generate, run, and normalize the Rhiza/Hiqlite comparison program.
# This script deliberately delegates cluster work to the established runners.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
expected_cells='["f1-h60","f1-h180","f1-h300","f2-h60","f2-h180","f2-h300","f3-h60","f3-h180","f3-h300"]'

die() { printf '%s\n' "$*" >&2; exit 1; }
path_parent() {
  local path="$1" parent
  case "$path" in */*) parent="${path%/*}" ;; *) parent=. ;; esac
  cd -P "$parent" 2>/dev/null && pwd -P
}
resolved_path() {
  local path="$1" parent name
  parent="$(path_parent "$path")" || die "cannot resolve parent directory: $path"
  name="${path##*/}"
  printf '%s/%s\n' "$parent" "$name"
}
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "missing SHA-256 command: sha256sum or shasum"
  fi
}
usage() {
  cat >&2 <<'EOF'
usage: scripts/bench-rhiza-hiqlite.sh COMMAND [arguments]

Commands:
  plan [OUTPUT]                         print the safe, machine-readable program plan
  run-recovery                          run the explicit 1,2,3 × 60,180,300 zero-PVC drills
  normalize-recovery RHIZA_JSONL HIQLITE_SUMMARY OUTPUT
                                        validate and join only measured recovery evidence

`plan` performs no cluster mutation. `run-recovery` is the only mutating command
and delegates deployment to scripts/e2e-vind-rustfs.sh and
scripts/e2e-hiqlite-recovery.sh.
EOF
}

emit_plan() {
  jq -n --argjson cells "$expected_cells" '
    {
      schema_version: 1,
      title: "Rhiza / Hiqlite executable comparison program",
      reference_baseline:{hiqlite:{release:"0.14.0",
        commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",openraft:"0.9.24",
        source_build_required:true,log_sync_modes:["immediate","immediate_async","interval"]},
        rhiza:{identity:"exact tested commit plus dirty-state flag"}},
      safety: {default_command:"plan", cluster_mutation:false,
        recovery_runner:"explicit run-recovery only"},
      executable_coverage:{recovery:"implemented",comparable_workload_resource:"pending",
        publishable_performance_comparison:false},
      adoption_hard_gates:[
        "correctness ledger and final state validation pass",
        "matched durability, topology, client path, and workload contract",
        "three rotated repetitions with raw provenance",
        "recovery matrix passes before adopting an availability claim",
        "do not publish a performance comparison until the comparable workload/resource runner exists"
      ],
      provenance_required: ["git_commit","git_dirty","image_digest","hardware",
        "kernel","filesystem","client_path","topology","durability_contract",
        "workload_seed","raw_artifact_paths","started_at","finished_at"],
      independent_scorecards: ["correctness_durability","steady_state_tail",
        "protocol_apply","failure_recovery","resource_object_cost"],
      contract_tiers: [
        {id:"D0",label:"diagnostic_memory",comparable:true},
        {id:"D1",label:"local_durable_quorum",comparable:true},
        {id:"D2",label:"single_volume_loss_rejoin",comparable:true},
        {id:"D3",label:"full_volume_restore",comparable:false,
          rhiza:"object-authoritative checkpoint",hiqlite:"backup restore"},
        {id:"D4",label:"rpo0_object_authoritative",comparable:false,
          rhiza:"sync checkpoint",hiqlite:"no equivalent per-write object contract"}
      ],
      non_comparable: [
        {dimension:"durability",labels:["D3","D4"],reason:"external-object ACK boundary differs"},
        {dimension:"graph",reason:"Hiqlite has no graph state machine"},
        {dimension:"kv",reason:"memory cache is not Rhiza persistent redb KV"},
        {dimension:"read",reason:"local/stale and consistent reads are separate leagues"},
        {dimension:"path",reason:"direct runtime and HA HTTP/TLS are separate leagues"}
      ],
      matrix: {
        profiles:["sql","kv","graph"], paths:["engine","state_machine","direct","ha_http_tls"],
        workloads:["single_write","transaction","batch","local_read","strong_read",
          "mixed_90r10w","mixed_50r50w","mixed_10r90w","scan","traversal"],
        batch_logical_ops:[1,2,8,32,64,256], concurrency:[1,4,16,64,256],
        payload_bytes:[64,1024,16384,262144], voters:[3,5,7],
        network_rtt_ms:[0.1,1,5,20,50], network_faults:["loss","jitter","reorder","partition"],
        recovery_cells:$cells, fault_types:["preferred_or_leader_kill","follower_kill",
          "one_volume_loss","two_peer_loss","three_peer_loss","object_store_outage",
          "checkpoint_during_fault","snapshot_or_log_corruption","rolling_replacement"],
        scale_db_gb:[1,10,100], soak_seconds:[1800,21600,86400]
      },
      mandatory_metrics: ["logical_ops_per_second","physical_log_entries_per_second",
        "latency_p50_p95_p99_p999_max","successes","errors","timeouts","retries",
        "ack_to_visible_seconds","queue_depth","apply_lag","fsync_count","fsync_seconds",
        "cpu_per_op","rss_peak","disk_bytes_per_op","network_bytes_per_op",
        "object_calls_by_method","object_bytes","retained_object_bytes","rpo",
        "service_rto_seconds","full_rto_seconds","full_redundancy_rto_seconds"],
      reporting_rules:["repeat_each_cell_at_least_three_times","rotate_run_order",
        "publish_median_and_iqr","retain_raw_artifacts","never_fill_missing_metrics",
        "deterministic_sql_writes_only","separate_cold_and_warm_cache_states",
        "never_compare_different_log_sync_modes"]
    }'
}

validate_cells() {
  local rhiza_jsonl="$1" hiqlite_summary="$2"
  [ -f "$rhiza_jsonl" ] || die "missing Rhiza source file: $rhiza_jsonl"
  [ -f "$hiqlite_summary" ] || die "missing Hiqlite source file: $hiqlite_summary"
  jq -e 'type == "array" and length == 9 and (unique | length) == 9' <<< "$expected_cells" >/dev/null
  jq -es --argjson expected "$expected_cells" '
    def finite_nonnegative: type == "number" and isfinite and . >= 0;
    def voter_array:
      . as $items |
      (type == "array" and length == 3) and
      ($items | all(.[]; type == "object" and (.pod|type) == "string" and (.pod|length) > 0 and
        (.uid|type) == "string" and (.uid|length) > 0)) and
      ($items | [.[].pod] | unique | length) == 3 and
      ($items | [.[].uid] | unique | length) == 3;
    [ .[] | select(.record_type == "cell") ] as $cells |
    [ .[] | select(.record_type == "summary") ] as $summaries |
    ($cells | length) == 9 and
    ($cells | all(.[]; .status == "passed" and (.run_id|type) == "string" and
      (.run_id|length) > 0 and .profile == "sql" and (.failed_peers|type) == "number" and
      (.failed_peers >= 1 and .failed_peers <= 3) and
      (.hold_requested_seconds | finite_nonnegative) and
      (.hold_actual_seconds | finite_nonnegative) and .hold_actual_seconds >= .hold_requested_seconds and
      (.service_rto_seconds | finite_nonnegative) and (.full_rto_seconds | finite_nonnegative) and
      .pvc_count == 0 and .ack_sentinel_preserved == true and
      .idempotency_boundary_verified == true and .markers_lost == true and .tip_hashes_equal == true and
      (.operator_dr|type) == "boolean" and
      (.old_pod_uids | voter_array) and (.new_pod_uids | voter_array) and
      (.failed_peers as $failed | .operator_dr as $operator |
        .old_pod_uids as $old | .new_pod_uids as $new |
        all(range(0; 3);
          if . < (if $operator then 0 else (3 - $failed) end) then
            $old[.].pod == $new[.].pod and $old[.].uid == $new[.].uid
          else
            $old[.].pod == $new[.].pod and $old[.].uid != $new[.].uid
          end)) and
      .cell_id == ("f\(.failed_peers)-h\(.hold_requested_seconds)"))) and
    ($cells | map(.cell_id) | sort) == ($expected | sort) and
    ($cells | map(.cell_id) | unique | length) == 9 and
    ($cells | map({run_id,profile}) | unique | length) == 1 and
    ($summaries | length) == 1 and $summaries[0].status == "passed" and
    ($summaries[0].run_id|type) == "string" and ($summaries[0].run_id|length) > 0 and
    $summaries[0].profile == "sql" and
    ({run_id:$summaries[0].run_id,profile:$summaries[0].profile} ==
      ($cells[0] | {run_id,profile}))
  ' "$rhiza_jsonl" >/dev/null || die "invalid Rhiza recovery matrix: require one passed zero-PVC cell for every exact matrix coordinate"
  jq -e --argjson expected "$expected_cells" '
    def finite_nonnegative: type == "number" and isfinite and . >= 0;
    . as $summary |
    $summary.system == "hiqlite" and $summary.voters == 3 and $summary.storage == "emptyDir" and
    $summary.zero_pvc == true and ($summary.phases|type) == "array" and ($summary.phases|length) == 9 and
    ([$summary.phases[] | .cell_id] | sort) == ($expected | sort) and
    ([$summary.phases[] | .cell_id] | unique | length) == 9 and
    ([$summary.phases[] | . as $phase | $phase.failure_count as $failed | $phase.hold_seconds as $hold |
      ($phase.failure_count|type) == "number" and ($failed >= 1 and $failed <= 3) and
      ($phase.hold_seconds | finite_nonnegative) and ($phase.failure_held_seconds | finite_nonnegative) and
      $phase.failure_held_seconds >= $phase.hold_seconds and ($phase.service_rto_seconds | finite_nonnegative) and
      ($phase.full_rto_seconds | finite_nonnegative) and ($phase.expected_vs_observed | type) == "object" and
      ($phase.expected_vs_observed.expected | type) == "object" and
      ($phase.expected_vs_observed.observed | type) == "object" and
      $phase.phase == ("f\($failed)") and
      $phase.cell_id == ("f\($failed)-h\($hold)")] | all)
  ' "$hiqlite_summary" >/dev/null || die "invalid Hiqlite recovery summary: require three voters, emptyDir, zero PVC, and every exact matrix coordinate"
}

normalize_recovery() {
  local rhiza_jsonl="$1" hiqlite_summary="$2" output="$3" output_dir output_path
  local rhiza_path hiqlite_path rhiza_sha256 hiqlite_sha256 temp_output
  output_dir="$(path_parent "$output")" || mkdir -p "$(dirname "$output")"
  output_dir="$(path_parent "$output")" || die "cannot create output directory: $output"
  rhiza_path="$(resolved_path "$rhiza_jsonl")"
  hiqlite_path="$(resolved_path "$hiqlite_summary")"
  output_path="$(resolved_path "$output")"
  if [ "$output_path" = "$rhiza_path" ] || [ "$output_path" = "$hiqlite_path" ] || \
    { [ -e "$output" ] && { [ "$output" -ef "$rhiza_jsonl" ] || [ "$output" -ef "$hiqlite_summary" ]; }; }; then
    die "output must not resolve to either source artifact"
  fi
  validate_cells "$rhiza_jsonl" "$hiqlite_summary"
  rhiza_sha256="$(sha256_file "$rhiza_jsonl")"
  hiqlite_sha256="$(sha256_file "$hiqlite_summary")"
  temp_output="$(mktemp "$output_dir/.${output##*/}.tmp.XXXXXX")"
  trap 'rm -f "$temp_output"' RETURN
  jq -n --arg rhiza_source "$rhiza_path" --arg hiqlite_source "$hiqlite_path" \
    --arg rhiza_sha256 "$rhiza_sha256" --arg hiqlite_sha256 "$hiqlite_sha256" \
    --rawfile rhiza_raw "$rhiza_jsonl" --slurpfile hiqlite "$hiqlite_summary" '
      ($rhiza_raw | split("\n") | map(select(length > 0) | fromjson) |
        map(select(.record_type == "cell")) | sort_by(.cell_id)) as $r |
      ($hiqlite[0].phases | sort_by(.cell_id)) as $h |
      {schema_version:1,kind:"rhiza_hiqlite_recovery_normalization",
       source_artifacts:{rhiza_jsonl:{path:$rhiza_source,sha256:$rhiza_sha256},
         hiqlite_summary:{path:$hiqlite_source,sha256:$hiqlite_sha256}},
       source_provenance:{
         rhiza_cells_common:([$r[] | {run_id,profile} | tojson] | unique | map(fromjson)),
         hiqlite_summary:($hiqlite[0] | del(.phases))},
       topology:{rhiza:{voters:3,storage:"zero-pvc ephemeral pod filesystem",zero_pvc:true},
         hiqlite:{voters:$hiqlite[0].voters,storage:$hiqlite[0].storage,zero_pvc:$hiqlite[0].zero_pvc}},
       durability_comparison:{status:"non_comparable",rhiza:"object-authoritative recovery semantics",
         hiqlite:"backup/snapshot recovery semantics"},
       metrics_policy:"not_measured is preserved; this normalizer does not infer throughput or resource data",
       cells:[$r[] as $rc | $h[] | select(.cell_id == $rc.cell_id) |
         {cell_id:$rc.cell_id,failure_count:$rc.failed_peers,hold_seconds:$rc.hold_requested_seconds,
          rhiza:{status:$rc.status,service_rto_seconds:$rc.service_rto_seconds,
            full_rto_seconds:$rc.full_rto_seconds,rpo_boundary:$rc.rpo_boundary,
            operator_dr:$rc.operator_dr,
            throughput:"not_measured",resource:"not_measured"},
          hiqlite:{service_rto_seconds:.service_rto_seconds,full_rto_seconds:.full_rto_seconds,
            throughput:"not_measured",resource:"not_measured"}}]}
    ' > "$temp_output"
  mv -f "$temp_output" "$output_path"
  trap - RETURN
}

run_recovery() {
  local coordinator_id coordinator_dir rhiza_root hiqlite_root rhiza_artifact hiqlite_artifact
  local rhiza_sha256 hiqlite_sha256
  local -a run_env
  coordinator_id="$(date -u +%Y%m%d-%H%M%S)-$$"
  coordinator_dir="$repo_root/target/rhiza-hiqlite-recovery/$coordinator_id"
  rhiza_root="$coordinator_dir/rhiza"
  hiqlite_root="$coordinator_dir/hiqlite"
  mkdir -p "$rhiza_root" "$hiqlite_root"
  run_env=("RHIZA_EXECUTION_PROFILE=sql" "RHIZA_E2E_RECOVERY_MATRIX=1"
    "RHIZA_E2E_RECOVERY_MATRIX_ONLY=1" "RHIZA_RECOVERY_FAIL_PEERS=1,2,3"
    "RHIZA_RECOVERY_HOLD_SECONDS=60,180,300" "RHIZA_E2E_TARGET_DIR=$rhiza_root")
  jq -n --arg coordinator_id "$coordinator_id" --arg root "$coordinator_dir" \
    '{schema_version:1,kind:"rhiza_hiqlite_recovery_run_started",coordinator_id:$coordinator_id,
    matrix:{failures:[1,2,3],holds_seconds:[60,180,300]},
    runners:{rhiza:"scripts/e2e-vind-rustfs.sh",hiqlite:"scripts/e2e-hiqlite-recovery.sh"},
    artifact_root:$root}' >&2
  printf '%s\n' 'Running Rhiza zero-PVC recovery matrix through the established runner.' >&2
  env "${run_env[@]}" "$repo_root/scripts/e2e-vind-rustfs.sh" >&2
  printf '%s\n' 'Running Hiqlite zero-PVC recovery matrix through the established runner.' >&2
  env HIQLITE_RECOVERY_FAIL_PEERS=1,2,3 HIQLITE_RECOVERY_HOLD_SECONDS=60,180,300 \
    HIQLITE_RECOVERY_TARGET_DIR="$hiqlite_root" \
    "$repo_root/scripts/e2e-hiqlite-recovery.sh" >&2
  [ "$(find "$rhiza_root" -type f -name recovery-matrix.jsonl -print | wc -l | tr -d ' ')" -eq 1 ] ||
    die "expected exactly one Rhiza artifact under $rhiza_root"
  [ "$(find "$hiqlite_root" -type f -name summary.json -print | wc -l | tr -d ' ')" -eq 1 ] ||
    die "expected exactly one Hiqlite artifact under $hiqlite_root"
  rhiza_artifact="$(find "$rhiza_root" -type f -name recovery-matrix.jsonl -print)"
  hiqlite_artifact="$(find "$hiqlite_root" -type f -name summary.json -print)"
  rhiza_sha256="$(sha256_file "$rhiza_artifact")"
  hiqlite_sha256="$(sha256_file "$hiqlite_artifact")"
  jq -n --arg coordinator_id "$coordinator_id" --arg rhiza "$rhiza_artifact" \
    --arg hiqlite "$hiqlite_artifact" --arg rhiza_sha256 "$rhiza_sha256" \
    --arg hiqlite_sha256 "$hiqlite_sha256" \
    --arg root "$coordinator_dir" \
    '{schema_version:1,kind:"rhiza_hiqlite_recovery_run_completed",coordinator_id:$coordinator_id,
      matrix:{failures:[1,2,3],holds_seconds:[60,180,300]},
      artifact_root:$root,
      source_artifacts:{rhiza_jsonl:{path:$rhiza,sha256:$rhiza_sha256},
        hiqlite_summary:{path:$hiqlite,sha256:$hiqlite_sha256}}}'
}

case "${1:-}" in
  plan)
    [ "$#" -le 2 ] || { usage; exit 64; }
    if [ "$#" -eq 2 ]; then mkdir -p "$(dirname "$2")"; emit_plan > "$2"; else emit_plan; fi
    ;;
  run-recovery)
    [ "$#" -eq 1 ] || { usage; exit 64; }
    run_recovery
    ;;
  normalize-recovery)
    [ "$#" -eq 4 ] || { usage; exit 64; }
    normalize_recovery "$2" "$3" "$4"
    ;;
  *) usage; exit 64 ;;
esac
