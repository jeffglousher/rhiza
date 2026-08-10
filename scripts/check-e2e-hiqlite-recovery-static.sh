#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script="$repo_root/scripts/e2e-hiqlite-recovery.sh"
client_dir="$repo_root/bench/hiqlite-recovery-client"
cluster_manifest="$repo_root/deploy/k8s/hiqlite-recovery-cluster.yaml"
object_manifest="$repo_root/deploy/k8s/hiqlite-recovery-rustfs.yaml"
readme="$client_dir/README.md"

require_literal() {
  file="$1"
  literal="$2"
  grep -Fq -- "$literal" "$file" || {
    echo "missing Hiqlite recovery contract in ${file#"$repo_root"/}: $literal" >&2
    exit 1
  }
}

for file in \
  "$script" \
  "$client_dir/Cargo.toml" \
  "$client_dir/src/main.rs" \
  "$client_dir/Dockerfile.server" \
  "$readme" \
  "$cluster_manifest" \
  "$object_manifest"; do
  test -f "$file" || {
    echo "missing Hiqlite recovery artifact: ${file#"$repo_root"/}" >&2
    exit 1
  }
done

bash -n "$script"
yq eval '.' "$cluster_manifest" "$object_manifest" >/dev/null

require_literal "$script" 'c8316c53799c509990475ea8e2aa2ef8679e070e'
require_literal "$script" 'HIQLITE_SOURCE_DIR'
require_literal "$script" 'rev-parse --is-inside-work-tree'
require_literal "$script" 'HIQLITE_BUILD_IMAGE:-1'
require_literal "$script" 'HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES:-0'
require_literal "$script" 'HIQLITE_RECOVERY_SKIP_IMAGE_LOAD:-0'
require_literal "$script" 'HIQLITE_RECOVERY_DIRECT_CLUSTER:-0'
require_literal "$script" 'HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER:-0'
require_literal "$script" 'requires HIQLITE_RECOVERY_DIRECT_CLUSTER=0'
require_literal "$script" 'requires HIQLITE_RECOVERY_REUSE_EXISTING=0'
require_literal "$script" 'requires exactly one recovery cell'
require_literal "$script" 'refuses an existing vcluster'
require_literal "$script" 'did not create a vcluster'
# shellcheck disable=SC2016
require_literal "$script" '(cd "$target" && vcluster node load-image "$node" --image "$image")'
# shellcheck disable=SC2016
require_literal "$script" '(cd "$target" && vcluster node load-image "$node" --image "$ingress_image")'
[ "$(grep -Fc 'vcluster node load-image' "$script")" -eq 2 ] || {
  echo "every Hiqlite recovery image load must run inside the target directory" >&2
  exit 1
}
require_literal "$script" 'direct_namespaces_created'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'owner="$(kubectl --context "$context" get namespace "$candidate"'
require_literal "$script" 'HIQLITE_RECOVERY_EXPECTED_LOCAL_IMAGE_ID'
require_literal "$script" 'HIQLITE_RECOVERY_EXPECTED_LOCAL_PROXY_IMAGE_ID'
require_literal "$script" 'verified-local-exact-source-reuse'
require_literal "$script" 'HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_IMAGE'
require_literal "$script" 'image_source=user-supplied-prebuilt'
require_literal "$script" 'source_commit_basis=user-supplied-unverified'
require_literal "$script" 'image_source_commit=""'
# These assertions intentionally match unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$build_source_dir"'
# shellcheck disable=SC2016
require_literal "$script" 'cargo generate-lockfile --manifest-path "$build_source_dir/Cargo.toml"'
require_literal "$script" 'lockfile_origin=generated-from-exact-source'
require_literal "$script" 'cargo_lock_sha256'
require_literal "$script" 'ingress_kind=hiqlite-application-proxy'
require_literal "$script" 'hiqlite-proxy-axum8.patch'
require_literal "$script" 'proxy_patch_sha256'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'patch --directory "$proxy_build_source_dir" --strip=1 < "$proxy_patch_file"'
require_literal "$script" 'upstream_proxy_incompatibility='
require_literal "$script" 'omits the stream raft-type path'
require_literal "$script" 'resolved_image'
require_literal "$script" 'resolved_proxy_image'
require_literal "$script" 'resolved_proxy_image_id'
require_literal "$script" 'HIQLITE_RECOVERY_PROXY_IMAGE'
require_literal "$script" 'verify_live_image_ids'
require_literal "$script" 'capture_direct_live_image_ids'
require_literal "$script" 'live-image-ids.json'
# These assertions intentionally match unexpanded jq source.
# shellcheck disable=SC2016
require_literal "$script" 'image_provenance_verified:$image_provenance_verified'
# shellcheck disable=SC2016
require_literal "$script" 'image_provenance_publishable:$image_provenance_publishable'
require_literal "$script" 'verification_mode:"direct-live-tags-only"'
require_literal "$script" 'image_source'
require_literal "$script" 'source_commit_basis'
require_literal "$script" 'HIQLITE_RECOVERY_HOLD_SECONDS:-60,180,300'
require_literal "$script" 'HIQLITE_RECOVERY_FAIL_PEERS:-1,2,3'
require_literal "$script" 'HIQLITE_RECOVERY_QUORUM_LOSS_TIMEOUT_SECONDS:-60'
require_literal "$script" 'wait_failure_established'
require_literal "$script" 'wait_f2_failure_established'
require_literal "$script" 'precondition_started_epoch'
require_literal "$script" 'precondition_ended_epoch'
require_literal "$script" 'write-ack-violation'
require_literal "$script" 'capture_f2_post_ack_evidence'
require_literal "$script" '--rawfile voter_log_raw'
require_literal "$script" 'embedded_raw:false'
require_literal "$script" 'could not assemble post-ACK evidence descriptor'
require_literal "$script" '--slurpfile post_ack'
require_literal "$script" 'ACK failure proof does not bind the acknowledgement and post-ACK evidence'
require_literal "$script" 'could not atomically publish ACK failure proof'
require_literal "$script" 'unilateral_state_machine_apply'
require_literal "$script" 'ack_without_local_apply'
require_literal "$script" 'ack_post_state_unknown'
require_literal "$script" 'HIQLITE_RECOVERY_EXPECTED_LOCKFILE_PATH'
require_literal "$script" 'derive_openraft_version'
require_literal "$script" 'openraft_version_source=generated-cargo-lock'
require_literal "$script" 'QuorumNotEnough'
require_literal "$script" 'transient_write_acks'
require_literal "$script" 'failure-established'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'probe_budget=$((probe_timeout * 4))'
require_literal "$script" 'ensure_port_forward'
require_literal "$script" 'ensure_port_forward_recoverable'
require_literal "$script" 'run_client_hard_timeout'
require_literal "$script" '--kill-after=2s'
require_literal "$script" 'metrics_to_recoverable'
require_literal "$script" 'start_port_forward 1 || return 1'
require_literal "$script" 'port_forward_pid=""'
require_literal "$script" 'wait_direct_quorum_after_restore'
require_literal "$script" 'wait_proxy_readonly_after_restore'
require_literal "$script" 'run_idempotent_restore_write'
require_literal "$script" 'idempotent-final-state-single-key-not-exactly-once'
require_literal "$script" 'ambiguous-retryable'
require_literal "$script" 'start_direct_voter_port_forwards'
require_literal "$script" 'stop_direct_port_forwards'
require_literal "$script" 'start_direct_survivor_port_forward'
# shellcheck disable=SC2016
require_literal "$script" 'pod/hiqlite-recovery-0 "$port:8200"'
require_literal "$script" '.voter_ids == [1,2,3] and .node_ids == [1,2,3]'
require_literal "$script" 'proxy-readonly-timeout'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'if ! kill -0 "$port_forward_pid"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'for failure_count in "${failure_values[@]}"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'for hold_seconds in "${hold_values[@]}"'
require_literal "$script" "cell_id=\"f1-h\$1\""
require_literal "$script" "cell_id=\"f2-h\$1\""
require_literal "$script" "cell_id=\"f3-h\$1\""
# These assertions intentionally match unexpanded jq source.
# shellcheck disable=SC2016
require_literal "$script" '($phases | length) == $expected_cell_count'
# shellcheck disable=SC2016
require_literal "$script" '($phases | map(.cell_id) | unique | length) == $expected_cell_count'
require_literal "$script" '(.hold_seconds | type) == "number"'
require_literal "$script" '(.failure_held_seconds >= .hold_seconds)'
require_literal "$script" '(.cell_id == "f\(.failure_count)-h\(.hold_seconds)")'
require_literal "$script" 'log_sync=Immediate'
require_literal "$script" 'recovery.jsonl'
require_literal "$script" 'summary.json'
require_literal "$script" 'service_rto_seconds'
require_literal "$script" 'full_rto_seconds'
require_literal "$script" 'failure_started_at'
require_literal "$script" 'failure_released_at'
require_literal "$script" 'expected_vs_observed'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas="$survivors"'
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas=3'
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas=0'
require_literal "$script" 'HQL_BACKUP_RESTORE'
require_literal "$script" 'clear_restore_from_running_pods'
require_literal "$script" 'rustfs_uid'
require_literal "$script" 'object_namespace_uid'
require_literal "$script" 'initial-object-inventory.json'
require_literal "$script" 'capture_object_inventory initial true'
require_literal "$script" 'backup-evidence.json'
require_literal "$script" 'inventory_after_sha256'
require_literal "$script" 'vcluster_node_uid'
require_literal "$script" 'prepare_matrix_cell'
require_literal "$script" 'run_client 30 reset'
require_literal "$script" 'k delete statefulset hiqlite-recovery --cascade=foreground --wait=true'
require_literal "$script" 'delete_previous_cell_namespace'
# shellcheck disable=SC2016
require_literal "$script" 'kubectl --context "$context" delete namespace "$candidate" --wait=true'
# shellcheck disable=SC2016
require_literal "$script" 'namespace="hiqlite-cell-${run_id}-${cell_id}-${matrix_cell_index}"'
# shellcheck disable=SC2016
require_literal "$script" 'create_managed_namespace "$namespace"'
# This assertion intentionally matches the fail-closed owner predicate.
# shellcheck disable=SC2016
require_literal "$script" '[ "$managed" = true ] && [ "$owner" = "$run_id" ]'
require_literal "$script" 'refusing to replace namespace not owned by this run'
# These assertions intentionally match unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'k apply -f "$target/hiqlite.yaml"'
require_literal "$script" 'start_direct_voter_port_forwards'
require_literal "$script" 'run_direct_voter_client'
require_literal "$script" 'verify_direct_empty_baseline'
require_literal "$script" 'no such table: hiqlite_recovery_sentinel'
require_literal "$script" 'did not prove all voters have an absent sentinel table before reset'
require_literal "$script" 'verify_direct_reset_baseline'
require_literal "$script" 'baseline-proof.json'
require_literal "$script" 'baseline_proof:{path:'
require_literal "$script" 'did not converge to post-reset empty baseline'
require_literal "$script" 'reset exited 0 without acknowledged:true'
require_literal "$script" 'capture_cell_identity'
require_literal "$script" 'require_cell_identity'
require_literal "$script" 'require_no_old_identity_uids'
require_literal "$script" 'controller_revision_hash'
# This assertion intentionally matches unexpanded jq source.
# shellcheck disable=SC2016
require_literal "$script" 'all(.[]; .controller_revision_hash == $update_revision)'
require_literal "$script" 'endpoint_target_uids'
require_literal "$script" 'cell-isolation'
require_literal "$script" 'fresh-managed-namespace'
require_literal "$script" 'backup key was reused'
require_literal "$script" 'capture_convergence_diagnostics'
require_literal "$script" 'previous.log'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'stderr_output="${output%.json}.stderr"'
# shellcheck disable=SC2016
require_literal "$script" 'rc_output="${output%.json}.rc"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'printf '\''%s\n'\'' "$status" > "$rc_output"'
capture_diagnostics_body="$(awk '
  /^capture_convergence_diagnostics\(\)/ { capture = 1 }
  capture { print }
  capture && /^}/ { exit }
' "$script")"
if grep -Fq 'metrics_to' <<< "$capture_diagnostics_body"; then
  echo "Hiqlite convergence diagnostics must not overwrite terminal metrics evidence" >&2
  exit 1
fi
require_literal "$script" 'cell_isolation:{success:true'
# shellcheck disable=SC2016 # Match the literal Bash array expression.
require_literal "$script" 'if (( ${#direct_port_forward_pids[@]} > 0 )); then'
# shellcheck disable=SC2016 # Match literal Bash array length guards.
require_literal "$script" 'if (( ${#cell_namespaces[@]} > 0 )); then'
# shellcheck disable=SC2016 # Match literal Bash array length guards.
require_literal "$script" 'if (( ${#seen_backup_keys[@]} > 0 )); then'
require_literal "$script" 'requires coordinator-supplied HIQLITE_RECOVERY_PROXY_IMAGE'
require_literal "$script" 'sub("^(docker-pullable|docker|containerd)://"; "")'
require_literal "$script" 'capture_node_cri_image_ids'
require_literal "$script" 'HIQLITE_RECOVERY_IMAGE and HIQLITE_RECOVERY_PROXY_IMAGE must be distinct'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'control_plane="vcluster.cp.$node"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'docker exec "$control_plane" crictl images --output json'
require_literal "$script" 'canonical_ref'
require_literal "$script" 'cri_image_id_candidates'
require_literal "$script" 'expected_node_cri'
require_literal "$script" 'node_cri'
require_literal "$script" 'local_docker'
# These assertions intentionally match unexpanded shell/jq source.
# shellcheck disable=SC2016
require_literal "$script" 'docker save "$ref"'
require_literal "$script" 'capture_local_image_config_ids'
require_literal "$script" 'local_voter_config_pre_sha'
# shellcheck disable=SC2016
require_literal "$script" '$record.id'
require_literal "$script" 'capture_cell_image_proof'
require_literal "$script" 'post-operator-dr'
require_literal "$script" 'post-restore-clear'
require_literal "$script" 'expected_image_proof_stages'
require_literal "$script" 'transition-ledger.jsonl'
require_literal "$script" 'initialize_transition_ledger'
require_literal "$script" '.acknowledged == true'
require_literal "$script" 'exited 0 without acknowledged:true'
require_literal "$script" 'verify_transition_ledger present'
require_literal "$script" 'verify_transition_ledger absent'
require_literal "$script" 'image-provenance-manifest.json'
require_literal "$script" 'canonical_docker_ref'
require_literal "$script" 'canonicalize to the same image'
require_literal "$script" 'mismatch_reason'
# These assertions intentionally match atomic diagnostic writes.
# shellcheck disable=SC2016
require_literal "$script" 'mv "$tmp" "$output"'
same_tag_stderr=""
if same_tag_stderr="$(HIQLITE_RECOVERY_IMAGE=hiqlite-recovery:collision \
  HIQLITE_RECOVERY_PROXY_IMAGE=hiqlite-recovery:collision \
  bash "$script" 2>&1)"; then
  echo "Hiqlite recovery accepted the same voter and proxy image tag" >&2
  exit 1
fi
rg -F 'HIQLITE_RECOVERY_IMAGE and HIQLITE_RECOVERY_PROXY_IMAGE must be distinct' \
  <<< "$same_tag_stderr" >/dev/null || {
    echo "Hiqlite recovery same-tag guard did not fail at the intended boundary" >&2
    exit 1
  }
# shellcheck disable=SC2016 # Match literal jq variables.
require_literal "$script" 'resolved_image_repo_digest'
# shellcheck disable=SC2016 # Match literal jq variables.
require_literal "$script" 'resolved_proxy_image_repo_digest'
require_literal "$script" 'matched_expected_id'
# shellcheck disable=SC2016 # Match literal jq type guard.
require_literal "$script" '(.matched_expected_id | type) == "string"'
require_literal "$script" 'controller_revision_proven:true'
# shellcheck disable=SC2016
require_literal "$script" 'baseline_direct_reads:$cell_baseline_direct_reads'
# shellcheck disable=SC2016
require_literal "$script" 'backup_key_unique:$cell_backup_key_unique'
require_literal "$script" 'markers_lost'
require_literal "$script" 'capture_learner_to_voter_evidence'
require_literal "$script" 'assert_probe_outcome'
require_literal "$script" 'voter_ids'
require_literal "$script" 'ack_sentinel_preserved'
require_literal "$script" 'rpo_to_backup'
require_literal "$script" '"auto_recovery":true,"operator_dr":false'
require_literal "$script" '"auto_recovery":false,"operator_dr":true'

require_literal "$client_dir/Cargo.toml" 'rev = "c8316c53799c509990475ea8e2aa2ef8679e070e"'
require_literal "$client_dir/Cargo.toml" 'default-features = false, features = ["full"]'
require_literal "$client_dir/Cargo.toml" '[workspace]'
require_literal "$client_dir/src/main.rs" 'Client::remote(args.nodes, false, false, args.secret, true, None, None)'
require_literal "$client_dir/Dockerfile.server" 'cargo build --locked --features server --release'
require_literal "$client_dir/Dockerfile.server" 'id=hiqlite-recovery-cargo-registry'
require_literal "$client_dir/Dockerfile.server" 'id=hiqlite-recovery-cargo-target'
require_literal "$client_dir/Dockerfile.server" 'install -D -m 0755 /work/target/release/hiqlite /out/hiqlite'
require_literal "$client_dir/Dockerfile.server" 'COPY --from=builder /out/hiqlite /app/hiqlite'
require_literal "$readme" 'f1-h60'
require_literal "$readme" 'f3-h300'
require_literal "$readme" 'feature-gated bincode wire schema'

if grep -Fq 'ghcr.io/sebadob/hiqlite' "$script" "$readme"; then
  echo "Hiqlite recovery harness references a nonexistent official image" >&2
  exit 1
fi
for command in execute reset query-local query-consistent backup metrics verify-sentinel; do
  require_literal "$client_dir/src/main.rs" "$command"
done
restore_clear_direct_contract='(.uid_current == true and .endpoint_current == true and .running == true and
  .leader != null and .voter_ids == [1,2,3] and .node_ids == [1,2,3])'
jq -e "$restore_clear_direct_contract" \
  <<< '{"uid_current":true,"endpoint_current":true,"running":true,"leader":1,"voter_ids":[1,2,3],"node_ids":[1,2,3]}' >/dev/null
if jq -e "$restore_clear_direct_contract" \
  <<< '{"uid_current":true,"endpoint_current":true,"running":true,"leader":1,"voter_ids":[1],"node_ids":[1]}' >/dev/null; then
  echo "Hiqlite recovery restore-clear direct gate accepted membership {1}" >&2
  exit 1
fi
f2_precondition_contract='(.cell_id == "f2-h60" and .precondition_ended_epoch >= .precondition_started_epoch and
  (.precondition_ended_epoch - .precondition_started_epoch) <= 2 and
  [.sequence[].kind] == ["pods","endpoints","metrics","consistent"] and
  .sequence[0].rc == 0 and .sequence[1].rc == 0 and .sequence[2].rc == 0 and .sequence[3].rc != 0 and
  .proven == true)'
jq -e "$f2_precondition_contract" <<< '{"cell_id":"f2-h60","precondition_started_epoch":10,"precondition_ended_epoch":12,"sequence":[{"kind":"pods","rc":0,"raw":"{}"},{"kind":"endpoints","rc":0,"raw":"{}"},{"kind":"metrics","rc":0,"raw":"{}"},{"kind":"consistent","rc":1,"raw":"QuorumNotEnough got: {1}"}],"proven":true}' >/dev/null
for invalid in \
  '{"cell_id":"f2-h60","precondition_started_epoch":10,"precondition_ended_epoch":13,"sequence":[{"kind":"pods","rc":0},{"kind":"endpoints","rc":0},{"kind":"metrics","rc":0},{"kind":"consistent","rc":1}],"proven":true}' \
  '{"cell_id":"f2-h60","precondition_started_epoch":10,"precondition_ended_epoch":11,"sequence":[{"kind":"pods","rc":0},{"kind":"metrics","rc":0},{"kind":"endpoints","rc":0},{"kind":"consistent","rc":1}],"proven":true}' \
  '{"cell_id":"f2-h60","precondition_started_epoch":10,"precondition_ended_epoch":11,"sequence":[{"kind":"pods","rc":0},{"kind":"endpoints","rc":0},{"kind":"metrics","rc":1},{"kind":"consistent","rc":1}],"proven":true}'; do
  if jq -e "$f2_precondition_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite F2 precondition fixture accepted invalid order/time/rc evidence" >&2; exit 1
  fi
done
proxy_readonly_contract='(.proxy_available == true and .port_forward_recovered == true and .sentinel_current == true)'
jq -e "$proxy_readonly_contract" <<< '{"proxy_available":true,"port_forward_recovered":true,"sentinel_current":true}' >/dev/null
for invalid in '{"proxy_available":false,"port_forward_recovered":false,"sentinel_current":true}' '{"proxy_available":true,"port_forward_recovered":true,"sentinel_current":false}'; do
  if jq -e "$proxy_readonly_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery proxy readonly contract accepted persistent proxy failure or stale sentinel" >&2
    exit 1
  fi
done
direct_pf_rebind_contract='(.old_terminated == true and .old_waited == true and .same_ports_rebound == true and
  .tracked_pids == .new_pids and (.tracked_pids | length) == 3 and .cleanup_empty == true)'
jq -e "$direct_pf_rebind_contract" \
  <<< '{"old_terminated":true,"old_waited":true,"same_ports_rebound":true,"tracked_pids":[101,102,103],"new_pids":[101,102,103],"cleanup_empty":true}' >/dev/null
if jq -e "$direct_pf_rebind_contract" \
  <<< '{"old_terminated":false,"old_waited":false,"same_ports_rebound":false,"tracked_pids":[1,2,3],"new_pids":[4,5,6],"cleanup_empty":false}' >/dev/null; then
  echo "Hiqlite recovery direct port-forward rebind contract accepted stale listeners" >&2
  exit 1
fi

if command -v timeout >/dev/null 2>&1; then
  timeout_fixture_bin=timeout
else
  timeout_fixture_bin=gtimeout
fi
"$timeout_fixture_bin" --signal=TERM --kill-after=1s 1s true
timeout_started="$(date +%s)"
if ("$timeout_fixture_bin" --signal=TERM --kill-after=1s 1s sh -c 'trap "" TERM; while :; do sleep 1; done') >/dev/null 2>&1; then
  echo "Hiqlite recovery hard-timeout fixture accepted TERM-ignoring child" >&2
  exit 1
else
  timeout_rc=$?
fi
timeout_elapsed=$(( $(date +%s) - timeout_started ))
[ "$timeout_rc" -ne 0 ] && [ "$timeout_elapsed" -le 4 ] || {
  echo "Hiqlite recovery hard-timeout fixture did not bound or preserve failure" >&2
  exit 1
}
idempotent_write_contract='length > 0 and .[-1].classification == "acknowledged" and
  all(.[]; (.attempt|type)=="number" and (.rc|type)=="number" and (.raw|type)=="string" and
    (.classification == "acknowledged" or .classification == "ambiguous-retryable"))'
jq -e "$idempotent_write_contract" <<< '[{"attempt":1,"rc":1,"raw":"transport timeout","classification":"ambiguous-retryable"},{"attempt":2,"rc":0,"raw":"ack","classification":"acknowledged"}]' >/dev/null
idempotent_rc_contract='.[0].classification == "ambiguous-retryable" and .[0].rc == 7 and .[1].classification == "acknowledged" and .[1].rc == 0'
jq -e "$idempotent_rc_contract" <<< '[{"attempt":1,"rc":7,"raw":"transport","classification":"ambiguous-retryable"},{"attempt":2,"rc":0,"raw":"ack","classification":"acknowledged"}]' >/dev/null
if jq -e "$idempotent_rc_contract" <<< '[{"attempt":1,"rc":0,"raw":"transport","classification":"ambiguous-retryable"},{"attempt":2,"rc":0,"raw":"ack","classification":"acknowledged"}]' >/dev/null; then
  echo "Hiqlite recovery idempotent write fixture accepted a lost transport exit status" >&2
  exit 1
fi
for invalid in \
  '[{"attempt":1,"rc":0,"raw":"mismatched ack","classification":"terminal"}]' \
  '[{"attempt":1,"rc":1,"raw":"timeout","classification":"ambiguous-retryable"}]'; do
  if jq -e "$idempotent_write_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery idempotent write contract accepted terminal or persistent ambiguous failure" >&2
    exit 1
  fi
done
# shellcheck disable=SC2016
idempotent_provenance_contract='.cell_id == $cell_id and .stage == "post-restore-clear" and
  (.id | startswith($cell_id + "-restore-idempotent-")) and .valid == true'
idempotent_provenance_fixture='{"cell_id":"f2-h180","stage":"post-restore-clear","id":"f2-h180-restore-idempotent-fixture","valid":true}'
jq -e --arg cell_id f2-h180 "$idempotent_provenance_contract" <<< "$idempotent_provenance_fixture" >/dev/null
for invalid in \
  '{"cell_id":"f1-h180","stage":"post-restore-clear","id":"f2-h180-restore-idempotent-fixture","valid":true}' \
  '{"cell_id":"f2-h180","stage":"pre-fault","id":"f2-h180-restore-idempotent-fixture","valid":true}'; do
  if jq -e --arg cell_id f2-h180 "$idempotent_provenance_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery idempotent proof accepted foreign cell or stage" >&2
    exit 1
  fi
done

test "$(yq eval 'select(.kind == "StatefulSet") | .spec.replicas' "$cluster_manifest")" = 3
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.updateStrategy.type' "$cluster_manifest")" = OnDelete
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.terminationGracePeriodSeconds' "$cluster_manifest")" = 0
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$cluster_manifest")" = true
test "$(yq eval 'select(.kind == "StatefulSet") | has("volumeClaimTemplates")' "$cluster_manifest")" = false
test "$(yq eval -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "hiqlite") | .env[] | select(.name == "HQL_LOG_SYNC") | .value' "$cluster_manifest")" = immediate
test "$(yq eval -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "hiqlite") | .imagePullPolicy' "$cluster_manifest")" = Never
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .image' "$cluster_manifest")" = __INGRESS_IMAGE__
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .imagePullPolicy' "$cluster_manifest")" = Never
test "$(yq eval -o=json 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .args' "$cluster_manifest" | jq -c .)" = '["proxy","--config-file","/dev/null","--log-level","debug"]'
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .readinessProbe.httpGet.path' "$cluster_manifest")" = /ping
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .env[] | select(.name == "HQL_SECRET_API") | .value' "$cluster_manifest")" = __SECRET_API__
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$object_manifest")" = true

if grep -Eq '(^|[[:space:]])(kind:[[:space:]]*PersistentVolumeClaim|volumeClaimTemplates:)' \
  "$cluster_manifest" "$object_manifest"; then
  echo "Hiqlite recovery drill must remain zero-PVC" >&2
  exit 1
fi

# jq expands these variables; the shell must preserve them literally.
# shellcheck disable=SC2016
summary_contract='
  .phases as $phases |
  ($phases | length) == 9 and
  ($phases | map(.cell_id)) == $expected_cells and
  ($phases | map(.cell_id) | unique | length) == 9 and
  ($phases | all(
    (.failure_count | type) == "number" and
    (.hold_seconds | type) == "number" and
    (.hold_seconds >= 0) and
    (.failure_held_seconds | type) == "number" and
    (.failure_held_seconds >= .hold_seconds) and
    (.cell_isolation.success == true) and
    (.cell_isolation.mode == "fresh-managed-namespace") and
    (.cell_isolation.namespace | type) == "string" and
    (.cell_isolation.namespace | length) > 0 and
    (.cell_isolation.release_scope == .cell_isolation.namespace) and
    (.cell_isolation.namespace_uid_proven == true) and
    (.cell_isolation.statefulset_uid_proven == true) and
    (.cell_isolation.voter_uids_proven == true) and
    (.cell_isolation.proxy_pod_uid_proven == true) and
    (.cell_isolation.controller_revision_proven == true) and
    (.cell_isolation.endpoint_target_uids_current == true) and
    (.cell_isolation.restore_env_absent == true) and
    (.cell_isolation.zero_pvc == true) and
    (.cell_isolation.no_host_path == true) and
    (.cell_isolation.rustfs_uid_stable == true) and
    (.cell_isolation.baseline_direct_reads == true) and
    (.cell_isolation.backup_key_unique == true) and
    (.phase == "f\(.failure_count)") and
    (.cell_id == "f\(.failure_count)-h\(.hold_seconds)")
  )) and .cell_isolation.all_cells_proven == true and
  (.cell_isolation.namespaces | unique | length) == 9
'
expected_cells='["f1-h60","f1-h180","f1-h300","f2-h60","f2-h180","f2-h300","f3-h60","f3-h180","f3-h300"]'
valid_summary="$(jq -cn '
  {phases:[
    range(1;4) as $failure |
    [60,180,300][] as $hold |
    {phase:"f\($failure)",cell_id:"f\($failure)-h\($hold)",failure_count:$failure,
      hold_seconds:$hold,failure_held_seconds:$hold,
      cell_isolation:{success:true,mode:"fresh-managed-namespace",fresh_vcluster_created:true,
        namespace:("hiqlite-cell-" + ($failure|tostring) + "-" + ($hold|tostring)),
        release_scope:("hiqlite-cell-" + ($failure|tostring) + "-" + ($hold|tostring)),
        namespace_uid_proven:true,statefulset_uid_proven:true,voter_uids_proven:true,
        proxy_pod_uid_proven:true,controller_revision_proven:true,
        endpoint_target_uids_current:true,restore_env_absent:true,zero_pvc:true,
        no_host_path:true,rustfs_uid_stable:true,baseline_direct_reads:true,
        backup_key_unique:true}}
  ],cell_isolation:{all_cells_proven:true,
    namespaces:[range(1;4) as $failure | [60,180,300][] as $hold |
      ("hiqlite-cell-" + ($failure|tostring) + "-" + ($hold|tostring))]}}
')"
jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$valid_summary" >/dev/null
duplicate_summary="$(jq '.phases[8].cell_id = .phases[0].cell_id' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$duplicate_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a duplicate cell" >&2
  exit 1
fi
missing_hold_summary="$(jq 'del(.phases[8].hold_seconds)' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$missing_hold_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a missing hold duration" >&2
  exit 1
fi
mismatched_hold_summary="$(jq '.phases[0].hold_seconds = 61' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$mismatched_hold_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a mismatched hold duration" >&2
  exit 1
fi
missing_isolation_summary="$(jq 'del(.phases[8].cell_isolation)' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$missing_isolation_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted missing phase isolation proof" >&2
  exit 1
fi
missing_summary_isolation="$(jq 'del(.cell_isolation)' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$missing_summary_isolation" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted missing summary isolation proof" >&2
  exit 1
fi
missing_voter_uid_proof="$(jq 'del(.phases[8].cell_isolation.voter_uids_proven)' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$missing_voter_uid_proof" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted missing voter UID proof" >&2
  exit 1
fi
missing_fresh_vcluster_proof="$(jq 'del(.phases[8].cell_isolation.fresh_vcluster_created)' <<< "$valid_summary")"
if jq -e '.phases[8].cell_isolation.fresh_vcluster_created == true' \
  <<< "$missing_fresh_vcluster_proof" >/dev/null; then
  echo "Hiqlite recovery summary fixture accepted missing fresh-vcluster proof" >&2
  exit 1
fi
reused_release_scope="$(jq '.phases[8].cell_isolation.namespace = .phases[0].cell_isolation.namespace | .phases[8].cell_isolation.release_scope = .phases[0].cell_isolation.release_scope | .cell_isolation.namespaces[8] = .cell_isolation.namespaces[0]' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$reused_release_scope" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a reused cell release scope" >&2
  exit 1
fi

# jq expands this variable; the shell must preserve it literally.
# shellcheck disable=SC2016
namespace_owner_contract='select(.managed == true and .owner == $run_id)'
namespace_owner_fixture='{"managed":true,"owner":"run-fixture"}'
jq -e --arg run_id run-fixture "$namespace_owner_contract" \
  <<< "$namespace_owner_fixture" >/dev/null
if jq -e --arg run_id run-fixture "$namespace_owner_contract" \
  <<< '{"managed":true,"owner":"different-run"}' >/dev/null; then
  echo "Hiqlite recovery namespace contract accepted a foreign owner" >&2
  exit 1
fi

# jq expands this variable; the shell must preserve it literally.
# shellcheck disable=SC2016
identity_contract='(.statefulset.update_revision) as $update_revision |
  (.voters | all(.[]; .controller_revision_hash == $update_revision))'
identity_fixture='{"statefulset":{"update_revision":"rev-new"},"voters":[
  {"name":"hiqlite-recovery-0","controller_revision_hash":"rev-new"},
  {"name":"hiqlite-recovery-1","controller_revision_hash":"rev-new"},
  {"name":"hiqlite-recovery-2","controller_revision_hash":"rev-new"}]}'
jq -e "$identity_contract" <<< "$identity_fixture" >/dev/null
if jq -e "$identity_contract" \
  <<< "$(jq '.voters[2].controller_revision_hash = "rev-old"' <<< "$identity_fixture")" >/dev/null; then
  echo "Hiqlite recovery identity contract accepted a stale voter controller revision" >&2
  exit 1
fi

worktree_fixture_root="$(mktemp -d "${TMPDIR:-/tmp}/hiqlite-worktree-check.XXXXXX")"
trap 'rm -rf -- "$worktree_fixture_root"' EXIT
git -C "$worktree_fixture_root" init -q source
git -C "$worktree_fixture_root/source" config user.name fixture
git -C "$worktree_fixture_root/source" config user.email fixture@example.invalid
touch "$worktree_fixture_root/source/tracked"
git -C "$worktree_fixture_root/source" add tracked
git -C "$worktree_fixture_root/source" commit -qm fixture
git -C "$worktree_fixture_root/source" worktree add -q --detach \
  "$worktree_fixture_root/linked" HEAD
test "$(git -C "$worktree_fixture_root/linked" rev-parse --is-inside-work-tree 2>/dev/null)" = true

# macOS ships Bash 3.2, where expanding an empty array under nounset aborts.
# The guarded form used by stop_port_forwards must remain safe there.
/bin/bash -uc 'direct_port_forward_pids=(); if (( ${#direct_port_forward_pids[@]} > 0 )); then
  for pid in "${direct_port_forward_pids[@]}"; do : "$pid"; done
fi'
/bin/bash -uc 'cell_namespaces=(); seen_backup_keys=();
  if (( ${#cell_namespaces[@]} > 0 )); then for value in "${cell_namespaces[@]}"; do : "$value"; done; fi
  if (( ${#seen_backup_keys[@]} > 0 )); then for value in "${seen_backup_keys[@]}"; do : "$value"; done; fi'

# Fresh-vcluster and proxy provenance are intentionally data contracts: reject
# the neighboring unsafe modes without invoking the live runner.
fresh_contract='(.require_fresh_vcluster == true and .direct_cluster == 0 and
  .reuse_existing == 0 and .cell_count == 1 and .created_cluster == true and
  .image_provenance_verified == true and .image_provenance_publishable == true and
  (.resolved_proxy_image | type) == "string" and (.resolved_proxy_image | length) > 0 and
  (.resolved_proxy_image_id | type) == "string" and (.resolved_proxy_image_id | length) > 0 and
  (.vcluster.context | type) == "string" and (.vcluster.node_uid | type) == "string")'
fresh_fixture='{"require_fresh_vcluster":true,"direct_cluster":0,"reuse_existing":0,"cell_count":1,"created_cluster":true,"image_provenance_verified":true,"image_provenance_publishable":true,"resolved_proxy_image":"sha256:proxy","resolved_proxy_image_id":"sha256:config","vcluster":{"context":"vcluster-docker-fixture","node_uid":"node-uid"}}'
jq -e "$fresh_contract" <<< "$fresh_fixture" >/dev/null
for unsafe in \
  '.direct_cluster = 1' '.reuse_existing = 1' '.cell_count = 2' \
  '.created_cluster = false' '.image_provenance_verified = false' '.image_provenance_publishable = false' 'del(.resolved_proxy_image_id)' 'del(.vcluster.node_uid)'; do
  if jq "$unsafe" <<< "$fresh_fixture" | jq -e "$fresh_contract" >/dev/null; then
    echo "Hiqlite recovery fresh-vcluster contract accepted unsafe fixture: $unsafe" >&2
    exit 1
  fi
done

direct_image_contract='(.direct_cluster == 1 and .verification_mode == "direct-live-tags-only" and
  .image_provenance_verified == false and .image_provenance_publishable == false and
  (.voters | length) == 3 and (.proxy | length) == 1 and
  ([.voters[], .proxy[] | .image_id | type == "string" and test("^sha256:[0-9a-f]{64}$")] | all))'
direct_image_fixture='{"direct_cluster":1,"verification_mode":"direct-live-tags-only","image_provenance_verified":false,"image_provenance_publishable":false,"voters":[{"image_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"image_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"image_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"proxy":[{"image_id":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}'
jq -e "$direct_image_contract" <<< "$direct_image_fixture" >/dev/null
for unsafe in '.image_provenance_verified = true' '.image_provenance_publishable = true' '.proxy[0].image_id = "proxy:tag"'; do
  if jq "$unsafe" <<< "$direct_image_fixture" | jq -e "$direct_image_contract" >/dev/null; then
    echo "Hiqlite direct-cluster image contract accepted unsafe fixture: $unsafe" >&2
    exit 1
  fi
done

# shellcheck disable=SC2016 # jq variables must remain literal.
image_id_contract='def normalized:
  sub("^(docker-pullable|docker|containerd)://"; "") |
  if contains("@") then split("@")[-1] else . end;
  . as $proof |
  (.node_cri.valid == true) and
  ([.expected_node_cri.voter_image_ids[], .expected_node_cri.proxy_image_ids[] |
    test("^sha256:[0-9a-f]{64}$")] | all) and
  (.voters | all(.[]; (.runtime_id | normalized) as $runtime_id |
    ($proof.expected_node_cri.voter_image_ids | index($runtime_id)) != null)) and
  (.proxy | all(.[]; (.runtime_id | normalized) as $runtime_id |
    ($proof.expected_node_cri.proxy_image_ids | index($runtime_id)) != null))'
voter_cri_id='sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
proxy_cri_id='sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
docker_config='sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
docker_index='sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
image_id_fixture="{\"node_cri\":{\"valid\":true},\"local_docker\":{\"voter_config_id\":\"$docker_config\",\"voter_index_or_repo_digest\":\"registry.example/hiqlite@$docker_index\"},\"expected_node_cri\":{\"voter_image_ids\":[\"$voter_cri_id\"],\"proxy_image_ids\":[\"$proxy_cri_id\"]},\"voters\":[{\"runtime_id\":\"containerd://$voter_cri_id\"}],\"proxy\":[{\"runtime_id\":\"docker-pullable://registry.example/hiqlite-proxy@$proxy_cri_id\"}]}"
jq -e "$image_id_contract" <<< "$image_id_fixture" >/dev/null
if jq --arg stale 'sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' \
  '.expected_node_cri.proxy_image_ids = [$stale]' <<< "$image_id_fixture" \
  | jq -e "$image_id_contract" >/dev/null; then
  echo "Hiqlite recovery image-ID contract accepted a stale proxy image" >&2
  exit 1
fi
if jq --arg cross "$voter_cri_id" '.expected_node_cri.proxy_image_ids = [$cross]' <<< "$image_id_fixture" \
  | jq -e "$image_id_contract" >/dev/null; then
  echo "Hiqlite recovery image-ID contract accepted a cross-platform proxy image" >&2
  exit 1
fi
if jq '.expected_node_cri.proxy_image_ids = ["hiqlite-recovery-proxy:tag"]' <<< "$image_id_fixture" \
  | jq -e "$image_id_contract" >/dev/null; then
  echo "Hiqlite recovery image-ID contract accepted a tag-only proxy image" >&2
  exit 1
fi

# The expected runtime ID is selected from exactly one CRI record for the
# canonicalized loaded tag. Docker config and index IDs are not candidates.
# shellcheck disable=SC2016
node_cri_contract='def canonical_ref:
  if test("^[^/]+$") then "docker.io/library/" + .
  elif (split("/")[0] | test("[.:]") or . == "localhost") then .
  else "docker.io/" + . end;
  def records($tag):
    ($tag | canonical_ref) as $canonical_tag |
    [.images[] | select((.repoTags // []) | map(canonical_ref) | index($canonical_tag))];
  def ids($tag): [records($tag)[].id | select(type == "string" and test("^sha256:[0-9a-f]{64}$"))] | unique;
  (records($voter_tag) | length) == 1 and
  (records($proxy_tag) | length) == 1 and
  (ids($voter_tag) == [$voter_cri_id]) and
  (ids($proxy_tag) == [$proxy_cri_id])'
node_runtime_fixture="{\"images\":[{\"id\":\"$voter_cri_id\",\"repoTags\":[\"docker.io/library/hiqlite-recovery:c8316c53799c\"],\"repoDigests\":[]},{\"id\":\"$proxy_cri_id\",\"repoTags\":[\"docker.io/library/hiqlite-recovery-proxy:fixture\"],\"repoDigests\":[]}]}"
jq -e --arg voter_tag hiqlite-recovery:c8316c53799c \
  --arg proxy_tag hiqlite-recovery-proxy:fixture \
  --arg voter_cri_id "$voter_cri_id" --arg proxy_cri_id "$proxy_cri_id" \
  "$node_cri_contract" <<< "$node_runtime_fixture" >/dev/null
if jq --arg duplicate "$voter_cri_id" '.images += [{id:$duplicate,repoTags:["hiqlite-recovery-proxy:fixture"],repoDigests:[]}]' \
  <<< "$node_runtime_fixture" \
  | jq -e --arg voter_tag hiqlite-recovery:c8316c53799c \
      --arg proxy_tag hiqlite-recovery-proxy:fixture \
      --arg voter_cri_id "$voter_cri_id" --arg proxy_cri_id "$proxy_cri_id" \
      "$node_cri_contract" >/dev/null; then
  echo "Hiqlite recovery node CRI contract accepted duplicate tag records" >&2
  exit 1
fi
for mutation in 'del(.images[1])' '.images[0].id = "sha256:not-a-valid-id"'; do
  if jq "$mutation" <<< "$node_runtime_fixture" \
    | jq -e --arg voter_tag hiqlite-recovery:c8316c53799c \
        --arg proxy_tag hiqlite-recovery-proxy:fixture \
        --arg voter_cri_id "$voter_cri_id" --arg proxy_cri_id "$proxy_cri_id" \
        "$node_cri_contract" >/dev/null; then
    echo "Hiqlite recovery node CRI contract accepted missing or malformed CRI ID" >&2
    exit 1
  fi
done

# Local docker-save Config IDs are the only source-bound expectation; CRI IDs
# must be exactly those values before any live Pod identity is accepted.
config_binding_contract='(.pre.voter == .post.voter and .pre.proxy == .post.proxy and
  .cri.voter == .post.voter and .cri.proxy == .post.proxy and
  ([.pre.voter,.pre.proxy,.cri.voter,.cri.proxy] | all(test("^sha256:[0-9a-f]{64}$"))))'
config_binding_fixture="{\"pre\":{\"voter\":\"$voter_cri_id\",\"proxy\":\"$proxy_cri_id\"},\"post\":{\"voter\":\"$voter_cri_id\",\"proxy\":\"$proxy_cri_id\"},\"cri\":{\"voter\":\"$voter_cri_id\",\"proxy\":\"$proxy_cri_id\"}}"
jq -e "$config_binding_contract" <<< "$config_binding_fixture" >/dev/null
for mutation in '.post.voter = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"' \
  '.cri.proxy = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"'; do
  if jq "$mutation" <<< "$config_binding_fixture" | jq -e "$config_binding_contract" >/dev/null; then
    echo "Hiqlite recovery config binding accepted retag or CRI mutation" >&2
    exit 1
  fi
done

image_proof_stage_contract='(.expected | map(.)) == (.proofs | map(.stage)) and
  (.proofs | all(.[]; (.path|type)=="string" and (.sha256|test("^[0-9a-f]{64}$"))))'
image_proof_stage_fixture='{"expected":["pre-fault","post-operator-dr","post-restore-clear"],"proofs":[{"stage":"pre-fault","path":"/tmp/f2-pre.json","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"stage":"post-operator-dr","path":"/tmp/f2-dr.json","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},{"stage":"post-restore-clear","path":"/tmp/f2-clear.json","sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}]}'
jq -e "$image_proof_stage_contract" <<< "$image_proof_stage_fixture" >/dev/null
if jq 'del(.proofs[1])' <<< "$image_proof_stage_fixture" | jq -e "$image_proof_stage_contract" >/dev/null; then
  echo "Hiqlite recovery image proof stages accepted a missing operator-DR proof" >&2
  exit 1
fi

f2_image_stage_contract='if .observed.auto_recovery == true and .observed.operator_dr == false
  then .expected == ["pre-fault","post-recovery"]
  elif .observed.auto_recovery == false and .observed.operator_dr == true
  then .expected == ["pre-fault","post-operator-dr","post-restore-clear"]
  else false end'
jq -e "$f2_image_stage_contract" \
  <<< '{"observed":{"auto_recovery":true,"operator_dr":false},"expected":["pre-fault","post-recovery"]}' >/dev/null
jq -e "$f2_image_stage_contract" \
  <<< '{"observed":{"auto_recovery":false,"operator_dr":true},"expected":["pre-fault","post-operator-dr","post-restore-clear"]}' >/dev/null
for invalid in \
  '{"observed":{"auto_recovery":true,"operator_dr":true},"expected":["pre-fault","post-recovery"]}' \
  '{"observed":{"auto_recovery":false,"operator_dr":false},"expected":["pre-fault","post-recovery"]}'; do
  if jq -e "$f2_image_stage_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery F2 stage contract accepted non-exclusive recovery flags" >&2
    exit 1
  fi
done

transition_ledger_contract='all(.[]; .acknowledged == true and (.id|type)=="string" and (.id|length)>0 and (.value|type)=="string" and (.value|length)>0) and
  (map(.id) | unique | length) == length'
jq -e "$transition_ledger_contract" <<< '[{"id":"f2-transition-1","value":"ack","acknowledged":true}]' >/dev/null
jq -e "$transition_ledger_contract" <<< '[]' >/dev/null
for invalid in '[{"id":"f2-transition-1","value":"ack","acknowledged":false}]' '[{"id":"f2-transition-1","value":"ack","acknowledged":true},{"id":"f2-transition-1","value":"other","acknowledged":true}]'; do
  if jq -e "$transition_ledger_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery transition ledger accepted false or duplicate acknowledgement" >&2
    exit 1
  fi
done
ledger_count_contract='.declared_count == (.records | length)'
jq -e "$ledger_count_contract" <<< '{"declared_count":0,"records":[]}' >/dev/null
if jq -e "$ledger_count_contract" <<< '{"declared_count":1,"records":[]}' >/dev/null; then
  echo "Hiqlite recovery transition ledger accepted a count mismatch" >&2
  exit 1
fi
empty_ledger_phase_contract='(.transition_ledger.path | type) == "string" and
  (.transition_ledger.path | length) > 0 and
  (.transition_ledger.sha256 | test("^[0-9a-f]{64}$")) and
  .transition_ledger.count == 0'
jq -e "$empty_ledger_phase_contract" \
  <<< '{"transition_ledger":{"path":"/tmp/f1-transition-ledger.jsonl","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","count":0}}' >/dev/null

fresh_baseline_contract='length == 3 and all(.[]; .state == "absent-table")'
jq -e "$fresh_baseline_contract" <<< '[{"state":"absent-table"},{"state":"absent-table"},{"state":"absent-table"}]' >/dev/null
post_reset_baseline_contract='length == 3 and all(.[]; .state == "empty")'
jq -e "$post_reset_baseline_contract" <<< '[{"state":"empty"},{"state":"empty"},{"state":"empty"}]' >/dev/null
for invalid in '[{"state":"absent-table"},{"state":"empty"}]' '[{"state":"empty"},{"state":"empty"},{"state":"empty"}]' '[{"state":"foreign-sentinel"},{"state":"foreign-sentinel"},{"state":"foreign-sentinel"}]' '[{"state":"query-error"},{"state":"query-error"},{"state":"query-error"}]' '[{"state":"malformed-json"},{"state":"malformed-json"},{"state":"malformed-json"}]'; do
  if jq -e "$fresh_baseline_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery fresh baseline contract accepted foreign, partial, or query failure state" >&2
    exit 1
  fi
done
reset_ack_contract='.rc == 0 and .acknowledged == true and (.raw | type) == "string"'
jq -e "$reset_ack_contract" <<< '{"rc":0,"acknowledged":true,"raw":"{\"acknowledged\":true}"}' >/dev/null
for invalid in '{"rc":0,"acknowledged":false,"raw":"{}"}' '{"rc":0,"raw":"{}"}'; do
  if jq -e "$reset_ack_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery reset contract accepted an unacknowledged reset" >&2
    exit 1
  fi
done
post_reset_response_contract='if .found == false then "empty" elif .found == true then "retained" else "malformed" end'
test "$(jq -r "$post_reset_response_contract" <<< '{"found":false}')" = empty
test "$(jq -r "$post_reset_response_contract" <<< '{"found":true}')" = retained
for invalid in 'not-json' '{}' '{"found":"false"}'; do
  classification="$(jq -r "$post_reset_response_contract" <<< "$invalid" 2>/dev/null || printf malformed)"
  test "$classification" = malformed || {
    echo "Hiqlite recovery post-reset contract accepted malformed successful JSON" >&2
    exit 1
  }
done
port_forward_retry_contract='(.attempts | length) == 2 and .attempts[0].status != 0 and
  .attempts[1].status == 0 and .ready_before_success == true and .stale_pid_reused == false'
jq -e "$port_forward_retry_contract" \
  <<< '{"attempts":[{"status":1},{"status":0}],"ready_before_success":true,"stale_pid_reused":false}' >/dev/null
for invalid in \
  '{"attempts":[{"status":1},{"status":1}],"ready_before_success":false,"stale_pid_reused":false}' \
  '{"attempts":[{"status":1},{"status":0}],"ready_before_success":true,"stale_pid_reused":true}'; do
  if jq -e "$port_forward_retry_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery port-forward retry contract accepted persistent failure or stale PID reuse" >&2
    exit 1
  fi
done

# A mismatch must remain a complete, parseable artifact. This models the
# runner's temp-write then rename boundary without contacting a cluster.
image_proof_fixture_root="$(mktemp -d "${TMPDIR:-/tmp}/hiqlite-image-proof.XXXXXX")"
trap 'rm -rf -- "$worktree_fixture_root" "$image_proof_fixture_root"' EXIT
image_proof="$image_proof_fixture_root/live-image-ids.json"
image_proof_tmp="$image_proof.tmp"
jq -n --arg voter "$voter_cri_id" --arg proxy "$proxy_cri_id" \
  --arg raw 'not-json' \
  '{valid:false,mismatch_reason:"voter runtime image ID differs from node CRI image ID",
    expected_node_cri:{voter_image_ids:[$voter],proxy_image_ids:[$proxy]},
    raw:{voters:$raw},voters:[],proxy:[]}' > "$image_proof_tmp"
mv "$image_proof_tmp" "$image_proof"
jq -e '.valid == false and (.mismatch_reason | length) > 0 and
  (.expected_node_cri.voter_image_ids | length) == 1 and .raw.voters == "not-json"' \
  "$image_proof" >/dev/null

# A no-quorum ACK must leave a self-contained immutable post-state object.
post_ack_contract='(.valid == true and (.cell_id|type)=="string" and (.cell_id|length)>0 and (.id|type)=="string" and (.id|length)>0 and (.value|type)=="string" and (.value|length)>0 and
  (.order|type)=="number" and (.captured_epoch|type)=="number" and
  (.classification == "unilateral_state_machine_apply" or .classification == "ack_without_local_apply" or .classification == "ack_post_state_unknown") and
  (.sequence | map(.kind) | sort) == ["direct_consistent","direct_local","direct_metrics","endpoints","pods","proxy_current_logs","proxy_describe","proxy_ping","proxy_previous_logs","voter0_current_logs"] and
  (.sequence | all(.[]; (.order|type)=="number" and (.rc|type)=="number" and (.raw|type)=="string" and (.started_epoch|type)=="number" and (.ended_epoch|type)=="number" and .ended_epoch >= .started_epoch)))'
post_ack_fixture='{"valid":true,"cell_id":"f2-h180","id":"f2-h180-transition-run","value":"transition-ack","order":1,"captured_epoch":1,"classification":"unilateral_state_machine_apply","sequence":[{"kind":"pods","rc":0,"raw":"{}","order":1,"started_epoch":1,"ended_epoch":1},{"kind":"endpoints","rc":0,"raw":"{}","order":2,"started_epoch":1,"ended_epoch":1},{"kind":"direct_metrics","rc":0,"raw":"{}","order":3,"started_epoch":1,"ended_epoch":1},{"kind":"direct_consistent","rc":1,"raw":"no quorum","order":4,"started_epoch":1,"ended_epoch":1},{"kind":"direct_local","rc":0,"raw":"{\\\"found\\\":true}","order":5,"started_epoch":1,"ended_epoch":1},{"kind":"proxy_ping","rc":0,"raw":"ok","order":6,"started_epoch":1,"ended_epoch":1},{"kind":"voter0_current_logs","rc":0,"raw":"","order":7,"started_epoch":1,"ended_epoch":1},{"kind":"proxy_current_logs","rc":0,"raw":"","order":8,"started_epoch":1,"ended_epoch":1},{"kind":"proxy_previous_logs","rc":1,"raw":"","order":9,"started_epoch":1,"ended_epoch":1},{"kind":"proxy_describe","rc":0,"raw":"","order":10,"started_epoch":1,"ended_epoch":1}]}'
jq -e "$post_ack_contract" <<< "$post_ack_fixture" >/dev/null
post_ack_classification_contract='(.classification == "unilateral_state_machine_apply" and .local.rc == 0 and .local.response.found == true and .local.response.id == .id and .local.response.value == .value) or
  (.classification == "ack_without_local_apply" and .local.rc == 0 and .local.response.found == false) or
  (.classification == "ack_post_state_unknown" and (.local.rc != 0 or (.local.response | type) != "object" or ((.local.response.found == false) | not) and ((.local.response.found == true and .local.response.id == .id and .local.response.value == .value) | not)))'
jq -e "$post_ack_classification_contract" \
  <<< '{"id":"f2-transition","value":"v","classification":"unilateral_state_machine_apply","local":{"rc":0,"response":{"found":true,"id":"f2-transition","value":"v"}}}' >/dev/null
jq -e "$post_ack_classification_contract" \
  <<< '{"id":"f2-transition","value":"v","classification":"ack_without_local_apply","local":{"rc":0,"response":{"found":false}}}' >/dev/null
jq -e "$post_ack_classification_contract" \
  <<< '{"id":"f2-transition","value":"v","classification":"ack_post_state_unknown","local":{"rc":1,"response":null}}' >/dev/null
for invalid in \
  '{"id":"f2-transition","value":"v","classification":"unilateral_state_machine_apply","local":{"rc":0,"response":{"found":false}}}' \
  '{"id":"f2-transition","value":"v","classification":"ack_without_local_apply","local":{"rc":0,"response":{"found":true,"id":"f2-transition","value":"v"}}}'; do
  if jq -e "$post_ack_classification_contract" <<< "$invalid" >/dev/null; then
    echo "Hiqlite recovery post-ACK classification accepted mismatched local state" >&2
    exit 1
  fi
done
for invalid in 'del(.sequence[0])' '.id=""' '.classification="unknown"'; do
  if jq "$invalid" <<< "$post_ack_fixture" | jq -e "$post_ack_contract" >/dev/null; then
    echo "Hiqlite recovery post-ACK contract accepted missing or malformed evidence" >&2
    exit 1
  fi
done
post_ack_binding_contract='(.proof.post_ack.path == .evidence.path and .proof.post_ack.sha256 == .evidence.sha256 and
  .proof.post_ack.classification == .evidence.classification and .evidence.valid == true and
  .proof.cell_id == .evidence.cell_id and .proof.write.id == .evidence.id and .proof.write.value == .evidence.value and
  .evidence.captured_epoch >= .proof.write.ended_epoch)'
post_ack_binding_fixture='{"proof":{"cell_id":"f2-h180","write":{"id":"f2-h180-transition-run","value":"transition-ack","ended_epoch":10},"post_ack":{"path":"/tmp/post.json","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","classification":"unilateral_state_machine_apply"}},"evidence":{"valid":true,"cell_id":"f2-h180","id":"f2-h180-transition-run","value":"transition-ack","captured_epoch":11,"path":"/tmp/post.json","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","classification":"unilateral_state_machine_apply"}}'
jq -e "$post_ack_binding_contract" <<< "$post_ack_binding_fixture" >/dev/null
for invalid in '.evidence.path="/tmp/other.json"' '.evidence.id="foreign"' '.evidence.captured_epoch=9'; do
  if jq "$invalid" <<< "$post_ack_binding_fixture" | jq -e "$post_ack_binding_contract" >/dev/null; then
    echo "Hiqlite recovery post-ACK binding accepted missing, mismatched, or stale evidence" >&2
    exit 1
  fi
done

# Ledger/event publication is downstream of a fully bound, atomically moved
# ACK proof. Exercise each failure boundary without invoking a cluster.
ack_gate="$image_proof_fixture_root/ack-gate"
mkdir -p "$ack_gate"
for failure in jq update mv binding; do
  proof_tmp="$ack_gate/$failure.proof.tmp"
  ledger="$ack_gate/$failure.ledger"
  event="$ack_gate/$failure.event"
  case "$failure" in
    jq) if false; then : > "$ledger"; : > "$event"; fi ;;
    update) if jq -n 'error("update")' > "$proof_tmp" 2>/dev/null; then : > "$ledger"; : > "$event"; fi ;;
    mv) if jq -n '{valid:true}' > "$proof_tmp" && false; then : > "$ledger"; : > "$event"; fi ;;
    binding) if jq -n '{valid:true}' > "$proof_tmp" && jq -e '.valid == false' "$proof_tmp" >/dev/null; then : > "$ledger"; : > "$event"; fi ;;
  esac
  test ! -e "$ledger" && test ! -e "$event" || {
    echo "Hiqlite recovery ACK failure fixture published a ledger/event after $failure failure" >&2
    exit 1
  }
done

# Raw diagnostics may exceed exec(2) argv limits. --rawfile must still produce
# a complete JSON proof; the bounded descriptor is the evidence-safe fallback.
arg_max="$(getconf ARG_MAX 2>/dev/null || printf 262144)"
large_raw="$image_proof_fixture_root/post-ack-large.log"
large_count=$((arg_max / 1024 + 2))
dd if=/dev/zero of="$large_raw" bs=1024 count="$large_count" status=none
large_proof="$image_proof_fixture_root/post-ack-large.json"
jq -n --rawfile raw "$large_raw" \
  '{valid:true,cell_id:"f2-h180",classification:"ack_post_state_unknown",sequence:[{kind:"proxy_current_logs",raw:$raw}]}' > "$large_proof"
jq -e --argjson minimum "$arg_max" '.valid == true and .cell_id == "f2-h180" and .classification == "ack_post_state_unknown" and (.sequence[0].raw | length) > $minimum' "$large_proof" >/dev/null
fallback_fixture='{"valid":true,"embedded_raw":false,"cell_id":"f2-h180","id":"f2-transition","value":"transition-ack","classification":"ack_post_state_unknown","sequence":[{"kind":"direct_local","path":"/tmp/direct-local.raw","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","rc":0}]}'
jq -e '.valid == true and .embedded_raw == false and .classification == "ack_post_state_unknown" and all(.sequence[]; (.path|type)=="string" and (.sha256|test("^[0-9a-f]{64}$")) and (.raw|not))' <<< "$fallback_fixture" >/dev/null

lock_fixture="$image_proof_fixture_root/Cargo.lock"
printf '%s\n' '[package]' 'name = "openraft"' 'version = "0.9.25"' > "$lock_fixture"
openraft_fixture_version="$(awk '$0 == "name = \"openraft\"" { in_package=1; next } in_package && /^version = / { value=$0; sub(/^version = "/, "", value); sub(/"$/, "", value); print value; count++; in_package=0 } END { if (count != 1) exit 1 }' "$lock_fixture")"
test "$openraft_fixture_version" = 0.9.25
printf '%s\n' '[package]' 'name = "openraft"' 'version = "0.9.25"' >> "$lock_fixture"
if awk '$0 == "name = \"openraft\"" { in_package=1; next } in_package && /^version = / { count++; in_package=0 } END { exit count == 1 ? 0 : 1 }' "$lock_fixture"; then
  echo "Hiqlite recovery OpenRaft provenance accepted multiple lock packages" >&2
  exit 1
fi

echo "Hiqlite recovery static contract passed"
