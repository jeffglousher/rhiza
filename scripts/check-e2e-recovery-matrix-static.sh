#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script="$repo_root/scripts/e2e-vind-rustfs.sh"

require_literal() {
  literal="$1"
  grep -Fq -- "$literal" "$script" || {
    echo "missing recovery-matrix contract: $literal" >&2
    exit 1
  }
}

require_literal_count() {
  literal="$1"
  expected="$2"
  actual="$(grep -Fc -- "$literal" "$script")"
  [ "$actual" -eq "$expected" ] || {
    echo "recovery-matrix contract count for '$literal': expected $expected, got $actual" >&2
    exit 1
  }
}

bash -n "$script"

# vcluster may create image.tar.gz in its current directory. Keep that generated
# artifact under the run target so source-freeze checks never see repository drift.
# shellcheck disable=SC2016
require_literal '(cd "$target" && vcluster node load-image "$node" --image "$image")'
[ "$(grep -Fc 'vcluster node load-image' "$script")" -eq 1 ] || {
  echo "every Rhiza recovery image load must run inside the target directory" >&2
  exit 1
}

marker_fixture="$(mktemp)"
trap 'rm -f "$marker_fixture"' EXIT
{
  yq eval -n '{"apiVersion":"v1","kind":"Service","metadata":{"name":"before"}}'
  printf '%s\n' '---'
  yq eval -n '{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"test"},
    "spec":{"template":{"spec":{"containers":[{"name":"rhiza"}]}}}}'
  printf '%s\n' '---'
  yq eval -n '{"apiVersion":"v1","kind":"Service","metadata":{"name":"after"}}'
} > "$marker_fixture"
MARKER_HELPER_IMAGE=busybox:1.36.1 yq eval --inplace '
  with(select(.kind == "StatefulSet");
    .spec.template.spec.containers += [{"name":"e2e-marker", "image":strenv(MARKER_HELPER_IMAGE)}]
  )
' "$marker_fixture"
[ "$(yq eval-all -o=json '[select(.kind == "Service") | .metadata.name]' "$marker_fixture" | jq -c .)" = '["before","after"]' ] || {
  echo "marker helper mutation must preserve Service documents" >&2
  exit 1
}
[ "$(yq eval -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "e2e-marker") | .name' "$marker_fixture" | grep -cx e2e-marker)" = 1 ] || {
  echo "marker helper mutation must add exactly one helper" >&2
  exit 1
}

require_literal 'marker_helper_container=e2e-marker'
# shellcheck disable=SC2016
require_literal 'marker_helper_image="${RHIZA_MARKER_HELPER_IMAGE:-busybox:1.36.1}"'
require_literal 'RHIZA_MARKER_HELPER_IMAGE must not be empty'
require_literal 'inject_marker_helper() {'
require_literal 'with(select(.kind == "StatefulSet");'
require_literal '"name":"e2e-marker", "image":strenv(MARKER_HELPER_IMAGE)'
require_literal '"resources":{"requests":{"cpu":"1m", "memory":"8Mi"}'
require_literal '"volumeMounts":[{"name":"data", "mountPath":"/var/lib/rhiza"}]'
# shellcheck disable=SC2016
require_literal 'inject_marker_helper "$target/config-c1.yaml"'
# shellcheck disable=SC2016
require_literal 'inject_marker_helper "$target/reconfigure/config-c2.yaml"'
require_literal 'marker_seed() {'
require_literal 'marker_present() {'
require_literal 'marker_absent() {'
require_literal 'verify_marker_helper() {'
require_literal 'marker helper is absent from StatefulSet template'
require_literal 'marker helper is absent from Pod'
# shellcheck disable=SC2016
require_literal 'k exec -c "$marker_helper_container" "$pod" --'
# shellcheck disable=SC2016
if grep -E 'k exec .*-- (/(bin/)?sh|test)' "$script" |
  grep -Fv -- '-c "$marker_helper_container"' >/dev/null; then
  echo "marker operations must target the explicit e2e-marker helper container" >&2
  exit 1
fi
# shellcheck disable=SC2016
require_literal 'k delete pod "${name_c2}-$ordinal" --wait=true >/dev/null'
# shellcheck disable=SC2016
require_literal 'verify_marker_helper "$name_c2"'
require_literal '.spec.template.metadata.labels["rhiza.dev/member-role"] = "voter"'

require_literal 'RHIZA_E2E_RECOVERY_MATRIX:-0'
require_literal 'RHIZA_E2E_RECOVERY_MATRIX_ONLY:-0'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER:-0'
require_literal 'RHIZA_RECOVERY_FORBIDDEN_SENTINEL:-'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_E2E_RECOVERY_MATRIX=1'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_E2E_RECOVERY_MATRIX_ONLY=1'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_DIRECT_CLUSTER=0'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_REUSE_EXISTING=0'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires exactly one failure cell'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires exactly one hold cell'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_RECOVERY_FORBIDDEN_SENTINEL'
require_literal 'fresh_assert_prebootstrap_absence() {'
require_literal 'fresh_verify_forbidden_sentinel() {'
require_literal 'fresh_capture_cell_isolation() {'
require_literal 'fresh isolation refuses an existing vcluster'
require_literal 'fresh isolation refuses an existing namespace'
require_literal 'fresh_assert_prebootstrap_absence'
require_literal 'fresh_capture_cell_isolation'
require_literal 'fresh isolation requires zero PVCs before bootstrap'
require_literal 'fresh isolation requires zero hostPath volumes before bootstrap'
require_literal 'fresh isolation observed preexisting Rhiza StatefulSet state'
require_literal 'fresh isolation observed preexisting Rhiza Pod state'
require_literal 'fresh isolation forbids restore environment input'
require_literal 'fresh isolation requires exact three voter Pod UIDs'
require_literal 'fresh isolation config-1 membership did not converge'
require_literal 'fresh isolation forbidden sentinel exists on voter'
require_literal 'fresh isolation bootstrap sentinel missing on voter'
require_literal "mode:\$mode,process_generation_new:true"
require_literal 'storage_generation_new:true'
require_literal 'process_generation_proof:'
require_literal 'storage_generation_proof:'
require_literal 'restore_env:"absent",restore_env_absent:true'
require_literal 'prior_sentinel_absent:true'
require_literal 'exact_membership:true,object_provenance_current:true'
require_literal 'object_provenance_proof:'
require_literal "identity_artifact_path:\$identity_artifact_path"
require_literal 'prebootstrap_qlog_materializer_state_absent:true'
require_literal "current_run_sentinel:{key:\$sentinel_key,value:\$sentinel_value}"
require_literal "--argjson cell_isolation \"\$fresh_cell_isolation\""
require_literal "cell_isolation:\$cell_isolation"
if grep -Fq 'cleanup_verified' "$script"; then
  echo "runner must not claim cleanup verification before its EXIT trap" >&2
  exit 1
fi
require_literal 'RHIZA_RECOVERY_HOLD_SECONDS:-60,180,300'
require_literal 'RHIZA_RECOVERY_FAIL_PEERS:-1,2,3'
require_literal 'RHIZA_RECOVERY_AUTO_TIMEOUT_SECONDS:-30'
require_literal 'RHIZA_RECOVERY_AUTO_TIMEOUT_SECONDS must be positive'
require_literal 'RHIZA_RECOVERY_F1_PROBE_INTERVAL_SECONDS:-10'
require_literal 'RHIZA_VIND_DIRECT_CLUSTER:-0'
require_literal 'RHIZA_VIND_SKIP_IMAGE_LOAD:-0'
require_literal 'RHIZA_VIND_DIRECT_CLUSTER=1 requires RHIZA_VIND_CONTEXT'
require_literal 'rhiza.dev/e2e-run-id'
require_literal 'recovery-matrix.jsonl'
require_literal 'rhiza_commit'
require_literal 'rhiza_dirty'
require_literal 'resolved_image'
require_literal 'service_rto_seconds'
require_literal 'full_rto_seconds'
require_literal 'failure_injected_at'
require_literal 'all_target_pods_deleted_at'
require_literal 'quorum_lost_at'
require_literal 'failure_released_at'
require_literal 'ack_ledger'
require_literal 'old_pod_uids'
require_literal 'new_pod_uids'
require_literal 'ack_sentinel_preserved'
require_literal 'markers_lost'
require_literal 'pvc_count'
require_literal 'failure_write_expected'
require_literal 'failure_write_actual_detail'
# shellcheck disable=SC2016
require_literal 'cell_write_actual_detail="$(matrix_last_http_failure_detail)"'
require_literal 'failure_read_barrier_expected'
require_literal 'survivor_local_read'
require_literal 'tip_hashes_equal'
require_literal 'recovery_deadline_exceeded'
require_literal 'matrix_run_no_quorum_safety_probe() {'
require_literal 'RHIZA_RECOVERY_NO_QUORUM_PROBE_MAX_LATENESS_SECONDS:-5'
require_literal 'RHIZA_RECOVERY_NO_QUORUM_PROBE_MAX_LATENESS_SECONDS must be positive'
require_literal "cell_failure_probe_expected_count=\$(((cell_hold - 1) / failure_probe_interval_seconds + 1))"
require_literal 'no_quorum_safety_probe_late'
require_literal 'no_quorum_safety_probe_count_mismatch'
require_literal "requested_at:\$requested_at,actual_started_at:\$actual_started_at,actual_finished_at:\$actual_finished_at"
require_literal "failure_probe_expected_count:\$failure_probe_expected_count"
require_literal "failure_probe_actual_count:\$failure_probe_actual_count"
require_literal "failure_probe_max_lateness_seconds:\$failure_probe_max_lateness_seconds"
require_literal "failure_probe_lateness_bound_seconds:\$failure_probe_lateness_bound_seconds"
require_literal "failure_probe_cadence_seconds:\$failure_probe_cadence_seconds"
require_literal "failure-safety-\${cell_id}-\${sequence}-\${run_id}"
require_literal 'Preserve the original no-quorum receipt'
require_literal "request_id=\"\$fault_request_id\""
require_literal 'matrix_persist_safety_observation() {'
require_literal 'matrix_persist_local_safety_observation() {'
require_literal 'matrix_last_http_original_rc'
require_literal "failure-safety-probes/\${cell_id}"
require_literal 'no_quorum_safety_probe_failed'
require_literal 'cell_failure_safety_probes'
require_literal "failure_safety_probes:\$failure_safety_probes"
require_literal 'survivor-local-read.stdout'
require_literal 'survivor-local-read.stderr'
require_literal 'matrix_expect_write_no_quorum'
require_literal 'matrix_expect_read_quorum_unavailable'
require_literal 'matrix_expect_zero_endpoint_transport_failure'
require_literal 'fresh_capture_empty_bucket_inventory() {'
require_literal 'fresh_capture_live_image_provenance() {'
require_literal 'normalize_image_id() {'
require_literal "value=\"\${value#containerd://}\""
require_literal "value=\"\${value#docker-pullable://}\""
require_literal 'fresh isolation live voter image IDs do not match built Docker image ID'
require_literal 'fresh isolation RustFS bucket is not empty before bootstrap'
require_literal "node_uid:\$node_uid,rustfs_uid:\$rustfs_uid"
require_literal "image_provenance_verified:true,bucket_inventory_path:\$bucket_inventory_path"
require_literal "expected_image_ids:\$expected_image_ids,matched_image_id:\$matched_image_id,live_rhiza_image_ids:\$live_rhiza_image_ids"
require_literal 's3api list-objects-v2 --bucket rhiza --output json'

# A one-shot F2/F3 sample cannot detect a later spontaneous quorum. This
# deterministic fixture injects success only at the middle sample and proves
# the probe policy rejects it rather than converting it into an expected error.
mid_hold_probe_fixture() {
  local fixture_result
  for fixture_result in retryable_failure success retryable_failure; do
    case "$fixture_result" in
      retryable_failure) ;;
      success) return 1 ;;
      *) return 2 ;;
    esac
  done
}
if mid_hold_probe_fixture; then
  echo "mid-hold success fixture was not rejected" >&2
  exit 1
fi
# The absolute schedule must reject a serial probe that completes after the
# configured lateness bound instead of skipping the missed middle sample.
slow_probe_requested=10
slow_probe_actual=16
slow_probe_lateness_bound=5
slow_probe_lateness=$((slow_probe_actual - slow_probe_requested))
if [ "$slow_probe_lateness" -le "$slow_probe_lateness_bound" ]; then
  echo "slow-probe lateness fixture was not rejected" >&2
  exit 1
fi
min_count_hold=25
min_count_interval=10
min_count_expected=$(((min_count_hold - 1) / min_count_interval + 1))
[ "$min_count_expected" = 3 ] || {
  echo "minimum probe-count fixture drifted" >&2
  exit 1
}
# Image runtimes encode the same digest differently. These fixtures lock the
# accepted normal forms without accepting a different digest.
portable_image_normalize() {
  local value="$1"
  value="${value#containerd://}"
  value="${value#docker://}"
  value="${value#docker-pullable://}"
  case "$value" in *@sha256:*) value="sha256:${value##*@sha256:}";; esac
  printf '%s\n' "$value"
}
portable_digest=sha256:0123456789abcdef
[ "$(portable_image_normalize "containerd://$portable_digest")" = "$portable_digest" ] || exit 1
[ "$(portable_image_normalize "docker-pullable://example/rhiza@$portable_digest")" = "$portable_digest" ] || exit 1
[ "$(portable_image_normalize "docker://$portable_digest")" = "$portable_digest" ] || exit 1
if [ "$(portable_image_normalize 'containerd://sha256:different')" = "$portable_digest" ]; then
  echo "image identity fixture accepted a different digest" >&2
  exit 1
fi
[ "$(grep -Fc "matrix_run_no_quorum_safety_probe \"\$probe_sequence\"" "$script")" -eq 1 ] || {
  echo "F2/F3 safety probe must run for every scheduled hold sample" >&2
  exit 1
}
require_literal 'matrix_expect_write_no_quorum'
require_literal '(.code == "write_timeout" or .code == "write_outcome_unknown" or .code == "unavailable")'
require_literal 'write_retry_deadline_seconds=60'
# The post-restore probe may encounter a short-lived quorum convergence window.
# It must retry only known retryable HTTP errors, with the original request ID.
require_literal 'retryable_write_failure'
require_literal 'HTTP 503 Service Unavailable code=(write_timeout|write_outcome_unknown|unavailable|writes_unavailable)'
retryable_write_pattern='^write failed: HTTP 503 Service Unavailable code=(write_timeout|write_outcome_unknown|unavailable|writes_unavailable)( |$)'
for retryable_code in write_timeout write_outcome_unknown unavailable writes_unavailable; do
  printf 'write failed: HTTP 503 Service Unavailable code=%s retryable=true\n' "$retryable_code" |
    grep -Eq "$retryable_write_pattern" || {
      echo "retryable write classifier rejected $retryable_code" >&2
      exit 1
    }
done
for non_retryable_code in write_unavailable internal_error unavailable_extra; do
  if printf 'write failed: HTTP 503 Service Unavailable code=%s retryable=true\n' "$non_retryable_code" |
    grep -Eq "$retryable_write_pattern"; then
    echo "retryable write classifier broadened to $non_retryable_code" >&2
    exit 1
  fi
done
# shellcheck disable=SC2016
require_literal 'for ((attempt=1; attempt<=60; attempt++)); do'
# shellcheck disable=SC2016
require_literal 'client "$pod" write --request-id "$request_id" --key "$key" --value "$value" 2> "$attempt_log"'
# shellcheck disable=SC2016
require_literal 'retryable_write_failure "$attempt_log"'
# shellcheck disable=SC2016
require_literal 'cat "$attempt_log" >&2'
require_literal 'matrix_expect_read_barrier_unavailable'
require_literal 'matrix_expect_f2_read_barrier_timeout'
require_literal 'failure_read_barrier_actual_detail'
require_literal 'read_no_quorum_latency_defect'
require_literal 'survivor_ready" = True'
require_literal 'endpoint_count" = 1'
# shellcheck disable=SC2016
require_literal 'case "$exit_code" in 28)'
require_literal 'Operation timed out after [0-9]+ milliseconds with 0 bytes received'
# shellcheck disable=SC2016
require_literal '[ "$matrix_last_http_status" = 503 ]'
require_literal '.code == "unavailable" and .retryable == true'
# shellcheck disable=SC2016
require_literal 'matrix_http_target="${name_c1}-0.${name_c1}"'
require_literal 'matrix_expect_zero_endpoint_transport_failure'
require_literal 'endpoint_count" = 0'
# shellcheck disable=SC2016
require_literal 'case "$exit_code" in 7|28)'
require_literal 'idempotency_boundary_verified'
require_literal '.node.active_config_id'
require_literal 'matrix_run_f1_availability_probe'
# shellcheck disable=SC2016
require_literal 'failure_probe_interval_seconds="$recovery_f1_probe_interval"'
# shellcheck disable=SC2016
require_literal 'failure_probe_interval_seconds:$failure_probe_interval_seconds'
# shellcheck disable=SC2016
require_literal '--argjson auto_recovery_timeout_seconds "$recovery_auto_timeout"'
# shellcheck disable=SC2016
require_literal 'auto_recovery_timeout_seconds:$auto_recovery_timeout_seconds'
# Both cell and summary records must describe the configured recovery deadline.
# shellcheck disable=SC2016
require_literal_count '--argjson auto_recovery_timeout_seconds "$recovery_auto_timeout"' 2
# shellcheck disable=SC2016
require_literal_count 'auto_recovery_timeout_seconds:$auto_recovery_timeout_seconds' 2
require_literal 'matrix_emit_summary'
require_literal 'same_pod_restart_covered:false'
require_literal 'arbitrary_leader_failure_covered:false'
# shellcheck disable=SC2016
require_literal 'k scale statefulset "$name_c1" --replicas="$cell_survivors"'
# shellcheck disable=SC2016
require_literal 'k scale statefulset "$name_c1" --replicas=3'
# shellcheck disable=SC2016
require_literal '"$BASH" scripts/wait-k8s-statefulset-ready.sh'

wait_script="$repo_root/scripts/wait-k8s-statefulset-ready.sh"
# shellcheck disable=SC2016
grep -Fq 'resource_json statefulset "$name" | jq' "$wait_script" || {
  echo "readiness check must stream StatefulSet JSON into jq" >&2
  exit 1
}
# shellcheck disable=SC2016
if grep -Fq '<<< "$statefulset_json"' "$wait_script"; then
  echo "readiness check must not use a potentially blocking StatefulSet here-string" >&2
  exit 1
fi

echo "e2e recovery matrix static contract passed"
