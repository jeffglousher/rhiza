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

require_literal 'umask 077'
umask_line="$(grep -n '^umask 077$' "$script" | cut -d: -f1)"
target_line="$(grep -n 'mkdir -p "\$target"' "$script" | head -1 | cut -d: -f1)"
build_line="$(grep -n 'docker build --load --build-arg' "$script" | head -1 | cut -d: -f1)"
disk_guard_line="$(grep -n '^require_one_gib_free "\$target"$' "$script" | cut -d: -f1)"
[ -n "$umask_line" ] && [ -n "$target_line" ] && [ "$umask_line" -lt "$target_line" ] || {
  echo "recovery artifacts must set umask 077 before creating the target" >&2
  exit 1
}
require_literal 'require_one_gib_free() {'
require_literal 'local target_root="${1-}" candidate parent available_kib'
require_literal '*) candidate="$repo_root/$target_root" ;'
require_literal '[ ! -L "$candidate" ] || die "target filesystem ancestor must not be a symlink"'
require_literal 'df -Pk "$candidate" | awk '
require_literal '[ "$available_kib" -ge 1048576 ]'
[ -n "$disk_guard_line" ] && [ -n "$build_line" ] &&
  [ "$disk_guard_line" -lt "$target_line" ] && [ "$disk_guard_line" -lt "$build_line" ] || {
  echo "recovery disk guard must run before target creation and Docker build" >&2
  exit 1
}
df_fixture="$(mktemp -d)"
trap 'rm -rf "$df_fixture"' EXIT
printf '%s\n' '#!/usr/bin/env sh' 'printf "%s\\n" "$2" > "$DF_CALL_PATH"' 'case "$DF_FIXTURE" in' \
  '  full) printf "%s\\n" "Filesystem 1024-blocks Used Available Capacity Mounted on" "/dev/mock 2000000 100000 1048576 9% /";;' \
  '  below) printf "%s\\n" "Filesystem 1024-blocks Used Available Capacity Mounted on" "/dev/mock 2000000 100000 1048575 9% /";;' \
  '  malformed) printf "%s\\n" "Filesystem 1024-blocks Used Available Capacity Mounted on" "/dev/mock 2000000 100000 unknown 9% /";;' \
  '  *) exit 2;;' 'esac' > "$df_fixture/df"
chmod 700 "$df_fixture/df"
mkdir -p "$df_fixture/repo" "$df_fixture/absolute" "$df_fixture/nested"
eval "$(awk '
  /^require_one_gib_free\(\) \{/ { capture = 1 }
  capture { print }
  capture && /^}$/ { exit }
' "$script")"
run_disk_guard_fixture() {
  local mode="$1" target_value="$2" root_value="$3" expected_path="$4" call_path
  call_path="$df_fixture/call-$mode-$$"
  (
    die() { exit 1; }
    repo_root="$root_value"
    export DF_FIXTURE="$mode" DF_CALL_PATH="$call_path"
    PATH="$df_fixture:$PATH"
    require_one_gib_free "$target_value"
  ) || return 1
  [ "$(cat "$call_path")" = "$expected_path" ]
}
run_disk_guard_fixture full 'target/rhiza-e2e/sql/run' "$df_fixture/repo" "$df_fixture/repo" &&
  run_disk_guard_fixture full "$df_fixture/absolute/missing/run" "$df_fixture/repo" "$df_fixture/absolute" &&
  run_disk_guard_fixture full "$df_fixture/nested/a/b" "$df_fixture/repo" "$df_fixture/nested" || {
  echo "disk guard did not select the target filesystem ancestor" >&2
  exit 1
}
if run_disk_guard_fixture below 'target/run' "$df_fixture/repo" "$df_fixture/repo" ||
  run_disk_guard_fixture malformed 'target/run' "$df_fixture/repo" "$df_fixture/repo"; then
  echo "disk guard accepted below-limit or malformed fixture" >&2
  exit 1
fi
rm -rf "$df_fixture"
private_mode_fixture="$(mktemp -d)"
private_mode() {
  local path="$1" expected="$2" mode
  mode="$(stat -f '%Lp' "$path" 2>/dev/null)" ||
    mode="$(stat -c '%a' "$path")" || return 1
  [ "$mode" = "$expected" ]
}
(
  umask 077
  mkdir "$private_mode_fixture/artifacts"
  : > "$private_mode_fixture/artifacts/config-c1.json"
)
private_mode "$private_mode_fixture/artifacts" 700 &&
  private_mode "$private_mode_fixture/artifacts/config-c1.json" 600 || {
  echo "secret-file mode fixture is not private" >&2
  exit 1
}
for mode in 640 604; do
  chmod "$mode" "$private_mode_fixture/artifacts/config-c1.json"
  if private_mode "$private_mode_fixture/artifacts/config-c1.json" 600; then
    echo "secret-file mode fixture accepted group/world-readable artifact" >&2
    exit 1
  fi
done
rm -rf "$private_mode_fixture"

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
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_DIRECT_CLUSTER=0'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_REUSE_EXISTING=0'
require_literal 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_RECOVERY_FORBIDDEN_SENTINEL'
require_literal 'if [ "$recovery_matrix" = 1 ]; then'
require_literal '[ "$recovery_matrix_only" = 1 ] || die "fresh recovery matrix must be matrix-only"'
require_literal 'fresh recovery matrix requires exactly one failure cell'
require_literal 'fresh recovery matrix requires exactly one hold cell'
if grep -Fq 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_E2E_RECOVERY_MATRIX=1' "$script" ||
  grep -Fq 'RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_E2E_RECOVERY_MATRIX_ONLY=1' "$script"; then
  echo "fresh full lifecycle must not require the recovery matrix" >&2
  exit 1
fi
fresh_matrix_is_valid() {
  local matrix="$1" only="$2" fresh="$3" direct="$4" reuse="$5" failures="$6" holds="$7" sentinel="$8"
  [ "$only" = 0 ] || [ "$matrix" = 1 ] || return 1
  [ "$fresh" = 0 ] && return 0
  [ "$direct" = 0 ] && [ "$reuse" = 0 ] && [ -n "$sentinel" ] || return 1
  [ "$matrix" = 0 ] || {
    [ "$only" = 1 ] && [ "$failures" = 1 ] && [ "$holds" = 1 ]
  }
}
if ! fresh_matrix_is_valid 0 0 0 0 0 3 3 '' ||
  ! fresh_matrix_is_valid 1 0 0 0 0 3 3 '' ||
  ! fresh_matrix_is_valid 1 1 0 0 0 3 3 '' ||
  ! fresh_matrix_is_valid 0 0 1 0 0 3 3 sentinel ||
  ! fresh_matrix_is_valid 1 1 1 0 0 1 1 sentinel; then
  echo "fresh recovery accepted environment fixture failed" >&2
  exit 1
fi
for rejected in \
  '0 1 0 0 0 3 3 sentinel' \
  '1 0 1 0 0 1 1 sentinel' \
  '1 1 1 1 0 1 1 sentinel' \
  '1 1 1 0 1 1 1 sentinel' \
  '1 1 1 0 0 2 1 sentinel' \
  '1 1 1 0 0 1 2 sentinel'; do
  read -r matrix only fresh direct reuse failures holds sentinel <<< "$rejected"
  if fresh_matrix_is_valid "$matrix" "$only" "$fresh" "$direct" "$reuse" "$failures" "$holds" "$sentinel"; then
    echo "fresh recovery accepted invalid environment fixture: $rejected" >&2
    exit 1
  fi
done
if fresh_matrix_is_valid 0 0 1 0 0 3 3 ''; then
  echo "fresh recovery accepted missing sentinel fixture" >&2
  exit 1
fi
fresh_inventory_line="$(grep -n '^fresh_capture_empty_bucket_inventory$' "$script" | cut -d: -f1)"
fresh_absence_line="$(grep -n '^fresh_assert_prebootstrap_absence$' "$script" | cut -d: -f1)"
fresh_image_line="$(grep -n '^fresh_capture_live_image_provenance$' "$script" | cut -d: -f1)"
fresh_cell_line="$(grep -n '^fresh_capture_cell_isolation$' "$script" | cut -d: -f1)"
matrix_branch_line="$(grep -n '^if \[ "\$recovery_matrix" = 1 \]; then$' "$script" | tail -1 | cut -d: -f1)"
for fresh_line in "$fresh_inventory_line" "$fresh_absence_line" "$fresh_image_line" "$fresh_cell_line"; do
  [ -n "$fresh_line" ] && [ -n "$matrix_branch_line" ] && [ "$fresh_line" -lt "$matrix_branch_line" ] || {
    echo "fresh isolation must run before the optional recovery matrix" >&2
    exit 1
  }
done
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
require_literal 'sql|graph|kv) ;;'
# shellcheck disable=SC2016
require_literal 'docker build --load --build-arg "RHIZA_PROFILE=$profile" -t "$image" .'
if grep -Fq "docker build --build-arg \"RHIZA_PROFILE=\$profile\" -t \"\$image\" ." "$script"; then
  echo "recovery image build must load the just-built local tag" >&2
  exit 1
fi
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
require_literal 'docker_save_config_digest() {'
require_literal 'docker image save "$image" | tar -xOf - manifest.json | jq -er '
require_literal 'if type != "array" or length != 1 or (.[0].Config | type) != "string" then'
require_literal 'test("^blobs/sha256/[0-9a-f]{64}$")'
require_literal 'test("^[0-9a-f]{64}[.]json$")'
if grep -Fq 'RepoTags' "$script"; then
  echo "recovery image provenance must not depend on RepoTags" >&2
  exit 1
fi
require_literal "value=\"\${value#containerd://}\""
require_literal "value=\"\${value#docker-pullable://}\""
require_literal 'jq -n -e --argjson live "$normalized_live_json" --arg expected "$expected_rhiza_config_id" '
if grep -Fq 'jq -e --argjson live "$normalized_live_json" --arg expected "$expected_rhiza_config_id" ' "$script"; then
  echo "fresh image identity comparison must not read stdin" >&2
  exit 1
fi
require_literal 'fresh isolation live voter image ID does not match built Docker config ID'
require_literal 'fresh isolation RustFS bucket is not empty before bootstrap'
require_literal "node_uid:\$node_uid,rustfs_uid:\$rustfs_uid"
require_literal "image_provenance_verified:true,bucket_inventory_path:\$bucket_inventory_path"
require_literal "expected_manifest_ids:\$expected_manifest_ids,expected_config_id:\$expected_config_id"
require_literal "matched_live_config_id:\$matched_live_config_id,live_rhiza_image_ids:\$live_rhiza_image_ids"
require_literal 's3api list-objects-v2 --bucket rhiza --output json'
require_literal 'length > 0 and'
require_literal 'length == ($plan[0].candidates | length)'
require_literal 'all(.[]; .plan_hash == $hash and .outcome == "deleted")'
require_literal '[.[] | {key, version}] | sort_by(.key, (.version | tojson))'
require_literal '[$plan[0].candidates[] | {key, version}] | sort_by(.key, (.version | tojson))'

gc_fixture="$(mktemp -d)"
printf '%s\n' '{"candidates":[{"key":"a","version":"1"},{"key":"b","version":"2"}]}' \
  > "$gc_fixture/plan.json"
gc_report_matches_plan() {
  jq -e --arg hash plan --slurpfile plan "$gc_fixture/plan.json" '
    .plan_hash == $hash and
    (.results |
      type == "array" and
      length > 0 and
      length == ($plan[0].candidates | length) and
      all(.[]; .plan_hash == $hash and .outcome == "deleted") and
      ([.[] | {key, version}] | sort_by(.key, (.version | tojson))) ==
        ([$plan[0].candidates[] | {key, version}] | sort_by(.key, (.version | tojson)))
    )
  ' "$1" >/dev/null
}
printf '%s\n' '{"plan_hash":"plan","results":[{"plan_hash":"plan","key":"b","version":"2","outcome":"deleted"},{"plan_hash":"plan","key":"a","version":"1","outcome":"deleted"}]}' \
  > "$gc_fixture/report.json"
gc_report_matches_plan "$gc_fixture/report.json" || {
  echo "GC report fixture rejected exact candidate coverage" >&2
  exit 1
}
printf '%s\n' '{"plan_hash":"plan","results":[{"plan_hash":"plan","key":"a","version":"1","outcome":"deleted"},{"plan_hash":"plan","key":"a","version":"1","outcome":"already_missing"}]}' \
  > "$gc_fixture/report.json"
if gc_report_matches_plan "$gc_fixture/report.json"; then
  echo "GC report fixture accepted duplicate or non-deleted result" >&2
  exit 1
fi
rm -rf "$gc_fixture"

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
manifest_config_digest() {
  jq -er '
    if type != "array" or length != 1 or (.[0].Config | type) != "string" then
      error("expected exactly one Docker save manifest config")
    else .[0].Config end |
    if test("^blobs/sha256/[0-9a-f]{64}$") then
      "sha256:" + ltrimstr("blobs/sha256/")
    elif test("^[0-9a-f]{64}[.]json$") then
      "sha256:" + rtrimstr(".json")
    else error("invalid Docker save manifest config") end
  '
}
config_hex=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
for config in "blobs/sha256/$config_hex" "$config_hex.json"; do
  printf '[{"Config":"%s"}]\n' "$config" | manifest_config_digest |
    grep -Fx "sha256:$config_hex" >/dev/null || {
      echo "Docker save manifest config fixture rejected $config" >&2
      exit 1
    }
done
for manifest in '[{}]' '[{"Config":"sha256:not-a-config"}]' \
  "[{\"Config\":\"blobs/sha256/$config_hex\"},{\"Config\":\"blobs/sha256/$config_hex\"}]"; do
  if printf '%s\n' "$manifest" | manifest_config_digest >/dev/null 2>&1; then
    echo "Docker save manifest config fixture accepted malformed, missing, or multiple records" >&2
    exit 1
  fi
done
if { false | manifest_config_digest; } >/dev/null 2>&1; then
  echo "Docker save manifest config pipeline masked an upstream failure" >&2
  exit 1
fi
identity_live='["sha256:0123456789abcdef"]'
identity_expected='sha256:0123456789abcdef'
jq -n -e --argjson live "$identity_live" --arg expected "$identity_expected" '
  ($live | length == 1) and ($live[0] == $expected)' < /dev/null >/dev/null || {
  echo "fresh image identity fixture must match without stdin" >&2
  exit 1
}
if jq -n -e --argjson live "$identity_live" \
  --arg expected 'sha256:different' '
    ($live | length == 1) and ($live[0] == $expected)' < /dev/null >/dev/null; then
  echo "fresh image identity fixture accepted a mismatched digest" >&2
  exit 1
fi
[ "$(grep -Fc "matrix_run_no_quorum_safety_probe \"\$probe_sequence\"" "$script")" -eq 1 ] || {
  echo "F2/F3 safety probe must run for every scheduled hold sample" >&2
  exit 1
}
require_literal 'matrix_expect_write_no_quorum'
require_literal '(.code == "write_timeout" or .code == "write_outcome_unknown" or
     .code == "ambiguous_mutation" or .code == "unavailable")'
require_literal 'write_retry_deadline_seconds=60'
# The post-restore probe may encounter a short-lived quorum convergence window.
# It must retry only known retryable HTTP errors, with the original request ID.
require_literal 'retryable_write_failure'
require_literal 'HTTP 503 Service Unavailable code=(write_timeout|write_outcome_unknown|ambiguous_mutation|unavailable|writes_unavailable)'
retryable_write_pattern='^write failed: HTTP 503 Service Unavailable code=(write_timeout|write_outcome_unknown|ambiguous_mutation|unavailable|writes_unavailable)( |$)'
for retryable_code in write_timeout write_outcome_unknown ambiguous_mutation unavailable writes_unavailable; do
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
require_literal 'profile_put "$pod" "$key" "$value" "$request_id" 2> "$attempt_log"'
# shellcheck disable=SC2016
require_literal_count 'profile_put "$pod" "$key" "$value" "$request_id" 2> "$attempt_log"' 1
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
require_literal 'matrix_response_is_ambiguous_mutation'
# shellcheck disable=SC2016
require_literal '[ "$status" = 503 ]'
require_literal '.code == "ambiguous_mutation" and .retryable == true'
# shellcheck disable=SC2016
require_literal '"$(wc -c < "$body")" -le 65536'
require_literal 'matrix_service_mutation_response()'
require_literal 'return 75'
require_literal 'ambiguous_mutation_retry_exhausted'
# shellcheck disable=SC2016
require_literal '[ "$write_status" = 0 ] && break'
# shellcheck disable=SC2016
require_literal 'if matrix_service_read "$service_rto_key" "$service_rto_value" read_barrier; then'
require_literal 'matrix_prepare_delete_request()'
# shellcheck disable=SC2016
require_literal 'first="$(matrix_service_mutation_response "$matrix_path" "$matrix_body")"'
# shellcheck disable=SC2016
require_literal 'second="$(profile_put "$pod" "$key" "$value" "$put_id")"'
# shellcheck disable=SC2016
require_literal 'delete_first="$(matrix_service_mutation_response "$matrix_path" "$matrix_body")"'
# shellcheck disable=SC2016
require_literal 'delete_second="$(profile_delete "$pod" "$key" "$delete_id")"'
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

[ "$(grep -Fc 'k exec "$pod" -- rhiza' "$script")" -eq 1 ] || {
  echo "runtime Pod exec must have exactly one approved invocation" >&2
  exit 1
}
# shellcheck disable=SC2016
grep -Fxq '  k exec "$pod" -- rhiza "$@" --url http://127.0.0.1:8080' "$script" || {
  echo "runtime Pod exec must use the Rhiza CLI invocation" >&2
  exit 1
}

retry_fixture_dir="$(mktemp -d)"
trap 'rm -rf "$retry_fixture_dir"' EXIT
target="$retry_fixture_dir"
# shellcheck disable=SC2034
matrix_path=/v1/write
# shellcheck disable=SC2034
matrix_body='{"request_id":"fixed","key":"key","value":"value"}'
matrix_prepare_write_request() { :; }
matrix_service_http() {
  fixture_call_count=$((fixture_call_count + 1))
  fixture_paths+=("$1")
  fixture_bodies+=("$2")
  # shellcheck disable=SC2034
  matrix_last_job="fixture-$fixture_call_count"
  matrix_last_http_status="${fixture_statuses[$((fixture_call_count - 1))]}"
  matrix_last_http_body="$target/fixture-$fixture_call_count.response"
  matrix_last_http_raw="$target/fixture-$fixture_call_count.response.raw"
  printf '%s' "${fixture_bodies_by_attempt[$((fixture_call_count - 1))]}" > "$matrix_last_http_body"
  printf 'fixture raw %s\n' "$fixture_call_count" > "$matrix_last_http_raw"
  case "$matrix_last_http_status" in
    2[0-9][0-9]) cat "$matrix_last_http_body"; return 0 ;;
    *) return 1 ;;
  esac
}
eval "$(awk '
  /^matrix_response_is_ambiguous_mutation\(\)/,/^matrix_service_write\(\)/ {
    if (/^matrix_service_write\(\)/) exit
    print
  }
' "$script")"
run_retry_fixture() {
  fixture_statuses=("$@")
  fixture_bodies_by_attempt=("${fixture_fixture_bodies[@]}")
  fixture_call_count=0
  fixture_paths=()
  fixture_bodies=()
}

fixture_fixture_bodies=(
  '{"code":"ambiguous_mutation","retryable":true}'
  '{"ok":true}'
)
run_retry_fixture 503 200
matrix_service_mutation_response /v1/fixture '{"request_id":"fixed"}' > "$target/fixture-output"
fixture_output="$(< "$target/fixture-output")"
[ "$fixture_output" = '{"ok":true}' ] &&
  [ "$fixture_call_count" -eq 2 ] &&
  [ "${fixture_paths[0]}" = "${fixture_paths[1]}" ] &&
  [ "${fixture_bodies[0]}" = "${fixture_bodies[1]}" ] || {
  echo 'matrix exact retry ambiguous->success fixture failed' >&2
  exit 1
}

fixture_fixture_bodies=(
  '{"code":"ambiguous_mutation","retryable":true}'
  '{"code":"ambiguous_mutation","retryable":true}'
)
run_retry_fixture 503 503
if matrix_service_mutation_response /v1/fixture '{"request_id":"fixed"}' >/dev/null; then
  echo 'matrix exact retry ambiguous->ambiguous unexpectedly succeeded' >&2
  exit 1
else
  retry_status=$?
fi
[ "$fixture_call_count" -eq 2 ] && [ "$retry_status" = 75 ] || exit 1

oversized_fixture_body="$(head -c 65537 /dev/zero | tr '\0' x)"
[ "$(LC_ALL=C printf '%s' "$oversized_fixture_body" | wc -c | tr -d ' ')" = 65537 ] || exit 1
for fixture_body in '' 'true' '{not-json' '{"code":"unavailable","retryable":true}' \
  '{"code":"ambiguous_mutation","retryable":false}' "$oversized_fixture_body"; do
  fixture_fixture_bodies=("$fixture_body")
  run_retry_fixture 503
  if matrix_service_mutation_response /v1/fixture '{"request_id":"fixed"}' >/dev/null; then
    echo 'matrix non-exact ambiguity fixture unexpectedly succeeded' >&2
    exit 1
  fi
  [ "$fixture_call_count" -eq 1 ] || {
    echo 'matrix non-exact ambiguity fixture retried' >&2
    exit 1
  }
done

fixture_fixture_bodies=('{"ok":true}')
run_retry_fixture 200
matrix_service_mutation_response /v1/fixture '{"request_id":"fixed"}' >/dev/null
[ "$fixture_call_count" -eq 1 ] || {
  echo 'matrix successful write fixture retried' >&2
  exit 1
}

fixture_fixture_bodies=('{"code":"unavailable","retryable":true}')
run_retry_fixture 503
if matrix_service_mutation_response /v1/fixture '{"request_id":"fixed"}' >/dev/null; then
  echo 'matrix nonambiguous failure fixture unexpectedly succeeded' >&2
  exit 1
fi
[ "$fixture_call_count" -eq 1 ] || {
  echo 'matrix nonambiguous failure retained attempt evidence or terminal output' >&2
  exit 1
}

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
