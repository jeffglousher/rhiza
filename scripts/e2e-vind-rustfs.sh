#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
profile="${RHIZA_EXECUTION_PROFILE-}"
logical_cluster_id=rhiza-vind
canonical_cluster_id="rhiza:${profile}:${logical_cluster_id}"
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
rhiza_commit="$(git -C "$repo_root" rev-parse HEAD)"
rhiza_dirty=false
[ -z "$(git -C "$repo_root" status --porcelain --untracked-files=all)" ] || rhiza_dirty=true
cluster="${RHIZA_VIND_CLUSTER:-rhiza-vind-${run_id}}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
image="${RHIZA_IMAGE:-rhiza:dev}"
rustfs_image="${RHIZA_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${RHIZA_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
cleanup="${RHIZA_VIND_CLEANUP:-1}"
deploy_only="${RHIZA_E2E_DEPLOY_ONLY:-0}"
skip_build="${RHIZA_VIND_SKIP_BUILD:-0}"
direct_cluster="${RHIZA_VIND_DIRECT_CLUSTER:-0}"
skip_image_load="${RHIZA_VIND_SKIP_IMAGE_LOAD:-0}"
recovery_matrix="${RHIZA_E2E_RECOVERY_MATRIX:-0}"
recovery_matrix_only="${RHIZA_E2E_RECOVERY_MATRIX_ONLY:-0}"
recovery_require_fresh_vcluster="${RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER:-0}"
recovery_forbidden_sentinel="${RHIZA_RECOVERY_FORBIDDEN_SENTINEL:-}"
recovery_hold_csv="${RHIZA_RECOVERY_HOLD_SECONDS:-60,180,300}"
recovery_fail_csv="${RHIZA_RECOVERY_FAIL_PEERS:-1,2,3}"
recovery_timeout="${RHIZA_STATEFULSET_READY_TIMEOUT:-420}"
recovery_auto_timeout="${RHIZA_RECOVERY_AUTO_TIMEOUT_SECONDS:-30}"
recovery_f1_probe_interval="${RHIZA_RECOVERY_F1_PROBE_INTERVAL_SECONDS:-10}"
recovery_no_quorum_probe_max_lateness="${RHIZA_RECOVERY_NO_QUORUM_PROBE_MAX_LATENESS_SECONDS:-5}"
# A freshly Ready StatefulSet can still be converging its peer/Recorder transports.
# Keep this finite so the post-restore probe cannot hide a persistent regression.
write_retry_deadline_seconds=60
target="${RHIZA_E2E_TARGET_DIR:-target/rhiza-e2e}/${profile:-missing}/$run_id"
context=""
previous_context=""
resolved_image=""
expected_rhiza_image_ids='[]'
expected_rhiza_config_id=""
matched_rhiza_config_id=""
created_cluster=false
created_namespace=false
namespace_uid=""
node_uid=""
bucket_inventory_path=""
live_rhiza_image_ids='[]'
fresh_cell_isolation=null
marker=/var/lib/rhiza/emptydir-marker
marker_helper_container=e2e-marker
marker_helper_image="${RHIZA_MARKER_HELPER_IMAGE:-busybox:1.36.1}"
chaos_run_id="${RHIZA_CHAOS_RUN_ID:-}"
diagnostic_secrets=()

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }; }
require_one_gib_free() {
  local target_root="${1-}" candidate parent available_kib
  [ -n "$target_root" ] || die "target path must not be empty"
  case "$target_root" in
    /) die "target path must not be filesystem root" ;;
    /*) candidate="$target_root" ;;
    *) candidate="$repo_root/$target_root" ;;
  esac
  case "/$candidate/" in */./*|*/../*) die "target path must not contain . or .. components";; esac
  while :; do
    [ ! -L "$candidate" ] || die "target filesystem ancestor must not be a symlink"
    if [ -e "$candidate" ]; then
      [ -d "$candidate" ] || die "target filesystem ancestor must be a directory"
      [ "$candidate" != / ] || die "target filesystem ancestor must not be root"
      break
    fi
    parent="$(dirname "$candidate")"
    [ "$parent" != "$candidate" ] || die "target filesystem ancestor is missing"
    candidate="$parent"
  done
  available_kib="$(df -Pk "$candidate" | awk '
    NR == 2 { available = $4 }
    END {
      if (NR != 2 || available !~ /^[0-9]+$/) exit 1
      print available
    }
  ')" || die "cannot determine free space for target filesystem"
  [ "$available_kib" -ge 1048576 ] || die "requires at least 1 GiB free on target filesystem; found ${available_kib} KiB"
}
case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac
for tool in docker kubectl jq yq openssl tar df; do require "$tool"; done
[ "$direct_cluster" = 1 ] || require vcluster
case "$cleanup" in 0|1) ;; *) die "RHIZA_VIND_CLEANUP must be 0 or 1";; esac
case "$deploy_only" in 0|1) ;; *) die "RHIZA_E2E_DEPLOY_ONLY must be 0 or 1";; esac
case "$skip_build" in 0|1) ;; *) die "RHIZA_VIND_SKIP_BUILD must be 0 or 1";; esac
case "$direct_cluster" in 0|1) ;; *) die "RHIZA_VIND_DIRECT_CLUSTER must be 0 or 1";; esac
case "$skip_image_load" in 0|1) ;; *) die "RHIZA_VIND_SKIP_IMAGE_LOAD must be 0 or 1";; esac
case "$marker_helper_image" in '') die "RHIZA_MARKER_HELPER_IMAGE must not be empty";; esac
if [ -n "$chaos_run_id" ] && ! [[ "$chaos_run_id" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]]; then
  die "RHIZA_CHAOS_RUN_ID must be a DNS label"
fi
case "$recovery_matrix" in 0|1) ;; *) die "RHIZA_E2E_RECOVERY_MATRIX must be 0 or 1";; esac
case "$recovery_matrix_only" in 0|1) ;; *) die "RHIZA_E2E_RECOVERY_MATRIX_ONLY must be 0 or 1";; esac
case "$recovery_require_fresh_vcluster" in 0|1) ;; *) die "RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER must be 0 or 1";; esac
[ "$recovery_matrix_only" = 0 ] || [ "$recovery_matrix" = 1 ] \
  || die "RHIZA_E2E_RECOVERY_MATRIX_ONLY=1 requires RHIZA_E2E_RECOVERY_MATRIX=1"
case "$recovery_timeout" in ''|*[!0-9]*|0) die "RHIZA_STATEFULSET_READY_TIMEOUT must be positive";; esac
case "$recovery_auto_timeout" in ''|*[!0-9]*|0) die "RHIZA_RECOVERY_AUTO_TIMEOUT_SECONDS must be positive";; esac
case "$recovery_f1_probe_interval" in ''|*[!0-9]*|0) die "RHIZA_RECOVERY_F1_PROBE_INTERVAL_SECONDS must be positive";; esac
case "$recovery_no_quorum_probe_max_lateness" in ''|*[!0-9]*|0) die "RHIZA_RECOVERY_NO_QUORUM_PROBE_MAX_LATENESS_SECONDS must be positive";; esac
IFS=, read -r -a recovery_holds <<< "$recovery_hold_csv"
IFS=, read -r -a recovery_failures <<< "$recovery_fail_csv"
[ "${#recovery_holds[@]}" -gt 0 ] || die "RHIZA_RECOVERY_HOLD_SECONDS must not be empty"
[ "${#recovery_failures[@]}" -gt 0 ] || die "RHIZA_RECOVERY_FAIL_PEERS must not be empty"
for hold in "${recovery_holds[@]}"; do
  case "$hold" in ''|*[!0-9]*|0) die "invalid RHIZA_RECOVERY_HOLD_SECONDS cell: $hold";; esac
done
for failed in "${recovery_failures[@]}"; do
  case "$failed" in 1|2|3) ;; *) die "invalid RHIZA_RECOVERY_FAIL_PEERS cell: $failed";; esac
done
if [ "$recovery_require_fresh_vcluster" = 1 ]; then
  [ "$direct_cluster" = 0 ] || die "RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_DIRECT_CLUSTER=0"
  [ "${RHIZA_VIND_REUSE_EXISTING:-0}" = 0 ] || die "RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_VIND_REUSE_EXISTING=0"
  [ -n "$recovery_forbidden_sentinel" ] || die "RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires RHIZA_RECOVERY_FORBIDDEN_SENTINEL"
  if [ "$recovery_matrix" = 1 ]; then
    [ "$recovery_matrix_only" = 1 ] || die "fresh recovery matrix must be matrix-only"
    [ "${#recovery_failures[@]}" = 1 ] || die "fresh recovery matrix requires exactly one failure cell"
    [ "${#recovery_holds[@]}" = 1 ] || die "fresh recovery matrix requires exactly one hold cell"
  fi
fi

k() { kubectl --context "$context" -n "$namespace" "$@"; }
validate_local_docker_context() {
  local selected endpoint
  selected="$(docker context show)" || die "cannot determine Docker context"
  [ -n "$selected" ] || die "Docker context must not be empty"
  endpoint="$(docker context inspect "$selected" --format '{{ .Endpoints.docker.Host }}')" \
    || die "cannot inspect Docker context $selected"
  case "$(uname -s)" in
    Darwin)
      case "$endpoint" in unix:///*) ;; *) die "Docker context $selected is not a local Unix socket";; esac
      ;;
    *)
      case "$endpoint" in unix:///*|npipe:////./pipe/*) ;; *) die "Docker context $selected is not local";; esac
      ;;
  esac
}
validate_local_vcluster_context() {
  local endpoint
  [ "$context" = "vcluster-docker_$cluster" ] || {
    printf 'local-context-preflight: unexpected-context context=%s\n' "$context" >&2
    return 1
  }
  endpoint="$(kubectl config view -o json 2>/dev/null | jq -er --arg context "$context" '
    [.contexts[]? | select(.name == $context) | .context.cluster] as $clusters |
    if ($clusters | length) != 1 or ($clusters[0] | type) != "string" then empty
    else [.clusters[]? | select(.name == $clusters[0]) | .cluster.server] as $servers |
      if ($servers | length) == 1 and ($servers[0] | type) == "string" and
         ($servers[0] | length) > 0 then $servers[0] else empty end
    end
  ' 2>/dev/null)" || {
    printf 'local-context-preflight: mapping-missing-or-ambiguous context=%s\n' "$context" >&2
    return 1
  }
  case "$endpoint" in
    https://127.0.0.1:*|https://localhost:*|https://[::1]:*) ;;
    *)
      printf 'local-context-preflight: endpoint-not-loopback context=%s\n' "$context" >&2
      return 1
      ;;
  esac
}
format_epoch_utc() {
  date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ
}
normalize_image_id() {
  local value="$1"
  value="${value#containerd://}"
  value="${value#docker://}"
  value="${value#docker-pullable://}"
  case "$value" in *@sha256:*) value="sha256:${value##*@sha256:}";; esac
  printf '%s\n' "$value"
}
docker_save_config_digest() {
  docker image save "$image" | tar -xOf - manifest.json | jq -er '
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
decode_base64() {
  base64 --decode 2>/dev/null || base64 -D
}
fresh_capture_live_image_provenance() {
  local normalized_live normalized_live_json
  [ "$recovery_require_fresh_vcluster" = 1 ] || return 0
  live_rhiza_image_ids="$(k get pods -l app.kubernetes.io/name=rhiza -o json | jq -c '
    [.items | sort_by(.metadata.name)[] |
      {pod:.metadata.name,image_id:(.status.containerStatuses[] | select(.name == "rhiza") | .imageID)}]')"
  [ "$(jq 'length' <<< "$live_rhiza_image_ids")" = 3 ] \
    || die "fresh isolation requires live image IDs from exactly three voters"
  normalized_live="$(jq -r '.[].image_id' <<< "$live_rhiza_image_ids" | while IFS= read -r image_id; do
    normalize_image_id "$image_id"
  done | sort -u)"
  normalized_live_json="$(printf '%s\n' "$normalized_live" | jq -R . | jq -sc 'unique')"
  jq -n -e --argjson live "$normalized_live_json" --arg expected "$expected_rhiza_config_id" '
    ($live | length == 1) and ($live[0] == $expected)' >/dev/null \
    || die "fresh isolation live voter image ID does not match built Docker config ID"
  matched_rhiza_config_id="$(jq -r '.[0]' <<< "$normalized_live_json")"
}
fresh_capture_empty_bucket_inventory() {
  local inventory_job access_key secret_key
  [ "$recovery_require_fresh_vcluster" = 1 ] || return 0
  inventory_job="rhiza-${profile}-fresh-inventory-${run_id}"
  access_key="$(k get secret rustfs-credentials -o jsonpath='{.data.access-key}' | decode_base64)"
  secret_key="$(k get secret rustfs-credentials -o jsonpath='{.data.secret-key}' | decode_base64)"
  [ -n "$access_key" ] && [ -n "$secret_key" ] || die "fresh isolation RustFS credentials unavailable"
  diagnostic_secrets+=("$access_key" "$secret_key")
  k run "$inventory_job" --image="$aws_image" --restart=Never \
    --env="AWS_ACCESS_KEY_ID=$access_key" --env="AWS_SECRET_ACCESS_KEY=$secret_key" \
    --env=AWS_DEFAULT_REGION=us-east-1 --command -- sh -ec \
    'aws --endpoint-url http://rustfs:9000 s3api list-objects-v2 --bucket rhiza --output json' >/dev/null
  k wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$inventory_job" --timeout=120s >/dev/null \
    || die "fresh isolation bucket inventory job failed"
  bucket_inventory_path="$target/fresh-rustfs-bucket-inventory.json"
  k logs "$inventory_job" > "$bucket_inventory_path"
  jq -e '(.KeyCount // 0) == 0 and ((.Contents // []) | length) == 0' \
    "$bucket_inventory_path" >/dev/null || die "fresh isolation RustFS bucket is not empty before bootstrap"
}
fresh_assert_prebootstrap_absence() {
  [ "$recovery_require_fresh_vcluster" = 1 ] || return 0
  [ "$created_cluster" = true ] || die "fresh isolation requires a newly created vcluster"
  [ "$created_namespace" = true ] || die "fresh isolation requires a newly created namespace"
  [ -n "$namespace_uid" ] || die "fresh isolation requires a namespace UID"
  [ "$(k get persistentvolumeclaims -o json | jq '.items | length')" = 0 ] \
    || die "fresh isolation requires zero PVCs before bootstrap"
  [ "$(k get pods -o json | jq '[.items[].spec.volumes[]? | select(has("hostPath"))] | length')" = 0 ] \
    || die "fresh isolation requires zero hostPath volumes before bootstrap"
  [ "$(k get statefulsets -l app.kubernetes.io/name=rhiza -o json | jq '.items | length')" = 0 ] \
    || die "fresh isolation observed preexisting Rhiza StatefulSet state"
  [ "$(k get pods -l app.kubernetes.io/name=rhiza -o json | jq '.items | length')" = 0 ] \
    || die "fresh isolation observed preexisting Rhiza Pod state"
  if env | grep -Eq '^RHIZA_(RESTORE|RECOVERY_RESTORE|SUCCESSOR_RESTORE)='; then
    die "fresh isolation forbids restore environment input"
  fi
}
fresh_verify_forbidden_sentinel() {
  local ordinal output
  [ "$recovery_require_fresh_vcluster" = 1 ] || return 0
  for ordinal in 0 1 2; do
    output="$(client "${name_c1}-$ordinal" read --key "$recovery_forbidden_sentinel" \
      --consistency read_barrier)" || die "fresh isolation forbidden sentinel read failed on voter $ordinal"
    [[ "$output" == value=null\ applied_index=*\ hash=* ]] \
      || die "fresh isolation forbidden sentinel exists on voter $ordinal"
  done
}
fresh_capture_cell_isolation() {
  local voter_uids statefulset_uid statuses sentinel_key sentinel_value artifact artifact_tmp
  [ "$recovery_require_fresh_vcluster" = 1 ] || return 0
  voter_uids="$(matrix_capture_uids)" || die "fresh isolation voter UID capture failed"
  jq -e 'length == 3 and
    (map(.pod) | sort == ["rhiza-sql-c1-0", "rhiza-sql-c1-1", "rhiza-sql-c1-2"]) and
    (map(.uid) | unique | length == 3)' <<< "$voter_uids" >/dev/null \
    || die "fresh isolation requires exact three voter Pod UIDs"
  statefulset_uid="$(k get statefulset "$name_c1" -o jsonpath='{.metadata.uid}')"
  [ -n "$statefulset_uid" ] || die "fresh isolation StatefulSet UID missing"
  [ -n "$rustfs_uid" ] || die "fresh isolation RustFS UID missing"
  [ -n "$node_uid" ] || die "fresh isolation vcluster node UID missing"
  [ -n "$bucket_inventory_path" ] && [ -f "$bucket_inventory_path" ] \
    || die "fresh isolation RustFS inventory evidence missing"
  [ "$(jq '(.KeyCount // 0) == 0 and ((.Contents // []) | length) == 0' "$bucket_inventory_path")" = true ] \
    || die "fresh isolation RustFS inventory is not empty"
  [ "$(k get persistentvolumeclaims -o json | jq '.items | length')" = 0 ] \
    || die "fresh isolation requires zero PVCs"
  [ "$(k get pods -o json | jq '[.items[].spec.volumes[]? | select(has("hostPath"))] | length')" = 0 ] \
    || die "fresh isolation requires zero hostPath volumes"
  statuses="$(for ordinal in 0 1 2; do
    scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-$ordinal" GET /v1/admin/membership/status
  done)"
  printf '%s\n' "$statuses" | jq -s --arg cluster "$canonical_cluster_id" '
    length == 3 and all(.[];
      .cluster_id == $cluster and .epoch == 1 and
      .members == ["node-1", "node-2", "node-3"] and
      .node.ready == true and .node.active_config_id == 1 and
      .node.configuration_state.config_id == 1 and
      .node.configuration_state.phase == "active")' >/dev/null \
    || die "fresh isolation config-1 membership did not converge"
  fresh_verify_forbidden_sentinel
  sentinel_key="fresh-bootstrap-${run_id}"
  sentinel_value="current-run-${run_id}"
  write_value "${name_c1}-0" "$sentinel_key" "$sentinel_value" "fresh-bootstrap-${run_id}" \
    || die "fresh isolation bootstrap sentinel write failed"
  for ordinal in 0 1 2; do
    read_value "${name_c1}-$ordinal" "$sentinel_key" "$sentinel_value" \
      || die "fresh isolation bootstrap sentinel missing on voter $ordinal"
  done
  artifact="$target/cell-isolation.json"
  artifact_tmp="${artifact}.tmp"
  jq -cn --arg mode fresh-vcluster --arg cluster "$cluster" --arg context "$context" \
    --arg namespace "$namespace" --arg namespace_uid "$namespace_uid" \
    --arg statefulset_uid "$statefulset_uid" --arg rustfs_uid "$rustfs_uid" --arg node_uid "$node_uid" \
    --argjson expected_manifest_ids "$expected_rhiza_image_ids" \
    --arg expected_config_id "$expected_rhiza_config_id" \
    --arg matched_live_config_id "$matched_rhiza_config_id" \
    --arg bucket_inventory_path "$bucket_inventory_path" \
    --argjson live_rhiza_image_ids "$live_rhiza_image_ids" \
    --arg identity_artifact_path "$artifact" \
    --arg sentinel_key "$sentinel_key" --arg sentinel_value "$sentinel_value" \
    --argjson voter_pod_uids "$voter_uids" \
    '{mode:$mode,process_generation_new:true,
      process_generation_proof:"new vcluster, managed namespace, StatefulSet UID, and three distinct voter Pod UIDs",
      storage_generation_new:true,
      storage_generation_proof:"new vcluster node and RustFS Pod UIDs with an empty RustFS bucket inventory and zero PVC/hostPath volumes",
      restore_env:"absent",restore_env_absent:true,restore_env_proof:"no RHIZA restore environment input",
      prior_sentinel_absent:true,prior_sentinel_proof:"coordinator forbidden sentinel read as null on all three voters before first voter write",
      exact_membership:true,object_provenance_current:true,
      object_provenance_proof:"current-run bootstrap sentinel acknowledged by all three config-1 voters after empty bucket inventory verification",
      vcluster:{name:$cluster,context:$context,created:true},
      namespace:{name:$namespace,uid:$namespace_uid,managed:true,created:true},
      statefulset_uid:$statefulset_uid,node_uid:$node_uid,rustfs_uid:$rustfs_uid,
      expected_manifest_ids:$expected_manifest_ids,expected_config_id:$expected_config_id,
      matched_live_config_id:$matched_live_config_id,live_rhiza_image_ids:$live_rhiza_image_ids,
      image_provenance_verified:true,bucket_inventory_path:$bucket_inventory_path,
      voter_pod_uids:$voter_pod_uids,pvc_count:0,hostpath_volume_count:0,
      prebootstrap_rhiza_workloads_absent:true,
      prebootstrap_qlog_materializer_state_absent:true,
      prebootstrap_qlog_materializer_proof:"no Rhiza StatefulSets or Pods existed before bootstrap",
      identity_artifact_path:$identity_artifact_path,
      current_run_sentinel:{key:$sentinel_key,value:$sentinel_value}}' > "$artifact_tmp"
  mv "$artifact_tmp" "$artifact"
  fresh_cell_isolation="$(jq -c . "$artifact")"
}
inject_marker_helper() {
  local manifest="$1"
  export MARKER_HELPER_IMAGE="$marker_helper_image"
  yq eval --inplace '
    with(select(.kind == "StatefulSet");
      .spec.template.spec.containers += [{
        "name":"e2e-marker", "image":strenv(MARKER_HELPER_IMAGE),
        "command":["sleep", "infinity"],
        "resources":{"requests":{"cpu":"1m", "memory":"8Mi"},
                     "limits":{"cpu":"10m", "memory":"32Mi"}},
        "volumeMounts":[{"name":"data", "mountPath":"/var/lib/rhiza"}]
      }]
    )
  ' "$manifest"
}
verify_marker_helper() {
  local statefulset="$1" ordinal
  k get statefulset "$statefulset" -o json | jq -e --arg helper "$marker_helper_container" \
    'any(.spec.template.spec.containers[]; .name == $helper)' >/dev/null ||
    die "marker helper is absent from StatefulSet template: $statefulset"
  for ordinal in 0 1 2; do
    k get pod "${statefulset}-$ordinal" -o json | jq -e --arg helper "$marker_helper_container" \
      'any(.spec.containers[]; .name == $helper)' >/dev/null ||
      die "marker helper is absent from Pod: ${statefulset}-$ordinal"
  done
}
marker_seed() {
  local pod="$1"
  # shellcheck disable=SC2016
  k exec -c "$marker_helper_container" "$pod" -- sh -ec \
    'printf marker > "$1"' sh "$marker"
}
marker_present() {
  local pod="$1"
  k exec -c "$marker_helper_container" "$pod" -- test -e "$marker"
}
marker_absent() {
  local pod="$1"
  k exec -c "$marker_helper_container" "$pod" -- test ! -e "$marker"
}
redact_diagnostic_stream() {
  local line secret
  while IFS= read -r line || [ -n "$line" ]; do
    for secret in "${diagnostic_secrets[@]}"; do
      [ -z "$secret" ] || line="${line//"$secret"/[REDACTED]}"
    done
    printf '%s\n' "$line"
  done
}
capture_failure_diagnostics() {
  local diagnostics="$target/failure-diagnostics" pod pod_name
  mkdir -p "$diagnostics"
  k get pods -o wide 2>&1 |
    redact_diagnostic_stream > "$diagnostics/pods.txt" || true
  k get pods -l app.kubernetes.io/name=rhiza -o json 2>&1 |
    redact_diagnostic_stream > "$diagnostics/rhiza-pods.json" || true
  k get events --sort-by=.metadata.creationTimestamp 2>&1 |
    redact_diagnostic_stream > "$diagnostics/events.txt" || true
  while IFS= read -r pod; do
    [ -n "$pod" ] || continue
    pod_name="${pod#pod/}"
    k describe "$pod" 2>&1 |
      redact_diagnostic_stream > "$diagnostics/${pod_name}.describe.txt" || true
    k logs "$pod" --all-containers=true 2>&1 |
      redact_diagnostic_stream > "$diagnostics/${pod_name}.current.log" || true
    k logs "$pod" --all-containers=true --previous 2>&1 |
      redact_diagnostic_stream > "$diagnostics/${pod_name}.previous.log" || true
  done < <(k get pods -l app.kubernetes.io/name=rhiza -o name 2>/dev/null || true)
}
capture_ready_context() {
  [ -n "$context" ] || die "no Kubernetes context selected"
  for ((attempt=1; attempt<=120; attempt++)); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then
      return
    fi
    [ "$attempt" -lt 120 ] || die "Kubernetes API did not become ready for context $context"
    sleep 1
  done
}
cleanup_run() {
  status="$1"
  if [ "$status" -ne 0 ] && [ -n "$context" ]; then
    capture_failure_diagnostics || true
    k get pods,deployments,statefulsets,jobs,services,persistentvolumeclaims -o wide >&2 || true
    k get events --sort-by=.metadata.creationTimestamp >&2 || true
  fi
  if [ "$cleanup" = 1 ] && "$created_cluster"; then
    if ! vcluster delete "$cluster" --driver docker > "$target/cleanup-vcluster-delete.log" 2>&1; then
      printf 'warning: local vcluster cleanup failed for %s; inspect %s\n' \
        "$cluster" "$target/cleanup-vcluster-delete.log" >&2
    fi
  fi
  if [ "$cleanup" = 1 ] && [ "$direct_cluster" = 1 ] && [ -n "$context" ]; then
    managed="$(kubectl --context "$context" get namespace "$namespace" \
      -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}' 2>/dev/null || true)"
    owner="$(kubectl --context "$context" get namespace "$namespace" \
      -o go-template='{{index .metadata.labels "rhiza.dev/e2e-run-id"}}' 2>/dev/null || true)"
    if [ "$managed" = true ] && [ "$owner" = "$run_id" ]; then
      kubectl --context "$context" delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
    fi
  fi
  if [ "$direct_cluster" = 0 ]; then
    [ -z "$previous_context" ] || kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
}
trap 'status=$?; cleanup_run "$status"; exit "$status"' EXIT

cd "$repo_root"
require_one_gib_free "$target"
mkdir -p "$target"
chmod 700 "$target"
previous_context="$(kubectl config current-context 2>/dev/null || true)"
validate_local_docker_context

if [ "$skip_build" = 1 ]; then
  docker image inspect "$image" >/dev/null 2>&1 \
    || die "RHIZA_VIND_SKIP_BUILD=1 requires existing local image: $image"
else
  docker build --load --build-arg "RHIZA_PROFILE=$profile" -t "$image" .
fi
resolved_image="$(docker image inspect --format '{{.Id}}' "$image")"
[ -n "$resolved_image" ] || die "cannot resolve Rhiza image ID: $image"
expected_rhiza_image_ids="$(docker image inspect "$image" | jq -r '.[0].Id, .[0].RepoDigests[]?' |
  while IFS= read -r image_id; do normalize_image_id "$image_id"; done |
  sort -u | jq -R . | jq -sc 'unique')"
jq -e 'length > 0 and all(.[]; type == "string" and startswith("sha256:"))' \
  <<< "$expected_rhiza_image_ids" >/dev/null \
  || die "cannot resolve expected Rhiza Docker manifest IDs"
expected_rhiza_config_id="$(docker_save_config_digest)" \
  || die "cannot resolve expected Rhiza Docker config ID"
if [ "$direct_cluster" = 1 ]; then
  context="${RHIZA_VIND_CONTEXT:-}"
  [ -n "$context" ] || die "RHIZA_VIND_DIRECT_CLUSTER=1 requires RHIZA_VIND_CONTEXT"
else
  vcluster use driver docker >/dev/null
  if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
    [ "${RHIZA_VIND_REUSE_EXISTING:-0}" = 1 ] || die "vind cluster already exists: $cluster"
    [ "$recovery_require_fresh_vcluster" = 0 ] || die "fresh isolation refuses an existing vcluster"
    vcluster connect "$cluster" --driver docker >/dev/null
  else
    vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
    created_cluster=true
  fi
  context="vcluster-docker_$cluster"
  if ! context_preflight="$(validate_local_vcluster_context 2>&1)"; then
    printf '%s\n' "$context_preflight" > "$target/local-context-preflight.log"
    printf '%s\n' "$context_preflight" >&2
    context=""
    die "refusing non-local vcluster context"
  fi
fi
capture_ready_context
[ "$direct_cluster" = 1 ] || kubectl config use-context "$context" >/dev/null
if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  [ "$recovery_require_fresh_vcluster" = 0 ] || die "fresh isolation refuses an existing namespace"
  managed="$(kubectl --context "$context" get namespace "$namespace" \
    -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}')"
  [ "$managed" = true ] || die "refusing to replace unmanaged namespace $namespace"
  kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
kubectl --context "$context" label namespace "$namespace" \
  rhiza.dev/e2e-managed=true "rhiza.dev/e2e-run-id=$run_id" >/dev/null
created_namespace=true
namespace_uid="$(kubectl --context "$context" get namespace "$namespace" -o jsonpath='{.metadata.uid}')"
[ -n "$namespace_uid" ] || die "cannot capture managed namespace UID"

node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node for image loading"
node_uid="$(kubectl --context "$context" get node "$node" -o jsonpath='{.metadata.uid}')"
[ -n "$node_uid" ] || die "cannot capture vind node UID"
if [ "$skip_image_load" = 0 ]; then
  [ "$direct_cluster" = 0 ] \
    || die "direct-cluster mode requires RHIZA_VIND_SKIP_IMAGE_LOAD=1 and a preloaded node image"
  (cd "$target" && vcluster node load-image "$node" --image "$image")
fi

client_token="$(openssl rand -hex 24)"
admin_token="$(openssl rand -hex 24)"
tail_token="$(openssl rand -hex 24)"
peer_tokens="$(jq -cn \
  --arg first "$(openssl rand -hex 24)" \
  --arg second "$(openssl rand -hex 24)" \
  --arg third "$(openssl rand -hex 24)" \
  '[$first, $second, $third]')"
[ "$(jq 'unique | length' <<< "$peer_tokens")" = 3 ] || die "peer tokens must be unique"
k create secret generic rhiza-auth \
  --from-literal=client-token="$client_token" \
  --from-literal=admin-token="$admin_token" \
  --from-literal=tail-token="$tail_token" >/dev/null

inject_chaos_labels() {
  [ -n "$chaos_run_id" ] || return 0
  export CHAOS_RUN_ID="$chaos_run_id" CHAOS_ROLE="$2"
  yq eval --inplace '
    with(select(.kind == strenv(CHAOS_KIND));
      .spec.template.metadata.labels["app.kubernetes.io/part-of"] = "rhiza-chaos" |
      .spec.template.metadata.labels["chaos.rhiza.io/run"] = strenv(CHAOS_RUN_ID) |
      .spec.template.metadata.labels["chaos.rhiza.io/role"] = strenv(CHAOS_ROLE)
    )
  ' "$1"
}

sed -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
  deploy/k8s/rustfs-e2e.yaml > "$target/rustfs.yaml"
export CHAOS_KIND=Deployment
inject_chaos_labels "$target/rustfs.yaml" object-store
yq eval '.' "$target/rustfs.yaml" >/dev/null
k apply -f "$target/rustfs.yaml" >/dev/null
k rollout status deployment/rustfs --timeout=240s >/dev/null
k wait --for=condition=complete job/rustfs-create-bucket --timeout=240s >/dev/null
rustfs_uid="$(k get pod -l app.kubernetes.io/name=rustfs -o jsonpath='{.items[0].metadata.uid}')"
[ -n "$rustfs_uid" ] || die "cannot capture RustFS pod UID"
[ -z "$(k get persistentvolumeclaims -o name)" ] || die "vind E2E must not create PVCs"
fresh_capture_empty_bucket_inventory
fresh_assert_prebootstrap_absence

make_bundle() {
  id="$1" output="$2" name="rhiza-${profile}-c${id}"
  jq -n --argjson id "$id" --argjson tokens "$peer_tokens" --arg name "$name" \
    --arg recorder_transport "${RHIZA_RECORDER_TRANSPORT:-http}" \
    --arg recorder_tls "${RHIZA_RECORDER_TLS:-off}" '
    {config_id:$id, members:[range(3) as $n | {
      node_id:("node-" + ($n + 1 | tostring)),
      url:("http://" + $name + "-" + ($n|tostring) + "." + $name + ":8081"),
      log_url:("http://" + $name + "-" + ($n|tostring) + "." + $name + ":8080"),
      token:$tokens[$n]
    } + (if $recorder_transport != "http" then {
      recorder_tcp_addr:($name + "-" + ($n|tostring) + "." + $name + ":8082")
    } else {} end) + (if $recorder_tls == "on" then {
      recorder_tls_server_name:($name + "-" + ($n|tostring) + "." + $name)
    } else {} end)]}
  ' > "$output"
}
make_bundle 1 "$target/config-c1.json"
make_bundle 2 "$target/config-c2-draft.json"
name_c1="rhiza-${profile}-c1"
name_c2="rhiza-${profile}-c2"
client_service="rhiza-${profile}-client"
if [ "$profile" = sql ]; then
  matrix_http_default_target="$client_service"
else
  matrix_http_default_target="${name_c1}-0.${name_c1}"
fi
matrix_http_target="$matrix_http_default_target"
jq -e '[.members[].token] | unique | length == 3' \
  "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
jq -se '(.[0].members | map(.token)) == (.[1].members | map(.token))' \
  "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
k create secret generic "${name_c1}-bundle" --from-file=config.json="$target/config-c1.json" \
  --dry-run=client -o yaml | yq eval '.immutable = true' - | k create -f - >/dev/null
k apply -f deploy/k8s/rhiza-client-services.yaml >/dev/null

export RHIZA_IMAGE="$image" RHIZA_KUBE_CONTEXT="$context" RHIZA_K8S_NAMESPACE="$namespace"
export RHIZA_CLUSTER_ID="$logical_cluster_id" RHIZA_RECOVERY_GENERATION=1
export RHIZA_CHECKPOINT_LEASE_MS=5000
export RHIZA_S3_ENDPOINT=http://rustfs:9000 RHIZA_OBJECT_SECRET=rustfs-credentials
export RHIZA_S3_ALLOW_HTTP=true

echo "== initialize object checkpoint and bootstrap config 1 =="
scripts/k8s-object-job.sh 1 "$target/config-c1.json" init-checkpoint >/dev/null
RHIZA_STARTUP_MODE=rejoin scripts/render-k8s-config.sh \
  1 3 "$target/config-c1.json" "$target/config-c1.yaml"
export CHAOS_KIND=StatefulSet
inject_chaos_labels "$target/config-c1.yaml" voter
inject_marker_helper "$target/config-c1.yaml"
k create -f "$target/config-c1.yaml" >/dev/null
"$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c1" 3 1
verify_marker_helper "$name_c1"
fresh_capture_live_image_provenance

client() {
  local pod="$1"
  shift
  k exec "$pod" -- rhiza "$@" --url http://127.0.0.1:8080
}
client_http() {
  pod="$1" path="$2" body="$3"
  request_id="$(date +%s)-$$-${RANDOM}"
  job="rhiza-${profile}-client-${request_id}"
  manifest="$target/${job}.yaml"
  response="$target/${job}.response"
  sed \
    -e "s|__JOB_NAME__|$job|g" \
    -e "s|__EXECUTION_PROFILE__|$profile|g" \
    -e 's|__CURL_IMAGE__|curlimages/curl:8.10.1|g' \
    -e 's|__METHOD__|POST|g' \
    -e 's|__BODY__|{}|g' \
    -e 's|__POD__|pod|g' \
    -e 's|__SERVICE__|service|g' \
    -e 's|__PATH__|/|g' \
    -e 's|__AUTH_SECRET__|rhiza-auth|g' \
    deploy/k8s/rhiza-admin-job.yaml > "$manifest"
  export RHIZA_E2E_HTTP_POD="$pod" RHIZA_E2E_HTTP_SERVICE="${pod%-*}"
  export RHIZA_E2E_HTTP_PATH="$path" RHIZA_E2E_HTTP_BODY="$body"
  # shellcheck disable=SC2016
  export RHIZA_E2E_HTTP_COMMAND='exec curl --fail-with-body --silent --show-error \
    --connect-timeout 5 --max-time 90 -X POST \
    -H "Authorization: Bearer ${RHIZA_ADMIN_TOKEN}" \
    -H "x-rhiza-version: 1" -H "Content-Type: application/json" \
    --data "$RHIZA_E2E_HTTP_BODY" \
    "http://${RHIZA_E2E_HTTP_POD}.${RHIZA_E2E_HTTP_SERVICE}:8080${RHIZA_E2E_HTTP_PATH}"'
  yq eval --inplace '
    .spec.template.spec.containers[0].args[0] = strenv(RHIZA_E2E_HTTP_COMMAND) |
    (.spec.template.spec.containers[0].env[] |
      select(.name == "RHIZA_ADMIN_TOKEN").valueFrom.secretKeyRef.key) = "client-token" |
    .spec.template.spec.containers[0].env += [
      {"name":"RHIZA_E2E_HTTP_POD", "value":strenv(RHIZA_E2E_HTTP_POD)},
      {"name":"RHIZA_E2E_HTTP_SERVICE", "value":strenv(RHIZA_E2E_HTTP_SERVICE)},
      {"name":"RHIZA_E2E_HTTP_PATH", "value":strenv(RHIZA_E2E_HTTP_PATH)},
      {"name":"RHIZA_E2E_HTTP_BODY", "value":strenv(RHIZA_E2E_HTTP_BODY)}]
  ' "$manifest"
  k create -f "$manifest" >/dev/null
  if ! k wait --for=condition=complete "job/$job" --timeout=120s >/dev/null; then
    k logs "job/$job" >&2 || true
    return 1
  fi
  k logs "job/$job" > "$response"
  jq -e -s 'length == 1' "$response" >/dev/null
  cat "$response"
}
matrix_service_http() {
  local path="$1" body="$2"
  local request_id job manifest response raw_response succeeded failed attempt
  request_id="$(date +%s)-$$-${RANDOM}"
  job="rhiza-${profile}-matrix-${request_id}"
  matrix_last_job="$job"
  manifest="$target/${job}.yaml"
  response="$target/${job}.response"
  raw_response="${response}.raw"
  matrix_last_http_status=""
  matrix_last_http_body="$response"
  matrix_last_http_original_rc=1
  sed \
    -e "s|__JOB_NAME__|$job|g" \
    -e "s|__EXECUTION_PROFILE__|$profile|g" \
    -e 's|__CURL_IMAGE__|curlimages/curl:8.10.1|g' \
    -e 's|__METHOD__|POST|g' \
    -e 's|__BODY__|{}|g' \
    -e 's|__POD__|pod|g' \
    -e 's|__SERVICE__|service|g' \
    -e 's|__PATH__|/|g' \
    -e 's|__AUTH_SECRET__|rhiza-auth|g' \
    deploy/k8s/rhiza-admin-job.yaml > "$manifest"
  export RHIZA_E2E_HTTP_SERVICE="$matrix_http_target"
  matrix_last_http_target="$RHIZA_E2E_HTTP_SERVICE"
  matrix_last_http_raw="$raw_response"
  export RHIZA_E2E_HTTP_PATH="$path" RHIZA_E2E_HTTP_BODY="$body"
  # shellcheck disable=SC2016
  export RHIZA_E2E_HTTP_COMMAND='http_status="$(curl --silent --show-error \
    --connect-timeout 2 --max-time 8 -o /tmp/rhiza-http-body -w "%{http_code}" -X POST \
    -H "Authorization: Bearer ${RHIZA_ADMIN_TOKEN}" \
    -H "x-rhiza-version: 1" -H "Content-Type: application/json" \
    --data "$RHIZA_E2E_HTTP_BODY" \
    "http://${RHIZA_E2E_HTTP_SERVICE}:8080${RHIZA_E2E_HTTP_PATH}")" \
    && cat /tmp/rhiza-http-body \
    && printf "\n__RHIZA_HTTP_STATUS__=%s\n" "$http_status"'
  yq eval --inplace '
    .spec.activeDeadlineSeconds = 12 |
    .spec.template.spec.containers[0].args[0] = strenv(RHIZA_E2E_HTTP_COMMAND) |
    (.spec.template.spec.containers[0].env[] |
      select(.name == "RHIZA_ADMIN_TOKEN").valueFrom.secretKeyRef.key) = "client-token" |
    .spec.template.spec.containers[0].env += [
      {"name":"RHIZA_E2E_HTTP_SERVICE", "value":strenv(RHIZA_E2E_HTTP_SERVICE)},
      {"name":"RHIZA_E2E_HTTP_PATH", "value":strenv(RHIZA_E2E_HTTP_PATH)},
      {"name":"RHIZA_E2E_HTTP_BODY", "value":strenv(RHIZA_E2E_HTTP_BODY)}]
  ' "$manifest"
  k create -f "$manifest" >/dev/null
  for ((attempt=1; attempt<=15; attempt++)); do
    succeeded="$(k get "job/$job" -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)"
    failed="$(k get "job/$job" -o 'jsonpath={.status.failed}' 2>/dev/null || true)"
    if [ "$succeeded" = 1 ]; then
      k logs "job/$job" > "$raw_response"
      matrix_last_http_status="$(sed -n 's/^__RHIZA_HTTP_STATUS__=//p' "$raw_response" | tail -1)"
      case "$matrix_last_http_status" in
        [1-5][0-9][0-9]) ;;
        *) return 1 ;;
      esac
      sed '$d' "$raw_response" > "$response"
      jq -e -s 'length == 1' "$response" >/dev/null
      case "$matrix_last_http_status" in
        2[0-9][0-9]) matrix_last_http_original_rc=0; cat "$response"; return 0 ;;
        *) return 1 ;;
      esac
    fi
    if [ "${failed:-0}" -gt 0 ]; then
      k logs "job/$job" > "$raw_response" 2>&1 || true
      return 1
    fi
    sleep 1
  done
  k logs "job/$job" > "$raw_response" 2>&1 || true
  return 1
}
matrix_persist_safety_observation() {
  local sample_dir="$1" operation="$2" classification="$3" started_at="$4" finished_at="$5" requested_at="$6"
  local stdout_file="$sample_dir/${operation}.stdout" stderr_file="$sample_dir/${operation}.stderr"
  local metadata="$sample_dir/${operation}.json"
  [ -f "$matrix_last_http_body" ] && cp "$matrix_last_http_body" "$stdout_file" || : > "$stdout_file"
  [ -f "${matrix_last_http_raw:-}" ] && cp "$matrix_last_http_raw" "$stderr_file" || : > "$stderr_file"
  jq -cn --arg operation "$operation" --arg classification "$classification" \
    --arg requested_at "$requested_at" --arg started_at "$started_at" --arg finished_at "$finished_at" \
    --arg http_status "${matrix_last_http_status:-}" \
    --arg stdout_path "$stdout_file" --arg stderr_path "$stderr_file" \
    --argjson original_rc "${matrix_last_http_original_rc:-1}" \
    '{operation:$operation,classification:$classification,requested_at:$requested_at,started_at:$started_at,
      finished_at:$finished_at,http_status:(if $http_status == "" then null else $http_status end),
      original_rc:$original_rc,stdout_path:$stdout_path,stderr_path:$stderr_path}' > "$metadata"
}
matrix_persist_local_safety_observation() {
  local sample_dir="$1" classification="$2" started_at="$3" finished_at="$4" original_rc="$5" requested_at="$6"
  local stdout_file="$sample_dir/survivor-local-read.stdout" stderr_file="$sample_dir/survivor-local-read.stderr"
  jq -cn --arg operation survivor_local_read --arg classification "$classification" \
    --arg requested_at "$requested_at" --arg started_at "$started_at" --arg finished_at "$finished_at" \
    --arg stdout_path "$stdout_file" --arg stderr_path "$stderr_file" --argjson original_rc "$original_rc" \
    '{operation:$operation,classification:$classification,requested_at:$requested_at,started_at:$started_at,finished_at:$finished_at,
      http_status:null,original_rc:$original_rc,stdout_path:$stdout_path,stderr_path:$stderr_path}' \
    > "$sample_dir/survivor-local-read.json"
}
matrix_run_no_quorum_safety_probe() {
  local sequence="$1" requested_epoch="$2" sample_dir request_id key value started_at finished_at requested_at sample_started_at
  local write_classification read_classification local_stdout local_stderr local_rc
  sample_dir="$target/failure-safety-probes/${cell_id}/$(printf '%04d' "$sequence")"
  mkdir -p "$sample_dir"
  if [ "$sequence" = 0 ]; then
    # Preserve the original no-quorum receipt so the post-recovery retry still
    # proves the idempotency boundary for the same request ID.
    request_id="$fault_request_id"
    key="$fault_key"
    value="$fault_value"
  else
    request_id="failure-safety-${cell_id}-${sequence}-${run_id}"
    key="matrix-failure-safety-${cell_id}-${sequence}-${run_id}"
    value="must-not-commit-${cell_id}-${sequence}"
  fi

  requested_at="$(format_epoch_utc "$requested_epoch")"
  sample_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  started_at="$sample_started_at"
  if [ "$cell_failed" = 2 ]; then
    matrix_prepare_write_request "$key" "$value" "$request_id"
    matrix_http_target="${name_c1}-0.${name_c1}"
    if matrix_service_http "$matrix_path" "$matrix_body" >/dev/null; then
      unset matrix_http_target
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      matrix_persist_safety_observation "$sample_dir" write success "$started_at" "$finished_at" "$requested_at"
      return 1
    fi
    unset matrix_http_target
    if matrix_expect_write_no_quorum; then
      write_classification="$(matrix_last_http_failure_detail)"
    else
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      matrix_persist_safety_observation "$sample_dir" write unexpected_failure "$started_at" "$finished_at" "$requested_at"
      return 1
    fi
    finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    matrix_persist_safety_observation "$sample_dir" write "$write_classification" "$started_at" "$finished_at" "$requested_at"

    started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local_stdout="$sample_dir/survivor-local-read.stdout"
    local_stderr="$sample_dir/survivor-local-read.stderr"
    if read_value_consistency "${name_c1}-0" suffix replayed local > "$local_stdout" 2> "$local_stderr"; then
      local_rc=0
    else
      local_rc=$?
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      matrix_persist_local_safety_observation "$sample_dir" failure "$started_at" "$finished_at" "$local_rc" "$requested_at"
      return 1
    fi
    finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    matrix_persist_local_safety_observation "$sample_dir" success "$started_at" "$finished_at" "$local_rc" "$requested_at"

    started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if matrix_expect_read_quorum_unavailable "$key" "$value" read_barrier; then
      read_classification="$matrix_last_read_failure_kind"
    else
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      matrix_persist_safety_observation "$sample_dir" read unexpected_failure "$started_at" "$finished_at" "$requested_at"
      return 1
    fi
  else
    matrix_prepare_write_request "$key" "$value" "$request_id"
    if matrix_expect_zero_endpoint_transport_failure "$matrix_path" "$matrix_body"; then
      write_classification=zero_endpoint_transport
    else
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      if [ "${matrix_last_http_original_rc:-1}" = 0 ]; then
        matrix_persist_safety_observation "$sample_dir" write success "$started_at" "$finished_at" "$requested_at"
      else
        matrix_persist_safety_observation "$sample_dir" write unexpected_failure "$started_at" "$finished_at" "$requested_at"
      fi
      return 1
    fi
    finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    matrix_persist_safety_observation "$sample_dir" write "$write_classification" "$started_at" "$finished_at" "$requested_at"

    started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if matrix_expect_read_zero_endpoint_failure "$key" "$value" read_barrier; then
      read_classification=zero_endpoint_transport
    else
      finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      matrix_persist_safety_observation "$sample_dir" read unexpected_failure "$started_at" "$finished_at" "$requested_at"
      return 1
    fi
  fi
  finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  matrix_persist_safety_observation "$sample_dir" read "$read_classification" "$started_at" "$finished_at" "$requested_at"
  cell_failure_safety_probes="$(jq -cn --argjson current "$cell_failure_safety_probes" \
    --arg request_id "$request_id" --arg sample_dir "$sample_dir" --arg write "$write_classification" \
    --arg read "$read_classification" --arg requested_at "$requested_at" \
    --arg actual_started_at "$sample_started_at" --arg actual_finished_at "$finished_at" --argjson sequence "$sequence" \
    '$current + [{sequence:$sequence,request_id:$request_id,artifact_dir:$sample_dir,
      requested_at:$requested_at,actual_started_at:$actual_started_at,actual_finished_at:$actual_finished_at,
      write_classification:$write,read_classification:$read}]')"
}
matrix_prepare_write_request() {
  local key="$1" value="$2" request_id="$3"
  case "$profile" in
    sql)
      matrix_body="$(jq -cn --arg request_id "$request_id" --arg key "$key" --arg value "$value" \
        '{request_id:$request_id,key:$key,value:$value}')"
      matrix_path=/v1/write
      ;;
    graph)
      matrix_body="$(jq -cn --arg request_id "$request_id" --arg id "$key" --arg value "$value" \
        '{request_id:$request_id,id:$id,value:{type:"string",value:$value}}')"
      matrix_path=/v1/graph/documents/put
      ;;
    kv)
      matrix_body="$(jq -cn --arg request_id "$request_id" \
        --arg key "$(profile_key "$key")" --arg value "$(profile_key "$value")" \
        '{request_id:$request_id,key:$key,value:$value}')"
      matrix_path=/v1/kv/put
      ;;
  esac
}
matrix_response_is_ambiguous_mutation() {
  local status="$1" body="$2"
  [ "$status" = 503 ] &&
    [ -f "$body" ] &&
    [ "$(wc -c < "$body")" -le 65536 ] &&
    jq -e -s 'length == 1 and (.[0] | type == "object" and
      .code == "ambiguous_mutation" and .retryable == true)' "$body" >/dev/null 2>&1
}
matrix_service_write_response() {
  matrix_prepare_write_request "$1" "$2" "$3"
  matrix_service_mutation_response "$matrix_path" "$matrix_body"
}
matrix_service_mutation_response() {
  local path="$1" body="$2" first_status first_body
  if matrix_service_http "$path" "$body"; then
    return 0
  fi
  first_status="$matrix_last_http_status"
  first_body="$matrix_last_http_body"
  matrix_response_is_ambiguous_mutation "$first_status" "$first_body" || return 1
  if matrix_service_http "$path" "$body"; then
    return 0
  fi
  return 75
}
matrix_service_write() {
  matrix_service_write_response "$1" "$2" "$3" >/dev/null
}
matrix_prepare_read_request() {
  local key="$1" expected="$2" consistency="$3"
  case "$profile" in
    sql)
      matrix_body="$(jq -cn --arg key "$key" --arg consistency "$consistency" \
        '{key:$key,consistency:$consistency}')"
      matrix_path=/v1/read
      # shellcheck disable=SC2016
      matrix_read_filter='.value == $expected'
      matrix_encoded_expected="$expected"
      ;;
    graph)
      matrix_body="$(jq -cn --arg id "$key" --arg consistency "$consistency" \
        '{id:$id,consistency:$consistency}')"
      matrix_path=/v1/graph/documents/get
      # shellcheck disable=SC2016
      matrix_read_filter='.value == {type:"string",value:$expected} and (.applied_index | type == "number") and (.hash | type == "array" and length == 32)'
      matrix_encoded_expected="$expected"
      ;;
    kv)
      matrix_body="$(jq -cn --arg key "$(profile_key "$key")" --arg consistency "$consistency" \
        '{key:$key,consistency:$consistency}')"
      matrix_path=/v1/kv/get
      # shellcheck disable=SC2016
      matrix_read_filter='.value == $expected and (.applied_index | type == "number") and (.hash | type == "array" and length == 32)'
      matrix_encoded_expected="$(profile_key "$expected")"
      ;;
  esac
}
matrix_service_read() {
  local matrix_read_response
  matrix_prepare_read_request "$1" "$2" "$3"
  if ! matrix_read_response="$(matrix_service_http "$matrix_path" "$matrix_body")"; then
    return 1
  fi
  jq -e --arg expected "$matrix_encoded_expected" "$matrix_read_filter" \
    <<< "$matrix_read_response" >/dev/null
}
matrix_expect_write_no_quorum() {
  [ "$matrix_last_http_status" = 503 ] || return 1
  jq -e '.retryable == true and
    (.code == "write_timeout" or .code == "write_outcome_unknown" or
     .code == "ambiguous_mutation" or .code == "unavailable")' \
    "$matrix_last_http_body" >/dev/null
}
matrix_last_http_failure_detail() {
  local code retryable
  code="$(jq -r 'if (.code | type) == "string" then .code else "missing_code" end' \
    "$matrix_last_http_body" 2>/dev/null || printf 'invalid_body')"
  retryable="$(jq -r 'if (.retryable | type) == "boolean" then (.retryable | tostring) else "missing" end' \
    "$matrix_last_http_body" 2>/dev/null || printf 'invalid')"
  printf 'http_%s_%s_retryable_%s\n' "${matrix_last_http_status:-missing}" "$code" "$retryable"
}
matrix_expect_read_barrier_unavailable() {
  [ "$matrix_last_http_status" = 503 ] || return 1
  jq -e '.code == "unavailable" and .retryable == true' \
    "$matrix_last_http_body" >/dev/null
}
matrix_expect_f2_read_barrier_timeout() {
  local survivor="${name_c1}-0" survivor_ready endpoint_service endpoint_count exit_code
  [ -z "$matrix_last_http_status" ] || return 1
  [ "$matrix_last_http_target" = "${survivor}.${name_c1}" ] || return 1
  survivor_ready="$(k get pod "$survivor" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}')" || return 1
  [ "$survivor_ready" = True ] || return 1
  if [ "$profile" = sql ]; then
    endpoint_service="$client_service"
  else
    endpoint_service="$name_c1"
  fi
  endpoint_count="$(k get endpoints "$endpoint_service" -o json |
    jq --arg survivor "$survivor" '
      [.subsets[]?.addresses[]?] as $addresses |
      if ($addresses | length) == 1 and $addresses[0].targetRef.name == $survivor
      then 1 else -1 end')" || return 1
  [ "$endpoint_count" = 1 ] || return 1
  exit_code="$(k get pod -l "job-name=$matrix_last_job" -o json |
    jq -er 'if (.items | length) == 1 then
      .items[0].status.containerStatuses[0].state.terminated.exitCode else empty end')" \
    || return 1
  case "$exit_code" in 28) ;; *) return 1;; esac
  grep -Eq '^curl: \(28\) Operation timed out after [0-9]+ milliseconds with 0 bytes received$' \
    "$matrix_last_http_raw"
}
matrix_expect_write_quorum_unavailable() {
  matrix_prepare_write_request "$1" "$2" "$3"
  matrix_http_target="${name_c1}-0.${name_c1}"
  if matrix_service_http "$matrix_path" "$matrix_body" >/dev/null; then
    unset matrix_http_target
    cell_write_actual=success
    cell_write_actual_detail=success
    return 1
  fi
  unset matrix_http_target
  cell_write_actual=failure
  cell_write_actual_detail="$(matrix_last_http_failure_detail)"
  matrix_expect_write_no_quorum
}
matrix_expect_read_quorum_unavailable() {
  matrix_prepare_read_request "$1" "$2" "$3"
  matrix_http_target="${name_c1}-0.${name_c1}"
  if matrix_service_http "$matrix_path" "$matrix_body" >/dev/null; then
    unset matrix_http_target
    return 1
  fi
  matrix_http_target="$matrix_http_default_target"
  if matrix_expect_read_barrier_unavailable; then
    matrix_last_read_failure_kind=unavailable
    return 0
  fi
  if matrix_expect_f2_read_barrier_timeout; then
    matrix_last_read_failure_kind=timeout
    return 0
  fi
  return 1
}
matrix_expect_zero_endpoint_transport_failure() {
  local path="$1" body="$2" endpoint_count exit_code attempt
  if [ "$profile" = sql ]; then
    endpoint_count=-1
    for ((attempt=1; attempt<=15; attempt++)); do
      endpoint_count="$(k get endpoints "$client_service" -o json |
        jq '[.subsets[]?.addresses[]?] | length')" || return 1
      [ "$endpoint_count" != 0 ] || break
      sleep 1
    done
    [ "$endpoint_count" = 0 ] || return 1
  else
    [ "$matrix_http_target" = "${name_c1}-0.${name_c1}" ] || return 1
    endpoint_count="$(k get statefulset "$name_c1" -o jsonpath='{.spec.replicas}')" || return 1
    [ "$endpoint_count" = 0 ] || return 1
    matrix_wait_pod_absent "${name_c1}-0" || return 1
  fi
  if matrix_service_http "$path" "$body" >/dev/null; then
    return 1
  fi
  [ -z "$matrix_last_http_status" ] || return 1
  exit_code="$(k get pod -l "job-name=$matrix_last_job" -o json |
    jq -er 'if (.items | length) == 1 then
      .items[0].status.containerStatuses[0].state.terminated.exitCode else empty end')" \
    || return 1
  if [ "$profile" = sql ]; then
    case "$exit_code" in 7|28) return 0;; *) return 1;; esac
  fi
  [ "$exit_code" = 6 ] || return 1
  grep -Eq "^curl: \(6\) Could not resolve host: ${name_c1}-0\\.${name_c1}$" \
    "$matrix_last_http_raw"
}
matrix_expect_write_zero_endpoint_failure() {
  matrix_prepare_write_request "$1" "$2" "$3"
  if matrix_expect_zero_endpoint_transport_failure "$matrix_path" "$matrix_body"; then
    cell_write_actual=failure
    cell_write_actual_detail=zero_endpoint_transport
    return 0
  fi
  if [ -n "$matrix_last_http_status" ]; then
    cell_write_actual=failure
    cell_write_actual_detail="$(matrix_last_http_failure_detail)"
  else
    cell_write_actual=unexpected_transport
    cell_write_actual_detail=unexpected_transport
  fi
  return 1
}
matrix_expect_read_zero_endpoint_failure() {
  matrix_prepare_read_request "$1" "$2" "$3"
  matrix_expect_zero_endpoint_transport_failure "$matrix_path" "$matrix_body"
}
retryable_write_failure() {
  local attempt_log="$1"
  grep -Eq \
    '^(write|graph put-document|kv put) failed: HTTP 503 Service Unavailable code=(write_timeout|write_outcome_unknown|ambiguous_mutation|unavailable|writes_unavailable)( |$)' \
    "$attempt_log"
}
profile_key() {
  case "$profile" in
    kv) printf '%s' "$1" | openssl base64 -A ;;
    *) printf '%s' "$1" ;;
  esac
}
profile_graph_value() {
  jq -cn --arg value "$1" '{type:"string",value:$value}'
}
profile_put() {
  local pod="$1" key="$2" value="$3" request_id="$4"
  case "$profile" in
    sql) client "$pod" write --request-id "$request_id" --key "$key" --value "$value" ;;
    graph)
      client "$pod" graph put-document --request-id "$request_id" --id "$key" \
        --value-json "$(profile_graph_value "$value")"
      ;;
    kv)
      client "$pod" kv put --request-id "$request_id" \
        --key-base64 "$(profile_key "$key")" --value-base64 "$(profile_key "$value")"
      ;;
  esac
}
profile_get() {
  local pod="$1" key="$2" consistency="$3" expected="${4-}"
  case "$profile" in
    sql) client "$pod" read --key "$key" --consistency "$consistency" --expect "$expected" ;;
    graph) client "$pod" graph get-document --id "$key" --consistency "$consistency" ;;
    kv) client "$pod" kv get --key-base64 "$(profile_key "$key")" --consistency "$consistency" ;;
  esac
}
profile_delete() {
  local pod="$1" key="$2" request_id="$3"
  case "$profile" in
    graph) client "$pod" graph delete-document --request-id "$request_id" --id "$key" ;;
    kv) client "$pod" kv delete --request-id "$request_id" --key-base64 "$(profile_key "$key")" ;;
    *) die "profile_delete is only valid for graph|kv" ;;
  esac
}
matrix_prepare_delete_request() {
  local key="$1" request_id="$2"
  case "$profile" in
    graph)
      matrix_body="$(jq -cn --arg request_id "$request_id" --arg id "$key" '{request_id:$request_id,id:$id}')"
      matrix_path=/v1/graph/documents/delete
      ;;
    kv)
      matrix_body="$(jq -cn --arg request_id "$request_id" --arg key "$(profile_key "$key")" '{request_id:$request_id,key:$key}')"
      matrix_path=/v1/kv/delete
      ;;
    *) die "matrix_prepare_delete_request is only valid for graph|kv" ;;
  esac
}
profile_response_has_value() {
  local response="$1" value="$2"
  case "$profile" in
    sql) return 0 ;;
    graph) jq -e --arg value "$value" '.value == {type:"string",value:$value} and (.applied_index | type == "number") and (.hash | type == "array" and length == 32)' <<< "$response" >/dev/null ;;
    kv) jq -e --arg value "$(profile_key "$value")" '.value == $value and (.applied_index | type == "number") and (.hash | type == "array" and length == 32)' <<< "$response" >/dev/null ;;
  esac
}
write_value() {
  local pod="$1" key="$2" value="$3" request_id="$4"
  local attempt deadline attempt_log write_attempt_dir
  deadline=$((SECONDS + write_retry_deadline_seconds))
  write_attempt_dir="$target/write-attempts"
  mkdir -p "$write_attempt_dir"

  for ((attempt=1; attempt<=60; attempt++)); do
    attempt_log="$(mktemp "$write_attempt_dir/write.XXXXXX")"
    if profile_put "$pod" "$key" "$value" "$request_id" 2> "$attempt_log"; then
      return 0
    fi
    if ! retryable_write_failure "$attempt_log"; then
      cat "$attempt_log" >&2
      return 1
    fi
    if [ "$attempt" -eq 60 ] || [ "$SECONDS" -ge "$deadline" ]; then
      echo "write did not converge after retryable failures (request_id=$request_id, attempts=$attempt)" >&2
      cat "$attempt_log" >&2
      return 1
    fi
    echo "retrying write after retryable failure (request_id=$request_id, attempt=$attempt, stderr=$attempt_log)" >&2
    sleep 1
  done
}
read_value_consistency() {
  pod="$1" key="$2" expected="$3" consistency="$4"
  response="$(profile_get "$pod" "$key" "$consistency" "$expected")" || return 1
  profile_response_has_value "$response" "$expected"
}
read_value() {
  read_value_consistency "$1" "$2" "$3" read_barrier
}
retry_read_value() {
  pod="$1" key="$2" expected="$3"
  for ((attempt=1; attempt<=60; attempt++)); do
    if read_value "$pod" "$key" "$expected" >/dev/null 2>&1; then
      return 0
    fi
    [ "$attempt" -lt 60 ] || return 1
    sleep 1
  done
}
verify_profile_mutation_contract() {
  local pod="$1" key="profile-contract-${run_id}" value="value with spaces and \"quotes\"" \
    altered="different payload" put_id="profile-put-${run_id}" delete_id="profile-delete-${run_id}"
  local first second get_response delete_first delete_second missing delete_operation
  [ "$profile" = sql ] && return 0
  case "$profile" in graph) delete_operation=delete_document;; kv) delete_operation=delete;; esac

  matrix_prepare_write_request "$key" "$value" "$put_id"
  first="$(matrix_service_mutation_response "$matrix_path" "$matrix_body")"
  jq -e '(.applied_index | type == "number") and (.hash | type == "array" and length == 32)' <<< "$first" >/dev/null
  second="$(profile_put "$pod" "$key" "$value" "$put_id")"
  jq -e --argjson first "$first" '$first == .' <<< "$second" >/dev/null \
    || die "exact profile put replay changed its receipt"
  matrix_prepare_write_request "$key" "$altered" "$put_id"
  if matrix_service_http "$matrix_path" "$matrix_body" > /dev/null; then
    die "same request_id with different profile put payload was accepted"
  fi
  if [ "$matrix_last_http_status" != 409 ] ||
    ! jq -e '.code == "request_conflict" and .retryable == false' \
      "$matrix_last_http_body" >/dev/null; then
    die "profile put conflict was not non-retryable request_conflict"
  fi
  get_response="$(profile_get "$pod" "$key" read_barrier)"
  profile_response_has_value "$get_response" "$value" || {
    die "profile read_barrier did not return the committed value and lineage"
  }

  matrix_prepare_delete_request "$key" "$delete_id"
  delete_first="$(matrix_service_mutation_response "$matrix_path" "$matrix_body")"
  jq -e --arg operation "$delete_operation" \
    '(.applied_index | type == "number") and (.hash | type == "array" and length == 32) and
    .result.operation == $operation and .result.existed == true' \
    <<< "$delete_first" >/dev/null \
    || die "profile delete did not report an existing document/key"
  delete_second="$(profile_delete "$pod" "$key" "$delete_id")"
  jq -e --argjson first "$delete_first" '$first == .' <<< "$delete_second" >/dev/null \
    || die "exact profile delete replay changed its receipt or advanced the tip"
  get_response="$(profile_get "$pod" "$key" read_barrier)"
  jq -e '.value == null and (.applied_index | type == "number") and (.hash | type == "array" and length == 32)' \
    <<< "$get_response" >/dev/null || die "profile delete remained visible after a read_barrier"
  matrix_prepare_delete_request "missing-${key}" "profile-missing-delete-${run_id}"
  missing="$(matrix_service_mutation_response "$matrix_path" "$matrix_body")"
  jq -e --arg operation "$delete_operation" \
    '.result.operation == $operation and .result.existed == false' <<< "$missing" >/dev/null \
    || die "profile missing delete did not report existed=false"
}
verify_same_membership_pod_recreation() {
  local target_pod survivor_a survivor_b
  local old_target_uid old_survivor_a_uid old_survivor_b_uid new_target_uid
  local old_generation old_replicas expected_digest statuses ordinal status_file
  local sample_complete
  target_pod="${name_c2}-1"
  survivor_a="${name_c2}-0"
  survivor_b="${name_c2}-2"

  for ordinal in 0 1 2; do
    marker_seed "${name_c2}-$ordinal"
  done
  old_target_uid="$(k get pod "$target_pod" -o jsonpath='{.metadata.uid}')"
  old_survivor_a_uid="$(k get pod "$survivor_a" -o jsonpath='{.metadata.uid}')"
  old_survivor_b_uid="$(k get pod "$survivor_b" -o jsonpath='{.metadata.uid}')"
  old_generation="$(k get statefulset "$name_c2" -o jsonpath='{.metadata.generation}')"
  old_replicas="$(k get statefulset "$name_c2" -o jsonpath='{.spec.replicas}')"
  scripts/k8s-admin-job.sh "$name_c2" "$target_pod" GET \
    /v1/admin/membership/status > "$target/pre-pod-recreation-status.json"
  expected_digest="$(jq -c '.node.active_membership_digest' \
    "$target/pre-pod-recreation-status.json")"

  # BEGIN same-membership automatic Pod recreation: no scale, config, or recovery command.
  k delete pod "$target_pod" --wait=true >/dev/null
  "$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c2" 3 2
  # END same-membership automatic Pod recreation.

  new_target_uid="$(k get pod "$target_pod" -o jsonpath='{.metadata.uid}')"
  if [ -z "$new_target_uid" ] || [ "$new_target_uid" = "$old_target_uid" ]; then
    die "StatefulSet did not recreate the deleted ordinal with a new Pod UID"
  fi
  [ "$(k get pod "$survivor_a" -o jsonpath='{.metadata.uid}')" = "$old_survivor_a_uid" ] \
    || die "first survivor Pod was replaced during one-Pod recovery"
  [ "$(k get pod "$survivor_b" -o jsonpath='{.metadata.uid}')" = "$old_survivor_b_uid" ] \
    || die "second survivor Pod was replaced during one-Pod recovery"
  marker_absent "$target_pod" \
    || die "replacement Pod retained deleted emptyDir data"
  marker_present "$survivor_a" \
    || die "first survivor lost its emptyDir data"
  marker_present "$survivor_b" \
    || die "second survivor lost its emptyDir data"
  [ "$(k get statefulset "$name_c2" -o jsonpath='{.metadata.generation}')" = "$old_generation" ] \
    || die "StatefulSet configuration changed during automatic Pod recovery"
  [ "$(k get statefulset "$name_c2" -o jsonpath='{.spec.replicas}')" = "$old_replicas" ] \
    || die "StatefulSet replica count changed during automatic Pod recovery"

  retry_read_value "$target_pod" generation two
  for ((attempt=1; attempt<=60; attempt++)); do
    sample_complete=true
    for ordinal in 0 1 2; do
      status_file="$target/pod-recreation-status-${ordinal}.json"
      if ! scripts/k8s-admin-job.sh "$name_c2" "${name_c2}-$ordinal" GET \
        /v1/admin/membership/status > "$status_file"; then
        sample_complete=false
        break
      fi
    done
    if "$sample_complete" &&
      statuses="$(jq -s '.' \
        "$target/pod-recreation-status-0.json" \
        "$target/pod-recreation-status-1.json" \
        "$target/pod-recreation-status-2.json")" &&
      jq -e --arg cluster "$canonical_cluster_id" --arg profile "$profile" --argjson digest "$expected_digest" '
      length == 3 and all(.[];
        .cluster_id == $cluster and
        .execution_profile == $profile and
        .epoch == 1 and
        .recovery_generation == 2 and
        .members == ["node-1", "node-2", "node-3"] and
        .node.ready == true and
        .node.configuration_status == "active" and
        .node.active_config_id == 2 and
        .node.configuration_state.phase == "active" and
        .node.configuration_state.config_id == 2 and
        .node.active_membership_digest == $digest and
        .node.configuration_state.digest == $digest) and
      ([.[].qlog_root] | unique | length == 1)
    ' <<< "$statuses" >/dev/null; then
      return 0
    fi
    [ "$attempt" -lt 60 ] || die "same-membership replacement did not converge"
    sleep 1
  done
}
matrix_capture_uids() {
  local ordinal uid result='[]'
  for ordinal in 0 1 2; do
    uid="$(k get pod "${name_c1}-$ordinal" -o jsonpath='{.metadata.uid}')" || return 1
    [ -n "$uid" ] || return 1
    result="$(jq -cn --argjson current "$result" --arg pod "${name_c1}-$ordinal" \
      --arg uid "$uid" '$current + [{pod:$pod,uid:$uid}]')"
  done
  printf '%s\n' "$result"
}
matrix_emit_cell() {
  jq -cn \
    --arg run_id "$run_id" --arg profile "$profile" --arg status "$cell_status" \
    --arg rhiza_commit "$rhiza_commit" --argjson rhiza_dirty "$rhiza_dirty" \
    --arg resolved_image "$resolved_image" \
    --arg phase "$cell_phase" --arg error "$cell_error" \
    --argjson failed_peers "$cell_failed" --argjson survivors "$cell_survivors" \
    --argjson hold_requested_seconds "$cell_hold" \
    --argjson failure_probe_interval_seconds "$recovery_f1_probe_interval" \
    --argjson auto_recovery_timeout_seconds "$recovery_auto_timeout" \
    --argjson hold_actual_seconds "$cell_hold_actual" \
    --argjson release_epoch_seconds "$cell_release_epoch" \
    --argjson service_rto_seconds "$cell_service_rto" \
    --argjson full_rto_seconds "$cell_full_rto" \
    --argjson failure_injected_at "$cell_failure_injected_at" \
    --argjson all_target_pods_deleted_at "$cell_all_target_pods_deleted_at" \
    --argjson quorum_lost_at "$cell_quorum_lost_at" \
    --argjson failure_released_at "$cell_failure_released_at" \
    --argjson old_pod_uids "$cell_old_uids" --argjson new_pod_uids "$cell_new_uids" \
    --arg failure_write_expected "$cell_write_expected" \
    --arg failure_write_actual "$cell_write_actual" \
    --arg failure_write_actual_detail "$cell_write_actual_detail" \
    --arg failure_read_barrier_expected "$cell_read_expected" \
    --arg failure_read_barrier_actual "$cell_read_actual" \
    --arg failure_read_barrier_actual_detail "$cell_read_actual_detail" \
     --arg survivor_local_read "$cell_local_read" \
     --argjson ack_sentinel_preserved "$cell_ack_preserved" \
     --argjson markers_lost "$cell_markers_lost" \
     --argjson pvc_count "$cell_pvc_count" \
    --argjson ack_ledger "$cell_ack_ledger" \
    --argjson idempotency_boundary_verified "$cell_idempotency_verified" \
    --argjson auto_recovery_attempted "$cell_auto_recovery_attempted" \
    --argjson auto_recovery_succeeded "$cell_auto_recovery_succeeded" \
    --argjson operator_dr "$cell_operator_dr" \
    --argjson checkpoint_root "$cell_checkpoint_root" \
    --argjson tip_hashes_equal "$cell_tips_equal" --argjson tip_hashes "$cell_tips" \
    --argjson failure_safety_probes "$cell_failure_safety_probes" \
    --argjson failure_probe_expected_count "$cell_failure_probe_expected_count" \
    --argjson failure_probe_actual_count "$cell_failure_probe_actual_count" \
    --argjson failure_probe_max_lateness_seconds "$cell_failure_probe_max_lateness_seconds" \
    --argjson failure_probe_lateness_bound_seconds "$recovery_no_quorum_probe_max_lateness" \
    --argjson failure_probe_cadence_seconds "$failure_probe_interval_seconds" \
    --argjson cell_isolation "$fresh_cell_isolation" \
    '{record_type:"cell",run_id:$run_id,profile:$profile,rhiza_commit:$rhiza_commit,
      rhiza_dirty:$rhiza_dirty,resolved_image:$resolved_image,status:$status,phase:$phase,
      error:(if $error == "" then null else $error end),failed_peers:$failed_peers,
      fault_target_policy:"statefulset_highest_ordinals",
      same_pod_restart_covered:false,arbitrary_leader_failure_covered:false,
      survivors:$survivors,hold_requested_seconds:$hold_requested_seconds,
      failure_probe_interval_seconds:$failure_probe_interval_seconds,
      auto_recovery_timeout_seconds:$auto_recovery_timeout_seconds,
      hold_actual_seconds:$hold_actual_seconds,release_epoch_seconds:$release_epoch_seconds,
      service_rto_seconds:$service_rto_seconds,full_rto_seconds:$full_rto_seconds,
      failure_injected_at:$failure_injected_at,
      all_target_pods_deleted_at:$all_target_pods_deleted_at,
      quorum_lost_at:$quorum_lost_at,failure_released_at:$failure_released_at,
      old_pod_uids:$old_pod_uids,new_pod_uids:$new_pod_uids,
      failure_write_expected:$failure_write_expected,
      failure_write_actual:$failure_write_actual,
      failure_write_actual_detail:$failure_write_actual_detail,
      failure_read_barrier_expected:$failure_read_barrier_expected,
      failure_read_barrier_actual:$failure_read_barrier_actual,
      failure_read_barrier_actual_detail:$failure_read_barrier_actual_detail,
      read_no_quorum_latency_defect:($failure_read_barrier_actual_detail == "timeout"),
      survivor_local_read:$survivor_local_read,
      ack_sentinel_preserved:$ack_sentinel_preserved,ack_ledger:$ack_ledger,
      idempotency_boundary_verified:$idempotency_boundary_verified,
      auto_recovery_attempted:$auto_recovery_attempted,
      auto_recovery_succeeded:$auto_recovery_succeeded,operator_dr:$operator_dr,
      rpo_boundary:(if $operator_dr then "last_sync_checkpoint" else "zero" end),
      checkpoint_root:$checkpoint_root,
      markers_lost:$markers_lost,
      pvc_count:$pvc_count,tip_hashes_equal:$tip_hashes_equal,tip_hashes:$tip_hashes,
      failure_safety_probes:$failure_safety_probes,
      failure_probe_expected_count:$failure_probe_expected_count,
      failure_probe_actual_count:$failure_probe_actual_count,
      failure_probe_max_lateness_seconds:$failure_probe_max_lateness_seconds,
      failure_probe_lateness_bound_seconds:$failure_probe_lateness_bound_seconds,
      failure_probe_cadence_seconds:$failure_probe_cadence_seconds}
      + (if $cell_isolation == null then {} else {cell_isolation:$cell_isolation} end)' \
    >> "$recovery_matrix_jsonl"
}
matrix_emit_summary() {
  local status="$1" error="${2-}"
  jq -cn --arg run_id "$run_id" --arg profile "$profile" --arg status "$status" \
    --arg rhiza_commit "$rhiza_commit" --argjson rhiza_dirty "$rhiza_dirty" \
    --arg resolved_image "$resolved_image" \
    --arg error "$error" \
    --argjson auto_recovery_timeout_seconds "$recovery_auto_timeout" '
    {record_type:"summary",run_id:$run_id,profile:$profile,rhiza_commit:$rhiza_commit,
      rhiza_dirty:$rhiza_dirty,resolved_image:$resolved_image,status:$status,
      error:(if $error == "" then null else $error end),
      auto_recovery_timeout_seconds:$auto_recovery_timeout_seconds,
      fault_target_policy:"statefulset_highest_ordinals",
      same_pod_restart_covered:false,arbitrary_leader_failure_covered:false}' \
    >> "$recovery_matrix_jsonl"
}
matrix_fail() {
  cell_phase="$1"
  cell_error="$2"
  cell_status=failed
  matrix_emit_cell
  die "recovery matrix F${cell_failed}/${cell_hold}s failed in ${cell_phase}: ${cell_error}"
}
matrix_wait_pod_absent() {
  local pod="$1" deadline=$((SECONDS + 180))
  while k get pod "$pod" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 1
  done
}
matrix_hold_until() {
  local deadline="$1" remaining sleep_for
  while [ "$(date +%s)" -lt "$deadline" ]; do
    remaining=$((deadline - $(date +%s)))
    sleep_for="$remaining"
    [ "$sleep_for" -le 30 ] || sleep_for=30
    sleep "$sleep_for"
  done
}
matrix_ledger_append() {
  local key="$1" value="$2" request_id="$3" response="${4-null}"
  cell_ack_ledger="$(jq -cn --argjson ledger "$cell_ack_ledger" --arg key "$key" \
    --arg value "$value" --arg request_id "$request_id" --argjson response "$response" \
    --argjson acknowledged_at "$(date +%s)" \
    '$ledger + [{key:$key,value:$value,request_id:$request_id,
      acknowledged_at:$acknowledged_at,response:$response}]')"
}
matrix_check_recovery_deadline() {
  [ "$SECONDS" -lt "$cell_recovery_deadline" ] \
    || matrix_fail full_recovery recovery_deadline_exceeded
}
matrix_run_f1_availability_probe() {
  local sequence="$1"
  local key="matrix-f1-${cell_id}-${sequence}-${run_id}"
  local value="available-${cell_id}-${sequence}"
  local request_id="f1-${cell_id}-${sequence}-${run_id}"
  local response
  response="$(matrix_service_write_response "$key" "$value" "$request_id")" \
    || matrix_fail failure_period f1_periodic_write_failed
  matrix_service_read "$key" "$value" read_barrier \
    || matrix_fail failure_period f1_periodic_read_barrier_failed
  matrix_ledger_append "$key" "$value" "$request_id" "$response"
  cell_fault_key="$key"
  cell_fault_value="$value"
  cell_fault_request_id="$request_id"
  cell_fault_response="$response"
}
matrix_publish_sync_checkpoint() {
  local cell_id="$1" status_file request_file response_file request
  status_file="$target/matrix-${cell_id}-checkpoint-status.json"
  request_file="$target/matrix-${cell_id}-checkpoint-request.json"
  response_file="$target/matrix-${cell_id}-checkpoint-response.json"
  scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-0" GET \
    /v1/admin/membership/status > "$status_file" || return 1
  request="$(jq -cn \
    --arg op "matrix-checkpoint-${cell_id}-${run_id}" \
    --argjson root "$(jq -c '.qlog_root' "$status_file")" \
    '{operation_id:$op,expected_config_id:1,expected_recovery_generation:1,
      expected_root:$root}')" || return 1
  printf '%s\n' "$request" > "$request_file"
  scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-0" POST \
    /v1/admin/checkpoint/compact "$request" > "$response_file" || return 1
  jq -e '.anchor.format_version == 2' "$response_file" >/dev/null || return 1
  cell_checkpoint_root="$(jq -c '.qlog_root' "$status_file")"
}
run_recovery_matrix_cell() {
  cell_failed="$1"
  cell_hold="$2"
  cell_survivors=$((3 - cell_failed))
  cell_status=running
  cell_phase=setup
  cell_error=""
  cell_hold_actual=null
  cell_release_epoch=null
  cell_service_rto=null
  cell_full_rto=null
  cell_failure_injected_at=null
  cell_all_target_pods_deleted_at=null
  cell_quorum_lost_at=null
  cell_failure_released_at=null
  cell_old_uids='[]'
  cell_new_uids='[]'
  cell_write_actual=not_run
  cell_write_actual_detail=not_run
  cell_read_actual=not_run
  cell_read_actual_detail=not_run
  cell_local_read=not_applicable
  cell_ack_preserved=false
  cell_markers_lost=false
  cell_ack_ledger='[]'
  cell_idempotency_verified=null
  cell_auto_recovery_attempted=false
  cell_auto_recovery_succeeded=false
  cell_operator_dr=false
  cell_checkpoint_root=null
  cell_pvc_count=null
  cell_tips_equal=false
  cell_tips='[]'
  cell_failure_safety_probes='[]'
  cell_failure_probe_expected_count=null
  cell_failure_probe_actual_count=null
  cell_failure_probe_max_lateness_seconds=null
  if [ "$cell_failed" = 1 ]; then
    cell_write_expected=success
    cell_read_expected=success
  else
    cell_write_expected=failure
    cell_read_expected=failure
  fi

  local cell_id="f${cell_failed}-h${cell_hold}"
  local ack_key="matrix-ack-${cell_id}-${run_id}"
  local ack_value="preserved-${cell_id}"
  local fault_key="matrix-fault-${cell_id}-${run_id}"
  local fault_value="failure-period-${cell_id}"
  local fault_request_id="fault-${cell_id}-${run_id}"
  local post_key="matrix-post-${cell_id}-${run_id}"
  local post_value="recovered-${cell_id}"
  local ordinal hold_start hold_deadline status_file tip_attempt
  local failure_probe_interval_seconds="$recovery_f1_probe_interval" next_probe probe_sequence now sleep_for
  local service_rto_key="matrix-service-rto-${cell_id}-${run_id}"
  local service_rto_value="service-restored-${cell_id}"
  local service_rto_request_id="service-rto-${cell_id}-${run_id}"
  local first_response second_response remaining_timeout uid_survivors write_status

  write_value "${name_c1}-0" "$ack_key" "$ack_value" "ack-${cell_id}-${run_id}" \
    || matrix_fail setup ack_sentinel_write_failed
  matrix_ledger_append "$ack_key" "$ack_value" "ack-${cell_id}-${run_id}"
  for ordinal in 0 1 2; do
    read_value "${name_c1}-$ordinal" "$ack_key" "$ack_value" \
      || matrix_fail setup ack_sentinel_preflight_failed
    # Re-seed every cell so prior cells cannot mask which emptyDirs this cell replaced.
    marker_seed "${name_c1}-$ordinal" \
      || matrix_fail setup marker_seed_failed
  done
  if [ "$cell_failed" -ge 2 ]; then
    matrix_publish_sync_checkpoint "$cell_id" \
      || matrix_fail setup sync_checkpoint_publish_failed
  fi
  cell_old_uids="$(matrix_capture_uids)" \
    || matrix_fail setup old_pod_uid_capture_failed

  cell_phase=failure_period
  cell_failure_injected_at="$(date +%s)"
  k scale statefulset "$name_c1" --replicas="$cell_survivors" >/dev/null \
    || matrix_fail failure_period scale_down_failed
  for ((ordinal=cell_survivors; ordinal<3; ordinal++)); do
    matrix_wait_pod_absent "${name_c1}-$ordinal" \
      || matrix_fail failure_period pod_emptydir_deletion_timeout
  done
  cell_all_target_pods_deleted_at="$(date +%s)"
  if [ "$cell_failed" -ge 2 ]; then
    cell_quorum_lost_at="$cell_all_target_pods_deleted_at"
  fi
  hold_start="$cell_all_target_pods_deleted_at"
  hold_deadline=$((hold_start + cell_hold))

  if [ "$cell_failed" = 1 ]; then
    next_probe="$hold_start"
    probe_sequence=0
    while [ "$next_probe" -lt "$hold_deadline" ]; do
      now="$(date +%s)"
      if [ "$now" -gt "$next_probe" ]; then
        [ $((now - next_probe)) -le 1 ] \
          || matrix_fail failure_period f1_availability_probe_interval_exceeded
      elif [ "$now" -lt "$next_probe" ]; then
        sleep_for=$((next_probe - now))
        [ "$sleep_for" -le "$failure_probe_interval_seconds" ] \
          || matrix_fail failure_period f1_availability_probe_interval_exceeded
        sleep "$sleep_for"
      fi
      matrix_run_f1_availability_probe "$probe_sequence"
      probe_sequence=$((probe_sequence + 1))
      next_probe=$((next_probe + failure_probe_interval_seconds))
    done
    matrix_hold_until "$hold_deadline"
    cell_write_actual=success
    cell_read_actual=success
    cell_read_actual_detail=success
  else
    cell_failure_probe_expected_count=$(((cell_hold - 1) / failure_probe_interval_seconds + 1))
    cell_failure_probe_actual_count=0
    cell_failure_probe_max_lateness_seconds=0
    probe_sequence=0
    while [ "$probe_sequence" -lt "$cell_failure_probe_expected_count" ]; do
      next_probe=$((hold_start + probe_sequence * failure_probe_interval_seconds))
      now="$(date +%s)"
      if [ "$now" -lt "$next_probe" ]; then
        sleep "$((next_probe - now))"
        now="$(date +%s)"
      fi
      sleep_for=$((now - next_probe))
      [ "$sleep_for" -le "$recovery_no_quorum_probe_max_lateness" ] \
        || matrix_fail failure_period no_quorum_safety_probe_late
      [ "$sleep_for" -le "$cell_failure_probe_max_lateness_seconds" ] \
        || cell_failure_probe_max_lateness_seconds="$sleep_for"
      matrix_run_no_quorum_safety_probe "$probe_sequence" "$next_probe" \
        || matrix_fail failure_period no_quorum_safety_probe_failed
      cell_failure_probe_actual_count=$((cell_failure_probe_actual_count + 1))
      if [ "$probe_sequence" = 0 ]; then
        cell_write_actual=failure
        if [ "$cell_failed" = 2 ]; then
          cell_write_actual_detail='http_503_retryable_no_quorum'
          cell_read_actual_detail=unavailable_or_timeout
          cell_local_read=success
        else
          cell_write_actual_detail=zero_endpoint_transport
          cell_read_actual_detail=zero_endpoint_transport
        fi
        cell_read_actual=failure
      fi
      probe_sequence=$((probe_sequence + 1))
    done
    [ "$cell_failure_probe_actual_count" = "$cell_failure_probe_expected_count" ] \
      || matrix_fail failure_period no_quorum_safety_probe_count_mismatch
    matrix_hold_until "$hold_deadline"
  fi
  cell_release_epoch="$(date +%s)"
  cell_failure_released_at="$cell_release_epoch"
  cell_hold_actual=$((cell_release_epoch - hold_start))
  [ "$cell_hold_actual" -ge "$cell_hold" ] \
    || matrix_fail failure_period hold_released_early

  cell_phase=service_recovery
  uid_survivors="$cell_survivors"
  if [ "$cell_failed" = 2 ]; then
    cell_auto_recovery_attempted=true
    k scale statefulset "$name_c1" --replicas=3 >/dev/null \
      || matrix_fail service_recovery scale_up_failed
    if RHIZA_STATEFULSET_READY_TIMEOUT="$recovery_auto_timeout" \
      "$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c1" 3 1 >/dev/null 2>&1; then
      cell_auto_recovery_succeeded=true
    else
      cell_operator_dr=true
      k scale statefulset "$name_c1" --replicas=0 >/dev/null \
        || matrix_fail service_recovery operator_dr_scale_zero_failed
      for ordinal in 0 1 2; do
        matrix_wait_pod_absent "${name_c1}-$ordinal" \
          || matrix_fail service_recovery operator_dr_delete_timeout
      done
      uid_survivors=0
      k scale statefulset "$name_c1" --replicas=3 >/dev/null \
        || matrix_fail service_recovery operator_dr_scale_up_failed
    fi
  else
    [ "$cell_failed" != 3 ] || cell_operator_dr=true
    k scale statefulset "$name_c1" --replicas=3 >/dev/null \
      || matrix_fail service_recovery scale_up_failed
  fi
  cell_recovery_deadline=$((SECONDS + recovery_timeout))
  while true; do
    if first_response="$(matrix_service_write_response \
      "$service_rto_key" "$service_rto_value" "$service_rto_request_id")"; then
      write_status=0
    else
      write_status=$?
    fi
    [ "$write_status" != 75 ] ||
      matrix_fail service_recovery ambiguous_mutation_retry_exhausted
    [ "$write_status" = 0 ] && break
    [ "$SECONDS" -lt "$cell_recovery_deadline" ] \
      || matrix_fail service_recovery recovery_deadline_exceeded
    sleep 1
  done
  while true; do
    if matrix_service_read "$service_rto_key" "$service_rto_value" read_barrier; then
      break
    fi
    [ "$SECONDS" -lt "$cell_recovery_deadline" ] \
      || matrix_fail service_recovery recovery_deadline_exceeded
    sleep 1
  done
  matrix_ledger_append "$service_rto_key" "$service_rto_value" \
    "$service_rto_request_id" "$first_response"
  cell_service_rto=$(($(date +%s) - cell_release_epoch))

  cell_phase=full_recovery
  remaining_timeout=$((cell_recovery_deadline - SECONDS))
  [ "$remaining_timeout" -gt 0 ] || matrix_fail full_recovery recovery_deadline_exceeded
  if ! RHIZA_STATEFULSET_READY_TIMEOUT="$remaining_timeout" \
    "$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c1" 3 1; then
    matrix_fail full_recovery recovery_deadline_exceeded
  fi
  matrix_check_recovery_deadline
  cell_new_uids="$(matrix_capture_uids)" \
    || matrix_fail full_recovery new_pod_uid_capture_failed
  if ! jq -e --argjson survivors "$uid_survivors" --argjson new "$cell_new_uids" '
    . as $old |
    all(range(0; 3);
      if . < $survivors then $old[.].uid == $new[.].uid
      else $old[.].uid != $new[.].uid end)
  ' <<< "$cell_old_uids" >/dev/null; then
    matrix_fail full_recovery pod_uid_replacement_mismatch
  fi
  for ordinal in 0 1 2; do
    if [ "$ordinal" -lt "$uid_survivors" ]; then
      marker_present "${name_c1}-$ordinal" \
        || matrix_fail full_recovery survivor_marker_lost
    else
      marker_absent "${name_c1}-$ordinal" \
        || matrix_fail full_recovery replaced_emptydir_marker_preserved
    fi
  done
  cell_markers_lost=true
  cell_pvc_count="$(k get persistentvolumeclaims -o json | jq '.items | length')"
  [ "$cell_pvc_count" = 0 ] || matrix_fail full_recovery unexpected_pvc
  matrix_check_recovery_deadline

  if [ "$cell_failed" = 1 ]; then
    first_response="$cell_fault_response"
    second_response="$(matrix_service_write_response "$cell_fault_key" \
      "$cell_fault_value" "$cell_fault_request_id")" \
      || matrix_fail full_recovery acknowledged_request_id_retry_failed
  else
    first_response="$(matrix_service_write_response \
      "$fault_key" "$fault_value" "$fault_request_id")" \
      || matrix_fail full_recovery failed_request_id_recovery_retry_failed
    second_response="$(matrix_service_write_response \
      "$fault_key" "$fault_value" "$fault_request_id")" \
      || matrix_fail full_recovery failed_request_id_idempotent_retry_failed
    matrix_ledger_append "$fault_key" "$fault_value" "$fault_request_id" "$first_response"
  fi
  jq -e --argjson first "$first_response" '$first == .' <<< "$second_response" >/dev/null \
    || matrix_fail full_recovery idempotency_response_mismatch
  cell_idempotency_verified=true

  while IFS=$'\t' read -r ledger_key ledger_value; do
    for ordinal in 0 1 2; do
      matrix_check_recovery_deadline
      read_value "${name_c1}-$ordinal" "$ledger_key" "$ledger_value" \
        || matrix_fail full_recovery acknowledged_ledger_entry_missing
    done
  done < <(jq -r '.[] | [.key,.value] | @tsv' <<< "$cell_ack_ledger")
  cell_ack_preserved=true
  write_value "${name_c1}-0" "$post_key" "$post_value" "post-${cell_id}-${run_id}" \
    || matrix_fail full_recovery post_recovery_write_failed
  for ordinal in 0 1 2; do
    matrix_check_recovery_deadline
    read_value "${name_c1}-$ordinal" "$post_key" "$post_value" \
      || matrix_fail full_recovery post_recovery_strong_read_failed
  done
  tip_attempt=0
  while true; do
    tip_attempt=$((tip_attempt + 1))
    for ordinal in 0 1 2; do
      matrix_check_recovery_deadline
      status_file="$target/matrix-${cell_id}-status-${ordinal}.json"
      scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-$ordinal" GET \
        /v1/admin/membership/status > "$status_file" \
        || matrix_fail full_recovery tip_capture_failed
    done
    matrix_check_recovery_deadline
    cell_tips="$(jq -s 'map({active_config_id:.node.active_config_id,
        state_config_id:.node.configuration_state.config_id,qlog_root})' \
      "$target/matrix-${cell_id}-status-0.json" \
      "$target/matrix-${cell_id}-status-1.json" \
      "$target/matrix-${cell_id}-status-2.json")" \
      || matrix_fail full_recovery tip_parse_failed
    if jq -e 'length == 3 and
      (map(.active_config_id) | unique == [1]) and
      (map(.state_config_id) | unique == [1]) and
      (map(.qlog_root) | unique | length == 1)' <<< "$cell_tips" >/dev/null; then
      break
    fi
    matrix_check_recovery_deadline
    sleep 1
  done
  cell_tips_equal=true
  matrix_check_recovery_deadline
  cell_full_rto=$(($(date +%s) - cell_release_epoch))
  cell_status=passed
  cell_phase=complete
  matrix_emit_cell
}
fresh_capture_cell_isolation
write_value "${name_c1}-0" snapshot restored "snapshot-${run_id}"
if [ "$profile" = sql ]; then
  client "${name_c1}-0" sql execute --request-id "sql-schema-${run_id}" \
    --sql 'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
  client "${name_c1}-0" sql execute --request-id "sql-snapshot-${run_id}" \
    --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
    --params-json '[{"type":"integer","value":1},{"type":"text","value":"snapshot"}]'
fi
if [ "$deploy_only" = 1 ]; then
  echo "vind config-1 deployment ready for an external qualification run"
  exit 0
fi
compact_status="$target/compact-status-c1.json"
scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-0" GET \
  /v1/admin/membership/status > "$compact_status"
compact_request="$(jq -cn \
  --arg op "local-compact-${run_id}" \
  --argjson root "$(jq -c '.qlog_root' "$compact_status")" \
  '{operation_id:$op, expected_config_id:1, expected_recovery_generation:1, expected_root:$root}')"
compact="$target/compact-c1.json"
scripts/k8s-admin-job.sh "$name_c1" "${name_c1}-0" POST \
  /v1/admin/checkpoint/compact "$compact_request" > "$compact"
jq -e '.anchor.format_version == 2' "$compact" >/dev/null
write_value "${name_c1}-0" suffix replayed "suffix-${run_id}"
if [ "$profile" = sql ]; then
  client "${name_c1}-0" sql execute --request-id "sql-suffix-${run_id}" \
    --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
    --params-json '[{"type":"integer","value":2},{"type":"text","value":"suffix"}]'
fi
verify_profile_mutation_contract "${name_c1}-0"
for ordinal in 0 1 2; do
  read_value "${name_c1}-$ordinal" suffix replayed
  marker_seed "${name_c1}-$ordinal"
done

if [ "$recovery_matrix" = 1 ]; then
  recovery_matrix_jsonl="$target/recovery-matrix.jsonl"
  : > "$recovery_matrix_jsonl"
  echo "== run config-1 emptyDir recovery matrix =="
  for hold in "${recovery_holds[@]}"; do
    for failed in "${recovery_failures[@]}"; do
      run_recovery_matrix_cell "$failed" "$hold"
    done
  done
  jq -e -s 'length > 0 and all(.[]; .status == "passed")' \
    "$recovery_matrix_jsonl" >/dev/null
  if [ "$recovery_matrix_only" = 1 ]; then
    if [ "$(k get pod -l app.kubernetes.io/name=rustfs -o jsonpath='{.items[0].metadata.uid}')" != "$rustfs_uid" ]; then
      matrix_emit_summary failed rustfs_uid_changed
      die "RustFS changed during the recovery matrix"
    fi
    if [ -n "$(k get persistentvolumeclaims -o name)" ]; then
      matrix_emit_summary failed unexpected_pvc
      die "vind recovery matrix created a PVC"
    fi
    matrix_emit_summary passed
    echo "vind config-1 recovery matrix passed: $recovery_matrix_jsonl"
    exit 0
  fi
fi

echo "== compact locally, stop config 1, and replace 3 -> 3 =="
RHIZA_RECONFIG_WORK_DIR="$target/reconfigure" \
  scripts/replace-k8s-config.sh "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
RHIZA_RECONFIG_WORK_DIR="$target/reconfigure" \
  scripts/replace-k8s-config.sh "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
successor="$target/reconfigure/config-c2.json"
final_checkpoint="$target/final-checkpoint-c1.json"
scripts/k8s-object-job.sh 1 "$target/config-c1.json" checkpoint inspect \
  > "$final_checkpoint"
jq -e '.format_version == 2 and .base.snapshot and (.segments | type == "array")' \
  "$final_checkpoint" >/dev/null
inject_marker_helper "$target/reconfigure/config-c2.yaml"
# replace-k8s-config.sh renders c2 as a learner before promoting its live template.
# Preserve that completed promotion when applying the E2E-only helper mutation.
yq eval --inplace 'select(.kind == "StatefulSet") |
  .spec.template.metadata.labels["rhiza.dev/member-role"] = "voter"' \
  "$target/reconfigure/config-c2.yaml"
k apply -f "$target/reconfigure/config-c2.yaml" >/dev/null
for ordinal in 0 1 2; do
  k delete pod "${name_c2}-$ordinal" --wait=true >/dev/null
done
"$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c2" 3 2
verify_marker_helper "$name_c2"

for ordinal in 0 1 2; do
  marker_absent "${name_c2}-$ordinal"
  read_value "${name_c2}-$ordinal" snapshot restored
  read_value "${name_c2}-$ordinal" suffix replayed
  if [ "$profile" = sql ]; then
    client "${name_c2}-$ordinal" sql query \
      --sql 'SELECT id, name FROM users ORDER BY id' --consistency read_barrier \
      > "$target/sql-c2-${ordinal}.json"
    jq -e '.columns == ["id", "name"] and
      .rows == [[{"type":"integer","value":1},{"type":"text","value":"snapshot"}],
                [{"type":"integer","value":2},{"type":"text","value":"suffix"}]]' \
      "$target/sql-c2-${ordinal}.json" >/dev/null
  fi
done

echo "== plan, inspect, and apply old-generation GC with exact hash =="
read_value "${name_c2}-0" suffix replayed
generation_compact="$target/generation-compact-c2.json"
generation_status="$target/generation-status-c2.json"
for ((attempt=1; attempt<=20; attempt++)); do
  scripts/k8s-admin-job.sh "$name_c2" "${name_c2}-0" GET \
    /v1/admin/membership/status > "$generation_status"
  generation_compact_request="$(jq -cn \
    --arg op "generation-roll-compact-${run_id}-${attempt}" \
    --argjson root "$(jq -c '.qlog_root' "$generation_status")" \
    '{operation_id:$op, expected_config_id:2,
      expected_recovery_generation:1, expected_root:$root}')"
  if scripts/k8s-admin-job.sh "$name_c2" "${name_c2}-0" POST \
    /v1/admin/checkpoint/compact "$generation_compact_request" \
    > "$generation_compact"; then
    break
  fi
  [ "$attempt" -lt 20 ] || die "active generation checkpoint compaction did not converge"
  sleep 1
done
jq -e '.anchor.format_version == 2 and .anchor.configuration_state.phase == "active"' \
  "$generation_compact" >/dev/null

echo "== recreate one successor Pod and rejoin from the target checkpoint =="
restart_pod="${name_c2}-1"
restart_uid="$(k get pod "$restart_pod" -o jsonpath='{.metadata.uid}')"
marker_seed "$restart_pod"
k delete pod "$restart_pod" --wait=true >/dev/null
restart_deadline=$((SECONDS + recovery_timeout))
until k get pod "$restart_pod" -o json 2>/dev/null |
  jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null; do
  [ "$SECONDS" -lt "$restart_deadline" ] ||
    die "successor Pod did not become Ready after emptyDir recreation"
  sleep 1
done
current_uid="$(k get pod "$restart_pod" -o jsonpath='{.metadata.uid}')"
[ "$current_uid" != "$restart_uid" ] || die "successor Pod was not recreated"
marker_absent "$restart_pod" ||
  die "successor Pod retained deleted emptyDir data"
retry_read_value "$restart_pod" suffix replayed

scripts/k8s-object-job.sh 2 "$successor" roll-checkpoint \
  --from-generation 1 --to-generation 2 >/dev/null
echo "== replace generation-1 pods with generation-2 S3 restores =="
k scale statefulset "$name_c2" --replicas=0 >/dev/null
k wait --for=delete pod -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=2" --timeout=180s >/dev/null
k set env "statefulset/$name_c2" RHIZA_RECOVERY_GENERATION=2 >/dev/null
k scale statefulset "$name_c2" --replicas=3 >/dev/null
"$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c2" 3 2
write_value "${name_c2}-0" generation two "generation-2-${run_id}"
verify_same_membership_pod_recreation
if [ "$profile" = sql ]; then
  client "${name_c2}-1" sql query --sql 'SELECT count(*) AS users FROM users' \
    --consistency read_barrier > "$target/sql-generation-2.json"
  jq -e '.columns == ["users"] and .rows == [[{"type":"integer","value":2}]]' \
    "$target/sql-generation-2.json" >/dev/null
fi

echo "== stop rhiza publishers and let their GC leases expire =="
k scale statefulset "$name_c2" --replicas=0 >/dev/null
k wait --for=delete pod -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=2" --timeout=180s >/dev/null
sleep 6

plan="$target/gc-plan.json"
RHIZA_RECOVERY_GENERATION=2 RHIZA_GC_GRACE_MS=0 \
  RHIZA_GC_MIN_AGE_MS=0 RHIZA_GC_RETAIN_GENERATIONS=0 \
  scripts/gc-k8s.sh plan "$successor" > "$plan"
plan_hash="$(jq -er '.plan_hash' "$plan")"
RHIZA_RECOVERY_GENERATION=2 \
  scripts/gc-k8s.sh inspect "$successor" "$plan_hash" >/dev/null
report="$target/gc-report.json"
RHIZA_RECOVERY_GENERATION=2 RHIZA_GC_CONFIRM_PLAN_HASH="$plan_hash" \
  scripts/gc-k8s.sh apply "$successor" "$plan_hash" > "$report"
jq -e --arg hash "$plan_hash" --slurpfile plan "$plan" '
  .plan_hash == $hash and
  (.results |
    type == "array" and
    length > 0 and
    length == ($plan[0].candidates | length) and
    all(.[]; .plan_hash == $hash and .outcome == "deleted") and
    ([.[] | {key, version}] | sort_by(.key, (.version | tojson))) ==
      ([$plan[0].candidates[] | {key, version}] | sort_by(.key, (.version | tojson)))
  )
' "$report" >/dev/null ||
  die "GC report does not exactly cover planned candidates with deleted outcomes"

k scale statefulset "$name_c2" --replicas=3 >/dev/null
"$BASH" scripts/wait-k8s-statefulset-ready.sh "$name_c2" 3 2
retry_read_value "${name_c2}-0" generation two

if [ "$(k get pod -l app.kubernetes.io/name=rustfs -o jsonpath='{.items[0].metadata.uid}')" != "$rustfs_uid" ]; then
  [ "$recovery_matrix" != 1 ] || matrix_emit_summary failed rustfs_uid_changed
  die "RustFS changed during the restore lifecycle"
fi
[ -z "$(k get persistentvolumeclaims -o name)" ] ||
  die "successor lifecycle must remain zero-PVC"
[ "$recovery_matrix" != 1 ] || matrix_emit_summary passed
echo "vind RustFS emptyDir recovery, zero-PVC prestage, V2 compact, 3->3 replacement, and exact-hash GC passed"
