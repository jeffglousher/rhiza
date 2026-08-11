#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
hiqlite_commit=c8316c53799c509990475ea8e2aa2ef8679e070e
hiqlite_release=0.14.0
openraft_version=""
openraft_version_source=""
log_sync=Immediate
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
cluster="${HIQLITE_RECOVERY_CLUSTER:-hiqlite-recovery-${run_id}}"
namespace="${HIQLITE_RECOVERY_NAMESPACE:-hiqlite-recovery}"
staging_namespace="$namespace"
object_namespace="${HIQLITE_RECOVERY_OBJECT_NAMESPACE:-hiqlite-recovery-object}"
local_image="hiqlite-recovery:${hiqlite_commit:0:12}"
requested_image="${HIQLITE_RECOVERY_IMAGE:-$local_image}"
image="$requested_image"
resolved_image=""
resolved_local_image_id=""
resolved_image_repo_digest=""
resolved_proxy_image=""
resolved_proxy_image_id=""
resolved_proxy_image_repo_digest=""
node_cri_image_ids_path=""
local_voter_config_sha=""
local_proxy_config_sha=""
local_voter_config_pre_sha=""
local_proxy_config_pre_sha=""
cell_image_proofs_json='[]'
cell_expected_image_proof_stages_json='[]'
cell_image_manifest_path=""
cell_image_manifest_sha256=""
transition_ledger_path=""
transition_ledger_sha256=""
transition_ledger_count=0
failure_establishment_proof_path=""
failure_establishment_proof_sha256=""
failure_establishment_resolution_path=""
failure_establishment_resolution_sha256=""
failure_establishment_post_ack_path=""
failure_establishment_post_ack_sha256=""
failure_establishment_post_ack_classification=""
cell_baseline_direct_reads=false
cell_baseline_evidence_path=""
cell_baseline_evidence_sha256=""
idempotent_restore_write_path=""
idempotent_restore_write_sha256=""
cell_baseline_pre_records='[]'
cell_baseline_reset_raw=""
image_source=exact-source-build
source_commit_basis=exact-commit
image_source_commit="$hiqlite_commit"
image_release="$hiqlite_release"
lockfile_origin=generated-from-exact-source
lockfile_sha256=""
ingress_kind=hiqlite-application-proxy
ingress_version="${hiqlite_release}+axum8-route-compat"
ingress_image="${HIQLITE_RECOVERY_PROXY_IMAGE:-hiqlite-recovery-proxy:${hiqlite_commit:0:12}-axum8-${run_id}}"
proxy_patch_file="$repo_root/bench/hiqlite-recovery-client/hiqlite-proxy-axum8.patch"
proxy_patch_sha256=""
upstream_proxy_incompatibility="v0.14.0 proxy uses Axum 0.7 route syntax and omits the stream raft-type path required by its v0.14.0 client"
rustfs_image="${HIQLITE_RECOVERY_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${HIQLITE_RECOVERY_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
hold_csv="${HIQLITE_RECOVERY_HOLD_SECONDS:-60,180,300}"
failure_csv="${HIQLITE_RECOVERY_FAIL_PEERS:-1,2,3}"
probe_interval="${HIQLITE_RECOVERY_PROBE_INTERVAL_SECONDS:-10}"
probe_timeout="${HIQLITE_RECOVERY_PROBE_TIMEOUT_SECONDS:-8}"
auto_recovery_timeout="${HIQLITE_RECOVERY_AUTO_TIMEOUT_SECONDS:-60}"
quorum_loss_timeout="${HIQLITE_RECOVERY_QUORUM_LOSS_TIMEOUT_SECONDS:-60}"
recovery_timeout="${HIQLITE_RECOVERY_TIMEOUT_SECONDS:-300}"
host_port="${HIQLITE_RECOVERY_PROXY_PORT:-18200}"
cleanup="${HIQLITE_RECOVERY_CLEANUP:-1}"
direct_cluster="${HIQLITE_RECOVERY_DIRECT_CLUSTER:-0}"
require_fresh_vcluster="${HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER:-0}"
build_image="${HIQLITE_BUILD_IMAGE:-1}"
reuse_exact_local_images="${HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES:-0}"
skip_image_load="${HIQLITE_RECOVERY_SKIP_IMAGE_LOAD:-0}"
skip_client_build="${HIQLITE_RECOVERY_SKIP_CLIENT_BUILD:-0}"
steady_mode="${HIQLITE_STEADY_MODE:-0}"
steady_concurrency="${HIQLITE_STEADY_CONCURRENCY:-1}"
target="${HIQLITE_RECOVERY_TARGET_DIR:-target/hiqlite-recovery}/${run_id}"
managed_source=false
if [ -n "${HIQLITE_SOURCE_DIR+x}" ]; then
  source_dir="$HIQLITE_SOURCE_DIR"
else
  source_dir="$target/hiqlite-source"
  managed_source=true
fi
client_manifest="$repo_root/bench/hiqlite-recovery-client/Cargo.toml"
client_bin="$repo_root/bench/hiqlite-recovery-client/target/release/hiqlite-recovery-client"
jsonl="$target/recovery.jsonl"
summary="$target/summary.json"
context=""
previous_context=""
created_cluster=false
direct_namespaces_created=false
port_forward_pid=""
matrix_cell_index=0
cell_isolation_mode=""
cell_isolation_uid_proof=false
cell_isolation_identity_path=""
cell_backup_key_unique=true
cell_backup_key=""
cell_namespaces=()
direct_port_forward_pids=()
previous_sentinel_ids=(baseline)
seen_backup_keys=()
triggered_backup_key=""
rustfs_uid=""
object_namespace_uid=""
object_inventory_initial_path=""
object_inventory_initial_digest=""
live_image_ids_path=""
image_provenance_verified=false
image_provenance_publishable=false
cell_backup_evidence_path=""
cell_backup_post_digest=""
vcluster_node_uid=""

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || die "missing required command: $1"; }
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
epoch_now() { date +%s; }
canonical_docker_ref() {
  local ref="$1" first remainder
  case "$ref" in
    */*)
      first="${ref%%/*}"
      remainder="${ref#*/}"
      case "$first" in
        index.docker.io) printf 'docker.io/%s\n' "$remainder" ;;
        *.*|*:*|localhost) printf '%s\n' "$ref" ;;
        *) printf 'docker.io/%s\n' "$ref" ;;
      esac
      ;;
    *) printf 'docker.io/library/%s\n' "$ref" ;;
  esac
}

case "$cleanup" in 0|1) ;; *) die "HIQLITE_RECOVERY_CLEANUP must be 0 or 1" ;; esac
case "$direct_cluster" in 0|1) ;; *) die "HIQLITE_RECOVERY_DIRECT_CLUSTER must be 0 or 1" ;; esac
case "$require_fresh_vcluster" in 0|1) ;; *) die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER must be 0 or 1" ;; esac
case "$build_image" in 0|1) ;; *) die "HIQLITE_BUILD_IMAGE must be 0 or 1" ;; esac
case "$reuse_exact_local_images" in 0|1) ;; *) die "HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES must be 0 or 1" ;; esac
case "$skip_image_load" in 0|1) ;; *) die "HIQLITE_RECOVERY_SKIP_IMAGE_LOAD must be 0 or 1" ;; esac
case "$skip_client_build" in 0|1) ;; *) die "HIQLITE_RECOVERY_SKIP_CLIENT_BUILD must be 0 or 1" ;; esac
case "$steady_mode" in 0|1) ;; *) die "HIQLITE_STEADY_MODE must be 0 or 1" ;; esac
case "$steady_concurrency" in 1|4|16) ;; *) die "HIQLITE_STEADY_CONCURRENCY must be 1, 4, or 16" ;; esac
[ "$image" != "$ingress_image" ] \
  || die "HIQLITE_RECOVERY_IMAGE and HIQLITE_RECOVERY_PROXY_IMAGE must be distinct"
if [ "$build_image" = 0 ] && [ -z "${HIQLITE_RECOVERY_IMAGE:-}" ]; then
  die "HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_IMAGE"
fi
if [ "$build_image" = 0 ] && [ -z "${HIQLITE_RECOVERY_PROXY_IMAGE:-}" ]; then
  die "HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_PROXY_IMAGE"
fi
IFS=, read -r -a hold_values <<< "$hold_csv"
if [ "${#hold_values[@]}" -lt 1 ] || [ "${#hold_values[@]}" -gt 3 ]; then
  die "HIQLITE_RECOVERY_HOLD_SECONDS must contain one to three durations"
fi
seen_holds=,
for value in "${hold_values[@]}"; do
  [[ "$value" =~ ^[0-9]+$ ]] \
    || die "HIQLITE_RECOVERY_HOLD_SECONDS values must be non-negative integers"
  case "$seen_holds" in
    *",$value,"*) die "HIQLITE_RECOVERY_HOLD_SECONDS values must be unique" ;;
  esac
  seen_holds="${seen_holds}${value},"
done
IFS=, read -r -a failure_values <<< "$failure_csv"
if [ "${#failure_values[@]}" -lt 1 ] || [ "${#failure_values[@]}" -gt 3 ]; then
  die "HIQLITE_RECOVERY_FAIL_PEERS must contain one to three failure counts"
fi
seen_failures=,
for value in "${failure_values[@]}"; do
  case "$value" in 1|2|3) ;; *) die "HIQLITE_RECOVERY_FAIL_PEERS values must be 1, 2, or 3" ;; esac
  case "$seen_failures" in
    *",$value,"*) die "HIQLITE_RECOVERY_FAIL_PEERS values must be unique" ;;
  esac
  seen_failures="${seen_failures}${value},"
done
if [ "$require_fresh_vcluster" = 1 ]; then
  [ "$direct_cluster" = 0 ] || die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires HIQLITE_RECOVERY_DIRECT_CLUSTER=0"
  [ "${HIQLITE_RECOVERY_REUSE_EXISTING:-0}" = 0 ] || die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires HIQLITE_RECOVERY_REUSE_EXISTING=0"
  [ "${#hold_values[@]}" = 1 ] && [ "${#failure_values[@]}" = 1 ] \
    || die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 requires exactly one recovery cell"
fi
for value in "$probe_interval" "$probe_timeout" "$auto_recovery_timeout" \
  "$quorum_loss_timeout" "$recovery_timeout"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "timeout and interval values must be positive integers"
done
image="$(canonical_docker_ref "$image")"
ingress_image="$(canonical_docker_ref "$ingress_image")"
[ "$image" != "$ingress_image" ] || die "voter and proxy image refs canonicalize to the same image"

for tool in awk cargo curl docker git jq kubectl openssl patch sed tar vcluster yq; do require "$tool"; done
if command -v timeout >/dev/null 2>&1; then
  timeout_bin=timeout
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_bin=gtimeout
else
  die "missing required command: timeout or gtimeout"
fi
"$timeout_bin" --signal=TERM --kill-after=2s 1s true >/dev/null 2>&1 \
  || die "$timeout_bin must support --kill-after for hard client timeouts"

run_client_hard_timeout() {
  local seconds="$1"
  shift
  "$timeout_bin" --signal=TERM --kill-after=2s "${seconds}s" "$client_bin" "$@"
}

k() { kubectl --context "$context" --namespace "$namespace" "$@"; }
kobj() { kubectl --context "$context" --namespace "$object_namespace" "$@"; }

stop_port_forwards() {
  local pid
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
    port_forward_pid=""
  fi
  stop_direct_port_forwards
}

stop_direct_port_forwards() {
  local pid
  if (( ${#direct_port_forward_pids[@]} > 0 )); then
    for pid in "${direct_port_forward_pids[@]}"; do
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    done
  fi
  direct_port_forward_pids=()
}

cleanup_run() {
  local status="$1" candidate managed owner
  local -a cleanup_namespaces
  stop_port_forwards
  if [ "$status" -ne 0 ] && [ -n "$context" ]; then
    k get pods,deployments,statefulsets,services,persistentvolumeclaims -o wide >&2 || true
    k get events --sort-by=.metadata.creationTimestamp >&2 || true
    kobj get pods,deployments,jobs,services,persistentvolumeclaims -o wide >&2 || true
  fi
  if [ "$cleanup" = 1 ] && "$created_cluster"; then
    if ! vcluster delete "$cluster" --driver docker > "$target/cleanup-vcluster-delete.log" 2>&1; then
      cat "$target/cleanup-vcluster-delete.log" >&2 || true
      [ "$status" -ne 0 ] || return 1
    fi
  fi
  if [ "$cleanup" = 1 ] && "$direct_namespaces_created" && [ -n "$context" ]; then
    cleanup_namespaces=("$staging_namespace" "$object_namespace")
    if (( ${#cell_namespaces[@]} > 0 )); then
      cleanup_namespaces=("${cell_namespaces[@]}" "${cleanup_namespaces[@]}")
    fi
    for candidate in "${cleanup_namespaces[@]}"; do
      managed="$(kubectl --context "$context" get namespace "$candidate" \
        -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}' 2>/dev/null || true)"
      owner="$(kubectl --context "$context" get namespace "$candidate" \
        -o go-template='{{index .metadata.labels "rhiza.dev/e2e-run-id"}}' 2>/dev/null || true)"
      if [ "$managed" = true ] && [ "$owner" = "$run_id" ]; then
        if ! kubectl --context "$context" delete namespace "$candidate" --wait=false \
          > "$target/cleanup-namespace-${candidate}.log" 2>&1; then
          cat "$target/cleanup-namespace-${candidate}.log" >&2 || true
          [ "$status" -ne 0 ] || return 1
        fi
      fi
    done
  fi
  if [ -n "$previous_context" ]; then
    kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
}
trap 'status=$?; cleanup_run "$status" || { [ "$status" -ne 0 ] || exit 1; }; exit "$status"' EXIT

record_event() {
  local phase="$1" event="$2" expected="$3" observed="$4" success="$5"
  local started_at="$6" finished_at="$7" duration="$8" detail="$9"
  jq -cn \
    --arg phase "$phase" \
    --arg event "$event" \
    --arg expected "$expected" \
    --arg observed "$observed" \
    --argjson success "$success" \
    --arg started_at "$started_at" \
    --arg finished_at "$finished_at" \
    --argjson duration_seconds "$duration" \
    --arg detail "$detail" \
    --arg hiqlite_commit "$hiqlite_commit" \
    --arg hiqlite_release "$hiqlite_release" \
    --arg image_release "$image_release" \
    --arg openraft_version "$openraft_version" \
    --arg openraft_version_source "$openraft_version_source" \
    --arg log_sync "$log_sync" \
    --arg image_source "$image_source" \
    --arg source_commit_basis "$source_commit_basis" \
    --arg image_source_commit "$image_source_commit" \
    --arg lockfile_origin "$lockfile_origin" \
    --arg lockfile_sha256 "$lockfile_sha256" \
    --arg ingress_kind "$ingress_kind" \
    --arg ingress_version "$ingress_version" \
    --arg ingress_image "$ingress_image" \
    --arg proxy_patch_sha256 "$proxy_patch_sha256" \
    --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
    --arg resolved_image "$resolved_image" \
    --arg resolved_proxy_image "$resolved_proxy_image" \
    --arg resolved_proxy_image_id "$resolved_proxy_image_id" \
    --arg rustfs_uid "$rustfs_uid" --arg object_namespace_uid "$object_namespace_uid" \
    --arg object_inventory_initial_path "$object_inventory_initial_path" \
    --arg object_inventory_initial_digest "$object_inventory_initial_digest" \
    --arg cell_backup_evidence_path "$cell_backup_evidence_path" \
    --arg cell_backup_post_digest "$cell_backup_post_digest" \
    --arg vcluster_node_uid "$vcluster_node_uid" \
    '{schema_version:1,system:"hiqlite",phase:$phase,event:$event,
      expected:$expected,observed:$observed,success:$success,
      started_at:$started_at,finished_at:$finished_at,duration_seconds:$duration_seconds,
      detail:$detail,hiqlite_reference_commit:$hiqlite_commit,
      hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      hiqlite_reference_release:$hiqlite_release,
      hiqlite_release:(if $image_release == "" then null else $image_release end),
      openraft_version:$openraft_version,openraft_version_source:$openraft_version_source,log_sync:$log_sync,
      image_source:$image_source,source_commit_basis:$source_commit_basis,
      image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      cargo_lock_origin:$lockfile_origin,
      cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
      ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
        patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
      upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
      resolved_image:$resolved_image,resolved_proxy_image:$resolved_proxy_image,
      resolved_proxy_image_id:$resolved_proxy_image_id,
      evidence:{rustfs_uid:(if $rustfs_uid == "" then null else $rustfs_uid end),
        object_namespace_uid:(if $object_namespace_uid == "" then null else $object_namespace_uid end),
        initial_inventory_path:(if $object_inventory_initial_path == "" then null else $object_inventory_initial_path end),
        initial_inventory_sha256:(if $object_inventory_initial_digest == "" then null else $object_inventory_initial_digest end),
        backup_evidence_path:(if $cell_backup_evidence_path == "" then null else $cell_backup_evidence_path end),
        backup_inventory_sha256:(if $cell_backup_post_digest == "" then null else $cell_backup_post_digest end),
        vcluster_node_uid:(if $vcluster_node_uid == "" then null else $vcluster_node_uid end)},
      voters:3,storage:"emptyDir",zero_pvc:true}' >> "$jsonl"
}

prepare_source() {
  local actual_commit
  if "$managed_source" && [ ! -e "$source_dir" ]; then
    mkdir -p "$(dirname "$source_dir")"
    git clone --filter=blob:none --no-checkout https://github.com/sebadob/hiqlite.git "$source_dir"
    git -C "$source_dir" checkout --detach "$hiqlite_commit"
  fi
  [ "$(git -C "$source_dir" rev-parse --is-inside-work-tree 2>/dev/null)" = true ] \
    || die "HIQLITE_SOURCE_DIR is not a Git checkout: $source_dir"
  actual_commit="$(git -C "$source_dir" rev-parse HEAD)"
  [ "$actual_commit" = "$hiqlite_commit" ] \
    || die "HIQLITE_SOURCE_DIR must be pinned to $hiqlite_commit, got $actual_commit"
  [ -z "$(git -C "$source_dir" status --porcelain --untracked-files=all)" ] \
    || die "HIQLITE_SOURCE_DIR must be a clean checkout"
}

derive_openraft_version() {
  local lockfile="$1" version
  [ -f "$lockfile" ] || die "missing Cargo.lock for OpenRaft provenance: $lockfile"
  version="$(awk '
    $0 == "name = \"openraft\"" { in_package=1; next }
    in_package && /^version = "/ {
      value=$0; sub(/^version = "/, "", value); sub(/"$/, "", value)
      print value; count++; in_package=0
    }
    END { if (count != 1) exit 1 }
  ' "$lockfile")" || die "Cargo.lock must prove exactly one OpenRaft package version"
  [ -n "$version" ] || die "Cargo.lock OpenRaft version is empty"
  printf '%s\n' "$version"
}

build_artifacts() {
  if [ "$build_image" = 1 ]; then
    prepare_source
    if [ "$reuse_exact_local_images" = 1 ]; then
      local expected_image_id expected_proxy_id expected_lock_sha expected_lock_path actual_proxy_id
      [ -n "${HIQLITE_RECOVERY_PROXY_IMAGE:-}" ] \
        || die "exact local image reuse requires coordinator-supplied HIQLITE_RECOVERY_PROXY_IMAGE"
      expected_image_id="${HIQLITE_RECOVERY_EXPECTED_LOCAL_IMAGE_ID:-}"
      expected_proxy_id="${HIQLITE_RECOVERY_EXPECTED_LOCAL_PROXY_IMAGE_ID:-}"
      expected_lock_sha="${HIQLITE_RECOVERY_EXPECTED_LOCKFILE_SHA256:-}"
      expected_lock_path="${HIQLITE_RECOVERY_EXPECTED_LOCKFILE_PATH:-}"
      if [ -z "$expected_image_id" ] || [ -z "$expected_proxy_id" ]; then
        die "exact local image reuse requires both expected image IDs"
      fi
      [ "${#expected_lock_sha}" -eq 64 ] \
        || die "exact local image reuse requires a 64-character expected lockfile SHA-256"
      [ -f "$expected_lock_path" ] \
        || die "exact local image reuse requires HIQLITE_RECOVERY_EXPECTED_LOCKFILE_PATH"
      [ "$(openssl dgst -sha256 -r "$expected_lock_path" | awk '{print $1}')" = "$expected_lock_sha" ] \
        || die "exact local image reuse lockfile SHA-256 mismatch"
      resolved_image="$(docker image inspect --format '{{.Id}}' "$image")"
      actual_proxy_id="$(docker image inspect --format '{{.Id}}' "$ingress_image")"
      [ "$resolved_image" = "$expected_image_id" ] \
        || die "local voter image ID mismatch: expected $expected_image_id, got $resolved_image"
      [ "$actual_proxy_id" = "$expected_proxy_id" ] \
        || die "local proxy image ID mismatch: expected $expected_proxy_id, got $actual_proxy_id"
      resolved_proxy_image_id="$actual_proxy_id"
      resolved_proxy_image="$ingress_image"
      proxy_patch_sha256="$(openssl dgst -sha256 -r "$proxy_patch_file" | awk '{print $1}')"
      lockfile_sha256="$expected_lock_sha"
      openraft_version="$(derive_openraft_version "$expected_lock_path")"
      openraft_version_source=generated-cargo-lock
      image_source=verified-local-exact-source-reuse
      lockfile_origin=reused-generated-from-exact-source
    else
    image_source=exact-source-build
    source_commit_basis=exact-commit
    image_source_commit="$hiqlite_commit"
    image_release="$hiqlite_release"
    build_source_dir="$target/hiqlite-build-context"
    [ ! -e "$build_source_dir" ] || die "build context already exists: $build_source_dir"
    mkdir -p "$build_source_dir"
    git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$build_source_dir"
    cargo generate-lockfile --manifest-path "$build_source_dir/Cargo.toml"
    [ -f "$build_source_dir/Cargo.lock" ] \
      || die "cargo generate-lockfile did not create $build_source_dir/Cargo.lock"
    lockfile_sha256="$(openssl dgst -sha256 -r "$build_source_dir/Cargo.lock" | awk '{print $1}')"
    [ "${#lockfile_sha256}" -eq 64 ] || die "cannot calculate generated Cargo.lock SHA-256"
    openraft_version="$(derive_openraft_version "$build_source_dir/Cargo.lock")"
    openraft_version_source=generated-cargo-lock
    docker build \
      --file "$repo_root/bench/hiqlite-recovery-client/Dockerfile.server" \
      --tag "$image" "$build_source_dir"
    resolved_image="$(docker image inspect --format '{{.Id}}' "$image")"
    proxy_build_source_dir="$target/hiqlite-proxy-build-context"
    mkdir -p "$proxy_build_source_dir"
    git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$proxy_build_source_dir"
    cp "$build_source_dir/Cargo.lock" "$proxy_build_source_dir/Cargo.lock"
    patch --directory "$proxy_build_source_dir" --strip=1 < "$proxy_patch_file"
    proxy_patch_sha256="$(openssl dgst -sha256 -r "$proxy_patch_file" | awk '{print $1}')"
    [ "${#proxy_patch_sha256}" -eq 64 ] || die "cannot calculate proxy patch SHA-256"
    docker build \
      --file "$repo_root/bench/hiqlite-recovery-client/Dockerfile.server" \
      --tag "$ingress_image" "$proxy_build_source_dir"
    resolved_proxy_image_id="$(docker image inspect --format '{{.Id}}' "$ingress_image")"
    resolved_proxy_image="$ingress_image"
    fi
  else
    image_source=user-supplied-prebuilt
    source_commit_basis=user-supplied-unverified
    image_source_commit=""
    image_release=""
    lockfile_origin=not-applicable-prebuilt
    lockfile_sha256=""
    openraft_version=""
    openraft_version_source=not-applicable-prebuilt
    docker pull "$image"
    resolved_image="$(docker image inspect --format '{{index .RepoDigests 0}}' "$image")"
    [ -n "$resolved_image" ] || die "cannot resolve pulled image digest for $image"
    ingress_image="$HIQLITE_RECOVERY_PROXY_IMAGE"
    docker pull "$ingress_image"
    resolved_proxy_image="$ingress_image"
    resolved_proxy_image_id="$(docker image inspect --format '{{.Id}}' "$ingress_image")"
  fi
  resolved_local_image_id="$(docker image inspect --format '{{.Id}}' "$image")"
  resolved_image_repo_digest="$(docker image inspect --format '{{index .RepoDigests 0}}' "$image" 2>/dev/null || true)"
  resolved_proxy_image_repo_digest="$(docker image inspect --format '{{index .RepoDigests 0}}' "$ingress_image" 2>/dev/null || true)"
  if [ "$skip_client_build" = 1 ]; then
    [ -x "$client_bin" ] \
      || die "HIQLITE_RECOVERY_SKIP_CLIENT_BUILD=1 requires $client_bin"
  else
    cargo build --release --manifest-path "$client_manifest"
  fi
}

docker_save_config_sha() {
  local ref="$1" manifest config
  manifest="$(docker save "$ref" | tar -xOf - manifest.json)" \
    || die "cannot read docker save manifest for $ref"
  config="$(jq -er 'if type == "array" and length == 1 and
      (.[0].Config | type) == "string" then .[0].Config else error("expected one docker save manifest config") end |
      capture("(?<sha>[0-9a-f]{64})") | .sha' <<< "$manifest")" \
    || die "cannot derive one config SHA-256 from docker save for $ref"
  printf 'sha256:%s\n' "$config"
}

capture_local_image_config_ids() {
  local stage="$1" output="$target/local-image-config-ids.json" tmp voter proxy
  voter="$(docker_save_config_sha "$image")"
  proxy="$(docker_save_config_sha "$ingress_image")"
  case "$stage" in
    pre-load)
      local_voter_config_pre_sha="$voter"
      local_proxy_config_pre_sha="$proxy"
      ;;
    post-load) ;;
    *) die "unknown local image config capture stage: $stage" ;;
  esac
  local_voter_config_sha="$voter"
  local_proxy_config_sha="$proxy"
  [ "$voter" != "$proxy" ] || die "voter and proxy docker save Config SHA values must differ"
  tmp="$output.tmp.$$"
  jq -n --arg voter_pre "$local_voter_config_pre_sha" --arg proxy_pre "$local_proxy_config_pre_sha" \
    --arg voter_post "$local_voter_config_sha" --arg proxy_post "$local_proxy_config_sha" \
    '{voter:{pre_load_config_sha256:$voter_pre,post_load_config_sha256:$voter_post},
      proxy:{pre_load_config_sha256:$proxy_pre,post_load_config_sha256:$proxy_post},
      valid:($voter_pre == $voter_post and $proxy_pre == $proxy_post)}' > "$tmp" \
    || jq -n '{valid:false,mismatch_reason:"cannot encode docker save config evidence"}' > "$tmp"
  mv "$tmp" "$output"
  [ "$stage" = pre-load ] || jq -e '.valid == true' "$output" >/dev/null \
    || die "local docker save config evidence changed after image load"
}

scale_failure() {
  survivors="$1"
  k scale statefulset/hiqlite-recovery --replicas="$survivors" >/dev/null
}

capture_ready_context() {
  local attempt
  if [ -z "$context" ]; then
    context="$(kubectl config current-context 2>/dev/null || true)"
  fi
  [ -n "$context" ] || die "vcluster did not select a Kubernetes context"
  for ((attempt=1; attempt<=120; attempt++)); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then return; fi
    [ "$attempt" -lt 120 ] || die "Kubernetes API did not become ready for $context"
    sleep 1
  done
}

create_managed_namespace() {
  local candidate="$1" managed owner
  if kubectl --context "$context" get namespace "$candidate" >/dev/null 2>&1; then
    managed="$(kubectl --context "$context" get namespace "$candidate" \
      -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}')"
    owner="$(kubectl --context "$context" get namespace "$candidate" \
      -o go-template='{{index .metadata.labels "rhiza.dev/e2e-run-id"}}')"
    [ "$managed" = true ] && [ "$owner" = "$run_id" ] \
      || die "refusing to replace namespace not owned by this run: $candidate"
    kubectl --context "$context" delete namespace "$candidate" --wait=true >/dev/null
  fi
  kubectl --context "$context" create namespace "$candidate" >/dev/null
  kubectl --context "$context" label namespace "$candidate" \
    rhiza.dev/e2e-managed=true "rhiza.dev/e2e-run-id=$run_id" >/dev/null
}

render_and_deploy() {
  local secret_raft secret_api
  secret_raft="$(openssl rand -hex 24)"
  secret_api="$(openssl rand -hex 24)"
  api_secret="$secret_api"
  sed \
    -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" \
    -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
    "$repo_root/deploy/k8s/hiqlite-recovery-rustfs.yaml" > "$target/rustfs.yaml"
  sed \
    -e "s|__HIQLITE_IMAGE__|$image|g" \
    -e "s|__INGRESS_IMAGE__|$ingress_image|g" \
    -e "s|__OBJECT_NAMESPACE__|$object_namespace|g" \
    -e "s|__SECRET_RAFT__|$secret_raft|g" \
    -e "s|__SECRET_API__|$secret_api|g" \
    "$repo_root/deploy/k8s/hiqlite-recovery-cluster.yaml" > "$target/hiqlite.yaml"
  if [ "$steady_mode" = 1 ]; then
    yq -i 'with(select(.kind == "StatefulSet" and .metadata.name == "hiqlite-recovery"); .spec.template.spec.containers[] |= (select(.name == "hiqlite").resources = {"requests":{"cpu":"250m","memory":"512Mi"},"limits":{"cpu":"1000m","memory":"1Gi"}}))' "$target/hiqlite.yaml"
  fi
  yq eval '.' "$target/rustfs.yaml" "$target/hiqlite.yaml" >/dev/null
  kobj apply -f "$target/rustfs.yaml" >/dev/null
  kobj rollout status deployment/rustfs --timeout=240s >/dev/null
  kobj rollout status deployment/rustfs-tools --timeout=240s >/dev/null
  kobj wait --for=condition=complete job/rustfs-create-hiqlite-bucket --timeout=240s >/dev/null
  rustfs_uid="$(kobj get pod -l app.kubernetes.io/component=object-store \
    -o jsonpath='{.items[0].metadata.uid}')"
  [ -n "$rustfs_uid" ] || die "cannot capture RustFS Pod UID"
  object_namespace_uid="$(kubectl --context "$context" get namespace "$object_namespace" -o jsonpath='{.metadata.uid}')"
  [ -n "$object_namespace_uid" ] || die "cannot capture object namespace UID"
  k apply -f "$target/hiqlite.yaml" >/dev/null
}

capture_node_cri_image_ids() {
  local output="$target/node-cri-image-ids.json" tmp raw status control_plane
  output="$target/node-cri-image-ids.json"
  tmp="$output.tmp.$$"
  control_plane="vcluster.cp.$node"
  node_cri_image_ids_path="$output"
  if raw="$(docker exec "$control_plane" crictl images --output json)"; then
    status=0
  else
    status=$?
    raw=""
  fi
  jq -n --arg voter_tag "$image" --arg proxy_tag "$ingress_image" \
    --arg voter_config_sha "$local_voter_config_sha" --arg proxy_config_sha "$local_proxy_config_sha" \
    --arg raw "$raw" --argjson command_status "$status" '
      def canonical_ref:
        if test("^[^/]+$") then "docker.io/library/" + .
        elif (split("/")[0] | test("[.:]") or . == "localhost") then .
        else "docker.io/" + . end;
      def evidence_for($tag):
        ($tag | canonical_ref) as $canonical_tag |
        [(.images // [])[] as $record |
          (($record.repoTags // []) | map(canonical_ref) | index($canonical_tag)) as $matches_tag |
          select($matches_tag != null) |
          {id:$record.id,repo_tags:($record.repoTags // []),repo_digests:($record.repoDigests // []),
           canonical_tag:$canonical_tag,
           matching_repo_tags:[($record.repoTags // [])[] | select(canonical_ref == $canonical_tag)],
           cri_image_id:(if ($record.id | type) == "string" and ($record.id | test("^sha256:[0-9a-f]{64}$")) then $record.id else null end)}];
      (try ($raw | fromjson) catch null) as $runtime |
      {node_cri:{command:"docker exec vcluster.cp.<node> crictl images --output json",
                     command_status:$command_status,raw:$raw},
       voter:{tag:$voter_tag,evidence:(if $runtime == null then [] else ($runtime | evidence_for($voter_tag)) end)},
       proxy:{tag:$proxy_tag,evidence:(if $runtime == null then [] else ($runtime | evidence_for($proxy_tag)) end)}} |
      .voter.cri_image_id_candidates = ([.voter.evidence[].cri_image_id | select(. != null)] | unique) |
      .proxy.cri_image_id_candidates = ([.proxy.evidence[].cri_image_id | select(. != null)] | unique) |
      .valid = ($command_status == 0 and
                (.voter.evidence | length) == 1 and
                (.proxy.evidence | length) == 1 and
                (.voter.cri_image_id_candidates | length) == 1 and
                (.proxy.cri_image_id_candidates | length) == 1 and
                .voter.cri_image_id_candidates == [$voter_config_sha] and
                .proxy.cri_image_id_candidates == [$proxy_config_sha]) |
      .mismatch_reason =
        if .valid then null
        elif $command_status != 0 then "cannot query node CRI image records"
        elif (.voter.evidence | length) != 1 then "voter tag does not map to exactly one node CRI image record"
        elif (.proxy.evidence | length) != 1 then "proxy tag does not map to exactly one node CRI image record"
        elif (.voter.cri_image_id_candidates | length) != 1 then "voter tag does not map to exactly one CRI image ID"
        elif (.proxy.cri_image_id_candidates | length) != 1 then "proxy tag does not map to exactly one CRI image ID"
        elif .voter.cri_image_id_candidates != [$voter_config_sha] then "voter CRI image ID differs from local docker save config SHA"
        else "proxy CRI image ID differs from local docker save config SHA"
        end
    ' > "$tmp" || {
      jq -n --arg error "cannot encode node CRI image evidence" \
        '{valid:false,mismatch_reason:$error}' > "$tmp"
    }
  mv "$tmp" "$output"
  jq -e '.valid == true' "$output" >/dev/null \
    || die "node CRI image evidence does not resolve exact image IDs"
}

capture_direct_live_image_ids() {
  local output="$target/live-image-ids.json" tmp voters proxy
  live_image_ids_path="$output"
  image_provenance_verified=false
  image_provenance_publishable=false
  voters="$(k get pods -l app.kubernetes.io/component=voter -o json)"
  proxy="$(k get pods -l app.kubernetes.io/component=proxy -o json)"
  tmp="$output.tmp.$$"
  jq -n --arg image "$image" --arg proxy_image "$ingress_image" \
    --arg voters_raw "$voters" --arg proxy_raw "$proxy" '
      def normalized_image_id:
        if type == "string" then
          sub("^(docker-pullable|docker|containerd)://"; "") |
          if contains("@") then split("@")[-1] else . end
        else null end;
      (try ($voters_raw | fromjson) catch null) as $voters |
      (try ($proxy_raw | fromjson) catch null) as $proxy |
      {verification_mode:"direct-live-tags-only",image_provenance_verified:false,image_provenance_publishable:false,
       expected_tags:{voter_image:$image,proxy_image:$proxy_image},
       raw:{voters:$voters_raw,proxy:$proxy_raw},
       voters:[(($voters.items // [])[]?) | {name:.metadata.name,
         image:([(.spec.containers // [])[] | select(.name == "hiqlite") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID | normalized_image_id][0] // null)}],
       proxy:[(($proxy.items // [])[]?) | {name:.metadata.name,
         image:([(.spec.containers // [])[] | select(.name == "proxy") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID | normalized_image_id][0] // null)}]}
      | .valid = ((.voters | length) == 3 and (.proxy | length) == 1 and
          (.voters | all(.[]; .image == $image and (.image_id | type) == "string" and
            (.image_id | test("^sha256:[0-9a-f]{64}$")))) and
          (.proxy | all(.[]; .image == $proxy_image and (.image_id | type) == "string" and
            (.image_id | test("^sha256:[0-9a-f]{64}$"))))) |
      .mismatch_reason =
        if .valid then null
        elif (.voters | length) != 3 then "expected exactly three voter containers"
        elif (.proxy | length) != 1 then "expected exactly one proxy container"
        elif ((.voters | all(.[]; .image == $image and (.image_id | type) == "string" and
          (.image_id | test("^sha256:[0-9a-f]{64}$")))) | not) then "voter direct-cluster tag or live SHA image ID is invalid"
        else "proxy direct-cluster tag or live SHA image ID is invalid"
        end
    ' > "$tmp" || {
      jq -n --arg error "cannot encode direct live image evidence" \
        '{verification_mode:"direct-live-tags-only",image_provenance_verified:false,image_provenance_publishable:false,valid:false,mismatch_reason:$error}' > "$tmp"
    }
  mv "$tmp" "$output"
  jq -e '.valid == true and .image_provenance_verified == false' "$output" >/dev/null \
    || die "direct-cluster live voter/proxy tags or SHA image IDs are invalid"
}

verify_live_image_ids() {
  local output="$target/live-image-ids.json" tmp voters proxy node_runtime
  if [ "$direct_cluster" = 1 ]; then
    capture_direct_live_image_ids
    return
  fi
  live_image_ids_path="$output"
  voters="$(k get pods -l app.kubernetes.io/component=voter -o json)"
  proxy="$(k get pods -l app.kubernetes.io/component=proxy -o json)"
  if [ -n "$node_cri_image_ids_path" ] && [ -f "$node_cri_image_ids_path" ]; then
    node_runtime="$(<"$node_cri_image_ids_path")"
  else
    node_runtime=""
  fi
  tmp="$output.tmp.$$"
  jq -n --arg image "$image" --arg image_id "$resolved_local_image_id" \
    --arg image_repo_digest "$resolved_image_repo_digest" \
    --arg proxy_image "$ingress_image" --arg proxy_image_id "$resolved_proxy_image_id" \
    --arg proxy_repo_digest "$resolved_proxy_image_repo_digest" \
    --arg voters_raw "$voters" --arg proxy_raw "$proxy" --arg node_runtime_raw "$node_runtime" '
      def normalized_image_id:
        if type == "string" then
          sub("^(docker-pullable|docker|containerd)://"; "") |
          if contains("@") then split("@")[-1] else . end
        else null end;
      (try ($voters_raw | fromjson) catch null) as $voters |
      (try ($proxy_raw | fromjson) catch null) as $proxy |
      (try ($node_runtime_raw | fromjson) catch null) as $node_runtime |
      {expected_node_cri:{voter_image:$image,
        voter_image_ids:(if $node_runtime == null then [] else ($node_runtime.voter.cri_image_id_candidates // []) end),
        proxy_image:$proxy_image,
        proxy_image_ids:(if $node_runtime == null then [] else ($node_runtime.proxy.cri_image_id_candidates // []) end)},
       local_docker:{voter_config_id:$image_id,voter_index_or_repo_digest:$image_repo_digest,
         proxy_config_id:$proxy_image_id,proxy_index_or_repo_digest:$proxy_repo_digest},
       node_cri:($node_runtime // {valid:false,mismatch_reason:"missing node CRI evidence"}),
       raw:{voters:$voters_raw,proxy:$proxy_raw},
       voters:[(($voters.items // [])[]?) | {name:.metadata.name,
         image:([(.spec.containers // [])[] | select(.name == "hiqlite") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID | normalized_image_id][0] // null)}],
       proxy:[(($proxy.items // [])[]?) | {name:.metadata.name,
         image:([(.spec.containers // [])[] | select(.name == "proxy") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID | normalized_image_id][0] // null)}]}
      | . as $proof
      | .voters |= map(. as $item | .matched_expected_id =
          ($proof.expected_node_cri.voter_image_ids | map(select(. == $item.image_id)) | .[0]))
      | .proxy |= map(. as $item | .matched_expected_id =
          ($proof.expected_node_cri.proxy_image_ids | map(select(. == $item.image_id)) | .[0]))
      | .valid = ($proof.node_cri.valid == true and
          (.voters | length) == 3 and (.proxy | length) == 1 and
          (.voters | all(.[]; .image == $image and (.matched_expected_id | type) == "string")) and
          (.proxy | all(.[]; .image == $proxy_image and (.matched_expected_id | type) == "string"))) |
      .mismatch_reason =
        if .valid then null
        elif $proof.node_cri.valid != true then ($proof.node_cri.mismatch_reason // "node CRI evidence is invalid")
        elif (.voters | length) != 3 then "expected exactly three voter containers"
        elif (.proxy | length) != 1 then "expected exactly one proxy container"
        elif ((.voters | all(.[]; .image == $image and (.matched_expected_id | type) == "string")) | not) then "voter runtime image ID differs from node CRI image ID"
        else "proxy runtime image ID differs from node CRI image ID"
        end
    ' > "$tmp" || {
      jq -n --arg error "cannot encode live image identity evidence" \
        '{valid:false,mismatch_reason:$error}' > "$tmp"
    }
  mv "$tmp" "$output"
  jq -e '.valid == true' "$output" >/dev/null \
    || die "live voter/proxy images do not match the node CRI image IDs"
  image_provenance_verified=true
  image_provenance_publishable=true
}

capture_cell_image_proof() {
  local cell_id="$1" stage="$2" output tmp voters proxy digest
  output="$target/${cell_id}-image-${stage}.json"
  tmp="$output.tmp.$$"
  voters="$(k get pods -l app.kubernetes.io/component=voter -o json)"
  proxy="$(k get pods -l app.kubernetes.io/component=proxy -o json)"
  jq -n --arg cell_id "$cell_id" --arg stage "$stage" --arg image "$image" --arg proxy_image "$ingress_image" \
    --arg voter_config_sha "$local_voter_config_sha" --arg proxy_config_sha "$local_proxy_config_sha" \
    --argjson provenance_verified "$image_provenance_verified" \
    --arg voters_raw "$voters" --arg proxy_raw "$proxy" '
      def normalized: if type == "string" then sub("^(docker-pullable|docker|containerd)://"; "") | if contains("@") then split("@")[-1] else . end else null end;
      (try ($voters_raw | fromjson) catch null) as $voters |
      (try ($proxy_raw | fromjson) catch null) as $proxy |
      {cell_id:$cell_id,stage:$stage,image_provenance_verified:$provenance_verified,
       expected_config_ids:{voter:$voter_config_sha,proxy:$proxy_config_sha},
       expected_cri_ids:{voter:$voter_config_sha,proxy:$proxy_config_sha},raw:{voters:$voters_raw,proxy:$proxy_raw},
       voters:[(($voters.items // [])[]?) | {name:.metadata.name,uid:.metadata.uid,creationTimestamp:.metadata.creationTimestamp,
         image:([(.spec.containers // [])[] | select(.name == "hiqlite") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "hiqlite") | .imageID | normalized][0] // null)}],
       proxy:[(($proxy.items // [])[]?) | {name:.metadata.name,uid:.metadata.uid,creationTimestamp:.metadata.creationTimestamp,
         image:([(.spec.containers // [])[] | select(.name == "proxy") | .image][0] // null),
         image_id_raw:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID][0] // null),
         image_id:([(.status.containerStatuses // [])[] | select(.name == "proxy") | .imageID | normalized][0] // null)}]}
      | .valid = ((.voters | length) == 3 and (.proxy | length) == 1 and
          (.voters | all(.[]; (.name|type)=="string" and (.uid|type)=="string" and (.creationTimestamp|type)=="string" and .image == $image and
            (.image_id|type)=="string" and (.image_id|test("^sha256:[0-9a-f]{64}$")) and
            (if $provenance_verified then .image_id == $voter_config_sha else true end))) and
          (.proxy | all(.[]; (.name|type)=="string" and (.uid|type)=="string" and (.creationTimestamp|type)=="string" and .image == $proxy_image and
            (.image_id|type)=="string" and (.image_id|test("^sha256:[0-9a-f]{64}$")) and
            (if $provenance_verified then .image_id == $proxy_config_sha else true end)))) |
      .mismatch_reason = if .valid then null else "cell pod image tags, IDs, or expected config IDs do not match" end
    ' > "$tmp" || jq -n --arg cell_id "$cell_id" --arg stage "$stage" \
      '{cell_id:$cell_id,stage:$stage,valid:false,mismatch_reason:"cannot encode cell image proof"}' > "$tmp"
  mv "$tmp" "$output"
  jq -e '.valid == true' "$output" >/dev/null || die "$cell_id $stage image proof failed"
  digest="$(openssl dgst -sha256 -r "$output" | awk '{print $1}')"
  cell_image_proofs_json="$(jq -cn --argjson existing "$cell_image_proofs_json" --arg stage "$stage" --arg path "$output" --arg sha256 "$digest" \
    '$existing + [{stage:$stage,path:$path,sha256:$sha256}]')"
}

capture_cell_image_manifest() {
  local cell_id="$1" output tmp config_sha node_sha live_sha
  output="$target/${cell_id}-image-provenance-manifest.json"
  tmp="$output.tmp.$$"
  config_sha=""
  [ ! -f "$target/local-image-config-ids.json" ] || config_sha="$(openssl dgst -sha256 -r "$target/local-image-config-ids.json" | awk '{print $1}')"
  node_sha=""
  [ -z "$node_cri_image_ids_path" ] || node_sha="$(openssl dgst -sha256 -r "$node_cri_image_ids_path" | awk '{print $1}')"
  live_sha="$(openssl dgst -sha256 -r "$live_image_ids_path" | awk '{print $1}')"
  jq -n --arg cell_id "$cell_id" --arg voter_tag "$image" --arg proxy_tag "$ingress_image" \
    --arg voter_config "$local_voter_config_sha" --arg proxy_config "$local_proxy_config_sha" \
    --arg config_path "$target/local-image-config-ids.json" --arg config_sha "$config_sha" \
    --arg cri_path "$node_cri_image_ids_path" --arg cri_sha "$node_sha" \
    --arg live_path "$live_image_ids_path" --arg live_sha "$live_sha" --argjson stages "$cell_expected_image_proof_stages_json" \
    --argjson proofs "$cell_image_proofs_json" '
      {cell_id:$cell_id,canonical_tags:{voter:$voter_tag,proxy:$proxy_tag},
       expected_config_ids:{voter:$voter_config,proxy:$proxy_config},expected_stages:$stages,
       references:{local_image_config:{path:(if $config_sha=="" then null else $config_path end),sha256:(if $config_sha=="" then null else $config_sha end)},node_cri:{path:(if $cri_path=="" then null else $cri_path end),sha256:(if $cri_sha=="" then null else $cri_sha end)},live_image:{path:$live_path,sha256:$live_sha},stage_proofs:$proofs},valid:true}' > "$tmp" \
    || jq -n --arg cell_id "$cell_id" '{cell_id:$cell_id,valid:false,mismatch_reason:"cannot encode image provenance manifest"}' > "$tmp"
  mv "$tmp" "$output"
  jq -e '.valid == true' "$output" >/dev/null || die "$cell_id cannot write image provenance manifest"
  cell_image_manifest_path="$output"
  cell_image_manifest_sha256="$(openssl dgst -sha256 -r "$output" | awk '{print $1}')"
}

ready_replicas() {
  k get statefulset hiqlite-recovery -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true
}

wait_ready_replicas() {
  local expected="$1" timeout_seconds="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    [ "$(ready_replicas)" = "$expected" ] && return 0
    sleep 1
  done
  return 1
}

wait_ready_pod() {
  local pod="$1" timeout_seconds="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if [ "$(k get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
      2>/dev/null || true)" = True ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_port_forward() {
  local recoverable="${1:-0}" attempt
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
    port_forward_pid=""
  fi
  k port-forward service/hiqlite-recovery-proxy "$host_port:8200" \
    > "$target/ingress-port-forward.log" 2>&1 &
  port_forward_pid=$!
  for ((attempt=1; attempt<=60; attempt++)); do
    if curl --fail --silent --max-time 2 "http://127.0.0.1:$host_port/ping" >/dev/null; then return; fi
    if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      port_forward_pid=""
      [ "$recoverable" = 1 ] && return 1
      die "Hiqlite ingress port-forward exited"
    fi
    if [ "$attempt" -eq 60 ]; then
      [ "$recoverable" = 1 ] && { kill "$port_forward_pid" >/dev/null 2>&1 || true; wait "$port_forward_pid" >/dev/null 2>&1 || true; port_forward_pid=""; return 1; }
      die "Hiqlite ingress port-forward did not become ready"
    fi
    sleep 1
  done
}

ensure_port_forward() {
  if [ -z "$port_forward_pid" ] || ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    start_port_forward
  fi
}

ensure_port_forward_recoverable() {
  if [ -z "$port_forward_pid" ] || ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    port_forward_pid=""
    start_port_forward 1 || return 1
  fi
}

start_direct_voter_port_forwards() {
  local ordinal port pid attempt
  stop_direct_port_forwards
  for ordinal in 0 1 2; do
    port=$((host_port + 10 + ordinal))
    k port-forward "pod/hiqlite-recovery-$ordinal" "$port:8200" \
      > "$target/direct-voter-$ordinal-port-forward.log" 2>&1 &
    pid=$!
    direct_port_forward_pids+=("$pid")
    for ((attempt=1; attempt<=60; attempt++)); do
      if curl --fail --silent --max-time 2 "http://127.0.0.1:$port/ping" >/dev/null; then break; fi
      kill -0 "$pid" >/dev/null 2>&1 || die "direct voter $ordinal port-forward exited"
      [ "$attempt" -lt 60 ] || die "direct voter $ordinal port-forward did not become ready"
      sleep 1
    done
  done
}

start_direct_survivor_port_forward() {
  local port pid attempt
  stop_direct_port_forwards
  port=$((host_port + 10))
  k port-forward pod/hiqlite-recovery-0 "$port:8200" \
    > "$target/direct-voter-0-survivor-port-forward.log" 2>&1 &
  pid=$!
  direct_port_forward_pids=("$pid")
  for ((attempt=1; attempt<=60; attempt++)); do
    if curl --fail --silent --max-time 2 "http://127.0.0.1:$port/ping" >/dev/null; then return 0; fi
    kill -0 "$pid" >/dev/null 2>&1 || die "direct survivor port-forward exited"
    [ "$attempt" -lt 60 ] || die "direct survivor port-forward did not become ready"
    sleep 1
  done
}

run_direct_voter_client() {
  local ordinal="$1" seconds="$2" status port
  shift 2
  port=$((host_port + 10 + ordinal))
  if run_client_hard_timeout "$seconds" \
    --nodes "127.0.0.1:$port" --secret "$api_secret" "$@"; then
    return 0
  else
    status=$?
  fi
  return "$status"
}

run_client() {
  local seconds="$1" status
  shift
  ensure_port_forward
  if run_client_hard_timeout "$seconds" \
    --nodes "127.0.0.1:$host_port" --secret "$api_secret" "$@"; then
    return 0
  else
    status=$?
  fi
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    start_port_forward
    run_client_hard_timeout "$seconds" \
      --nodes "127.0.0.1:$host_port" --secret "$api_secret" "$@"
    return $?
  fi
  return "$status"
}

run_client_recoverable() {
  local seconds="$1" status
  shift
  ensure_port_forward_recoverable || return 1
  run_client_hard_timeout "$seconds" \
    --nodes "127.0.0.1:$host_port" --secret "$api_secret" "$@" || status=$?
  [ -z "${status:-}" ] && return 0
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    port_forward_pid=""
  fi
  return "$status"
}

probe() {
  local phase="$1" operation="$2" expected="$3"
  local started_at started_epoch out success observed status finished_at duration detail
  shift 3
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  if [ "$operation" = metrics ]; then
    out="$target/probe-${phase}-${operation}-$(date +%s)-${RANDOM}.json"
    if metrics_to "$out"; then status=0; else status=$?; fi
  else
    out="$target/probe-${phase}-${operation}-$(date +%s)-${RANDOM}.out"
    if run_client "$probe_timeout" "$@" > "$out" 2>&1; then status=0; else status=$?; fi
  fi
  if [ "$status" -eq 0 ]; then
    if [ "$expected" = success ] && [ "$operation" = write ] \
      && ! jq -e '.acknowledged == true' "$out" >/dev/null 2>&1; then
      success=false
      observed=invalid-ack
      status=1
    elif [ "$expected" = success ] \
      && { [ "$operation" = local-query ] || [ "$operation" = query-consistent ]; } \
      && ! jq -e '.found == true' "$out" >/dev/null 2>&1; then
      success=false
      observed=missing-sentinel
      status=1
    else
      success=true
      observed=success
      status=0
    fi
  else
    success=false
    observed=failed
    status=1
  fi
  finished_at="$(iso_now)"
  duration=$(( $(epoch_now) - started_epoch ))
  detail="$(tail -c 4000 "$out")"
  record_event "$phase" "$operation" "$expected" "$observed" "$success" \
    "$started_at" "$finished_at" "$duration" "$detail"
  return "$status"
}

probe_window() {
  local phase="$1" hold_seconds="$2" write_expected="$3" local_expected="$4"
  local consistent_expected="$5" ack_file="$6" query_id="$7"
  local deadline iteration id value now remaining sleep_for probe_budget
  deadline=$(( $(epoch_now) + hold_seconds ))
  probe_budget=$((probe_timeout * 4))
  iteration=0
  while :; do
    iteration=$((iteration + 1))
    id="${phase}-hold-${iteration}-${run_id}"
    value="ack-${phase}-${iteration}"
    if probe "$phase" write "$write_expected" execute "$id" "$value"; then
      printf '%s\t%s\n' "$id" "$value" >> "$ack_file"
      remember_sentinel "$id"
    fi
    probe "$phase" local-query "$local_expected" query-local "$query_id" || true
    probe "$phase" query-consistent "$consistent_expected" query-consistent "$query_id" || true
    probe "$phase" metrics observable metrics || true
    now="$(epoch_now)"
    [ "$now" -ge "$deadline" ] && break
    remaining=$((deadline - now))
    # Do not begin another four-probe sample if its timeout budget can cross the
    # requested recovery release. The first sample always runs, so every
    # operation still has evidence even for a short smoke hold.
    if [ "$remaining" -le "$probe_budget" ]; then
      sleep "$remaining"
      break
    fi
    sleep_for="$probe_interval"
    [ "$remaining" -lt "$sleep_for" ] && sleep_for="$remaining"
    sleep "$sleep_for"
  done
}

capture_f2_post_ack_evidence() {
  local phase="$1" id="$2" value="$3" order="$4" prefix proof tmp classification
  local pods endpoints metrics consistent local_query ping voter_log proxy_log proxy_previous proxy_describe
  local pods_rc endpoints_rc metrics_rc consistent_rc local_rc ping_rc voter_log_rc proxy_log_rc proxy_previous_rc proxy_describe_rc
  local pods_started pods_ended endpoints_started endpoints_ended metrics_started metrics_ended consistent_started consistent_ended local_started local_ended ping_started ping_ended voter_log_started voter_log_ended proxy_log_started proxy_log_ended proxy_previous_started proxy_previous_ended proxy_describe_started proxy_describe_ended
  prefix="$target/${phase}-f2-post-ack"
  pods="$prefix-pods.raw"; endpoints="$prefix-endpoints.raw"; metrics="$prefix-metrics.raw"
  consistent="$prefix-consistent.raw"; local_query="$prefix-local.raw"; ping="$prefix-proxy-ping.raw"
  voter_log="$prefix-voter0-current.log"; proxy_log="$prefix-proxy-current.log"
  proxy_previous="$prefix-proxy-previous.log"; proxy_describe="$prefix-proxy-describe.log"

  pods_started="$(epoch_now)"; if k get pods -l app.kubernetes.io/component=voter -o json > "$pods" 2>&1; then pods_rc=0; else pods_rc=$?; fi; pods_ended="$(epoch_now)"
  endpoints_started="$(epoch_now)"; if k get endpointslice -l kubernetes.io/service-name=hiqlite-recovery-headless -o json > "$endpoints" 2>&1; then endpoints_rc=0; else endpoints_rc=$?; fi; endpoints_ended="$(epoch_now)"
  metrics_started="$(epoch_now)"; if run_direct_voter_client 0 "$probe_timeout" metrics > "$metrics" 2>&1; then metrics_rc=0; else metrics_rc=$?; fi; metrics_ended="$(epoch_now)"
  consistent_started="$(epoch_now)"; if run_direct_voter_client 0 "$probe_timeout" query-consistent "$id" > "$consistent" 2>&1; then consistent_rc=0; else consistent_rc=$?; fi; consistent_ended="$(epoch_now)"
  local_started="$(epoch_now)"; if run_direct_voter_client 0 "$probe_timeout" query-local "$id" > "$local_query" 2>&1; then local_rc=0; else local_rc=$?; fi; local_ended="$(epoch_now)"
  ping_started="$(epoch_now)"; if curl --fail --silent --max-time 1 "http://127.0.0.1:$host_port/ping" > "$ping" 2>&1; then ping_rc=0; else ping_rc=$?; fi; ping_ended="$(epoch_now)"
  voter_log_started="$(epoch_now)"; if k logs hiqlite-recovery-0 -c hiqlite > "$voter_log" 2>&1; then voter_log_rc=0; else voter_log_rc=$?; fi; voter_log_ended="$(epoch_now)"
  proxy_log_started="$(epoch_now)"; if k logs deployment/hiqlite-recovery-proxy -c proxy > "$proxy_log" 2>&1; then proxy_log_rc=0; else proxy_log_rc=$?; fi; proxy_log_ended="$(epoch_now)"
  proxy_previous_started="$(epoch_now)"; if k logs deployment/hiqlite-recovery-proxy -c proxy --previous > "$proxy_previous" 2>&1; then proxy_previous_rc=0; else proxy_previous_rc=$?; fi; proxy_previous_ended="$(epoch_now)"
  proxy_describe_started="$(epoch_now)"; if k describe deployment hiqlite-recovery-proxy > "$proxy_describe" 2>&1; then proxy_describe_rc=0; else proxy_describe_rc=$?; fi; proxy_describe_ended="$(epoch_now)"

  proof="$prefix-evidence.json"
  tmp="$proof.tmp.$$"
  if jq -n --arg phase "$phase" --arg id "$id" --arg value "$value" --argjson order "$order" --argjson captured "$(epoch_now)" \
    --rawfile pods_raw "$pods" --rawfile endpoints_raw "$endpoints" --rawfile metrics_raw "$metrics" \
    --rawfile consistent_raw "$consistent" --rawfile local_raw "$local_query" --rawfile ping_raw "$ping" \
    --rawfile voter_log_raw "$voter_log" --rawfile proxy_log_raw "$proxy_log" --rawfile proxy_previous_raw "$proxy_previous" --rawfile proxy_describe_raw "$proxy_describe" \
    --argjson pods_rc "$pods_rc" --argjson endpoints_rc "$endpoints_rc" --argjson metrics_rc "$metrics_rc" --argjson consistent_rc "$consistent_rc" --argjson local_rc "$local_rc" --argjson ping_rc "$ping_rc" --argjson voter_log_rc "$voter_log_rc" --argjson proxy_log_rc "$proxy_log_rc" --argjson proxy_previous_rc "$proxy_previous_rc" --argjson proxy_describe_rc "$proxy_describe_rc" \
    --argjson pods_started "$pods_started" --argjson pods_ended "$pods_ended" --argjson endpoints_started "$endpoints_started" --argjson endpoints_ended "$endpoints_ended" --argjson metrics_started "$metrics_started" --argjson metrics_ended "$metrics_ended" --argjson consistent_started "$consistent_started" --argjson consistent_ended "$consistent_ended" --argjson local_started "$local_started" --argjson local_ended "$local_ended" --argjson ping_started "$ping_started" --argjson ping_ended "$ping_ended" --argjson voter_log_started "$voter_log_started" --argjson voter_log_ended "$voter_log_ended" --argjson proxy_log_started "$proxy_log_started" --argjson proxy_log_ended "$proxy_log_ended" --argjson proxy_previous_started "$proxy_previous_started" --argjson proxy_previous_ended "$proxy_previous_ended" --argjson proxy_describe_started "$proxy_describe_started" --argjson proxy_describe_ended "$proxy_describe_ended" '
      (try ($local_raw|fromjson) catch null) as $local |
      (if $local_rc == 0 and $local != null and $local.found == true and $local.id == $id and $local.value == $value then "unilateral_state_machine_apply"
       elif $local_rc == 0 and $local != null and $local.found == false then "ack_without_local_apply"
       else "ack_post_state_unknown" end) as $classification |
      {schema_version:1,valid:true,cell_id:$phase,id:$id,value:$value,order:$order,captured_epoch:$captured,
       classification:$classification,
       sequence:[
         {order:1,kind:"pods",rc:$pods_rc,raw:$pods_raw,started_epoch:$pods_started,ended_epoch:$pods_ended},{order:2,kind:"endpoints",rc:$endpoints_rc,raw:$endpoints_raw,started_epoch:$endpoints_started,ended_epoch:$endpoints_ended},
         {order:3,kind:"direct_metrics",rc:$metrics_rc,raw:$metrics_raw,started_epoch:$metrics_started,ended_epoch:$metrics_ended},{order:4,kind:"direct_consistent",rc:$consistent_rc,raw:$consistent_raw,started_epoch:$consistent_started,ended_epoch:$consistent_ended},
         {order:5,kind:"direct_local",rc:$local_rc,raw:$local_raw,started_epoch:$local_started,ended_epoch:$local_ended},{order:6,kind:"proxy_ping",rc:$ping_rc,raw:$ping_raw,started_epoch:$ping_started,ended_epoch:$ping_ended},
         {order:7,kind:"voter0_current_logs",rc:$voter_log_rc,raw:$voter_log_raw,started_epoch:$voter_log_started,ended_epoch:$voter_log_ended},{order:8,kind:"proxy_current_logs",rc:$proxy_log_rc,raw:$proxy_log_raw,started_epoch:$proxy_log_started,ended_epoch:$proxy_log_ended},
         {order:9,kind:"proxy_previous_logs",rc:$proxy_previous_rc,raw:$proxy_previous_raw,started_epoch:$proxy_previous_started,ended_epoch:$proxy_previous_ended},{order:10,kind:"proxy_describe",rc:$proxy_describe_rc,raw:$proxy_describe_raw,started_epoch:$proxy_describe_started,ended_epoch:$proxy_describe_ended}
       ]}' > "$tmp" \
    && jq -e --arg phase "$phase" --arg id "$id" --arg value "$value" '
      .valid == true and .cell_id == $phase and .id == $id and .value == $value and
      (.classification == "unilateral_state_machine_apply" or .classification == "ack_without_local_apply" or .classification == "ack_post_state_unknown")
    ' "$tmp" >/dev/null; then
    mv "$tmp" "$proof"
  else
    rm -f -- "$tmp"
    # Do not lose the acknowledged write merely because embedding diagnostic
    # logs failed. This bounded descriptor deliberately contains paths+hashes,
    # never truncated or fabricated raw text.
    if ! jq -n --arg phase "$phase" --arg id "$id" --arg value "$value" --argjson order "$order" --argjson captured "$(epoch_now)" \
      --arg pods_path "$pods" --arg endpoints_path "$endpoints" --arg metrics_path "$metrics" --arg consistent_path "$consistent" --arg local_path "$local_query" --arg ping_path "$ping" --arg voter_log_path "$voter_log" --arg proxy_log_path "$proxy_log" --arg proxy_previous_path "$proxy_previous" --arg proxy_describe_path "$proxy_describe" \
      --arg pods_sha "$(openssl dgst -sha256 -r "$pods" | awk '{print $1}')" --arg endpoints_sha "$(openssl dgst -sha256 -r "$endpoints" | awk '{print $1}')" --arg metrics_sha "$(openssl dgst -sha256 -r "$metrics" | awk '{print $1}')" --arg consistent_sha "$(openssl dgst -sha256 -r "$consistent" | awk '{print $1}')" --arg local_sha "$(openssl dgst -sha256 -r "$local_query" | awk '{print $1}')" --arg ping_sha "$(openssl dgst -sha256 -r "$ping" | awk '{print $1}')" --arg voter_log_sha "$(openssl dgst -sha256 -r "$voter_log" | awk '{print $1}')" --arg proxy_log_sha "$(openssl dgst -sha256 -r "$proxy_log" | awk '{print $1}')" --arg proxy_previous_sha "$(openssl dgst -sha256 -r "$proxy_previous" | awk '{print $1}')" --arg proxy_describe_sha "$(openssl dgst -sha256 -r "$proxy_describe" | awk '{print $1}')" \
      --argjson pods_rc "$pods_rc" --argjson endpoints_rc "$endpoints_rc" --argjson metrics_rc "$metrics_rc" --argjson consistent_rc "$consistent_rc" --argjson local_rc "$local_rc" --argjson ping_rc "$ping_rc" --argjson voter_log_rc "$voter_log_rc" --argjson proxy_log_rc "$proxy_log_rc" --argjson proxy_previous_rc "$proxy_previous_rc" --argjson proxy_describe_rc "$proxy_describe_rc" '
      {schema_version:1,valid:true,embedded_raw:false,cell_id:$phase,id:$id,value:$value,order:$order,captured_epoch:$captured,classification:"ack_post_state_unknown",sequence:[
        {kind:"pods",path:$pods_path,sha256:$pods_sha,rc:$pods_rc},{kind:"endpoints",path:$endpoints_path,sha256:$endpoints_sha,rc:$endpoints_rc},{kind:"direct_metrics",path:$metrics_path,sha256:$metrics_sha,rc:$metrics_rc},{kind:"direct_consistent",path:$consistent_path,sha256:$consistent_sha,rc:$consistent_rc},{kind:"direct_local",path:$local_path,sha256:$local_sha,rc:$local_rc},{kind:"proxy_ping",path:$ping_path,sha256:$ping_sha,rc:$ping_rc},{kind:"voter0_current_logs",path:$voter_log_path,sha256:$voter_log_sha,rc:$voter_log_rc},{kind:"proxy_current_logs",path:$proxy_log_path,sha256:$proxy_log_sha,rc:$proxy_log_rc},{kind:"proxy_previous_logs",path:$proxy_previous_path,sha256:$proxy_previous_sha,rc:$proxy_previous_rc},{kind:"proxy_describe",path:$proxy_describe_path,sha256:$proxy_describe_sha,rc:$proxy_describe_rc}
      ]}' > "$tmp"; then
      die "$phase could not assemble post-ACK evidence descriptor"
    fi
    if ! jq -e --arg phase "$phase" --arg id "$id" --arg value "$value" '.valid == true and .embedded_raw == false and .classification == "ack_post_state_unknown" and .cell_id == $phase and .id == $id and .value == $value and (.sequence | length) == 10 and all(.sequence[]; (.path|type)=="string" and (.sha256|test("^[0-9a-f]{64}$")))' "$tmp" >/dev/null; then
      die "$phase fallback post-ACK evidence descriptor is invalid"
    fi
    mv "$tmp" "$proof"
  fi
  if [ ! -s "$proof" ] || ! jq -e '.valid == true and (.classification|type) == "string" and (.classification|length) > 0' "$proof" >/dev/null; then
    die "$phase post-ACK evidence is not a valid nonempty JSON proof"
  fi
  failure_establishment_post_ack_path="$proof"
  failure_establishment_post_ack_sha256="$(openssl dgst -sha256 -r "$proof" | awk '{print $1}')"
  failure_establishment_post_ack_classification="$(jq -er '.classification' "$proof")"
}

wait_f2_failure_established() {
  local phase="$1" query_id="$2" deadline remaining attempt proof_started proof_ended proof_path tmp pods endpoints metrics consistent write
  local pods_rc endpoints_rc metrics_rc consistent_rc write_rc ping_rc id value timeout_seconds write_started write_ended outcome ping post_ack_actual_sha
  deadline=$(( $(epoch_now) + quorum_loss_timeout ))
  transition_ledger_path="$target/${phase}-transition-ledger.jsonl"
  : > "$transition_ledger_path"
  transition_ledger_count=0
  attempt=0
  while [ "$(epoch_now)" -le "$deadline" ]; do
    attempt=$((attempt + 1))
    proof_started="$(epoch_now)"
    pods="$target/${phase}-f2-precondition-${attempt}-pods.raw"; endpoints="$target/${phase}-f2-precondition-${attempt}-endpoints.raw"
    metrics="$target/${phase}-f2-precondition-${attempt}-metrics.raw"; consistent="$target/${phase}-f2-precondition-${attempt}-consistent.raw"
    if k get pods -l app.kubernetes.io/component=voter -o json > "$pods" 2>&1; then pods_rc=0; else pods_rc=$?; fi
    if k get endpointslice -l kubernetes.io/service-name=hiqlite-recovery-headless -o json > "$endpoints" 2>&1; then endpoints_rc=0; else endpoints_rc=$?; fi
    remaining=$((deadline - $(epoch_now))); [ "$remaining" -ge 1 ] || break
    timeout_seconds=$probe_timeout; [ "$timeout_seconds" -le "$remaining" ] || timeout_seconds="$remaining"
    if run_direct_voter_client 0 "$timeout_seconds" metrics > "$metrics" 2>&1; then metrics_rc=0; else metrics_rc=$?; fi
    remaining=$((deadline - $(epoch_now))); [ "$remaining" -ge 1 ] || break
    timeout_seconds=$probe_timeout; [ "$timeout_seconds" -le "$remaining" ] || timeout_seconds="$remaining"
    if run_direct_voter_client 0 "$timeout_seconds" query-consistent "$query_id" > "$consistent" 2>&1; then consistent_rc=0; else consistent_rc=$?; fi
    proof_ended="$(epoch_now)"
    proof_path="$target/${phase}-failure-establishment-proof.json"
    tmp="$proof_path.tmp.$$"
    jq -n --arg phase "$phase" --argjson attempt "$attempt" --argjson started "$proof_started" --argjson ended "$proof_ended" \
      --rawfile pods_raw "$pods" --rawfile endpoints_raw "$endpoints" --rawfile metrics_raw "$metrics" --rawfile consistent_raw "$consistent" \
      --argjson pods_rc "$pods_rc" --argjson endpoints_rc "$endpoints_rc" --argjson metrics_rc "$metrics_rc" --argjson consistent_rc "$consistent_rc" '
      (try ($pods_raw|fromjson) catch null) as $pods | (try ($endpoints_raw|fromjson) catch null) as $endpoints |
      (try ($metrics_raw|fromjson) catch null) as $metrics |
      {cell_id:$phase,attempt:$attempt,precondition_started_epoch:$started,precondition_ended_epoch:$ended,
       sequence:[{kind:"pods",rc:$pods_rc,raw:$pods_raw},{kind:"endpoints",rc:$endpoints_rc,raw:$endpoints_raw},{kind:"metrics",rc:$metrics_rc,raw:$metrics_raw},{kind:"consistent",rc:$consistent_rc,raw:$consistent_raw}],
       proven:($pods_rc==0 and $endpoints_rc==0 and $metrics_rc==0 and $consistent_rc!=0 and
         ($pods.items|length)==1 and $pods.items[0].metadata.name=="hiqlite-recovery-0" and $pods.items[0].status.phase=="Running" and ($pods.items[0].status.conditions|any(.type=="Ready" and .status=="True")) and
         ([ $endpoints.items[]?.endpoints[]? ]|length)==1 and
         ([ $endpoints.items[]?.endpoints[]? | select(.targetRef.uid==$pods.items[0].metadata.uid and .targetRef.name=="hiqlite-recovery-0" and .conditions.ready==true) ]|length)==1 and
         $metrics.running==true and $metrics.voter_ids==[1,2,3] and $metrics.node_ids==[1,2,3] and
         ($consistent_raw|test("QuorumNotEnough") and test("got: \\{1\\}")))}' > "$tmp"
    mv "$tmp" "$proof_path"
    failure_establishment_proof_path="$proof_path"
    failure_establishment_proof_sha256="$(openssl dgst -sha256 -r "$proof_path" | awk '{print $1}')"
    if jq -e '.proven == true' "$proof_path" >/dev/null && [ $(( proof_ended - proof_started )) -le 2 ]; then
      remaining=$(( deadline - $(epoch_now) )); [ "$remaining" -ge 1 ] || break
      id="${phase}-transition-${run_id}"; value="transition-ack"
      write="$target/${phase}-f2-transition-write.raw"
      remaining=$(( deadline - $(epoch_now) )); [ "$remaining" -ge 1 ] || break
      timeout_seconds=$probe_timeout; [ "$timeout_seconds" -le "$remaining" ] || timeout_seconds="$remaining"
      ping="$target/${phase}-f2-precondition-${attempt}-proxy-ping.raw"
      if curl --fail --silent --max-time 1 "http://127.0.0.1:$host_port/ping" > "$ping" 2>&1; then ping_rc=0; else ping_rc=$?; fi
      if [ "$ping_rc" -ne 0 ]; then sleep 1; continue; fi
      write_started="$(epoch_now)"
      [ $(( write_started - proof_ended )) -le 2 ] || { sleep 1; continue; }
      if run_client_hard_timeout "$timeout_seconds" --nodes "127.0.0.1:$host_port" --secret "$api_secret" execute "$id" "$value" > "$write" 2>&1; then
        write_rc=0
      else
        write_rc=$?
      fi
      write_ended="$(epoch_now)"
      if [ "$write_rc" -eq 0 ]; then
        jq -e --arg id "$id" --arg value "$value" '.acknowledged==true and .id==$id and .value==$value' "$write" >/dev/null || die "$phase malformed transition acknowledgement"
        # A successful proxy response is itself the violation. Capture state
        # once, without replaying the mutation, before fail-closed cleanup.
        capture_f2_post_ack_evidence "$phase" "$id" "$value" "$attempt"
        outcome="write-ack-violation-$failure_establishment_post_ack_classification"
        post_ack_actual_sha="$(openssl dgst -sha256 -r "$failure_establishment_post_ack_path" | awk '{print $1}')"
        [ "$post_ack_actual_sha" = "$failure_establishment_post_ack_sha256" ] \
          || die "$phase post-ACK evidence SHA changed before failure-proof binding"
        tmp="$proof_path.tmp.$$"
        if ! jq --arg outcome "$outcome" --arg id "$id" --arg value "$value" --rawfile raw "$write" --rawfile ping_raw "$ping" --arg post_ack_path "$failure_establishment_post_ack_path" --arg post_ack_sha256 "$failure_establishment_post_ack_sha256" --arg post_ack_classification "$failure_establishment_post_ack_classification" --argjson ping_rc "$ping_rc" --argjson rc "$write_rc" --argjson started "$write_started" --argjson ended "$write_ended" \
          '. + {valid:true,outcome:$outcome,proof_end_epoch:.precondition_ended_epoch,write:{id:$id,value:$value,rc:$rc,raw:$raw,started_epoch:$started,ended_epoch:$ended,timeout_seconds:null,remaining_seconds:null},proxy_ping:{rc:$ping_rc,raw:$ping_raw},post_ack:{path:$post_ack_path,sha256:$post_ack_sha256,classification:$post_ack_classification}}' "$proof_path" > "$tmp"; then
          rm -f -- "$tmp"
          die "$phase could not assemble ACK failure proof"
        fi
        if ! jq -e --arg phase "$phase" --arg outcome "$outcome" --arg id "$id" --arg value "$value" --rawfile raw "$write" --arg post_ack_path "$failure_establishment_post_ack_path" --arg post_ack_sha256 "$failure_establishment_post_ack_sha256" --arg post_ack_classification "$failure_establishment_post_ack_classification" --slurpfile post_ack "$failure_establishment_post_ack_path" '
          .valid == true and .cell_id == $phase and .outcome == $outcome and
          .write.rc == 0 and .write.id == $id and .write.value == $value and .write.raw == $raw and
          .post_ack == {path:$post_ack_path,sha256:$post_ack_sha256,classification:$post_ack_classification} and
          ($post_ack | length) == 1 and $post_ack[0].valid == true and $post_ack[0].cell_id == $phase and
          $post_ack[0].id == $id and $post_ack[0].value == $value and $post_ack[0].classification == $post_ack_classification
        ' "$tmp" >/dev/null; then
          rm -f -- "$tmp"
          die "$phase ACK failure proof does not bind the acknowledgement and post-ACK evidence"
        fi
        if ! mv "$tmp" "$proof_path"; then
          rm -f -- "$tmp"
          die "$phase could not atomically publish ACK failure proof"
        fi
        if ! jq -e --arg outcome "$outcome" --arg id "$id" --arg value "$value" --arg post_ack_sha256 "$failure_establishment_post_ack_sha256" '.valid == true and .outcome == $outcome and .write.rc == 0 and .write.id == $id and .write.value == $value and .post_ack.sha256 == $post_ack_sha256' "$proof_path" >/dev/null; then
          die "$phase published ACK failure proof is invalid"
        fi
        failure_establishment_proof_sha256="$(openssl dgst -sha256 -r "$proof_path" | awk '{print $1}')"
        [ "${#failure_establishment_proof_sha256}" -eq 64 ] \
          || die "$phase cannot hash ACK failure proof"
        jq -cn --arg id "$id" --arg value "$value" '{id:$id,value:$value,acknowledged:true}' > "$transition_ledger_path"
        transition_ledger_count=1; transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
        record_event "$phase" failure-established fail-closed "$outcome" false "$(iso_now)" "$(iso_now)" 0 "attempt=$attempt proof=$failure_establishment_proof_sha256 post_ack=$failure_establishment_post_ack_sha256"
        die "no_quorum_$outcome"
      fi
      if grep -Eqi 'QuorumNotEnough|no.quorum|got: \{1\}' "$write"; then
        outcome="application-no-quorum-rejection"
        tmp="$proof_path.tmp.$$"
        jq --arg outcome "$outcome" --arg id "$id" --arg value "$value" --rawfile raw "$write" --rawfile ping_raw "$ping" --argjson ping_rc "$ping_rc" --argjson rc "$write_rc" --argjson started "$write_started" --argjson ended "$write_ended" --argjson timeout "$timeout_seconds" --argjson remaining "$remaining" \
          '. + {valid:true,outcome:$outcome,proof_end_epoch:.precondition_ended_epoch,write:{id:$id,value:$value,rc:$rc,raw:$raw,started_epoch:$started,ended_epoch:$ended,timeout_seconds:$timeout,remaining_seconds:$remaining},proxy_ping:{rc:$ping_rc,raw:$ping_raw}}' "$proof_path" > "$tmp" && mv "$tmp" "$proof_path"
        failure_establishment_proof_sha256="$(openssl dgst -sha256 -r "$proof_path" | awk '{print $1}')"
        transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
        record_event "$phase" failure-established fail-closed fail-closed true "$(iso_now)" "$(iso_now)" 0 "attempt=$attempt proof=$failure_establishment_proof_sha256"
        return 0
      fi
      if curl --fail --silent --max-time 1 "http://127.0.0.1:$host_port/ping" >/dev/null 2>&1; then
        outcome="no_ack_unknown"
        tmp="$proof_path.tmp.$$"
        jq --arg outcome "$outcome" --arg id "$id" --arg value "$value" --rawfile raw "$write" --rawfile ping_raw "$ping" --argjson ping_rc "$ping_rc" --argjson rc "$write_rc" --argjson started "$write_started" --argjson ended "$write_ended" --argjson timeout "$timeout_seconds" --argjson remaining "$remaining" \
          '. + {valid:true,outcome:$outcome,proof_end_epoch:.precondition_ended_epoch,write:{id:$id,value:$value,rc:$rc,raw:$raw,started_epoch:$started,ended_epoch:$ended,timeout_seconds:$timeout,remaining_seconds:$remaining},proxy_ping:{rc:$ping_rc,raw:$ping_raw}}' "$proof_path" > "$tmp" && mv "$tmp" "$proof_path"
        failure_establishment_proof_sha256="$(openssl dgst -sha256 -r "$proof_path" | awk '{print $1}')"
        transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
        record_event "$phase" failure-established fail-closed no-ack-unknown true "$(iso_now)" "$(iso_now)" 0 "attempt=$attempt id=$id value=$value proof=$failure_establishment_proof_sha256"
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

resolve_f2_unknown_write() {
  local cell_id="$1" branch="$2" proof id value output tmp
  proof="$failure_establishment_proof_path"
  [ -f "$proof" ] || die "missing F2 establishment proof for resolution"
  [ "$(jq -r '.outcome // empty' "$proof")" = no_ack_unknown ] || return 0
  id="$(jq -er '.write.id' "$proof")"; value="$(jq -er '.write.value' "$proof")"
  output="$target/f2-unknown-write-resolution.json"
  tmp="$output.tmp.$$"
  if ! run_client "$probe_timeout" query-consistent "$id" > "$output.raw" 2>&1; then
    die "F2 unknown write resolution query failed"
  fi
  jq -n --arg cell_id "$cell_id" --arg branch "$branch" --arg id "$id" --arg value "$value" --arg raw "$(<"$output.raw")" '
    (try ($raw|fromjson) catch null) as $result |
    {cell_id:$cell_id,mode:$branch,outcome:"no_ack_unknown",id:$id,value:$value,raw:$raw,
     valid:(if $branch == "operator-dr" then $result.found == false
       else ($result.found == false or ($result.found == true and $result.id == $id and $result.value == $value)) end)}' > "$tmp"
  mv "$tmp" "$output"
  jq -e '.valid == true' "$output" >/dev/null || die "F2 unknown write resolved to conflicting or malformed value"
  failure_establishment_resolution_path="$output"
  failure_establishment_resolution_sha256="$(openssl dgst -sha256 -r "$output" | awk '{print $1}')"
}

wait_failure_established() {
  local phase="$1" local_must_fail="$2" query_id="$3"
  local deadline attempt transition_id transition_value write_failed consistent_failed local_failed
  local transient_write_acks=0 out_prefix
  [ "$local_must_fail" = false ] && { wait_f2_failure_established "$phase" "$query_id"; return $?; }
  deadline=$(( $(epoch_now) + quorum_loss_timeout ))
  transition_ledger_path="$target/${phase}-transition-ledger.jsonl"
  : > "$transition_ledger_path"
  transition_ledger_count=0
  transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
  attempt=0
  while [ "$(epoch_now)" -le "$deadline" ]; do
    attempt=$((attempt + 1))
    transition_id="${phase}-transition-${attempt}-${run_id}"
    transition_value="transition-ack-${attempt}"
    out_prefix="$target/${phase}-failure-transition-${attempt}"
    write_failed=false
    consistent_failed=false
    local_failed=false
    if run_client "$probe_timeout" execute "$transition_id" "$transition_value" \
      > "${out_prefix}-write.out" 2>&1; then
      if jq -e '.acknowledged == true' "${out_prefix}-write.out" >/dev/null; then
        jq -cn --arg id "$transition_id" --arg value "$transition_value" \
          '{id:$id,value:$value,acknowledged:true}' >> "$transition_ledger_path"
        transition_ledger_count=$((transition_ledger_count + 1))
        transient_write_acks=$((transient_write_acks + 1))
        remember_sentinel "$transition_id"
      else
        die "$phase transition write exited 0 without acknowledged:true"
      fi
    else
      write_failed=true
    fi
    if ! run_client "$probe_timeout" query-consistent "$query_id" \
      > "${out_prefix}-consistent.out" 2>&1; then
      consistent_failed=true
    fi
    if [ "$local_must_fail" = false ] || ! run_client "$probe_timeout" query-local "$query_id" \
      > "${out_prefix}-local.out" 2>&1; then
      local_failed=true
    fi
    if "$write_failed" && "$consistent_failed" && "$local_failed"; then
      transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
      record_event "$phase" failure-established fail-closed fail-closed true \
        "$(iso_now)" "$(iso_now)" 0 \
        "attempts=$attempt transient_write_acks=$transient_write_acks local_must_fail=$local_must_fail"
      return 0
    fi
    sleep 1
  done
  record_event "$phase" failure-established fail-closed transition-never-quiesced false \
    "$(iso_now)" "$(iso_now)" "$quorum_loss_timeout" \
    "attempts=$attempt transient_write_acks=$transient_write_acks local_must_fail=$local_must_fail"
  return 1
}

initialize_transition_ledger() {
  local cell_id="$1"
  transition_ledger_path="$target/${cell_id}-transition-ledger.jsonl"
  : > "$transition_ledger_path"
  transition_ledger_count=0
  transition_ledger_sha256="$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')"
}

verify_transition_ledger() {
  local mode="$1" id value actual_count
  actual_count="$(jq -s 'length' "$transition_ledger_path")"
  [ "$actual_count" -eq "$transition_ledger_count" ] \
    || die "transition ledger count does not match its JSONL records"
  jq -s -e 'all(.[]; .acknowledged == true and (.id|type) == "string" and (.id|length) > 0 and
    (.value|type) == "string" and (.value|length) > 0) and
    (map(.id) | unique | length) == length' "$transition_ledger_path" >/dev/null \
    || die "transition ledger contains an invalid or duplicate acknowledgement"
  while IFS=$'\t' read -r id value; do
    [ -n "$id" ] || continue
    case "$mode" in
      present) run_client 30 verify-sentinel "$id" "$value" >/dev/null \
        || die "acknowledged transition write was lost: $id" ;;
      absent) verify_missing "$id" || die "operator DR retained transition write: $id" ;;
      *) die "unknown transition ledger verification mode: $mode" ;;
    esac
  done < <(jq -r 'select(.acknowledged == true) | [.id,.value] | @tsv' "$transition_ledger_path")
}

assert_probe_outcome() {
  local phase="$1" operation="$2" expected_success="$3"
  jq -s -e \
    --arg phase "$phase" \
    --arg operation "$operation" \
    --argjson expected_success "$expected_success" \
    '([.[] | select(.phase == $phase and .event == $operation)]) as $samples |
      ($samples | length) > 0 and ($samples | all(.success == $expected_success))' \
    "$jsonl" >/dev/null
}

metrics_to() {
  local output="$1" stderr_output rc_output status
  stderr_output="${output%.json}.stderr"
  rc_output="${output%.json}.rc"
  if run_client "$probe_timeout" metrics > "$output" 2> "$stderr_output"; then
    status=0
  else
    status=$?
  fi
  printf '%s\n' "$status" > "$rc_output"
  return "$status"
}

metrics_to_recoverable() {
  local output="$1" stderr_output rc_output status
  stderr_output="${output%.json}.stderr"
  rc_output="${output%.json}.rc"
  if run_client_recoverable "$probe_timeout" metrics > "$output" 2> "$stderr_output"; then status=0; else status=$?; fi
  printf '%s\n' "$status" > "$rc_output"
  return "$status"
}

capture_convergence_diagnostics() {
  local label="$1" voter
  k get statefulset hiqlite-recovery -o yaml > "$target/${label}-statefulset.yaml" 2>&1 || true
  k get pods -l app.kubernetes.io/component=voter -o wide > "$target/${label}-voter-pods.txt" 2>&1 || true
  k get pods -l app.kubernetes.io/component=voter -o json > "$target/${label}-voter-pods.json" 2>&1 || true
  k get events --sort-by=.metadata.creationTimestamp > "$target/${label}-events.txt" 2>&1 || true
  k get deployment hiqlite-recovery-proxy -o yaml > "$target/${label}-proxy-deployment.yaml" 2>&1 || true
  k get pods -l app.kubernetes.io/component=proxy -o wide > "$target/${label}-proxy-pods.txt" 2>&1 || true
  k describe deployment hiqlite-recovery-proxy > "$target/${label}-proxy-describe.txt" 2>&1 || true
  k get endpointslice -l kubernetes.io/service-name=hiqlite-recovery-proxy -o yaml > "$target/${label}-proxy-endpoints.yaml" 2>&1 || true
  for voter in $(k get pods -l app.kubernetes.io/component=proxy -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
    k logs "$voter" -c proxy > "$target/${label}-${voter}-proxy.log" 2>&1 || true
    k logs "$voter" -c proxy --previous > "$target/${label}-${voter}-proxy-previous.log" 2>&1 || true
  done
  for voter in hiqlite-recovery-0 hiqlite-recovery-1 hiqlite-recovery-2; do
    k describe pod "$voter" > "$target/${label}-${voter}-describe.txt" 2>&1 || true
    k logs "$voter" -c hiqlite > "$target/${label}-${voter}-current.log" 2>&1 || true
    k logs "$voter" -c hiqlite --previous > "$target/${label}-${voter}-previous.log" 2>&1 || true
  done
}

wait_service() {
  local timeout_seconds="$1" sentinel_id="$2" sentinel_value="$3" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if run_client "$probe_timeout" query-consistent "$sentinel_id" \
      | jq -e --arg value "$sentinel_value" '.found == true and .value == $value' \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_convergence() {
  local timeout_seconds="$1" output="$2" deadline label
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if [ "$(ready_replicas)" = 3 ] && metrics_to_recoverable "$output" \
      && jq -e '.running == true and .current_leader != null and
        .voter_ids == [1,2,3] and .node_ids == [1,2,3]' "$output" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  label="${output##*/}"
  label="${label%.json}"
  capture_convergence_diagnostics "$label"
  return 1
}

capture_uids() {
  local output="$1"
  k get pods -l app.kubernetes.io/component=voter -o json \
    | jq '[.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)' > "$output"
}

require_exact_voter_uids() {
  local uids_file="$1"
  jq -e '
    length == 3 and
    map(.name) == ["hiqlite-recovery-0", "hiqlite-recovery-1", "hiqlite-recovery-2"] and
    all(.[]; (.uid | type) == "string" and length > 0)
  ' "$uids_file" >/dev/null
}

require_replaced_voter_uids() {
  local old_uids_file="$1" new_uids_file="$2" ordinal pod old_uid new_uid
  require_exact_voter_uids "$old_uids_file" || return 1
  require_exact_voter_uids "$new_uids_file" || return 1
  for ordinal in 0 1 2; do
    pod="hiqlite-recovery-$ordinal"
    old_uid="$(jq -er --arg pod "$pod" '.[] | select(.name == $pod) | .uid' "$old_uids_file")"
    new_uid="$(jq -er --arg pod "$pod" '.[] | select(.name == $pod) | .uid' "$new_uids_file")"
    [ "$old_uid" != "$new_uid" ] || return 1
  done
}

wait_namespace_absent() {
  local candidate="$1" timeout_seconds="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    kubectl --context "$context" get namespace "$candidate" >/dev/null 2>&1 || return 0
    sleep 1
  done
  return 1
}

assert_clean_voter_lifecycle() {
  local cell_id="$1"
  if k get statefulset hiqlite-recovery -o json \
    | jq -e '.spec.template.spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")' >/dev/null; then
    die "$cell_id isolation retained HQL_BACKUP_RESTORE in the StatefulSet template"
  fi
  if k get pods -o json \
    | jq -e '[.items[].spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "$cell_id isolation retained HQL_BACKUP_RESTORE on a voter"
  fi
  [ "$(kobj get pod -l app.kubernetes.io/component=object-store \
    -o jsonpath='{.items[0].metadata.uid}')" = "$rustfs_uid" ] \
    || die "$cell_id isolation changed the RustFS Pod"
  [ -z "$(k get persistentvolumeclaims -o name)$(kobj get persistentvolumeclaims -o name)" ] \
    || die "$cell_id isolation no longer has zero PVCs"
  if k get pods -o json \
    | jq -e '[.items[].spec.volumes[]? | select(has("hostPath"))] | length > 0' >/dev/null; then
    die "$cell_id isolation created a voter hostPath"
  fi
}

capture_cell_identity() {
  local output="$1" namespace_json statefulset_json voters_json proxy_json revisions_json endpoints_json
  namespace_json="$(kubectl --context "$context" get namespace "$namespace" -o json)"
  statefulset_json="$(k get statefulset hiqlite-recovery -o json)"
  voters_json="$(k get pods -l app.kubernetes.io/component=voter -o json)"
  proxy_json="$(k get pods -l app.kubernetes.io/component=proxy -o json)"
  revisions_json="$(k get controllerrevision -o json)"
  endpoints_json="$(k get endpointslice -l kubernetes.io/service-name=hiqlite-recovery-headless -o json)"
  jq -n \
    --argjson namespace "$namespace_json" \
    --argjson statefulset "$statefulset_json" \
    --argjson voters "$voters_json" \
    --argjson proxy "$proxy_json" \
    --argjson revisions "$revisions_json" \
    --argjson endpoints "$endpoints_json" '
      ($statefulset.status.updateRevision) as $revision |
      {namespace:{name:$namespace.metadata.name,uid:$namespace.metadata.uid},
       statefulset:{uid:$statefulset.metadata.uid,update_revision:$revision},
       voters:([$voters.items[] | {name:.metadata.name,uid:.metadata.uid,
         controller_revision_hash:.metadata.labels["controller-revision-hash"]}] | sort_by(.name)),
       proxy_pods:([$proxy.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)),
       controller_revision:([$revisions.items[] | select(.metadata.name == $revision) |
         {name:.metadata.name,uid:.metadata.uid}][0]),
       endpoint_target_uids:[$endpoints.items[]?.endpoints[]?.targetRef? |
         select(.kind == "Pod") | .uid] | unique}' > "$output"
}

require_cell_identity() {
  local identity_file="$1"
  jq -e '
    (.statefulset.update_revision) as $update_revision |
    (.namespace.uid | type) == "string" and (.namespace.uid | length) > 0 and
    (.statefulset.uid | type) == "string" and (.statefulset.uid | length) > 0 and
    (.statefulset.update_revision | type) == "string" and (.statefulset.update_revision | length) > 0 and
    (.controller_revision.uid | type) == "string" and (.controller_revision.uid | length) > 0 and
    (.voters | map(.name)) == ["hiqlite-recovery-0", "hiqlite-recovery-1", "hiqlite-recovery-2"] and
    (.voters | all(.[]; (.uid | type) == "string" and length > 0)) and
    (.voters | all(.[]; .controller_revision_hash == $update_revision)) and
    (.proxy_pods | length) == 1 and
    (.proxy_pods | all(.[]; (.uid | type) == "string" and length > 0)) and
    (.endpoint_target_uids | length) == 3 and
    (.endpoint_target_uids | sort) == (.voters | map(.uid) | sort)
  ' "$identity_file" >/dev/null
}

require_no_old_identity_uids() {
  local old_file="$1" new_file="$2" old_uids new_uids uid
  old_uids="$(jq -r '.. | .uid? // empty' "$old_file" | LC_ALL=C sort -u)"
  new_uids="$(jq -r '.. | .uid? // empty' "$new_file" | LC_ALL=C sort -u)"
  while IFS= read -r uid; do
    [ -z "$uid" ] && continue
    ! grep -Fqx -- "$uid" <<< "$new_uids" || return 1
  done <<< "$old_uids"
}

verify_direct_empty_baseline() {
  local cell_id="$1" ordinal output status absent_count records tmp
  absent_count=0
  records='[]'
  cell_baseline_direct_reads=false
  cell_baseline_evidence_path="$target/${cell_id}-fresh-baseline.json"
  for ordinal in 0 1 2; do
    output="$target/${cell_id}-baseline-voter-${ordinal}-pre-reset.raw"
    if run_direct_voter_client "$ordinal" "$probe_timeout" query-local baseline > "$output" 2>&1; then
      status=0
    else
      status=$?
    fi
    if [ "$status" -ne 0 ] && grep -Fq 'no such table: hiqlite_recovery_sentinel' "$output"; then
      absent_count=$((absent_count + 1))
      records="$(jq -cn --argjson prior "$records" --argjson ordinal "$ordinal" --argjson rc "$status" --arg raw "$(<"$output")" \
        '$prior + [{ordinal:$ordinal,rc:$rc,classification:"absent-table",raw:$raw}]')"
    else
      records="$(jq -cn --argjson prior "$records" --argjson ordinal "$ordinal" --argjson rc "$status" --arg raw "$(<"$output")" \
        '$prior + [{ordinal:$ordinal,rc:$rc,classification:"unexpected",raw:$raw}]')"
    fi
  done
  tmp="$cell_baseline_evidence_path.tmp.$$"
  jq -n --arg cell_id "$cell_id" --argjson records "$records" \
    '{cell_id:$cell_id,stage:"pre-reset",records:$records,
      valid:(($records|length)==3 and ($records|all(.[]; .classification=="absent-table")))}' > "$tmp"
  mv "$tmp" "$cell_baseline_evidence_path"
  if [ "$absent_count" -ne 3 ] || ! jq -e '.valid == true' "$cell_baseline_evidence_path" >/dev/null; then
    die "$cell_id fresh isolation did not prove all voters have an absent sentinel table before reset"
  fi
  cell_baseline_pre_records="$records"
}

verify_direct_reset_baseline() {
  local cell_id="$1" ordinal output records tmp deadline attempts status classification
  records='[]'
  for ordinal in 0 1 2; do
    deadline=$(( $(epoch_now) + recovery_timeout ))
    output="$target/${cell_id}-baseline-voter-${ordinal}-post-reset.json"
    attempts=0
    classification="timeout"
    while [ "$(epoch_now)" -le "$deadline" ]; do
      attempts=$((attempts + 1))
      if run_direct_voter_client "$ordinal" "$probe_timeout" query-local baseline > "$output" 2>&1; then
        status=0
        case "$(jq -r 'if .found == false then "empty" elif .found == true then "retained" else "malformed" end' "$output" 2>/dev/null || printf 'malformed')" in
          empty) classification="empty"; break ;;
          retained) die "$cell_id voter $ordinal retained a sentinel after reset" ;;
          *) die "$cell_id voter $ordinal returned malformed successful baseline JSON" ;;
        esac
      else
        status=$?
        if grep -Fq 'no such table: hiqlite_recovery_sentinel' "$output" || grep -Eqi 'connection refused|timed out|transport|unavailable' "$output"; then
          sleep 1
          continue
        fi
        die "$cell_id voter $ordinal post-reset baseline query failed"
      fi
    done
    [ "$classification" = empty ] || die "$cell_id voter $ordinal did not converge to post-reset empty baseline"
    records="$(jq -cn --argjson prior "$records" --argjson ordinal "$ordinal" --argjson attempts "$attempts" --argjson rc "$status" --arg raw "$(<"$output")" \
      '$prior + [{ordinal:$ordinal,attempts:$attempts,rc:$rc,classification:"empty",raw:$raw}]')"
  done
  output="$target/${cell_id}-baseline-proof.json"
  tmp="$output.tmp.$$"
  jq -n --arg cell_id "$cell_id" --arg reset_raw "$cell_baseline_reset_raw" --argjson pre "$cell_baseline_pre_records" --argjson post "$records" \
    '{cell_id:$cell_id,pre:{records:$pre},reset:{rc:0,acknowledged:true,raw:$reset_raw},post:{records:$post},valid:(($pre|length)==3 and ($pre|all(.[];.classification=="absent-table")) and ($post|length)==3 and ($post|all(.[];.classification=="empty")))}' > "$tmp"
  mv "$tmp" "$output"
  cell_baseline_direct_reads=true
  cell_baseline_evidence_path="$output"
  cell_baseline_evidence_sha256="$(openssl dgst -sha256 -r "$output" | awk '{print $1}')"
}

remember_sentinel() {
  local id="$1" seen
  for seen in "${previous_sentinel_ids[@]}"; do [ "$seen" != "$id" ] || return; done
  previous_sentinel_ids+=("$id")
}

delete_previous_cell_namespace() {
  local candidate="$1" managed owner
  kubectl --context "$context" get namespace "$candidate" >/dev/null 2>&1 || return 0
  managed="$(kubectl --context "$context" get namespace "$candidate" \
    -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}')"
  owner="$(kubectl --context "$context" get namespace "$candidate" \
    -o go-template='{{index .metadata.labels "rhiza.dev/e2e-run-id"}}')"
  [ "$managed" = true ] && [ "$owner" = "$run_id" ] \
    || die "refusing to delete unmanaged previous cell namespace $candidate"
  if k get statefulset hiqlite-recovery >/dev/null 2>&1; then
    k delete statefulset hiqlite-recovery --cascade=foreground --wait=true >/dev/null
  fi
  kubectl --context "$context" delete namespace "$candidate" --wait=true >/dev/null
  wait_namespace_absent "$candidate" "$recovery_timeout" \
    || die "previous cell namespace $candidate did not terminate"
}

prepare_matrix_cell() {
  local cell_id="$1" old_identity new_identity started_at started_epoch
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  old_identity="$target/${cell_id}-isolation-old-identity.json"
  capture_cell_identity "$old_identity"
  stop_port_forwards
  delete_previous_cell_namespace "$namespace"
  namespace="hiqlite-cell-${run_id}-${cell_id}-${matrix_cell_index}"
  create_managed_namespace "$namespace"
  cell_namespaces+=("$namespace")
  k apply -f "$target/hiqlite.yaml" >/dev/null
  wait_ready_replicas 3 "$recovery_timeout" \
    || die "$cell_id fresh namespace did not have three ready voters"
  k rollout status deployment/hiqlite-recovery-proxy --timeout="${recovery_timeout}s" >/dev/null
  verify_live_image_ids
  initialize_transition_ledger "$cell_id"
  cell_image_proofs_json='[]'
  cell_expected_image_proof_stages_json='[]'
  capture_cell_image_proof "$cell_id" pre-fault
  wait_convergence "$recovery_timeout" "$target/${cell_id}-isolation-metrics.json" \
    || die "$cell_id fresh namespace did not converge"
  new_identity="$target/${cell_id}-isolation-identity.json"
  capture_cell_identity "$new_identity"
  require_cell_identity "$new_identity" \
    || die "$cell_id identity proof is incomplete"
  require_no_old_identity_uids "$old_identity" "$new_identity" \
    || die "$cell_id fresh namespace retained an old identity UID"
  cell_isolation_mode=fresh-managed-namespace
  cell_isolation_uid_proof=true
  cell_isolation_identity_path="$new_identity"
  cell_backup_key_unique=true
  cell_backup_key=""
  cell_backup_evidence_path=""
  cell_backup_post_digest=""
  assert_clean_voter_lifecycle "$cell_id"
  start_port_forward
  start_direct_voter_port_forwards
  verify_direct_empty_baseline "$cell_id"
  record_event "$cell_id" cell-isolation "$cell_isolation_mode" "$cell_isolation_mode" true \
    "$started_at" "$(iso_now)" "$(( $(epoch_now) - started_epoch ))" \
    "namespace=$namespace uid_proof=$cell_isolation_uid_proof rustfs_uid=$rustfs_uid zero_pvc=true"
  matrix_cell_index=$((matrix_cell_index + 1))
}

markers_lost() {
  local old_uids_file="$1" ordinal pod old_uid new_uid
  shift
  for ordinal in "$@"; do
    pod="hiqlite-recovery-$ordinal"
    old_uid="$(jq -er --arg pod "$pod" '.[] | select(.name == $pod) | .uid' "$old_uids_file")"
    new_uid="$(k get pod "$pod" -o jsonpath='{.metadata.uid}')"
    [ "$old_uid" != "$new_uid" ] || return 1
    k exec "$pod" -c marker-inspector -- test -f "/marker/emptydir-marker-$new_uid"
    k exec "$pod" -c marker-inspector -- test ! -e "/marker/emptydir-marker-$old_uid"
  done
}

verify_ack_file() {
  local ack_file="$1" id value
  while IFS=$'\t' read -r id value; do
    [ -n "$id" ] || continue
    run_client "$probe_timeout" verify-sentinel "$id" "$value" >/dev/null
  done < "$ack_file"
}

capture_learner_to_voter_evidence() {
  local phase="$1" node_id="$2" since_time="$3" pod
  local output="$target/${phase}-learner-to-voter.log"
  : > "$output"
  k get pods -l app.kubernetes.io/component=voter -o name \
    | while IFS= read -r pod; do
        k logs "$pod" -c hiqlite --since-time="$since_time" >> "$output" 2>&1 || true
      done
  grep -Eq "Added node ${node_id} as .* learner" "$output" \
    && grep -Eq "Added node ${node_id} as .* member" "$output"
}

verify_missing() {
  local id="$1"
  run_client "$probe_timeout" query-consistent "$id" | jq -e '.found == false' >/dev/null
}

list_backup_objects() {
  kobj exec deployment/rustfs-tools -- aws --endpoint-url http://rustfs:9000 \
    s3api list-objects-v2 --bucket hiqlite --output json
}

capture_object_inventory() {
  local label="$1" require_empty="${2:-false}" output digest
  output="$target/${label}-object-inventory.json"
  list_backup_objects > "$output"
  jq -e '(.Contents // []) | all(.[]; (.Key | type) == "string" and length > 0)' "$output" >/dev/null
  if [ "$require_empty" = true ]; then
    jq -e '((.Contents // []) | length) == 0' "$output" >/dev/null \
      || die "$label expected a new empty RustFS bucket"
  fi
  digest="$(openssl dgst -sha256 -r "$output" | awk '{print $1}')"
  printf '%s\n' "$digest"
}

trigger_external_backup() {
  local label="$1" deadline objects key head_json candidate seen
  triggered_backup_key=""
  local before="$target/${label}-objects-before.txt" before_digest
  before_digest="$(capture_object_inventory "$label-before")"
  jq -r '.Contents[]?.Key' "$target/${label}-before-object-inventory.json" | sort > "$before"
  run_client 30 backup > "$target/${label}-backup-trigger.json"
  deadline=$(( $(epoch_now) + recovery_timeout ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    objects="$target/${label}-objects-current.json"
    if list_backup_objects > "$objects" 2>/dev/null; then
      key="$(jq -r '.Contents[]?.Key' "$objects" \
        | while IFS= read -r candidate; do
            if ! grep -Fqx -- "$candidate" "$before"; then printf '%s\n' "$candidate"; fi
          done | tail -n 1)"
      if [ -n "$key" ]; then
        head_json="$target/${label}-backup-head.json"
        if kobj exec deployment/rustfs-tools -- aws --endpoint-url http://rustfs:9000 \
          s3api head-object --bucket hiqlite --key "$key" --output json > "$head_json" \
          && jq -e '.ContentLength > 0 and ((.ETag | type) == "string" and (.ETag | length) > 0 or (.VersionId | type) == "string" and (.VersionId | length) > 0)' "$head_json" >/dev/null; then
          if (( ${#seen_backup_keys[@]} > 0 )); then
            for seen in "${seen_backup_keys[@]}"; do
              [ "$seen" != "$key" ] || die "$label backup key was reused: $key"
            done
          fi
          seen_backup_keys+=("$key")
          triggered_backup_key="$key"
          printf '%s\n' "$key" > "$target/${label}-backup-key.txt"
          cell_backup_post_digest="$(capture_object_inventory "$label-after")"
          cell_backup_evidence_path="$target/${label}-backup-evidence.json"
          jq -n --arg bucket hiqlite --arg key "$key" --arg before_digest "$before_digest" \
            --arg after_digest "$cell_backup_post_digest" --argjson head "$(cat "$head_json")" \
            '{bucket:$bucket,key:$key,inventory_before_sha256:$before_digest,
              inventory_after_sha256:$after_digest,head:$head}' > "$cell_backup_evidence_path"
          return 0
        fi
      fi
    fi
    sleep 1
  done
  return 1
}

set_restore_object() {
  local key="$1"
  k set env statefulset/hiqlite-recovery "HQL_BACKUP_RESTORE=s3:$key" >/dev/null
}

clear_restore_object() {
  k set env statefulset/hiqlite-recovery HQL_BACKUP_RESTORE- >/dev/null
  if k get statefulset hiqlite-recovery -o json \
    | jq -e '.spec.template.spec.containers[] | select(.name == "hiqlite") |
      .env // [] | any(.name == "HQL_BACKUP_RESTORE")' >/dev/null; then
    die "HQL_BACKUP_RESTORE remained in the StatefulSet template"
  fi
}

require_current_endpoint_target() {
  local pod="$1" uid="$2"
  k get endpointslice -l kubernetes.io/service-name=hiqlite-recovery-headless -o json \
    | jq -e --arg pod "$pod" --arg uid "$uid" '
      [.items[]?.endpoints[]?.targetRef? |
        select(.kind == "Pod" and .name == $pod and .uid == $uid)] | length == 1
    ' >/dev/null
}

clear_restore_from_running_pods() {
  local phase="$1" sentinel_id="$2" sentinel_value="$3" ordinal pod old_uid new_uid revision
  clear_restore_object
  for ordinal in 2 1 0; do
    pod="hiqlite-recovery-$ordinal"
    old_uid="$(k get pod "$pod" -o jsonpath='{.metadata.uid}')"
    k get pod "$pod" -o json > "$target/${phase}-restore-clear-${ordinal}-old-pod.json"
    k logs "$pod" -c hiqlite > "$target/${phase}-restore-clear-${ordinal}-old.log" 2>&1 || true
    k logs "$pod" -c hiqlite --previous > "$target/${phase}-restore-clear-${ordinal}-old-previous.log" 2>&1 || true
    k delete pod "$pod" --wait=true >/dev/null
    wait_ready_pod "$pod" "$recovery_timeout" \
      || die "$pod did not become ready without HQL_BACKUP_RESTORE"
    new_uid="$(k get pod "$pod" -o jsonpath='{.metadata.uid}')"
    [ "$old_uid" != "$new_uid" ] \
      || die "$phase restore clear did not replace $pod"
    revision="$(k get statefulset hiqlite-recovery -o jsonpath='{.status.updateRevision}')"
    [ "$(k get pod "$pod" -o jsonpath='{.metadata.labels.controller-revision-hash}')" = "$revision" ] \
      || die "$phase restore clear $pod did not use StatefulSet updateRevision"
    if k get pod "$pod" -o json | jq -e '.spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")' >/dev/null; then
      die "$phase restore clear retained HQL_BACKUP_RESTORE on $pod"
    fi
    require_current_endpoint_target "$pod" "$new_uid" \
      || die "$phase restore clear EndpointSlice did not reference current $pod"
    wait_direct_quorum_after_restore "$phase" "$pod" "$new_uid" \
      || die "$phase direct quorum did not converge after replacing $pod"
  done
  if k get pods -l app.kubernetes.io/component=voter -o json \
    | jq -e '[.items[].spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "HQL_BACKUP_RESTORE remained on a running voter after $phase recovery"
  fi
  wait_proxy_readonly_after_restore "$phase" "$sentinel_id" "$sentinel_value" \
    || die "$phase proxy did not recover for read-only sentinel verification"
  run_idempotent_restore_write "$phase"
}

wait_direct_quorum_after_restore() {
  local phase="$1" pod="$2" expected_uid="$3" deadline output
  deadline=$(( $(epoch_now) + recovery_timeout ))
  start_direct_voter_port_forwards
  while [ "$(epoch_now)" -le "$deadline" ]; do
    [ "$(k get pod "$pod" -o jsonpath='{.metadata.uid}')" = "$expected_uid" ] \
      || die "$phase restore clear lost current Pod UID for $pod"
    require_current_endpoint_target "$pod" "$expected_uid" \
      || die "$phase restore clear EndpointSlice did not reference current $pod"
    output="$target/${phase}-${pod}-direct-quorum-metrics.json"
    if run_direct_voter_client 0 "$probe_timeout" metrics > "$output" 2>&1 \
      && jq -e '.running == true and .current_leader != null and .voter_ids == [1,2,3] and .node_ids == [1,2,3]' "$output" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  capture_convergence_diagnostics "${phase}-${pod}-direct-quorum-timeout"
  return 1
}

wait_proxy_readonly_after_restore() {
  local phase="$1" sentinel_id="$2" sentinel_value="$3" deadline
  deadline=$(( $(epoch_now) + recovery_timeout ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if k rollout status deployment/hiqlite-recovery-proxy --timeout=5s >/dev/null 2>&1 \
      && ensure_port_forward_recoverable \
      && run_client_recoverable "$probe_timeout" verify-sentinel "$sentinel_id" "$sentinel_value" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  capture_convergence_diagnostics "${phase}-proxy-readonly-timeout"
  return 1
}

run_idempotent_restore_write() {
  local phase="$1" id value deadline attempt output status attempts tmp final_raw
  id="${phase}-restore-idempotent-${run_id}"
  value="restore-idempotent-${run_id}"
  idempotent_restore_write_path="$target/${phase}-idempotent-final-state.json"
  attempts='[]'
  deadline=$(( $(epoch_now) + recovery_timeout ))
  attempt=0
  while [ "$(epoch_now)" -le "$deadline" ]; do
    attempt=$((attempt + 1))
    output="$target/${phase}-idempotent-attempt-${attempt}.raw"
    if run_client_recoverable "$probe_timeout" execute "$id" "$value" > "$output" 2>&1; then
      status=0
      if jq -e --arg id "$id" --arg value "$value" '.acknowledged == true and .id == $id and .value == $value' "$output" >/dev/null; then
        attempts="$(jq -cn --argjson prior "$attempts" --argjson n "$attempt" --argjson rc "$status" --arg raw "$(<"$output")" '$prior + [{attempt:$n,rc:$rc,raw:$raw,classification:"acknowledged"}]')"
        break
      fi
      die "$phase idempotent restore write exited 0 with malformed or mismatched acknowledgement"
    else
      status=$?
    fi
    if grep -Eqi 'connection refused|timed out|transport|unavailable|port-forward' "$output"; then
      attempts="$(jq -cn --argjson prior "$attempts" --argjson n "$attempt" --argjson rc "$status" --arg raw "$(<"$output")" '$prior + [{attempt:$n,rc:$rc,raw:$raw,classification:"ambiguous-retryable"}]')"
      sleep 1
      continue
    fi
    die "$phase idempotent restore write had terminal failure"
  done
  if ! jq -e 'length > 0 and .[-1].classification == "acknowledged"' <<< "$attempts" >/dev/null; then
    tmp="$idempotent_restore_write_path.tmp.$$"
    jq -n --arg cell_id "$phase" --arg id "$id" --arg value "$value" --argjson attempts "$attempts" \
      '{cell_id:$cell_id,stage:"post-restore-clear",contract:"idempotent-final-state-single-key-not-exactly-once",id:$id,value:$value,attempts:$attempts,valid:false,mismatch_reason:"acknowledgement timeout"}' > "$tmp"
    mv "$tmp" "$idempotent_restore_write_path"
    die "$phase idempotent restore write did not acknowledge before timeout; last_rc=$(jq -r '.[-1].rc // "none"' <<< "$attempts")"
  fi
  final_raw=""
  deadline=$(( $(epoch_now) + recovery_timeout ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    output="$target/${phase}-idempotent-final-read.raw"
    if run_client_recoverable "$probe_timeout" query-consistent "$id" > "$output" 2>&1; then
      final_raw="$(<"$output")"
      jq -e --arg id "$id" --arg value "$value" '.found == true and .id == $id and .value == $value' "$output" >/dev/null \
        || die "$phase idempotent restore write has conflicting or malformed final read"
      break
    fi
    sleep 1
  done
  [ -n "$final_raw" ] || die "$phase idempotent restore write final consistent read timed out"
  tmp="$idempotent_restore_write_path.tmp.$$"
  jq -n --arg cell_id "$phase" --arg stage post-restore-clear --arg id "$id" --arg value "$value" --arg final_raw "$final_raw" --argjson attempts "$attempts" \
    '{cell_id:$cell_id,stage:$stage,contract:"idempotent-final-state-single-key-not-exactly-once",id:$id,value:$value,attempts:$attempts,
      final:{found:true,id:$id,value:$value,rc:0,raw:$final_raw,single_logical_key:true,single_key_basis:"PRIMARY KEY(id)"},
      valid:($cell_id != "" and $stage == "post-restore-clear" and ($id | startswith($cell_id + "-restore-idempotent-")) and ($attempts|length)>0 and $attempts[-1].classification=="acknowledged" and
        (($final_raw|fromjson) | .found == true and .id == $id and .value == $value))}' > "$tmp"
  mv "$tmp" "$idempotent_restore_write_path"
  jq -e '.valid == true' "$idempotent_restore_write_path" >/dev/null \
    || die "$phase idempotent restore write proof is invalid"
  idempotent_restore_write_sha256="$(openssl dgst -sha256 -r "$idempotent_restore_write_path" | awk '{print $1}')"
}

verify_cell_boundary() {
  local cell_id="$1" boundary="$2"
  local started_at started_epoch boundary_id boundary_value detail
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  [ "$(k get statefulset hiqlite-recovery -o jsonpath='{.spec.replicas}')" = 3 ] \
    || die "$cell_id $boundary boundary does not have three desired voters"
  wait_ready_replicas 3 "$recovery_timeout" \
    || die "$cell_id $boundary boundary does not have three ready voters"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-${boundary}-metrics.json" \
    || die "$cell_id $boundary boundary did not converge"
  if k get statefulset hiqlite-recovery -o json \
    | jq -e '[.spec.template.spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "$cell_id $boundary boundary retained HQL_BACKUP_RESTORE in the template"
  fi
  if k get pods -l app.kubernetes.io/component=voter -o json \
    | jq -e '[.items[].spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "$cell_id $boundary boundary retained HQL_BACKUP_RESTORE on a voter"
  fi
  [ "$(kobj get pod -l app.kubernetes.io/component=object-store \
    -o jsonpath='{.items[0].metadata.uid}')" = "$rustfs_uid" ] \
    || die "$cell_id $boundary boundary changed the RustFS Pod"
  [ -z "$(k get persistentvolumeclaims -o name)$(kobj get persistentvolumeclaims -o name)" ] \
    || die "$cell_id $boundary boundary no longer has zero PVCs"
  detail=three-voter-convergence
  if [ "$boundary" = start ]; then
    cell_baseline_reset_raw="$(run_client 30 reset)" \
      || die "$cell_id reset did not succeed"
    jq -e '.acknowledged == true' <<< "$cell_baseline_reset_raw" >/dev/null \
      || die "$cell_id reset exited 0 without acknowledged:true"
    verify_direct_reset_baseline "$cell_id"
    detail=three-voter-convergence-and-application-reset
  fi
  boundary_id="${cell_id}-${boundary}-boundary"
  boundary_value="healthy-${run_id}"
  run_client 30 execute "$boundary_id" "$boundary_value" >/dev/null
  remember_sentinel "$boundary_id"
  run_client 30 verify-sentinel "$boundary_id" "$boundary_value" >/dev/null
  record_event "$cell_id" "boundary-$boundary" healthy healthy true "$started_at" \
    "$(iso_now)" "$(( $(epoch_now) - started_epoch ))" "$detail"
}

append_phase_summary() {
  local phase="$1" cell_id="$2" failure_count="$3" hold_seconds="$4"
  local expected_json="$5" observed_json="$6"
  local failure_started_at="$7" failure_released_at="$8"
  local service_rto="$9" full_rto="${10}" failure_held="${11}" stage_plan
  stage_plan="$(jq -cn --arg phase "$phase" --argjson observed "$observed_json" '
    if $phase == "f1" and $observed.auto_recovery == true and $observed.operator_dr == false then
      ["pre-fault","post-recovery"]
    elif $phase == "f2" and $observed.auto_recovery == true and $observed.operator_dr == false then
      ["pre-fault","post-recovery"]
    elif ($phase == "f2" or $phase == "f3") and $observed.auto_recovery == false and $observed.operator_dr == true then
      ["pre-fault","post-operator-dr","post-restore-clear"]
    else error("invalid mutually exclusive recovery outcome for image proof stages") end
  ')" || die "$cell_id has invalid recovery outcome for image proof stages"
  cell_expected_image_proof_stages_json="$stage_plan"
  [ -n "$transition_ledger_path" ] && [ -f "$transition_ledger_path" ] \
    || die "$cell_id transition ledger is missing"
  [ "$(openssl dgst -sha256 -r "$transition_ledger_path" | awk '{print $1}')" = "$transition_ledger_sha256" ] \
    || die "$cell_id transition ledger SHA-256 changed without recording it"
  jq -e --argjson expected "$cell_expected_image_proof_stages_json" '
    (map(.stage) == $expected) and length == ($expected | length) and
    all(.[]; (.path|type) == "string" and (.path|length) > 0 and
      (.sha256|type) == "string" and (.sha256|test("^[0-9a-f]{64}$")))
  ' <<< "$cell_image_proofs_json" >/dev/null \
    || die "$cell_id image proof stages are incomplete"
  capture_cell_image_manifest "$cell_id"
  jq -cn \
    --arg phase "$phase" \
    --arg cell_id "$cell_id" \
    --argjson failure_count "$failure_count" \
    --argjson hold_seconds "$hold_seconds" \
    --arg hiqlite_commit "$hiqlite_commit" \
    --arg hiqlite_release "$hiqlite_release" \
    --arg image_release "$image_release" \
    --arg openraft_version "$openraft_version" \
    --arg log_sync "$log_sync" \
    --arg image_source "$image_source" \
    --arg source_commit_basis "$source_commit_basis" \
    --arg image_source_commit "$image_source_commit" \
    --arg lockfile_origin "$lockfile_origin" \
    --arg lockfile_sha256 "$lockfile_sha256" \
    --arg ingress_kind "$ingress_kind" \
    --arg ingress_version "$ingress_version" \
    --arg ingress_image "$ingress_image" \
    --arg proxy_patch_sha256 "$proxy_patch_sha256" \
    --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
    --arg resolved_image "$resolved_image" \
    --arg resolved_proxy_image "$resolved_proxy_image" \
    --arg resolved_proxy_image_id "$resolved_proxy_image_id" \
    --arg rustfs_uid "$rustfs_uid" --arg object_namespace_uid "$object_namespace_uid" \
    --arg object_inventory_initial_path "$object_inventory_initial_path" \
    --arg object_inventory_initial_digest "$object_inventory_initial_digest" \
    --arg live_image_ids_path "$live_image_ids_path" \
    --arg cell_backup_evidence_path "$cell_backup_evidence_path" \
    --arg cell_backup_post_digest "$cell_backup_post_digest" \
    --arg vcluster_node_uid "$vcluster_node_uid" \
    --arg failure_started_at "$failure_started_at" \
    --arg failure_released_at "$failure_released_at" \
    --arg cluster "$cluster" --arg context "$context" \
    --arg namespace "$namespace" \
    --arg cell_isolation_mode "$cell_isolation_mode" \
    --arg cell_isolation_identity_path "$cell_isolation_identity_path" \
    --arg cell_backup_key "$cell_backup_key" \
    --argjson cell_isolation_uid_proof "$cell_isolation_uid_proof" \
    --argjson cell_backup_key_unique "$cell_backup_key_unique" \
    --argjson fresh_vcluster_created "$created_cluster" \
    --argjson image_provenance_verified "$image_provenance_verified" \
    --argjson image_provenance_publishable "$image_provenance_publishable" \
    --argjson image_proofs "$cell_image_proofs_json" \
    --argjson expected_image_proof_stages "$cell_expected_image_proof_stages_json" \
    --arg cell_image_manifest_path "$cell_image_manifest_path" \
    --arg cell_image_manifest_sha256 "$cell_image_manifest_sha256" \
    --arg transition_ledger_path "$transition_ledger_path" \
    --arg transition_ledger_sha256 "$transition_ledger_sha256" \
    --argjson transition_ledger_count "$transition_ledger_count" \
    --arg cell_baseline_evidence_path "$cell_baseline_evidence_path" \
    --arg cell_baseline_evidence_sha256 "$cell_baseline_evidence_sha256" \
    --arg idempotent_restore_write_path "$idempotent_restore_write_path" \
    --arg idempotent_restore_write_sha256 "$idempotent_restore_write_sha256" \
    --arg failure_establishment_proof_path "$failure_establishment_proof_path" \
    --arg failure_establishment_proof_sha256 "$failure_establishment_proof_sha256" \
    --arg failure_establishment_resolution_path "$failure_establishment_resolution_path" \
    --arg failure_establishment_resolution_sha256 "$failure_establishment_resolution_sha256" \
    --argjson cell_baseline_direct_reads "$cell_baseline_direct_reads" \
    --argjson service_rto_seconds "$service_rto" \
    --argjson full_rto_seconds "$full_rto" \
    --argjson failure_held_seconds "$failure_held" \
    --argjson expected_vs_observed_expected "$expected_json" \
    --argjson expected_vs_observed_observed "$observed_json" \
    '{schema_version:1,system:"hiqlite",event:"phase_summary",phase:$phase,
      cell_id:$cell_id,failure_count:$failure_count,hold_seconds:$hold_seconds,
      hiqlite_reference_commit:$hiqlite_commit,
      hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      hiqlite_reference_release:$hiqlite_release,
      hiqlite_release:(if $image_release == "" then null else $image_release end),
      openraft_version:$openraft_version,openraft_version_source:$openraft_version_source,log_sync:$log_sync,
      image_source:$image_source,source_commit_basis:$source_commit_basis,
      image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      cargo_lock_origin:$lockfile_origin,
      cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
      ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
        patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
      upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
      resolved_image:$resolved_image,resolved_proxy_image:$resolved_proxy_image,
      resolved_proxy_image_id:$resolved_proxy_image_id,
      voters:3,storage:"emptyDir",zero_pvc:true,
      cell_isolation:{success:true,mode:$cell_isolation_mode,
        fresh_vcluster_created:$fresh_vcluster_created,
        namespace:$namespace,release_scope:$namespace,
        identity_path:$cell_isolation_identity_path,
        namespace_uid_proven:true,statefulset_uid_proven:true,
        voter_uids_proven:$cell_isolation_uid_proof,proxy_pod_uid_proven:true,
        controller_revision_proven:true,endpoint_target_uids_current:true,
        restore_env_absent:true,zero_pvc:true,no_host_path:true,
        rustfs_uid_stable:true,baseline_direct_reads:$cell_baseline_direct_reads,
        baseline_evidence_path:(if $cell_baseline_evidence_path == "" then null else $cell_baseline_evidence_path end),
        baseline_proof:{path:(if $cell_baseline_evidence_path == "" then null else $cell_baseline_evidence_path end),sha256:(if $cell_baseline_evidence_sha256 == "" then null else $cell_baseline_evidence_sha256 end)},
        idempotent_recovery_write:{path:(if $idempotent_restore_write_path == "" then null else $idempotent_restore_write_path end),sha256:(if $idempotent_restore_write_sha256 == "" then null else $idempotent_restore_write_sha256 end),contract:"idempotent-final-state-single-key-not-exactly-once"},
        failure_establishment_proof:{path:(if $failure_establishment_proof_path == "" then null else $failure_establishment_proof_path end),sha256:(if $failure_establishment_proof_sha256 == "" then null else $failure_establishment_proof_sha256 end)},
        failure_establishment_post_ack:{path:(if $failure_establishment_post_ack_path == "" then null else $failure_establishment_post_ack_path end),sha256:(if $failure_establishment_post_ack_sha256 == "" then null else $failure_establishment_post_ack_sha256 end),classification:(if $failure_establishment_post_ack_classification == "" then null else $failure_establishment_post_ack_classification end)},
        failure_establishment_resolution:{path:(if $failure_establishment_resolution_path == "" then null else $failure_establishment_resolution_path end),sha256:(if $failure_establishment_resolution_sha256 == "" then null else $failure_establishment_resolution_sha256 end)},
        backup_key:(if $cell_backup_key == "" then null else $cell_backup_key end),
        backup_key_unique:$cell_backup_key_unique,
        vcluster:{name:$cluster,context:$context,node_uid:(if $vcluster_node_uid == "" then null else $vcluster_node_uid end)},
        rustfs_uid:$rustfs_uid,object_namespace_uid:$object_namespace_uid,
        initial_inventory_path:$object_inventory_initial_path,
        initial_inventory_sha256:$object_inventory_initial_digest,
        image_provenance_verified:$image_provenance_verified,
        image_provenance_publishable:$image_provenance_publishable,live_image_ids_path:$live_image_ids_path,
        image_proofs:$image_proofs,expected_image_proof_stages:$expected_image_proof_stages,
        image_provenance_manifest:{path:$cell_image_manifest_path,sha256:$cell_image_manifest_sha256},
        transition_ledger:{path:(if $transition_ledger_path == "" then null else $transition_ledger_path end),
          sha256:(if $transition_ledger_sha256 == "" then null else $transition_ledger_sha256 end),count:$transition_ledger_count},
        backup_evidence_path:(if $cell_backup_evidence_path == "" then null else $cell_backup_evidence_path end),
        backup_inventory_sha256:(if $cell_backup_post_digest == "" then null else $cell_backup_post_digest end),
        vcluster_node_uid:(if $vcluster_node_uid == "" then null else $vcluster_node_uid end)},
      failure_started_at:$failure_started_at,failure_released_at:$failure_released_at,
      failure_held_seconds:$failure_held_seconds,
      service_rto_seconds:$service_rto_seconds,full_rto_seconds:$full_rto_seconds,
      expected_vs_observed:{expected:$expected_vs_observed_expected,
        observed:$expected_vs_observed_observed}}' >> "$jsonl"
}

cd "$repo_root"
mkdir -p "$target"
chmod 700 "$target"
: > "$jsonl"
build_artifacts
if [ "$direct_cluster" = 0 ]; then
  capture_local_image_config_ids pre-load
fi

previous_context="$(kubectl config current-context 2>/dev/null || true)"
if [ "$direct_cluster" = 1 ]; then
  context="${HIQLITE_RECOVERY_DIRECT_CONTEXT:-$previous_context}"
  [ -n "$context" ] || die "direct cluster mode requires an active or explicit context"
else
  vcluster use driver docker >/dev/null
  if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
    [ "$require_fresh_vcluster" = 0 ] \
      || die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 refuses an existing vcluster: $cluster"
    [ "${HIQLITE_RECOVERY_REUSE_EXISTING:-0}" = 1 ] \
      || die "vind cluster already exists: $cluster"
    vcluster connect "$cluster" --driver docker >/dev/null
  else
    vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
    created_cluster=true
  fi
fi
[ "$require_fresh_vcluster" = 0 ] || "$created_cluster" \
  || die "HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 did not create a vcluster"
capture_ready_context
kubectl config use-context "$context" >/dev/null
create_managed_namespace "$namespace"
[ "$direct_cluster" = 0 ] || direct_namespaces_created=true
create_managed_namespace "$object_namespace"
node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node"
vcluster_node_uid="$(kubectl --context "$context" get node "$node" -o jsonpath='{.metadata.uid}')"
[ -n "$vcluster_node_uid" ] || die "cannot capture vcluster node UID"
if [ "$direct_cluster" = 0 ] && [ "$skip_image_load" = 0 ]; then
  (cd "$target" && vcluster node load-image "$node" --image "$image")
  (cd "$target" && vcluster node load-image "$node" --image "$ingress_image")
fi
if [ "$direct_cluster" = 0 ]; then
  capture_local_image_config_ids post-load
  capture_node_cri_image_ids
fi
render_and_deploy
wait_ready_replicas 3 "$recovery_timeout" || die "initial voters did not become ready"
k rollout status deployment/hiqlite-recovery-proxy --timeout="${recovery_timeout}s" >/dev/null
verify_live_image_ids
object_inventory_initial_path="$target/initial-object-inventory.json"
object_inventory_initial_digest="$(capture_object_inventory initial true)"
start_port_forward
run_client 30 execute baseline initial > "$target/baseline-execute.json"
run_client 30 verify-sentinel baseline initial > "$target/baseline-verify.json"

if [ "$steady_mode" = 1 ]; then
  # Match the D1 container contract before warmup; this is intentionally a
  # concrete mode of this deployment owner, not a caller-provided shell hook.
  live_pods="$target/live-pods.json"
  build_provenance="$target/build-provenance.json"
  k get pods -l app.kubernetes.io/name=hiqlite-recovery -o json > "$live_pods"
  jq -e '
    .items | length == 3 and all(.[]; any(.spec.containers[];
      .name == "hiqlite" and .resources == {requests:{cpu:"250m",memory:"512Mi"},limits:{cpu:"1000m",memory:"1Gi"}}))
  ' "$live_pods" >/dev/null || die "live Hiqlite pod resources do not match D1 contract"
  jq -n --arg release "$hiqlite_release" --arg commit "$hiqlite_commit" --arg openraft "$openraft_version" --arg source_build "$image_source" --arg lock "$lockfile_sha256" --arg patch "$proxy_patch_sha256" --arg image "$resolved_image" '{release:$release,commit:$commit,openraft:$openraft,log_sync:"Immediate",source_build:$source_build,cargo_lock_sha256:$lock,proxy_patch_sha256:$patch,image_digest:$image}' > "$build_provenance"
  start_port_forward
  steady_value="$(LC_ALL=C tr -dc 'a' < /dev/zero | head -c 128 || true)"
  [ "${#steady_value}" = 128 ] || die "cannot construct deterministic 128-byte steady value"
  run_client 90 bench-write --warmup-seconds 10 --measure-seconds 60 \
    --concurrency "$steady_concurrency" --id d1-steady --value "$steady_value" > "$target/bench.json"
  bench_sha256="$(openssl dgst -sha256 -r "$target/bench.json" | awk '{print $1}')"
  manifest_sha256="$(openssl dgst -sha256 -r "$target/hiqlite.yaml" | awk '{print $1}')"
  live_sha256="$(openssl dgst -sha256 -r "$live_pods" | awk '{print $1}')"
  provenance_sha256="$(openssl dgst -sha256 -r "$build_provenance" | awk '{print $1}')"
  jq -n --slurpfile bench "$target/bench.json" \
    --arg run_id "$run_id" --arg target "$target" --arg image "$resolved_image" \
    --arg bench_sha256 "$bench_sha256" --arg manifest_sha "$manifest_sha256" --arg live_sha "$live_sha256" --arg provenance_sha "$provenance_sha256" --arg hiqlite_commit "$hiqlite_commit" --arg hiqlite_release "$hiqlite_release" --arg openraft_version "$openraft_version" --arg openraft_version_source "$openraft_version_source" --arg lockfile_sha256 "$lockfile_sha256" --arg patch_sha256 "$proxy_patch_sha256" \
    '{schema_version:1,system:"hiqlite",run_id:$run_id,contract:"D1",workload:"d1_sql_unique_request_id_deterministic_write",client_path:"public_host_side",voters:3,storage:"emptyDir",zero_pvc:true,durability:"Immediate",resources:{cpu_request:"250m",cpu_limit:"1000m",memory_request:"512Mi",memory_limit:"1Gi"},image_digest:$image,hiqlite_provenance:{release:$hiqlite_release,commit:$hiqlite_commit,openraft:$openraft_version,openraft_version_source:$openraft_version_source,log_sync:"Immediate",source_build:$image_source,cargo_lock_sha256:$lockfile_sha256,proxy_patch_sha256:$patch_sha256},raw_artifact_paths:{root:$target,bench:"\($target)/bench.json",cluster_manifest:"\($target)/hiqlite.yaml",live_pods:"\($target)/live-pods.json",build_provenance:"\($target)/build-provenance.json"},raw_digests:{bench_sha256:$bench_sha256,cluster_manifest_sha256:$manifest_sha,live_pods_sha256:$live_sha,build_provenance_sha256:$provenance_sha},bench:$bench[0],performance:{logical_ops_per_second:$bench[0].measurement.successes_per_second,latency_ms:{p50:($bench[0].measurement.latency_ns.p50 / 1000000),p95:($bench[0].measurement.latency_ns.p95 / 1000000),p99:($bench[0].measurement.latency_ns.p99 / 1000000),p999:($bench[0].measurement.latency_ns.p999 / 1000000),max:($bench[0].measurement.latency_ns.max / 1000000)}},telemetry:{cpu_rss:"not_measured",disk_bytes_per_op:"not_measured",network_bytes_per_op:"not_measured",fsync_count:"not_measured"},performance_publishable:true,resource_publishable:false,publication_blockers:["CPU/RSS/disk/network/fsync telemetry not_measured; resource scorecard is non-publishable"]}' > "$target/steady-summary.json"
  cat "$target/steady-summary.json"
  exit 0
fi

pvc_count=$(( $(k get persistentvolumeclaims -o name | wc -l) \
  + $(kobj get persistentvolumeclaims -o name | wc -l) ))
[ "$pvc_count" -eq 0 ] || die "Hiqlite recovery drill created $pvc_count PVCs"
capture_uids "$target/uids-initial.json"
jq -cn \
  --arg run_id "$run_id" --arg hiqlite_commit "$hiqlite_commit" \
  --arg hiqlite_release "$hiqlite_release" \
  --arg image_release "$image_release" \
  --arg openraft_version "$openraft_version" --arg openraft_version_source "$openraft_version_source" --arg source_dir "$source_dir" \
  --arg log_sync "$log_sync" --arg image_source "$image_source" \
  --arg source_commit_basis "$source_commit_basis" \
  --arg image_source_commit "$image_source_commit" \
  --arg lockfile_origin "$lockfile_origin" \
  --arg lockfile_sha256 "$lockfile_sha256" \
  --arg ingress_kind "$ingress_kind" \
  --arg ingress_version "$ingress_version" \
  --arg ingress_image "$ingress_image" \
  --arg proxy_patch_sha256 "$proxy_patch_sha256" \
  --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
  --arg requested_image "$requested_image" --arg resolved_image "$resolved_image" \
  --argjson exact_source_build "$build_image" \
  --argjson pvc_count "$pvc_count" --arg rustfs_uid "$rustfs_uid" \
  '{schema_version:1,system:"hiqlite",event:"run_started",run_id:$run_id,
    hiqlite_reference_commit:$hiqlite_commit,
    hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    hiqlite_reference_release:$hiqlite_release,
    hiqlite_release:(if $image_release == "" then null else $image_release end),
    openraft_version:$openraft_version,openraft_version_source:$openraft_version_source,
    source_dir:(if $exact_source_build == 1 then $source_dir else null end),
    source_commit_basis:$source_commit_basis,
    image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    cargo_lock_origin:$lockfile_origin,
    cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
    ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
      patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
    upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
    log_sync:$log_sync,
    image_source:$image_source,requested_image:$requested_image,
    resolved_image:$resolved_image,voters:3,storage:"emptyDir",
    zero_pvc:true,pvc_count:$pvc_count,rustfs_uid:$rustfs_uid}' >> "$jsonl"

run_f1_cell() {
  local hold_seconds="$1" cell_id="f1-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local ack_file="$target/${cell_id}-acks.tsv"
  local uids_file="$target/uids-before-${cell_id}.json"
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  echo "== $cell_id: one peer lost for ${hold_seconds}s =="
  prepare_matrix_cell "$cell_id"
  verify_cell_boundary "$cell_id" start
  : > "$ack_file"
  capture_uids "$uids_file"
  scale_failure 2
  k wait --for=delete pod/hiqlite-recovery-2 --timeout=180s >/dev/null
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" success success success "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write true \
    || die "$cell_id write probe failed while quorum remained"
  assert_probe_outcome "$cell_id" local-query true \
    || die "$cell_id local query probe failed while quorum remained"
  assert_probe_outcome "$cell_id" query-consistent true \
    || die "$cell_id consistent query probe failed while quorum remained"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  wait_service "$recovery_timeout" "$service_id" "$service_value" \
    || die "$cell_id service did not recover"
  service_epoch="$(epoch_now)"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-converged-metrics.json" \
    || die "$cell_id did not converge to three voters"
  capture_learner_to_voter_evidence "$cell_id" 3 "$released_at" \
    || die "$cell_id logs did not prove learner-to-voter promotion for node 3"
  capture_cell_image_proof "$cell_id" post-recovery
  markers_lost "$uids_file" 2 || die "$cell_id emptyDir marker was retained"
  verify_ack_file "$ack_file"
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  append_phase_summary f1 "$cell_id" 1 "$hold_seconds" \
    '{"write":"success","local_query":"success","query_consistent":"success","auto_recovery":true,"rpo":"0"}' \
    '{"auto_recovery":true,"operator_dr":false,"learner_to_voter":true,"markers_lost":true,"ack_sentinel_preserved":true,"voter_ids":[1,2,3]}' \
    "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

run_f2_cell() {
  local hold_seconds="$1" cell_id="f2-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local backup_id="${cell_id}-backup" after_id="${cell_id}-after-backup"
  local backup_started_at backup_started_epoch backup_key ack_file uids_file
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  local dr_released_at dr_release_epoch observed
  local auto_recovered=false operator_dr=false rpo_to_backup=false
  echo "== $cell_id: two peers lost for ${hold_seconds}s =="
  prepare_matrix_cell "$cell_id"
  verify_cell_boundary "$cell_id" start
  run_client 30 execute "$backup_id" "before-${cell_id}-backup" >/dev/null
  remember_sentinel "$backup_id"
  backup_started_at="$(iso_now)"
  backup_started_epoch="$(epoch_now)"
  trigger_external_backup "$cell_id" \
    || die "$cell_id prerequisite external backup did not complete"
  backup_key="$triggered_backup_key"
  cell_backup_key="$backup_key"
  record_event "$cell_id" external-backup completed-object completed-object true \
    "$backup_started_at" "$(iso_now)" "$(( $(epoch_now) - backup_started_epoch ))" \
    "$backup_key"
  run_client 30 execute "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
  remember_sentinel "$after_id"
  uids_file="$target/uids-before-${cell_id}.json"
  ack_file="$target/${cell_id}-acks.tsv"
  capture_uids "$uids_file"
  : > "$ack_file"
  scale_failure 1
  k wait --for=delete pod/hiqlite-recovery-1 pod/hiqlite-recovery-2 --timeout=180s >/dev/null
  start_direct_survivor_port_forward
  wait_failure_established "$cell_id" false "$service_id" \
    || die "$cell_id did not establish a stable no-quorum failure"
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" fail-closed stale-local-allowed fail-closed \
    "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write false \
    || die "$cell_id accepted a write without old-cluster quorum"
  assert_probe_outcome "$cell_id" query-consistent false \
    || die "$cell_id served a consistent query without old-cluster quorum"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  if wait_convergence "$auto_recovery_timeout" "$target/${cell_id}-auto-metrics.json"; then
    auto_recovered=true
    wait_service "$recovery_timeout" "$service_id" "$service_value" \
      || die "$cell_id membership converged without service"
    service_epoch="$(epoch_now)"
    markers_lost "$uids_file" 1 2 \
      || die "$cell_id unexpected recovery did not replace both emptyDir voters"
    verify_ack_file "$ack_file"
    run_client 30 verify-sentinel "$backup_id" "before-${cell_id}-backup" >/dev/null
    run_client 30 verify-sentinel "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
    verify_transition_ledger present
    resolve_f2_unknown_write "$cell_id" auto-recovery
    capture_cell_image_proof "$cell_id" post-recovery
  else
    echo "$cell_id remained fail-closed; invoking operator backup DR"
    operator_dr=true
    k scale statefulset/hiqlite-recovery --replicas=0 >/dev/null
    k wait --for=delete pod -l app.kubernetes.io/component=voter --timeout=180s >/dev/null
    set_restore_object "$backup_key"
    dr_released_at="$(iso_now)"
    dr_release_epoch="$(epoch_now)"
    k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
    wait_service "$recovery_timeout" "$service_id" "$service_value" \
      || die "$cell_id operator DR did not restore service"
    service_epoch="$(epoch_now)"
    wait_convergence "$recovery_timeout" "$target/${cell_id}-dr-metrics.json" \
      || die "$cell_id operator DR did not converge"
    capture_cell_image_proof "$cell_id" post-operator-dr
    clear_restore_from_running_pods "$cell_id" "$service_id" "$service_value"
    capture_cell_image_proof "$cell_id" post-restore-clear
    run_client 30 verify-sentinel "$backup_id" "before-${cell_id}-backup" >/dev/null
    verify_missing "$after_id"
    verify_transition_ledger absent
    resolve_f2_unknown_write "$cell_id" operator-dr
    rpo_to_backup=true
    markers_lost "$uids_file" 0 1 2 \
      || die "$cell_id operator DR retained an old emptyDir marker"
    record_event "$cell_id" operator-dr manual-trigger restored true "$dr_released_at" \
      "$(iso_now)" "$(( $(epoch_now) - dr_release_epoch ))" "$backup_key"
  fi
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  observed="$(jq -cn \
    --argjson auto_recovery "$auto_recovered" \
    --argjson operator_dr "$operator_dr" \
    --argjson rpo_to_backup "$rpo_to_backup" \
    '{auto_recovery:$auto_recovery,operator_dr:$operator_dr,
      rpo_to_backup:$rpo_to_backup,markers_lost:true,voter_ids:[1,2,3]}')"
  jq -e '(.auto_recovery == true and .operator_dr == false) or
    (.auto_recovery == false and .operator_dr == true)' <<< "$observed" >/dev/null \
    || die "$cell_id recovery outcome must be exactly auto-recovery or operator DR"
  append_phase_summary f2 "$cell_id" 2 "$hold_seconds" \
    '{"write":"fail-closed","query_consistent":"fail-closed","auto_recovery":false,"next":"operator_dr"}' \
    "$observed" "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

run_f3_cell() {
  local hold_seconds="$1" cell_id="f3-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local backup_id="${cell_id}-backup" after_id="${cell_id}-after-backup"
  local backup_started_at backup_started_epoch backup_key ack_file uids_file
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  echo "== $cell_id: three peers lost for ${hold_seconds}s =="
  prepare_matrix_cell "$cell_id"
  verify_cell_boundary "$cell_id" start
  run_client 30 execute "$backup_id" "present-in-${cell_id}-backup" >/dev/null
  remember_sentinel "$backup_id"
  backup_started_at="$(iso_now)"
  backup_started_epoch="$(epoch_now)"
  trigger_external_backup "$cell_id" \
    || die "$cell_id prerequisite external backup did not complete"
  backup_key="$triggered_backup_key"
  cell_backup_key="$backup_key"
  record_event "$cell_id" external-backup completed-object completed-object true \
    "$backup_started_at" "$(iso_now)" "$(( $(epoch_now) - backup_started_epoch ))" \
    "$backup_key"
  run_client 30 execute "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
  remember_sentinel "$after_id"
  uids_file="$target/uids-before-${cell_id}.json"
  ack_file="$target/${cell_id}-acks.tsv"
  capture_uids "$uids_file"
  : > "$ack_file"
  scale_failure 0
  k wait --for=delete pod -l app.kubernetes.io/component=voter --timeout=180s >/dev/null
  wait_failure_established "$cell_id" true "$service_id" \
    || die "$cell_id did not establish a stable zero-voter failure"
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" fail-closed unavailable fail-closed \
    "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write false \
    || die "$cell_id accepted a write with no voters"
  assert_probe_outcome "$cell_id" local-query false \
    || die "$cell_id served a local query with no voters"
  assert_probe_outcome "$cell_id" query-consistent false \
    || die "$cell_id served a consistent query with no voters"
  set_restore_object "$backup_key"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  wait_service "$recovery_timeout" "$service_id" "$service_value" \
    || die "$cell_id operator DR did not restore service"
  service_epoch="$(epoch_now)"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-converged-metrics.json" \
    || die "$cell_id operator DR did not converge"
  capture_cell_image_proof "$cell_id" post-operator-dr
  clear_restore_from_running_pods "$cell_id" "$service_id" "$service_value"
  capture_cell_image_proof "$cell_id" post-restore-clear
  run_client 30 verify-sentinel "$backup_id" "present-in-${cell_id}-backup" >/dev/null
  verify_missing "$after_id"
  verify_transition_ledger absent
  markers_lost "$uids_file" 0 1 2 \
    || die "$cell_id operator DR retained an old emptyDir marker"
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  record_event "$cell_id" operator-dr manual-trigger restored true "$released_at" \
    "$(iso_now)" "$((full_epoch - release_epoch))" "$backup_key"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  append_phase_summary f3 "$cell_id" 3 "$hold_seconds" \
    '{"write":"fail-closed","query_consistent":"fail-closed","operator_dr":true,"rpo":"to_backup"}' \
    '{"auto_recovery":false,"operator_dr":true,"markers_lost":true,"ack_sentinel_preserved":true,"rpo_to_backup":true,"voter_ids":[1,2,3]}' \
    "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

for failure_count in "${failure_values[@]}"; do
  for hold_seconds in "${hold_values[@]}"; do
    case "$failure_count" in
      1) run_f1_cell "$hold_seconds" ;;
      2) run_f2_cell "$hold_seconds" ;;
      3) run_f3_cell "$hold_seconds" ;;
    esac
  done
done

current_rustfs_uid="$(kobj get pod -l app.kubernetes.io/component=object-store \
  -o jsonpath='{.items[0].metadata.uid}')"
[ "$current_rustfs_uid" = "$rustfs_uid" ] \
  || die "RustFS Pod changed during voter failure lifecycle"
[ "$namespace" != "$object_namespace" ] || die "RustFS must be outside the voter namespace"
[ -z "$(k get persistentvolumeclaims -o name)$(kobj get persistentvolumeclaims -o name)" ] \
  || die "recovery drill no longer has zero PVCs"

hold_values_json="$(printf '%s\n' "${hold_values[@]}" | jq -Rsc 'split("\n")[:-1] | map(tonumber)')"
failure_values_json="$(printf '%s\n' "${failure_values[@]}" | jq -Rsc 'split("\n")[:-1] | map(tonumber)')"
expected_cells="$(jq -cn --argjson failures "$failure_values_json" --argjson holds "$hold_values_json" \
  '[$failures[] as $failure | $holds[] as $hold | "f\($failure)-h\($hold)"]')"
expected_cell_count=$(( ${#failure_values[@]} * ${#hold_values[@]} ))
jq -s \
  --arg run_id "$run_id" \
  --arg hiqlite_commit "$hiqlite_commit" \
  --arg hiqlite_release "$hiqlite_release" \
  --arg image_release "$image_release" \
  --arg openraft_version "$openraft_version" \
  --arg openraft_version_source "$openraft_version_source" \
  --arg log_sync "$log_sync" \
  --arg image_source "$image_source" \
  --arg source_commit_basis "$source_commit_basis" \
  --arg image_source_commit "$image_source_commit" \
  --arg lockfile_origin "$lockfile_origin" \
  --arg lockfile_sha256 "$lockfile_sha256" \
  --arg ingress_kind "$ingress_kind" \
  --arg ingress_version "$ingress_version" \
  --arg ingress_image "$ingress_image" \
  --arg proxy_patch_sha256 "$proxy_patch_sha256" \
  --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
  --arg resolved_image "$resolved_image" \
  --arg resolved_proxy_image "$resolved_proxy_image" \
  --arg resolved_proxy_image_id "$resolved_proxy_image_id" \
  --arg rustfs_uid "$rustfs_uid" \
  --arg object_namespace_uid "$object_namespace_uid" \
  --arg object_inventory_initial_path "$object_inventory_initial_path" \
  --arg object_inventory_initial_digest "$object_inventory_initial_digest" \
  --argjson failure_counts "$failure_values_json" \
  --argjson hold_seconds "$hold_values_json" \
  '{schema_version:1,system:"hiqlite",run_id:$run_id,
    hiqlite_reference_commit:$hiqlite_commit,
    hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    hiqlite_reference_release:$hiqlite_release,
    hiqlite_release:(if $image_release == "" then null else $image_release end),
    openraft_version:$openraft_version,openraft_version_source:$openraft_version_source,log_sync:$log_sync,
    image_source:$image_source,source_commit_basis:$source_commit_basis,
    image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    cargo_lock_origin:$lockfile_origin,
    cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
    ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
      patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
    upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
    resolved_image:$resolved_image,resolved_proxy_image:$resolved_proxy_image,
    resolved_proxy_image_id:$resolved_proxy_image_id,
    voters:3,storage:"emptyDir",zero_pvc:true,
    rustfs_uid:$rustfs_uid,object_namespace_uid:$object_namespace_uid,
    object_inventory_initial:{path:$object_inventory_initial_path,sha256:$object_inventory_initial_digest},
    failure_counts:$failure_counts,hold_seconds:$hold_seconds,
    phases:[.[] | select(.event == "phase_summary")],
    cell_isolation:{all_cells_proven:(
      [.[] | select(.event == "phase_summary") |
        .cell_isolation.success == true and
        .cell_isolation.namespace_uid_proven == true and
        .cell_isolation.statefulset_uid_proven == true and
        .cell_isolation.voter_uids_proven == true and
        .cell_isolation.proxy_pod_uid_proven == true and
        .cell_isolation.controller_revision_proven == true and
        .cell_isolation.endpoint_target_uids_current == true and
        .cell_isolation.restore_env_absent == true and
        .cell_isolation.zero_pvc == true and
        .cell_isolation.no_host_path == true and
        .cell_isolation.rustfs_uid_stable == true and
        .cell_isolation.baseline_direct_reads == true and
        .cell_isolation.backup_key_unique == true] | all),
      namespaces:[.[] | select(.event == "phase_summary") | .cell_isolation.namespace]},
    events:length}' \
  "$jsonl" > "$summary"
jq -e --argjson expected_cells "$expected_cells" --argjson expected_cell_count "$expected_cell_count" '
  .phases as $phases |
  (.resolved_proxy_image | type) == "string" and (.resolved_proxy_image | length) > 0 and
  (.resolved_proxy_image_id | type) == "string" and (.resolved_proxy_image_id | length) > 0 and
  (.rustfs_uid | type) == "string" and (.rustfs_uid | length) > 0 and
  (.object_namespace_uid | type) == "string" and (.object_namespace_uid | length) > 0 and
  (.object_inventory_initial.sha256 | type) == "string" and (.object_inventory_initial.sha256 | length) == 64 and
  ($phases | length) == $expected_cell_count and
  ($phases | map(.cell_id)) == $expected_cells and
  ($phases | map(.cell_id) | unique | length) == $expected_cell_count and
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
    (.cell_isolation.vcluster.name | type) == "string" and
    (.cell_isolation.vcluster.context | type) == "string" and
    (.cell_isolation.vcluster.node_uid | type) == "string" and
    (.cell_isolation.rustfs_uid | type) == "string" and
    (.cell_isolation.object_namespace_uid | type) == "string" and
    (.cell_isolation.initial_inventory_sha256 | type) == "string" and
    (.phase == "f\(.failure_count)") and
    (.cell_id == "f\(.failure_count)-h\(.hold_seconds)")
  )) and .cell_isolation.all_cells_proven == true and
  (.cell_isolation.namespaces | unique | length) == $expected_cell_count
' "$summary" >/dev/null
echo "Hiqlite zero-PVC recovery drill passed: $summary"
