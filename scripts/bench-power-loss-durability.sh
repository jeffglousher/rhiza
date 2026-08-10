#!/usr/bin/env bash
# Fail-closed envelope for an external, disposable physical-power-loss lab.
set -euo pipefail

die() { printf 'power-loss durability gate: %s\n' "$*" >&2; exit 1; }
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}
usage() {
  cat <<'EOF'
usage: scripts/bench-power-loss-durability.sh plan OUTPUT.json
       scripts/bench-power-loss-durability.sh validate-live
       scripts/bench-power-loss-durability.sh provider-run-envelope OUTPUT_DIR
       scripts/bench-power-loss-durability.sh validate-artifacts OUTPUT_DIR

provider-run-envelope is destructive only with:
  POWER_LOSS_DURABILITY_OPT_IN=I_UNDERSTAND_DESTRUCTIVE_POWER_LOSS
EOF
}

configure_live_commands() {
  if [ "${POWER_LOSS_STATIC_TEST_MODE:-}" = I_ACKNOWLEDGE_NO_REAL_DEVICE ]; then
    UNAME_CMD="${POWER_LOSS_UNAME_CMD:?missing test uname shim}"
    LSBLK_CMD="${POWER_LOSS_LSBLK_CMD:?missing test lsblk shim}"
    FINDMNT_CMD="${POWER_LOSS_FINDMNT_CMD:?missing test findmnt shim}"
    READLINK_CMD="${POWER_LOSS_READLINK_CMD:?missing test readlink shim}"
    SWAPS_FILE="${POWER_LOSS_SWAPS_FILE:?missing test swaps fixture}"
    SYS_DEV_BLOCK_ROOT="${POWER_LOSS_SYS_DEV_BLOCK_ROOT:?missing test sysfs fixture}"
  else
    UNAME_CMD='uname'; LSBLK_CMD='lsblk'; FINDMNT_CMD='findmnt'; READLINK_CMD='readlink'
    SWAPS_FILE=/proc/swaps; SYS_DEV_BLOCK_ROOT=/sys/dev/block
  fi
}
is_block_device() {
  if [ "${POWER_LOSS_STATIC_TEST_MODE:-}" = I_ACKNOWLEDGE_NO_REAL_DEVICE ]; then [ -n "$("$LSBLK_CMD" -dnro TYPE "$1")" ];
  else [ -b "$1" ]; fi
}

validate_declarations() {
  [ -r "${POWER_LOSS_PROVIDER_DECLARATION:-}" ] || die 'missing provider declaration JSON'
  [ -r "${POWER_LOSS_DURABILITY_DECLARATION:-}" ] || die 'missing durability declaration JSON'
  jq -e '
    .schema_version == 1 and .kind == "provider-declaration" and .disposable == true and
    (.provider == "block-device" or .provider == "vm") and (.target|test("^/dev/")) and
    (.filesystem == "ext4" or .filesystem == "xfs") and
    ([.controller_id,.rhiza_commit,.hiqlite_commit,.rhiza_image,.hiqlite_image] | all(type == "string" and length > 0)) and
    (.csi == null or (.csi.declared == true and (.csi.driver|type)=="string" and (.csi.driver|length)>0))
  ' "$POWER_LOSS_PROVIDER_DECLARATION" >/dev/null || die 'invalid provider declaration'
  jq -e '
    .schema_version == 1 and .kind == "durability-declaration" and
    .rhiza_ack == "object-authoritative-sync" and .hiqlite_ack == "comparable-durable-ack" and
    .fsync_policy_match == true and .log_policy_match == true and .failure_rpo_semantics_match == true and
    (.matched_rpo_ms|type)=="number" and .matched_rpo_ms >= 0
  ' "$POWER_LOSS_DURABILITY_DECLARATION" >/dev/null || die 'declarations do not define an equal-durability league'
}

require_hook() {
  local name="$1" path="${!1:-}"
  [ -n "$path" ] && [ -x "$path" ] && [ ! -L "$path" ] || die "missing, non-executable, or symlinked hook: $name"
  case "$path" in /*) ;; *) die "hook must be an absolute path: $name" ;; esac
  local canonical
  canonical="$("$READLINK_CMD" -f "$path")"
  [ "$canonical" = "$path" ] || die "hook path or a component is symlinked: $name"
  printf -v "${name}_CANONICAL" '%s' "$canonical"
  printf -v "${name}_SHA256" '%s' "$(sha256_file "$canonical")"
}

require_hook_unchanged() {
  local name="$1" path="${!1}" canonical_name="${1}_CANONICAL" sha_name="${1}_SHA256"
  [ "$("$READLINK_CMD" -f "$path")" = "${!canonical_name}" ] || die "hook path changed after live validation: $name"
  [ "$(sha256_file "$path")" = "${!sha_name}" ] || die "hook executable changed after live validation: $name"
}

snapshot_hook() {
  local name="$1" stage="$2" run_dir="$3" canonical_name="${1}_CANONICAL" sha_name="${1}_SHA256"
  local snapshot="$run_dir/hooks/$stage" tmp="$run_dir/hooks/.$stage.tmp"
  require_hook_unchanged "$name"
  cp "${!canonical_name}" "$tmp"; chmod 500 "$tmp"
  [ "$(sha256_file "$tmp")" = "${!sha_name}" ] || die "hook changed while snapshotting: $name"
  require_hook_unchanged "$name"
  mv "$tmp" "$snapshot"
  printf -v "${name}_SNAPSHOT" '%s' "$snapshot"
  printf -v "${name}_SNAPSHOT_REL" '%s' "hooks/$stage"
}

snapshot_hooks() {
  local run_dir="$1"
  mkdir -p "$run_dir/hooks"; chmod 700 "$run_dir/hooks"
  snapshot_hook POWER_LOSS_PROVIDER_VALIDATE_HOOK provider-validate "$run_dir"
  snapshot_hook POWER_LOSS_WORKLOAD_PREPARE_HOOK workload-prepare "$run_dir"
  snapshot_hook POWER_LOSS_CUT_AT_BARRIER_HOOK cut-at-barrier "$run_dir"
  snapshot_hook POWER_LOSS_PROVIDER_REBOOT_HOOK reboot "$run_dir"
  snapshot_hook POWER_LOSS_POST_REBOOT_VERIFY_HOOK verify "$run_dir"
}

validate_live() {
  configure_live_commands
  [ "$("$UNAME_CMD" -s)" = Linux ] || die 'live physical-power-loss execution is Linux-only'
  if ! command -v jq >/dev/null || [ ! -x "$LSBLK_CMD" ] || [ ! -x "$FINDMNT_CMD" ] || [ ! -x "$READLINK_CMD" ]; then
    die 'jq, lsblk, and findmnt are required'
  fi
  validate_declarations
  local target provider fs root_source system_mount name dev_node holders mountpoints swap_dev
  target="$(jq -r .target "$POWER_LOSS_PROVIDER_DECLARATION")"
  provider="$(jq -r .provider "$POWER_LOSS_PROVIDER_DECLARATION")"
  fs="$(jq -r .filesystem "$POWER_LOSS_PROVIDER_DECLARATION")"
  [ -n "${POWER_LOSS_TARGET:-}" ] && [ "$POWER_LOSS_TARGET" = "$target" ] || die 'explicit POWER_LOSS_TARGET must exactly match the declaration'
  is_block_device "$target" || die 'target is not a block device'
  [ "$("$READLINK_CMD" -f "$target")" = "$target" ] || die 'target must be a canonical /dev path; aliases are refused'
  [ "$("$LSBLK_CMD" -dnro FSTYPE "$target")" = "$fs" ] || die 'live filesystem does not match the ext4/XFS declaration'
  mountpoints="$("$LSBLK_CMD" -nrpo MOUNTPOINTS "$target" | sed '/^[[:space:]]*$/d')"
  [ -z "$mountpoints" ] || die 'target or a child device is mounted'
  for system_mount in / /boot /boot/efi /usr /var; do
    root_source="$("$FINDMNT_CMD" -nro SOURCE --target "$system_mount" 2>/dev/null || true)"
    [ -n "$root_source" ] || continue
    is_block_device "$root_source" || die "cannot prove target separation from system mount $system_mount"
    while IFS= read -r name; do
      [ "$name" != "$target" ] || die "target backs system mount $system_mount"
    done < <("$LSBLK_CMD" -s -nrpo NAME "$root_source")
  done
  while read -r swap_dev _; do
    [ "$swap_dev" = Filename ] && continue
    is_block_device "$swap_dev" || continue
    while IFS= read -r name; do
      [ "$name" != "$target" ] || die 'target backs active swap'
    done < <("$LSBLK_CMD" -s -nrpo NAME "$swap_dev")
  done < "$SWAPS_FILE"
  while IFS= read -r name; do
    dev_node="$("$LSBLK_CMD" -dnro MAJ:MIN "$name")"
    [ -n "$dev_node" ] || die "cannot resolve device identity for holder check: $name"
    holders="$SYS_DEV_BLOCK_ROOT/$dev_node/holders"
    [ ! -d "$holders" ] || [ -z "$(find "$holders" -mindepth 1 -maxdepth 1 -print -quit)" ] || die "target device has holders: $name"
  done < <("$LSBLK_CMD" -nrpo NAME "$target")
  for name in POWER_LOSS_PROVIDER_VALIDATE_HOOK POWER_LOSS_WORKLOAD_PREPARE_HOOK POWER_LOSS_CUT_AT_BARRIER_HOOK POWER_LOSS_PROVIDER_REBOOT_HOOK POWER_LOSS_POST_REBOOT_VERIFY_HOOK; do require_hook "$name"; done
  printf 'validated live disposable %s target %s (%s); no hook was executed\n' "$provider" "$target" "$fs"
}

capture_hook() {
  local stage="$2" cutpoint="$3" trial_dir="$4"
  local hook_name="$1" canonical_name="${1}_CANONICAL" sha_name="${1}_SHA256" snapshot_name="${1}_SNAPSHOT" snapshot_rel_name="${1}_SNAPSHOT_REL"
  local hook="${!snapshot_name}"
  local started ended rc stdout_tmp stderr_tmp
  mkdir -p "$trial_dir"
  [ "$(sha256_file "$hook")" = "${!sha_name}" ] || die "snapshotted hook changed before execution: $hook_name"
  stdout_tmp="$trial_dir/$stage.stdout.json.tmp"; stderr_tmp="$trial_dir/$stage.stderr.tmp"
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$hook" "$POWER_LOSS_TARGET" "$cutpoint" "$trial_dir" >"$stdout_tmp" 2>"$stderr_tmp"
  rc=$?
  set -e
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  [ "$(sha256_file "$hook")" = "${!sha_name}" ] || die "snapshotted hook changed during execution: $hook_name"
  mv "$stdout_tmp" "$trial_dir/$stage.stdout.json"; mv "$stderr_tmp" "$trial_dir/$stage.stderr"
  jq -n --arg hook_original_canonical "${!canonical_name}" --arg hook_snapshot_path "${!snapshot_rel_name}" --arg hook_sha256 "${!sha_name}" --arg stage "$stage" --arg cutpoint "$cutpoint" --arg started "$started" --arg ended "$ended" \
    --arg stdout_sha256 "$(sha256_file "$trial_dir/$stage.stdout.json")" --arg stderr_sha256 "$(sha256_file "$trial_dir/$stage.stderr")" --argjson rc "$rc" \
    '{schema_version:1,kind:"hook-capture",hook_original_canonical:$hook_original_canonical,hook_snapshot_path:$hook_snapshot_path,hook_sha256:$hook_sha256,stage:$stage,cutpoint:$cutpoint,started_at:$started,ended_at:$ended,rc:$rc,stdout_sha256:$stdout_sha256,stderr_sha256:$stderr_sha256}' \
    > "$trial_dir/$stage.meta.json.tmp"
  mv "$trial_dir/$stage.meta.json.tmp" "$trial_dir/$stage.meta.json"
  [ "$rc" -eq 0 ] || die "$stage hook failed; captured in $trial_dir"
}

write_manifest() {
  local dir="$1" path rel digest
  local entries_tmp sums_tmp manifest_tmp
  dir="$(cd "$dir" && pwd -P)"
  entries_tmp="$(mktemp)"; sums_tmp="$(mktemp)"; manifest_tmp="$(mktemp)"
  : > "$entries_tmp"; : > "$sums_tmp"
  while IFS= read -r path; do
    rel="${path#"$dir/"}"; digest="$(sha256_file "$path")"
    jq -cn --arg path "$rel" --arg sha256 "$digest" '{path:$path,sha256:$sha256}' >> "$entries_tmp"
    printf '%s  %s\n' "$digest" "$rel" >> "$sums_tmp"
  done < <(find "$dir" -type f ! -path "$dir/manifest.json" ! -path "$dir/SHA256SUMS" | LC_ALL=C sort)
  jq -s --slurpfile provider "$dir/declarations/provider.json" --slurpfile durability "$dir/declarations/durability.json" \
    '{schema_version:1,kind:"power-loss-artifact-manifest",publishable:false,provider:$provider[0],durability:$durability[0],files:.}' "$entries_tmp" > "$manifest_tmp"
  mv "$manifest_tmp" "$dir/manifest.json"; mv "$sums_tmp" "$dir/SHA256SUMS"
  rm -f "$entries_tmp"
}

provider_run_envelope() {
  [ $# = 1 ] || { usage >&2; exit 2; }
  [ "${POWER_LOSS_DURABILITY_OPT_IN:-}" = I_UNDERSTAND_DESTRUCTIVE_POWER_LOSS ] || die 'provider-run-envelope is disabled until exact destructive opt-in is set'
  [ "${POWER_LOSS_STATIC_TEST_MODE:-}" != I_ACKNOWLEDGE_NO_REAL_DEVICE ] || die 'provider-run-envelope refuses static-test command shims'
  validate_live
  local dir="$1" cutpoint trial
  [ ! -e "$dir" ] || die 'output directory must not already exist'
  mkdir -p "$dir/declarations" "$dir/raw"
  snapshot_hooks "$dir"
  cp "$POWER_LOSS_PROVIDER_DECLARATION" "$dir/declarations/provider.json"
  cp "$POWER_LOSS_DURABILITY_DECLARATION" "$dir/declarations/durability.json"
  for cutpoint in pre-ack post-ack; do
    trial="$dir/raw/$cutpoint"; mkdir -p "$trial"
    capture_hook POWER_LOSS_PROVIDER_VALIDATE_HOOK provider-validate "$cutpoint" "$trial"
    jq -e --arg cutpoint "$cutpoint" --slurpfile p "$dir/declarations/provider.json" '
      .schema_version==1 and .kind=="machine-capture" and .stage=="provider-validate" and .cutpoint==$cutpoint and
      .live_target_safe==true and .logical_process_kill==false and
      .controller_id==$p[0].controller_id and .target==$p[0].target and .provider==$p[0].provider and .filesystem==$p[0].filesystem
    ' "$trial/provider-validate.stdout.json" >/dev/null || die 'live provider hook did not corroborate the exact safe target; refusing power cut'
    capture_hook POWER_LOSS_WORKLOAD_PREPARE_HOOK workload-prepare "$cutpoint" "$trial"
    jq -e --arg cutpoint "$cutpoint" --slurpfile p "$dir/declarations/provider.json" '
      .schema_version==1 and .kind=="machine-capture" and .stage=="workload-prepare" and .cutpoint==$cutpoint and
      .controller_id==$p[0].controller_id and .target==$p[0].target and .logical_process_kill==false and
      (.prepared|type)=="array" and (.prepared|length)==2 and ([.prepared[].system]|sort)==["hiqlite","rhiza"] and
      ([.prepared[].id]|unique|length)==2 and (.prepared|all((keys|sort)==["id","system"]))
    ' "$trial/workload-prepare.stdout.json" >/dev/null || die 'workload did not emit two system-bound unique prepared IDs; refusing power cut'
    capture_hook POWER_LOSS_CUT_AT_BARRIER_HOOK cut-at-barrier "$cutpoint" "$trial"
    jq -s -e --arg cutpoint "$cutpoint" --slurpfile p "$dir/declarations/provider.json" --slurpfile c "$trial/cut-at-barrier.meta.json" '
      .[0].prepared as $prepared | .[1] as $cut |
      ($cut.power_removed_at|fromdateiso8601) as $removed |
      $cut.schema_version==1 and $cut.kind=="machine-capture" and $cut.stage=="cut-at-barrier" and $cut.cutpoint==$cutpoint and $cut.mode==$cutpoint and
      $cut.controller_id==$p[0].controller_id and $cut.target==$p[0].target and $cut.logical_process_kill==false and
      ($cut.captured_at|fromdateiso8601)>=$removed and $cut.barrier_held_until_power_removed==true and
      ($cut.controller_event_id|type)=="string" and ($cut.controller_event_id|length)>0 and
      ($cut.entries|length)==2 and ([ $cut.entries[].system ]|sort)==["hiqlite","rhiza"] and
      ([ $prepared[] | [.system,.id] ]|sort)==([ $cut.entries[] | [.system,.id] ]|sort) and
      ($cut.entries|all(.request_started==true and
        (.request_started_at|fromdateiso8601)>=($c[0].started_at|fromdateiso8601) and
        (.request_started_at|fromdateiso8601)<$removed and
        .ack_kind==(if .system=="rhiza" then "object-authoritative-sync" else "comparable-durable-ack" end) and
        (if $cutpoint=="pre-ack" then
           .durable_ack_observed==false and .durable_ack_at==null and
           $cut.barrier_invariant=="controller-held-durable-ack-path-until-power-removed" and $cut.ack_prevented_until_power_removed==true
         else .durable_ack_observed==true and
           (.durable_ack_at|fromdateiso8601)>=(.request_started_at|fromdateiso8601) and
           (.durable_ack_at|fromdateiso8601)<$removed end)))
    ' "$trial/workload-prepare.stdout.json" "$trial/cut-at-barrier.stdout.json" >/dev/null || die 'atomic request barrier and physical power removal were not captured'
    capture_hook POWER_LOSS_PROVIDER_REBOOT_HOOK reboot "$cutpoint" "$trial"
    capture_hook POWER_LOSS_POST_REBOOT_VERIFY_HOOK verify "$cutpoint" "$trial"
  done
  write_manifest "$dir"
  printf 'provider envelope captured as nonpublishable; run validate-artifacts %s\n' "$dir"
}

validate_artifacts() {
  [ $# = 1 ] || { usage >&2; exit 2; }
  [ -d "$1" ] || die 'artifact directory does not exist'
  local dir manifest path expected actual cutpoint stage meta out actual_paths declared_paths
  dir="$(cd "$1" && pwd -P)"; manifest="$dir/manifest.json"
  [ -r "$manifest" ] && [ -r "$dir/SHA256SUMS" ] || die 'missing manifest or SHA256SUMS'
  jq -e '.schema_version==1 and .kind=="power-loss-artifact-manifest" and .publishable==false and (.files|type)=="array"' "$manifest" >/dev/null || die 'invalid manifest'
  POWER_LOSS_PROVIDER_DECLARATION="$dir/declarations/provider.json"
  POWER_LOSS_DURABILITY_DECLARATION="$dir/declarations/durability.json"
  validate_declarations
  jq -e --slurpfile p "$POWER_LOSS_PROVIDER_DECLARATION" --slurpfile d "$POWER_LOSS_DURABILITY_DECLARATION" '.provider==$p[0] and .durability==$d[0]' "$manifest" >/dev/null || die 'manifest declarations do not match captured declaration files'
  actual_paths="$(find "$dir" -type f ! -path "$dir/manifest.json" ! -path "$dir/SHA256SUMS" | sed "s|^$dir/||" | LC_ALL=C sort)"
  declared_paths="$(jq -r '.files[].path' "$manifest" | LC_ALL=C sort)"
  [ "$actual_paths" = "$declared_paths" ] || die 'manifest does not enumerate the exact artifact set'
  jq -e 'all(.files[]; (.path|test("^raw/(pre-ack|post-ack)/(cutpoint|power-cut)\\."))|not)' "$manifest" >/dev/null || die 'legacy separate cutpoint/power-cut evidence is nonpublishable'
  while IFS=$'\t' read -r path expected; do
    case "$path" in /*|..|../*|*/..|*/../*) die "unsafe manifest path: $path" ;; esac
    [ -f "$dir/$path" ] || die "missing artifact: $path"
    actual="$(sha256_file "$dir/$path")"; [ "$actual" = "$expected" ] || die "tampered artifact: $path"
  done < <(jq -r '.files[] | [.path,.sha256] | @tsv' "$manifest")
  diff -u <(jq -r '.files[] | "\(.sha256)  \(.path)"' "$manifest") "$dir/SHA256SUMS" >/dev/null || die 'SHA256SUMS does not match manifest'
  for cutpoint in pre-ack post-ack; do
    for stage in provider-validate workload-prepare cut-at-barrier reboot verify; do
      meta="$dir/raw/$cutpoint/$stage.meta.json"; out="$dir/raw/$cutpoint/$stage.stdout.json"
      [ -f "$meta" ] && [ -f "$out" ] && [ -f "$dir/raw/$cutpoint/$stage.stderr" ] || die "missing $cutpoint/$stage capture"
      jq -e --arg meta "raw/$cutpoint/$stage.meta.json" --arg out "raw/$cutpoint/$stage.stdout.json" --arg err "raw/$cutpoint/$stage.stderr" \
        'any(.files[];.path==$meta) and any(.files[];.path==$out) and any(.files[];.path==$err)' "$manifest" >/dev/null || die "capture omitted from manifest: $cutpoint/$stage"
      jq -e --arg stage "$stage" --arg cutpoint "$cutpoint" '
        .schema_version==1 and .kind=="hook-capture" and .stage==$stage and .cutpoint==$cutpoint and .rc==0 and
        (.started_at|fromdateiso8601) <= (.ended_at|fromdateiso8601) and
        (.hook_original_canonical|type)=="string" and (.hook_original_canonical|startswith("/")) and
        .hook_snapshot_path==("hooks/"+$stage) and
        (.hook_sha256|test("^[0-9a-f]{64}$"))
      ' "$meta" >/dev/null || die "invalid capture metadata: $cutpoint/$stage"
      [ -f "$dir/hooks/$stage" ] && [ -x "$dir/hooks/$stage" ] && [ ! -L "$dir/hooks/$stage" ] || die "missing or unsafe snapshotted hook: $stage"
      [ "$(jq -r .hook_sha256 "$meta")" = "$(sha256_file "$dir/hooks/$stage")" ] || die "executed hook snapshot hash mismatch: $cutpoint/$stage"
      jq -e --arg hook "hooks/$stage" 'any(.files[];.path==$hook)' "$manifest" >/dev/null || die "hook snapshot omitted from manifest: $stage"
      [ "$(jq -r .stdout_sha256 "$meta")" = "$(sha256_file "$out")" ] || die "stdout capture hash mismatch: $cutpoint/$stage"
      [ "$(jq -r .stderr_sha256 "$meta")" = "$(sha256_file "$dir/raw/$cutpoint/$stage.stderr")" ] || die "stderr capture hash mismatch: $cutpoint/$stage"
      jq -e --arg stage "$stage" --arg cutpoint "$cutpoint" --slurpfile m "$manifest" --slurpfile c "$meta" '
        .schema_version==1 and .kind=="machine-capture" and .stage==$stage and .cutpoint==$cutpoint and
        ((.captured_at|fromdateiso8601) >= ($c[0].started_at|fromdateiso8601)) and
        ((.captured_at|fromdateiso8601) <= ($c[0].ended_at|fromdateiso8601)) and
        .controller_id==$m[0].provider.controller_id and .target==$m[0].provider.target and
        .provider==$m[0].provider.provider and .filesystem==$m[0].provider.filesystem and
        .rhiza_commit==$m[0].provider.rhiza_commit and .hiqlite_commit==$m[0].provider.hiqlite_commit and
        .rhiza_image==$m[0].provider.rhiza_image and .hiqlite_image==$m[0].provider.hiqlite_image and
        .logical_process_kill==false and
        ($m[0].provider.csi==null or (.csi.evidence==true and .csi.driver==$m[0].provider.csi.driver))
      ' "$out" >/dev/null || die "invalid or logical-kill machine capture: $cutpoint/$stage"
      case "$stage" in
        provider-validate) jq -e '.live_target_safe==true' "$out" >/dev/null || die "provider did not attest live target validation: $cutpoint" ;;
        workload-prepare) jq -e '(.prepared|type)=="array" and (.prepared|length)==2 and ([.prepared[].system]|sort)==["hiqlite","rhiza"] and ([.prepared[].id]|unique|length)==2 and (.prepared|all((keys|sort)==["id","system"]))' "$out" >/dev/null || die "invalid system-bound prepared IDs: $cutpoint" ;;
        cut-at-barrier) jq -e --arg cutpoint "$cutpoint" --slurpfile c "$meta" '
          . as $barrier |
          (.power_removed_at|fromdateiso8601) as $removed |
          .mode==$cutpoint and .barrier_held_until_power_removed==true and
          ($c[0].started_at|fromdateiso8601)<=$removed and $removed<=($c[0].ended_at|fromdateiso8601) and
          (.captured_at|fromdateiso8601)>=$removed and
          (.controller_event_id|type)=="string" and (.controller_event_id|length)>0 and
          (.entries|length)==2 and ([.entries[].system]|sort)==["hiqlite","rhiza"] and
          (.entries|all(.request_started==true and
            (.request_started_at|fromdateiso8601)>=($c[0].started_at|fromdateiso8601) and
            (.request_started_at|fromdateiso8601)<$removed and
            .ack_kind==(if .system=="rhiza" then "object-authoritative-sync" else "comparable-durable-ack" end) and
            (if $cutpoint=="pre-ack" then
               .durable_ack_observed==false and .durable_ack_at==null and
               $barrier.ack_prevented_until_power_removed==true
             else .durable_ack_observed==true and
               (.durable_ack_at|fromdateiso8601)>=(.request_started_at|fromdateiso8601) and
               (.durable_ack_at|fromdateiso8601)<$removed end))) and
          (if $cutpoint=="pre-ack" then .barrier_invariant=="controller-held-durable-ack-path-until-power-removed" else true end)
        ' "$out" >/dev/null || die "atomic request barrier/power removal failed: $cutpoint" ;;
        reboot) jq -e '.reboot_restart_observed==true and (.boot_id_before|type)=="string" and (.boot_id_after|type)=="string" and .boot_id_before!=.boot_id_after' "$out" >/dev/null || die "reboot/restart transition was not captured: $cutpoint" ;;
      esac
    done
    jq -e --arg cutpoint "$cutpoint" --slurpfile m "$manifest" '
      .stage=="verify" and (.service_rto_ms|type)=="number" and .service_rto_ms>=0 and
      (.full_rto_ms|type)=="number" and .full_rto_ms>=.service_rto_ms and
      .matched_rpo_ms==$m[0].durability.matched_rpo_ms and
      .durability_observed.rhiza_ack=="object-authoritative-sync" and
      .durability_observed.hiqlite_ack=="comparable-durable-ack" and
      .durability_observed.fsync_policy_match==true and .durability_observed.log_policy_match==true and
      .durability_observed.failure_rpo_semantics_match==true and
      (.ledger|length)==2 and ([.ledger[].system]|sort)==["hiqlite","rhiza"] and ([.ledger[].id]|unique|length)==2 and
      (.final_lookup|length)==2 and
      (.ledger|all(.cutpoint==$cutpoint and (.id|type)=="string" and (.id|length)>0 and
        (.occurrences|type)=="number" and (.occurrences|floor)==.occurrences and (.occurrences==0 or .occurrences==1) and
        (.observed_rpo_ms|type)=="number" and .observed_rpo_ms>=0 and
        .matched_rpo_ms==$m[0].durability.matched_rpo_ms and .observed_rpo_ms<=.matched_rpo_ms and
        ((.final=="present" and .occurrences==1 and .classification=="present") or
         (.final=="absent" and .occurrences==0 and .classification=="absent-within-declared-rpo")) and
        (if $cutpoint=="post-ack" then .final=="present" else true end))) and
      (. as $v | .ledger | all(. as $row | any($v.final_lookup[];
        .system==$row.system and .id==$row.id and (.occurrences|type)=="number" and (.occurrences|floor)==.occurrences and
        .occurrences==$row.occurrences and .found==($row.final=="present"))))
    ' "$dir/raw/$cutpoint/verify.stdout.json" >/dev/null || die "RPO/RTO/ledger correctness failed: $cutpoint"
    jq -s -e '
      ([.[0].prepared[] | [.system,.id]]|sort)==([.[1].ledger[] | [.system,.id]]|sort) and
      ([.[0].prepared[] | [.system,.id]]|sort)==([.[2].entries[] | [.system,.id]]|sort) and
      ([.[0].prepared[] | [.system,.id]]|sort)==([.[1].final_lookup[] | [.system,.id]]|sort)
    ' \
      "$dir/raw/$cutpoint/workload-prepare.stdout.json" "$dir/raw/$cutpoint/verify.stdout.json" "$dir/raw/$cutpoint/cut-at-barrier.stdout.json" >/dev/null || die "prepared, barrier, and verified IDs differ: $cutpoint"
    jq -s -e --slurpfile reboot_meta "$dir/raw/$cutpoint/reboot.meta.json" '
      (.[0].power_removed_at|fromdateiso8601) as $removed |
      .[1].reboot_restart_observed==true and (.[1].boot_transition_at|fromdateiso8601)>$removed and
      (.[1].boot_transition_at|fromdateiso8601)>=($reboot_meta[0].started_at|fromdateiso8601) and
      (.[1].captured_at|fromdateiso8601)>=(.[1].boot_transition_at|fromdateiso8601)
    ' "$dir/raw/$cutpoint/cut-at-barrier.stdout.json" "$dir/raw/$cutpoint/reboot.stdout.json" >/dev/null || die "reboot transition is not after physical power removal: $cutpoint"
    jq -s -e '.[0].ended_at<=.[1].started_at and .[1].ended_at<=.[2].started_at and .[2].ended_at<=.[3].started_at and .[3].ended_at<=.[4].started_at' \
      "$dir/raw/$cutpoint/provider-validate.meta.json" "$dir/raw/$cutpoint/workload-prepare.meta.json" "$dir/raw/$cutpoint/cut-at-barrier.meta.json" "$dir/raw/$cutpoint/reboot.meta.json" "$dir/raw/$cutpoint/verify.meta.json" >/dev/null || die "hook stage chronology is invalid: $cutpoint"
  done
  for stage in provider-validate workload-prepare cut-at-barrier reboot verify; do
    jq -s -e '.[0].hook_original_canonical==.[1].hook_original_canonical and .[0].hook_snapshot_path==.[1].hook_snapshot_path and .[0].hook_sha256==.[1].hook_sha256' \
      "$dir/raw/pre-ack/$stage.meta.json" "$dir/raw/post-ack/$stage.meta.json" >/dev/null || die "hook identity changed between trials: $stage"
  done
  jq -s -e '([.[].ledger[].id]|length)==([.[].ledger[].id]|unique|length)' "$dir/raw/pre-ack/verify.stdout.json" "$dir/raw/post-ack/verify.stdout.json" >/dev/null || die 'duplicate unique IDs across trials'
  printf 'publishable-eligible: artifact correctness passed; publication still requires repeated rotated trials\n'
}

plan() {
  [ $# = 1 ] || { usage >&2; exit 2; }
  mkdir -p "$(dirname "$1")"
  jq -n --arg git_head "$(git rev-parse HEAD)" '{schema_version:2,kind:"power-loss-durability-plan",git_head:$git_head,logical_process_kill_is_power_loss:false,commands:["validate-live","provider-run-envelope","validate-artifacts"],cutpoints:["pre-ack","post-ack"],publish_rule:"validate-artifacts plus repeated rotated trials"}' > "$1"
}

case "${1:-}" in
  plan) shift; plan "$@" ;;
  validate-live) shift; [ $# = 0 ] || { usage >&2; exit 2; }; validate_live ;;
  provider-run-envelope) shift; provider_run_envelope "$@" ;;
  validate-artifacts) shift; validate_artifacts "$@" ;;
  *) usage >&2; exit 2 ;;
esac
