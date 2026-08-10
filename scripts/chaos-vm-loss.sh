#!/usr/bin/env bash
# Abrupt QEMU process loss is a VM crash model, never physical power loss.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
die() { printf 'chaos-vm-loss: %s\n' "$*" >&2; exit 1; }
sha256_file() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
usage() { cat <<'EOF'
usage: scripts/chaos-vm-loss.sh plan
       scripts/chaos-vm-loss.sh run OUTPUT_DIR pre-ack|post-ack

run needs externally controlled launch, reboot, and verification hooks. The
controller SIGKILLs the attested QEMU_BINARY process; hooks must reuse the
exact QEMU_DISK path.
EOF
}
require_hook() { local n="$1" p="${!1:-}" canonical; [ -n "$p" ] && [ -x "$p" ] && [[ "$p" = /* ]] || die "$n must be an executable absolute path"; canonical="$(readlink -f "$p")"; [ "$canonical" = "$p" ] || die "$n must not be symlinked"; }
plan() { jq -n '{schema_version:1,kind:"qemu-vm-loss-plan",physical_power_loss:false,fault:"external-controller-sigkill-qemu",ack_boundaries:["pre-ack","post-ack"]}'; }
run() {
  [ $# = 2 ] || { usage >&2; exit 2; }
  local dir="$1" cutpoint="$2" before after pid started killed ended launch_sha reboot_sha verify_sha start_token
  case "$cutpoint" in pre-ack|post-ack) ;; *) die 'cutpoint must be pre-ack or post-ack' ;; esac
  [ "$(uname -s)" = Linux ] || die 'live VM-loss execution is Linux-only'
  [ "${QEMU_VM_LOSS_OPT_IN:-}" = I_UNDERSTAND_DISPOSABLE_VM_LOSS ] || die 'exact disposable-VM opt-in is required'
  [ -n "${QEMU_DISK:-}" ] && [ -f "$QEMU_DISK" ] && [[ "$QEMU_DISK" = /* ]] && [ "$(readlink -f "$QEMU_DISK")" = "$QEMU_DISK" ] || die 'QEMU_DISK must be a canonical absolute regular file'
  [ -n "${QEMU_BINARY:-}" ] && [ -x "$QEMU_BINARY" ] && [[ "$QEMU_BINARY" = /* ]] && [ "$(readlink -f "$QEMU_BINARY")" = "$QEMU_BINARY" ] || die 'QEMU_BINARY must be a canonical executable path'
  [ ! -e "$dir" ] || die 'output directory must not exist'; mkdir -p "$dir"; started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  require_hook QEMU_VM_LAUNCH_HOOK; require_hook QEMU_VM_REBOOT_HOOK; require_hook QEMU_VM_VERIFY_HOOK
  before="$(sha256_file "$QEMU_DISK")"; QEMU_VM_LAUNCH_HOOK "$QEMU_DISK" "$cutpoint" "$dir" > "$dir/launch.json"
  pid="$(jq -er --arg binary "$QEMU_BINARY" --arg disk "$QEMU_DISK" --arg before "$before" '.schema_version==1 and .kind=="qemu-vm-launch" and .physical_power_loss==false and .qemu_binary==$binary and .disk==$disk and .disk_sha256_before==$before and (.qemu_pid|type)=="number" and (.qemu_pid|floor)==.qemu_pid | .qemu_pid' "$dir/launch.json")" || die 'launch hook did not attest the exact QEMU binary, disk, and PID'
  launch_sha="$(sha256_file "$QEMU_VM_LAUNCH_HOOK")"; reboot_sha="$(sha256_file "$QEMU_VM_REBOOT_HOOK")"; verify_sha="$(sha256_file "$QEMU_VM_VERIFY_HOOK")"
  kill -0 "$pid" 2>/dev/null || die 'attested QEMU PID is not alive'
  [ "$(readlink -f "/proc/$pid/exe")" = "$QEMU_BINARY" ] || die 'attested PID is not executing QEMU_BINARY'
  start_token="$(awk '{print $22}' "/proc/$pid/stat")"; [ -n "$start_token" ] || die 'cannot attest QEMU process start identity'
  "$root/scripts/chaos-pidfd-kill.py" "$pid" "$QEMU_BINARY" "$start_token" > "$dir/pidfd-sigkill.json" || die 'pidfd QEMU SIGKILL failed'
  killed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; printf '%s\n' "$pid" > "$dir/qemu-sigkill.pid"
  [ "$(sha256_file "$QEMU_VM_LAUNCH_HOOK")" = "$launch_sha" ] || die 'launch hook changed during run'
  QEMU_VM_REBOOT_HOOK "$QEMU_DISK" "$cutpoint" "$dir" > "$dir/reboot.json"; [ "$(sha256_file "$QEMU_VM_REBOOT_HOOK")" = "$reboot_sha" ] || die 'reboot hook changed during run'; after="$(sha256_file "$QEMU_DISK")"
  jq -e --arg disk "$QEMU_DISK" '.schema_version==1 and .kind=="qemu-vm-reboot" and .disk==$disk' "$dir/reboot.json" >/dev/null || die 'reboot hook did not attest the exact disk'
  QEMU_VM_VERIFY_HOOK "$QEMU_DISK" "$cutpoint" "$dir" > "$dir/verify.json"; [ "$(sha256_file "$QEMU_VM_VERIFY_HOOK")" = "$verify_sha" ] || die 'verify hook changed during run'
  jq -e --arg disk "$QEMU_DISK" --arg cutpoint "$cutpoint" '.schema_version==1 and .kind=="chaos-workload-boundary" and .phase=="verify" and .cutpoint==$cutpoint and .disk==$disk and .fault_observed==true and (.entries|length)==2 and (if $cutpoint=="pre-ack" then (.entries|all(.durable_ack_observed==false)) else (.entries|all(.durable_ack_observed==true)) end)' "$dir/verify.json" >/dev/null || die 'invalid verification boundary'
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n --arg disk "$QEMU_DISK" --arg before "$before" --arg after "$after" --arg cutpoint "$cutpoint" --arg binary "$QEMU_BINARY" --arg started "$started" --arg killed "$killed" --arg ended "$ended" --arg launch "$(sha256_file "$dir/launch.json")" --arg reboot "$(sha256_file "$dir/reboot.json")" --arg verify "$(sha256_file "$dir/verify.json")" --argjson pid "$pid" '{schema_version:1,kind:"qemu-vm-loss-artifact",physical_power_loss:false,fault_class:"vm_abrupt_loss",fault:"external-controller-sigkill-qemu",started_at:$started,sigkill_at:$killed,ended_at:$ended,qemu_binary:$binary,disk:$disk,disk_sha256_before:$before,disk_sha256_after:$after,cutpoint:$cutpoint,qemu_pid:$pid,evidence_sha256:{launch:$launch,reboot:$reboot,verify:$verify}}' > "$dir/manifest.json"
}
case "${1:-}" in plan) [ $# = 1 ] || exit 2; plan ;; run) shift; run "$@" ;; *) usage >&2; exit 2 ;; esac
