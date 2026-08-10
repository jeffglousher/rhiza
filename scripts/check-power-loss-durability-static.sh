#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"; cd "$repo_root"
tmp="$(mktemp -d)"; tmp="$(cd -P "$tmp" && pwd -P)"; trap 'rm -rf "$tmp"' EXIT
gate=scripts/bench-power-loss-durability.sh
sha256_file() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
expect_reject() { if bash "$gate" validate-artifacts "$1" >"$tmp/reject.out" 2>&1; then echo "invalid fixture passed: $1" >&2; exit 1; fi; }
rehash_capture() {
  local dir="$1" cutpoint="$2" stage="$3" out="$1/raw/$2/$3.stdout.json" meta="$1/raw/$2/$3.meta.json"
  jq --arg sha "$(sha256_file "$out")" '.stdout_sha256=$sha' "$meta" > "$tmp/meta-new"; mv "$tmp/meta-new" "$meta"
}

refresh_manifest() {
  local dir="$1" entries="$tmp/entries.jsonl" sums="$tmp/sums" path rel digest
  : > "$entries"; : > "$sums"
  while IFS= read -r path; do
    rel="${path#"$dir/"}"; digest="$(sha256_file "$path")"
    jq -cn --arg path "$rel" --arg sha256 "$digest" '{path:$path,sha256:$sha256}' >> "$entries"
    printf '%s  %s\n' "$digest" "$rel" >> "$sums"
  done < <(find "$dir" -type f ! -path "$dir/manifest.json" ! -path "$dir/SHA256SUMS" | LC_ALL=C sort)
  jq -s --slurpfile provider "$dir/declarations/provider.json" --slurpfile durability "$dir/declarations/durability.json" \
    '{schema_version:1,kind:"power-loss-artifact-manifest",publishable:false,provider:$provider[0],durability:$durability[0],files:.}' "$entries" > "$dir/manifest.json"
  cp "$sums" "$dir/SHA256SUMS"
}

make_fixture() {
  local dir="$1" cutpoint stage out meta started ended captured hook_sha
  mkdir -p "$dir/declarations" "$dir/hooks"
  jq -n '{schema_version:1,kind:"provider-declaration",disposable:true,provider:"vm",target:"/dev/fixture0",filesystem:"ext4",controller_id:"controller-fixture",rhiza_commit:"rhiza-commit",hiqlite_commit:"hiqlite-commit",rhiza_image:"sha256:rhiza",hiqlite_image:"sha256:hiqlite",csi:null}' > "$dir/declarations/provider.json"
  jq -n '{schema_version:1,kind:"durability-declaration",rhiza_ack:"object-authoritative-sync",hiqlite_ack:"comparable-durable-ack",fsync_policy_match:true,log_policy_match:true,failure_rpo_semantics_match:true,matched_rpo_ms:10}' > "$dir/declarations/durability.json"
  for stage in provider-validate workload-prepare cut-at-barrier reboot verify; do
    printf '#!/usr/bin/env bash\n# fixture %s\nexit 0\n' "$stage" > "$dir/hooks/$stage"; chmod 500 "$dir/hooks/$stage"
  done
  for cutpoint in pre-ack post-ack; do
    mkdir -p "$dir/raw/$cutpoint"
    for stage in provider-validate workload-prepare cut-at-barrier reboot verify; do
      hook_sha="$(sha256_file "$dir/hooks/$stage")"
      case "$stage" in
        provider-validate) started=00; ended=01; captured=01 ;;
        workload-prepare) started=02; ended=03; captured=03 ;;
        cut-at-barrier) started=04; ended=06; captured=05 ;;
        reboot) started=07; ended=09; captured=08 ;;
        verify) started=10; ended=11; captured=10 ;;
      esac
      out="$dir/raw/$cutpoint/$stage.stdout.json"; meta="$dir/raw/$cutpoint/$stage.meta.json"
      jq -n --arg stage "$stage" --arg cutpoint "$cutpoint" --arg captured "2026-08-10T00:00:${captured}Z" '{schema_version:1,kind:"machine-capture",stage:$stage,cutpoint:$cutpoint,captured_at:$captured,controller_id:"controller-fixture",target:"/dev/fixture0",provider:"vm",filesystem:"ext4",rhiza_commit:"rhiza-commit",hiqlite_commit:"hiqlite-commit",rhiza_image:"sha256:rhiza",hiqlite_image:"sha256:hiqlite",logical_process_kill:false}' > "$out"
      jq --arg cutpoint "$cutpoint" '. + {live_target_safe:true,prepared:(["rhiza","hiqlite"]|map(. as $system|{system:$system,id:($cutpoint+"-"+$system+"-id")})),mode:$cutpoint,power_removed_at:"2026-08-10T00:00:05Z",barrier_held_until_power_removed:true,barrier_invariant:"controller-held-durable-ack-path-until-power-removed",ack_prevented_until_power_removed:($cutpoint=="pre-ack"),entries:(["rhiza","hiqlite"]|map(. as $system|{system:$system,id:($cutpoint+"-"+$system+"-id"),request_started:true,request_started_at:"2026-08-10T00:00:04Z",durable_ack_observed:($cutpoint=="post-ack"),durable_ack_at:(if $cutpoint=="post-ack" then "2026-08-10T00:00:04Z" else null end),ack_kind:(if $system=="rhiza" then "object-authoritative-sync" else "comparable-durable-ack" end)})),controller_event_id:("event-"+$cutpoint),reboot_restart_observed:true,boot_id_before:("before-"+$cutpoint),boot_id_after:("after-"+$cutpoint),boot_transition_at:"2026-08-10T00:00:08Z"}' "$out" > "$out.new"; mv "$out.new" "$out"
      if [ "$stage" = verify ]; then
        jq --arg cutpoint "$cutpoint" '. + {service_rto_ms:2,full_rto_ms:4,matched_rpo_ms:10,durability_observed:{rhiza_ack:"object-authoritative-sync",hiqlite_ack:"comparable-durable-ack",fsync_policy_match:true,log_policy_match:true,failure_rpo_semantics_match:true},ledger:(["rhiza","hiqlite"] | map(. as $system | {system:$system,id:($cutpoint+"-"+$system+"-id"),cutpoint:$cutpoint,final:"present",occurrences:1,observed_rpo_ms:3,matched_rpo_ms:10,classification:"present"}))} | .final_lookup=[.ledger[]|{system,id,found:(.final=="present"),occurrences}]' "$out" > "$out.new"; mv "$out.new" "$out"
      fi
      : > "$dir/raw/$cutpoint/$stage.stderr"
      jq -n --arg stage "$stage" --arg cutpoint "$cutpoint" --arg hook "/fixture/$stage-hook" --arg snapshot "hooks/$stage" --arg hook_sha256 "$hook_sha" --arg started "2026-08-10T00:00:${started}Z" --arg ended "2026-08-10T00:00:${ended}Z" --arg stdout_sha256 "$(sha256_file "$out")" --arg stderr_sha256 "$(sha256_file "$dir/raw/$cutpoint/$stage.stderr")" '{schema_version:1,kind:"hook-capture",hook_original_canonical:$hook,hook_snapshot_path:$snapshot,hook_sha256:$hook_sha256,stage:$stage,cutpoint:$cutpoint,started_at:$started,ended_at:$ended,rc:0,stdout_sha256:$stdout_sha256,stderr_sha256:$stderr_sha256}' > "$meta"
    done
  done
  refresh_manifest "$dir"
}

make_live_shims() {
  local bin="$tmp/live-bin"; mkdir -p "$bin"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "Linux\\n"' > "$bin/uname"
  # shellcheck disable=SC2016 # These single-quoted strings are fixture script source.
  printf '%s\n' '#!/usr/bin/env bash' 'target="${!#}"' 'if [ "$target" = /dev/fixture0 ]; then [ "${FAKE_STATE:-}" = canonical ] && printf "/dev/other\\n" || printf "%s\\n" "$target"; else parent="${target%/*}"; name="${target##*/}"; printf "%s/%s\\n" "$(cd -P "$parent" && pwd -P)" "$name"; fi' > "$bin/readlink"
  # shellcheck disable=SC2016 # These single-quoted strings are fixture script source.
  printf '%s\n' '#!/usr/bin/env bash' 'args="$*"; target="${!#}"' \
    'case "$args" in *" TYPE "*) printf "part\\n";; *FSTYPE*) [ "${FAKE_STATE:-}" = filesystem ] && printf "xfs\\n" || printf "ext4\\n";; *MOUNTPOINTS*) case "${FAKE_STATE:-}" in mounted-target) printf "/mnt/target\\n";; mounted-child) printf "\\n/mnt/child\\n";; esac;; *"-s -nrpo NAME"*) if [ "$target" = /dev/system ]; then [ "${FAKE_STATE:-}" = root ] && printf "/dev/fixture0\\n/dev/system\\n" || printf "/dev/system\\n"; elif [ "$target" = /dev/swap ]; then [ "${FAKE_STATE:-}" = swap ] && printf "/dev/fixture0\\n/dev/swap\\n" || printf "/dev/swap\\n"; fi;; *"MAJ:MIN"*) [ "$target" = /dev/fixture-child ] && printf "7:2\\n" || printf "7:1\\n";; *"-nrpo NAME"*) printf "/dev/fixture0\\n/dev/fixture-child\\n";; esac' > "$bin/lsblk"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "/dev/system\\n"' > "$bin/findmnt"
  chmod 700 "$bin/uname" "$bin/readlink" "$bin/lsblk" "$bin/findmnt"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 97' > "$tmp/live-hook"; chmod 700 "$tmp/live-hook"
}

run_live_case() {
  local expectation="$1" state="$2" hook_mode="${3:-normal}"
  local base="$tmp/live-$state-$hook_mode" hook="$tmp/live-hook"
  mkdir -p "$base/sys/7:1/holders" "$base/sys/7:2/holders"
  printf 'Filename Type Size Used Priority\n' > "$base/swaps"
  [ "$state" != swap ] || printf '/dev/swap partition 1 0 -2\n' >> "$base/swaps"
  [ "$state" != holders ] || : > "$base/sys/7:1/holders/dm-0"
  if [ "$hook_mode" = symlink ]; then ln -s "$tmp/live-hook" "$base/hook-link"; hook="$base/hook-link"; fi
  if [ "$hook_mode" = component ]; then mkdir -p "$base/real"; cp "$tmp/live-hook" "$base/real/hook"; ln -s "$base/real" "$base/link-dir"; hook="$base/link-dir/hook"; fi
  if [ "$hook_mode" = relative ]; then hook=relative-hook; fi
  local -a live_env=(
    POWER_LOSS_STATIC_TEST_MODE=I_ACKNOWLEDGE_NO_REAL_DEVICE
    POWER_LOSS_UNAME_CMD="$tmp/live-bin/uname" POWER_LOSS_LSBLK_CMD="$tmp/live-bin/lsblk"
    POWER_LOSS_FINDMNT_CMD="$tmp/live-bin/findmnt" POWER_LOSS_READLINK_CMD="$tmp/live-bin/readlink"
    POWER_LOSS_SWAPS_FILE="$base/swaps" POWER_LOSS_SYS_DEV_BLOCK_ROOT="$base/sys"
    POWER_LOSS_PROVIDER_DECLARATION="$tmp/valid/declarations/provider.json"
    POWER_LOSS_DURABILITY_DECLARATION="$tmp/valid/declarations/durability.json"
    POWER_LOSS_TARGET=/dev/fixture0 FAKE_STATE="$state"
    POWER_LOSS_PROVIDER_VALIDATE_HOOK="$hook" POWER_LOSS_WORKLOAD_PREPARE_HOOK="$tmp/live-hook"
    POWER_LOSS_CUT_AT_BARRIER_HOOK="$tmp/live-hook"
    POWER_LOSS_PROVIDER_REBOOT_HOOK="$tmp/live-hook" POWER_LOSS_POST_REBOOT_VERIFY_HOOK="$tmp/live-hook"
  )
  if env "${live_env[@]}" bash "$gate" validate-live > "$base/out" 2>&1; then
    [ "$expectation" = pass ] || { echo "unsafe live fixture passed: $state/$hook_mode" >&2; exit 1; }
  else
    [ "$expectation" = reject ] || { cat "$base/out" >&2; exit 1; }
  fi
}

bash -n "$gate"; bash "$gate" plan "$tmp/plan.json"
jq -e '.logical_process_kill_is_power_loss==false and .commands==["validate-live","provider-run-envelope","validate-artifacts"]' "$tmp/plan.json" >/dev/null
if POWER_LOSS_DURABILITY_OPT_IN='' POWER_LOSS_TARGET=/dev/never bash "$gate" provider-run-envelope "$tmp/must-not-exist" >"$tmp/default.out" 2>&1; then
  echo 'default provider run unexpectedly passed' >&2; exit 1
fi
[ ! -e "$tmp/must-not-exist" ] || { echo 'default provider run changed state' >&2; exit 1; }

make_fixture "$tmp/valid"
bash "$gate" validate-artifacts "$tmp/valid" | rg -q '^publishable-eligible:'
make_live_shims
run_live_case pass normal
run_live_case reject canonical
run_live_case reject filesystem
run_live_case reject mounted-target
run_live_case reject mounted-child
run_live_case reject root
run_live_case reject swap
run_live_case reject holders
run_live_case reject hook-symlink symlink
run_live_case reject hook-component component
run_live_case reject hook-relative relative

cp -R "$tmp/valid" "$tmp/logical"; jq '.logical_process_kill=true' "$tmp/logical/raw/pre-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/logical/raw/pre-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/logical" pre-ack cut-at-barrier; refresh_manifest "$tmp/logical"; expect_reject "$tmp/logical"
cp -R "$tmp/valid" "$tmp/missing"; rm "$tmp/missing/raw/post-ack/reboot.stdout.json"; expect_reject "$tmp/missing"
cp -R "$tmp/valid" "$tmp/duplicate"; jq '.ledger[1].id=.ledger[0].id' "$tmp/duplicate/raw/post-ack/verify.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/duplicate/raw/post-ack/verify.stdout.json"; jq --arg sha "$(sha256_file "$tmp/duplicate/raw/post-ack/verify.stdout.json")" '.stdout_sha256=$sha' "$tmp/duplicate/raw/post-ack/verify.meta.json" > "$tmp/x"; mv "$tmp/x" "$tmp/duplicate/raw/post-ack/verify.meta.json"; refresh_manifest "$tmp/duplicate"; expect_reject "$tmp/duplicate"
cp -R "$tmp/valid" "$tmp/unknown"; jq '.ledger[0].classification="unknown"' "$tmp/unknown/raw/pre-ack/verify.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/unknown/raw/pre-ack/verify.stdout.json"; jq --arg sha "$(sha256_file "$tmp/unknown/raw/pre-ack/verify.stdout.json")" '.stdout_sha256=$sha' "$tmp/unknown/raw/pre-ack/verify.meta.json" > "$tmp/x"; mv "$tmp/x" "$tmp/unknown/raw/pre-ack/verify.meta.json"; refresh_manifest "$tmp/unknown"; expect_reject "$tmp/unknown"
cp -R "$tmp/valid" "$tmp/tampered"; printf 'tampered\n' >> "$tmp/tampered/raw/pre-ack/reboot.stderr"; expect_reject "$tmp/tampered"
cp -R "$tmp/valid" "$tmp/not-equal"; jq 'del(.fsync_policy_match)' "$tmp/not-equal/declarations/durability.json" > "$tmp/x"; mv "$tmp/x" "$tmp/not-equal/declarations/durability.json"; refresh_manifest "$tmp/not-equal"; expect_reject "$tmp/not-equal"
cp -R "$tmp/valid" "$tmp/pre-ack-lied"; jq '.entries |= map(.durable_ack_observed=true | .durable_ack_at="2026-08-10T00:00:04Z")' "$tmp/pre-ack-lied/raw/pre-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/pre-ack-lied/raw/pre-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/pre-ack-lied" pre-ack cut-at-barrier; refresh_manifest "$tmp/pre-ack-lied"; expect_reject "$tmp/pre-ack-lied"
cp -R "$tmp/valid" "$tmp/barrier-released"; jq '.barrier_held_until_power_removed=false' "$tmp/barrier-released/raw/pre-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/barrier-released/raw/pre-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/barrier-released" pre-ack cut-at-barrier; refresh_manifest "$tmp/barrier-released"; expect_reject "$tmp/barrier-released"
cp -R "$tmp/valid" "$tmp/post-ack-unacked"; jq '.entries |= map(.durable_ack_observed=false | .durable_ack_at=null)' "$tmp/post-ack-unacked/raw/post-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/post-ack-unacked/raw/post-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/post-ack-unacked" post-ack cut-at-barrier; refresh_manifest "$tmp/post-ack-unacked"; expect_reject "$tmp/post-ack-unacked"
cp -R "$tmp/valid" "$tmp/noncausal-cut"; jq '.power_removed_at="2026-08-10T00:00:03Z"' "$tmp/noncausal-cut/raw/pre-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/noncausal-cut/raw/pre-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/noncausal-cut" pre-ack cut-at-barrier; refresh_manifest "$tmp/noncausal-cut"; expect_reject "$tmp/noncausal-cut"
cp -R "$tmp/valid" "$tmp/id-rebound"; jq '.entries[0].id="different-id"' "$tmp/id-rebound/raw/post-ack/cut-at-barrier.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/id-rebound/raw/post-ack/cut-at-barrier.stdout.json"; rehash_capture "$tmp/id-rebound" post-ack cut-at-barrier; refresh_manifest "$tmp/id-rebound"; expect_reject "$tmp/id-rebound"
cp -R "$tmp/valid" "$tmp/arbitrary"; jq '.ledger[0].classification="lost"' "$tmp/arbitrary/raw/pre-ack/verify.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/arbitrary/raw/pre-ack/verify.stdout.json"; rehash_capture "$tmp/arbitrary" pre-ack verify; refresh_manifest "$tmp/arbitrary"; expect_reject "$tmp/arbitrary"
cp -R "$tmp/valid" "$tmp/fractional"; jq '.ledger[0].occurrences=0.5 | .final_lookup[0].occurrences=0.5' "$tmp/fractional/raw/pre-ack/verify.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/fractional/raw/pre-ack/verify.stdout.json"; rehash_capture "$tmp/fractional" pre-ack verify; refresh_manifest "$tmp/fractional"; expect_reject "$tmp/fractional"
cp -R "$tmp/valid" "$tmp/absent-one"; jq '.ledger[0].final="absent" | .ledger[0].classification="absent-within-declared-rpo"' "$tmp/absent-one/raw/pre-ack/verify.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/absent-one/raw/pre-ack/verify.stdout.json"; rehash_capture "$tmp/absent-one" pre-ack verify; refresh_manifest "$tmp/absent-one"; expect_reject "$tmp/absent-one"
cp -R "$tmp/valid" "$tmp/hook-replaced"; jq '.hook_sha256=("f"*64)' "$tmp/hook-replaced/raw/post-ack/cut-at-barrier.meta.json" > "$tmp/x"; mv "$tmp/x" "$tmp/hook-replaced/raw/post-ack/cut-at-barrier.meta.json"; refresh_manifest "$tmp/hook-replaced"; expect_reject "$tmp/hook-replaced"
cp -R "$tmp/valid" "$tmp/hook-snapshot-tampered"; chmod 700 "$tmp/hook-snapshot-tampered/hooks/cut-at-barrier"; printf '# tampered\n' >> "$tmp/hook-snapshot-tampered/hooks/cut-at-barrier"; refresh_manifest "$tmp/hook-snapshot-tampered"; expect_reject "$tmp/hook-snapshot-tampered"
cp -R "$tmp/valid" "$tmp/legacy-split"; : > "$tmp/legacy-split/raw/pre-ack/cutpoint.stdout.json"; refresh_manifest "$tmp/legacy-split"; expect_reject "$tmp/legacy-split"
cp -R "$tmp/valid" "$tmp/reboot-before-power"; jq '.boot_transition_at="2026-08-10T00:00:04Z"' "$tmp/reboot-before-power/raw/post-ack/reboot.stdout.json" > "$tmp/x"; mv "$tmp/x" "$tmp/reboot-before-power/raw/post-ack/reboot.stdout.json"; rehash_capture "$tmp/reboot-before-power" post-ack reboot; refresh_manifest "$tmp/reboot-before-power"; expect_reject "$tmp/reboot-before-power"
cp -R "$tmp/valid" "$tmp/stage-overlap"; jq '.started_at="2026-08-10T00:00:05Z"' "$tmp/stage-overlap/raw/pre-ack/reboot.meta.json" > "$tmp/x"; mv "$tmp/x" "$tmp/stage-overlap/raw/pre-ack/reboot.meta.json"; refresh_manifest "$tmp/stage-overlap"; expect_reject "$tmp/stage-overlap"
cp -R "$tmp/valid" "$tmp/nested-hidden"; mkdir -p "$tmp/nested-hidden/raw/pre-ack/nested"; : > "$tmp/nested-hidden/raw/pre-ack/nested/manifest.json"; expect_reject "$tmp/nested-hidden"
printf 'power-loss durability static checks passed\n'
