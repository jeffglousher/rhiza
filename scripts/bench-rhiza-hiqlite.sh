#!/usr/bin/env bash
# Generate, run, and normalize the Rhiza/Hiqlite comparison program.
# This script deliberately delegates cluster work to the established runners.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
expected_cells='["f1-h60","f1-h180","f1-h300","f2-h60","f2-h180","f2-h300","f3-h60","f3-h180","f3-h300"]'
isolation_schema='{"mode":"fresh-vcluster","process_generation_new":true,"storage_generation_new":true,"restore_env_absent":true,"prior_sentinel_absent":true,"exact_membership":true,"object_provenance_current":true,"cleanup_verified":true}'
active_exact_cluster=""

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
verify_image_proof_uid_generations() {
  local proof_json="$1" failed="$2" auto="$3" pre post clear
  pre="$(jq -r '.[] | select(.stage == "pre-fault") | .path' <<< "$proof_json")"
  if [ "$auto" = true ]; then
    post="$(jq -r '.[] | select(.stage == "post-recovery") | .path' <<< "$proof_json")"
    jq -es --argjson failed "$failed" '
      length == 2 and
      ([.[0].voters[].name] | sort) == ([.[1].voters[].name] | sort) and
      ([.[0].voters[].uid] | unique | length) == 3 and ([.[1].voters[].uid] | unique | length) == 3 and
      .[0].proxy[0].uid == .[1].proxy[0].uid and
      ([.[0].voters[] as $before | .[1].voters[] | select(.name == $before.name) |
        {name,changed:(.uid != $before.uid)}] |
        all(.[]; if (.name | test("-([0-9]+)$")) then
          (.name | capture("-(?<ordinal>[0-9]+)$").ordinal | tonumber) as $ordinal |
          .changed == ($ordinal >= (3 - $failed))
        else false end))
    ' "$pre" "$post" >/dev/null || die "invalid auto-recovery voter UID generation proof"
  else
    post="$(jq -r '.[] | select(.stage == "post-operator-dr") | .path' <<< "$proof_json")"
    clear="$(jq -r '.[] | select(.stage == "post-restore-clear") | .path' <<< "$proof_json")"
    jq -es '
      length == 3 and
      .[0].proxy[0].uid == .[1].proxy[0].uid and .[1].proxy[0].uid == .[2].proxy[0].uid and
      (([.[0].voters[].uid] | unique) as $pre | ([.[1].voters[].uid] | unique) as $operator |
        ([.[2].voters[].uid] | unique) as $clear |
        ($pre|length) == 3 and ($operator|length) == 3 and ($clear|length) == 3 and
        (($pre - $operator)|length) == 3 and (($operator - $clear)|length) == 3)
    ' "$pre" "$post" "$clear" >/dev/null || die "invalid operator-DR voter UID generation proof"
  fi
}
snapshot_image_proofs() {
  local phase_json="$1" failed="$2" cell_root="$3" stages proof stage path digest snapshot actual auto cell expected_config manifest_path manifest_sha
  stages="$(jq -cer --argjson failed "$failed" '
    .expected_vs_observed.observed as $observed |
    if (($observed.auto_recovery|type) != "boolean" or ($observed.operator_dr|type) != "boolean") then
      error("recovery flags must be booleans")
    elif $failed == 1 and $observed == ($observed + {auto_recovery:true,operator_dr:false}) then
      ["pre-fault","post-recovery"]
    elif $failed == 2 and $observed == ($observed + {auto_recovery:true,operator_dr:false}) then
      ["pre-fault","post-recovery"]
    elif ($failed == 2 or $failed == 3) and $observed == ($observed + {auto_recovery:false,operator_dr:true}) then
      ["pre-fault","post-operator-dr","post-restore-clear"]
    else error("invalid recovery branch") end
  ' <<< "$phase_json")" || die "invalid $failed observed recovery path"
  auto="$(jq -er '.expected_vs_observed.observed.auto_recovery' <<< "$phase_json")"
  cell="$(jq -er '.cell_id | select(type == "string" and length > 0)' <<< "$phase_json")"
  jq -e --argjson expected "$stages" '.cell_isolation.expected_image_proof_stages == $expected' <<< "$phase_json" >/dev/null \
    || die "invalid $failed declared image proof stages"
  proof="$(jq -c '.cell_isolation.image_proofs' <<< "$phase_json")"
  manifest_path="$(jq -er '.cell_isolation.image_provenance_manifest.path' <<< "$phase_json")"
  manifest_sha="$(jq -er '.cell_isolation.image_provenance_manifest.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$phase_json")"
  [ -f "$manifest_path" ] && [ "$(sha256_file "$manifest_path")" = "$manifest_sha" ] || die "image proof manifest source mismatch"
  expected_config="$(jq -c '.expected_config_ids' "$manifest_path")"
  jq -e --argjson expected "$stages" '
    type == "array" and ([.[].stage] | sort) == ($expected | sort) and
    ([.[].stage] | unique | length) == length and ([.[].path] | unique | length) == length and
    all(.[]; (.stage|type)=="string" and (.path|type)=="string" and (.path|length)>0 and
      (.sha256|type)=="string" and (.sha256|test("^[0-9a-f]{64}$")))' <<< "$proof" >/dev/null \
    || die "invalid $failed image proof stage set"
  verify_image_proof_uid_generations "$proof" "$failed" "$auto"
  mkdir -p "$cell_root/hiqlite/image-proofs"
  chmod 700 "$cell_root/hiqlite/image-proofs"
  while IFS=$'\t' read -r stage path digest; do
    [ -f "$path" ] || die "missing image proof: $path"
    actual="$(sha256_file "$path")"; [ "$actual" = "$digest" ] || die "image proof source hash mismatch: $path"
    jq -e --arg cell "$cell" --arg stage "$stage" --argjson expected "$expected_config" ' .valid == true and .image_provenance_verified == true and
      .cell_id == $cell and .stage == $stage and
      .expected_config_ids == $expected and .expected_cri_ids == $expected and
      (.voters|type)=="array" and (.voters|length)==3 and
      ([.voters[].name] | sort) == ["hiqlite-recovery-0","hiqlite-recovery-1","hiqlite-recovery-2"] and
      ([.voters[].uid] | unique | length)==3 and
      (.proxy|type)=="array" and (.proxy|length)==1 and
      (.proxy[0].uid|type)=="string" and (.proxy[0].uid|length)>0 and
      (.expected_cri_ids.voter|type)=="string" and (.expected_cri_ids.voter|test("^sha256:[0-9a-f]{64}$")) and
      (.expected_cri_ids.proxy|type)=="string" and (.expected_cri_ids.proxy|test("^sha256:[0-9a-f]{64}$")) and
      .expected_cri_ids as $expected and
      (.voters | all(.[]; .image_id == $expected.voter)) and
      (.proxy | all(.[]; .image_id == $expected.proxy))' "$path" >/dev/null \
      || die "invalid image proof payload: $path"
    snapshot="$cell_root/hiqlite/image-proofs/${stage}.json"
    cp "$path" "$snapshot"
    chmod 600 "$snapshot"
    actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "image proof snapshot mismatch: $snapshot"
    jq -cn --arg stage "$stage" --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" \
      '{stage:$stage,path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
  done < <(jq -r '.[] | [.stage,.path,.sha256] | @tsv' <<< "$proof") | jq -s .
}
snapshot_image_provenance_manifest() {
  local phase_json="$1" cell_root="$2" manifest path manifest_path digest name actual cell
  cell="$(jq -er '.cell_id | select(type == "string" and length > 0)' <<< "$phase_json")"
  manifest="$(jq -c '.cell_isolation.image_provenance_manifest' <<< "$phase_json")"
  manifest_path="$(jq -er '.path' <<< "$manifest")"; digest="$(jq -er '.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$manifest")"
  [ -f "$manifest_path" ] && [ "$(sha256_file "$manifest_path")" = "$digest" ] || die "image provenance manifest source mismatch"
  jq -e --arg cell "$cell" --argjson proofs "$(jq -c '.cell_isolation.image_proofs' <<< "$phase_json")" '
    .valid == true and .cell_id == $cell and
    (.canonical_tags.voter|type)=="string" and (.canonical_tags.proxy|type)=="string" and
    .canonical_tags.voter != .canonical_tags.proxy and
    (.expected_config_ids.voter|test("^sha256:[0-9a-f]{64}$")) and
    (.expected_config_ids.proxy|test("^sha256:[0-9a-f]{64}$")) and
    .references.stage_proofs == $proofs and
    ([.references.local_image_config,.references.node_cri,.references.live_image] |
      all(.[]; (.path|type)=="string" and (.sha256|test("^[0-9a-f]{64}$"))))
  ' "$manifest_path" >/dev/null || die "invalid image provenance manifest"
  jq -es --argjson manifest "$(<"$manifest_path")" '
    length == 3 and $manifest.expected_config_ids as $expected and
    .[0].valid == true and
    .[0].voter.pre_load_config_sha256 == $expected.voter and .[0].voter.post_load_config_sha256 == $expected.voter and
    .[0].proxy.pre_load_config_sha256 == $expected.proxy and .[0].proxy.post_load_config_sha256 == $expected.proxy and
    .[1].valid == true and .[1].voter.cri_image_id_candidates == [$expected.voter] and
    .[1].proxy.cri_image_id_candidates == [$expected.proxy] and
    .[2].valid == true and .[2].expected_node_cri.voter_image_ids == [$expected.voter] and
    .[2].expected_node_cri.proxy_image_ids == [$expected.proxy] and
    ([.[2].voters[].image_id] | unique) == [$expected.voter] and
    ([.[2].proxy[].image_id] | unique) == [$expected.proxy]
  ' "$(jq -r '.references.local_image_config.path' "$manifest_path")" \
    "$(jq -r '.references.node_cri.path' "$manifest_path")" \
    "$(jq -r '.references.live_image.path' "$manifest_path")" >/dev/null \
    || die "invalid image provenance evidence semantics"
  mkdir -p "$cell_root/hiqlite/image-provenance"; chmod 700 "$cell_root/hiqlite/image-provenance"
  while IFS=$'\t' read -r name path digest; do
    [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "image provenance reference mismatch: $name"
    cp "$path" "$cell_root/hiqlite/image-provenance/${name}.json"
    chmod 600 "$cell_root/hiqlite/image-provenance/${name}.json"
    actual="$(sha256_file "$cell_root/hiqlite/image-provenance/${name}.json")"
    [ "$actual" = "$digest" ] || die "image provenance snapshot mismatch: $name"
  done < <(jq -r '.references | to_entries[] | select(.key != "stage_proofs") | [.key,.value.path,.value.sha256] | @tsv' "$manifest_path")
  cp "$manifest_path" "$cell_root/hiqlite/image-provenance/manifest.json"; chmod 600 "$cell_root/hiqlite/image-provenance/manifest.json"
  [ "$(sha256_file "$cell_root/hiqlite/image-provenance/manifest.json")" = "$digest" ] || die "manifest snapshot mismatch"
  jq -cn --arg path "$manifest_path" --arg sha256 "$digest" --arg snapshot "$cell_root/hiqlite/image-provenance/manifest.json" \
    '{path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
}
snapshot_transition_ledger() {
  local phase_json="$1" cell_root="$2" path digest count actual snapshot phase
  phase="$(jq -er '.phase | select(type == "string" and test("^f[123]$"))' <<< "$phase_json")"
  path="$(jq -er '.cell_isolation.transition_ledger.path | select(type == "string" and length > 0)' <<< "$phase_json")"
  digest="$(jq -er '.cell_isolation.transition_ledger.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$phase_json")"
  count="$(jq -er '.cell_isolation.transition_ledger.count | select(type == "number" and . >= 0 and floor == .)' <<< "$phase_json")"
  [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "transition ledger source mismatch"
  jq -es --argjson count "$count" --arg phase "$phase" '
    length == $count and
    all(.[]; (.id|type)=="string" and (.id|test("^" + $phase + "-transition-")) and
      (.value|type)=="string" and (.value|test("^transition-ack-")) and .acknowledged == true) and
    ([.[].id] | unique | length) == length and ([.[].value] | unique | length) == length
  ' "$path" >/dev/null || die "invalid transition ledger rows"
  mkdir -p "$cell_root/hiqlite/transition-ledger"; chmod 700 "$cell_root/hiqlite/transition-ledger"
  snapshot="$cell_root/hiqlite/transition-ledger/ledger.jsonl"
  cp "$path" "$snapshot"; chmod 600 "$snapshot"
  actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "transition ledger snapshot mismatch"
  jq -cn --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" --argjson count "$count" \
    '{path:$path,sha256:$sha256,count:$count,snapshot:{path:$snapshot,sha256:$sha256}}'
}
snapshot_baseline_proof() {
  local phase_json="$1" cell_root="$2" cell path digest snapshot actual
  cell="$(jq -er '.cell_id | select(type == "string" and length > 0)' <<< "$phase_json")"
  path="$(jq -er '.cell_isolation.baseline_proof.path' <<< "$phase_json")"
  digest="$(jq -er '.cell_isolation.baseline_proof.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$phase_json")"
  [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "baseline proof source mismatch"
  jq -e --arg cell "$cell" '
    .valid == true and .cell_id == $cell and .reset.rc == 0 and .reset.acknowledged == true and
    (.pre.records|type) == "array" and (.pre.records|length) == 3 and
    ([.pre.records[].ordinal] | sort) == [0,1,2] and
    (.pre.records | all(.[]; (.rc|type)=="number" and .rc != 0 and .classification == "absent-table")) and
    (.post.records|type) == "array" and (.post.records|length) == 3 and
    ([.post.records[].ordinal] | sort) == [0,1,2] and
    (.post.records | all(.[]; (.attempts|type)=="number" and .attempts > 0 and
      (.rc|type)=="number" and .rc == 0 and .classification == "empty" and
      (.raw|type)=="string" and (try (.raw | fromjson | .found == false) catch false)))
  ' "$path" >/dev/null || die "invalid baseline proof semantics"
  mkdir -p "$cell_root/hiqlite/baseline-proof"; chmod 700 "$cell_root/hiqlite/baseline-proof"
  snapshot="$cell_root/hiqlite/baseline-proof/proof.json"
  cp "$path" "$snapshot"; chmod 600 "$snapshot"
  actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "baseline proof snapshot mismatch"
  jq -cn --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" \
    '{path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
}
snapshot_failure_establishment_proof() {
  local phase_json="$1" cell_root="$2" cell path digest snapshot actual outcome id value
  local post_descriptor post_path post_digest post_classification post_snapshot post_actual write_ended
  cell="$(jq -er '.cell_id' <<< "$phase_json")"
  path="$(jq -er '.cell_isolation.failure_establishment_proof.path' <<< "$phase_json")"
  digest="$(jq -er '.cell_isolation.failure_establishment_proof.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$phase_json")"
  [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "failure proof source mismatch"
  jq -e --arg cell "$cell" '
    .valid == true and .proven == true and .cell_id == $cell and
    (.outcome == "application-no-quorum-rejection" or .outcome == "no_ack_unknown" or
     (.outcome|test("^write-ack-violation-(unilateral_state_machine_apply|ack_without_local_apply|ack_post_state_unknown)$"))) and
    [.sequence[].kind] == ["pods","endpoints","metrics","consistent"] and
    ([.sequence[].order] == [range(1;5)]) and
    (.sequence | all(.[]; (.rc|type)=="number" and (.raw|type)=="string" and (.started_epoch|type)=="number" and (.ended_epoch|type)=="number" and .ended_epoch >= .started_epoch)) and
    (.sequence[0] as $pods | .sequence[1] as $endpoints | .sequence[2] as $metrics | .sequence[3] as $consistent |
    ($pods.rc == 0 and $endpoints.rc == 0 and $metrics.rc == 0 and $consistent.rc != 0) and
    (try ($pods.raw|fromjson) catch null) as $pods_json | (try ($endpoints.raw|fromjson) catch null) as $endpoints_json | (try ($metrics.raw|fromjson) catch null) as $metrics_json |
    ($pods_json.items|length) == 1 and $pods_json.items[0].metadata.name == "hiqlite-recovery-0" and $pods_json.items[0].status.phase == "Running" and
    ($pods_json.items[0].status.conditions | any(.type == "Ready" and .status == "True")) and
    ([ $endpoints_json.items[]?.endpoints[]? ]|length) == 1 and
    ($endpoints_json.items[0].endpoints[0].targetRef.uid == $pods_json.items[0].metadata.uid and $endpoints_json.items[0].endpoints[0].targetRef.name == "hiqlite-recovery-0" and $endpoints_json.items[0].endpoints[0].conditions.ready == true) and
    ($metrics_json.running == true and $metrics_json.voter_ids == [1,2,3] and $metrics_json.node_ids == [1,2,3]) and
    ($consistent.raw|test("QuorumNotEnough") and test("got: \\{1\\}")) and
    (.proof_end_epoch|type)=="number" and (.write.started_epoch|type)=="number" and (.write.started_epoch - .proof_end_epoch) >= 0 and (.write.started_epoch - .proof_end_epoch) <= 2 and
    .proxy_ping.rc == 0 and (.proxy_ping.raw|type)=="string" and
    (.write.id|type)=="string" and (.write.value|type)=="string" and (.write.raw|type)=="string" and
    (.write.started_epoch|type)=="number" and (.write.ended_epoch|type)=="number" and .write.ended_epoch >= .write.started_epoch and
    (if .outcome == "application-no-quorum-rejection" then .write.rc != 0 and (.write.raw|test("QuorumNotEnough|no.quorum|got: \\{1\\}"))
     elif .outcome == "no_ack_unknown" then .write.rc != 0
     else .write.rc == 0 and (.write.id as $id | .write.value as $value |
       try (.write.raw|fromjson | . as $ack | $ack.acknowledged == true and $ack.id == $id and $ack.value == $value) catch false) end) and
    (if (.outcome|startswith("write-ack-violation-")) then
       (.post_ack.path|type)=="string" and (.post_ack.sha256|test("^[0-9a-f]{64}$")) and
       (.post_ack.classification == (.outcome | sub("^write-ack-violation-"; "")))
     else (.post_ack? == null) end)
  ' "$path" >/dev/null || die "invalid failure establishment proof"
  mkdir -p "$cell_root/hiqlite/failure-establishment"; chmod 700 "$cell_root/hiqlite/failure-establishment"
  snapshot="$cell_root/hiqlite/failure-establishment/proof.json"; cp "$path" "$snapshot"; chmod 600 "$snapshot"
  actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "failure proof snapshot mismatch"
  outcome="$(jq -er '.outcome' "$path")"
  if [[ "$outcome" != write-ack-violation-* ]]; then
    jq -e '.cell_isolation.failure_establishment_post_ack.path == null and .cell_isolation.failure_establishment_post_ack.sha256 == null and .cell_isolation.failure_establishment_post_ack.classification == null' <<< "$phase_json" >/dev/null || die "unexpected post-ACK evidence for $outcome"
    jq -cn --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" '{path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
    return 0
  fi
  id="$(jq -er '.write.id' "$path")"; value="$(jq -er '.write.value' "$path")"; write_ended="$(jq -er '.write.ended_epoch' "$path")"
  post_descriptor="$(jq -c '.cell_isolation.failure_establishment_post_ack' <<< "$phase_json")"
  post_path="$(jq -er '.path' <<< "$post_descriptor")"
  post_digest="$(jq -er '.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$post_descriptor")"
  post_classification="$(jq -er '.classification' <<< "$post_descriptor")"
  jq -e --arg path "$(jq -r '.post_ack.path' "$path")" --arg sha256 "$(jq -r '.post_ack.sha256' "$path")" --arg classification "$(jq -r '.post_ack.classification' "$path")" '
    .path == $path and .sha256 == $sha256 and .classification == $classification
  ' <<< "$post_descriptor" >/dev/null || die "post-ACK descriptor does not bind failure proof"
  [ -f "$post_path" ] && [ "$(sha256_file "$post_path")" = "$post_digest" ] || die "post-ACK evidence source mismatch"
  jq -e --arg cell "$cell" --arg id "$id" --arg value "$value" --arg classification "$post_classification" --argjson write_ended "$write_ended" '
    .valid == true and .cell_id == $cell and .id == $id and .value == $value and
    (.order|type)=="number" and .order >= 1 and (.captured_epoch|type)=="number" and .captured_epoch >= $write_ended and
    .classification == $classification and
    [.sequence[].kind] == ["pods","endpoints","direct_metrics","direct_consistent","direct_local","proxy_ping","voter0_current_logs","proxy_current_logs","proxy_previous_logs","proxy_describe"] and
    ([.sequence[].order] == [range(1;11)]) and
    (.sequence | all(.[]; (.rc|type)=="number" and (.raw|type)=="string" and (.started_epoch|type)=="number" and (.ended_epoch|type)=="number" and .ended_epoch >= .started_epoch and .started_epoch >= $write_ended)) and
    (.sequence[0] as $pods | .sequence[1] as $endpoints | .sequence[2] as $metrics | .sequence[4] as $local |
    ($pods.rc == 0 and $endpoints.rc == 0 and $metrics.rc == 0) and
    (try ($pods.raw|fromjson) catch null) as $pods_json | (try ($endpoints.raw|fromjson) catch null) as $endpoints_json | (try ($metrics.raw|fromjson) catch null) as $metrics_json |
    ($pods_json.items|length) == 1 and $pods_json.items[0].metadata.name == "hiqlite-recovery-0" and $pods_json.items[0].status.phase == "Running" and
    ($pods_json.items[0].status.conditions | any(.type == "Ready" and .status == "True")) and
    ([ $endpoints_json.items[]?.endpoints[]? ]|length) == 1 and
    ($endpoints_json.items[0].endpoints[0].targetRef.uid == $pods_json.items[0].metadata.uid and $endpoints_json.items[0].endpoints[0].targetRef.name == "hiqlite-recovery-0" and $endpoints_json.items[0].endpoints[0].conditions.ready == true) and
    ($metrics_json.running == true and $metrics_json.voter_ids == [1,2,3] and $metrics_json.node_ids == [1,2,3]) and
    (if $classification == "unilateral_state_machine_apply" then
       $local.rc == 0 and (try ($local.raw|fromjson | . as $query | $query.found == true and $query.id == $id and $query.value == $value) catch false)
     elif $classification == "ack_without_local_apply" then
       $local.rc == 0 and (try ($local.raw|fromjson|.found == false) catch false)
     else
       (($local.rc == 0 and (try ($local.raw|fromjson | . as $query | $query.found == true and $query.id == $id and $query.value == $value) catch false)) | not) and
       (($local.rc == 0 and (try ($local.raw|fromjson|.found == false) catch false)) | not)
     end))
  ' "$post_path" >/dev/null || die "invalid post-ACK direct-local evidence"
  mkdir -p "$cell_root/hiqlite/failure-establishment/post-ack"; chmod 700 "$cell_root/hiqlite/failure-establishment/post-ack"
  post_snapshot="$cell_root/hiqlite/failure-establishment/post-ack/$(basename "$post_path")"
  cp "$post_path" "$post_snapshot"; chmod 600 "$post_snapshot"
  post_actual="$(sha256_file "$post_snapshot")"; [ "$post_actual" = "$post_digest" ] || die "post-ACK evidence snapshot mismatch"
  jq -cn --arg failure_path "$path" --arg failure_sha256 "$digest" --arg failure_snapshot "$snapshot" \
    --arg post_path "$post_path" --arg post_sha256 "$post_digest" --arg post_snapshot "$post_snapshot" \
    '{failure_proof:{path:$failure_path,sha256:$failure_sha256,snapshot:{path:$failure_snapshot,sha256:$failure_sha256}},post_ack:{path:$post_path,sha256:$post_sha256,snapshot:{path:$post_snapshot,sha256:$post_sha256}}}' \
    > "$cell_root/hiqlite/failure-establishment/ack-violation-binding.json.tmp.$$"
  chmod 600 "$cell_root/hiqlite/failure-establishment/ack-violation-binding.json.tmp.$$"
  mv "$cell_root/hiqlite/failure-establishment/ack-violation-binding.json.tmp.$$" "$cell_root/hiqlite/failure-establishment/ack-violation-binding.json"
  # An ACK at the proven no-quorum boundary is a P0 violation.  Preserve the
  # independently hashed post-ACK evidence first, then never publish a phase.
  die "Hiqlite F2 write ACK violation ($post_classification); evidence snapshotted at $post_snapshot"
}
snapshot_failure_establishment_resolution() {
  local phase_json="$1" cell_root="$2" cell outcome descriptor path digest snapshot actual proof id value
  cell="$(jq -er '.cell_id' <<< "$phase_json")"
  outcome="$(jq -er '.cell_isolation.failure_establishment_proof.path' <<< "$phase_json" | xargs -I{} jq -r '.outcome' {})"
  descriptor="$(jq -c '.cell_isolation.failure_establishment_resolution' <<< "$phase_json")"
  if [ "$outcome" != no_ack_unknown ]; then
    jq -e '.path == null and .sha256 == null' <<< "$descriptor" >/dev/null || die "unexpected failure resolution for $outcome"
    printf '%s\n' 'null'; return 0
  fi
  path="$(jq -er '.path' <<< "$descriptor")"; digest="$(jq -er '.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$descriptor")"
  [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "failure resolution source mismatch"
  proof="$(jq -er '.cell_isolation.failure_establishment_proof.path' <<< "$phase_json")"
  id="$(jq -er '.write.id' "$proof")"; value="$(jq -er '.write.value' "$proof")"
  jq -e --arg cell "$cell" --arg id "$id" --arg value "$value" '
    .valid == true and .cell_id == $cell and .outcome == "no_ack_unknown" and .id == $id and .value == $value and
    (.mode == "auto-recovery" or .mode == "operator-dr") and (.raw|type)=="string" and
    (try (.raw|fromjson) catch null) as $result |
    if .mode == "operator-dr" then $result.found == false
    else ($result.found == false or ($result.found == true and $result.id == $id and $result.value == $value)) end
  ' "$path" >/dev/null || die "invalid failure resolution semantics"
  mkdir -p "$cell_root/hiqlite/failure-resolution"; chmod 700 "$cell_root/hiqlite/failure-resolution"
  snapshot="$cell_root/hiqlite/failure-resolution/resolution.json"; cp "$path" "$snapshot"; chmod 600 "$snapshot"
  actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "failure resolution snapshot mismatch"
  jq -cn --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" '{path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
}
snapshot_idempotent_recovery_write() {
  local phase_json="$1" cell_root="$2" cell path digest snapshot actual
  cell="$(jq -er '.cell_id | select(type == "string" and length > 0)' <<< "$phase_json")"
  path="$(jq -er '.cell_isolation.idempotent_recovery_write.path' <<< "$phase_json")"
  digest="$(jq -er '.cell_isolation.idempotent_recovery_write.sha256 | select(test("^[0-9a-f]{64}$"))' <<< "$phase_json")"
  [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "idempotent recovery write source mismatch"
  jq -e --arg cell "$cell" '
    .valid == true and .contract == "idempotent-final-state-single-key-not-exactly-once" and
    .cell_id == $cell and .stage == "post-restore-clear" and
    (.id|type)=="string" and (.id|test("^" + $cell + "-restore-idempotent-")) and
    (.value|type)=="string" and (.value|length)>0 and
    .id as $id | .value as $value |
    (.attempts|type)=="array" and (.attempts|length)>=1 and
    ([.attempts[].attempt] == [range(1; (.attempts|length)+1)]) and
    (.attempts[-1].classification == "acknowledged") and
    (.attempts | all(.[]; (.rc|type)=="number" and (.raw|type)=="string" and
      (.classification == "ambiguous-retryable" or .classification == "acknowledged"))) and
    (.attempts[:-1] | all(.[]; .classification == "ambiguous-retryable" and .rc != 0)) and
    (.attempts[-1] | .classification == "acknowledged" and .rc == 0 and
      (try (.raw | fromjson | .acknowledged == true and .id == $id and .value == $value) catch false)) and
    (.final|type)=="object" and .final.found == true and .final.id == $id and .final.value == $value and
    .final.rc == 0 and .final.single_logical_key == true and .final.single_key_basis == "PRIMARY KEY(id)" and
    (.final.raw|type)=="string" and
    (try (.final.raw | fromjson | .found == true and .id == $id and .value == $value) catch false)
  ' "$path" >/dev/null || die "invalid idempotent recovery write proof"
  mkdir -p "$cell_root/hiqlite/idempotent-recovery-write"; chmod 700 "$cell_root/hiqlite/idempotent-recovery-write"
  snapshot="$cell_root/hiqlite/idempotent-recovery-write/proof.json"
  cp "$path" "$snapshot"; chmod 600 "$snapshot"
  actual="$(sha256_file "$snapshot")"; [ "$actual" = "$digest" ] || die "idempotent recovery write snapshot mismatch"
  jq -cn --arg path "$path" --arg sha256 "$digest" --arg snapshot "$snapshot" \
    '{path:$path,sha256:$sha256,snapshot:{path:$snapshot,sha256:$sha256}}'
}
canonical_source_fingerprint() {
  { git rev-parse HEAD
    git diff --binary HEAD -- . ':(exclude)target/**'
    git submodule status --recursive
    git ls-files --others --exclude-standard -- ':!target/**' | LC_ALL=C sort | while IFS= read -r source_file; do
      printf '%s\t%s\n' "$source_file" "$(sha256_file "$source_file")"
    done; } | sha256_file /dev/stdin
}
cleanup_exact_vcluster() {
  local cluster="$1" listing attempt
  listing="$(vcluster list --driver docker --output json)" || return 1
  if jq -e --arg cluster "$cluster" '.. | objects | .name? | select(. == $cluster)' <<< "$listing" >/dev/null; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || return 1
  fi
  for ((attempt=0; attempt<30; attempt++)); do
    listing="$(vcluster list --driver docker --output json)" || return 1
    jq -e --arg cluster "$cluster" '.. | objects | .name? | select(. == $cluster)' <<< "$listing" >/dev/null || return 0
    sleep 1
  done
  return 1
}
run_adapter_checked() {
  local cluster="$1" accepted="$2" cell_root="$3" status=0 cleanup_status=0 pre post
  shift 3
  pre="$(canonical_source_fingerprint)"; printf '%s\n' "$pre" > "$cell_root/source-pre.sha256"
  [ "$pre" = "$accepted" ] || return 70
  active_exact_cluster="$cluster"
  "$@" >"$cell_root/adapter.stdout.log" 2>"$cell_root/adapter.stderr.log" || status=$?
  printf '%s\n' "$status" > "$cell_root/adapter.rc"
  cleanup_exact_vcluster "$cluster" || cleanup_status=$?
  printf '%s\n' "$cleanup_status" > "$cell_root/cleanup.rc"
  [ "$cleanup_status" -ne 0 ] || active_exact_cluster=""
  post="$(canonical_source_fingerprint)"; printf '%s\n' "$post" > "$cell_root/source-post.sha256"
  [ "$post" = "$accepted" ] || { [ "$status" -eq 0 ] && return 72; }
  if [ "$cleanup_status" -ne 0 ]; then
    printf 'adapter_rc=%s cleanup_rc=%s cluster=%s\n' "$status" "$cleanup_status" "$cluster" >&2
    [ "$status" -eq 0 ] && return 71
  fi
  return "$status"
}
cleanup_active_exact_cluster() {
  [ -z "$active_exact_cluster" ] || cleanup_exact_vcluster "$active_exact_cluster" >&2 || true
}
trap cleanup_active_exact_cluster EXIT
usage() {
  cat >&2 <<'EOF'
usage: scripts/bench-rhiza-hiqlite.sh COMMAND [arguments]

Commands:
  plan [OUTPUT]                         print the safe, machine-readable program plan
  run-recovery                          run the explicit 1,2,3 × 60,180,300 zero-PVC drills
  run-steady                            run D1 c=1,4,16 × three fresh alternating repetitions
  normalize-recovery RHIZA_JSONL HIQLITE_SUMMARY OUTPUT
                                        validate and join only measured recovery evidence

`plan` performs no cluster mutation. `run-recovery` and `run-steady` create
disposable clusters through the established deployment owners.
EOF
}

emit_plan() {
  jq -n --argjson cells "$expected_cells" '
    {
      schema_version: 1,
      title: "Rhiza / Hiqlite executable comparison program",
      reference_baseline:{hiqlite:{release:"0.14.0",
        commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
        openraft:"resolved from the generated Cargo.lock and recorded per trial",
        openraft_version_source:"generated-cargo-lock",
        source_build_required:true,log_sync_modes:["immediate","immediate_async","interval"]},
        rhiza:{identity:"exact tested commit plus dirty-state flag"}},
      safety: {default_command:"plan", cluster_mutation:false,
        recovery_runner:"explicit run-recovery only"},
      executable_coverage:{recovery:"implemented",comparable_d1_steady:"implemented",comparable_workload_resource:"pending",
        publishable_performance_comparison:false},
      recovery_execution:{command:"run-recovery",kind:"single_diagnostic_trial",
        publishable:false,required_publication_repetitions:3,
        note:"publication requires an order-rotated multi-trial program"},
      adoption_hard_gates:[
        "correctness ledger and final state validation pass",
        "matched durability, topology, client path, and workload contract",
        "three rotated repetitions with raw provenance",
        "recovery matrix passes before adopting an availability claim",
        "do not publish a D1 result until the steady runner completes every provenance and correctness gate"
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
          "mixed_90r10w","mixed_50r50w","mixed_10r90w","scan","traversal",
          "locks","counters","notifications"],
        external_baselines:["sqlite_single_node","redb_single_node","ladybugdb_standalone"],
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
  jq -es --argjson expected "$expected_cells" --argjson isolation "$isolation_schema" '
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
      (.rhiza_commit|type) == "string" and (.rhiza_commit|test("^[0-9a-f]{40}$")) and
      (.rhiza_dirty|type) == "boolean" and
      (.resolved_image|type) == "string" and (.resolved_image|length) > 0 and
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
    ($cells | map(.source_run_id) | unique | length) == 9 and
    ($cells | map(.source_artifact.path) | unique | length) == 9 and
    ($cells | map({run_id,profile,rhiza_commit,rhiza_dirty,resolved_image}) | unique | length) == 1 and
    ($cells | all(.[]; (.source_run_id|type) == "string" and (.source_run_id|length) > 0 and
      .cell_isolation == $isolation and (.adapter_cell_isolation|type) == "object" and
      (.source_artifact.path|type) == "string" and (.source_artifact.path|length) > 0 and
      (.source_artifact.sha256|type) == "string" and (.source_artifact.sha256|test("^[0-9a-f]{64}$")))) and
    ($summaries | length) == 1 and $summaries[0].status == "passed" and
    ($summaries[0].run_id|type) == "string" and ($summaries[0].run_id|length) > 0 and
    $summaries[0].profile == "sql" and
    ($summaries[0].rhiza_commit|type) == "string" and
    ($summaries[0].rhiza_dirty|type) == "boolean" and
    ($summaries[0].resolved_image|type) == "string" and
    ({run_id:$summaries[0].run_id,profile:$summaries[0].profile,
      rhiza_commit:$summaries[0].rhiza_commit,rhiza_dirty:$summaries[0].rhiza_dirty,
      resolved_image:$summaries[0].resolved_image} ==
      ($cells[0] | {run_id,profile,rhiza_commit,rhiza_dirty,resolved_image}))
  ' "$rhiza_jsonl" >/dev/null || die "invalid Rhiza recovery matrix: require fresh-vcluster proof for every exact coordinate"
  jq -e --argjson expected "$expected_cells" --argjson isolation "$isolation_schema" '
    def finite_nonnegative: type == "number" and isfinite and . >= 0;
    . as $summary |
    $summary.system == "hiqlite" and ($summary.run_id|type) == "string" and
    ($summary.run_id|length) > 0 and
    $summary.hiqlite_reference_commit == "c8316c53799c509990475ea8e2aa2ef8679e070e" and
    $summary.hiqlite_commit == $summary.hiqlite_reference_commit and
    $summary.hiqlite_reference_release == "0.14.0" and $summary.hiqlite_release == "0.14.0" and
    ($summary.openraft_version|type) == "string" and
    ($summary.openraft_version|length) > 0 and
    $summary.openraft_version_source == "generated-cargo-lock" and
    $summary.log_sync == "Immediate" and
    $summary.image_source == "exact-source-build-with-verified-local-reuse" and
    $summary.source_commit_basis == "exact-commit" and
    $summary.image_source_commit == $summary.hiqlite_reference_commit and
    $summary.cargo_lock_origin == "generated-once-then-verified-reuse" and
    ($summary.cargo_lock_sha256|type) == "string" and
    ($summary.cargo_lock_sha256|test("^[0-9a-f]{64}$")) and
    ($summary.resolved_image|type) == "string" and ($summary.resolved_image|length) > 0 and
    ($summary.resolved_proxy_image|type) == "string" and ($summary.resolved_proxy_image|length) > 0 and
    ($summary.resolved_proxy_image_id|type) == "string" and ($summary.resolved_proxy_image_id|length) > 0 and
    $summary.ingress.kind == "hiqlite-application-proxy" and
    ($summary.ingress.version|type) == "string" and ($summary.ingress.version|length) > 0 and
    $summary.ingress.image == $summary.resolved_proxy_image and
    ($summary.ingress.patch_sha256|type) == "string" and
    ($summary.ingress.patch_sha256|test("^[0-9a-f]{64}$")) and
    ($summary.upstream_proxy_incompatibility|type) == "string" and
    ($summary.upstream_proxy_incompatibility|length) > 0 and
    $summary.voters == 3 and $summary.storage == "emptyDir" and
    $summary.zero_pvc == true and ($summary.phases|type) == "array" and ($summary.phases|length) == 9 and
    $summary.failure_counts == [1,2,3] and $summary.hold_seconds == [60,180,300] and
    $summary.cell_isolation.all_cells_proven == true and
    ($summary.cell_isolation.vclusters|type) == "array" and ($summary.cell_isolation.vclusters|length) == 9 and
    ($summary.cell_isolation.vclusters|unique|length) == 9 and
    ($summary.cell_isolation.node_uids|type) == "array" and ($summary.cell_isolation.node_uids|length) == 9 and
    ($summary.cell_isolation.node_uids|unique|length) == 9 and
    ($summary.cell_isolation.namespaces|type) == "array" and ($summary.cell_isolation.namespaces|length) == 9 and
    ($summary.cell_isolation.namespaces|unique|length) == 9 and
    ([$summary.phases[] | .cell_id] | sort) == ($expected | sort) and
    ([$summary.phases[] | .cell_id] | unique | length) == 9 and
    ([$summary.phases[] | .source_run_id] | unique | length) == 9 and
    ([$summary.phases[] | .source_artifact.path] | unique | length) == 9 and
    ([$summary.phases[] | select(.image_source == "exact-source-build")] | length) == 1 and
    ([$summary.phases[] | select(.image_source == "verified-local-exact-source-reuse")] | length) == 8 and
    ([$summary.phases[] | select(.cargo_lock_origin == "generated-from-exact-source")] | length) == 1 and
    ([$summary.phases[] | select(.cargo_lock_origin == "reused-generated-from-exact-source")] | length) == 8 and
    ([$summary.phases[] | . as $phase | $phase.failure_count as $failed | $phase.hold_seconds as $hold |
      ($phase.failure_count|type) == "number" and ($failed >= 1 and $failed <= 3) and
      ($phase.hold_seconds | finite_nonnegative) and ($phase.failure_held_seconds | finite_nonnegative) and
      $phase.failure_held_seconds >= $phase.hold_seconds and ($phase.service_rto_seconds | finite_nonnegative) and
      ($phase.full_rto_seconds | finite_nonnegative) and ($phase.expected_vs_observed | type) == "object" and
      ($phase.expected_vs_observed.expected | type) == "object" and
      ($phase.expected_vs_observed.observed | type) == "object" and
      ($phase.expected_vs_observed.observed.auto_recovery|type) == "boolean" and
      ($phase.expected_vs_observed.observed.operator_dr|type) == "boolean" and
      (($phase.expected_vs_observed.observed.auto_recovery != $phase.expected_vs_observed.observed.operator_dr)) and
      (if $failed == 1 then
         $phase.expected_vs_observed.observed.auto_recovery == true and $phase.expected_vs_observed.observed.operator_dr == false
       elif $failed == 3 then
         $phase.expected_vs_observed.observed.auto_recovery == false and $phase.expected_vs_observed.observed.operator_dr == true
       else true end) and
      ($phase.source_run_id|type) == "string" and ($phase.source_run_id|length) > 0 and
      $phase.cell_isolation == $isolation and
      ($phase.adapter_cell_isolation|type) == "object" and
      ($phase.source_artifact.path|type) == "string" and ($phase.source_artifact.path|length) > 0 and
      ($phase.source_artifact.sha256|type) == "string" and ($phase.source_artifact.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.source_events.path|type) == "string" and ($phase.source_events.path|length) > 0 and
      ($phase.source_events.sha256|type) == "string" and ($phase.source_events.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.image_proofs|type) == "array" and
      ([ $phase.image_proofs[].stage ] | sort) ==
        (if $phase.expected_vs_observed.observed.auto_recovery then ["post-recovery","pre-fault"] else ["post-operator-dr","post-restore-clear","pre-fault"] end) and
      ([$phase.image_proofs[].path] | unique | length) == ($phase.image_proofs|length) and
      ([$phase.image_proofs[].snapshot.path] | unique | length) == ($phase.image_proofs|length) and
      ($phase.image_proofs | all(.[]; (.path|type)=="string" and (.sha256|test("^[0-9a-f]{64}$")) and
        (.snapshot.path|type)=="string" and .snapshot.sha256 == .sha256)) and
      ($phase.image_provenance_manifest.path|type)=="string" and ($phase.image_provenance_manifest.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.image_provenance_manifest.snapshot.path|type)=="string" and
      $phase.image_provenance_manifest.snapshot.sha256 == $phase.image_provenance_manifest.sha256 and
      ($phase.transition_ledger.path|type)=="string" and ($phase.transition_ledger.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.transition_ledger.count|type)=="number" and $phase.transition_ledger.count >= 0 and
      ($phase.transition_ledger.snapshot.path|type)=="string" and
      $phase.transition_ledger.snapshot.sha256 == $phase.transition_ledger.sha256 and
      ($phase.baseline_proof.path|type)=="string" and ($phase.baseline_proof.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.baseline_proof.snapshot.path|type)=="string" and
      $phase.baseline_proof.snapshot.sha256 == $phase.baseline_proof.sha256 and
      ($phase.idempotent_recovery_write.path|type)=="string" and ($phase.idempotent_recovery_write.sha256|test("^[0-9a-f]{64}$")) and
      ($phase.idempotent_recovery_write.snapshot.path|type)=="string" and
      $phase.idempotent_recovery_write.snapshot.sha256 == $phase.idempotent_recovery_write.sha256 and
      (if $failed == 2 then
         ($phase.failure_establishment_proof.path|type)=="string" and ($phase.failure_establishment_proof.sha256|test("^[0-9a-f]{64}$")) and
         ($phase.failure_establishment_proof.snapshot.path|type)=="string" and $phase.failure_establishment_proof.snapshot.sha256 == $phase.failure_establishment_proof.sha256 and
         ($phase.failure_establishment_proof.outcome == "application-no-quorum-rejection" or $phase.failure_establishment_proof.outcome == "no_ack_unknown") and
         (if $phase.failure_establishment_proof.outcome == "no_ack_unknown" then
            ($phase.failure_establishment_resolution.path|type)=="string" and ($phase.failure_establishment_resolution.sha256|test("^[0-9a-f]{64}$")) and
            ($phase.failure_establishment_resolution.snapshot.path|type)=="string" and $phase.failure_establishment_resolution.snapshot.sha256 == $phase.failure_establishment_resolution.sha256
          else $phase.failure_establishment_resolution == null end)
       else $phase.failure_establishment_proof == null and $phase.failure_establishment_resolution == null end) and
      $phase.hiqlite_commit == $summary.hiqlite_commit and
      $phase.hiqlite_reference_commit == $summary.hiqlite_reference_commit and
      $phase.hiqlite_release == $summary.hiqlite_release and
      $phase.hiqlite_reference_release == $summary.hiqlite_reference_release and
      ($phase.openraft_version|type) == "string" and
      ($phase.openraft_version|length) > 0 and
      $phase.openraft_version == $summary.openraft_version and
      $phase.openraft_version_source == $summary.openraft_version_source and
      $phase.log_sync == $summary.log_sync and
      ($phase.image_source == "exact-source-build" or $phase.image_source == "verified-local-exact-source-reuse") and
      $phase.source_commit_basis == $summary.source_commit_basis and
      $phase.image_source_commit == $summary.image_source_commit and
      ($phase.cargo_lock_origin == "generated-from-exact-source" or $phase.cargo_lock_origin == "reused-generated-from-exact-source") and
      $phase.cargo_lock_sha256 == $summary.cargo_lock_sha256 and
      $phase.resolved_image == $summary.resolved_image and
      $phase.resolved_proxy_image == $summary.resolved_proxy_image and
      $phase.resolved_proxy_image_id == $summary.resolved_proxy_image_id and
      $phase.ingress.kind == $summary.ingress.kind and
      $phase.ingress.version == $summary.ingress.version and
      $phase.ingress.image == $summary.ingress.image and
      $phase.ingress.patch_sha256 == $summary.ingress.patch_sha256 and
      $phase.upstream_proxy_incompatibility == $summary.upstream_proxy_incompatibility and
      $phase.phase == ("f\($failed)") and
      $phase.cell_id == ("f\($failed)-h\($hold)")] | all) and
    ([$summary.phases[].image_proofs[].path] | unique | length) ==
      ([$summary.phases[].image_proofs[].path] | length) and
    ([$summary.phases[].image_proofs[].snapshot.path] | unique | length) ==
      ([$summary.phases[].image_proofs[].snapshot.path] | length) and
    ([$summary.phases[] | .failure_establishment_proof.path?,.failure_establishment_proof.post_ack.path?,.failure_establishment_resolution.path?,.baseline_proof.path?,.idempotent_recovery_write.path?,.image_provenance_manifest.path?,.transition_ledger.path? | select(type == "string")] as $paths |
      ($paths | unique | length) == ($paths | length)) and
    ([$summary.phases[] | .failure_establishment_proof.snapshot.path?,.failure_establishment_proof.post_ack.snapshot.path?,.failure_establishment_resolution.snapshot.path?,.baseline_proof.snapshot.path?,.idempotent_recovery_write.snapshot.path?,.image_provenance_manifest.snapshot.path?,.transition_ledger.snapshot.path? | select(type == "string")] as $paths |
      ($paths | unique | length) == ($paths | length))
  ' "$hiqlite_summary" >/dev/null || die "invalid Hiqlite recovery summary: require three voters, emptyDir, zero PVC, and every exact matrix coordinate"
}
verify_referenced_sources() {
  local path digest actual source_run cell phase summary_path summary_digest events_path events_digest expected_phase auto failed
  while IFS=$'\t' read -r path digest source_run cell; do
    [ -f "$path" ] || die "missing referenced raw artifact: $path"
    actual="$(sha256_file "$path")"
    [ "$actual" = "$digest" ] || die "referenced raw artifact digest mismatch: $path"
    jq -es --arg run "$source_run" --arg cell "$cell" '([.[] | select(.record_type == "cell" and .run_id == $run and .cell_id == $cell)] | length) == 1 and ([.[] | select(.record_type == "summary" and .run_id == $run)] | length) == 1' "$path" >/dev/null || die "Rhiza raw semantic identity mismatch: $path"
  done < <(jq -r 'select(.record_type == "cell") | [.source_artifact.path,.source_artifact.sha256,.source_run_id,.cell_id] | @tsv' "$1")
  while IFS= read -r phase; do
    summary_path="$(jq -r '.source_artifact.path' <<< "$phase")"
    summary_digest="$(jq -r '.source_artifact.sha256' <<< "$phase")"
    events_path="$(jq -r '.source_events.path' <<< "$phase")"
    events_digest="$(jq -r '.source_events.sha256' <<< "$phase")"
    source_run="$(jq -r '.source_run_id' <<< "$phase")"
    cell="$(jq -r '.cell_id' <<< "$phase")"
    expected_phase="$(jq -c '. as $aggregate |
      .cell_isolation = $aggregate.adapter_cell_isolation |
      del(.source_run_id,.source_artifact,.source_events,.adapter_cell_isolation,.image_proofs,.image_provenance_manifest,.transition_ledger,.baseline_proof,.idempotent_recovery_write,.failure_establishment_proof,.failure_establishment_resolution)' <<< "$phase")"
    [ -f "$summary_path" ] || die "missing referenced Hiqlite summary: $summary_path"
    [ "$(sha256_file "$summary_path")" = "$summary_digest" ] \
      || die "referenced Hiqlite summary digest mismatch: $summary_path"
    jq -se --arg run "$source_run" --argjson expected "$expected_phase" '
      length == 1 and .[0].run_id == $run and .[0].phases == [$expected]
    ' "$summary_path" >/dev/null || die "Hiqlite raw summary phase mismatch: $summary_path"
    [ -f "$events_path" ] || die "missing referenced Hiqlite events: $events_path"
    [ "$(sha256_file "$events_path")" = "$events_digest" ] \
      || die "referenced Hiqlite events digest mismatch: $events_path"
    jq -es --arg run "$source_run" --arg cell "$cell" --argjson expected "$expected_phase" '
      ([.[] | select(.event == "run_started" and .run_id == $run)] | length) == 1 and
      ([.[] | select(.event == "phase_summary")] == [$expected]) and
      ([.[] | select(has("phase") and .event != "phase_summary") | .phase] | all(. == $cell))
    ' "$events_path" >/dev/null || die "Hiqlite raw event phase mismatch: $events_path"
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "image proof source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "image proof snapshot mismatch: $snapshot"
    done < <(jq -r '.image_proofs[] | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    auto="$(jq -r '.expected_vs_observed.observed.auto_recovery' <<< "$phase")"
    failed="$(jq -r '.failure_count' <<< "$phase")"
    verify_image_proof_uid_generations "$(jq -c '.image_proofs' <<< "$phase")" "$failed" "$auto"
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "image manifest source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "image manifest snapshot mismatch: $snapshot"
    done < <(jq -r '.image_provenance_manifest | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "transition ledger source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "transition ledger snapshot mismatch: $snapshot"
    done < <(jq -r '.transition_ledger | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "baseline proof source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "baseline proof snapshot mismatch: $snapshot"
    done < <(jq -r '.baseline_proof | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "idempotent recovery write source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "idempotent recovery write snapshot mismatch: $snapshot"
    done < <(jq -r '.idempotent_recovery_write | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "failure proof source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "failure proof snapshot mismatch: $snapshot"
    done < <(jq -r '.failure_establishment_proof | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
    while IFS=$'\t' read -r path digest snapshot snapshot_digest; do
      [ -f "$path" ] && [ "$(sha256_file "$path")" = "$digest" ] || die "failure resolution source mismatch: $path"
      [ -f "$snapshot" ] && [ "$(sha256_file "$snapshot")" = "$snapshot_digest" ] || die "failure resolution snapshot mismatch: $snapshot"
    done < <(jq -r '.failure_establishment_resolution | select(.path != null) | [.path,.sha256,.snapshot.path,.snapshot.sha256] | @tsv' <<< "$phase")
  done < <(jq -c '.phases[]' "$2")
}

normalize_recovery() {
  local rhiza_jsonl="$1" hiqlite_summary="$2" output="$3" output_dir output_path
  local rhiza_path hiqlite_path rhiza_sha256 hiqlite_sha256 temp_output rhiza_snapshot hiqlite_snapshot
  output_dir="$(path_parent "$output")" || mkdir -p "$(dirname "$output")"
  output_dir="$(path_parent "$output")" || die "cannot create output directory: $output"
  rhiza_path="$(resolved_path "$rhiza_jsonl")"
  hiqlite_path="$(resolved_path "$hiqlite_summary")"
  output_path="$(resolved_path "$output")"
  if [ "$output_path" = "$rhiza_path" ] || [ "$output_path" = "$hiqlite_path" ] || \
    { [ -e "$output" ] && { [ "$output" -ef "$rhiza_jsonl" ] || [ "$output" -ef "$hiqlite_summary" ]; }; }; then
    die "output must not resolve to either source artifact"
  fi
  rhiza_snapshot="$(mktemp "$output_dir/.rhiza.snapshot.XXXXXX")"
  hiqlite_snapshot="$(mktemp "$output_dir/.hiqlite.snapshot.XXXXXX")"
  trap 'rm -f "${temp_output:-}" "${rhiza_snapshot:-}" "${hiqlite_snapshot:-}"' RETURN
  cp "$rhiza_jsonl" "$rhiza_snapshot"
  cp "$hiqlite_summary" "$hiqlite_snapshot"
  jq -se 'length == 1 and (.[0] | type == "object")' "$hiqlite_snapshot" >/dev/null \
    || die "Hiqlite summary must contain exactly one JSON object"
  verify_referenced_sources "$rhiza_snapshot" "$hiqlite_snapshot"
  validate_cells "$rhiza_snapshot" "$hiqlite_snapshot"
  rhiza_sha256="$(sha256_file "$rhiza_snapshot")"
  hiqlite_sha256="$(sha256_file "$hiqlite_snapshot")"
  temp_output="$(mktemp "$output_dir/.${output##*/}.tmp.XXXXXX")"
  trap 'rm -f "${temp_output:-}" "${rhiza_snapshot:-}" "${hiqlite_snapshot:-}"' RETURN
  jq -n --arg rhiza_source "$rhiza_path" --arg hiqlite_source "$hiqlite_path" \
    --arg rhiza_sha256 "$rhiza_sha256" --arg hiqlite_sha256 "$hiqlite_sha256" \
    --rawfile rhiza_raw "$rhiza_snapshot" --slurpfile hiqlite "$hiqlite_snapshot" '
      ($rhiza_raw | split("\n") | map(select(length > 0) | fromjson) |
        map(select(.record_type == "cell")) | sort_by(.cell_id)) as $r |
      ($hiqlite[0].phases | sort_by(.cell_id)) as $h |
      {schema_version:1,kind:"rhiza_hiqlite_recovery_normalization",
       source_artifacts:{rhiza_jsonl:{path:$rhiza_source,sha256:$rhiza_sha256},
         hiqlite_summary:{path:$hiqlite_source,sha256:$hiqlite_sha256}},
       publication:{eligible:false,reason:"single diagnostic recovery trial; three order-rotated repetitions are required"},
       source_provenance:{
         rhiza_cells_common:([$r[] | {run_id,profile,rhiza_commit,rhiza_dirty,resolved_image} | tojson] | unique | map(fromjson)),
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
            raw:$rc.source_artifact,
            throughput:"not_measured",resource:"not_measured"},
          hiqlite:{service_rto_seconds:.service_rto_seconds,full_rto_seconds:.full_rto_seconds,
            raw:.source_artifact,events:.source_events,
            throughput:"not_measured",resource:"not_measured"}}]}
    ' > "$temp_output"
  mv -f "$temp_output" "$output_path"
  trap - RETURN
}

run_recovery() {
  local coordinator_id coordinator_dir cell failed hold rhiza_artifact hiqlite_artifact hiqlite_events
  local rhiza_image rhiza_image_id hiqlite_image_id hiqlite_voter_tag hiqlite_proxy_id hiqlite_proxy_image hiqlite_proxy_tag hiqlite_lock hiqlite_lock_path hiqlite_phase image_proof_snapshots image_manifest_snapshot transition_ledger_snapshot baseline_proof_snapshot failure_proof_snapshot failure_resolution_snapshot
  local rhiza_combined hiqlite_phases hiqlite_summary summary_record
  local cell_root rhiza_cluster hiqlite_cluster rhiza_forbidden_sentinel next_sentinel
  local hiqlite_source_dir accepted_source_fingerprint
  coordinator_id="$(date -u +%Y%m%d-%H%M%S)-$$"
  coordinator_dir="$repo_root/target/rhiza-hiqlite-recovery/$coordinator_id"
  rhiza_image="rhiza-recovery-${coordinator_id}"
  hiqlite_voter_tag="hiqlite-recovery:${coordinator_id}"
  hiqlite_proxy_tag="hiqlite-recovery-proxy:${coordinator_id}"
  rhiza_combined="$coordinator_dir/rhiza-recovery.jsonl"
  hiqlite_phases="$coordinator_dir/hiqlite-phases.jsonl"
  mkdir -p "$coordinator_dir/cells"
  : > "$rhiza_combined"
  : > "$hiqlite_phases"
  rhiza_forbidden_sentinel="coordinator-forbidden-${coordinator_id}"
  hiqlite_source_dir="${HIQLITE_SOURCE_DIR:-}"
  accepted_source_fingerprint="$(canonical_source_fingerprint)"
  jq -n --arg coordinator_id "$coordinator_id" --arg root "$coordinator_dir" \
    '{schema_version:1,kind:"rhiza_hiqlite_recovery_run_started",coordinator_id:$coordinator_id,
    matrix:{failures:[1,2,3],holds_seconds:[60,180,300]},
    runners:{rhiza:"scripts/e2e-vind-rustfs.sh",hiqlite:"scripts/e2e-hiqlite-recovery.sh"},
    artifact_root:$root}' >&2
  for failed in 1 2 3; do for hold in 60 180 300; do
    cell="f${failed}-h${hold}"
    cell_root="$coordinator_dir/cells/$cell"
    rhiza_cluster="rhiza-${coordinator_id}-${cell}"
    hiqlite_cluster="hiqlite-${coordinator_id}-${cell}"
    mkdir -p "$cell_root/rhiza" "$cell_root/hiqlite"
    if [ -z "${rhiza_image_id:-}" ]; then
      run_adapter_checked "$rhiza_cluster" "$accepted_source_fingerprint" "$cell_root/rhiza" env RHIZA_VIND_CLEANUP=1 RHIZA_VIND_SKIP_BUILD=0 RHIZA_VIND_DIRECT_CLUSTER=0 RHIZA_VIND_REUSE_EXISTING=0 RHIZA_EXECUTION_PROFILE=sql RHIZA_E2E_RECOVERY_MATRIX=1 RHIZA_E2E_RECOVERY_MATRIX_ONLY=1 RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 RHIZA_RECOVERY_FORBIDDEN_SENTINEL="$rhiza_forbidden_sentinel" RHIZA_RECOVERY_FAIL_PEERS="$failed" RHIZA_RECOVERY_HOLD_SECONDS="$hold" RHIZA_E2E_TARGET_DIR="$cell_root/rhiza" RHIZA_VIND_CLUSTER="$rhiza_cluster" RHIZA_IMAGE="$rhiza_image" "$repo_root/scripts/e2e-vind-rustfs.sh"
      rhiza_image_id="$(docker image inspect --format '{{.Id}}' "$rhiza_image")"
    else
      [ "$(docker image inspect --format '{{.Id}}' "$rhiza_image")" = "$rhiza_image_id" ] || die "Rhiza exact image reuse mismatch"
      run_adapter_checked "$rhiza_cluster" "$accepted_source_fingerprint" "$cell_root/rhiza" env RHIZA_VIND_CLEANUP=1 RHIZA_VIND_SKIP_BUILD=1 RHIZA_VIND_DIRECT_CLUSTER=0 RHIZA_VIND_REUSE_EXISTING=0 RHIZA_EXECUTION_PROFILE=sql RHIZA_E2E_RECOVERY_MATRIX=1 RHIZA_E2E_RECOVERY_MATRIX_ONLY=1 RHIZA_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 RHIZA_RECOVERY_FORBIDDEN_SENTINEL="$rhiza_forbidden_sentinel" RHIZA_RECOVERY_FAIL_PEERS="$failed" RHIZA_RECOVERY_HOLD_SECONDS="$hold" RHIZA_E2E_TARGET_DIR="$cell_root/rhiza" RHIZA_VIND_CLUSTER="$rhiza_cluster" RHIZA_IMAGE="$rhiza_image" "$repo_root/scripts/e2e-vind-rustfs.sh"
    fi
    cleanup_exact_vcluster "$rhiza_cluster" || die "$cell Rhiza cleanup proof failed"
    rhiza_artifact="$(find "$cell_root/rhiza" -type f -name recovery-matrix.jsonl -print)"; [ "$(printf '%s\n' "$rhiza_artifact" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] || die "$cell requires one Rhiza raw artifact"
    next_sentinel="$(jq -er '[. | select(.record_type == "cell")] | if length == 1 then .[0].cell_isolation.current_run_sentinel.key else empty end' "$rhiza_artifact")" || die "$cell lacks current-run sentinel proof"
    [ "$next_sentinel" != "$rhiza_forbidden_sentinel" ] || die "$cell reused forbidden sentinel"
    jq -es --arg cell "$cell" --arg coordinator "$coordinator_id" --arg raw "$rhiza_artifact" --arg raw_sha "$(sha256_file "$rhiza_artifact")" --argjson isolation "$isolation_schema" '
      [ .[] | select(.record_type == "cell") ] as $c | [ .[] | select(.record_type == "summary") ] as $s |
      ($c|length)==1 and ($s|length)==1 and $c[0].status=="passed" and $s[0].status=="passed" and $c[0].cell_id==$cell and
      ($c[0].cell_isolation // empty) as $proof |
      ($proof.mode == "fresh-vcluster" and $proof.process_generation_new == true and $proof.storage_generation_new == true and $proof.restore_env_absent == true and $proof.prior_sentinel_absent == true and $proof.exact_membership == true and $proof.object_provenance_current == true) |
      if . then $c[0] + {run_id:$coordinator,source_run_id:$c[0].run_id,source_artifact:{path:$raw,sha256:$raw_sha},adapter_cell_isolation:$proof,cell_isolation:$isolation} else error("missing Rhiza cell isolation proof") end' "$rhiza_artifact" >> "$rhiza_combined"
    rhiza_forbidden_sentinel="$next_sentinel"
    if [ -z "${hiqlite_image_id:-}" ]; then
      run_adapter_checked "$hiqlite_cluster" "$accepted_source_fingerprint" "$cell_root/hiqlite" env HIQLITE_RECOVERY_CLEANUP=1 HIQLITE_BUILD_IMAGE=1 HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 HIQLITE_RECOVERY_REUSE_EXISTING=0 HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES=0 HIQLITE_RECOVERY_DIRECT_CLUSTER=0 HIQLITE_RECOVERY_SKIP_IMAGE_LOAD=0 HIQLITE_RECOVERY_SKIP_CLIENT_BUILD=0 HIQLITE_RECOVERY_IMAGE="$hiqlite_voter_tag" HIQLITE_RECOVERY_FAIL_PEERS="$failed" HIQLITE_RECOVERY_HOLD_SECONDS="$hold" HIQLITE_RECOVERY_TARGET_DIR="$cell_root/hiqlite" HIQLITE_RECOVERY_CLUSTER="$hiqlite_cluster" HIQLITE_RECOVERY_PROXY_IMAGE="$hiqlite_proxy_tag" "$repo_root/scripts/e2e-hiqlite-recovery.sh"
      hiqlite_artifact="$(find "$cell_root/hiqlite" -type f -name summary.json -print)"; hiqlite_image_id="$(jq -er .resolved_image "$hiqlite_artifact")"; hiqlite_proxy_image="$(jq -er .resolved_proxy_image "$hiqlite_artifact")"; [ "$hiqlite_proxy_image" = "$hiqlite_proxy_tag" ] || die "Hiqlite runner resolved an unexpected proxy tag"; hiqlite_proxy_id="$(jq -er .resolved_proxy_image_id "$hiqlite_artifact")"; hiqlite_lock="$(jq -er .cargo_lock_sha256 "$hiqlite_artifact")"; [ -n "$hiqlite_source_dir" ] || hiqlite_source_dir="$(dirname "$hiqlite_artifact")/hiqlite-source"; hiqlite_lock_path="$hiqlite_source_dir/Cargo.lock"; [ -f "$hiqlite_lock_path" ] && [ "$(sha256_file "$hiqlite_lock_path")" = "$hiqlite_lock" ] || die "first Hiqlite Cargo.lock source proof mismatch"; [ "$(git -C "$hiqlite_source_dir" rev-parse --is-inside-work-tree 2>/dev/null)" = true ] && [ "$(git -C "$hiqlite_source_dir" rev-parse HEAD)" = c8316c53799c509990475ea8e2aa2ef8679e070e ] && [ -z "$(git -C "$hiqlite_source_dir" status --porcelain --untracked-files=all)" ] || die "first Hiqlite cell source proof is not clean pinned source"
    else
      [ "$(docker image inspect --format '{{.Id}}' "$hiqlite_voter_tag")" = "$hiqlite_image_id" ] || die "Hiqlite voter exact image reuse mismatch"
      [ "$(docker image inspect --format '{{.Id}}' "$hiqlite_proxy_image")" = "$hiqlite_proxy_id" ] || die "Hiqlite proxy exact image reuse mismatch"
      run_adapter_checked "$hiqlite_cluster" "$accepted_source_fingerprint" "$cell_root/hiqlite" env HIQLITE_RECOVERY_CLEANUP=1 HIQLITE_BUILD_IMAGE=1 HIQLITE_RECOVERY_REQUIRE_FRESH_VCLUSTER=1 HIQLITE_RECOVERY_REUSE_EXISTING=0 HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES=1 HIQLITE_RECOVERY_DIRECT_CLUSTER=0 HIQLITE_RECOVERY_SKIP_IMAGE_LOAD=0 HIQLITE_RECOVERY_SKIP_CLIENT_BUILD=0 HIQLITE_SOURCE_DIR="$hiqlite_source_dir" HIQLITE_RECOVERY_IMAGE="$hiqlite_voter_tag" HIQLITE_RECOVERY_FAIL_PEERS="$failed" HIQLITE_RECOVERY_HOLD_SECONDS="$hold" HIQLITE_RECOVERY_TARGET_DIR="$cell_root/hiqlite" HIQLITE_RECOVERY_CLUSTER="$hiqlite_cluster" HIQLITE_RECOVERY_PROXY_IMAGE="$hiqlite_proxy_tag" HIQLITE_RECOVERY_EXPECTED_LOCAL_IMAGE_ID="$hiqlite_image_id" HIQLITE_RECOVERY_EXPECTED_LOCAL_PROXY_IMAGE_ID="$hiqlite_proxy_id" HIQLITE_RECOVERY_EXPECTED_LOCKFILE_PATH="$hiqlite_lock_path" HIQLITE_RECOVERY_EXPECTED_LOCKFILE_SHA256="$hiqlite_lock" "$repo_root/scripts/e2e-hiqlite-recovery.sh"
      hiqlite_artifact="$(find "$cell_root/hiqlite" -type f -name summary.json -print)"
    fi
    cleanup_exact_vcluster "$hiqlite_cluster" || die "$cell Hiqlite cleanup proof failed"
    [ "$(find "$cell_root/hiqlite" -type f -name summary.json -print | wc -l | tr -d ' ')" = 1 ] \
      || die "$cell requires one Hiqlite summary artifact"
    hiqlite_events="$(find "$cell_root/hiqlite" -type f -name recovery.jsonl -print)"
    [ "$(printf '%s\n' "$hiqlite_events" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] \
      || die "$cell requires one Hiqlite raw event artifact"
    jq -es --arg run_id "$(jq -er .run_id "$hiqlite_artifact")" --arg cell "$cell" \
      --slurpfile summary "$hiqlite_artifact" '
      ([.[] | select(.event == "run_started" and .run_id == $run_id)] | length) == 1 and
      ([.[] | select(.event == "phase_summary" and .cell_id == $cell)] | length) == 1 and
      ([.[] | select(.event == "phase_summary" and .cell_id == $cell)][0] == $summary[0].phases[0]) and
      ([.[] | select(has("phase") and .event != "phase_summary") | .phase] | all(. == $cell))
    ' "$hiqlite_events" >/dev/null || die "$cell raw Hiqlite events do not match its summary/run"
    hiqlite_phase="$(jq -c '.phases[0]' "$hiqlite_artifact")"
    image_proof_snapshots="$(snapshot_image_proofs "$hiqlite_phase" "$failed" "$cell_root")"
    image_manifest_snapshot="$(snapshot_image_provenance_manifest "$hiqlite_phase" "$cell_root")"
    transition_ledger_snapshot="$(snapshot_transition_ledger "$hiqlite_phase" "$cell_root")"
    baseline_proof_snapshot="$(snapshot_baseline_proof "$hiqlite_phase" "$cell_root")"
    if [ "$failed" = 2 ]; then
      failure_proof_snapshot="$(snapshot_failure_establishment_proof "$hiqlite_phase" "$cell_root")"
      failure_resolution_snapshot="$(snapshot_failure_establishment_resolution "$hiqlite_phase" "$cell_root")"
    else
      failure_proof_snapshot=null
      failure_resolution_snapshot=null
    fi
    jq -e --arg cell "$cell" --arg raw "$hiqlite_artifact" --arg raw_sha "$(sha256_file "$hiqlite_artifact")" \
      --arg events "$hiqlite_events" --arg events_sha "$(sha256_file "$hiqlite_events")" \
      --argjson isolation "$isolation_schema" --argjson image_proofs "$image_proof_snapshots" --argjson image_manifest "$image_manifest_snapshot" --argjson transition_ledger "$transition_ledger_snapshot" --argjson baseline_proof "$baseline_proof_snapshot" --argjson failure_proof "$failure_proof_snapshot" --argjson failure_resolution "$failure_resolution_snapshot" '
      . as $summary | .phases as $p | ($p|length)==1 and $p[0].cell_id==$cell and
      ($p[0].cell_isolation.success == true and $p[0].cell_isolation.fresh_vcluster_created == true and
       ($p[0].cell_isolation.vcluster.node_uid|type) == "string" and ($p[0].cell_isolation.vcluster.node_uid|length) > 0 and
       ($p[0].cell_isolation.rustfs_uid|type) == "string" and ($p[0].cell_isolation.rustfs_uid|length) > 0 and
       ($p[0].cell_isolation.object_namespace_uid|type) == "string" and ($p[0].cell_isolation.object_namespace_uid|length) > 0 and
       ($p[0].cell_isolation.initial_inventory_sha256|test("^[0-9a-f]{64}$")) and
       $p[0].cell_isolation.image_provenance_verified == true and
       $p[0].cell_isolation.image_provenance_publishable == true and
       ($p[0].cell_isolation.live_image_ids_path|type) == "string" and ($p[0].cell_isolation.live_image_ids_path|length) > 0 and
       $p[0].cell_isolation.namespace_uid_proven == true and $p[0].cell_isolation.statefulset_uid_proven == true and $p[0].cell_isolation.voter_uids_proven == true and $p[0].cell_isolation.restore_env_absent == true and $p[0].cell_isolation.baseline_direct_reads == true and $p[0].cell_isolation.endpoint_target_uids_current == true and $p[0].cell_isolation.backup_key_unique == true) |
      if . then $p[0] + {source_run_id:$summary.run_id,
        source_artifact:{path:$raw,sha256:$raw_sha},
        source_events:{path:$events,sha256:$events_sha},
        adapter_cell_isolation:$p[0].cell_isolation,cell_isolation:$isolation,
        image_proofs:$image_proofs,image_provenance_manifest:$image_manifest,transition_ledger:$transition_ledger,baseline_proof:$baseline_proof,failure_establishment_proof:$failure_proof,failure_establishment_resolution:$failure_resolution}
      else error("missing Hiqlite cell isolation proof") end' "$hiqlite_artifact" >> "$hiqlite_phases"
  done; done
  summary_record="$(jq -s --arg coordinator "$coordinator_id" '.[0] | {record_type:"summary",run_id:$coordinator,profile,rhiza_commit,rhiza_dirty,resolved_image,status:"passed"}' "$rhiza_combined")"
  printf '%s\n' "$summary_record" >> "$rhiza_combined"
  hiqlite_summary="$coordinator_dir/hiqlite-summary.json"
  jq -s --arg coordinator "$coordinator_id" '
    . as $phases | ($phases[0]) as $first |
    ([ $phases[] | {system,hiqlite_reference_commit,hiqlite_commit,hiqlite_reference_release,hiqlite_release,
      openraft_version,openraft_version_source,log_sync,source_commit_basis,image_source_commit,
      cargo_lock_sha256,resolved_image,resolved_proxy_image,resolved_proxy_image_id,ingress,
      upstream_proxy_incompatibility} | tojson ] | unique | length) == 1 or error("mixed common provenance") |
    {system:$first.system,hiqlite_reference_commit:$first.hiqlite_reference_commit,
     hiqlite_commit:$first.hiqlite_commit,hiqlite_reference_release:$first.hiqlite_reference_release,
     hiqlite_release:$first.hiqlite_release,openraft_version:$first.openraft_version,
     openraft_version_source:$first.openraft_version_source,log_sync:$first.log_sync,
     image_source:"exact-source-build-with-verified-local-reuse",source_commit_basis:$first.source_commit_basis,
     image_source_commit:$first.image_source_commit,cargo_lock_origin:"generated-once-then-verified-reuse",
     cargo_lock_sha256:$first.cargo_lock_sha256,resolved_image:$first.resolved_image,
     resolved_proxy_image:$first.resolved_proxy_image,resolved_proxy_image_id:$first.resolved_proxy_image_id,
     ingress:$first.ingress,upstream_proxy_incompatibility:$first.upstream_proxy_incompatibility,
     voters:3,storage:"emptyDir",zero_pvc:true,run_id:$coordinator,
     failure_counts:([$phases[].failure_count]|unique|sort),hold_seconds:([$phases[].hold_seconds]|unique|sort),
     cell_isolation:{all_cells_proven:([$phases[].adapter_cell_isolation.success]|all),
       vclusters:([$phases[].adapter_cell_isolation.vcluster.name]|unique),
       node_uids:([$phases[].adapter_cell_isolation.vcluster.node_uid]|unique),
       namespaces:([$phases[].adapter_cell_isolation.namespace]|unique)},phases:$phases}' "$hiqlite_phases" > "$hiqlite_summary"
  canonical_source_fingerprint | grep -Fxq "$accepted_source_fingerprint" || die "canonical source fingerprint drifted before normalization"
  normalize_recovery "$rhiza_combined" "$hiqlite_summary" "$coordinator_dir/normalized.json"
  jq -n --arg coordinator_id "$coordinator_id" --arg fingerprint "$accepted_source_fingerprint" \
    --arg normalized "$coordinator_dir/normalized.json" '{schema_version:1,coordinator_id:$coordinator_id,accepted_source_fingerprint:$fingerprint,normalized_artifact:$normalized}' > "$coordinator_dir/completion.json"
  printf '%s\n' "$coordinator_dir/normalized.json"
}

run_steady() { "$repo_root/scripts/bench-rhiza-hiqlite-steady.sh"; }

case "${1:-}" in
  plan)
    [ "$#" -le 2 ] || { usage; exit 64; }
    if [ "$#" -eq 2 ]; then mkdir -p "$(dirname "$2")"; emit_plan > "$2"; else emit_plan; fi
    ;;
  run-recovery)
    [ "$#" -eq 1 ] || { usage; exit 64; }
    run_recovery
    ;;
  run-steady)
    [ "$#" -eq 1 ] || { usage; exit 64; }
    run_steady
    ;;
  normalize-recovery)
    [ "$#" -eq 4 ] || { usage; exit 64; }
    normalize_recovery "$2" "$3" "$4"
    ;;
  *) usage; exit 64 ;;
esac
