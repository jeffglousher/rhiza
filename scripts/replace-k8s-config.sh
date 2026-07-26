#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: $0 OLD_BUNDLE_JSON SUCCESSOR_DRAFT_JSON" >&2
  exit 64
}
if [ -n "${RHIZA_OBJECT_JOB_RESPONSE_FILE+x}" ] ||
  [ -n "${RHIZA_OBJECT_JOB_RENDER_ONLY+x}" ] ||
  [ -n "${RHIZA_ADMIN_JOB_RESPONSE_FILE+x}" ] ||
  [ -n "${RHIZA_ADMIN_JOB_RENDER_ONLY+x}" ] ||
  [ -n "${RHIZA_STATEFULSET_FIXTURE_DIR+x}" ]; then
  echo "test-only Job response/render hooks are forbidden during configuration replacement" >&2
  exit 65
fi
old_bundle="$1"
successor_draft="$2"
profile="${RHIZA_EXECUTION_PROFILE-}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
context="${RHIZA_KUBE_CONTEXT:-}"
work_dir="${RHIZA_RECONFIG_WORK_DIR:-target/rhiza-reconfigure-${profile}}"
status_path="${RHIZA_ADMIN_STATUS_PATH:-/v1/admin/membership/status}"
stop_path="${RHIZA_ADMIN_STOP_PATH:-/v1/admin/membership/stop}"
compact_path="${RHIZA_ADMIN_COMPACT_PATH:-/v1/admin/checkpoint/compact}"
cluster_id="${RHIZA_CLUSTER_ID:-rhiza-vind}"
effective_cluster_id="rhiza:${profile}:${cluster_id}"
epoch="${RHIZA_EPOCH:-1}"
generation="${RHIZA_RECOVERY_GENERATION:-1}"
auth_secret="${RHIZA_AUTH_SECRET:-rhiza-auth}"
object_secret="${RHIZA_OBJECT_SECRET-}"
object_secret_set="${RHIZA_OBJECT_SECRET+x}"
member_role_label="rhiza.dev/member-role"

case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac

for tool in kubectl jq yq openssl; do command -v "$tool" >/dev/null || { echo "missing required command: $tool" >&2; exit 127; }; done
stage_successor_manifest() {
  local manifest="$1"
  yq eval --inplace '
    (select(.kind == "StatefulSet") |
      .spec.template.metadata.labels["rhiza.dev/member-role"]) = "learner"
  ' "$manifest"
  if [ "$(yq eval -r 'select(.kind == "StatefulSet") |
    .spec.template.metadata.labels["rhiza.dev/member-role"]' "$manifest")" != learner ] ||
    [ "$(yq eval-all '[select(.kind == "Service")] | length' "$manifest")" != 1 ]; then
    echo "successor manifest did not preserve one headless peer/admin Service and learner Pods" >&2
    exit 65
  fi
}
old_id="$(jq -er '.config_id' "$old_bundle")"
new_id="$(jq -er '.config_id' "$successor_draft")"
old_replicas="$(jq -er '.members | length' "$old_bundle")"
new_replicas="$(jq -er '.members | length' "$successor_draft")"
[ "$new_id" -eq $((old_id + 1)) ] || { echo "successor config_id must be S+1" >&2; exit 65; }
case "$old_replicas:$new_replicas" in [3-7]:[3-7]) ;; *) exit 65;; esac
jq -e '(.predecessor | not)' "$successor_draft" >/dev/null
old_name="rhiza-${profile}-c${old_id}"
new_name="rhiza-${profile}-c${new_id}"

umask 077
old_preflight_yaml="$(mktemp)"
successor_preflight_yaml="$(mktemp)"
transition_secret_compare="$(mktemp)"
trap 'rm -f "$old_preflight_yaml" "$successor_preflight_yaml" "$transition_secret_compare"' EXIT
scripts/render-k8s-config.sh \
  "$old_id" "$old_replicas" "$old_bundle" "$old_preflight_yaml"
RHIZA_PRESTAGE_SOURCE_SECRET="${old_name}-bundle" scripts/render-k8s-config.sh \
  "$new_id" "$new_replicas" "$successor_draft" "$successor_preflight_yaml" successor
stage_successor_manifest "$successor_preflight_yaml"

mkdir -p "$work_dir"
chmod 700 "$work_dir"
stop_json="$work_dir/stop-c${old_id}.json"
stop_state="$work_dir/stop-c${old_id}.state.json"
successor_bundle="$work_dir/config-c${new_id}.json"
successor_yaml="$work_dir/config-c${new_id}.yaml"
compact_json="$work_dir/compact-c${new_id}.json"
target_inspect_json="$work_dir/checkpoint-c${new_id}.json"
status_json="$work_dir/status.json"
object_preflight_json="$work_dir/checkpoint-c${old_id}.preflight.json"
bundle_preflight_json="$work_dir/config-c${old_id}.preflight.json"
old_pod_uids="$work_dir/pod-uids-c${old_id}.json"
successor_pod_uids="$work_dir/pod-uids-c${new_id}.json"
endpoint_slices_json="$work_dir/endpointslices-c${new_id}.json"
client_service="rhiza-${profile}-client"

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
if ! stable_client_json="$("${k[@]}" get service "$client_service" -o json 2>/dev/null)" ||
  ! jq -e --arg name "$client_service" --arg profile "$profile" '
    .metadata.name == $name and
    (.spec.selector | keys | sort) == [
      "app.kubernetes.io/name",
      "rhiza.dev/execution-profile",
      "rhiza.dev/member-role"
    ] and
    .spec.selector["app.kubernetes.io/name"] == "rhiza" and
    .spec.selector["rhiza.dev/execution-profile"] == $profile and
    .spec.selector["rhiza.dev/member-role"] == "voter" and
    (.spec.ports | length) == 1 and
    .spec.ports[0].name == "client" and
    .spec.ports[0].port == 8080 and
    .spec.ports[0].targetPort == "client"
  ' <<< "$stable_client_json" >/dev/null; then
  echo "stable client Service is unavailable or does not match deploy/k8s/rhiza-client-services.yaml: $client_service" >&2
  exit 65
fi
"${k[@]}" get statefulset "$old_name" >/dev/null

validate_runtime_bundle() {
  local bundle="$1" expected_id="$2" label="$3"
  if ! "${k[@]}" exec -i "${old_name}-0" -- rhiza validate-config-bundle --stdin \
    < "$bundle" > "$bundle_preflight_json"; then
    echo "runtime rejected the $label configuration bundle" >&2
    exit 65
  fi
  jq -e --argjson id "$expected_id" '.config_id == $id' \
    "$bundle_preflight_json" >/dev/null || {
    echo "runtime rejected the $label configuration bundle" >&2
    exit 65
  }
}

admin() {
  RHIZA_KUBE_CONTEXT="$context" RHIZA_K8S_NAMESPACE="$namespace" \
    scripts/k8s-admin-job.sh "$@"
}

be64() {
  printf '%b' "$(printf '%016x' "$1" | sed 's/../\\x&/g')"
}

successor_digest() {
  digest_input="$(mktemp)"
  trap 'rm -f "$digest_input"' RETURN
  printf 'QMEM\0\1' > "$digest_input"
  be64 "$new_replicas" >> "$digest_input"
  while IFS= read -r member; do
    be64 "${#member}" >> "$digest_input"
    printf '%s' "$member" >> "$digest_input"
  done < <(jq -r '[.members[].node_id] | sort[]' "$successor_draft")
  openssl dgst -sha256 -binary "$digest_input" \
    | od -An -v -tu1 \
    | awk '{for (i=1; i<=NF; i++) values[++n]=$i} END {printf "["; for (i=1; i<=n; i++) printf "%s%s", (i>1 ? "," : ""), values[i]; print "]"}'
}

transition_secret_matches_artifacts() {
  local secret_json="$1" bundle="$2" stop="$3" pod_uids="$4"
  jq -e --slurpfile bundle "$bundle" --slurpfile stop "$stop" \
    --slurpfile pod_uids "$pod_uids" '
    (.data["config.json"] |
      if type == "string" then (try (@base64d | fromjson) catch null)
      else null end) as $actual_bundle |
    (.data["stop.json"] |
      if type == "string" then (try (@base64d | fromjson) catch null)
      else null end) as $actual_stop |
    (.data["old-pod-uids.json"] |
      if type == "string" then (try (@base64d | fromjson) catch null)
      else null end) as $actual_pod_uids |
    ($bundle | length == 1) and ($stop | length == 1) and
    ($pod_uids | length == 1) and
    $actual_bundle != null and $actual_stop != null and
    ($actual_pod_uids | type == "array") and
    $actual_bundle == $bundle[0] and $actual_stop == $stop[0] and
    ($actual_pod_uids | sort) == ($pod_uids[0] | sort)
  ' "$secret_json" >/dev/null
}

durable_resume=false
if durable_secret_json="$("${k[@]}" get secret "${new_name}-bundle" -o json 2>/dev/null)"; then
  durable_secret_file="$work_dir/${new_name}-bundle.secret.json"
  printf '%s' "$durable_secret_json" > "$durable_secret_file"
  scripts/k8s-stop-state.sh hydrate "$durable_secret_file" \
    "$old_bundle" "$successor_draft" "$stop_json" "$successor_bundle"
  old_pod_uids_attempt="${old_pod_uids}.attempt.$$"
  jq -er '.data["old-pod-uids.json"] |
    select(type == "string" and length > 0) | @base64d | fromjson |
    select(type == "array" and length > 0 and
      all(.[]; type == "string" and length > 0) and
      (unique | length) == length)
  ' "$durable_secret_file" > "$old_pod_uids_attempt" || {
    rm -f "$durable_secret_file" "$old_pod_uids_attempt"
    echo "durable transition Secret has no valid old Pod UID evidence" >&2
    exit 65
  }
  chmod 600 "$old_pod_uids_attempt"
  mv "$old_pod_uids_attempt" "$old_pod_uids"
  rm -f "$durable_secret_file"
  durable_resume=true
  echo "recovered transition state from durable Secret ${new_name}-bundle" >&2
fi
if ! auth_secret_json="$("${k[@]}" get secret "$auth_secret" -o json 2>/dev/null)"; then
  echo "runtime authentication Secret is unavailable: $auth_secret" >&2
  exit 65
fi
jq -e --slurpfile successor "$successor_draft" '
  def auth_token:
    type == "string" and (explode | length > 0 and all(. >= 33 and . <= 126));
  (.data["client-token"] |
    if type == "string" and length > 0 then (try @base64d catch null) else null end) as $client |
  (.data["admin-token"] |
    if type == "string" and length > 0 then (try @base64d catch null) else null end) as $admin |
  (.data["tail-token"] |
    if type == "string" and length > 0 then (try @base64d catch null) else null end) as $tail |
  ($client | auth_token) and ($admin | auth_token) and ($tail | auth_token) and
  ([ $client, $admin, $tail ] | unique | length) == 3 and
  all($successor[0].members[].token;
    . != $client and . != $admin and . != $tail)
' <<< "$auth_secret_json" >/dev/null || {
  echo "runtime authentication Secret has invalid or conflicting tokens" >&2
  exit 65
}
if [ -n "$object_secret_set" ] &&
  ! "${k[@]}" get secret "$object_secret" >/dev/null; then
  echo "object credential Secret is unavailable: $object_secret" >&2
  exit 65
fi
resume=false
if [ -s "$stop_json" ] && [ -s "$successor_bundle" ]; then
  jq -e --argjson old "$old_id" --argjson new "$new_id" '
    (.stop | keys | sort) == ["entry", "proof"] and
    .stop.entry.config_id == $old and
    .successor.config_id == $new
  ' "$stop_json" >/dev/null
  if ! jq empty "$successor_bundle" >/dev/null 2>&1; then
    echo "incomplete successor bundle artifact will be rebuilt: $successor_bundle" >&2
    rm -f "$successor_bundle"
  elif jq -e --argjson new "$new_id" '
    .config_id == $new and .predecessor != null
  ' "$successor_bundle" >/dev/null; then
    resume="$durable_resume"
  else
    echo "existing successor bundle is valid JSON but does not match configuration $new_id: $successor_bundle" >&2
    exit 65
  fi
fi

verify_old_active_configuration() {
  local ordinal
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    if ! admin "$old_name" "${old_name}-${ordinal}" GET "$status_path" \
      > "$status_json"; then
      echo "cannot verify live membership for ${old_name}-${ordinal}" >&2
      exit 65
    fi
    jq -e --arg cluster "$effective_cluster_id" --arg profile "$profile" \
      --argjson epoch "$epoch" --argjson generation "$generation" \
      --argjson id "$old_id" --argjson members "$expected_old_members" '
      .cluster_id == $cluster and .execution_profile == $profile and
      .epoch == $epoch and .recovery_generation == $generation and
      .node.configuration_status == "active" and
      .node.active_config_id == $id and
      .node.configuration_state.phase == "active" and
      .node.configuration_state.config_id == $id and
      .members == $members and (.members | length) == ($members | length)
    ' "$status_json" >/dev/null || {
      echo "live membership is not exact Active(S): ${old_name}-${ordinal}" >&2
      exit 65
    }
  done
}

adopt_old_voter_role() {
  local statefulset_json pods_json voter_patch name resource_version
  statefulset_json="$(mktemp)"
  pods_json="$(mktemp)"
  trap 'rm -f "$statefulset_json" "$pods_json"' RETURN
  "${k[@]}" get statefulset "$old_name" -o json > "$statefulset_json"
  jq -e --arg name "$old_name" --arg profile "$profile" \
    --argjson id "$old_id" --argjson replicas "$old_replicas" \
    --arg role_label "$member_role_label" '
    .metadata.name == $name and .spec.replicas == $replicas and
    .spec.selector.matchLabels["rhiza.dev/execution-profile"] == $profile and
    .spec.selector.matchLabels["rhiza.dev/config-id"] == ($id | tostring) and
    .spec.template.metadata.labels["rhiza.dev/execution-profile"] == $profile and
    .spec.template.metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
    ((.spec.template.metadata.labels[$role_label] // "voter") == "voter")
  ' "$statefulset_json" >/dev/null || {
    echo "old StatefulSet is not safe for voter-role adoption: $old_name" >&2
    exit 65
  }
  "${k[@]}" get pod \
    -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" \
    -o json > "$pods_json"
  jq -e --arg name "$old_name" --arg profile "$profile" \
    --argjson id "$old_id" --argjson replicas "$old_replicas" \
    --arg role_label "$member_role_label" '
    (.items | length) == $replicas and
    ([.items[].metadata.name] | sort) ==
      [range(0; $replicas) | "\($name)-\(.)"] and
    all(.items[];
      (.metadata.uid | type == "string" and length > 0) and
      (.metadata.resourceVersion | type == "string" and length > 0) and
      (.metadata.deletionTimestamp == null) and .status.phase == "Running" and
      .metadata.labels["rhiza.dev/execution-profile"] == $profile and
      .metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
      ((.metadata.labels[$role_label] // "voter") == "voter") and
      any(.metadata.ownerReferences[]?;
        .kind == "StatefulSet" and .name == $name and .controller == true))
  ' "$pods_json" >/dev/null || {
    echo "old Pods are not safe for voter-role adoption: $old_name" >&2
    exit 65
  }
  voter_patch="$(jq -cn --arg role_label "$member_role_label" '
    {spec:{template:{metadata:{labels:{($role_label):"voter"}}}}}
  ')"
  "${k[@]}" patch statefulset "$old_name" --type=merge -p "$voter_patch" >/dev/null
  while IFS=$'\t' read -r name resource_version; do
    "${k[@]}" label pod "$name" "${member_role_label}=voter" --overwrite \
      --resource-version="$resource_version" >/dev/null
  done < <(jq -r '.items[] | [.metadata.name,.metadata.resourceVersion] | @tsv' \
    "$pods_json")
  trap - RETURN
  rm -f "$statefulset_json" "$pods_json"
}

old_scale_down_complete=false
capture_or_validate_old_pod_uids() {
  local pods_file current_uids count captured desired_replicas
  pods_file="$(mktemp)"
  current_uids="${old_pod_uids}.current.$$"
  trap 'rm -f "$pods_file" "$current_uids"' RETURN
  "${k[@]}" get pod \
    -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" \
    -o json > "$pods_file"
  captured=false
  [ ! -s "$old_pod_uids" ] || captured=true
  count="$(jq '.items | length' "$pods_file")"
  if "$captured"; then
    jq -e --argjson replicas "$old_replicas" '
      type == "array" and length == $replicas and
      all(.[]; type == "string" and length > 0) and
      (unique | length) == length
    ' "$old_pod_uids" >/dev/null || {
      echo "persisted old Pod UID evidence is invalid" >&2
      exit 65
    }
    if [ "$count" -eq 0 ]; then
      if ! desired_replicas="$("${k[@]}" get statefulset "$old_name" \
        -o jsonpath='{.spec.replicas}')"; then
        echo "cannot verify old StatefulSet scale-down state" >&2
        exit 65
      fi
      [ "$desired_replicas" = 0 ] || {
        echo "old Pods are absent while the old StatefulSet still desires replicas" >&2
        exit 65
      }
      old_scale_down_complete=true
      trap - RETURN
      rm -f "$pods_file" "$current_uids"
      return
    fi
  fi
  jq -e --arg profile "$profile" --argjson id "$old_id" \
    --argjson replicas "$old_replicas" --argjson captured "$captured" \
    --arg role_label "$member_role_label" '
    (.items | length) == $replicas and
    all(.items[];
      (.metadata.uid | type == "string" and length > 0) and
      .metadata.labels["rhiza.dev/execution-profile"] == $profile and
      .metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
      (if $captured
       then (.metadata.labels[$role_label] == "voter" or
         .metadata.labels[$role_label] == "sealed")
       else .metadata.labels[$role_label] == "voter"
       end))
  ' "$pods_file" >/dev/null || {
    echo "old configuration Pods must have exact phase-appropriate role labels and UIDs" >&2
    exit 65
  }
  jq -c '[.items[].metadata.uid] | sort | unique' "$pods_file" > "$current_uids"
  count="$(jq 'length' "$current_uids")"
  if "$captured"; then
    jq -e --slurpfile stored "$old_pod_uids" '
      ($stored | length == 1) and . == ($stored[0] | sort)
    ' "$current_uids" >/dev/null || {
      echo "old configuration Pod UID set changed after transition evidence was captured" >&2
      exit 65
    }
  else
    [ "$count" -eq "$old_replicas" ] || {
      echo "cannot capture exact old Pod UIDs before configuration replacement" >&2
      exit 65
    }
    chmod 600 "$current_uids"
    mv "$current_uids" "$old_pod_uids"
  fi
  trap - RETURN
  rm -f "$pods_file" "$current_uids"
}

successor_members="$(jq -c '[.members[].node_id] | sort' "$successor_draft")"
successor_digest_json="$(successor_digest)"
stop_successor="$(jq -cn --argjson id "$new_id" --argjson members "$successor_members" \
  --argjson digest "$successor_digest_json" \
  '{config_id:$id,members:$members,digest:$digest}')"
expected_old_members="$(jq -ec '[.members[].node_id] | sort' "$old_bundle")"

recover_exact_stop_from_all_old_nodes() {
  local ordinal persisted_operation statuses_file status_file recovered_file
  [ -e "$stop_state" ] || return 1
  [ -s "$stop_state" ] || {
    echo "Stop state file is empty" >&2
    exit 65
  }
  persisted_operation="$(jq -er '.operation_id' "$stop_state")" || {
    echo "Stop state has no valid operation id" >&2
    exit 65
  }
  scripts/k8s-stop-state.sh prepare "$stop_state" \
    "$old_id" "$new_id" "$stop_successor" "$persisted_operation" >/dev/null
  statuses_file="${status_json}.stopped.$$"
  trap 'rm -f "$statuses_file" "${statuses_file}".*' RETURN
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    status_file="${statuses_file}.${ordinal}"
    admin "$old_name" "${old_name}-${ordinal}" GET "$status_path" \
      > "$status_file" || return 1
  done
  jq -s '.' "${statuses_file}".* > "$statuses_file"
  if ! jq -e --argjson replicas "$old_replicas" '
    length == $replicas and
    all(.[]; .node.configuration_status == "stopped")
  ' "$statuses_file" >/dev/null; then
    return 1
  fi
  jq -e --arg cluster "$effective_cluster_id" --arg profile "$profile" \
    --argjson epoch "$epoch" --argjson generation "$generation" \
    --argjson id "$old_id" --argjson members "$expected_old_members" \
    --argjson successor "$stop_successor" '
    . as $statuses |
    all($statuses[];
      .cluster_id == $cluster and .execution_profile == $profile and
      .epoch == $epoch and .recovery_generation == $generation and
      .node.configuration_status == "stopped" and
      .node.active_config_id == $id and
      .node.configuration_state.phase == "stopped" and
      .members == $members and (.members | length) == ($members | length) and
      (.stopped_transition.stop | keys | sort) == ["entry", "proof"] and
      .stopped_transition.stop.entry.config_id == $id and
      .stopped_transition.stop.proof != null and
      .stopped_transition.successor == $successor) and
    all($statuses[1:][]; .stopped_transition ==
      $statuses[0].stopped_transition)
  ' "$statuses_file" >/dev/null || {
    echo "old nodes do not agree on the exact stopped transition" >&2
    exit 65
  }
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    recovered_file="${statuses_file}.recovered.${ordinal}"
    scripts/k8s-stop-state.sh recover "$stop_state" \
      "${statuses_file}.${ordinal}" "$recovered_file"
    if [ "$ordinal" -eq 0 ]; then
      chmod 600 "$recovered_file"
      mv "$recovered_file" "$stop_json"
    else
      jq -e --slurpfile recovered "$recovered_file" \
        '. == $recovered[0]' "$stop_json" >/dev/null || {
        echo "old nodes produced different Stop recovery evidence" >&2
        exit 65
      }
    fi
  done
  scripts/k8s-stop-state.sh validate "$stop_state" "$stop_json"
  scripts/k8s-stop-state.sh write-bundle \
    "$stop_json" "$old_bundle" "$successor_draft" "$successor_bundle"
  chmod 600 "$stop_json"
  trap - RETURN
  rm -f "$statuses_file" "${statuses_file}".*
}

stopped_resume=false
if ! "$durable_resume"; then
  if ! mounted_bundle_json="$("${k[@]}" get secret "${old_name}-bundle" -o json 2>/dev/null |
    jq -er '.data["config.json"] | @base64d')"; then
    echo "runtime configuration bundle Secret is unavailable or invalid: ${old_name}-bundle" >&2
    exit 65
  fi
  jq -e --argjson mounted "$mounted_bundle_json" '. == $mounted' "$old_bundle" >/dev/null || {
    echo "runtime configuration bundle differs from the old bundle input" >&2
    exit 65
  }
  validate_runtime_bundle "$old_bundle" "$old_id" old
  validate_runtime_bundle "$successor_draft" "$new_id" successor-draft
  if recover_exact_stop_from_all_old_nodes; then
    stopped_resume=true
    resume=true
    validate_runtime_bundle "$successor_bundle" "$new_id" successor
    echo "recovered exact Stop from every old node" >&2
  else
    verify_old_active_configuration
    echo "preflighting checkpoint and object-store access"
    RHIZA_RECOVERY_GENERATION="$generation" \
      scripts/k8s-object-job.sh "$old_id" "$old_bundle" checkpoint inspect \
      > "$object_preflight_json"
    jq -e --argjson id "$old_id" '.identity.config_id == $id' \
      "$object_preflight_json" >/dev/null || {
      echo "object-store preflight returned a checkpoint for another configuration" >&2
      exit 65
    }
    echo "preflighting Kubernetes transition mutations"
    "${k[@]}" create secret generic "${new_name}-bundle" \
      --from-file=config.json="$successor_draft" --from-file=stop.json="$old_bundle" \
      --from-file=old-pod-uids.json="$old_bundle" \
      --dry-run=client -o yaml \
      | yq eval '.immutable = true' - \
      | "${k[@]}" create --dry-run=server -f - >/dev/null
    "${k[@]}" scale statefulset "$old_name" --replicas=0 --dry-run=server >/dev/null
    "${k[@]}" apply --server-side --dry-run=server --validate=false \
      -f "$successor_preflight_yaml" >/dev/null
    verify_old_active_configuration
    adopt_old_voter_role
    capture_or_validate_old_pod_uids
    "${k[@]}" create secret generic "${new_name}-prestage" \
      --from-file=config.json="$successor_draft" \
      --dry-run=client -o yaml \
      | yq eval '.immutable = true' - \
      | "${k[@]}" apply -f - >/dev/null
  fi
  cp "$successor_preflight_yaml" "$successor_yaml"
  "${k[@]}" apply --dry-run=client --validate=false -f "$successor_yaml" >/dev/null
  "$stopped_resume" || "${k[@]}" apply -f "$successor_yaml" >/dev/null
fi

if ! "$resume"; then
  echo "waiting for every successor to reach pre-Stop readiness"
  for ((attempt=1; attempt<=210; attempt++)); do
    if successor_pods_json="$("${k[@]}" get pod \
      -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${new_id}" \
      -o json 2>/dev/null)" &&
      jq -e --arg profile "$profile" --argjson id "$new_id" \
        --argjson replicas "$new_replicas" --arg role_label "$member_role_label" '
        (.items | length) == $replicas and
        all(.items[];
          .status.phase == "Running" and
          any(.status.conditions[]?;
            .type == "Ready" and .status == "True") and
          .metadata.labels["rhiza.dev/execution-profile"] == $profile and
          .metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
          .metadata.labels[$role_label] == "learner")
      ' <<< "$successor_pods_json" >/dev/null; then
      break
    fi
    [ "$attempt" -lt 210 ] || {
      echo "successor quorum did not reach pre-Stop readiness; refusing to Stop" >&2
      exit 1
    }
    sleep 2
  done
fi

echo "stopping configuration $old_id"
if [ -s "$stop_json" ]; then
  jq -e --argjson successor "$stop_successor" '.successor == $successor' \
    "$stop_json" >/dev/null || { echo "existing Stop response differs from successor draft" >&2; exit 65; }
  stop_candidate="$(jq -er '.operation_id' "$stop_json")"
else
  stop_candidate="stop-c${old_id}-to-c${new_id}-$(date -u +%Y%m%dT%H%M%SZ)"
fi
stop_operation="$(scripts/k8s-stop-state.sh prepare "$stop_state" \
  "$old_id" "$new_id" "$stop_successor" "$stop_candidate")"
if [ -s "$stop_json" ]; then
  scripts/k8s-stop-state.sh validate "$stop_state" "$stop_json"
fi
stop_request="$(jq -cn --arg op "$stop_operation" --argjson id "$old_id" \
  --argjson successor "$stop_successor" \
  '{operation_id:$op, expected_config_id:$id, successor:$successor}')"

recover_stop_from_status() {
  local rc
  admin "$old_name" "${old_name}-0" GET "$status_path" > "$status_json" || return 1
  if scripts/k8s-stop-state.sh recover "$stop_state" "$status_json" "$stop_json"; then
    return 0
  else
    rc=$?
  fi
  [ "$rc" -eq 1 ] && return 1
  return "$rc"
}
if ! "$resume"; then
stop_ready=false
if recover_stop_from_status; then
  stop_ready=true
else
  rc=$?
  [ "$rc" -eq 1 ] || exit "$rc"
fi
if ! "$stop_ready"; then
  stop_attempt_json="$stop_json.attempt"
  for ((attempt=1; attempt<=60; attempt++)); do
    if admin "$old_name" "${old_name}-0" POST "$stop_path" "$stop_request" \
      > "$stop_attempt_json"; then
      if ! scripts/k8s-stop-state.sh validate "$stop_state" "$stop_attempt_json"; then
        rm -f "$stop_attempt_json"
        exit 65
      fi
      mv "$stop_attempt_json" "$stop_json"
      break
    fi
    rm -f "$stop_attempt_json"
    if recover_stop_from_status; then
      break
    else
      rc=$?
      [ "$rc" -eq 1 ] || exit "$rc"
    fi
    [ "$attempt" -lt 60 ] || { echo "configuration stop did not converge" >&2; exit 1; }
    sleep 1
  done
fi
scripts/k8s-stop-state.sh validate "$stop_state" "$stop_json"

for ((attempt=1; attempt<=60; attempt++)); do
  all_stopped=true
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    admin "$old_name" "${old_name}-${ordinal}" GET "$status_path" \
      > "$status_json" || { all_stopped=false; break; }
    jq -e --argjson id "$old_id" \
      '.node.configuration_status == "stopped" and .node.active_config_id == $id and .node.configuration_state.phase == "stopped"' \
      "$status_json" >/dev/null || { all_stopped=false; break; }
  done
  "$all_stopped" && break
  [ "$attempt" -lt 60 ] || { echo "not every old node reached Stopped(S)" >&2; exit 1; }
  sleep 1
done

scripts/k8s-stop-state.sh write-bundle \
  "$stop_json" "$old_bundle" "$successor_draft" "$successor_bundle"
chmod 600 "$stop_json"
validate_runtime_bundle "$successor_bundle" "$new_id" successor

fi

capture_or_validate_old_pod_uids
if "${k[@]}" get secret "${new_name}-bundle" -o json \
  > "$transition_secret_compare" 2>/dev/null; then
  if ! transition_secret_matches_artifacts \
    "$transition_secret_compare" "$successor_bundle" "$stop_json" "$old_pod_uids"; then
    echo "existing successor transition Secret differs from the resume artifacts" >&2
    exit 65
  fi
else
  "${k[@]}" create secret generic "${new_name}-bundle" \
    --from-file=config.json="$successor_bundle" --from-file=stop.json="$stop_json" \
    --from-file=old-pod-uids.json="$old_pod_uids" \
    --dry-run=client -o yaml \
    | yq eval '.immutable = true' - \
    | "${k[@]}" create -f - >/dev/null
fi

if ! scripts/k8s-object-job.sh "$new_id" "$successor_bundle" validate-config-bundle \
  > "$bundle_preflight_json"; then
  echo "runtime rejected the successor configuration bundle" >&2
  exit 65
fi
jq -e --argjson id "$new_id" '.config_id == $id' "$bundle_preflight_json" >/dev/null || {
  echo "runtime rejected the successor configuration bundle" >&2
  exit 65
}

for ((attempt=1; attempt<=60; attempt++)); do
  if successor_pods_json="$("${k[@]}" get pod \
    -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${new_id}" \
    -o json 2>/dev/null)" &&
    jq -e --arg profile "$profile" --argjson id "$new_id" \
      --argjson replicas "$new_replicas" --arg role_label "$member_role_label" '
      (.items | length) == $replicas and
      all(.items[];
        .status.phase == "Running" and
        .metadata.labels["rhiza.dev/execution-profile"] == $profile and
        .metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
        (.metadata.labels[$role_label] == "learner" or
          .metadata.labels[$role_label] == "voter"))
    ' <<< "$successor_pods_json" >/dev/null; then
    break
  fi
  [ "$attempt" -lt 60 ] || {
    echo "successor learner Pods did not reach Running with exact role labels" >&2
    exit 1
  }
  sleep 1
done

for ((attempt=1; attempt<=60; attempt++)); do
  all_active=true
  for ((ordinal=0; ordinal<new_replicas; ordinal++)); do
    admin "$new_name" "${new_name}-${ordinal}" GET "$status_path" \
      > "$status_json" || { all_active=false; break; }
    jq -e --argjson id "$new_id" \
      '.node.configuration_status == "active" and .node.active_config_id == $id and .node.configuration_state.phase == "active"' \
      "$status_json" >/dev/null || { all_active=false; break; }
  done
  "$all_active" && break
  for ((ordinal=0; ordinal<new_replicas; ordinal++)); do
    if ! "${k[@]}" get pod "${new_name}-${ordinal}" -o json |
      jq -e --arg role_label "$member_role_label" \
        '.metadata.labels[$role_label] == "learner"' >/dev/null; then
      echo "successor Pod ${new_name}-${ordinal} became voter before Active(S+1)" >&2
      exit 65
    fi
  done
  [ "$attempt" -lt 60 ] || {
    echo "not every successor node auto-activated to Active(S+1)" >&2
    exit 1
  }
  sleep 1
done

voter_patch="$(jq -cn --arg role_label "$member_role_label" '
  {spec:{template:{metadata:{labels:{($role_label):"voter"}}}}}
')"
"${k[@]}" patch statefulset "$new_name" --type=merge -p "$voter_patch" >/dev/null
for ((ordinal=0; ordinal<new_replicas; ordinal++)); do
  "${k[@]}" label pod "${new_name}-${ordinal}" \
    "${member_role_label}=voter" --overwrite >/dev/null
done

sealed_patch="$(jq -cn --arg role_label "$member_role_label" '
  {spec:{template:{metadata:{labels:{($role_label):"sealed"}}}}}
')"
"${k[@]}" patch statefulset "$old_name" --type=merge -p "$sealed_patch" >/dev/null
if ! "$old_scale_down_complete"; then
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    "${k[@]}" label pod "${old_name}-${ordinal}" \
      "${member_role_label}=sealed" --overwrite >/dev/null
  done
fi

for ((attempt=1; attempt<=60; attempt++)); do
  if "${k[@]}" get pod \
    -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${new_id}" \
    -o json > "${successor_pod_uids}.pods" 2>/dev/null &&
    jq -e --arg profile "$profile" --argjson id "$new_id" \
      --argjson replicas "$new_replicas" --arg role_label "$member_role_label" '
      (.items | length) == $replicas and
      all(.items[];
        (.metadata.uid | type == "string" and length > 0) and
        .metadata.labels["rhiza.dev/execution-profile"] == $profile and
        .metadata.labels["rhiza.dev/config-id"] == ($id | tostring) and
        .metadata.labels[$role_label] == "voter" and
        any(.status.conditions[]?;
          .type == "Ready" and .status == "True"))
    ' "${successor_pod_uids}.pods" >/dev/null; then
    jq -c '[.items[].metadata.uid] | sort | unique' \
      "${successor_pod_uids}.pods" > "$successor_pod_uids"
    chmod 600 "$successor_pod_uids"
    rm -f "${successor_pod_uids}.pods"
    break
  fi
  rm -f "${successor_pod_uids}.pods"
  [ "$attempt" -lt 60 ] || {
    echo "Active successor Pods did not converge to Ready voter roles" >&2
    exit 1
  }
  sleep 1
done

for ((attempt=1; attempt<=60; attempt++)); do
  if "${k[@]}" get endpointslices.discovery.k8s.io \
    -l "kubernetes.io/service-name=${client_service}" \
    -o json > "$endpoint_slices_json" 2>/dev/null &&
    jq -e --slurpfile old "$old_pod_uids" \
      --slurpfile successor "$successor_pod_uids" '
      [.items[].endpoints[]?] as $endpoints |
      ($old | length == 1) and ($successor | length == 1) and
      ($successor[0] | length > 0) and
      all($endpoints[];
        .targetRef.kind == "Pod" and
        (.targetRef.uid | type == "string" and length > 0) and
        .conditions.ready == true and
        (.targetRef.uid as $uid | ($old[0] | index($uid)) == null)) and
      ([$endpoints[].targetRef.uid] | sort | unique) ==
        ($successor[0] | sort | unique)
    ' "$endpoint_slices_json" >/dev/null; then
    break
  fi
  [ "$attempt" -lt 60 ] || {
    echo "stable client Service ${client_service} EndpointSlices retained old Pod UIDs or did not converge to the exact Active successor voters; install deploy/k8s/rhiza-client-services.yaml first" >&2
    exit 1
  }
  sleep 1
done

echo "publishing first Active checkpoint for configuration $new_id"
admin "$new_name" "${new_name}-0" GET "$status_path" > "$status_json"
compact_request="$(jq -cn \
  --arg op "compact-c${new_id}-${stop_operation}" \
  --argjson id "$new_id" \
  --argjson generation "$generation" \
  --argjson root "$(jq -c '.qlog_root' "$status_json")" \
  '{operation_id:$op, expected_config_id:$id,
    expected_recovery_generation:$generation, expected_root:$root}')"
admin "$new_name" "${new_name}-0" POST "$compact_path" "$compact_request" \
  > "$compact_json"
jq -e '.anchor.configuration_state.phase == "active"' "$compact_json" >/dev/null
RHIZA_RECOVERY_GENERATION="$generation" \
  scripts/k8s-object-job.sh "$new_id" "$successor_bundle" checkpoint inspect \
  > "$target_inspect_json"
jq -e --argjson id "$new_id" \
  '.identity.config_id == $id and .base.snapshot and
   .base.snapshot.anchor.configuration_state.phase == "active"' \
  "$target_inspect_json" >/dev/null

echo "scaling sealed configuration $old_id to zero"
"${k[@]}" scale statefulset "$old_name" --replicas=0 >/dev/null
"${k[@]}" wait --for=delete pod \
  -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" \
  --timeout=180s >/dev/null
[ "$("${k[@]}" get statefulset "$old_name" -o jsonpath='{.spec.replicas}')" = 0 ]
[ -z "$("${k[@]}" get pod \
  -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" -o name)" ]

echo "configuration $new_id is Active; GC is now permitted"
echo "$successor_bundle"
