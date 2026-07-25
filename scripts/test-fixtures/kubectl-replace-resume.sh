#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$RHIZA_REPLACE_RESUME_LOG"

profile="${RHIZA_EXECUTION_PROFILE:?}"
old_name="rhiza-${profile}-c3"
new_name="rhiza-${profile}-c4"

service_json() {
  local service="$1" config_id="${1##*c}"
  jq -cn --arg service "$service" --arg profile "$profile" \
    --arg config_id "$config_id" '{
    metadata:{name:$service,labels:{
      "rhiza.dev/execution-profile":$profile,
      "rhiza.dev/config-id":$config_id}},
    spec:{selector:{
      "rhiza.dev/execution-profile":$profile,
      "rhiza.dev/config-id":$config_id}}
  }'
}

named_pod_json() {
  local pod="$1" service="${1%-*}" config_id
  config_id="${service##*c}"
  jq -cn --arg pod "$pod" --arg service "$service" --arg profile "$profile" \
    --arg config_id "$config_id" '{
    metadata:{name:$pod,labels:{
      "rhiza.dev/execution-profile":$profile,
      "rhiza.dev/config-id":$config_id},
      ownerReferences:[{kind:"StatefulSet",name:$service,controller:true}]}
  }'
}

case " $* " in
  *" get service rhiza-${profile}-client -o json "*)
    jq -cn --arg profile "$profile" '{
      metadata:{name:("rhiza-" + $profile + "-client")},
      spec:{selector:{
        "app.kubernetes.io/name":"rhiza",
        "rhiza.dev/execution-profile":$profile,
        "rhiza.dev/member-role":"voter"},
        ports:[{name:"client",port:8080,targetPort:"client"}]}
    }'
    ;;
  *" get statefulset ${old_name} -o jsonpath={.spec.replicas} "*)
    case "$RHIZA_REPLACE_RESUME_MODE" in
      sealed-zero) printf 0 ;;
      sealed-zero-live) printf 3 ;;
      *) printf 3 ;;
    esac
    ;;
  *" get statefulset ${old_name} "*) ;;
  *" get secret ${new_name}-bundle -o json "*)
    if [[ "$RHIZA_REPLACE_RESUME_MODE" == sealed* ]]; then
      cat "$RHIZA_REPLACE_RESUME_TRANSITION_SECRET"
    else
      exit 1
    fi
    ;;
  *" get secret ${old_name}-bundle -o json "*)
    jq -n \
      --arg bundle "$(openssl base64 -A -in "$RHIZA_REPLACE_RESUME_OLD_BUNDLE")" \
      '{data:{"config.json":$bundle}}'
    ;;
  *" get secret rhiza-auth -o json "*)
    cat "$RHIZA_REPLACE_RESUME_AUTH_SECRET"
    ;;
  *" get service ${old_name} -o json "*) service_json "$old_name" ;;
  *" get service ${new_name} -o json "*) service_json "$new_name" ;;
  *" get pod ${old_name}-"*" -o json "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] &&
        [ "${arguments[index + 1]}" = pod ]; then
        pod="${arguments[index + 2]}"
        break
      fi
    done
    named_pod_json "$pod"
    ;;
  *" get pod ${new_name}-"*" -o json "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] &&
        [ "${arguments[index + 1]}" = pod ]; then
        pod="${arguments[index + 2]}"
        break
      fi
    done
    named_pod_json "$pod"
    ;;
  *" get pod -l rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=3 -o json "*)
    case "$RHIZA_REPLACE_RESUME_MODE" in
      sealed-zero | sealed-zero-live)
        printf '{"items":[]}\n'
        exit
        ;;
    esac
    role=voter
    [[ "$RHIZA_REPLACE_RESUME_MODE" != sealed* ]] || role=sealed
    recreated=false
    [ "$RHIZA_REPLACE_RESUME_MODE" != sealed-recreated ] || recreated=true
    jq -cn --arg role "$role" --arg profile "$profile" --arg name "$old_name" \
      --argjson recreated "$recreated" '{
      items:[range(0; 3) | {
        metadata:{
          name:($name + "-" + tostring),
          uid:(if $recreated and . == 2 then "recreated-uid"
            else ("old-uid-" + tostring) end),
          labels:{
            "rhiza.dev/execution-profile":$profile,
            "rhiza.dev/config-id":"3",
            "rhiza.dev/member-role":$role}}
      }]
    }'
    ;;
  *" exec -i ${old_name}-0 -- rhiza validate-config-bundle --stdin "*)
    "$RHIZA_REPLACE_RESUME_RHIZA" validate-config-bundle --stdin
    ;;
  *" create secret generic ${new_name}-bundle "*" --dry-run=client -o yaml "*)
    config_file=""
    stop_file=""
    uid_file=""
    for argument in "$@"; do
      case "$argument" in
        --from-file=config.json=*) config_file="${argument#--from-file=config.json=}" ;;
        --from-file=stop.json=*) stop_file="${argument#--from-file=stop.json=}" ;;
        --from-file=old-pod-uids.json=*)
          uid_file="${argument#--from-file=old-pod-uids.json=}"
          ;;
      esac
    done
    jq -n \
      --arg name "$new_name-bundle" \
      --arg config "$(openssl base64 -A -in "$config_file")" \
      --arg stop "$(openssl base64 -A -in "$stop_file")" \
      --arg uids "$(openssl base64 -A -in "$uid_file")" '{
      apiVersion:"v1",kind:"Secret",metadata:{name:$name},
      data:{"config.json":$config,"stop.json":$stop,"old-pod-uids.json":$uids}
    }'
    ;;
  *" apply --dry-run=client --validate=false -f "*) ;;
  *" create -f - "*)
    yq eval -e '
      .kind == "Secret" and .metadata.name == "rhiza-sql-c4-bundle" and
      .immutable == true and
      (.data["config.json"] | length > 0) and
      (.data["stop.json"] | length > 0) and
      (.data["old-pod-uids.json"] | length > 0)
    ' - >/dev/null
    printf '%s\n' immutable-transition-secret >> "$RHIZA_REPLACE_RESUME_LOG"
    exit 73
    ;;
  *" create -f "*)
    manifest="${*: -1}"
    if [ "$(yq eval -r '.spec.template.spec.containers[0].name' "$manifest")" = curl ]; then
      yq eval -r '.spec.template.spec.containers[0].env[] |
        select(.name == "RHIZA_ADMIN_POD") | .value' "$manifest" \
        > "$RHIZA_REPLACE_RESUME_ADMIN_POD"
      printf '%s\n' admin-status-job >> "$RHIZA_REPLACE_RESUME_LOG"
    else
      args="$(yq eval -r '.spec.template.spec.containers[0].args | join(" ")' "$manifest")"
      printf 'object-job %s\n' "$args" >> "$RHIZA_REPLACE_RESUME_LOG"
    fi
    ;;
  *" get job/rhiza-${profile}-admin-"*"Complete"*) printf True ;;
  *" get job/rhiza-${profile}-admin-"*"Failed"*) ;;
  *" logs job/rhiza-${profile}-admin-"*)
    pod="$(cat "$RHIZA_REPLACE_RESUME_ADMIN_POD")"
    cat "$RHIZA_REPLACE_RESUME_STOPPED_STATUS_DIR/${pod}.json"
    ;;
  *" get job/rhiza-${profile}-object-"*"Complete"*) ;;
  *" get job/rhiza-${profile}-object-"*"Failed"*) printf True ;;
  *" logs job/rhiza-${profile}-object-"*)
    echo "fixture stopped after successor bundle validation" >&2
    ;;
  *) exit 99 ;;
esac
