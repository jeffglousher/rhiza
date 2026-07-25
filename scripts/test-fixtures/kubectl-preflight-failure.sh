#!/usr/bin/env bash
set -euo pipefail
profile="${RHIZA_EXECUTION_PROFILE:?}"

printf '%s\n' "$*" >> "$RHIZA_KUBECTL_FIXTURE_LOG"

case " $* " in
  *" get statefulset rhiza-${profile}-c"*) exit 0 ;;
  *" get service rhiza-${profile}-client -o json "*)
    [ "$RHIZA_KUBECTL_FIXTURE_PROFILE" != stable-service-missing ] || exit 1
    role=voter
    [ "$RHIZA_KUBECTL_FIXTURE_PROFILE" != stable-service-mismatch ] || role=learner
    jq -cn --arg profile "$profile" --arg role "$role" '
      {metadata:{name:("rhiza-" + $profile + "-client")},
       spec:{selector:{
         "app.kubernetes.io/name":"rhiza",
         "rhiza.dev/execution-profile":$profile,
         "rhiza.dev/member-role":$role},
       ports:[{name:"client",port:8080,targetPort:"client"}]}}'
    ;;
  *" get service rhiza-${profile}-c"*" -o json "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] && [ "${arguments[index + 1]}" = service ]; then
        service="${arguments[index + 2]}"
        break
      fi
    done
    config_id="${service##*c}"
    jq -cn --arg service "$service" --arg profile "$profile" --arg config_id "$config_id" '
      {metadata:{name:$service,labels:{"rhiza.dev/execution-profile":$profile,
        "rhiza.dev/config-id":$config_id}},spec:{selector:{
        "rhiza.dev/execution-profile":$profile,"rhiza.dev/config-id":$config_id}}}'
    ;;
  *" get pod rhiza-${profile}-c"*" -o json "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] && [ "${arguments[index + 1]}" = pod ]; then
        pod="${arguments[index + 2]}"
        break
      fi
    done
    service="${pod%-*}"
    config_id="${service##*c}"
    jq -cn --arg service "$service" --arg pod "$pod" --arg profile "$profile" \
      --arg config_id "$config_id" '
      {metadata:{name:$pod,labels:{"rhiza.dev/execution-profile":$profile,
        "rhiza.dev/config-id":$config_id},ownerReferences:[{kind:"StatefulSet",
        name:$service,controller:true}]}}'
    ;;
  *" get secret rhiza-${profile}-c"*)
    case "$*" in *"-bundle -o json") ;; *) exit 99 ;; esac
    arguments=("$@")
    requested=""
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] && [ "${arguments[index + 1]}" = secret ]; then
        requested="${arguments[index + 2]}"
        break
      fi
    done
    source_id="$(jq -r '.config_id' "$RHIZA_KUBECTL_FIXTURE_BUNDLE_FILE")"
    if [ "$requested" = "rhiza-${profile}-c${source_id}-bundle" ]; then
      jq -n --arg bundle "$(openssl base64 -A -in "$RHIZA_KUBECTL_FIXTURE_BUNDLE_FILE")" \
        '{data:{"config.json":$bundle}}'
    else
      exit 1
    fi
    ;;
  *" get secret rhiza-auth -o json "*) cat "$RHIZA_KUBECTL_FIXTURE_AUTH_RESPONSE" ;;
  *" get secret missing-object-credentials "*) exit 1 ;;
  *" exec -i rhiza-${profile}-c"*" -- rhiza validate-config-bundle --stdin "*)
    "$RHIZA_KUBECTL_FIXTURE_RHIZA" validate-config-bundle --stdin
    ;;
  *" create secret generic "*" --dry-run=client -o yaml "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = secret ] && [ "${arguments[index + 1]}" = generic ]; then
        secret_name="${arguments[index + 2]}"
        break
      fi
    done
    printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\ndata:\n  config.json: e30=\n  stop.json: e30=\n' \
      "$secret_name"
    ;;
  *" create --dry-run=server -f - "*)
    yq eval -e '.kind == "Secret" and .immutable == true' - >/dev/null
    [ "$RHIZA_KUBECTL_FIXTURE_PROFILE" != dry-run-secret-denied ]
    ;;
  *" scale statefulset "*" --replicas=0 --dry-run=server "*)
    [ "$RHIZA_KUBECTL_FIXTURE_PROFILE" != dry-run-scale-denied ]
    ;;
  *" apply --server-side --dry-run=server --validate=false -f "*)
    [ "$RHIZA_KUBECTL_FIXTURE_PROFILE" != dry-run-apply-denied ]
    ;;
  *" create -f "*)
    manifest="${*: -1}"
    if [ "$(yq eval -r '.spec.template.spec.containers[0].name' "$manifest")" = curl ]; then
      method="$(yq eval -r '.spec.template.spec.containers[0].env[] |
        select(.name == "RHIZA_ADMIN_METHOD") | .value' "$manifest")"
      path="$(yq eval -r '.spec.template.spec.containers[0].env[] |
        select(.name == "RHIZA_ADMIN_PATH") | .value' "$manifest")"
      [ "$method $path" = "GET /v1/admin/membership/status" ]
      printf 'admin %s %s\n' "$method" "$path" >> "$RHIZA_KUBECTL_FIXTURE_LOG"
    else
      args="$(yq eval -r '.spec.template.spec.containers[0].args | join(" ")' "$manifest")"
      printf '%s\n' "$args" >> "$RHIZA_KUBECTL_FIXTURE_LOG"
      case "$args" in
        validate-config-bundle)
          if RHIZA_CONFIG_BUNDLE_FILE="$RHIZA_KUBECTL_FIXTURE_BUNDLE_FILE" \
            "$RHIZA_KUBECTL_FIXTURE_RHIZA" validate-config-bundle \
            > "$RHIZA_KUBECTL_FIXTURE_OBJECT_RESPONSE" 2>/dev/null; then
            printf success > "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE"
          else
            printf failed > "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE"
          fi
          ;;
        "checkpoint inspect")
          case "$RHIZA_KUBECTL_FIXTURE_PROFILE" in
            endpoint)
              [ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
                select(.name == "RHIZA_S3_ENDPOINT") | .value' "$manifest")" = \
                http://127.0.0.1:1 ]
              ;;
            *)
              [ "$(yq eval '[.spec.template.spec.containers[0].env[] |
                select(.name == "RHIZA_S3_ENDPOINT" or
                  .name == "RHIZA_S3_ACCESS_KEY" or
                  .name == "RHIZA_S3_SECRET_KEY")] | length' "$manifest")" = 0 ]
              ;;
          esac
          case "$RHIZA_KUBECTL_FIXTURE_PROFILE" in
            dry-run-*)
              source_id="$(jq -r '.config_id' "$RHIZA_KUBECTL_FIXTURE_BUNDLE_FILE")"
              jq -n --argjson id "$source_id" '{identity:{config_id:$id}}' \
                > "$RHIZA_KUBECTL_FIXTURE_OBJECT_RESPONSE"
              printf success > "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE"
              ;;
            *)
              printf failed > "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE"
              : > "$RHIZA_KUBECTL_FIXTURE_OBJECT_RESPONSE"
              ;;
          esac
          ;;
        *) exit 99 ;;
      esac
    fi
    ;;
  *" get job/rhiza-${profile}-admin-"*"Complete"*) printf 'True' ;;
  *" get job/rhiza-${profile}-admin-"*"Failed"*) exit 0 ;;
  *" logs job/rhiza-${profile}-admin-"*) cat "$RHIZA_KUBECTL_FIXTURE_ADMIN_RESPONSE" ;;
  *" get job/rhiza-${profile}-object-"*"Complete"*)
    [ "$(cat "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE")" = success ] && printf 'True'
    ;;
  *" get job/rhiza-${profile}-object-"*"Failed"*)
    [ "$(cat "$RHIZA_KUBECTL_FIXTURE_OBJECT_STATE")" = failed ] && printf 'True'
    ;;
  *" logs job/rhiza-${profile}-object-"*)
    if [ -s "$RHIZA_KUBECTL_FIXTURE_OBJECT_RESPONSE" ]; then
      cat "$RHIZA_KUBECTL_FIXTURE_OBJECT_RESPONSE"
    else
      echo "fixture object-store preflight failed" >&2
    fi
    ;;
  *) exit 99 ;;
esac
