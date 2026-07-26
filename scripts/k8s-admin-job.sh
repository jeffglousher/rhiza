#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 SERVICE POD METHOD PATH [JSON_BODY]" >&2
  exit 64
fi
service="$1" pod="$2" method="$3" path="$4" body="${5-}"
[ -n "$body" ] || body='{}'
profile="${RHIZA_EXECUTION_PROFILE-}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
context="${RHIZA_KUBE_CONTEXT:-}"
auth_secret="${RHIZA_AUTH_SECRET:-rhiza-auth}"
curl_image="${RHIZA_CURL_IMAGE:-curlimages/curl:8.10.1}"
job="rhiza-${profile}-admin-$(date +%s)-$$-${RANDOM}"
manifest="$(mktemp)"
response="$(mktemp)"
trap 'rm -f "$manifest" "$response"' EXIT

emit_single_json() {
  file="$1"
  if ! jq -e -s 'length == 1' "$file" >/dev/null; then
    echo "admin Job stdout must contain exactly one JSON document" >&2
    cat "$file" >&2
    return 1
  fi
  cat "$file"
}

case "$profile" in
  sql|graph) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph" >&2; exit 65 ;;
esac
case "$service$pod" in *[!a-z0-9-]*) exit 64;; esac
target_prefix="rhiza-${profile}-c"
case "$service" in
  "$target_prefix"*) config_id="${service#"$target_prefix"}" ;;
  *) echo "SERVICE must target rhiza-${profile}-cCONFIG_ID" >&2; exit 65 ;;
esac
case "$config_id" in ''|0|*[!0-9]*) echo "SERVICE must target rhiza-${profile}-cCONFIG_ID" >&2; exit 65;; esac
case "$pod" in
  "$service"-*) ordinal="${pod#"$service"-}" ;;
  *) echo "POD must belong to SERVICE $service" >&2; exit 65 ;;
esac
case "$ordinal" in ''|*[!0-9]*) echo "POD must belong to SERVICE $service" >&2; exit 65;; esac
case "$method" in GET|POST|PUT) ;; *) exit 64;; esac
case "$path" in /*) ;; *) exit 64;; esac
printf '%s' "$body" | jq -e . >/dev/null

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
sed \
  -e "s|__JOB_NAME__|$job|g" \
  -e "s|__EXECUTION_PROFILE__|$profile|g" \
  -e 's|__CURL_IMAGE__|curlimages/curl:8.10.1|g' \
  -e 's|__METHOD__|GET|g' \
  -e 's|__BODY__|{}|g' \
  -e 's|__POD__|pod|g' \
  -e 's|__SERVICE__|service|g' \
  -e 's|__PATH__|/|g' \
  -e 's|__AUTH_SECRET__|rhiza-auth|g' \
  deploy/k8s/rhiza-admin-job.yaml > "$manifest"
# These variables expand inside the Job container.
# shellcheck disable=SC2016
export RHIZA_ADMIN_JOB_COMMAND='attempt=1
while :; do
  if curl --fail-with-body --silent --show-error \
    --connect-timeout 5 --max-time 90 \
    -X "$RHIZA_ADMIN_METHOD" \
    -H "Authorization: Bearer ${RHIZA_ADMIN_TOKEN}" \
    -H "x-rhiza-version: 1" \
    -H "Content-Type: application/json" \
    --data "$RHIZA_ADMIN_BODY" \
    "http://${RHIZA_ADMIN_POD}.${RHIZA_ADMIN_SERVICE}:8080${RHIZA_ADMIN_PATH}" \
    2>/tmp/rhiza-admin-curl-error; then
    rm -f /tmp/rhiza-admin-curl-error
    exit 0
  else
    curl_status=$?
  fi
  case "$curl_status" in
    6|7)
      if [ "$attempt" -ge 10 ]; then
        cat /tmp/rhiza-admin-curl-error >&2
        exit "$curl_status"
      fi
      attempt=$((attempt + 1))
      sleep 1
      ;;
    *)
      cat /tmp/rhiza-admin-curl-error >&2
      exit "$curl_status"
      ;;
  esac
done'
export RHIZA_ADMIN_JOB_IMAGE="$curl_image"
export RHIZA_ADMIN_JOB_AUTH_SECRET="$auth_secret"
export RHIZA_ADMIN_METHOD="$method"
export RHIZA_ADMIN_BODY="$body"
export RHIZA_ADMIN_POD="$pod"
export RHIZA_ADMIN_SERVICE="$service"
export RHIZA_ADMIN_PATH="$path"
yq eval --inplace '
  .spec.template.spec.containers[0].image = strenv(RHIZA_ADMIN_JOB_IMAGE) |
  .spec.template.spec.containers[0].args[0] = strenv(RHIZA_ADMIN_JOB_COMMAND) |
  (.spec.template.spec.containers[0].env[] |
    select(.name == "RHIZA_ADMIN_TOKEN").valueFrom.secretKeyRef.name) =
      strenv(RHIZA_ADMIN_JOB_AUTH_SECRET) |
  .spec.template.spec.containers[0].env += [
    {"name":"RHIZA_ADMIN_METHOD", "value":strenv(RHIZA_ADMIN_METHOD)},
    {"name":"RHIZA_ADMIN_BODY", "value":strenv(RHIZA_ADMIN_BODY)},
    {"name":"RHIZA_ADMIN_POD", "value":strenv(RHIZA_ADMIN_POD)},
    {"name":"RHIZA_ADMIN_SERVICE", "value":strenv(RHIZA_ADMIN_SERVICE)},
    {"name":"RHIZA_ADMIN_PATH", "value":strenv(RHIZA_ADMIN_PATH)}
  ]
' "$manifest"

if [ -n "${RHIZA_ADMIN_JOB_RENDER_ONLY:-}" ]; then
  cp "$manifest" "$RHIZA_ADMIN_JOB_RENDER_ONLY"
  exit 0
fi
if [ -n "${RHIZA_ADMIN_JOB_RESPONSE_FILE:-}" ]; then
  emit_single_json "$RHIZA_ADMIN_JOB_RESPONSE_FILE"
  exit
fi

if ! service_json="$("${k[@]}" get service "$service" -o json 2>/dev/null)" ||
  ! pod_json="$("${k[@]}" get pod "$pod" -o json 2>/dev/null)"; then
  echo "admin target is unavailable: $pod.$service" >&2
  exit 65
fi
jq -e --arg service "$service" --arg pod "$pod" --arg profile "$profile" \
  --arg config_id "$config_id" '
  .metadata.name == $service and
  .metadata.labels["rhiza.dev/execution-profile"] == $profile and
  .metadata.labels["rhiza.dev/config-id"] == $config_id and
  .spec.selector["rhiza.dev/execution-profile"] == $profile and
  .spec.selector["rhiza.dev/config-id"] == $config_id
' <<< "$service_json" >/dev/null || {
  echo "SERVICE does not belong to rhiza-${profile}-c${config_id}" >&2
  exit 65
}
jq -e --arg service "$service" --arg pod "$pod" --arg profile "$profile" \
  --arg config_id "$config_id" '
  .metadata.name == $pod and
  .metadata.labels["rhiza.dev/execution-profile"] == $profile and
  .metadata.labels["rhiza.dev/config-id"] == $config_id and
  any(.metadata.ownerReferences[]?;
    .kind == "StatefulSet" and .name == $service and .controller == true)
' <<< "$pod_json" >/dev/null || {
  echo "POD does not belong to StatefulSet $service" >&2
  exit 65
}

"${k[@]}" create -f "$manifest" >/dev/null
deadline=$((SECONDS + 130))
while :; do
  complete="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
  if [ "$complete" = True ]; then
    if ! "${k[@]}" logs "job/$job" > "$response"; then
      cat "$response" >&2
      exit 1
    fi
    emit_single_json "$response"
    exit 0
  fi
  failed="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  if [ "$failed" = True ]; then
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  fi
  [ "$SECONDS" -lt "$deadline" ] || {
    echo "timed out waiting for admin Job $job" >&2
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  }
  sleep 1
done
