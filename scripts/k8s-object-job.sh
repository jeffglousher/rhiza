#!/usr/bin/env bash
set -euo pipefail

[ "$#" -ge 3 ] || {
  echo "usage: $0 CONFIG_ID BUNDLE_JSON COMMAND [ARG ...]" >&2
  exit 64
}
config_id="$1" bundle="$2"
shift 2
profile="${RHIZA_EXECUTION_PROFILE-}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
context="${RHIZA_KUBE_CONTEXT:-}"
name="rhiza-${profile}-c${config_id}"
job="rhiza-${profile}-object-${config_id}-$(date +%s)-$$-${RANDOM}"
manifest="$(mktemp)"
response="$(mktemp)"
trap 'rm -f "$manifest" "$response"' EXIT

emit_command_stdout() { cat "$1"; }

case "$config_id" in ''|*[!0-9]*|0) exit 64;; esac
case "$profile" in
  sql|graph) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph" >&2; exit 65 ;;
esac
jq -e --argjson id "$config_id" '.config_id == $id' "$bundle" >/dev/null
command_json="$(printf '%s\0' "$@" | jq -Rs 'split("\u0000")[:-1]')"

export JOB_NAME="$job"
export PROFILE="$profile"
export JOB_IMAGE="${RHIZA_IMAGE:-rhiza:dev}"
export COMMAND_JSON="$command_json"
export CLUSTER_ID="${RHIZA_CLUSTER_ID:-rhiza-vind}"
export EPOCH="${RHIZA_EPOCH:-1}"
export S3_ENDPOINT="${RHIZA_S3_ENDPOINT-}"
export S3_ENDPOINT_SET="${RHIZA_S3_ENDPOINT+x}"
export S3_BUCKET="${RHIZA_S3_BUCKET:-rhiza}"
export S3_REGION="${RHIZA_S3_REGION:-us-east-1}"
export S3_ALLOW_HTTP="${RHIZA_S3_ALLOW_HTTP:-false}"
export OBJECT_SECRET="${RHIZA_OBJECT_SECRET-}"
export OBJECT_SECRET_SET="${RHIZA_OBJECT_SECRET+x}"
export RECOVERY_GENERATION="${RHIZA_RECOVERY_GENERATION:-1}"
export BUNDLE_SECRET="${name}-bundle"
die() { echo "$*" >&2; exit 65; }
case "$EPOCH" in
  ''|*[!0-9]*|0) die "RHIZA_EPOCH must be a positive integer" ;;
esac
case "$RECOVERY_GENERATION" in
  ''|*[!0-9]*|0) die "RHIZA_RECOVERY_GENERATION must be a positive integer" ;;
esac
case "$S3_ALLOW_HTTP" in
  true|false|1|0) ;;
  *) die "RHIZA_S3_ALLOW_HTTP must be true|false|1|0" ;;
esac
[ -z "$S3_ENDPOINT_SET" ] || [ -n "$S3_ENDPOINT" ] || {
  echo "RHIZA_S3_ENDPOINT must not be empty when set" >&2
  exit 65
}
[ -z "$OBJECT_SECRET_SET" ] || [ -n "$OBJECT_SECRET" ] || {
  echo "RHIZA_OBJECT_SECRET must not be empty when set" >&2
  exit 65
}
yq eval '
  .metadata.name = strenv(JOB_NAME) |
  .metadata.labels["rhiza.dev/execution-profile"] = strenv(PROFILE) |
  .spec.template.spec.containers[0].image = strenv(JOB_IMAGE) |
  .spec.template.spec.containers[0].args = (strenv(COMMAND_JSON) | from_json) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_EXECUTION_PROFILE").value) = strenv(PROFILE) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_CLUSTER_ID").value) = strenv(CLUSTER_ID) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_CONFIG_BUNDLE_FILE").value) = ("/etc/rhiza/" + strenv(PROFILE) + "/config.json") |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_EPOCH").value) = strenv(EPOCH) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_S3_BUCKET").value) = strenv(S3_BUCKET) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_S3_REGION").value) = strenv(S3_REGION) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_S3_ALLOW_HTTP").value) = strenv(S3_ALLOW_HTTP) |
  (.spec.template.spec.containers[0].env[] | select(.name == "RHIZA_RECOVERY_GENERATION").value) = strenv(RECOVERY_GENERATION) |
  (.spec.template.spec.volumes[] | select(.name == "config").secret.secretName) = strenv(BUNDLE_SECRET) |
  (.spec.template.spec.containers[0].volumeMounts[] | select(.name == "config").mountPath) = ("/etc/rhiza/" + strenv(PROFILE)) |
  .spec.template.spec.containers[0].env |= (
    map(select(.name != "RHIZA_S3_ENDPOINT" and
      .name != "RHIZA_S3_ACCESS_KEY" and
      .name != "RHIZA_S3_SECRET_KEY")) +
    ([{"name":"RHIZA_S3_ENDPOINT", "value":strenv(S3_ENDPOINT)}] |
      map(select(strenv(S3_ENDPOINT_SET) == "x"))) +
    ([
      {"name":"RHIZA_S3_ACCESS_KEY", "valueFrom":{"secretKeyRef":{
        "name":strenv(OBJECT_SECRET), "key":"access-key"}}},
      {"name":"RHIZA_S3_SECRET_KEY", "valueFrom":{"secretKeyRef":{
        "name":strenv(OBJECT_SECRET), "key":"secret-key"}}}
    ] | map(select(strenv(OBJECT_SECRET_SET) == "x")))
  )
' deploy/k8s/rhiza-checkpoint-job.yaml > "$manifest"

if [ -n "${RHIZA_OBJECT_JOB_RENDER_ONLY:-}" ]; then
  cp "$manifest" "$RHIZA_OBJECT_JOB_RENDER_ONLY"
  exit 0
fi
if [ -n "${RHIZA_OBJECT_JOB_RESPONSE_FILE:-}" ]; then
  emit_command_stdout "$RHIZA_OBJECT_JOB_RESPONSE_FILE"
  exit
fi

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
"${k[@]}" create -f "$manifest" >/dev/null
deadline=$((SECONDS + 310))
while :; do
  complete="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
  if [ "$complete" = True ]; then
    if ! "${k[@]}" logs "job/$job" > "$response"; then
      cat "$response" >&2
      exit 1
    fi
    emit_command_stdout "$response"
    exit 0
  fi
  failed="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  if [ "$failed" = True ]; then
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  fi
  [ "$SECONDS" -lt "$deadline" ] || {
    echo "timed out waiting for object Job $job" >&2
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  }
  sleep 1
done
