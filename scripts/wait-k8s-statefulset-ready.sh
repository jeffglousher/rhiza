#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 3 ] || {
  echo "usage: $0 STATEFULSET REPLICAS CONFIG_ID" >&2
  exit 64
}
name="$1"
replicas="$2"
config_id="$3"
profile="${RHIZA_EXECUTION_PROFILE-}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
context="${RHIZA_KUBE_CONTEXT:-}"
timeout_seconds="${RHIZA_STATEFULSET_READY_TIMEOUT:-420}"

case "$name" in ''|*[!a-z0-9-]*) exit 64;; esac
case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac
[ "$name" = "rhiza-${profile}-c${config_id}" ] || exit 64
case "$replicas" in 3|4|5|6|7) ;; *) exit 64;; esac
case "$config_id" in ''|*[!0-9]*|0) exit 64;; esac
case "$timeout_seconds" in ''|*[!0-9]*|0) exit 64;; esac

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")

resource_json() {
  kind="$1" resource="$2"
  if [ -n "${RHIZA_STATEFULSET_FIXTURE_DIR:-}" ]; then
    case "$kind" in
      statefulset) cat "$RHIZA_STATEFULSET_FIXTURE_DIR/statefulset.json" ;;
      pod) cat "$RHIZA_STATEFULSET_FIXTURE_DIR/${resource}.json" ;;
    esac
  else
    "${k[@]}" get "$kind" "$resource" -o json 2>/dev/null
  fi
}

ready_now() {
  update_revision="$(resource_json statefulset "$name" | \
    jq -er '.status.updateRevision | select(type == "string" and length > 0)')" || return 1
  resource_json statefulset "$name" | jq -e --argjson replicas "$replicas" '
    .metadata.generation != null and
    (.status.observedGeneration // 0) >= .metadata.generation and
    .spec.replicas == $replicas and
    (.status.readyReplicas // 0) == $replicas
  ' >/dev/null || return 1

  for ((ordinal=0; ordinal<replicas; ordinal++)); do
    resource_json pod "${name}-${ordinal}" | jq -e \
      --arg id "$config_id" --arg profile "$profile" --arg revision "$update_revision" '
      (.metadata.deletionTimestamp == null) and
      .metadata.labels["rhiza.dev/config-id"] == $id and
      .metadata.labels["rhiza.dev/execution-profile"] == $profile and
      .metadata.labels["controller-revision-hash"] == $revision and
      any(.status.conditions[]?; .type == "Ready" and .status == "True")
    ' >/dev/null || return 1
  done
}

if [ -n "${RHIZA_STATEFULSET_FIXTURE_DIR:-}" ]; then
  ready_now
  exit
fi

deadline=$((SECONDS + timeout_seconds))
until ready_now; do
  [ "$SECONDS" -lt "$deadline" ] || {
    echo "timed out waiting for StatefulSet $name and all expected pods to become Ready" >&2
    exit 1
  }
  sleep 1
done
