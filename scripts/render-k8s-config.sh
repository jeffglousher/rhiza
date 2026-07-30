#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 CONFIG_ID REPLICAS BUNDLE_JSON OUTPUT_YAML [successor]" >&2
  exit 64
}

if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
  usage
fi
config_id="$1"
replicas="$2"
bundle="$3"
output="$4"
successor="${5:-}"
profile="${RHIZA_EXECUTION_PROFILE-}"
recorder_transport="${RHIZA_RECORDER_TRANSPORT:-http}"
recorder_tls="${RHIZA_RECORDER_TLS:-off}"
recorder_tls_secret="${RHIZA_RECORDER_TLS_SECRET-}"
recorder_tls_secret_set="${RHIZA_RECORDER_TLS_SECRET+x}"

case "$config_id" in ''|*[!0-9]*|0) usage;; esac
case "$replicas" in 3|4|5|6|7) ;; *) usage;; esac
case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac
case "$recorder_transport" in
  http|tcp-postcard|tcp-postcard-rpc) ;;
  *) echo "RHIZA_RECORDER_TRANSPORT must be http|tcp-postcard|tcp-postcard-rpc" >&2; exit 65 ;;
esac
case "$recorder_tls" in
  on|off) ;;
  *) echo "RHIZA_RECORDER_TLS must be on|off" >&2; exit 65 ;;
esac
[ "$recorder_tls" != on ] || [ "$recorder_transport" != http ] || {
  echo "RHIZA_RECORDER_TLS=on requires RHIZA_RECORDER_TRANSPORT=tcp-postcard|tcp-postcard-rpc" >&2
  exit 65
}
[ -r "$bundle" ] || { echo "cannot read bundle: $bundle" >&2; exit 66; }

require() { command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }; }
require jq
require sed
require yq

jq -e --argjson id "$config_id" --argjson replicas "$replicas" --arg profile "$profile" \
  --arg recorder_transport "$recorder_transport" --arg recorder_tls "$recorder_tls" '
  ((keys | sort) == ["config_id", "members"] or
   (keys | sort) == ["config_id", "members", "predecessor"]) and
  .config_id == $id and
  (.members | length) == $replicas and
  all(.members[];
    (($recorder_transport == "http" and
      (keys | sort) == ["log_url", "node_id", "token", "url"]) or
     (($recorder_transport == "tcp-postcard" or $recorder_transport == "tcp-postcard-rpc") and
      $recorder_tls == "off" and
      (keys | sort) == ["log_url", "node_id", "recorder_tcp_addr", "token", "url"]) or
     (($recorder_transport == "tcp-postcard" or $recorder_transport == "tcp-postcard-rpc") and
      $recorder_tls == "on" and
      (keys | sort) == ["log_url", "node_id", "recorder_tcp_addr", "recorder_tls_server_name", "token", "url"]))) and
  ((has("predecessor") | not) or .predecessor == null or
    (.predecessor |
      type == "object" and
      (keys | sort) == ["members", "stop_entry", "stop_proof"] and
      (.members | type == "array") and
      (.stop_entry | type == "object") and
      (.stop_proof | type == "object"))) and
  ([.members[].node_id] | sort) ==
    [range(1; $replicas + 1) | "node-\(.)"] and
  ([.members[].token] | all(type == "string" and test("^[!-~]+$"))) and
  ([.members[].token] | unique | length) == $replicas and
  ([.members | sort_by(.node_id)[] | {node_id, url, log_url}] ==
    [range(0; $replicas) as $n | {
      node_id: "node-\($n + 1)",
      url: "http://rhiza-\($profile)-c\($id)-\($n).rhiza-\($profile)-c\($id):8081",
      log_url: "http://rhiza-\($profile)-c\($id)-\($n).rhiza-\($profile)-c\($id):8080"
    }]) and
  ($recorder_transport == "http" or
    ([.members | sort_by(.node_id)[] | {recorder_tcp_addr}] ==
      [range(0; $replicas) as $n | {
        recorder_tcp_addr: "rhiza-\($profile)-c\($id)-\($n).rhiza-\($profile)-c\($id):8082"
      }])) and
  ($recorder_tls != "on" or
    ([.members | sort_by(.node_id)[] | {recorder_tls_server_name}] ==
      [range(0; $replicas) as $n | {
        recorder_tls_server_name: "rhiza-\($profile)-c\($id)-\($n).rhiza-\($profile)-c\($id)"
      }]))
' "$bundle" >/dev/null || { echo "invalid v1 bundle/config/replica identity" >&2; exit 65; }

name="rhiza-${profile}-c${config_id}"
image="${RHIZA_IMAGE:-rhiza-${profile}:dev}"
cluster_id="${RHIZA_CLUSTER_ID:-rhiza-vind}"
epoch="${RHIZA_EPOCH:-1}"
generation="${RHIZA_RECOVERY_GENERATION:-1}"
startup="${RHIZA_STARTUP_MODE-rejoin}"
durability="${RHIZA_DURABILITY_MODE-sync}"
durability_max_lag="${RHIZA_DURABILITY_MAX_LAG-}"
durability_interval="${RHIZA_DURABILITY_INTERVAL-}"
durability_max_lag_set="${RHIZA_DURABILITY_MAX_LAG+x}"
durability_interval_set="${RHIZA_DURABILITY_INTERVAL+x}"
s3_endpoint="${RHIZA_S3_ENDPOINT-}"
s3_endpoint_set="${RHIZA_S3_ENDPOINT+x}"
s3_bucket="${RHIZA_S3_BUCKET:-rhiza}"
s3_region="${RHIZA_S3_REGION:-us-east-1}"
s3_http="${RHIZA_S3_ALLOW_HTTP:-false}"
auth_secret="${RHIZA_AUTH_SECRET:-rhiza-auth}"
object_secret="${RHIZA_OBJECT_SECRET-}"
object_secret_set="${RHIZA_OBJECT_SECRET+x}"
checkpoint_lease_ms="${RHIZA_CHECKPOINT_LEASE_MS:-300000}"
cpu_request="${RHIZA_CPU_REQUEST:-250m}"
memory_request="${RHIZA_MEMORY_REQUEST:-512Mi}"
cpu_limit="${RHIZA_CPU_LIMIT:-2}"
memory_limit="${RHIZA_MEMORY_LIMIT:-2Gi}"
data_size_limit="${RHIZA_DATA_SIZE_LIMIT:-20Gi}"
bundle_secret="${name}-bundle"

die() { echo "$*" >&2; exit 65; }
u64_max=18446744073709551615
decimal_at_most() {
  local value="$1" maximum="$2" LC_ALL=C
  while [ "${value#0}" != "$value" ]; do value="${value#0}"; done
  [ -n "$value" ] || return 1
  if [ "${#value}" -ne "${#maximum}" ]; then
    [ "${#value}" -lt "${#maximum}" ]
    return
  fi
  [[ "$value" < "$maximum" || "$value" = "$maximum" ]]
}
validate_positive_u64() {
  local name="$1" value="$2"
  case "$value" in
    ''|*[!0-9]*) die "$name must be a positive integer" ;;
  esac
  decimal_at_most "$value" "$u64_max" || die "$name must be a positive integer"
}
validate_positive_u64 RHIZA_EPOCH "$epoch"
validate_positive_u64 RHIZA_RECOVERY_GENERATION "$generation"
case "$startup" in
  rejoin) ;;
  *) die "RHIZA_STARTUP_MODE must be rejoin when set" ;;
esac
case "$s3_http" in
  true|false|1|0) ;;
  *) die "RHIZA_S3_ALLOW_HTTP must be true|false|1|0" ;;
esac
validate_positive_u64 RHIZA_CHECKPOINT_LEASE_MS "$checkpoint_lease_ms"
validate_cpu_quantity() {
  local name="$1" value="$2"
  case "$value" in
    *[!0-9m]*|''|0|0m) die "$name must be a positive CPU quantity such as 250m or 2" ;;
    *m) case "${value%m}" in ''|*[!0-9]*) die "$name must be a positive CPU quantity such as 250m or 2" ;; esac ;;
  esac
}
validate_memory_quantity() {
  local name="$1" value="$2" amount
  case "$value" in
    *Ki) amount="${value%Ki}" ;;
    *Mi) amount="${value%Mi}" ;;
    *Gi) amount="${value%Gi}" ;;
    *Ti) amount="${value%Ti}" ;;
    *) die "$name must be a positive binary memory quantity such as 512Mi or 2Gi" ;;
  esac
  case "$amount" in ''|0|*[!0-9]*) die "$name must be a positive binary memory quantity such as 512Mi or 2Gi" ;; esac
}
validate_cpu_quantity RHIZA_CPU_REQUEST "$cpu_request"
validate_cpu_quantity RHIZA_CPU_LIMIT "$cpu_limit"
validate_memory_quantity RHIZA_MEMORY_REQUEST "$memory_request"
validate_memory_quantity RHIZA_MEMORY_LIMIT "$memory_limit"
validate_memory_quantity RHIZA_DATA_SIZE_LIMIT "$data_size_limit"
[ -z "$s3_endpoint_set" ] || [ -n "$s3_endpoint" ] ||
  die "RHIZA_S3_ENDPOINT must not be empty when set"
[ -z "$object_secret_set" ] || [ -n "$object_secret" ] ||
  die "RHIZA_OBJECT_SECRET must not be empty when set"
if [ "$recorder_tls" = on ]; then
  if [ -z "$recorder_tls_secret_set" ] || [ -z "$recorder_tls_secret" ]; then
    die "RHIZA_RECORDER_TLS_SECRET is required when RHIZA_RECORDER_TLS=on"
  fi
elif [ -n "$recorder_tls_secret_set" ]; then
  die "RHIZA_RECORDER_TLS_SECRET is irrelevant unless RHIZA_RECORDER_TLS=on"
fi
validate_duration() {
  local name="$1" value="$2" amount maximum
  case "$value" in
    *ms) amount="${value%ms}"; maximum=18446744073709551615 ;;
    *s) amount="${value%s}"; maximum=18446744073709551 ;;
    *m) amount="${value%m}"; maximum=307445734561825 ;;
    *h) amount="${value%h}"; maximum=5124095576030 ;;
    *) die "$name must be a positive duration with ms/s/m/h suffix" ;;
  esac
  case "$amount" in ''|*[!0-9]*) die "$name must be a positive duration with ms/s/m/h suffix" ;; esac
  decimal_at_most "$amount" "$u64_max" ||
    die "$name must be a positive duration with ms/s/m/h suffix"
  decimal_at_most "$amount" "$maximum" || die "$name duration is too large"
}

durability_max_lag_env=""
durability_interval_env=""
case "$durability" in
  sync)
    [ -z "$durability_max_lag_set" ] || die "RHIZA_DURABILITY_MAX_LAG is irrelevant for sync durability"
    [ -z "$durability_interval_set" ] || die "RHIZA_DURABILITY_INTERVAL is irrelevant for sync durability"
    ;;
  bounded)
    if [ -z "$durability_max_lag_set" ] || [ -z "$durability_max_lag" ]; then
      die "RHIZA_DURABILITY_MAX_LAG is required for bounded durability"
    fi
    [ -z "$durability_interval_set" ] || die "RHIZA_DURABILITY_INTERVAL is irrelevant for bounded durability"
    validate_duration RHIZA_DURABILITY_MAX_LAG "$durability_max_lag"
    durability_max_lag_env="            - {name: RHIZA_DURABILITY_MAX_LAG, value: $durability_max_lag}"
    ;;
  periodic)
    if [ -z "$durability_interval_set" ] || [ -z "$durability_interval" ]; then
      die "RHIZA_DURABILITY_INTERVAL is required for periodic durability"
    fi
    [ -z "$durability_max_lag_set" ] || die "RHIZA_DURABILITY_MAX_LAG is irrelevant for periodic durability"
    validate_duration RHIZA_DURABILITY_INTERVAL "$durability_interval"
    durability_interval_env="            - {name: RHIZA_DURABILITY_INTERVAL, value: $durability_interval}"
    ;;
  *) die "RHIZA_DURABILITY_MODE must be sync|bounded|periodic" ;;
esac

case "$successor" in
  '') successor_flag=false ;;
  successor)
    successor_flag=true
    prestage_source_secret="${RHIZA_PRESTAGE_SOURCE_SECRET:-${name}-source}"
    ;;
  *) usage ;;
esac

escape() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
object_secret_placeholder="${object_secret:-unused-object-credentials}"
sed \
  -e "s|__CONFIG_NAME__|$(escape "$name")|g" \
  -e "s|__EXECUTION_PROFILE__|${profile}|g" \
  -e "s|__CONFIG_ID__|${config_id}|g" \
  -e "s|__REPLICAS__|${replicas}|g" \
  -e "s|__RHIZA_IMAGE__|$(escape "$image")|g" \
  -e "s|__CLUSTER_ID__|$(escape "$cluster_id")|g" \
  -e "s|__EPOCH__|${epoch}|g" \
  -e "s|__RECOVERY_GENERATION__|${generation}|g" \
  -e "s|__STARTUP_MODE__|$(escape "$startup")|g" \
  -e "s|__DURABILITY_MODE__|$(escape "$durability")|g" \
  -e "s|__CHECKPOINT_LEASE_MS__|${checkpoint_lease_ms}|g" \
  -e "s|__CPU_REQUEST__|$(escape "$cpu_request")|g" \
  -e "s|__MEMORY_REQUEST__|$(escape "$memory_request")|g" \
  -e "s|__CPU_LIMIT__|$(escape "$cpu_limit")|g" \
  -e "s|__MEMORY_LIMIT__|$(escape "$memory_limit")|g" \
  -e "s|__DATA_SIZE_LIMIT__|$(escape "$data_size_limit")|g" \
  -e "s|            # __DURABILITY_MAX_LAG_ENV__|$(escape "$durability_max_lag_env")|g" \
  -e "s|            # __DURABILITY_INTERVAL_ENV__|$(escape "$durability_interval_env")|g" \
  -e "s|__S3_BUCKET__|$(escape "$s3_bucket")|g" \
  -e "s|__S3_REGION__|$(escape "$s3_region")|g" \
  -e "s|__S3_ALLOW_HTTP__|$(escape "$s3_http")|g" \
  -e "s|__AUTH_SECRET__|$(escape "$auth_secret")|g" \
  -e "s|__OBJECT_SECRET__|$(escape "$object_secret_placeholder")|g" \
  -e "s|__BUNDLE_SECRET__|$(escape "$bundle_secret")|g" \
  -e "s|__SUCCESSOR__|${successor_flag}|g" \
  deploy/k8s/rhiza-cluster.yaml > "$output"
export S3_ENDPOINT="$s3_endpoint" S3_ENDPOINT_SET="$s3_endpoint_set"
export OBJECT_SECRET="$object_secret" OBJECT_SECRET_SET="$object_secret_set"
yq eval --inplace '
  (select(.kind == "StatefulSet") |
    .spec.template.spec.containers[] | select(.name == "rhiza") | .env) |= (
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
' "$output"
export CONFIG_NAME="$name" RECORDER_TRANSPORT="$recorder_transport" RECORDER_TLS="$recorder_tls"
yq eval --inplace '
  (select(.kind == "StatefulSet") |
    .spec.template.spec.containers[] | select(.name == "rhiza") | .env) |= (
      map(select(.name != "RHIZA_RECORDER_TRANSPORT" and .name != "RHIZA_RECORDER_TLS")) +
      [
        {"name":"RHIZA_RECORDER_TRANSPORT", "value":strenv(RECORDER_TRANSPORT)},
        {"name":"RHIZA_RECORDER_TLS", "value":strenv(RECORDER_TLS)}
      ]
    )
' "$output"
if [ "$recorder_transport" != http ]; then
  yq eval --inplace '
    (select(.kind == "Service" and .metadata.name == strenv(CONFIG_NAME)) |
      .spec.ports) += [{"name":"recorder-tcp", "port":8082, "targetPort":"recorder-tcp"}] |
    (select(.kind == "StatefulSet") |
      .spec.template.spec.containers[] | select(.name == "rhiza") | .ports) +=
        [{"name":"recorder-tcp", "containerPort":8082}] |
    (select(.kind == "StatefulSet") |
      .spec.template.spec.containers[] | select(.name == "rhiza") | .env) +=
        [{"name":"RHIZA_RECORDER_TCP_LISTEN", "value":"0.0.0.0:8082"}]
  ' "$output"
fi
if [ "$recorder_tls" = on ]; then
  export RECORDER_TLS_SECRET="$recorder_tls_secret"
  yq eval --inplace '
    (select(.kind == "StatefulSet") |
      .spec.template.spec.containers[] | select(.name == "rhiza") | .env) += [
        {"name":"RHIZA_RECORDER_TLS_CERT_FILE", "value":"/run/secrets/rhiza/recorder-tls/tls.crt"},
        {"name":"RHIZA_RECORDER_TLS_KEY_FILE", "value":"/run/secrets/rhiza/recorder-tls/tls.key"},
        {"name":"RHIZA_RECORDER_TLS_CA_FILE", "value":"/run/secrets/rhiza/recorder-tls/ca-bundle.pem"}
      ] |
    (select(.kind == "StatefulSet") |
      .spec.template.spec.containers[] | select(.name == "rhiza") | .volumeMounts) += [{
        "name":"recorder-tls", "mountPath":"/run/secrets/rhiza/recorder-tls", "readOnly":true
      }] |
    (select(.kind == "StatefulSet") | .spec.template.spec.volumes) += [{
      "name":"recorder-tls",
      "secret":{
        "secretName":strenv(RECORDER_TLS_SECRET),
        "items":[
          {"key":"tls.crt", "path":"tls.crt"},
          {"key":"tls.key", "path":"tls.key"},
          {"key":"ca-bundle.pem", "path":"ca-bundle.pem"}
        ]
      }
    }]
  ' "$output"
fi
if "$successor_flag"; then
  export PRESTAGE_SOURCE_SECRET="$prestage_source_secret"
  export PRESTAGE_DRAFT_SECRET="${name}-prestage"
  # shellcheck disable=SC2016
  yq eval --inplace '
    with(select(.kind == "StatefulSet");
      .spec.template.metadata.labels["rhiza.dev/member-role"] = "learner" |
      with(.spec.template.spec.volumes[] | select(.name == "config");
        .secret.optional = true
      ) |
      .spec.template.spec.volumes += [
        {"name":"prestage-draft","secret":{"secretName":strenv(PRESTAGE_DRAFT_SECRET)}},
        {"name":"prestage-source","secret":{"secretName":strenv(PRESTAGE_SOURCE_SECRET)}}
      ] |
      with(.spec.template.spec.containers[] | select(.name == "rhiza");
        with(.env[] | select(.name == "RHIZA_CONFIG_BUNDLE_FILE");
          .value = "/etc/rhiza/prestage/config.json"
        ) |
        del(.command) |
        .args = ["prestage", "serve"] |
        .env += [
          {"name":"RHIZA_PRESTAGE_SOURCE_BUNDLE_FILE","value":"/etc/rhiza/source/config.json"},
          {"name":"RHIZA_PRESTAGE_TRANSITION_BUNDLE_FILE","value":"/etc/rhiza/transition/config.json"},
          {"name":"RHIZA_PRESTAGE_DIR","value":"/var/lib/rhiza/.prestage"}
        ] |
        .volumeMounts = (
          [.volumeMounts[] | select(.name != "config")] + [
            {"name":"prestage-draft","mountPath":"/etc/rhiza/prestage","readOnly":true},
            {"name":"prestage-source","mountPath":"/etc/rhiza/source","readOnly":true},
            {"name":"config","mountPath":"/etc/rhiza/transition","readOnly":true}
          ]
        )
      ) |
      del(.spec.template.spec.initContainers)
    )
  ' "$output"
fi
yq eval --inplace '
  (select(.kind == "StatefulSet") |
    .spec.template.spec.containers[].env[]? |
    select(has("value")) | .value) style = "double"
' "$output"
if grep -Eq '__[A-Z0-9_]+__' "$output"; then
  echo "unrendered placeholder" >&2
  exit 65
fi
