#!/usr/bin/env bash
# Disposable-Linux Chaos Mesh runner. It never claims physical power loss.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd -P)"
lock="$root/deploy/chaos/chaos-mesh.lock.json"
die() { printf 'chaos-k8s: %s\n' "$*" >&2; exit 1; }
sha256_file() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
safe_name() { [[ "$1" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]]; }
usage() { cat <<'EOF'
usage: scripts/chaos-k8s.sh plan
       scripts/chaos-k8s.sh install VALUES.yaml
       scripts/chaos-k8s.sh run OUTPUT_DIR pod-kill|network-partition|io-error

run requires a disposable Linux context named rhiza-chaos-* (or kind-/k3d-
rhiza-chaos-*), or an explicitly authorized shared development context. It
also requires a non-default rhiza-chaos-* namespace, an exact run label, and a
workload hook. IOChaos is intentionally restricted to this same lane.
EOF
}
require_disposable_context() {
  command -v kubectl >/dev/null || die 'kubectl is required'
  [ "${CHAOS_DISPOSABLE_CLUSTER:-}" = I_UNDERSTAND_DISPOSABLE_CHAOS ] || die 'set exact disposable-cluster opt-in'
  [ -n "${CHAOS_K8S_CONTEXT:-}" ] || die 'CHAOS_K8S_CONTEXT is required'
  [ "$(kubectl config current-context)" = "$CHAOS_K8S_CONTEXT" ] || die 'current context differs from CHAOS_K8S_CONTEXT'
  case "$CHAOS_K8S_CONTEXT" in
    rhiza-chaos-*|kind-rhiza-chaos-*|k3d-rhiza-chaos-*) ;;
    *)
      [ "${CHAOS_SHARED_DEV_CLUSTER:-}" = I_ACCEPT_NAMESPACE_SCOPED_CHAOS ] \
        || die 'context name is not an explicit disposable rhiza-chaos context'
      [ "${CHAOS_AUTHORIZED_CONTEXT:-}" = "$CHAOS_K8S_CONTEXT" ] \
        || die 'shared development context is not exactly authorized'
      ;;
  esac
  kubectl get nodes -o json | jq -e \
    '.items | length > 0 and all(.[]; .metadata.labels["kubernetes.io/os"] == "linux")' \
    >/dev/null || die 'all selected Kubernetes nodes must be Linux'
}
require_run_scope() {
  require_disposable_context
  if [ -z "${CHAOS_NAMESPACE:-}" ] || ! safe_name "$CHAOS_NAMESPACE"; then die 'CHAOS_NAMESPACE must be a DNS label'; fi
  case "$CHAOS_NAMESPACE" in rhiza-chaos-*) ;; *) die 'namespace must begin rhiza-chaos-' ;; esac
  if [ -z "${CHAOS_RUN_ID:-}" ] || ! safe_name "$CHAOS_RUN_ID"; then die 'CHAOS_RUN_ID must be a DNS label'; fi
  if [ -z "${CHAOS_ROLE:-}" ] || ! safe_name "$CHAOS_ROLE"; then die 'CHAOS_ROLE must be a DNS label'; fi
  if [ -z "${CHAOS_TARGET_ROLE:-}" ] || ! safe_name "$CHAOS_TARGET_ROLE"; then die 'CHAOS_TARGET_ROLE must be a DNS label'; fi
  [ "$CHAOS_ROLE" != "$CHAOS_TARGET_ROLE" ] || die 'source and target roles must differ'
}
require_locked_chart() {
  command -v jq >/dev/null || die 'jq is required'
  jq -e '.schema_version==1 and .chart.repository=="https://charts.chaos-mesh.org" and .chart.name=="chaos-mesh" and .chart.version=="2.8.2"' "$lock" >/dev/null || die 'invalid Chaos Mesh lock'
  jq -e '.chart.sha256|type=="string" and test("^[0-9a-f]{64}$")' "$lock" >/dev/null || die 'official chart digest is not locked'
  jq -e '(.images|type)=="array" and (.images|length)>0 and all(.images[]; (.digest|type)=="string" and (.digest|test("^sha256:[0-9a-f]{64}$")))' "$lock" >/dev/null || die 'official controller/image digests are not locked'
}
pull_locked_chart() {
  local dir="$1" chart actual helm_root
  chart="$dir/chaos-mesh-2.8.2.tgz"
  helm_root="$dir/helm"; mkdir -p "$helm_root/config" "$helm_root/cache" "$helm_root/data"
  HELM_CONFIG_HOME="$helm_root/config" HELM_CACHE_HOME="$helm_root/cache" HELM_DATA_HOME="$helm_root/data" helm pull chaos-mesh --repo https://charts.chaos-mesh.org --version 2.8.2 --destination "$dir" >/dev/null
  actual="$(sha256_file "$chart")"
  [ "$actual" = "$(jq -r '.chart.sha256' "$lock")" ] || die 'downloaded chart does not match locked SHA-256'
  printf '%s\n' "$chart"
}
verify_rendered_images() {
  local chart="$1" values="$2" rendered="$3" raw="${3%.yaml}.raw.yaml" helm_root
  helm_root="$(dirname "$chart")/helm"
  HELM_CONFIG_HOME="$helm_root/config" HELM_CACHE_HOME="$helm_root/cache" HELM_DATA_HOME="$helm_root/data" helm template chaos-mesh "$chart" --include-crds --namespace chaos-mesh --values "$values" > "$raw"
  "$root/scripts/chaos-mesh-post-render.sh" < "$raw" > "$rendered"
  expected="$(jq -r '.images[] | (.reference|capture("^(?<repo>.+):[^:]+$").repo) + "@" + .digest' "$lock" | LC_ALL=C sort)"
  actual="$(awk '/^[[:space:]]*image:/{print $2}' "$rendered" | tr -d '"' | LC_ALL=C sort -u)"
  [ -n "$actual" ] || die 'rendered Chaos Mesh image set is empty'
  comm -23 <(printf '%s\n' "$actual") <(printf '%s\n' "$expected") | grep -q . \
    && die 'rendered images contain an entry outside the reviewed lock'
  printf '%s\n' "$actual" | grep -Fq 'ghcr.io/chaos-mesh/chaos-mesh@' \
    || die 'rendered controller image is missing'
  printf '%s\n' "$actual" | grep -Fq 'ghcr.io/chaos-mesh/chaos-daemon@' \
    || die 'rendered daemon image is missing'
}
verify_live_controller_images() {
  local expected actual
  expected="$(jq -r '.images[] | (.reference|capture("^(?<repo>.+):[^:]+$").repo) + "@" + .digest' "$lock" | LC_ALL=C sort)"
  actual="$(kubectl get deployments,daemonsets -n chaos-mesh -o json | jq -r '.items[].spec.template.spec.containers[].image' | LC_ALL=C sort -u)"
  [ -n "$actual" ] || die 'installed Chaos Mesh image set is empty'
  comm -23 <(printf '%s\n' "$actual") <(printf '%s\n' "$expected") | grep -q . \
    && die 'installed Chaos Mesh images contain an entry outside the reviewed lock'
  printf '%s\n' "$actual" | grep -Fq 'ghcr.io/chaos-mesh/chaos-mesh@' \
    || die 'installed controller image is missing'
  printf '%s\n' "$actual" | grep -Fq 'ghcr.io/chaos-mesh/chaos-daemon@' \
    || die 'installed daemon image is missing'
}
render() {
  local input="$1" output="$2"
  sed -e "s/__NAMESPACE__/$CHAOS_NAMESPACE/g" -e "s/__RUN_ID__/$CHAOS_RUN_ID/g" -e "s/__ROLE__/$CHAOS_ROLE/g" -e "s/__TARGET_ROLE__/$CHAOS_TARGET_ROLE/g" -e "s|__VOLUME_PATH__|${CHAOS_VOLUME_PATH:-/var/lib/rhiza}|g" "$input" > "$output"
}
wait_for_injection() {
  local resource="$1" name="$2" output="$3"
  for _ in $(seq 1 30); do
    kubectl get "$resource" -n "$CHAOS_NAMESPACE" "$name" -o json > "$output"
    if jq -e '([.status.conditions[]? | select(.type=="Selected" and .status=="True")]|length)>0 and ([.status.conditions[]? | select(.type=="AllInjected" and .status=="True")]|length)>0 and ([.status.experiment.containerRecords[]?.injectedCount] | add // 0)>0' "$output" >/dev/null; then return 0; fi
    sleep 1
  done
  die 'Chaos Mesh did not attest an active or completed injection'
}
cleanup_active_fault() { [ -z "${CHAOS_APPLIED_RENDERED:-}" ] || kubectl delete -f "$CHAOS_APPLIED_RENDERED" --wait=true >/dev/null 2>&1 || true; }
bind_workload_hook() {
  [ -n "${CHAOS_WORKLOAD_HOOK:-}" ] && [ -x "$CHAOS_WORKLOAD_HOOK" ] && [[ "$CHAOS_WORKLOAD_HOOK" = /* ]] || die 'CHAOS_WORKLOAD_HOOK must be an executable absolute path'
  CHAOS_WORKLOAD_HOOK_CANONICAL="$(readlink -f "$CHAOS_WORKLOAD_HOOK")"
  [ "$CHAOS_WORKLOAD_HOOK_CANONICAL" = "$CHAOS_WORKLOAD_HOOK" ] || die 'CHAOS_WORKLOAD_HOOK must not be symlinked'
  CHAOS_WORKLOAD_HOOK_SHA256="$(sha256_file "$CHAOS_WORKLOAD_HOOK")"
}
capture_hook() {
  local phase="$1" cutpoint="$2" dir="$3" out expected_systems_json
  out="$dir/$cutpoint-$phase.json"
  expected_systems_json="$(printf '%s' "${CHAOS_EXPECTED_SYSTEMS:-hiqlite,rhiza}" | jq -R 'split(",") | sort')"
  jq -e 'length > 0 and length == (unique | length) and all(.[]; test("^[a-z0-9][a-z0-9-]*$"))' <<<"$expected_systems_json" >/dev/null \
    || die 'CHAOS_EXPECTED_SYSTEMS must be a unique comma-separated system list'
  [ "$(sha256_file "$CHAOS_WORKLOAD_HOOK")" = "$CHAOS_WORKLOAD_HOOK_SHA256" ] || die 'workload hook changed during run'
  "$CHAOS_WORKLOAD_HOOK" "$phase" "$cutpoint" "$dir" > "$out"
  [ "$(sha256_file "$CHAOS_WORKLOAD_HOOK")" = "$CHAOS_WORKLOAD_HOOK_SHA256" ] || die 'workload hook changed during run'
  jq -e --arg phase "$phase" --arg cutpoint "$cutpoint" --argjson expected_systems "$expected_systems_json" '
    .schema_version==1 and .kind=="chaos-workload-boundary" and .phase==$phase and .cutpoint==$cutpoint and
    (.entries|type)=="array" and ([.entries[].system]|sort)==$expected_systems and
    (.entries|all((.id|type)=="string" and (.id|length)>0 and (.ack_kind|type)=="string" and (.durable_ack_observed|type)=="boolean")) and
    (if $phase=="verify" then .fault_observed==true else true end) and
    (if $phase=="verify" and $cutpoint=="pre-ack" then (.entries|all(.durable_ack_observed==false))
     elif $phase=="verify" and $cutpoint=="post-ack" then (.entries|all(.durable_ack_observed==true)) else true end)
  ' "$out" >/dev/null || die "invalid $phase boundary capture"
}
manifest() {
  local dir="$1" entries file rel
  entries="$dir/.entries"
  : > "$entries"
  while IFS= read -r file; do rel="${file#"$dir/"}"; jq -cn --arg path "$rel" --arg sha256 "$(sha256_file "$file")" '{path:$path,sha256:$sha256}' >> "$entries"; done < <(find "$dir" -type f ! -name manifest.json ! -name .entries | LC_ALL=C sort)
  jq -s --arg context "$CHAOS_K8S_CONTEXT" --arg namespace "$CHAOS_NAMESPACE" --arg run "$CHAOS_RUN_ID" --arg role "$CHAOS_ROLE" --arg target_role "$CHAOS_TARGET_ROLE" --arg fault_class "$CHAOS_FAULT_CLASS" --arg started "$CHAOS_STARTED_AT" --arg ended "$CHAOS_ENDED_AT" --arg hook "$CHAOS_WORKLOAD_HOOK_CANONICAL" --arg hook_sha256 "$CHAOS_WORKLOAD_HOOK_SHA256" '{schema_version:1,kind:"chaos-artifact-manifest",physical_power_loss:false,fault_class:$fault_class,context:$context,namespace:$namespace,run_id:$run,source_role:$role,target_role:$target_role,started_at:$started,ended_at:$ended,workload_hook:{canonical:$hook,sha256:$hook_sha256},files:.}' "$entries" > "$dir/manifest.json"
  rm -f "$entries"
}
plan() { jq -n --slurpfile lock "$lock" '{schema_version:1,kind:"chaos-plan",physical_power_loss:false,chaos_mesh:$lock[0],scenarios:["pod-kill","network-partition","io-error"],ack_boundaries:["pre-ack","post-ack"]}'; }
install() {
  [ $# = 1 ] && [ -r "$1" ] || die 'install needs one readable digest-pinned values file'
  require_disposable_context; require_locked_chart
  command -v helm >/dev/null || die 'helm is required'
  local tmp chart
  if [ "${CHAOS_SHARED_DEV_CLUSTER:-}" = I_ACCEPT_NAMESPACE_SCOPED_CHAOS ]; then
    command -v yq >/dev/null || die 'yq is required for shared development cluster values'
    yq -e '.clusterScoped == true and .controllerManager.enableFilterNamespace == true and .dashboard.create == false and .dnsServer.create == false and .chaosDaemon.runtime == "containerd" and .chaosDaemon.socketPath == "/run/containerd/containerd.sock"' "$1" >/dev/null \
      || die 'shared development values must enable namespace filtering and minimal containerd components'
  fi
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' RETURN
  chart="$(pull_locked_chart "$tmp")"; verify_rendered_images "$chart" "$1" "$tmp/rendered.yaml"
  if [ -n "${CHAOS_INSTALL_MANIFEST_OUT:-}" ]; then
    [[ "$CHAOS_INSTALL_MANIFEST_OUT" = /* ]] || die 'CHAOS_INSTALL_MANIFEST_OUT must be absolute'
    [ ! -e "$CHAOS_INSTALL_MANIFEST_OUT" ] || die 'CHAOS_INSTALL_MANIFEST_OUT must not exist'
    mkdir -p "$(dirname "$CHAOS_INSTALL_MANIFEST_OUT")"
    cp "$tmp/rendered.yaml" "$CHAOS_INSTALL_MANIFEST_OUT"
    chmod 600 "$CHAOS_INSTALL_MANIFEST_OUT"
  fi
  kubectl create namespace chaos-mesh --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply --server-side --field-manager=rhiza-chaos -f "$tmp/rendered.yaml"
}
run() {
  [ $# = 2 ] || { usage >&2; exit 2; }
  local dir="$1" scenario="$2" template rendered
  require_run_scope; require_locked_chart; verify_live_controller_images
  bind_workload_hook
  [ ! -e "$dir" ] || die 'output directory must not exist'
  case "$scenario" in pod-kill) template=pod-kill.yaml.in; CHAOS_FAULT_CLASS=process_kill ;; network-partition) template=network-partition.yaml.in; CHAOS_FAULT_CLASS=network_chaos ;; io-error) [[ "${CHAOS_VOLUME_PATH:-}" =~ ^/([A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+$ ]] || die 'IOChaos requires a conservative absolute CHAOS_VOLUME_PATH'; template=io-error.yaml.in; CHAOS_FAULT_CLASS=io_fault ;; *) die 'unknown scenario' ;; esac
  CHAOS_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  mkdir -p "$dir"; rendered="$dir/scenario.yaml"; render "$root/deploy/chaos/$template" "$rendered"
  kubectl get namespace "$CHAOS_NAMESPACE" >/dev/null || die 'refusing to create a namespace; provision the disposable workload first'
  [ "$(kubectl get namespace "$CHAOS_NAMESPACE" -o jsonpath='{.metadata.annotations.chaos-mesh\.org/inject}')" = enabled ] \
    || die 'namespace is not explicitly enabled for Chaos Mesh injection'
  kubectl get pods -n "$CHAOS_NAMESPACE" -l "app.kubernetes.io/part-of=rhiza-chaos,chaos.rhiza.io/run=$CHAOS_RUN_ID,chaos.rhiza.io/role=$CHAOS_ROLE" -o name | grep -q . || die 'source selector matches no pod'
  if [ "$scenario" = network-partition ]; then kubectl get pods -n "$CHAOS_NAMESPACE" -l "app.kubernetes.io/part-of=rhiza-chaos,chaos.rhiza.io/run=$CHAOS_RUN_ID,chaos.rhiza.io/role=$CHAOS_TARGET_ROLE" -o name | grep -q . || die 'network target selector matches no pod'; fi
  local resource name; resource="$(awk '/^kind:/{print tolower($2)}' "$rendered")"; name="rhiza-$CHAOS_RUN_ID-${scenario}"; CHAOS_APPLIED_RENDERED="$rendered"; trap cleanup_active_fault EXIT
  capture_hook prepare pre-ack "$dir"; kubectl apply -f "$rendered"; wait_for_injection "$resource" "$name" "$dir/injected.json"; capture_hook verify pre-ack "$dir"
  kubectl delete -f "$rendered" --wait=true; CHAOS_APPLIED_RENDERED=; capture_hook prepare post-ack "$dir"; kubectl apply -f "$rendered"; CHAOS_APPLIED_RENDERED="$rendered"; wait_for_injection "$resource" "$name" "$dir/injected-post-ack.json"; capture_hook verify post-ack "$dir"; kubectl delete -f "$rendered" --wait=true; CHAOS_APPLIED_RENDERED=
  kubectl version -o yaml > "$dir/kubectl-version.yaml"; CHAOS_ENDED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; manifest "$dir"
}
case "${1:-}" in plan) [ $# = 1 ] || exit 2; plan ;; install) shift; install "$@" ;; run) shift; run "$@" ;; *) usage >&2; exit 2 ;; esac
