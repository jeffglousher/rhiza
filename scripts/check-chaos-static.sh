#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"; cd "$root"
for script in scripts/chaos-k8s.sh scripts/chaos-rhiza-workload-hook.sh scripts/chaos-vm-loss.sh; do bash -n "$script"; done
command -v shellcheck >/dev/null
shellcheck scripts/chaos-k8s.sh scripts/chaos-mesh-post-render.sh scripts/chaos-rhiza-workload-hook.sh scripts/chaos-vm-loss.sh scripts/check-chaos-static.sh
jq -e '.schema_version==1 and .chart.version=="2.8.2" and .chart.sha256=="52c7858f11d14450da524a48f460bbfc491e90d58a703f0b6f851f2f431b3db3" and .images==[{reference:"ghcr.io/chaos-mesh/chaos-daemon:v2.8.2",digest:"sha256:6e36a3cd02b73d0ce18a5d258dc961dfc2f77d30210e939d5970c66948520aa0"},{reference:"ghcr.io/chaos-mesh/chaos-dashboard:v2.8.2",digest:"sha256:f024c378180000df33d5dd8b4c72d03e3df5497906868c28bf138f062ab19603"},{reference:"ghcr.io/chaos-mesh/chaos-coredns:v0.2.8",digest:"sha256:38bfdf5e37749a097873d226351013c34ba74174a8ddcfbe2871ec952af2d7b3"},{reference:"ghcr.io/chaos-mesh/chaos-mesh:v2.8.2",digest:"sha256:3d6127f3881d5b2f64ff9b536423d715f4e1fe0bb68976f41402690880427829"}] and .status=="official-chart-and-rendered-image-digests-locked"' deploy/chaos/chaos-mesh.lock.json >/dev/null
scripts/chaos-k8s.sh plan | jq -e '.physical_power_loss==false and .chaos_mesh.chart.version=="2.8.2" and (.ack_boundaries|sort)==["post-ack","pre-ack"]' >/dev/null
scripts/chaos-vm-loss.sh plan | jq -e '.physical_power_loss==false and .fault=="external-controller-sigkill-qemu"' >/dev/null
for f in deploy/chaos/*.yaml.in; do grep -Fq 'app.kubernetes.io/part-of: rhiza-chaos' "$f"; grep -Fq 'chaos.rhiza.io/run: __RUN_ID__' "$f"; done
grep -Fq -- 'chaos-mesh-post-render.sh' scripts/chaos-k8s.sh
grep -Fq -- 'verify_live_controller_images' scripts/chaos-k8s.sh
grep -Fq -- 'AllInjected' scripts/chaos-k8s.sh
grep -Fq -- 'Selected' scripts/chaos-k8s.sh
grep -Fq -- 'CHAOS_SHARED_DEV_CLUSTER' scripts/chaos-k8s.sh
grep -Fq -- 'CHAOS_AUTHORIZED_CONTEXT' scripts/chaos-k8s.sh
grep -Fq -- '--include-crds' scripts/chaos-k8s.sh
grep -Fq -- 'CHAOS_INSTALL_MANIFEST_OUT' scripts/chaos-k8s.sh
grep -Fq -- 'CHAOS_EXPECTED_SYSTEMS' scripts/chaos-k8s.sh
grep -Fq -- 'wait_for_recovered_quorum' scripts/chaos-rhiza-workload-hook.sh
grep -Fq -- 'ready_voters' scripts/chaos-rhiza-workload-hook.sh
grep -Fq -- 'read_barrier' scripts/chaos-rhiza-workload-hook.sh
grep -Fq -- 'apply --server-side --field-manager=rhiza-chaos' scripts/chaos-k8s.sh
yq -e '.clusterScoped == true and .controllerManager.enableFilterNamespace == true and .dashboard.create == false and .dnsServer.create == false and .chaosDaemon.runtime == "containerd" and .chaosDaemon.socketPath == "/run/containerd/containerd.sock"' deploy/chaos/values-shared-gke.yaml >/dev/null
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"
# shellcheck disable=SC2016 # literal $1 belongs to the fake command.
printf '#!/usr/bin/env bash\nif [ "$1" = -s ]; then printf Linux; else printf default; fi\n' > "$tmp/bin/uname"
# shellcheck disable=SC2016 # literal $1/$2 belong to the fake command.
printf '#!/usr/bin/env bash\nif [ "$1 $2" = "config current-context" ]; then printf default; else echo unexpected-kubectl-call >&2; exit 97; fi\n' > "$tmp/bin/kubectl"
chmod 700 "$tmp/bin/uname" "$tmp/bin/kubectl"
if PATH="$tmp/bin:$PATH" CHAOS_DISPOSABLE_CLUSTER=I_UNDERSTAND_DISPOSABLE_CHAOS CHAOS_K8S_CONTEXT=default CHAOS_NAMESPACE=rhiza-chaos-test CHAOS_RUN_ID=run-a CHAOS_ROLE=node-0 CHAOS_TARGET_ROLE=node-1 scripts/chaos-k8s.sh run "$tmp/output" pod-kill >"$tmp/default-context.out" 2>"$tmp/default-context.err"; then echo 'unsafe default context passed' >&2; exit 1; fi
grep -Fx 'chaos-k8s: context name is not an explicit disposable rhiza-chaos context' "$tmp/default-context.err" >/dev/null
printf 'chaos static checks passed\n'
