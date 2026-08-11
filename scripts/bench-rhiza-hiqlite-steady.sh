#!/usr/bin/env bash
# Sequential, fresh-cluster D1 Rhiza/Hiqlite program using concrete runners.
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
target="${RHIZA_HIQLITE_STEADY_TARGET_DIR:-$repo_root/target/rhiza-hiqlite-steady/$(date -u +%Y%m%d-%H%M%S)-$$}"
die() { echo "$*" >&2; exit 1; }
sha256_file() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
mkdir -p "$target"; chmod 700 "$target"
started_at="$(utc_now)"
workload_contract='D1|sql|unique-request-id|insert-only|128-byte|warmup=10|measure=60|concurrency=1,4,16|rhiza=sync|hiqlite=Immediate|voters=3|emptyDir|public-host-client'
contract_hash="$(printf '%s' "$workload_contract" | { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; } | awk '{print $1}')"
jq -n --arg started_at "$started_at" --arg contract "$workload_contract" --arg hash "$contract_hash" \
  --arg os "$(uname -s)" --arg kernel "$(uname -r)" --arg arch "$(uname -m)" \
  --arg cpu "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)" \
  --arg cpu_count "$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo unknown)" \
  --arg memory "$(sysctl -n hw.memsize 2>/dev/null || echo unknown)" \
  --arg docker "$(docker --version 2>/dev/null || echo unavailable)" --arg vcluster "$(vcluster version 2>/dev/null || echo unavailable)" \
  --arg docker_cpus "$(docker info --format '{{.NCPU}}' 2>/dev/null || echo unknown)" \
  --arg docker_memory "$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo unknown)" \
  --arg docker_kernel "$(docker info --format '{{.KernelVersion}}' 2>/dev/null || echo unknown)" \
  --arg docker_storage "$(docker info --format '{{.Driver}}' 2>/dev/null || echo unknown)" \
  --arg filesystem "$(df -P "$target" | awk 'NR==2 {print $1 ":" $6}')" \
  '{started_at:$started_at,workload_contract:$contract,workload_contract_sha256:$hash,
    host:{os:$os,kernel:$kernel,arch:$arch,cpu_model:$cpu,cpu_count:$cpu_count,
      memory_bytes:$memory,docker:$docker,vcluster:$vcluster,target_filesystem:$filesystem,
      docker_engine:{cpus:$docker_cpus,memory_bytes:$docker_memory,kernel:$docker_kernel,
        storage_driver:$docker_storage}}}' > "$target/contract.json"
summaries=()
for concurrency in 1 4 16; do
  for repetition in 1 2 3; do
    if [ "$repetition" = 2 ]; then order=(hiqlite rhiza); else order=(rhiza hiqlite); fi
    for system in "${order[@]}"; do
    cell="$target/c${concurrency}-repetition-$repetition-$system"; mkdir -p "$cell"
    if [ "$system" = hiqlite ]; then
      HIQLITE_STEADY_RUN_ID="c${concurrency}-r${repetition}-hiqlite" HIQLITE_STEADY_CONCURRENCY="$concurrency" HIQLITE_STEADY_TARGET_DIR="$cell" scripts/bench-hiqlite-steady.sh > "$cell/stdout.json"
      cp "$cell/steady-summary.json" "$cell/summary.checked.json"
    else
      # bench-vind owns the disposable Rhiza deployment and public host client.
      RHIZA_BENCH_TARGET_DIR="$cell/rhiza" RHIZA_DURABILITY_MODE=sync RHIZA_BENCH_D1_EXACT_WRITE=1 RHIZA_BENCH_MULTI_ENDPOINT=0 \
        RHIZA_BENCH_RHIZA_CPU_REQUEST=250m RHIZA_BENCH_RHIZA_CPU_LIMIT=1000m RHIZA_BENCH_RHIZA_MEMORY_REQUEST=512Mi RHIZA_BENCH_RHIZA_MEMORY_LIMIT=1Gi \
        scripts/bench-vind.sh --duration 60s --warmup 10s --concurrency "$concurrency" \
        --workload write --fault none > "$cell/rhiza-artifacts.json"
      benchmark="$(jq -er '.artifacts.benchmark_json' "$cell/rhiza-artifacts.json")"
      cluster_manifest="$(jq -er '.artifacts.cluster_manifest' "$cell/rhiza-artifacts.json")"
      runtime_pods="$(jq -er '.artifacts.runtime_pods_json' "$cell/rhiza-artifacts.json")"
      [ -f "$benchmark" ] || die 'Rhiza artifacts reference a missing benchmark report'
      [ -f "$cluster_manifest" ] && [ -f "$runtime_pods" ] || die 'Rhiza artifacts lack rendered or live deployment evidence'
      yq -e 'select(.kind == "StatefulSet") | .spec.replicas == 3 and ([.spec.template.spec.volumes[] | .emptyDir] | length > 0) and ([.spec.template.spec.containers[] | select(.name == "rhiza") | .resources] | all(.[]; .requests.cpu == "250m" and .limits.cpu == "1000m" and .requests.memory == "512Mi" and .limits.memory == "1Gi"))' "$cluster_manifest" >/dev/null || die 'Rhiza rendered StatefulSet violates D1 topology/resources'
      jq -e '[.items[] | .spec.containers[] | select(.name == "rhiza")] as $c | ($c|length)==3 and ($c|all(.[]; .resources.requests.cpu=="250m" and .resources.limits.cpu=="1000m" and .resources.requests.memory=="512Mi" and .resources.limits.memory=="1Gi")) and ($c|map(.imageID // .image)|unique|length)==1' "$runtime_pods" >/dev/null || die 'Rhiza live pod evidence violates D1 resources/image homogeneity'
      bench_sha="$(sha256_file "$benchmark")"
      artifacts_sha="$(sha256_file "$cell/rhiza-artifacts.json")"
      cluster_sha="$(sha256_file "$cluster_manifest")"
      runtime_sha="$(sha256_file "$runtime_pods")"
      jq -n --slurpfile artifacts "$cell/rhiza-artifacts.json" --slurpfile report "$benchmark" \
        --arg run_id "c${concurrency}-r${repetition}-rhiza" --arg root "$cell" --arg sha "$bench_sha" --arg artifacts_path "$cell/rhiza-artifacts.json" --arg artifacts_sha "$artifacts_sha" --arg cluster_path "$cluster_manifest" --arg cluster_sha "$cluster_sha" --arg runtime_path "$runtime_pods" --arg runtime_sha "$runtime_sha" \
        '($artifacts[0].provenance.execution.runtime_images.rhiza) as $runtime |
        if $runtime.status != "verified" or $runtime.observed_instances != 3 or
          ($runtime.image_digests|length) != 1
        then error("require one verified homogeneous Rhiza runtime image across three voters")
        else {schema_version:1,system:"rhiza",run_id:$run_id,contract:"D1",workload:"d1_sql_unique_request_id_deterministic_write",client_path:"public_host_side",voters:3,storage:"emptyDir",zero_pvc:true,durability:"sync",resources:{cpu_request:"250m",cpu_limit:"1000m",memory_request:"512Mi",memory_limit:"1Gi"},image_digest:$runtime.image_digests[0],provenance:{source:$artifacts[0].provenance.source,build_mode:$artifacts[0].provenance.image.build_mode,benchmark_binary_sha256:$artifacts[0].provenance.execution.benchmark_client.sha256,measurement_window:$artifacts[0].measurement_window,cleanup:$artifacts[0].cleanup,publishable:$artifacts[0].provenance.publishable,reasons:$artifacts[0].provenance.reasons},raw_artifact_paths:{root:$root,bench:$artifacts[0].artifacts.benchmark_json,artifacts:$artifacts_path,cluster_manifest:$cluster_path,runtime_pods:$runtime_path},raw_digests:{bench_sha256:$sha,artifacts_sha256:$artifacts_sha,cluster_manifest_sha256:$cluster_sha,runtime_pods_sha256:$runtime_sha},bench:{warmup_seconds:$report[0].configured.warmup_seconds,measure_seconds_requested:$report[0].configured.duration_seconds,concurrency:$report[0].configured.concurrency,closed_loop:true,retries:0,successes_after_valid_response:true,insert_only:$report[0].d1.insert_only,unique_request_ids:$report[0].d1.unique_request_ids,value_ledger_verified:$report[0].d1.value_ledger_verified,endpoint_count:$report[0].d1.endpoint_count,endpoint_fallback_attempts:$report[0].d1.endpoint_fallback_attempts,row_count_delta:$report[0].d1.row_count_delta,final_consistent_verification:$report[0].d1.final_consistent_verification,final_id:$report[0].d1.final_id,final_value:$report[0].d1.final_value,measurement:{attempts:$report[0].measurement.totals.attempts,successes:$report[0].measurement.totals.successes,errors:$report[0].measurement.totals.errors}},performance:{logical_ops_per_second:$report[0].measurement.totals.successful_committed_transactions_per_second,latency_ms:{p50:$report[0].measurement.totals.latency.p50_ms,p95:$report[0].measurement.totals.latency.p95_ms,p99:$report[0].measurement.totals.latency.p99_ms,p999:$report[0].measurement.totals.latency.p99_9_ms,max:([ $report[0].measurement.totals.latency.histogram_us[] | select(.count > 0) | .upper_bound_us / 1000 ] | max)}},telemetry:{cpu_rss:"not_measured",disk_bytes_per_op:"not_measured",network_bytes_per_op:"not_measured",fsync_count:"not_measured"},performance_publishable:($artifacts[0].provenance.publishable == true),resource_publishable:false,publication_blockers:["Hiqlite CPU/RSS/disk/network/fsync telemetry not_measured; resource scorecard is non-publishable"]} end' > "$cell/summary.json"
      cp "$cell/summary.json" "$cell/summary.checked.json"
    fi
    summaries+=("$cell/summary.checked.json")
    done
  done
done
jq -s --arg root "$target" --slurpfile contract "$target/contract.json" --arg finished_at "$(utc_now)" '
  def stats($xs): ($xs|sort) as $v | {median:$v[1],iqr:($v[2]-$v[0])};
  . as $runs | {schema_version:1,kind:"rhiza_hiqlite_d1_steady",repetitions:3,concurrencies:[1,4,16],orders:["rhiza,hiqlite","hiqlite,rhiza","rhiza,hiqlite"],raw_artifact_root:$root,coordinator_contract:$contract[0],finished_at:$finished_at,runs:$runs,
    aggregates:[$runs | group_by([.system,.bench.concurrency])[] | {system:.[0].system,concurrency:.[0].bench.concurrency,runs:length,throughput:stats(map(.performance.logical_ops_per_second)),latency_ms:{p50:stats(map(.performance.latency_ms.p50)),p95:stats(map(.performance.latency_ms.p95)),p99:stats(map(.performance.latency_ms.p99)),p999:stats(map(.performance.latency_ms.p999)),max:(map(.performance.latency_ms.max)|max)}}]}' "${summaries[@]}" > "$target/program.json"
scripts/check-bench-rhiza-hiqlite-steady-static.sh "$target/program.json"
cat "$target/program.json"
