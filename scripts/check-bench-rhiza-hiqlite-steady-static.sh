#!/usr/bin/env bash
# Validate stored D1 evidence; invalid or incomplete evidence is never publishable.
set -euo pipefail
sha256_file() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
die() { echo "$*" >&2; exit 1; }

if [ "${1:-}" = --fixture ]; then
  fixture="$(mktemp -d)"; trap 'rm -rf "$fixture"' EXIT
  runs=()
  for i in $(seq 0 17); do
    case $((i % 6)) in 0|3|4) system=rhiza; durability=sync; container=rhiza;; *) system=hiqlite; durability=Immediate; container=hiqlite;; esac
    concurrency=$((1 << ((i / 6) * 2)))
    bench="$fixture/bench-$i.json"; manifest="$fixture/manifest-$i.yaml"; pods="$fixture/pods-$i.json"; evidence="$fixture/evidence-$i.json"
    if [ "$system" = rhiza ]; then
      jq -n --argjson i "$i" --argjson concurrency "$concurrency" '
        {configured:{warmup_seconds:10,duration_seconds:60,concurrency:$concurrency},
         d1:{insert_only:true,unique_request_ids:true,value_ledger_verified:true,endpoint_count:1,
           endpoint_fallback_attempts:0,row_count_delta:1,final_consistent_verification:true,
           final_id:"id",final_value:("x"*128)},
         measurement:{totals:{attempts:1,successes:1,errors:0,
           successful_committed_transactions_per_second:(10+$i),
           latency:{p50_ms:(1+$i),p95_ms:(2+$i),p99_ms:(3+$i),p99_9_ms:(4+$i),
             histogram_us:[{upper_bound_us:((5+$i)*1000),count:1}]}}}}' > "$bench"
    else
      jq -n --argjson i "$i" --argjson concurrency "$concurrency" '
        {schema_version:1,command:"bench-write",workload:"d1_sql_unique_request_id_deterministic_write",
         durability:"Hiqlite Immediate",payload_bytes:128,id_prefix:"fixture",insert_only:true,
         unique_request_ids:true,warmup_seconds:10,measure_seconds_requested:60,
         concurrency:$concurrency,closed_loop:true,retries:0,successes_after_valid_response:true,
         warmup:{attempts:1,successes:1,errors:0},
         measurement:{attempts:1,successes:1,errors:0,elapsed_seconds:1,
           successes_per_second:(10+$i),latency_ns:{p50:((1+$i)*1000000),p95:((2+$i)*1000000),
             p99:((3+$i)*1000000),p999:((4+$i)*1000000),max:((5+$i)*1000000)}},
         measured_start_sequence:0,measured_end_sequence:1,row_count_delta:1,
         final_consistent_verification:true,value_ledger_verified:true,final_id:"id",final_value:("x"*128)}' > "$bench"
    fi
    FIXTURE_CONTAINER="$container" yq -n '{"apiVersion":"apps/v1","kind":"StatefulSet","spec":{"replicas":3,"template":{"spec":{"volumes":[{"name":"data","emptyDir":{}}],"containers":[{"name":strenv(FIXTURE_CONTAINER),"resources":{"requests":{"cpu":"250m","memory":"512Mi"},"limits":{"cpu":"1000m","memory":"1Gi"}}}]}}}}' > "$manifest"
    jq -n --arg container "$container" '{items:[range(0;3)|{spec:{containers:[{name:$container,imageID:"sha256:fixture",resources:{requests:{cpu:"250m",memory:"512Mi"},limits:{cpu:"1000m",memory:"1Gi"}}}]}}]}' > "$pods"
    if [ "$system" = rhiza ]; then
      jq -n '{provenance:{source:"fixture",image:{build_mode:"fixture"},execution:{runtime_images:{rhiza:{status:"verified",observed_instances:3,image_digests:["sha256:fixture"]}},benchmark_client:{sha256:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}' > "$evidence"
    else
      jq -n '{release:"0.14.0",commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",openraft:"0.9.24",log_sync:"Immediate",source_build:"exact-source-build",cargo_lock_sha256:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",proxy_patch_sha256:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",image_digest:"sha256:fixture"}' > "$evidence"
    fi
    jq -n --slurpfile raw "$bench" --arg system "$system" --arg durability "$durability" --arg run_id "fixture-$i" --arg root "$fixture/run-$i" --arg bench "$bench" --arg manifest "$manifest" --arg pods "$pods" --arg evidence "$evidence" --arg bench_sha "$(sha256_file "$bench")" --arg manifest_sha "$(sha256_file "$manifest")" --arg pods_sha "$(sha256_file "$pods")" --arg evidence_sha "$(sha256_file "$evidence")" '
      ($raw[0]) as $r |
      (if $system == "rhiza" then
        {warmup_seconds:$r.configured.warmup_seconds,measure_seconds_requested:$r.configured.duration_seconds,
         concurrency:$r.configured.concurrency,closed_loop:true,retries:0,successes_after_valid_response:true,
         insert_only:$r.d1.insert_only,unique_request_ids:$r.d1.unique_request_ids,
         value_ledger_verified:$r.d1.value_ledger_verified,endpoint_count:$r.d1.endpoint_count,
         endpoint_fallback_attempts:$r.d1.endpoint_fallback_attempts,row_count_delta:$r.d1.row_count_delta,
         final_consistent_verification:$r.d1.final_consistent_verification,final_id:$r.d1.final_id,
         final_value:$r.d1.final_value,measurement:{attempts:$r.measurement.totals.attempts,
           successes:$r.measurement.totals.successes,errors:$r.measurement.totals.errors}}
       else $r end) as $normalized_bench |
      {system:$system,run_id:$run_id,contract:"D1",workload:"d1_sql_unique_request_id_deterministic_write",
       client_path:"public_host_side",voters:3,storage:"emptyDir",zero_pvc:true,durability:$durability,
       resources:{cpu_request:"250m",cpu_limit:"1000m",memory_request:"512Mi",memory_limit:"1Gi"},
       image_digest:"sha256:fixture",raw_artifact_paths:{root:$root,bench:$bench,cluster_manifest:$manifest},
       raw_digests:{bench_sha256:$bench_sha,cluster_manifest_sha256:$manifest_sha},bench:$normalized_bench,
       performance:(if $system == "rhiza" then
         {logical_ops_per_second:$r.measurement.totals.successful_committed_transactions_per_second,
          latency_ms:{p50:$r.measurement.totals.latency.p50_ms,p95:$r.measurement.totals.latency.p95_ms,
            p99:$r.measurement.totals.latency.p99_ms,p999:$r.measurement.totals.latency.p99_9_ms,
            max:([$r.measurement.totals.latency.histogram_us[]|select(.count>0)|.upper_bound_us/1000]|max)}}
       else {logical_ops_per_second:$r.measurement.successes_per_second,
         latency_ms:{p50:($r.measurement.latency_ns.p50/1000000),p95:($r.measurement.latency_ns.p95/1000000),
           p99:($r.measurement.latency_ns.p99/1000000),p999:($r.measurement.latency_ns.p999/1000000),
           max:($r.measurement.latency_ns.max/1000000)}} end),
       telemetry:{cpu_rss:"not_measured",disk_bytes_per_op:"not_measured",network_bytes_per_op:"not_measured",fsync_count:"not_measured"},
       performance_publishable:true,resource_publishable:false,publication_blockers:["resource"]} as $base |
      $base + if $system == "rhiza" then
        {raw_artifact_paths:($base.raw_artifact_paths+{artifacts:$evidence,runtime_pods:$pods}),
         raw_digests:($base.raw_digests+{artifacts_sha256:$evidence_sha,runtime_pods_sha256:$pods_sha}),
         provenance:{source:"fixture",build_mode:"fixture",benchmark_binary_sha256:("a"*64)}}
      else
        {raw_artifact_paths:($base.raw_artifact_paths+{live_pods:$pods,build_provenance:$evidence}),
         raw_digests:($base.raw_digests+{live_pods_sha256:$pods_sha,build_provenance_sha256:$evidence_sha}),
         hiqlite_provenance:{release:"0.14.0",commit:"c8316c53799c509990475ea8e2aa2ef8679e070e",
           openraft:"0.9.24",log_sync:"Immediate",source_build:"exact-source-build",
           cargo_lock_sha256:("a"*64),proxy_patch_sha256:("b"*64)}} end' > "$fixture/run-$i.json"
    runs+=("$fixture/run-$i.json")
  done
  jq -s 'def stats($x):($x|sort) as $v|{median:$v[1],iqr:($v[2]-$v[0])}; . as $r | {schema_version:1,repetitions:3,concurrencies:[1,4,16],orders:["rhiza,hiqlite","hiqlite,rhiza","rhiza,hiqlite"],coordinator_contract:{workload_contract_sha256:("c"*64),started_at:"2020-01-01T00:00:00Z",host:{os:"fixture"}},finished_at:"2020-01-01T00:01:00Z",runs:$r,aggregates:[$r|group_by([.system,.bench.concurrency])[]|{system:.[0].system,concurrency:.[0].bench.concurrency,runs:length,throughput:stats(map(.performance.logical_ops_per_second)),latency_ms:{p50:stats(map(.performance.latency_ms.p50)),p95:stats(map(.performance.latency_ms.p95)),p99:stats(map(.performance.latency_ms.p99)),p999:stats(map(.performance.latency_ms.p999)),max:(map(.performance.latency_ms.max)|max)}}]}' "${runs[@]}" > "$fixture/program.json"
  "$0" "$fixture/program.json"
  for mutation in '(.aggregates[0].throughput.median=999)' '(.runs[0].raw_digests.bench_sha256="bad")' 'del(.runs[1].hiqlite_provenance)' '(.runs[0].system="hiqlite")' '(.runs[0].performance.logical_ops_per_second=0)' '(.runs[0].bench.concurrency=4)' '(.runs|=.[0:17])'; do
    jq "$mutation" "$fixture/program.json" > "$fixture/bad.json"
    if "$0" "$fixture/bad.json" >/dev/null 2>&1; then echo "fixture mutation accepted: $mutation" >&2; exit 1; fi
  done
  yq -i '.spec.template.spec.containers[0].resources.limits.cpu = "2"' "$fixture/manifest-0.yaml"
  jq --arg digest "$(sha256_file "$fixture/manifest-0.yaml")" '(.runs[0].raw_digests.cluster_manifest_sha256 = $digest)' "$fixture/program.json" > "$fixture/bad.json"
  if "$0" "$fixture/bad.json" >/dev/null 2>&1; then echo 'fixture mutation accepted: raw resource override' >&2; exit 1; fi
  jq '.source_build = "unverified"' "$fixture/evidence-1.json" > "$fixture/evidence-mutated.json"
  jq --arg evidence "$fixture/evidence-mutated.json" --arg digest "$(sha256_file "$fixture/evidence-mutated.json")" '(.runs[1].raw_artifact_paths.build_provenance = $evidence | .runs[1].raw_digests.build_provenance_sha256 = $digest)' "$fixture/program.json" > "$fixture/bad.json"
  if "$0" "$fixture/bad.json" >/dev/null 2>&1; then echo 'fixture mutation accepted: raw provenance override' >&2; exit 1; fi
  exit 0
fi

input="${1:?usage: check-bench-rhiza-hiqlite-steady-static.sh PROGRAM_JSON|--fixture}"
[ -f "$input" ] || die "missing program: $input"
verify_digest() { [ -f "$1" ] && [ "$(sha256_file "$1")" = "$2" ] || die "$3 digest mismatch: $1"; }
run_count="$(jq '.runs | length' "$input")"
for ((index=0; index<run_count; index++)); do
  system="$(jq -r ".runs[$index].system" "$input")"
  image="$(jq -r ".runs[$index].image_digest" "$input")"
  bench="$(jq -r ".runs[$index].raw_artifact_paths.bench" "$input")"
  bench_sha="$(jq -r ".runs[$index].raw_digests.bench_sha256" "$input")"
  manifest="$(jq -r ".runs[$index].raw_artifact_paths.cluster_manifest" "$input")"
  manifest_sha="$(jq -r ".runs[$index].raw_digests.cluster_manifest_sha256" "$input")"
  if [ "$system" = rhiza ]; then
    pods="$(jq -r ".runs[$index].raw_artifact_paths.runtime_pods" "$input")"
    pods_sha="$(jq -r ".runs[$index].raw_digests.runtime_pods_sha256" "$input")"
    evidence="$(jq -r ".runs[$index].raw_artifact_paths.artifacts" "$input")"
    evidence_sha="$(jq -r ".runs[$index].raw_digests.artifacts_sha256" "$input")"
  else
    pods="$(jq -r ".runs[$index].raw_artifact_paths.live_pods" "$input")"
    pods_sha="$(jq -r ".runs[$index].raw_digests.live_pods_sha256" "$input")"
    evidence="$(jq -r ".runs[$index].raw_artifact_paths.build_provenance" "$input")"
    evidence_sha="$(jq -r ".runs[$index].raw_digests.build_provenance_sha256" "$input")"
  fi
  verify_digest "$bench" "$bench_sha" "$system benchmark"
  verify_digest "$manifest" "$manifest_sha" "$system manifest"
  verify_digest "$pods" "$pods_sha" "$system pods"
  verify_digest "$evidence" "$evidence_sha" "$system provenance"
  normalized="$(jq -c ".runs[$index]" "$input")"
  container="$system"; [ "$system" = hiqlite ] || container=rhiza
  [ "$(yq -r 'select(.kind == "StatefulSet") | .spec.replicas' "$manifest")" = 3 ] || die "$system manifest replica mismatch"
  [ "$(yq -r 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(has("emptyDir")) | .name' "$manifest" | wc -l)" -gt 0 ] || die "$system manifest emptyDir mismatch"
  [ -z "$(yq -r 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(has("persistentVolumeClaim")) | .name' "$manifest")" ] || die "$system manifest PVC mismatch"
  yq -o=json 'select(.kind == "StatefulSet")' "$manifest" | jq -e --arg container "$container" '[.spec.template.spec.containers[] | select(.name == $container) | .resources] == [{"requests":{"cpu":"250m","memory":"512Mi"},"limits":{"cpu":"1000m","memory":"1Gi"}}]' >/dev/null || die "$system manifest resources mismatch"
  jq -e --arg container "$container" --arg image "$image" '[.items[] | .spec.containers[] | select(.name == $container)] as $c | ($c|length)==3 and ($c|all(.[]; .resources.requests.cpu=="250m" and .resources.limits.cpu=="1000m" and .resources.requests.memory=="512Mi" and .resources.limits.memory=="1Gi")) and ($c|map(.imageID // .image)|unique)==[$image]' "$pods" >/dev/null || die "$system live pod topology/resources/image mismatch"
  if [ "$system" = rhiza ]; then
    jq -e --arg image "$image" '.provenance.execution.runtime_images.rhiza | .status == "verified" and .observed_instances == 3 and .image_digests == [$image]' "$evidence" >/dev/null || die 'Rhiza runtime provenance mismatch'
    jq -e --argjson run "$normalized" '
      . as $raw |
      $run.bench.warmup_seconds == $raw.configured.warmup_seconds and
      $run.bench.measure_seconds_requested == $raw.configured.duration_seconds and
      $run.bench.concurrency == $raw.configured.concurrency and
      $run.bench.insert_only == $raw.d1.insert_only and
      $run.bench.unique_request_ids == $raw.d1.unique_request_ids and
      $run.bench.value_ledger_verified == $raw.d1.value_ledger_verified and
      $run.bench.endpoint_count == $raw.d1.endpoint_count and
      $run.bench.endpoint_fallback_attempts == $raw.d1.endpoint_fallback_attempts and
      $run.bench.row_count_delta == $raw.d1.row_count_delta and
      $run.bench.final_consistent_verification == $raw.d1.final_consistent_verification and
      $run.bench.final_id == $raw.d1.final_id and $run.bench.final_value == $raw.d1.final_value and
      $run.bench.measurement == {
        attempts:$raw.measurement.totals.attempts,
        successes:$raw.measurement.totals.successes,
        errors:$raw.measurement.totals.errors
      } and
      $run.performance == {
        logical_ops_per_second:$raw.measurement.totals.successful_committed_transactions_per_second,
        latency_ms:{
          p50:$raw.measurement.totals.latency.p50_ms,
          p95:$raw.measurement.totals.latency.p95_ms,
          p99:$raw.measurement.totals.latency.p99_ms,
          p999:$raw.measurement.totals.latency.p99_9_ms,
          max:([$raw.measurement.totals.latency.histogram_us[] | select(.count > 0) | .upper_bound_us / 1000] | max)
        }
      }
    ' "$bench" >/dev/null || die 'Rhiza normalized benchmark mismatch'
  else
    expected="$(jq -c ".runs[$index].hiqlite_provenance" "$input")"
    jq -e --arg image "$image" --argjson expected "$expected" '.image_digest == $image and del(.image_digest) == $expected' "$evidence" >/dev/null || die 'Hiqlite build provenance mismatch'
    jq -e --argjson run "$normalized" '
      $run.bench == . and
      $run.performance == {
        logical_ops_per_second:.measurement.successes_per_second,
        latency_ms:{
          p50:(.measurement.latency_ns.p50 / 1000000),
          p95:(.measurement.latency_ns.p95 / 1000000),
          p99:(.measurement.latency_ns.p99 / 1000000),
          p999:(.measurement.latency_ns.p999 / 1000000),
          max:(.measurement.latency_ns.max / 1000000)
        }
      }
    ' "$bench" >/dev/null || die 'Hiqlite normalized benchmark mismatch'
  fi
done

jq -e '
  def sha256: type == "string" and test("^[0-9a-f]{64}$");
  def runok: .contract == "D1" and .workload == "d1_sql_unique_request_id_deterministic_write" and .client_path == "public_host_side" and .voters == 3 and .storage == "emptyDir" and .zero_pvc == true and (.image_digest|type == "string" and length > 0) and .resources == {cpu_request:"250m",cpu_limit:"1000m",memory_request:"512Mi",memory_limit:"1Gi"} and (.bench.warmup_seconds == 10 and .bench.measure_seconds_requested == 60 and (.bench.concurrency == 1 or .bench.concurrency == 4 or .bench.concurrency == 16)) and .bench.closed_loop == true and .bench.retries == 0 and .bench.successes_after_valid_response == true and .bench.insert_only == true and .bench.unique_request_ids == true and .bench.value_ledger_verified == true and .bench.measurement.errors == 0 and .bench.measurement.attempts == .bench.measurement.successes and .bench.row_count_delta == .bench.measurement.successes and .bench.final_consistent_verification == true and (.bench.final_id|type == "string" and length > 0) and (.bench.final_value|type == "string" and length == 128) and .telemetry.disk_bytes_per_op == "not_measured" and .telemetry.network_bytes_per_op == "not_measured" and .telemetry.fsync_count == "not_measured" and (if .system == "rhiza" then .durability == "sync" and .bench.endpoint_count == 1 and .bench.endpoint_fallback_attempts == 0 and (.raw_artifact_paths.artifacts|type == "string") else .system == "hiqlite" and .durability == "Immediate" and (.raw_artifact_paths.build_provenance|type == "string") and .hiqlite_provenance.release == "0.14.0" and .hiqlite_provenance.commit == "c8316c53799c509990475ea8e2aa2ef8679e070e" and .hiqlite_provenance.openraft == "0.9.24" and .hiqlite_provenance.log_sync == "Immediate" and (.hiqlite_provenance.source_build == "exact-source-build" or .hiqlite_provenance.source_build == "verified-local-exact-source-reuse") and (.hiqlite_provenance.cargo_lock_sha256|sha256) and (.hiqlite_provenance.proxy_patch_sha256|sha256) end);
  def stats($xs): ($xs|sort) as $v | {median:$v[1],iqr:($v[2]-$v[0])};
  def aggregate($runs): {system:$runs[0].system,concurrency:$runs[0].bench.concurrency,runs:($runs|length),throughput:stats($runs|map(.performance.logical_ops_per_second)),latency_ms:{p50:stats($runs|map(.performance.latency_ms.p50)),p95:stats($runs|map(.performance.latency_ms.p95)),p99:stats($runs|map(.performance.latency_ms.p99)),p999:stats($runs|map(.performance.latency_ms.p999)),max:($runs|map(.performance.latency_ms.max)|max)}};
  .schema_version == 1 and .repetitions == 3 and .concurrencies == [1,4,16] and .orders == ["rhiza,hiqlite","hiqlite,rhiza","rhiza,hiqlite"] and (.runs|length == 18) and ([.runs[].system]|sort == ([range(0;9)|"hiqlite"] + [range(0;9)|"rhiza"] | sort)) and ([.runs[] | .run_id]|unique|length == 18) and (.runs|all(runok)) and ([.runs[] | .raw_artifact_paths.bench] | unique | length == 18) and ([.runs[] | .raw_artifact_paths.root] | unique | length == 18) and ([.runs[] | .system] == ["rhiza","hiqlite","hiqlite","rhiza","rhiza","hiqlite","rhiza","hiqlite","hiqlite","rhiza","rhiza","hiqlite","rhiza","hiqlite","hiqlite","rhiza","rhiza","hiqlite"]) and ([.runs[0:6][]|.bench.concurrency] | unique == [1]) and ([.runs[6:12][]|.bench.concurrency] | unique == [4]) and ([.runs[12:18][]|.bench.concurrency] | unique == [16]) and (. as $p | ([$p.runs | group_by([.system,.bench.concurrency])[] | aggregate(.)] | sort_by([.system,.concurrency]) == ($p.aggregates | sort_by([.system,.concurrency])))) and (.runs|all(.performance_publishable == true and .resource_publishable == false and (.publication_blockers|type == "array" and length > 0) and (.performance.logical_ops_per_second|type == "number" and isfinite and . > 0) and (.performance.latency_ms|(.p50,.p95,.p99,.p999,.max)|type == "number" and isfinite and . >= 0)))
' "$input" >/dev/null || die 'D1 evidence rejected: contract, order, aggregate, ledger, or publication evidence mismatch'
