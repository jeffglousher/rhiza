#!/bin/sh
# Manual release gate: fresh-state wire interoperability only.
set -eu

die() {
  printf '%s\n' "e2e mixed wire: $*" >&2
  exit 1
}

command -v nc >/dev/null 2>&1 || die "macOS/BSD nc is required to verify the stopped recorder port"
command -v base64 >/dev/null 2>&1 || die "base64 is required for the KV profile"

repo_root=$(unset CDPATH; cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"

tag_object=243565bf7c0288133ec23d47fb6e592564acc040
tag_commit=0cd547be53fdec89e74c7fca232ca412ccdf5143
current_head=$(git rev-parse HEAD)
pids=
cell_dir=
client_token=
peer_one=
peer_two=
peer_three=
node_one_pid=
node_two_pid=
node_three_pid=

sha256() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    die "shasum or sha256sum is required"
  fi
}

random_token() {
  token=$(LC_ALL=C od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
  [ "${#token}" -eq 32 ] || die "cannot read /dev/urandom"
  printf '%s\n' "$token"
}

free_kib=$(df -Pk "${TMPDIR:-/tmp}" | awk 'NR == 2 { print $4 }')
case "$free_kib" in ''|*[!0-9]*) die "cannot determine temporary filesystem free space" ;; esac
[ "$free_kib" -ge 1048576 ] || die "requires at least 1 GiB free on the temporary filesystem"

umask 077
tmp=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-mixed-wire.XXXXXX")
chmod 700 "$tmp"

redacted_logs() {
  for log in "$cell_dir/node-1.log" "$cell_dir/node-2.log" "$cell_dir/node-3.log"; do
    [ -f "$log" ] || continue
    printf '%s\n' "--- $(basename "$log") (last 40 lines) ---" >&2
    tail -n 40 "$log" | sed \
      -e "s/$client_token/[redacted]/g" \
      -e "s/$peer_one/[redacted]/g" \
      -e "s/$peer_two/[redacted]/g" \
      -e "s/$peer_three/[redacted]/g" >&2
  done
}

wait_clean_exit() {
  pid=$1
  deadline=$(( $(date +%s) + 30 ))
  while kill -0 "$pid" 2>/dev/null; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 1
  done
  wait "$pid"
}

wait_stopped() {
  pid=$1
  wait_clean_exit "$pid" && return 0
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

stop_live_nodes() {
  [ -n "$pids" ] || return 0
  for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in $pids; do wait_stopped "$pid" || true; done
  pids=
}

stop_live_nodes_clean() {
  [ -n "$pids" ] || return 0
  for pid in $pids; do kill -TERM "$pid" 2>/dev/null || die "a live node exited before clean shutdown"; done
  for pid in $pids; do wait_clean_exit "$pid" || die "a live node did not exit cleanly"; done
  pids=
}

stop_node_two() {
  kill -TERM "$node_two_pid" 2>/dev/null || die "node-2 exited before controlled shutdown"
  wait_clean_exit "$node_two_pid" || die "node-2 did not exit cleanly after TERM"
  node_two_pid=
  pids="$node_one_pid $node_three_pid"
}

cleanup() {
  status=$?
  set +e
  [ "$status" -eq 0 ] || redacted_logs
  stop_live_nodes
  rm -rf "$tmp"
}
trap cleanup 0
trap 'exit 130' 1 2
trap 'exit 143' 15

actual_tag_object=$(git rev-parse 'v0.7.1^{tag}' 2>/dev/null || true)
[ "$actual_tag_object" = "$tag_object" ] || die "v0.7.1 tag object does not match the pinned baseline"
actual_tag_commit=$(git rev-parse 'v0.7.1^{commit}' 2>/dev/null || true)
[ "$actual_tag_commit" = "$tag_commit" ] || die "v0.7.1 commit does not match the pinned baseline"

archive_commit() {
  label=$1
  commit=$2
  destination=$3
  mkdir "$destination"
  git archive --format=tar "$commit" > "$tmp/$label.tar"
  tar -xf "$tmp/$label.tar" -C "$destination"
  rm -f "$tmp/$label.tar"
}

archive_commit tag "$tag_commit" "$tmp/tag-source"
archive_commit current "$current_head" "$tmp/current-source"

printf '%s\n' "mixed wire tag_object=$tag_object tag_commit=$tag_commit current_head=$current_head"
printf '%s\n' "mixed wire Cargo.lock sha256 tag=$(sha256 "$tmp/tag-source/Cargo.lock") current=$(sha256 "$tmp/current-source/Cargo.lock")"

(
  cd "$tmp/tag-source"
  CARGO_TARGET_DIR="$tmp/tag-target" cargo build --locked --offline -p rhiza-cli --bin rhiza \
    --no-default-features --features sql,graph,kv,recorder-postcard-rpc
)
(
  cd "$tmp/current-source"
  CARGO_TARGET_DIR="$tmp/current-target" cargo build --locked --offline -p rhiza-cli --bin rhiza \
    --no-default-features --features sql,graph,kv,recorder-postcard-rpc
)

tag_bin=$tmp/tag-target/debug/rhiza
current_bin=$tmp/current-target/debug/rhiza
[ -x "$tag_bin" ] || die "tag CLI build did not produce rhiza"
[ -x "$current_bin" ] || die "current CLI build did not produce rhiza"

client_token=$(random_token)
peer_one=$(random_token)
peer_two=$(random_token)
peer_three=$(random_token)

assert_live_nodes() {
  for pid in $pids; do kill -0 "$pid" 2>/dev/null || die "a live node exited unexpectedly"; done
}

wait_ready() {
  bin=$1
  url=$2
  deadline=$(( $(date +%s) + 60 ))
  while ! "$bin" health --url "$url" --ready >/dev/null 2>&1; do
    assert_live_nodes
    [ "$(date +%s)" -lt "$deadline" ] || die "timed out waiting for $url readiness"
    sleep 1
  done
}

wait_recorder_port() {
  port=$1
  deadline=$(( $(date +%s) + 60 ))
  while ! nc -z -w 1 127.0.0.1 "$port" >/dev/null 2>&1; do
    assert_live_nodes
    [ "$(date +%s)" -lt "$deadline" ] || die "timed out waiting for recorder port $port"
    sleep 1
  done
}

wait_node_ready() {
  wait_ready "$1" "$2"
  wait_recorder_port "$3"
}

assert_node_stopped() {
  bin=$1
  url=$2
  recorder_port=$3
  node_id=$4
  if "$bin" health --url "$url" --ready >/dev/null 2>&1 \
    || nc -z -w 1 127.0.0.1 "$recorder_port" >/dev/null 2>&1; then
    die "stopped $node_id remains reachable"
  fi
}

start_node() {
  binary=$1
  node_id=$2
  client_port=$3
  recorder_port=$4
  transport=$5
  log=$6
  env -i PATH="$PATH" \
    RHIZA_EXECUTION_PROFILE="$profile" \
    RHIZA_CLUSTER_ID="mixed-wire-v071-$profile-$transport" \
    RHIZA_NODE_ID="$node_id" \
    RHIZA_DATA_DIR="$cell_dir/$node_id-data" \
    RHIZA_EPOCH=1 \
    RHIZA_RECOVERY_GENERATION=1 \
    RHIZA_CONFIG_BUNDLE_FILE="$cell_dir/config.json" \
    RHIZA_CLIENT_TOKEN="$client_token" \
    RHIZA_CLIENT_LISTEN="127.0.0.1:$client_port" \
    RHIZA_RECORDER_TRANSPORT="$transport" \
    RHIZA_RECORDER_TCP_LISTEN="127.0.0.1:$recorder_port" \
    RHIZA_RECORDER_TLS=off \
    "$binary" serve > "$log" 2>&1 &
  pid=$!
  pids="$pids $pid"
  case "$node_id" in
    node-1) node_one_pid=$pid ;;
    node-2) node_two_pid=$pid ;;
    node-3) node_three_pid=$pid ;;
    *) die "unexpected node id" ;;
  esac
}

base64_text() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

assert_contains() {
  profile_output=$1
  profile_token=$2
  profile_context=$3
  printf '%s\n' "$profile_output" | grep -F "$profile_token" >/dev/null \
    || die "$profile_context did not contain its exact expected value"
}

profile_read_barrier() {
  profile_bin=$1
  profile_url=$2
  profile_key=$3
  case "$profile" in
    sql)
      "$profile_bin" read --url "$profile_url" --token "$client_token" \
        --key "$profile_key" --consistency read_barrier >/dev/null
      ;;
    kv)
      "$profile_bin" kv get --url "$profile_url" --token "$client_token" \
        --key-base64 "$(base64_text "$profile_key")" --consistency read_barrier >/dev/null
      ;;
    graph)
      "$profile_bin" graph query --url "$profile_url" --token "$client_token" \
        --cypher 'RETURN 1 AS value LIMIT 1' --consistency read_barrier --max-rows 1 >/dev/null
      ;;
    *) die "unexpected execution profile" ;;
  esac
}

profile_write() {
  profile_bin=$1
  profile_url=$2
  profile_request_id=$3
  profile_key=$4
  profile_value=$5
  case "$profile" in
    sql)
      "$profile_bin" write --url "$profile_url" --token "$client_token" \
        --request-id "$profile_request_id" --key "$profile_key" --value "$profile_value"
      ;;
    kv)
      "$profile_bin" kv put --url "$profile_url" --token "$client_token" \
        --request-id "$profile_request_id" --key-base64 "$(base64_text "$profile_key")" \
        --value-base64 "$(base64_text "$profile_value")"
      ;;
    graph)
      "$profile_bin" graph put-document --url "$profile_url" --token "$client_token" \
        --request-id "$profile_request_id" --id "$profile_key" \
        --value-json "{\"type\":\"string\",\"value\":\"$profile_value\"}"
      ;;
    *) die "unexpected execution profile" ;;
  esac
}

profile_assert() {
  profile_bin=$1
  profile_url=$2
  profile_key=$3
  profile_value=$4
  case "$profile" in
    sql)
      "$profile_bin" read --url "$profile_url" --token "$client_token" \
        --key "$profile_key" --consistency read_barrier --expect "$profile_value"
      ;;
    kv)
      profile_output=$("$profile_bin" kv get --url "$profile_url" --token "$client_token" \
        --key-base64 "$(base64_text "$profile_key")" --consistency read_barrier)
      assert_contains "$profile_output" "\"value\":\"$(base64_text "$profile_value")\"" "KV read"
      ;;
    graph)
      profile_output=$("$profile_bin" graph query --url "$profile_url" --token "$client_token" \
        --cypher "MATCH (v:RhizaDocument) WHERE v.id = '$profile_key' RETURN v.string_value LIMIT 1" \
        --consistency read_barrier --max-rows 1)
      assert_contains "$profile_output" "\"rows\":[[{\"type\":\"string\",\"value\":\"$profile_value\"}]]" "Graph query"
      ;;
    *) die "unexpected execution profile" ;;
  esac
}

run_cell() {
  profile=$1
  transport=$2
  offset=$3
  base_port=$(( 20000 + ($$ % 4000) * 10 + offset ))
  old_client_port=$base_port
  middle_client_port=$(( base_port + 1 ))
  current_client_port=$(( base_port + 2 ))
  old_recorder_port=$(( base_port + 3 ))
  middle_recorder_port=$(( base_port + 4 ))
  current_recorder_port=$(( base_port + 5 ))
  old_url=http://127.0.0.1:$old_client_port
  middle_url=http://127.0.0.1:$middle_client_port
  current_url=http://127.0.0.1:$current_client_port
  cell_dir=$tmp/$profile-$transport
  mkdir "$cell_dir"

  cat > "$cell_dir/config.json" <<EOF
{"config_id":1,"members":[{"node_id":"node-1","url":"$old_url","log_url":"$old_url","recorder_tcp_addr":"127.0.0.1:$old_recorder_port","token":"$peer_one"},{"node_id":"node-2","url":"$middle_url","log_url":"$middle_url","recorder_tcp_addr":"127.0.0.1:$middle_recorder_port","token":"$peer_two"},{"node_id":"node-3","url":"$current_url","log_url":"$current_url","recorder_tcp_addr":"127.0.0.1:$current_recorder_port","token":"$peer_three"}]}
EOF

  start_node "$tag_bin" node-1 "$old_client_port" "$old_recorder_port" "$transport" "$cell_dir/node-1.log"
  start_node "$current_bin" node-2 "$middle_client_port" "$middle_recorder_port" "$transport" "$cell_dir/node-2.log"
  start_node "$current_bin" node-3 "$current_client_port" "$current_recorder_port" "$transport" "$cell_dir/node-3.log"
  wait_node_ready "$tag_bin" "$old_url" "$old_recorder_port"
  wait_node_ready "$current_bin" "$middle_url" "$middle_recorder_port"
  wait_node_ready "$current_bin" "$current_url" "$current_recorder_port"
  stop_node_two
  assert_node_stopped "$current_bin" "$middle_url" "$middle_recorder_port" node-2
  wait_node_ready "$tag_bin" "$old_url" "$old_recorder_port"
  wait_node_ready "$current_bin" "$current_url" "$current_recorder_port"

  assert_live_nodes
  profile_read_barrier "$tag_bin" "$old_url" "preflight-$profile-$transport"
  profile_read_barrier "$current_bin" "$current_url" "preflight-$profile-$transport"
  profile_write "$current_bin" "$old_url" "current-to-old-$profile-$transport" \
    "current-$profile-$transport" current
  profile_assert "$current_bin" "$old_url" "current-$profile-$transport" current
  profile_write "$tag_bin" "$current_url" "old-to-current-$profile-$transport" \
    "old-$profile-$transport" old
  profile_assert "$tag_bin" "$current_url" "old-$profile-$transport" old
  assert_live_nodes
  stop_live_nodes_clean
  assert_node_stopped "$tag_bin" "$old_url" "$old_recorder_port" node-1
  assert_node_stopped "$current_bin" "$current_url" "$current_recorder_port" node-3

  start_node "$current_bin" node-1 "$old_client_port" "$old_recorder_port" "$transport" "$cell_dir/node-1.log"
  start_node "$current_bin" node-2 "$middle_client_port" "$middle_recorder_port" "$transport" "$cell_dir/node-2.log"
  start_node "$tag_bin" node-3 "$current_client_port" "$current_recorder_port" "$transport" "$cell_dir/node-3.log"
  wait_node_ready "$current_bin" "$old_url" "$old_recorder_port"
  wait_node_ready "$current_bin" "$middle_url" "$middle_recorder_port"
  wait_node_ready "$tag_bin" "$current_url" "$current_recorder_port"
  profile_read_barrier "$current_bin" "$old_url" "restart-preflight-$profile-$transport"
  profile_read_barrier "$current_bin" "$middle_url" "restart-preflight-$profile-$transport"
  profile_read_barrier "$tag_bin" "$current_url" "restart-preflight-$profile-$transport"

  stop_node_two
  assert_node_stopped "$current_bin" "$middle_url" "$middle_recorder_port" node-2
  wait_node_ready "$current_bin" "$old_url" "$old_recorder_port"
  wait_node_ready "$tag_bin" "$current_url" "$current_recorder_port"

  profile_assert "$current_bin" "$old_url" "current-$profile-$transport" current
  profile_assert "$current_bin" "$old_url" "old-$profile-$transport" old
  profile_assert "$tag_bin" "$current_url" "current-$profile-$transport" current
  profile_assert "$tag_bin" "$current_url" "old-$profile-$transport" old
  profile_write "$current_bin" "$old_url" "current-restart-$profile-$transport" \
    "current-restart-$profile-$transport" current-restart
  profile_assert "$current_bin" "$old_url" "current-restart-$profile-$transport" current-restart
  profile_write "$tag_bin" "$current_url" "old-restart-$profile-$transport" \
    "old-restart-$profile-$transport" old-restart
  profile_assert "$tag_bin" "$current_url" "old-restart-$profile-$transport" old-restart
  stop_live_nodes_clean
  cell_dir=
}

run_cell sql tcp-postcard 0
run_cell sql tcp-postcard-rpc 20
run_cell kv tcp-postcard 40
run_cell graph tcp-postcard 60
printf '%s\n' 'mixed wire v0.7.1/current fresh-state interoperability passed'
