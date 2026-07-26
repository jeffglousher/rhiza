#!/usr/bin/env bash
set -euo pipefail

profile="${RHIZA_EXECUTION_PROFILE-}"
case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac

die() {
  echo "$*" >&2
  exit 65
}

case "${1-}" in
  prepare)
    [ "$#" -eq 6 ] || die "usage: $0 prepare STATE OLD_ID NEW_ID SUCCESSOR_JSON CANDIDATE_OPERATION_ID"
    state_file="$2"
    old_id="$3"
    new_id="$4"
    successor="$5"
    candidate="$6"
    case "$old_id:$new_id" in
      *[!0-9:]*|:*|*:) die "configuration ids must be positive integers" ;;
    esac
    if [ "$old_id" -le 0 ] || [ "$new_id" -le 0 ]; then
      die "configuration ids must be positive integers"
    fi
    [ -n "$candidate" ] || die "candidate Stop operation id must not be empty"
    jq -e --argjson new "$new_id" '
      (keys | sort) == ["config_id", "digest", "members"] and
      .config_id == $new and
      (.members | type == "array" and length >= 3 and
        all(type == "string" and length > 0)) and
      (.digest | type == "array" and length == 32 and
        all(type == "number" and floor == . and . >= 0 and . <= 255))
    ' <<< "$successor" >/dev/null || die "invalid successor descriptor"

    if [ -e "$state_file" ]; then
      [ -s "$state_file" ] || die "Stop state file is empty"
      jq -e --argjson old "$old_id" --argjson new "$new_id" \
        --argjson successor "$successor" '
        .version == 1 and .old_config_id == $old and .new_config_id == $new and
        (.operation_id | type == "string" and length > 0) and
        .successor == $successor
      ' "$state_file" >/dev/null || die "existing Stop state does not match requested transition"
    else
      state_attempt="${state_file}.attempt.$$"
      trap 'rm -f "$state_attempt"' EXIT
      umask 077
      jq -n --argjson old "$old_id" --argjson new "$new_id" \
        --arg operation "$candidate" --argjson successor "$successor" '
        {version:1, old_config_id:$old, new_config_id:$new,
         operation_id:$operation, successor:$successor}
      ' > "$state_attempt"
      chmod 600 "$state_attempt"
      mv "$state_attempt" "$state_file"
      trap - EXIT
    fi
    jq -er '.operation_id' "$state_file"
    ;;
  validate)
    [ "$#" -eq 3 ] || die "usage: $0 validate STATE STOP_RESPONSE_JSON"
    state_file="$2"
    response_file="$3"
    [ -s "$state_file" ] || die "Stop state is unavailable"
    [ -s "$response_file" ] || die "Stop response is unavailable"
    old_id="$(jq -er '.old_config_id' "$state_file")"
    operation="$(jq -er '.operation_id' "$state_file")"
    successor="$(jq -ec '.successor' "$state_file")"
    jq -e --argjson old "$old_id" --arg operation "$operation" \
      --argjson successor "$successor" '
      .operation_id == $operation and
      (.stop | keys | sort) == ["entry", "proof"] and
      .stop.entry.config_id == $old and .stop.proof != null and
      .successor == $successor
    ' "$response_file" >/dev/null || die "Stop response does not match persisted Stop state"
    ;;
  write-bundle)
    [ "$#" -eq 5 ] || die "usage: $0 write-bundle STOP_RESPONSE OLD_BUNDLE SUCCESSOR_DRAFT OUTPUT"
    response_file="$2"
    old_bundle="$3"
    successor_draft="$4"
    output_file="$5"
    [ -s "$response_file" ] || die "Stop response is unavailable"
    [ -s "$old_bundle" ] || die "old configuration bundle is unavailable"
    [ -s "$successor_draft" ] || die "successor draft is unavailable"
    old_id="$(jq -er '.config_id' "$old_bundle")"
    new_id="$(jq -er '.config_id' "$successor_draft")"
    jq -e --argjson old "$old_id" --argjson new "$new_id" '
      .operation_id != null and
      (.stop | keys | sort) == ["entry", "proof"] and
      .stop.entry.config_id == $old and .stop.proof != null and
      .successor.config_id == $new
    ' "$response_file" >/dev/null || die "Stop response cannot produce the successor bundle"
    bundle_attempt="${output_file}.attempt.$$"
    trap 'rm -f "$bundle_attempt"' EXIT
    umask 077
    jq --slurpfile stopped "$response_file" --slurpfile old "$old_bundle" '
      . + {predecessor: {
        members: [$old[0].members[].node_id],
        stop_entry: $stopped[0].stop.entry,
        stop_proof: $stopped[0].stop.proof
      }}
    ' "$successor_draft" > "$bundle_attempt"
    jq -e --argjson new "$new_id" --slurpfile stopped "$response_file" \
      --slurpfile old "$old_bundle" '
      .config_id == $new and
      .predecessor.members == [$old[0].members[].node_id] and
      .predecessor.stop_entry == $stopped[0].stop.entry and
      .predecessor.stop_proof == $stopped[0].stop.proof
    ' "$bundle_attempt" >/dev/null || die "generated successor bundle is invalid"
    chmod 600 "$bundle_attempt"
    mv "$bundle_attempt" "$output_file"
    trap - EXIT
    ;;
  hydrate)
    [ "$#" -eq 6 ] || die "usage: $0 hydrate SECRET_JSON OLD_BUNDLE SUCCESSOR_DRAFT STOP_OUTPUT BUNDLE_OUTPUT"
    secret_file="$2"
    old_bundle="$3"
    successor_draft="$4"
    stop_output="$5"
    bundle_output="$6"
    [ -s "$secret_file" ] || die "durable transition Secret is unavailable"
    stop_attempt="${stop_output}.attempt.$$"
    bundle_attempt="${bundle_output}.attempt.$$"
    trap 'rm -f "$stop_attempt" "$bundle_attempt"' EXIT
    umask 077
    jq -er '.data["stop.json"] | select(type == "string" and length > 0) |
      @base64d | fromjson' "$secret_file" \
      > "$stop_attempt" || die "durable transition Secret has no valid Stop response"
    jq -er '.data["config.json"] | select(type == "string" and length > 0) |
      @base64d | fromjson' "$secret_file" \
      > "$bundle_attempt" || die "durable transition Secret has no valid successor bundle"
    old_id="$(jq -er '.config_id' "$old_bundle")"
    new_id="$(jq -er '.config_id' "$successor_draft")"
    jq -e --argjson old "$old_id" --argjson new "$new_id" \
      --slurpfile stop "$stop_attempt" --slurpfile old_bundle "$old_bundle" \
      --slurpfile draft "$successor_draft" '
      .config_id == $new and
      (del(.predecessor) == $draft[0]) and
      .predecessor.members == [$old_bundle[0].members[].node_id] and
      .predecessor.stop_entry == $stop[0].stop.entry and
      .predecessor.stop_proof == $stop[0].stop.proof and
      ($stop[0].operation_id | type == "string" and length > 0) and
      ($stop[0].stop | keys | sort) == ["entry", "proof"] and
      $stop[0].stop.entry.config_id == $old and $stop[0].stop.proof != null and
      $stop[0].successor.config_id == $new
    ' "$bundle_attempt" >/dev/null || die "durable transition Secret is inconsistent"
    chmod 600 "$stop_attempt" "$bundle_attempt"
    mv "$stop_attempt" "$stop_output"
    mv "$bundle_attempt" "$bundle_output"
    trap - EXIT
    ;;
  recover)
    [ "$#" -eq 4 ] || die "usage: $0 recover STATE STATUS_JSON STOP_RESPONSE_JSON"
    state_file="$2"
    status_file="$3"
    response_file="$4"
    [ -s "$state_file" ] || die "Stop state is unavailable"
    [ -s "$status_file" ] || die "admin status is unavailable"
    old_id="$(jq -er '.old_config_id' "$state_file")"
    operation="$(jq -er '.operation_id' "$state_file")"
    successor="$(jq -ec '.successor' "$state_file")"
    if ! jq -e '
      .node.configuration_status == "stopped" and .stopped_transition != null
    ' "$status_file" >/dev/null; then
      exit 1
    fi
    jq -e --argjson old "$old_id" --argjson successor "$successor" '
      .node.configuration_status == "stopped" and
      .node.active_config_id == $old and
      .node.configuration_state.phase == "stopped" and
      (.stopped_transition.stop | keys | sort) == ["entry", "proof"] and
      .stopped_transition.stop.entry.config_id == $old and
      .stopped_transition.stop.proof != null and
      .stopped_transition.successor == $successor
    ' "$status_file" >/dev/null || die "stopped transition does not match persisted Stop state"
    response_attempt="${response_file}.attempt.$$"
    trap 'rm -f "$response_attempt"' EXIT
    umask 077
    jq --arg operation "$operation" '
      {operation_id:$operation, stop:.stopped_transition.stop,
       successor:.stopped_transition.successor}
    ' "$status_file" > "$response_attempt"
    chmod 600 "$response_attempt"
    mv "$response_attempt" "$response_file"
    trap - EXIT
    ;;
  *)
    die "usage: $0 prepare|validate|write-bundle|hydrate|recover ..."
    ;;
esac
