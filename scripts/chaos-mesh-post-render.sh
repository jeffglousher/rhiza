#!/usr/bin/env bash
# Helm post-renderer: replace only the exact reviewed Chaos Mesh image tags.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"; lock="$root/deploy/chaos/chaos-mesh.lock.json"
command -v jq >/dev/null || { echo 'chaos-mesh-post-render: jq is required' >&2; exit 1; }
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
cat > "$tmp"
while IFS=$'\t' read -r ref digest; do
  repo="${ref%:*}"
  sed -i.bak "s|$ref|$repo@$digest|g" "$tmp"; rm -f "$tmp.bak"
done < <(jq -r '.images[] | [.reference,.digest] | @tsv' "$lock")
cat "$tmp"
