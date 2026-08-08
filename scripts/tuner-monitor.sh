#!/usr/bin/env bash
# tuner-monitor.sh — Monitor rhiza MAB tuner metrics from a running cluster.
#
# Usage:
#   ./scripts/tuner-monitor.sh [ADMIN_ENDPOINT] [TOKEN]
#
# Example:
#   ./scripts/tuner-monitor.sh http://localhost:8080 my-admin-token
#   ./scripts/tuner-monitor.sh http://node-0:8080 my-admin-token --watch

set -euo pipefail

ADMIN_ENDPOINT="${1:-http://localhost:8080}"
TOKEN="${2:-}"
WATCH_MODE="${3:-}"

if [[ -z "$TOKEN" ]]; then
    echo "Usage: $0 <ADMIN_ENDPOINT> <TOKEN> [--watch]"
    echo ""
    echo "Examples:"
    echo "  $0 http://localhost:8080 my-admin-token"
    echo "  $0 http://node-0:8080 my-admin-token --watch"
    exit 1
fi

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

fetch_metrics() {
    local endpoint="$1"
    local token="$2"
    curl -sS -H "Authorization: Bearer $token" \
         -H "Accept: application/json" \
         "$endpoint/v1/admin/tuner/metrics" 2>/dev/null
}

fetch_status() {
    local endpoint="$1"
    local token="$2"
    curl -sS -H "Authorization: Bearer $token" \
         -H "Accept: application/json" \
         "$endpoint/v1/admin/membership/status" 2>/dev/null
}

print_header() {
    echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${BLUE}║           rhiza MAB Tuner Performance Monitor               ║${NC}"
    echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_cluster_info() {
    local status_json="$1"
    if [[ -z "$status_json" ]]; then
        echo -e "${RED}✗ Cannot fetch cluster status${NC}"
        return
    fi

    local cluster_id epoch node_status members profile
    if ! jq -e 'type == "object"' <<<"$status_json" >/dev/null ||
       ! cluster_id=$(jq -r '.cluster_id // "unknown"' <<<"$status_json") ||
       ! epoch=$(jq -r '.epoch // "unknown"' <<<"$status_json") ||
       ! node_status=$(jq -r '.node // "unknown"' <<<"$status_json") ||
       ! members=$(jq -r '.members // [] | join(", ")' <<<"$status_json") ||
       ! profile=$(jq -r '.execution_profile // "unknown"' <<<"$status_json"); then
        echo -e "${RED}✗ Invalid cluster status response${NC}"
        return
    fi

    echo -e "${CYAN}Cluster:${NC} $cluster_id"
    echo -e "${CYAN}Epoch:${NC} $epoch"
    echo -e "${CYAN}Profile:${NC} $profile"
    echo -e "${CYAN}Node:${NC} $node_status"
    echo -e "${CYAN}Members:${NC} $members"
    echo ""
}

print_tuner_metrics() {
    local metrics_json="$1"
    if [[ -z "$metrics_json" ]]; then
        echo -e "${RED}✗ Cannot fetch tuner metrics${NC}"
        return 1
    fi

    local error total_samples is_fresh cold_start_passed
    if ! jq -e '
        type == "object" and
        (
          (has("error") and (.error | type == "string" and length > 0)) or
          (
            has("total_samples") and
            has("is_fresh") and
            has("cold_start_gates_passed") and
            (.total_samples |
              type == "number" and . >= 0 and . <= 9007199254740991 and floor == .) and
            (.is_fresh | type == "boolean") and
            (.cold_start_gates_passed | type == "boolean")
          )
        )
    ' <<<"$metrics_json" >/dev/null ||
       ! error=$(jq -r '.error // empty' <<<"$metrics_json") ||
       ! total_samples=$(jq -r '.total_samples // 0' <<<"$metrics_json") ||
       ! is_fresh=$(jq -r '.is_fresh // false' <<<"$metrics_json") ||
       ! cold_start_passed=$(jq -r '.cold_start_gates_passed // false' <<<"$metrics_json"); then
        echo -e "${RED}✗ Invalid tuner metrics response${NC}"
        return 1
    fi
    if [[ -n "$error" ]]; then
        echo -e "${YELLOW}⚠ Tuner not available: $error${NC}"
        return 1
    fi

    echo -e "${BOLD}${GREEN}┌─ Telemetry Collector ────────────────────────────────────────┐${NC}"
    printf "│ %-30s %28s │\n" "Total Samples:" "$total_samples"

    if [[ "$is_fresh" == "true" ]]; then
        printf "│ %-30s %28s │\n" "Data Freshness:" "${GREEN}FRESH${NC}"
    else
        printf "│ %-30s %28s │\n" "Data Freshness:" "${RED}STALE${NC}"
    fi

    if [[ "$cold_start_passed" == "true" ]]; then
        printf "│ %-30s %28s │\n" "Cold Start Gate:" "${GREEN}PASSED${NC}"
    else
        printf "│ %-30s %28s │\n" "Cold Start Gate:" "${YELLOW}COLLECTING${NC}"
    fi

    echo -e "${BOLD}${GREEN}└──────────────────────────────────────────────────────────────┘${NC}"
    echo ""
}

print_recommendations() {
    local metrics_json="$1"
    local total_samples cold_start_passed
    if ! total_samples=$(jq -r '.total_samples // 0' <<<"$metrics_json") ||
       ! cold_start_passed=$(jq -r '.cold_start_gates_passed // false' <<<"$metrics_json"); then
        echo -e "${RED}✗ Invalid tuner metrics response${NC}"
        return
    fi

    echo -e "${BOLD}${YELLOW}┌─ Recommendations ────────────────────────────────────────────┐${NC}"

    if [[ "$cold_start_passed" == "false" ]]; then
        local remaining=$((100 - total_samples))
        if [[ $remaining -gt 0 ]]; then
            printf "│ ${YELLOW}⏳ Collect %d more samples to pass cold-start gate${NC}%*s│\n" "$remaining" $((28 - ${#remaining})) ""
        fi
    else
        printf "│ ${GREEN}✓ Tuner is active and learning${NC}%*s│\n" 32 ""
    fi

    if [[ "$total_samples" -gt 1000 ]]; then
        printf "│ ${GREEN}✓ Sufficient data for reliable tuning${NC}%*s│\n" 24 ""
    fi

    echo -e "${BOLD}${YELLOW}└──────────────────────────────────────────────────────────────┘${NC}"
    echo ""
}

# Main
print_header

if [[ "$WATCH_MODE" == "--watch" ]]; then
    echo -e "${CYAN}Watching tuner metrics (Ctrl+C to stop)...${NC}"
    echo ""
    while true; do
        tput clear
        print_header
        STATUS=$(fetch_status "$ADMIN_ENDPOINT" "$TOKEN") || STATUS=""
        print_cluster_info "$STATUS"
        METRICS=$(fetch_metrics "$ADMIN_ENDPOINT" "$TOKEN") || METRICS=""
        if print_tuner_metrics "$METRICS"; then
            print_recommendations "$METRICS"
        fi
        echo -e "${BLUE}Last updated: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
        sleep 5
    done
else
    STATUS=$(fetch_status "$ADMIN_ENDPOINT" "$TOKEN") || STATUS=""
    print_cluster_info "$STATUS"
    METRICS=$(fetch_metrics "$ADMIN_ENDPOINT" "$TOKEN") || METRICS=""
    if print_tuner_metrics "$METRICS"; then
        print_recommendations "$METRICS"
    fi
fi
