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
    curl -s -H "Authorization: Bearer $token" \
         -H "Accept: application/json" \
         "$endpoint/v1/admin/tuner/metrics" 2>/dev/null
}

fetch_status() {
    local endpoint="$1"
    local token="$2"
    curl -s -H "Authorization: Bearer $token" \
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

    local cluster_id=$(echo "$status_json" | jq -r '.cluster_id // "unknown"')
    local epoch=$(echo "$status_json" | jq -r '.epoch // "unknown"')
    local node_status=$(echo "$status_json" | jq -r '.node // "unknown"')
    local members=$(echo "$status_json" | jq -r '.members // [] | join(", ")')
    local profile=$(echo "$status_json" | jq -r '.execution_profile // "unknown"')

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
        return
    fi

    local error=$(echo "$metrics_json" | jq -r '.error // empty')
    if [[ -n "$error" ]]; then
        echo -e "${YELLOW}⚠ Tuner not available: $error${NC}"
        return
    fi

    local total_samples=$(echo "$metrics_json" | jq -r '.total_samples // 0')
    local is_fresh=$(echo "$metrics_json" | jq -r '.is_fresh // false')
    local cold_start_passed=$(echo "$metrics_json" | jq -r '.cold_start_gates_passed // false')

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
    local total_samples=$(echo "$metrics_json" | jq -r '.total_samples // 0')
    local cold_start_passed=$(echo "$metrics_json" | jq -r '.cold_start_gates_passed // false')

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
        STATUS=$(fetch_status "$ADMIN_ENDPOINT" "$TOKEN")
        print_cluster_info "$STATUS"
        METRICS=$(fetch_metrics "$ADMIN_ENDPOINT" "$TOKEN")
        print_tuner_metrics "$METRICS"
        print_recommendations "$METRICS"
        echo -e "${BLUE}Last updated: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
        sleep 5
    done
else
    STATUS=$(fetch_status "$ADMIN_ENDPOINT" "$TOKEN")
    print_cluster_info "$STATUS"
    METRICS=$(fetch_metrics "$ADMIN_ENDPOINT" "$TOKEN")
    print_tuner_metrics "$METRICS"
    print_recommendations "$METRICS"
fi
