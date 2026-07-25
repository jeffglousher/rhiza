#!/usr/bin/env python3
"""Run seeded, process-isolated RecorderRpc transport pair experiments."""

import argparse
import hashlib
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
from pathlib import Path


STRATA = {"plaintext": ("tcp-postcard", "tcp-rkyv")}
CELLS = (
    "identity",
    "store_command/payload-0",
    "store_command/payload-128",
    "store_command/payload-4096",
    "fetch_command/payload-0",
    "fetch_command/payload-128",
    "fetch_command/payload-4096",
    "record/payload-0",
    "record/payload-128",
    "record/payload-4096",
    "install_decision_proof",
    "inspect_decision_proof",
    "inspect_record_summary",
    "observe_read_fence",
)
WIRE_VERSIONS = {"tcp-postcard": 3, "tcp-rkyv": 6}
CODECS = {"tcp-postcard": "postcard", "tcp-rkyv": "rkyv"}
RATIO_FIELDS = (
    "attempt_throughput_per_second",
    "success_throughput_per_second",
    "successful_latency_p50_us",
    "successful_latency_p95_us",
    "successful_latency_p99_us",
)
MAX_DISTINCT_ERROR_MESSAGES = 8
BLOCKS = ("AB", "BA", "AA", "BB")


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("requires a positive integer")
    return parsed


def nonnegative_int(value):
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("requires a non-negative integer")
    return parsed


def positive_float(value):
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("requires a positive number")
    return parsed


def positive_csv(value):
    values = tuple(int(item) for item in value.split(","))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("requires comma-separated positive integers")
    return values


def canonical_cells(value):
    values = tuple(value.split(","))
    if not values or any(item not in CELLS for item in values):
        raise argparse.ArgumentTypeError("contains an unknown or non-canonical cell id")
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("cell ids must be unique")
    return values


def security_csv(value):
    values = tuple(value.split(","))
    if not values or any(item not in STRATA for item in values):
        raise argparse.ArgumentTypeError("only plaintext is supported")
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("security strata must be unique")
    return values


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def block_candidates(stratum, block):
    a, b = STRATA[stratum]
    return {"AB": (a, b), "BA": (b, a), "AA": (a, a), "BB": (b, b)}[block]


def make_schedule(stratum, pairs, seed):
    rng = random.Random(f"{seed}:{stratum}")
    schedule = []
    for pair in range(1, pairs + 1):
        blocks = list(BLOCKS)
        rng.shuffle(blocks)
        for block_position, block in enumerate(blocks, start=1):
            schedule.append(
                {
                    "pair": pair,
                    "block": block,
                    "block_position": block_position,
                    "candidates": list(block_candidates(stratum, block)),
                }
            )
    return schedule


def validate_report(
    report,
    candidate,
    min_attempts,
    warmup,
    min_duration_ms,
    concurrencies,
    cells,
):
    errors = []
    conditions = report.get("conditions", {})
    if report.get("schema_version") != 3:
        errors.append("schema_version must be 3")
    if report.get("production_valid") is not True:
        errors.append("production_valid is false")
    if conditions.get("candidates") != [candidate]:
        errors.append("raw process is not exclusive to the requested candidate")
    if conditions.get("cells") != list(cells):
        errors.append("cell list mismatch")
    if conditions.get("concurrency") != list(concurrencies):
        errors.append("concurrency list mismatch")
    if conditions.get("minimum_attempts_per_metric") != min_attempts:
        errors.append("minimum attempt metadata mismatch")
    if conditions.get("minimum_duration_ms_per_metric") != min_duration_ms:
        errors.append("minimum duration metadata mismatch")
    if conditions.get("recorder_server_operation_cap") != 32:
        errors.append("recorder server operation cap metadata mismatch")
    if conditions.get("connections_prewarmed_per_lane") != 2:
        errors.append("prewarmed connection count metadata mismatch")
    if "exactly two persistent connections per lane" not in conditions.get(
        "topology_invariant", ""
    ):
        errors.append("two-connections-per-lane topology metadata is missing")

    rows = report.get("metrics", [])
    expected = {(candidate, cell, concurrency) for cell in cells for concurrency in concurrencies}
    actual = {
        (row.get("candidate"), row.get("cell_id"), row.get("concurrency"))
        for row in rows
    }
    if len(rows) != len(expected) or actual != expected:
        errors.append("metric row set is incomplete or duplicated")
    for row in rows:
        key = (row.get("candidate"), row.get("cell_id"), row.get("concurrency"))
        attempts = row.get("attempts")
        successes = row.get("successes")
        failures = row.get("errors")
        if not isinstance(attempts, int) or attempts < min_attempts:
            errors.append(f"{key}: minimum attempts not reached")
            continue
        if row.get("minimum_attempts_per_metric") != min_attempts:
            errors.append(f"{key}: minimum attempts metadata mismatch")
        if row.get("wall_seconds", 0) < min_duration_ms / 1000:
            errors.append(f"{key}: minimum wall duration not reached")
        if row.get("minimum_wall_seconds") != min_duration_ms / 1000:
            errors.append(f"{key}: minimum wall metadata mismatch")
        if successes + failures != attempts:
            errors.append(f"{key}: attempt accounting mismatch")
        if failures != sum(row.get("error_classes", {}).values()):
            errors.append(f"{key}: error class accounting mismatch")
        if failures != 0:
            errors.append(f"{key}: measured calls contain failures")
        messages = row.get("error_messages", [])
        if len(messages) > MAX_DISTINCT_ERROR_MESSAGES:
            errors.append(f"{key}: too many error messages retained")
        captured = sum(item.get("count", 0) for item in messages)
        omitted = row.get("unrecorded_error_message_occurrences", 0)
        if failures != captured + omitted:
            errors.append(f"{key}: error message accounting mismatch")
        if row.get("warmup_attempts") != warmup or row.get("warmup_errors") != 0:
            errors.append(f"{key}: warmup mismatch")
        if row.get("lane_prewarm_attempts") != 4 or row.get("lane_prewarm_errors") != 0:
            errors.append(f"{key}: four-call concurrent lane prewarm mismatch")
        if row.get("lane_prewarm_gate_errors") != 0:
            errors.append(f"{key}: backend prewarm gate failed")
        if row.get("diagnostic_valid") is not True:
            errors.append(f"{key}: diagnostic_valid is false")
        if row.get("codec") != CODECS.get(candidate):
            errors.append(f"{key}: codec metadata mismatch")
        if row.get("production_wire_version") != WIRE_VERSIONS.get(candidate):
            errors.append(f"{key}: wire version mismatch")
        if row.get("connections_per_lane") != 2:
            errors.append(f"{key}: connection topology mismatch")
    if report.get("diagnostic_valid") is not True:
        errors.append("raw report diagnostic_valid is false")
    return errors


def row_index(report):
    return {
        (row["cell_id"], row["concurrency"]): row
        for row in report["metrics"]
    }


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def geometric_mean(values):
    values = [value for value in values if value is not None]
    if not values or any(value <= 0 for value in values):
        return None
    return math.exp(sum(math.log(value) for value in values) / len(values))


def bootstrap_median_ci(values, samples, seed):
    if not values:
        return None
    rng = random.Random(seed)
    medians = []
    for _ in range(samples):
        medians.append(statistics.median(rng.choice(values) for _ in values))
    medians.sort()
    low = medians[int(0.025 * (len(medians) - 1))]
    high = medians[int(0.975 * (len(medians) - 1))]
    return {"low": low, "high": high, "samples": samples}


def analyze_pairs(executions, stratum, bootstrap_samples, seed, max_control_drift):
    a, b = STRATA[stratum]
    keys = sorted(row_index(executions[0]["reports"][0]))
    pair_samples = {field: {key: [] for key in keys} for field in RATIO_FIELDS}
    controls = {
        candidate: {
            field: {key: [] for key in keys}
            for field in RATIO_FIELDS
        }
        for candidate in (a, b)
    }

    for pair in sorted({item["pair"] for item in executions}):
        blocks = {
            item["block"]: item
            for item in executions
            if item["pair"] == pair
        }
        if set(blocks) != set(BLOCKS):
            raise ValueError(f"pair {pair} does not contain exactly AB/BA/AA/BB")
        for field in RATIO_FIELDS:
            for key in keys:
                directional = []
                for block in ("AB", "BA"):
                    first, second = blocks[block]["reports"]
                    first_row = row_index(first)[key]
                    second_row = row_index(second)[key]
                    if block == "AB":
                        directional.append(ratio(second_row[field], first_row[field]))
                    else:
                        directional.append(ratio(first_row[field], second_row[field]))
                pair_samples[field][key].append(geometric_mean(directional))
            for candidate, block in ((a, "AA"), (b, "BB")):
                first, second = blocks[block]["reports"]
                first_index, second_index = row_index(first), row_index(second)
                for key in keys:
                    controls[candidate][field][key].append(
                        ratio(second_index[key][field], first_index[key][field])
                    )

    cells = []
    for key in keys:
        cell = {"cell_id": key[0], "concurrency": key[1], "ratios": {}}
        for field in RATIO_FIELDS:
            values = pair_samples[field][key]
            median = statistics.median(values)
            confidence = bootstrap_median_ci(
                values, bootstrap_samples, f"{seed}:{key}:{field}"
            )
            lower_is_better = field.startswith("successful_latency_")
            cell["ratios"][field] = {
                "direction": f"{b} / {a}",
                "pair_first_median": median,
                "percent_delta": (median - 1) * 100,
                "pair_samples": values,
                "bootstrap_median_95_ci": confidence,
                "advantage_detected": (
                    confidence["high"] < 1
                    if lower_is_better
                    else confidence["low"] > 1
                ),
            }
        cells.append(cell)

    control_summary = {}
    controls_valid = True
    for candidate, fields in controls.items():
        control_summary[candidate] = {}
        for field, keyed_values in fields.items():
            control_summary[candidate][field] = []
            for key, values in keyed_values.items():
                median = statistics.median(values)
                sample_drifts = [abs(value - 1) for value in values]
                drift = max(sample_drifts)
                valid = all(
                    sample_drift <= max_control_drift
                    for sample_drift in sample_drifts
                )
                controls_valid &= valid
                confidence = bootstrap_median_ci(
                    values,
                    bootstrap_samples,
                    f"{seed}:control:{candidate}:{field}:{key}",
                )
                control_summary[candidate][field].append(
                    {
                        "cell_id": key[0],
                        "concurrency": key[1],
                        "direction": "second / first",
                        "median": median,
                        "samples": values,
                        "sample_absolute_drifts": sample_drifts,
                        "maximum_absolute_drift": drift,
                        "max_allowed_drift": max_control_drift,
                        "valid": valid,
                        "bootstrap_median_95_ci": confidence,
                        "bootstrap_ci_contains_one": (
                            confidence["low"] <= 1 <= confidence["high"]
                        ),
                    }
                )
    return {
        "cells": cells,
        "same_candidate_controls": control_summary,
        "controls_valid": controls_valid,
    }


def fake_report(candidate, cells=("identity",), concurrencies=(4,), value=1000.0):
    metrics = []
    for cell in cells:
        workload = cell.split("/", 1)[0]
        payload = int(cell.rsplit("-", 1)[1]) if "/payload-" in cell else None
        for concurrency in concurrencies:
            metrics.append(
                {
                    "candidate": candidate,
                    "cell_id": cell,
                    "workload": workload,
                    "payload_bytes": payload,
                    "lane": "consensus" if workload in ("record", "install_decision_proof") else "control",
                    "concurrency": concurrency,
                    "attempts": 100,
                    "minimum_attempts_per_metric": 100,
                    "successes": 100,
                    "errors": 0,
                    "error_classes": {},
                    "error_messages": [],
                    "unrecorded_error_message_occurrences": 0,
                    "warmup_attempts": 20,
                    "warmup_errors": 0,
                    "lane_prewarm_attempts": 4,
                    "lane_prewarm_errors": 0,
                    "lane_prewarm_gate_errors": 0,
                    "wall_seconds": 0.1,
                    "minimum_wall_seconds": 0.1,
                    "diagnostic_valid": True,
                    "codec": CODECS[candidate],
                    "production_wire_version": WIRE_VERSIONS[candidate],
                    "connections_per_lane": 2,
                    **{field: value for field in RATIO_FIELDS},
                }
            )
    return {
        "schema_version": 3,
        "production_valid": True,
        "diagnostic_valid": True,
        "environment": {"git_commit": "test", "git_dirty": False},
        "conditions": {
            "candidates": [candidate],
            "cells": list(cells),
            "concurrency": list(concurrencies),
            "minimum_attempts_per_metric": 100,
            "minimum_duration_ms_per_metric": 100,
            "connections_prewarmed_per_lane": 2,
            "recorder_server_operation_cap": 32,
            "topology_invariant": "exactly two persistent connections per lane",
        },
        "metrics": metrics,
    }


def self_test():
    assert canonical_cells("identity,record/payload-4096") == (
        "identity",
        "record/payload-4096",
    )
    for invalid in ("identity,identity", "record/payload-1", ""):
        try:
            canonical_cells(invalid)
            raise AssertionError(f"accepted invalid cells: {invalid!r}")
        except argparse.ArgumentTypeError:
            pass

    schedule = make_schedule("plaintext", 3, 7)
    for pair in range(1, 4):
        assert {item["block"] for item in schedule if item["pair"] == pair} == set(BLOCKS)
    assert schedule == make_schedule("plaintext", 3, 7)
    assert bootstrap_median_ci([0.9, 1.0, 1.1], 1000, 9) == bootstrap_median_ci(
        [0.9, 1.0, 1.1], 1000, 9
    )

    executions = []
    for item in make_schedule("plaintext", 3, 11):
        reports = []
        for position, candidate in enumerate(item["candidates"]):
            base = 1000.0 if candidate == "tcp-postcard" else 1200.0
            reports.append(
                fake_report(
                    candidate,
                    cells=("identity", "record/payload-0"),
                    value=base * (1 + position * 0.01),
                )
            )
        executions.append({**item, "reports": reports})
    analysis = analyze_pairs(executions, "plaintext", 1000, 11, 0.05)
    observed = analysis["cells"][0]["ratios"]["attempt_throughput_per_second"][
        "pair_first_median"
    ]
    assert 1.19 < observed < 1.21
    assert analysis["controls_valid"]
    drifted = json.loads(json.dumps(executions))
    for execution in drifted:
        if execution["block"] in ("AA", "BB"):
            for first_row, second_row in zip(
                execution["reports"][0]["metrics"],
                execution["reports"][1]["metrics"],
            ):
                for field in RATIO_FIELDS:
                    second_row[field] = first_row[field]
            if execution["pair"] == 1:
                for field in RATIO_FIELDS:
                    execution["reports"][1]["metrics"][0][field] *= 1.25
    drift_analysis = analyze_pairs(drifted, "plaintext", 1000, 11, 0.05)
    assert not drift_analysis["controls_valid"]
    postcard_controls = drift_analysis["same_candidate_controls"]["tcp-postcard"][
        "attempt_throughput_per_second"
    ]
    assert len(postcard_controls) == 2
    assert {control["cell_id"] for control in postcard_controls} == {
        "identity",
        "record/payload-0",
    }
    identity_control = next(
        control for control in postcard_controls if control["cell_id"] == "identity"
    )
    record_control = next(
        control
        for control in postcard_controls
        if control["cell_id"] == "record/payload-0"
    )
    assert identity_control["samples"] == [1.25, 1.0, 1.0]
    assert not identity_control["valid"]
    assert record_control["valid"]
    assert result_exit_code(False, False) == 1
    assert result_exit_code(False, True) == 0
    assert result_exit_code(True, False) == 0

    raw = fake_report("tcp-postcard")
    assert not validate_report(raw, "tcp-postcard", 100, 20, 100, (4,), ("identity",))
    invalid = json.loads(json.dumps(raw))
    invalid["metrics"][0]["attempts"] = 99
    assert validate_report(invalid, "tcp-postcard", 100, 20, 100, (4,), ("identity",))
    mixed = json.loads(json.dumps(raw))
    mixed["conditions"]["candidates"].append("tcp-rkyv")
    assert validate_report(mixed, "tcp-postcard", 100, 20, 100, (4,), ("identity",))
    print("run-recorder-transport self-test: ok")


def result_exit_code(comparison_valid, allow_unpublishable):
    return 0 if comparison_valid or allow_unpublishable else 1


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", type=Path, default=root / "target/release/rhiza-recorder-transport"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=root / "target/recorder-transport-results"
    )
    parser.add_argument("--warmup", type=positive_int, default=1000)
    parser.add_argument("--operations", type=positive_int, default=10000)
    parser.add_argument("--min-duration-ms", type=positive_int, default=250)
    parser.add_argument("--concurrency", type=positive_csv, default=positive_csv("1,4,32"))
    parser.add_argument("--cells", type=canonical_cells, default=CELLS)
    parser.add_argument("--security", type=security_csv, default=("plaintext",))
    parser.add_argument("--pairs", type=positive_int, default=3)
    parser.add_argument("--seed", type=nonnegative_int, default=20260725)
    parser.add_argument("--bootstrap-samples", type=positive_int, default=10000)
    parser.add_argument("--max-control-drift", type=positive_float, default=0.10)
    parser.add_argument(
        "--allow-unpublishable",
        action="store_true",
        help="return success for a diagnostic run even when comparison_valid is false",
    )
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def run_stratum(args, binary, stratum):
    output_dir = args.output_dir / stratum
    output_dir.mkdir(parents=True, exist_ok=True)
    executions = []
    validation_errors = []
    for block_run, item in enumerate(
        make_schedule(stratum, args.pairs, args.seed), start=1
    ):
        reports = []
        raw_files = []
        for process_position, candidate in enumerate(item["candidates"], start=1):
            command = [
                str(binary),
                "--warmup", str(args.warmup),
                "--operations", str(args.operations),
                "--min-duration-ms", str(args.min_duration_ms),
                "--concurrency", ",".join(map(str, args.concurrency)),
                "--cells", ",".join(args.cells),
                "--candidates", candidate,
            ]
            completed = subprocess.run(command, check=False, capture_output=True, text=True)
            raw_path = output_dir / (
                f"pair-{item['pair']:03d}-block-{item['block']}-"
                f"position-{process_position}-{candidate}.json"
            )
            raw_path.write_text(completed.stdout, encoding="utf-8")
            if completed.returncode != 0:
                raise SystemExit(
                    f"{stratum} pair {item['pair']} block {item['block']} "
                    f"{candidate} failed ({completed.returncode}): {completed.stderr.strip()}"
                )
            try:
                report = json.loads(completed.stdout)
            except json.JSONDecodeError as error:
                raise SystemExit(f"{raw_path} emitted invalid JSON: {error}") from error
            validation_errors.extend(
                f"{raw_path.name}: {error}"
                for error in validate_report(
                    report,
                    candidate,
                    args.operations,
                    args.warmup,
                    args.min_duration_ms,
                    args.concurrency,
                    args.cells,
                )
            )
            reports.append(report)
            raw_files.append(str(raw_path.relative_to(args.output_dir)))
        executions.append(
            {
                **item,
                "block_run": block_run,
                "reports": reports,
                "raw_files": raw_files,
            }
        )
    return executions, validation_errors


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary not found: {binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    strata = {}
    all_reports = []
    validation_errors = []
    controls_valid = True
    for stratum in args.security:
        executions, errors = run_stratum(args, binary, stratum)
        reports = [report for execution in executions for report in execution["reports"]]
        all_reports.extend(reports)
        validation_errors.extend(f"{stratum}: {error}" for error in errors)
        analysis = None
        if not errors:
            analysis = analyze_pairs(
                executions,
                stratum,
                args.bootstrap_samples,
                args.seed,
                args.max_control_drift,
            )
            controls_valid &= analysis["controls_valid"]
        strata[stratum] = {
            "ratio_direction": f"{STRATA[stratum][1]} / {STRATA[stratum][0]}",
            "analysis": analysis,
            "schedule": [
                {
                    key: value
                    for key, value in execution.items()
                    if key not in ("reports",)
                }
                for execution in executions
            ],
        }

    first_environment = all_reports[0]["environment"]
    git_commit = first_environment.get("git_commit")
    git_dirty = first_environment.get("git_dirty")
    consistent_git = all(
        report["environment"].get("git_commit") == git_commit
        and report["environment"].get("git_dirty") == git_dirty
        for report in all_reports
    )
    production_valid = not validation_errors and all(
        report.get("production_valid") is True for report in all_reports
    )
    diagnostic_valid = production_valid and consistent_git
    blockers = []
    if validation_errors:
        blockers.append("raw-run validation failed")
    if not consistent_git:
        blockers.append("Git provenance changed between raw processes")
    if git_dirty is not False:
        blockers.append("Git tree is dirty or its state is unknown")
    if not controls_valid:
        blockers.append("AA or BB same-candidate time-drift control exceeded threshold")
    comparison_valid = diagnostic_valid and not blockers
    summary = {
        "schema_version": 3,
        "diagnostic_valid": diagnostic_valid,
        "comparison_valid": comparison_valid,
        "publishable": comparison_valid,
        "production_valid": production_valid,
        "comparison_blockers": blockers,
        "validation_errors": validation_errors,
        "design": {
            "pairs": args.pairs,
            "blocks_per_pair": list(BLOCKS),
            "seed": args.seed,
            "bootstrap_samples": args.bootstrap_samples,
            "max_control_drift": args.max_control_drift,
            "aggregation": "AB and BA ratios are combined geometrically within each pair, then pair samples are summarized by median with deterministic bootstrap 95% CI; raw process rows are never pooled or merged",
            "process_isolation": "each raw file contains exactly one candidate process; schedule references raw files without synthesizing combined raw reports",
        },
        "strata": strata,
        "provenance": {
            "binary": {"path": str(binary), "sha256": sha256_file(binary)},
            "git": {
                "commit": git_commit,
                "dirty": git_dirty,
                "consistent_across_runs": consistent_git,
            },
            "environment": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "rustc": first_environment.get("rustc"),
                "os": first_environment.get("os"),
                "cpu": first_environment.get("cpu"),
                "cwd": os.getcwd(),
            },
        },
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    (args.output_dir / "summary.json").write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return result_exit_code(comparison_valid, args.allow_unpublishable)


if __name__ == "__main__":
    raise SystemExit(main())
