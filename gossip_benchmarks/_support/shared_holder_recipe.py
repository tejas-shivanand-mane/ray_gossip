"""Compare legacy and shared immutable holder recipes on the same native build.

Reuses the unmodified timed producer/two-borrower workload from experiment 01.
No profiling or failures in timed cases; no plot rendering in this entry point.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import subprocess
import sys

import comparison
from suite_runner import run_process

HERE = Path(__file__).resolve().parent
BASELINE_COMMIT = "4a4ec13927091ae870ae5ecf7fc6f6720b8381b0"
VARIANTS = ("fixed_r", "succession_k1", "fixed_k32", "succession_k32")


def paired_rows(rows):
    pairs = {}
    for row in rows:
        key = (row["variant"], row["repetition"])
        pairs.setdefault(key, {})[row["shared_recipe"]] = row
    result = []
    for (variant, repetition), pair in pairs.items():
        if set(pair) != {0, 1}:
            continue
        off, on = pair[0], pair[1]
        result.append({
            "variant": variant,
            "method": comparison.method_family(variant),
            "frontier_k": comparison.k_for(variant),
            "repetition": repetition,
            "legacy_throughput_rps": off["throughput_rps"],
            "shared_recipe_throughput_rps": on["throughput_rps"],
            "throughput_change_pct": 100 * (
                on["throughput_rps"] / off["throughput_rps"] - 1),
            "p95_change_ms": on["latency_p95_ms"] - off["latency_p95_ms"],
        })
    return result


def write_outputs(out, rows):
    comparison.write_csv(out / "shared_holder_recipe_runs.csv", rows)
    paired = paired_rows(rows)
    comparison.write_csv(out / "shared_holder_recipe_paired.csv", paired)
    summaries = []
    for variant in VARIANTS:
        selected = [row for row in paired if row["variant"] == variant]
        if not selected:
            continue
        summary = {"variant": variant, "paired_repetitions": len(selected)}
        for metric in ("legacy_throughput_rps", "shared_recipe_throughput_rps",
                       "throughput_change_pct", "p95_change_ms"):
            for stat, value in comparison.describe(row[metric] for row in selected).items():
                summary[f"{metric}_{stat}"] = value
        summaries.append(summary)
    comparison.write_csv(out / "shared_holder_recipe_summary.csv", summaries)
    return summaries


def run_parent(args):
    out = (args.output_dir or (
        HERE.parent / "results" / "shared_holder_recipe" /
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )).resolve()
    # Fresh directory prevents accidental mixing of settings or native builds.
    out.mkdir(parents=True, exist_ok=False)
    metadata = {**vars(args), "output_dir": str(out),
                "baseline_commit": BASELINE_COMMIT,
                "comparison": "same-build flag OFF versus ON",
                "profiling_enabled": False, "borrowers": 2,
                "ray_commit": getattr(comparison.b58.ray, "__commit__", "unknown"),
                "ray_version": comparison.b58.ray.__version__}
    (out / "experiment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    padding = comparison.b58.SpecPadding(f"{args.padding_bytes}B", args.padding_bytes)
    case_number = 0
    print(f"Baseline recorded: {BASELINE_COMMIT}", flush=True)
    print(f"{args.repetitions * 8} fresh-cluster cases; R=W=B=2; K=1,32; "
          "profiling OFF. Each OFF/ON pair uses the same method and K.", flush=True)
    for repetition in range(1, args.repetitions + 1):
        variants = list(VARIANTS)
        random.Random(args.seed + repetition).shuffle(variants)
        for variant in variants:
            # Balance OFF/ON order for each variant over every two repetitions.
            first = (repetition + VARIANTS.index(variant)) % 2
            for enabled in (first, 1 - first):
                case_number += 1
                name = f"rep_{repetition}_{variant}_shared_{enabled}"
                output_json = out / f"{name}.json"
                cmd = comparison.perf_cmd(args, variant, padding, repetition, output_json)
                cmd[1] = str(Path(__file__).resolve())
                cmd += ["--shared-recipe", str(enabled)]
                env = comparison.b58.child_env(profiling=False)
                env["RAY_RECOVERY_SHARED_HOLDER_RECIPE"] = str(enabled)
                env["RAY_RECOVERY_WITNESS_BATCH_ACK"] = "0"
                print(f"[{case_number}/{args.repetitions * 8}] {name}", flush=True)
                log_path = out / f"{name}.log"
                try:
                    run_process(cmd, log_path=log_path, env=env,
                                timeout=args.case_timeout_seconds)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    print(log_path.read_text(errors="replace")[-12000:], flush=True)
                    raise SystemExit(f"Case failed; inspect {log_path}") from exc
                if not output_json.exists():
                    raise RuntimeError(f"Case failed; inspect {out / (name + '.log')}")
                row = json.loads(output_json.read_text())
                if row["shared_recipe"] != enabled or row["variant"] != variant:
                    raise RuntimeError(f"Mismatched child result: {output_json}")
                if not math.isfinite(row["throughput_rps"]) or row["throughput_rps"] <= 0:
                    raise RuntimeError(f"Invalid throughput: {output_json}")
                row["case_position"] = case_number
                rows.append(row)
                write_outputs(out, rows)
                print(f"  {row['throughput_rps']:.1f} pipelines/s", flush=True)
    print("\nPaired throughput change: flag ON versus OFF (mean +/- 95% CI)")
    for summary in write_outputs(out, rows):
        print(f"  {summary['variant']:16s} "
              f"{summary['throughput_change_pct_mean']:+.2f}% +/- "
              f"{summary['throughput_change_pct_ci95']:.2f} pp")
    print(f"Results: {out}")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("run", "_single-perf"), nargs="?", default="run")
    p.add_argument("--output-dir", type=Path)
    p.add_argument("--repetitions", type=int, default=4)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--padding-bytes", type=int, default=1024)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--warmup-seconds", type=float, default=5)
    p.add_argument("--settle-seconds", type=float, default=1)
    p.add_argument("--duration-seconds", type=float, default=20)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--wait-timeout-seconds", type=float, default=1)
    p.add_argument("--drain-timeout-seconds", type=float, default=180)
    p.add_argument("--case-timeout-seconds", type=float, default=600)
    p.add_argument("--holders", type=int, choices=(2,), default=2)
    p.add_argument("--witness-count", type=int, choices=(2,), default=2)
    p.add_argument("--shared-recipe", type=int, choices=(0, 1))
    p.add_argument("--single-variant", choices=VARIANTS)
    p.add_argument("--single-padding-name")
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-output-json")
    return p


def main():
    p = parser()
    args = p.parse_args()
    if args.repetitions < 2 or args.padding_bytes < 0 or args.payload_bytes < 8:
        p.error("Require repetitions >=2, padding >=0, payload >=8")
    if (args.burst_size <= 0 or args.burst_size % 32 or args.inflight_tasks <= 0
            or args.inflight_tasks % args.burst_size or args.inline_chunk_bytes <= 0
            or args.cpus_per_node <= 0):
        p.error("Require positive sizes/CPUs, burst divisible by 32, inflight by burst")
    for name in ("warmup_seconds", "settle_seconds", "duration_seconds",
                 "cluster_timeout_seconds", "wait_timeout_seconds",
                 "drain_timeout_seconds", "case_timeout_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (
                name not in ("warmup_seconds", "settle_seconds") and value == 0):
            p.error(f"Invalid {name}")
    if args.command == "_single-perf":
        if (args.shared_recipe is None or args.single_variant is None
                or args.single_padding_bytes is None or args.single_padding_bytes < 0
                or args.single_repetition is None or not args.single_output_json):
            p.error("Missing or invalid child settings")
        if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
            p.error("Timed child requires RAY_RECOVERY_PROFILING=0")
        os.environ["RAY_RECOVERY_SHARED_HOLDER_RECIPE"] = str(args.shared_recipe)
        os.environ["RAY_RECOVERY_WITNESS_BATCH_ACK"] = "0"
        row = comparison.b58.single_perf(args)
        expected = bool(args.shared_recipe) and comparison.method_family(args.single_variant) == "succession"
        if row["shared_holder_recipe_enabled"] != expected:
            raise RuntimeError("Native shared-recipe mode mismatch: rebuild this commit first")
        row.update(shared_recipe=args.shared_recipe, method=comparison.method_family(args.single_variant))
        Path(args.single_output_json).write_text(json.dumps(row, indent=2) + "\n")
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
