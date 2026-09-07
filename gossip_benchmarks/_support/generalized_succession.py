"""Owner-node failure and witness confirmation with generalized piggybacks."""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
import sys

from suite_runner import run_process

HERE = Path(__file__).resolve().parent


def counts(value):
    try:
        r, w = map(int, value.split(":"))
        if r <= 0 or w <= 0:
            raise ValueError()
        return r, w
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use positive R:W, for example 3:1") from exc


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rw", type=counts, nargs="+",
                   default=[(1, 1), (1, 3), (2, 2), (2, 3), (3, 1), (3, 3)])
    p.add_argument("--shared-recipes", type=int, choices=(0, 1), nargs="+", default=[0, 1])
    p.add_argument("--k", type=int, choices=(2, 4, 8, 16, 32), default=32)
    p.add_argument("--case-timeout-seconds", type=float, default=600)
    p.add_argument("--output-dir", type=Path)
    args = p.parse_args()
    if sys.flags.optimize:
        p.error("Run without -O: assertions are required")
    if not math.isfinite(args.case_timeout_seconds) or args.case_timeout_seconds <= 0:
        p.error("Case timeout must be finite and positive")
    out = args.output_dir or (HERE.parent / "results" / "generalized_succession" /
                             datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ"))
    out = out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    scenarios = [
        ("k1_owner_node", "succession_node_failure.py", ["--ordinary-k1"]),
        ("frontier_owner_node", "succession_node_failure.py", ["--initial-piggyback-k", str(args.k)]),
        ("frontier_commit_gap", "succession_commit_gap.py", ["--initial-piggyback-k", str(args.k)]),
        ("frontier_blocked_confirmation", "succession_commit_gap.py",
         ["--initial-piggyback-k", str(args.k), "--fail-holder-witness-confirmation"]),
    ]
    rows = []
    (out / "settings.json").write_text(json.dumps({
        "rw": args.rw, "shared_recipes": args.shared_recipes, "k": args.k,
        "case_timeout_seconds": args.case_timeout_seconds, "batch_ack": False,
    }, indent=2) + "\n")
    for r, w in dict.fromkeys(args.rw):
        for shared in dict.fromkeys(args.shared_recipes):
            env = os.environ.copy()
            env.pop("PYTHONOPTIMIZE", None)
            env["RAY_RECOVERY_WITNESS_BATCH_ACK"] = "0"
            env["RAY_RECOVERY_SHARED_HOLDER_RECIPE"] = str(shared)
            env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
            env["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
            env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
            for name, script, options in scenarios:
                label = f"r{r}_w{w}_shared{shared}_{name}"
                log = out / f"{label}.log"
                print(label, flush=True)
                row = {"r": r, "w": w, "shared_recipe": shared, "case": name, "passed": False}
                try:
                    run_process([sys.executable, "-u", str(HERE / script),
                                 "--holders", str(r), "--witness-count", str(w), *options],
                                log_path=log, timeout=args.case_timeout_seconds, env=env)
                    row["passed"] = True
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    print(log.read_text(errors="replace")[-12000:], flush=True)
                    raise SystemExit(f"Failed {label}; log: {log}") from exc
                finally:
                    rows.append(row)
                    (out / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
                print("  PASS", flush=True)
    print(f"PASS: {len(rows)} cases; logs: {out}")
