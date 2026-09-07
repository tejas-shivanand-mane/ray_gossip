#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# Benchmarks that inspect recovery logs should keep backend info logging enabled.
os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")


@dataclass(frozen=True)
class Method:
    key: str
    label: str
    recovery_enabled: bool
    baseline_enabled: bool
    holders: int
    protection_interval: int = 1


def disabled() -> Method:
    return Method("disabled", "Disabled", False, False, 0)


def succession(holders: int) -> Method:
    return Method(
        "succession",
        f"Succession-R{holders}",
        True,
        False,
        holders,
    )


def witness_baseline(holders: int) -> Method:
    return Method(
        "witness_baseline",
        f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )


def frontier_proxy(protection_interval: int) -> Method:
    if protection_interval <= 0:
        raise ValueError("protection_interval must be positive")
    return Method(
        f"frontier_proxy_k{protection_interval}",
        f"FrontierProxy-K{protection_interval}",
        True,
        True,
        1,
        protection_interval,
    )


def recovery_methods(holders: int, include_disabled: bool = False) -> list[Method]:
    out: list[Method] = []
    if include_disabled:
        out.append(disabled())
    out.extend([succession(holders), witness_baseline(holders)])
    return out


def system_config(
    method: Method,
    *,
    witness_count: int = 2,
    object_timeout_ms: int | None = None,
    profiling_enabled: bool = False,
) -> dict[str, Any]:
    """Return the Ray system config for one experimental method.

    The proposed method and witness-as-holder baseline deliberately use the same
    recovery_succession_target_holder_count value R.  The baseline is selected
    only by enable_recovery_witness_holder_baseline.
    """
    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )

    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    baseline_protect_every_n = (
        int(method.protection_interval)
        if method.key.startswith("frontier_proxy_k")
        else 1
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
    # Keep this switch only for non-baseline Succession experiments.
    task_manager_pin = (
        os.environ.get("RAY_RECOVERY_TASKMANAGER_PIN", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )

    config: dict[str, Any] = {
        "enable_recovery_succession": method.recovery_enabled,
        "enable_recovery_witness_holder_baseline": method.baseline_enabled,
        "enable_recovery_succession_certificate_admission": certificate_admission,
        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_baseline_perf_protect_every_n": baseline_protect_every_n,
        "recovery_succession_witness_count": max(1, int(witness_count)),
        "enable_recovery_succession_profiling": bool(profiling_enabled),
    }
    if method.recovery_enabled:
        config["recovery_succession_target_holder_count"] = int(method.holders)
    # Explicit opt-in for the bounded ACK experiment. Omit the new native key
    # when unset so existing commands can still run against the baseline build.
    batch_ack = os.environ.get("RAY_RECOVERY_WITNESS_BATCH_ACK")
    if batch_ack is not None:
        if batch_ack not in ("0", "1"):
            raise ValueError("RAY_RECOVERY_WITNESS_BATCH_ACK must be 0 or 1")
        config["enable_recovery_witness_batch_ack"] = (
            batch_ack == "1" and method.recovery_enabled
        )
    if object_timeout_ms is not None:
        config["object_timeout_milliseconds"] = int(object_timeout_ms)
    shared_recipe = os.environ.get("RAY_RECOVERY_SHARED_HOLDER_RECIPE")
    if shared_recipe is not None:
        if shared_recipe not in ("0", "1"):
            raise ValueError("RAY_RECOVERY_SHARED_HOLDER_RECIPE must be 0 or 1")
        config["enable_recovery_succession_shared_holder_recipe"] = (
            shared_recipe == "1" and method.recovery_enabled and not method.baseline_enabled
        )
    return config


def percentile(values: Iterable[float], q: float) -> float:
    vals = sorted(float(v) for v in values)
    if not vals:
        return math.nan
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
    26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def mean_ci95(values: Iterable[float]) -> tuple[float, float]:
    vals = [float(v) for v in values if not math.isnan(float(v))]
    if not vals:
        return math.nan, math.nan
    mean = statistics.fmean(vals)
    if len(vals) == 1:
        return mean, 0.0
    stdev = statistics.stdev(vals)
    tcrit = _T95.get(len(vals) - 1, 1.96)
    return mean, tcrit * stdev / math.sqrt(len(vals))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def session_dirs(cluster: Any) -> set[Path]:
    return {
        Path(node.get_session_dir_path())
        for node in cluster.list_all_nodes()
    }


def find_log_lines(session_paths: set[Path], text: str) -> list[str]:
    out: list[str] = []
    for session_dir in session_paths:
        log_dir = session_dir / "logs"
        if not log_dir.exists():
            continue
        for path in log_dir.glob("*"):
            if not path.is_file():
                continue
            try:
                content = path.read_text(errors="replace")
            except OSError:
                continue
            for line in content.splitlines():
                if text in line:
                    out.append(f"{path.name}: {line}")
    return out


def wait_for_log(
    session_paths: set[Path],
    text: str,
    timeout_s: float,
    *,
    min_count: int = 1,
) -> list[str]:
    deadline = time.monotonic() + timeout_s
    last: list[str] = []
    while time.monotonic() < deadline:
        last = find_log_lines(session_paths, text)
        if len(last) >= min_count:
            return last
        time.sleep(0.05)
    return last


def wait_for_cluster(ray_module: Any, expected_nodes: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive = 0
    while time.monotonic() < deadline:
        alive = sum(1 for node in ray_module.nodes() if node.get("Alive"))
        if alive >= expected_nodes:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected_nodes} logical Ray nodes became alive")


def wait_for_protection(
    *,
    method: Method,
    session_paths: set[Path],
    timeout_s: float,
    rank: int | None = None,
) -> None:
    if not method.recovery_enabled:
        return
    if method.baseline_enabled:
        needle = "Installed full TaskSpec on all witness-holder baseline nodes"
    else:
        if rank is None:
            rank = method.holders
        needle = (
            "Committed recovery succession manifest after witness publication "
            f"with {rank + 1} total members"
        )
    if not wait_for_log(session_paths, needle, timeout_s):
        raise RuntimeError(
            f"Protection did not become ready for {method.label}; missing log: {needle!r}"
        )


def safe_shutdown(ray_module: Any, cluster: Any | None) -> None:
    try:
        ray_module.shutdown()
    except Exception:
        pass
    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:
            pass


def append_marker(path: str | Path, event: str, token: str = "") -> None:
    with Path(path).open("a", buffering=1) as f:
        f.write(f"{event},{time.time_ns()},{os.getpid()},{token}\n")


def read_marker(path: str | Path) -> list[tuple[str, int, int, str]]:
    p = Path(path)
    if not p.exists():
        return []
    out: list[tuple[str, int, int, str]] = []
    for line in p.read_text(errors="replace").splitlines():
        parts = line.split(",", 3)
        if len(parts) != 4:
            continue
        try:
            out.append((parts[0], int(parts[1]), int(parts[2]), parts[3]))
        except ValueError:
            continue
    return out


def wait_for_marker(path: str | Path, event: str, timeout_s: float, min_count: int = 1) -> list[tuple[str, int, int, str]]:
    deadline = time.monotonic() + timeout_s
    matches: list[tuple[str, int, int, str]] = []
    while time.monotonic() < deadline:
        matches = [row for row in read_marker(path) if row[0] == event]
        if len(matches) >= min_count:
            return matches
        time.sleep(0.02)
    return matches


def add_method_columns(row: dict[str, Any], method: Method) -> dict[str, Any]:
    row.update(
        {
            "method": method.key,
            "method_label": method.label,
            "recovery_enabled": int(method.recovery_enabled),
            "baseline_enabled": int(method.baseline_enabled),
            "holders": method.holders,
            "protection_interval": method.protection_interval,
        }
    )
    return row
