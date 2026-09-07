#!/usr/bin/env python3
"""Shared fresh-cluster workload used by performance experiments.

Two borrowers by default; the borrower sweep explicitly varies this count.
R=2/W=2 by default; Benchmark 08 explicitly opts into its R/W sweep.
Timed completion is application completion; admission can overlap it."""
from __future__ import annotations

import argparse
import math
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from common import disabled, percentile, safe_shutdown, succession, system_config, wait_for_cluster

R_DEFAULT = 2
VARIANTS = [
    "disabled",
    "succession_k1",
    "succession_k2",
    "succession_k4",
    "succession_k8",
    "succession_k16",
    "succession_k32",
]
K_BY_VARIANT = {
    "succession_k1": 1,
    "succession_k2": 2,
    "succession_k4": 4,
    "succession_k8": 8,
    "succession_k16": 16,
    "succession_k32": 32,
}
ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
    ("candidate_rpc_physical_rpcs_sent", "candidate_rpc_physical_rpcs_completed"),
]
BORROWER_PROFILE_KEYS = [
    "candidate_rpc_logical_reports_sent",
    "candidate_rpc_logical_reports_completed",
    "candidate_rpc_physical_rpcs_sent",
    "candidate_rpc_physical_rpcs_completed",
    "candidate_rpc_request_bytes_sent",
    "task_argument_metadata_calls",
    "task_argument_metadata_refs_attached",
    "task_argument_metadata_transport_bytes",
    "register_executor_candidate_reports_built",
]
OWNER_PROFILE_KEYS = [
    "candidate_reports_received",
    "candidate_reports_accepted",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "holder_admissions_committed",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
    "initial_manifest_build_count",
]

@dataclass(frozen=True)
class SpecPadding:
    name: str
    size_bytes: int


def parse_padding(text: str) -> SpecPadding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("TaskSpec padding must be NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError("invalid TaskSpec padding")
    return SpecPadding(name, size)


def build_padding(total: int, chunk: int) -> tuple[bytes, ...]:
    if total <= 0:
        return ()
    out, left, token = [], total, 1
    while left:
        n = min(left, chunk)
        out.append(bytes([token % 251]) * n)
        left -= n
        token += 1
    return tuple(out)


def k_for(variant: str) -> int:
    return K_BY_VARIANT.get(variant, 0)


def child_env(*, profiling: bool) -> dict[str, str]:
    env = dict(os.environ)
    env["RAY_RECOVERY_PROFILING"] = "1" if profiling else "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
    return env


def case_config(variant: str, holders: int, witnesses: int, profiling: bool) -> dict[str, Any]:
    method = disabled() if variant == "disabled" else succession(holders)
    cfg = system_config(method, witness_count=witnesses, profiling_enabled=profiling and method.recovery_enabled)
    k = k_for(variant)
    cfg.update({
        # K=1 is the ordinary Succession baseline. The production adaptive
        # Frontier composition is intentionally enabled only for K>1.
        "enable_recovery_frontier": bool(method.recovery_enabled and k > 1),
        "recovery_frontier_group_size": max(1, k),
        "recovery_baseline_perf_protect_every_n": 1,
        "enable_recovery_succession_certificate_admission": False,
    })
    return cfg


def start_cluster(args: argparse.Namespace, variant: str, profiling: bool, *,
                  borrower_count: int = 2, replication_sweep: bool = False,
                  witness_nodes: int | None = None) -> tuple[Cluster, str]:
    allowed = (1, 2, 3) if replication_sweep else (2,)
    if args.holders not in allowed:
        raise ValueError(f"This workload requires --holders in {allowed}")
    if args.witness_count != args.holders:
        raise ValueError("--witness-count must equal --holders")
    if borrower_count < 1:
        raise ValueError("Need at least one application borrower")
    witness_nodes = args.witness_count if witness_nodes is None else witness_nodes
    if witness_nodes < args.witness_count:
        raise ValueError("Not enough witness-capable nodes for configured W")
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=case_config(variant, args.holders, args.witness_count, profiling), include_dashboard=False)
    producer = cluster.add_node(num_cpus=args.cpus_per_node, resources={"producer_node": 1})
    for i in range(borrower_count):
        cluster.add_node(num_cpus=args.cpus_per_node, resources={f"borrower_node_{i}": 1})
    for i in range(witness_nodes):
        cluster.add_node(num_cpus=0, resources={f"witness_node_{i}": 1})
    return cluster, producer.node_id


def remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int, *padding: bytes) -> bytes:
        if padding and padding[0]:
            _ = padding[0][0]
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
    class Borrower:
        def __init__(self):
            self.refs = []
        def touch(self, wrapped_ref):
            value = ray.get(wrapped_ref[0])
            return int.from_bytes(value[:8], "little", signed=False)
        def hold(self, wrapped_refs):
            refs = list(wrapped_refs)
            self.refs.extend(refs)
            return [ref.hex() for ref in refs]
        def clear(self):
            self.refs.clear()
        def ping(self):
            import os as _os
            return _os.getpid()
        def reset_profile(self):
            from ray._private.worker import global_worker as gw
            gw.core_worker.reset_recovery_succession_profile()
        def profile(self):
            from ray._private.worker import global_worker as gw
            return dict(gw.core_worker.get_recovery_succession_profile())
    return produce, Borrower


def run_window(*, produce: Any, borrowers: list[Any], strategy: Any, padding: tuple[bytes, ...], payload_bytes: int,
               duration_s: float, inflight: int, burst: int, wait_timeout: float, drain_timeout: float,
               request_base: int) -> dict[str, Any]:
    if not borrowers:
        raise ValueError("timed workload requires at least one application borrower")
    if burst <= 0 or inflight < burst or inflight % burst:
        raise ValueError("invalid burst/inflight configuration")

    pending: dict[ray.ObjectRef, int] = {}
    remaining: dict[int, int] = {}
    submitted_ns: dict[int, int] = {}
    next_id = request_base
    completed = 0
    submitted = 0
    latencies: list[float] = []
    end_ns = time.perf_counter_ns() + int(duration_s * 1e9)

    def submit_burst() -> None:
        nonlocal next_id, submitted
        created: list[tuple[int, ray.ObjectRef]] = []
        # Register the full burst before exporting any ref. This gives K>1 full
        # groups while preserving an identical application workload for all K.
        for _ in range(burst):
            rid = next_id
            next_id += 1
            submitted += 1
            submitted_ns[rid] = time.perf_counter_ns()
            ref = produce.options(scheduling_strategy=strategy, num_cpus=1).remote(rid, payload_bytes, *padding)
            created.append((rid, ref))
        for rid, ref in created:
            remaining[rid] = len(borrowers)
            for borrower in borrowers:
                pending[borrower.touch.remote([ref])] = rid

    def process_ready() -> None:
        nonlocal completed
        if not pending:
            return
        ready, _ = ray.wait(list(pending), num_returns=min(64, len(pending)), timeout=wait_timeout)
        for result_ref in ready:
            rid = pending.pop(result_ref)
            if int(ray.get(result_ref)) != rid:
                raise RuntimeError("consumer observed wrong producer result")
            left = remaining[rid] - 1
            if left:
                remaining[rid] = left
                continue
            del remaining[rid]
            now_ns = time.perf_counter_ns()
            if now_ns <= end_ns:
                completed += 1
            latencies.append((now_ns - submitted_ns.pop(rid)) / 1e6)

    while len(remaining) + burst <= inflight:
        submit_burst()
    while time.perf_counter_ns() < end_ns:
        process_ready()
        while time.perf_counter_ns() < end_ns and len(remaining) + burst <= inflight:
            submit_burst()
    deadline = time.monotonic() + drain_timeout
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"drain timed out: {len(pending)} borrower calls remain")
        process_ready()
    if not latencies:
        raise RuntimeError("no completed pipelines")
    return {
        "throughput_rps": completed / duration_s,
        "completed_in_window": completed,
        "total_pipeline_submitted": submitted,
        "latency_sample_count": len(latencies),
        "latency_mean_ms": statistics.fmean(latencies),
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
    }


def single_perf(args: argparse.Namespace, *, borrower_count: int = 2,
                replication_sweep: bool = False,
                witness_nodes: int | None = None) -> dict[str, Any]:
    cluster = None
    witness_nodes = args.witness_count if witness_nodes is None else witness_nodes
    try:
        cluster, producer_node = start_cluster(
            args, args.single_variant, False, borrower_count=borrower_count,
            replication_sweep=replication_sweep, witness_nodes=witness_nodes)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + borrower_count + witness_nodes, args.cluster_timeout_seconds)
        produce, Borrower = remote_types()
        borrowers = [Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote() for i in range(borrower_count)]
        ray.get([b.ping.remote() for b in borrowers])
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = build_padding(args.single_padding_bytes, args.inline_chunk_bytes)
        if args.warmup_seconds > 0:
            run_window(produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
                       payload_bytes=args.payload_bytes, duration_s=args.warmup_seconds,
                       inflight=args.inflight_tasks, burst=args.burst_size,
                       wait_timeout=args.wait_timeout_seconds, drain_timeout=args.drain_timeout_seconds,
                       request_base=1_000_000)
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)
        perf = run_window(produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
                          payload_bytes=args.payload_bytes, duration_s=args.duration_seconds,
                          inflight=args.inflight_tasks, burst=args.burst_size,
                          wait_timeout=args.wait_timeout_seconds, drain_timeout=args.drain_timeout_seconds,
                          request_base=10_000_000)
        return {
            "variant": args.single_variant,
            "frontier_k": k_for(args.single_variant),
            "repetition": args.single_repetition,
            "holders": args.holders,
            "borrowers_per_pipeline": len(borrowers),
            "task_spec_padding_name": args.single_padding_name,
            "task_spec_padding_bytes": args.single_padding_bytes,
            "payload_bytes": args.payload_bytes,
            "burst_size": args.burst_size,
            "inflight_tasks": args.inflight_tasks,
            "profiling_enabled": 0,
            "shared_holder_recipe_enabled": bool(dict(
                global_worker.core_worker.get_recovery_succession_profile()
            ).get("shared_holder_recipe_enabled", False)),
            # Read outside the timed window; identify the native mode actually
            # loaded rather than relying solely on the launch environment.
            "witness_batch_ack_enabled": bool(dict(
                global_worker.core_worker.get_recovery_succession_profile()
            ).get("witness_batch_ack_enabled", False)),
            **perf,
        }
    finally:
        safe_shutdown(ray, cluster)
