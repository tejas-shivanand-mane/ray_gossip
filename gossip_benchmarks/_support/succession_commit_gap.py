#!/usr/bin/env python3
"""Recovery Frontier + Succession witness-ACK / candidate-commit gap.

This benchmark targets the deterministic crash window:

    Frontier[T1,T2] metadata reaches borrower H1
        ->
    H1 installs the shared Frontier replay snapshot provisionally
        ->
    compact witness ACKs the proposed Succession manifest containing H1
        ->
    OWNER NODE DIES before owner-side holder commit and before
    CommitRecoveryManifest reaches H1
        ->
    H1 independently confirms its provisional state from the witness
        ->
    request T2
        ->
    replay T2 exactly once; never replay T1

The default-off C++ test hook

    recovery_succession_test_fail_after_witness_ack

makes the post-witness/pre-candidate-commit state deterministic.  There is no
sleep-based attempt to hit a race.

This composes the correctness property from Benchmark 14 with the shared
Recovery Frontier topology proven by Benchmarks 52/53.  A recovery-only pass is
not enough: before owner failure we additionally require one candidate report
for the two Frontier members, a completed provisional holder install and
witness publication, zero owner-side committed admissions, and zero candidate
commit RPCs.

Use --holders R --witness-count W for positive independent counts, and
--initial-piggyback-k K for K=2/4/8/16/32 with W
independent witness nodes. The in-progress H1 admission must store all K
recipes through a leader-last owner export with zero holder-install RPCs. Recovery
requests the last member; every other member must have zero replay starts.
--initial-k2-piggyback remains an alias for K=2.
Add --fail-holder-witness-confirmation to require recovery to fail without
replay when only the holder's independent witness confirmation is blocked.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from common import (
    read_marker,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_log,
    wait_for_marker,
)

R = 2
K = 4
NUM_TASKS = 2
TARGET_INDEX = 1
WITNESS_COUNT = 2
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 300.0
FAULT_TIMEOUT_S = 30.0
PROFILE_TIMEOUT_S = 30.0
OWNER_DEAD_TIMEOUT_S = 30.0
PROMOTION_LOG_TIMEOUT_S = 15.0

FAULT_CONFIG_KEY = "recovery_succession_test_fail_after_witness_ack"
FAULT_LOG = (
    "TEST ONLY: injected recovery succession failure after witness ACK "
    "before candidate commit"
)
PROMOTION_LOG = (
    "Promoted provisional recovery holder from witness-backed manifest"
)


def frontier_succession_system_config() -> dict:
    config = system_config(
        succession(R),
        witness_count=WITNESS_COUNT,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    config.update(
        {
            "enable_recovery_frontier": K > 1,
            "recovery_frontier_group_size": K,
            "recovery_baseline_perf_protect_every_n": 1,
            # Keep this benchmark on the ordinary ordered-admission path so the
            # fault log/state is exactly the Benchmark-14 commit gap.
            "enable_recovery_succession_certificate_admission": False,
            # Deterministically stop after a real witness ACK but before both
            # owner-side admission commit and candidate CommitRecoveryManifest.
            FAULT_CONFIG_KEY: True,
        }
    )
    return config


def normalize_node_id(value: Any) -> str:
    return str(value).strip().lower()


def wait_for_node_dead(node_id: str, timeout_s: float) -> None:
    target = normalize_node_id(node_id)
    deadline = time.monotonic() + timeout_s
    last_nodes: list[dict[str, Any]] = []

    while time.monotonic() < deadline:
        last_nodes = ray.nodes()
        for info in last_nodes:
            current = normalize_node_id(info.get("NodeID", ""))
            if current == target and not bool(info.get("Alive", False)):
                return

        present = any(
            normalize_node_id(info.get("NodeID", "")) == target
            for info in last_nodes
        )
        if not present:
            return
        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for owner node {node_id} to become DEAD. "
        f"Last ray.nodes()={last_nodes}"
    )


def count_token_starts(marker: Path, token: str, *, after_ns: int = 0) -> int:
    return sum(
        1
        for event, wall_ns, _pid, row_token in read_marker(marker)
        if event == "START" and wall_ns >= after_ns and row_token == token
    )


def types():
    @ray.remote(max_retries=2)
    def work(token: str, marker_path: str, payload_bytes: int):
        marker = Path(marker_path)

        prior_starts = 0
        if marker.exists():
            for line in marker.read_text(errors="replace").splitlines():
                parts = line.split(",", 3)
                if len(parts) == 4 and parts[0] == "START" and parts[3] == token:
                    prior_starts += 1

        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")

        if prior_starts == 0:
            # Keep original executions unavailable.  Recovery replay observes
            # the prior START marker and returns without waiting.
            release = Path(str(marker) + f".release.{token}")
            deadline = time.monotonic() + INITIAL_BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Initial execution for {token} was never released"
                    )
                time.sleep(0.05)

        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")

        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class HolderBorrower:
        def hold(self, wrapped_refs, leader_last: bool = False):
            # Both Frontier refs arrive together at one CoreWorker.  Correct
            # Frontier+Succession metadata handling must deduplicate them to one
            # candidate report for the shared group.
            self.refs = list(reversed(wrapped_refs)) if leader_last else list(wrapped_refs)
            return [ref.hex() for ref in self.refs]

        def read(self, index: int):
            ref = self.refs[index]
            return ref.hex(), ray.get(ref)

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch_and_export(
            self,
            executor_node_id: str,
            tokens: list[str],
            marker_path: str,
            payload_bytes: int,
            holder,
            leader_last: bool = False,
        ):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id,
                soft=False,
            )
            refs = [
                work.options(
                    scheduling_strategy=strategy,
                    num_cpus=0.1,
                ).remote(token, marker_path, payload_bytes)
                for token in tokens
            ]

            # Critical: export directly FROM THE OWNER.  A driver relay would
            # not exercise normal task-argument Recovery Succession metadata.
            # Native metadata handling must consume the recipe even when a
            # non-leader sidecar creates the group candidate first.
            exported_refs = list(reversed(refs)) if leader_last else refs
            held_ids = ray.get(holder.hold.remote(exported_refs, leader_last))
            return [ref.hex() for ref in refs], held_ids

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    return Owner, HolderBorrower


def wait_for_profile(
    owner,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_s: float,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}

    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        if predicate(last):
            return last
        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for {description}. Last profile={last}"
    )


def main() -> None:
    global R, K, NUM_TASKS, TARGET_INDEX, WITNESS_COUNT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--holders", type=int, default=2)
    parser.add_argument("--witness-count", type=int, default=2)
    initial = parser.add_mutually_exclusive_group()
    initial.add_argument(
        "--initial-piggyback-k", type=int, choices=(2, 4, 8, 16, 32),
        help="Require a full initial group with verified recipe piggybacks",
    )
    initial.add_argument(
        "--initial-k2-piggyback", dest="initial_piggyback_k",
        action="store_const", const=2,
        help="Compatibility alias for --initial-piggyback-k 2",
    )
    parser.add_argument(
        "--fail-holder-witness-confirmation", action="store_true",
        help="Negative check: requester discovery cannot authorize holder replay",
    )
    initial.add_argument("--ordinary-k1", action="store_true")
    args = parser.parse_args()
    if args.holders <= 0 or args.witness_count <= 0:
        parser.error("R and W must be positive")
    if sys.flags.optimize:
        parser.error("Run without -O: correctness checks require assertions")
    R, WITNESS_COUNT = args.holders, args.witness_count
    if args.ordinary_k1:
        K, NUM_TASKS, TARGET_INDEX = 1, 1, 0
    if args.fail_holder_witness_confirmation and not (args.initial_piggyback_k or args.ordinary_k1):
        parser.error("--fail-holder-witness-confirmation requires --initial-piggyback-k")
    if args.initial_piggyback_k:
        K = args.initial_piggyback_k
        NUM_TASKS = K
        TARGET_INDEX = K - 1
        os.environ["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
        os.environ["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_succession_commit_gap_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster = Cluster()
        config = frontier_succession_system_config()
        config["recovery_succession_test_fail_holder_witness_confirmation"] = (
            args.fail_holder_witness_confirmation
        )
        cluster.add_node(
            num_cpus=0,
            _system_config=config,
            include_dashboard=False,
        )
        owner_node = cluster.add_node(
            num_cpus=1,
            resources={"owner_node": 1},
        )
        executor_node = cluster.add_node(
            # Every original task blocks, so all K must fit concurrently.
            num_cpus=max(2, (NUM_TASKS + 9) // 10 + 1),
            resources={"executor_node": 1},
        )
        holder_node = cluster.add_node(
            num_cpus=1,
            resources={"holder_node": 1},
        )

        # Keep all witnesses outside the owner and holder nodes.
        for i in range(WITNESS_COUNT):
            cluster.add_node(num_cpus=0, resources={f"witness_node_{i}": 1})

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        wait_for_cluster(
            ray, 4 + WITNESS_COUNT, 30.0
        )
        sessions = session_dirs(cluster)

        Owner, HolderBorrower = types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        holder = HolderBorrower.options(
            resources={"holder_node": 0.01},
            num_cpus=0,
        ).remote()

        tokens = [f"member-{i}-{uuid.uuid4().hex}" for i in range(NUM_TASKS)]

        object_ids, held_ids = ray.get(
            owner.dispatch_and_export.remote(
                executor_node.node_id,
                tokens,
                str(marker),
                PAYLOAD_BYTES,
                holder,
                bool(args.initial_piggyback_k),
            )
        )
        assert len(object_ids) == NUM_TASKS
        assert len(set(object_ids)) == NUM_TASKS
        assert held_ids == object_ids, (held_ids, object_ids)

        starts = wait_for_marker(
            marker,
            "START",
            timeout_s=60.0,
            min_count=NUM_TASKS,
        )
        assert len(starts) >= NUM_TASKS, read_marker(marker)
        for token in tokens:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        # This log is emitted only after a real witness has ACKed the proposed
        # manifest and immediately before the injected pre-commit failure.
        fault_logs = wait_for_log(
            sessions,
            FAULT_LOG,
            FAULT_TIMEOUT_S,
        )
        assert fault_logs, (
            "Did not observe the deterministic post-witness/pre-candidate-commit "
            "fault window"
        )

        profile = wait_for_profile(
            owner,
            lambda p: (
                int(p.get("candidate_reports_received", 0)) >= 1
                and (
                    (args.ordinary_k1 and int(p.get("first_holder_piggyback_copies_sent", 0)) >= 1)
                    or int(p.get("frontier_recipe_piggyback_admissions", 0)) >= 1
                    or int(p.get("holder_install_rpcs_completed", 0)) >= 1
                )
                and int(p.get("witness_update_rpcs_completed", 0)) >= WITNESS_COUNT
            ),
            PROFILE_TIMEOUT_S,
            "provisional Frontier holder + witness durability",
        )

        reports_received = int(profile.get("candidate_reports_received", 0))
        reports_accepted = int(profile.get("candidate_reports_accepted", 0))
        install_sent = int(profile.get("holder_install_rpcs_sent", 0))
        install_completed = int(profile.get("holder_install_rpcs_completed", 0))
        witness_sent = int(profile.get("witness_update_rpcs_sent", 0))
        witness_completed = int(profile.get("witness_update_rpcs_completed", 0))
        commit_sent = int(profile.get("holder_commit_rpcs_sent", 0))
        commit_completed = int(profile.get("holder_commit_rpcs_completed", 0))
        admissions = int(profile.get("holder_admissions_committed", 0))

        # Shared-Frontier assertion under the fault path: both T1/T2 metadata
        # arrived at one borrower, but only one group candidate was reported.
        assert reports_received == 1, profile
        assert reports_accepted == 1, profile
        if args.initial_piggyback_k:
            assert profile.get("initial_install_profile_version") == 3, profile
            holder_profile = ray.get(holder.recovery_profile.remote(), timeout=PROFILE_TIMEOUT_S)
            evidence = {"owner": profile, "holder": holder_profile}
            assert int(profile.get("frontier_recipe_piggyback_admissions", 0)) == 1, evidence
            assert install_sent == 0 and install_completed == 0, evidence
            assert int(holder_profile.get("frontier_recipe_piggybacks_stored", 0)) == 1, holder_profile
            assert int(holder_profile.get("frontier_holder_materialize_members", 0)) == NUM_TASKS, holder_profile
            if os.environ.get("RAY_RECOVERY_SHARED_HOLDER_RECIPE") == "1":
                assert holder_profile.get("shared_holder_recipe_enabled"), holder_profile
                assert int(holder_profile.get("shared_holder_recipes_current", 0)) == NUM_TASKS, holder_profile
        elif args.ordinary_k1:
            assert install_sent == 0 and install_completed == 0, profile
        else:
            assert install_sent == 1, profile
            assert install_completed == 1, profile

        # Decisive crash-window assertions.  The holder has its provisional
        # replay snapshot and witnesses are durable, but normal commit has NOT
        # happened on either the owner or candidate.
        assert witness_sent == WITNESS_COUNT, profile
        assert witness_completed == WITNESS_COUNT, profile
        assert admissions == 0, profile
        assert commit_sent == 0, profile
        assert commit_completed == 0, profile

        failure_wall_ns = time.time_ns()
        failure_start = time.perf_counter()
        cluster.remove_node(owner_node, allow_graceful=False)
        wait_for_node_dead(owner_node.node_id, OWNER_DEAD_TIMEOUT_S)

        # Ask the provisional group holder for NON-LEADER T2.  It must not rely
        # on the dead owner or on a requester vouching for it; its recovery path
        # independently queries the witness and promotes its own provisional
        # state before replay.
        if args.fail_holder_witness_confirmation:
            try:
                ray.get(holder.read.remote(TARGET_INDEX), timeout=GET_TIMEOUT_S)
            except ray.exceptions.OwnerDiedError:
                pass
            else:
                raise AssertionError("Provisional holder replayed without its witness confirmation")
            blocked_logs = wait_for_log(
                sessions,
                "TEST ONLY: suppressing provisional holder witness confirmation",
                PROMOTION_LOG_TIMEOUT_S,
            )
            assert blocked_logs, (
                "Failure did not reach the holder confirmation boundary; "
                "requester witness discovery may still be broken"
            )
            for token in tokens:
                assert count_token_starts(marker, token, after_ns=failure_wall_ns) == 0, read_marker(marker)
            print("PASS: requester discovery cannot replace holder witness confirmation")
            print(f"  R={R} W={WITNESS_COUNT} K={K}; post-failure replays=0")
            return

        recovered_object_id, value = ray.get(
            holder.read.remote(TARGET_INDEX),
            timeout=GET_TIMEOUT_S,
        )
        failure_to_result_s = time.perf_counter() - failure_start

        assert recovered_object_id == object_ids[TARGET_INDEX]
        assert value["token"] == tokens[TARGET_INDEX]
        assert len(value["payload"]) == PAYLOAD_BYTES

        promotion_logs = wait_for_log(
            sessions,
            PROMOTION_LOG,
            PROMOTION_LOG_TIMEOUT_S,
        )
        assert promotion_logs, (
            "Recovery succeeded without observing the expected provisional-holder "
            "witness-promotion log"
        )

        target_post_failure_starts = count_token_starts(
            marker,
            tokens[TARGET_INDEX],
            after_ns=failure_wall_ns,
        )
        leader_post_failure_starts = count_token_starts(
            marker,
            tokens[0],
            after_ns=failure_wall_ns,
        )

        assert target_post_failure_starts == 1, read_marker(marker)
        assert leader_post_failure_starts == (1 if TARGET_INDEX == 0 else 0), read_marker(marker)
        for i, token in enumerate(tokens):
            if i != TARGET_INDEX:
                assert count_token_starts(
                    marker, token, after_ns=failure_wall_ns
                ) == 0, read_marker(marker)

        print("PASS: Recovery Frontier + Succession witness-ACK commit-gap recovery")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  W                         = {WITNESS_COUNT}")
        if args.initial_piggyback_k:
            print("  provisional recipe piggybacks = 1; separate install RPCs = 0")
        print(f"  candidate reports recv    = {reports_received}")
        print(f"  candidate reports accept  = {reports_accepted}")
        print(f"  holder install            = {install_sent}/{install_completed}")
        print(f"  witness update            = {witness_sent}/{witness_completed}")
        print(f"  owner admissions pre-kill = {admissions}")
        print(f"  candidate commit pre-kill = {commit_sent}/{commit_completed}")
        print(f"  promotion log observed    = {len(promotion_logs)}")
        print(f"  leader ObjectID           = {object_ids[0]}")
        print(f"  target ObjectID           = {object_ids[TARGET_INDEX]}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  failure-to-result (s)     = {failure_to_result_s:.3f}")
        print(f"  target post-failure START = {target_post_failure_starts}")
        print(f"  leader post-failure START = {leader_post_failure_starts}")

    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass
        for token in locals().get("tokens", []):
            try:
                Path(str(marker) + f".release.{token}").unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()
