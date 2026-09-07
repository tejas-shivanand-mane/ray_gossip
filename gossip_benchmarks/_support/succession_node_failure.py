#!/usr/bin/env python3
"""Recovery Frontier + Recovery Succession non-leader owner-node failure target.

This is intentionally a correctness/integration benchmark, not a performance
benchmark. It is the owner-node failure counterpart to Benchmark 52 and proves
that true Frontier+Succession composition survives loss of the entire owner
node while preserving per-task replay semantics.

Target semantics (R=2, K=4):
  1. Two independent normal tasks owned by the same worker join one Frontier.
  2. That owner directly exports both member refs to two distinct borrower
     workers. This direct owner->borrower hop is essential for real Succession
     candidate formation; a driver relay is not equivalent.
  3. The Frontier has ONE shared Succession topology and therefore commits
     exactly R non-owner holder admissions for the group, not R admissions per
     member.
  4. Both member recipes are available at the admitted group holders.
  5. The entire owner node is removed ungracefully while the executor node
     remains alive.
  6. A borrower requests task #2, the non-leader member.
  7. The same ObjectRef/ObjectID resolves after exactly one replay of task #2.
  8. The leader task is not replayed as a side effect.

A recovery-only pass is NOT sufficient: the benchmark must also prove the
shared Frontier Succession topology via exactly R holder admissions.

Use --holders R --witness-count W to choose positive independent counts.
Use --initial-piggyback-k K for K=2/4/8/16/32. This fills
one group, exports the leader last, repeats an export to the first borrower,
and requires R verified recipe-piggyback admissions with zero separate
holder-install RPCs before
killing the owner. Recover the last member only, with no other member replay.
--initial-k2-piggyback remains an alias for K=2.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from common import (
    read_marker,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
)

R = 2
W = 2
K = 4
NUM_TASKS = 2
TARGET_INDEX = 1
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 300.0
PROTECTION_TIMEOUT_S = 30.0
PROTECTION_STABLE_S = 0.75


def frontier_succession_system_config() -> dict:
    config = system_config(
        succession(R),
        witness_count=W,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    config.update(
        {
            "enable_recovery_frontier": K > 1,
            "recovery_frontier_group_size": K,
            # This knob belongs to the old fixed-R density proxy and must not
            # participate in the real Frontier+Succession experiment.
            "recovery_baseline_perf_protect_every_n": 1,
        }
    )
    return config


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
            # Keep every original execution unavailable. A replay of this
            # deterministic logical task observes its prior START and returns.
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
    class Borrower:
        def hold(self, wrapped_refs, leader_last: bool = False):
            # Nested refs force normal task-argument recovery metadata transport
            # while keeping the ObjectRefs alive on this borrower.
            self.refs = list(reversed(wrapped_refs)) if leader_last else list(wrapped_refs)
            return [ref.hex() for ref in self.refs]

        def read(self, index: int):
            ref = self.refs[index]
            return ref.hex(), ray.get(ref)

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return global_worker.core_worker.get_recovery_succession_profile()

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch_and_export(
            self,
            executor_node_id: str,
            tokens: list[str],
            marker_path: str,
            payload_bytes: int,
            borrowers,
            duplicate_first_borrower: bool = False,
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

            # Succession candidate formation happens at downstream CoreWorkers.
            # Submit these actor calls FROM THE OWNER so recovery metadata for
            # the producer refs is attached by the producer owner's manager.
            # Exercise non-leader metadata before the leader recipe. The
            # receiver restores application order only after native admission.
            exported_refs = list(reversed(refs)) if duplicate_first_borrower else refs
            if duplicate_first_borrower:
                # The first two exports go to the same worker. They must not
                # consume another independent holder's recipe opportunity.
                ray.get(borrowers[0].hold.remote(exported_refs, True), timeout=GET_TIMEOUT_S)
            held_ids = ray.get(
                [
                    borrower.hold.remote(exported_refs, bool(duplicate_first_borrower))
                    for borrower in borrowers
                ]
            )
            object_ids = [ref.hex() for ref in refs]
            return object_ids, held_ids

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return global_worker.core_worker.get_recovery_succession_profile()

    return Owner, Borrower


def _owner_profile_signature(profile: dict) -> tuple[int, ...]:
    return tuple(
        int(profile.get(key, 0))
        for key in (
            "candidate_reports_received",
            "candidate_reports_accepted",
            "holder_admissions_committed",
            "holder_install_rpcs_sent",
            "holder_install_rpcs_completed",
            "holder_commit_rpcs_sent",
            "holder_commit_rpcs_completed",
            "witness_update_rpcs_sent",
            "witness_update_rpcs_completed",
            "manifest_generations_committed",
            "max_non_owner_holders",
        )
    )


def _owner_async_outstanding(profile: dict) -> int:
    return sum(
        max(0, int(profile.get(sent, 0)) - int(profile.get(done, 0)))
        for sent, done in (
            ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
            ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
            ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
        )
    )


def wait_for_protection_quiescence(owner, borrowers, timeout_s: float) -> dict:
    """Wait until candidate/admission activity reaches a stable completed state.

    Waiting merely for >=R admissions is insufficient: ordinary per-task
    Succession can transiently pass through R admissions on its way to
    NUM_TASKS*R. Requiring the owner counters to stop changing prevents a false
    grouped-success result.
    """
    deadline = time.monotonic() + timeout_s
    last: dict = {}
    last_signature: tuple[int, ...] | None = None
    stable_since: float | None = None

    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        reports = int(last.get("candidate_reports_received", 0))
        admissions = int(last.get("holder_admissions_committed", 0))
        max_holders = int(last.get("max_non_owner_holders", 0))
        signature = _owner_profile_signature(last)
        now = time.monotonic()

        ready = (
            reports >= R
            and admissions >= R
            and max_holders >= R
            and _owner_async_outstanding(last) == 0
        )

        if ready:
            if signature == last_signature:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= PROTECTION_STABLE_S:
                    return last
            else:
                stable_since = now
        else:
            stable_since = None

        last_signature = signature
        time.sleep(0.05)

    borrower_profiles = []
    for borrower in borrowers:
        try:
            borrower_profiles.append(ray.get(borrower.recovery_profile.remote()))
        except Exception as exc:  # diagnostic only
            borrower_profiles.append({"profile_error": repr(exc)})

    raise TimeoutError(
        "Frontier+Succession protection did not become ready/quiescent: "
        f"owner={last}; borrowers={borrower_profiles}"
    )


def main() -> None:
    global R, W, K, NUM_TASKS, TARGET_INDEX
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
    initial.add_argument("--ordinary-k1", action="store_true")
    args = parser.parse_args()
    if args.holders <= 0 or args.witness_count <= 0:
        parser.error("R and W must be positive")
    if sys.flags.optimize:
        parser.error("Run without -O: correctness checks require assertions")
    R, W = args.holders, args.witness_count
    if args.ordinary_k1:
        K, NUM_TASKS, TARGET_INDEX = 1, 1, 0
    if args.initial_piggyback_k:
        K = args.initial_piggyback_k
        NUM_TASKS = K
        TARGET_INDEX = K - 1
        os.environ["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
        os.environ["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_succession_nonleader_node_failure_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_succession_system_config(),
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
        # Succession witnesses are control-plane durability nodes. They are
        # distinct from the CoreWorker holders admitted below.
        for i in range(W):
            cluster.add_node(
                num_cpus=0,
                resources={f"witness_node_{i}": 1},
            )
        for i in range(R):
            cluster.add_node(
                num_cpus=1,
                resources={f"borrower_node_{i}": 1},
            )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        expected_nodes = 1 + 1 + 1 + W + R
        wait_for_cluster(ray, expected_nodes, 30.0)

        Owner, Borrower = types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        borrowers = [
            Borrower.options(
                resources={f"borrower_node_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(R)
        ]

        tokens = [f"member-{i}-{uuid.uuid4().hex}" for i in range(NUM_TASKS)]

        # Do not relay producer ObjectRefs through the driver. The actual owner
        # creates them and directly submits the borrower actor calls that carry
        # the nested refs and their recovery metadata.
        object_ids, held_ids = ray.get(
            owner.dispatch_and_export.remote(
                executor_node.node_id,
                tokens,
                str(marker),
                PAYLOAD_BYTES,
                borrowers,
                args.initial_piggyback_k,
            )
        )
        assert len(object_ids) == NUM_TASKS
        assert len(set(object_ids)) == NUM_TASKS
        for ids in held_ids:
            assert ids == object_ids, (ids, object_ids)

        starts = wait_for_marker(
            marker,
            "START",
            timeout_s=60.0,
            min_count=NUM_TASKS,
        )
        assert len(starts) >= NUM_TASKS, read_marker(marker)
        for token in tokens:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        profile = wait_for_protection_quiescence(
            owner,
            borrowers,
            PROTECTION_TIMEOUT_S,
        )
        initial_manifest_builds = int(profile.get("initial_manifest_build_count", 0))
        reports_received = int(profile.get("candidate_reports_received", 0))
        reports_accepted = int(profile.get("candidate_reports_accepted", 0))
        admissions = int(profile.get("holder_admissions_committed", 0))
        max_holders = int(profile.get("max_non_owner_holders", 0))

        # Decisive composition assertion: two members x R holders would produce
        # NUM_TASKS*R admissions under ordinary task-centric Succession. A real
        # shared Frontier topology admits each of the R holders only once.
        assert admissions == R, (
            "Expected exactly R group-holder admissions; Frontier is still "
            "using per-task Succession topology",
            profile,
        )
        assert max_holders == R, profile
        if args.initial_piggyback_k:
            borrower_profiles = ray.get(
                [borrower.recovery_profile.remote() for borrower in borrowers],
                timeout=PROTECTION_TIMEOUT_S,
            )
            evidence = {"owner": profile, "borrowers": borrower_profiles}
            assert profile.get("initial_install_profile_version") == 3, evidence
            assert int(profile.get("frontier_recipe_piggyback_admissions", 0)) == R, evidence
            assert int(profile.get("holder_install_rpcs_sent", 0)) == 0, evidence
            assert int(profile.get("holder_install_rpcs_completed", 0)) == 0, evidence
            for borrower_profile in borrower_profiles:
                if os.environ.get("RAY_RECOVERY_SHARED_HOLDER_RECIPE") == "1":
                    assert borrower_profile.get("shared_holder_recipe_enabled"), evidence
                    assert int(borrower_profile.get(
                        "shared_holder_recipes_current", 0
                    )) == NUM_TASKS, evidence
                assert int(borrower_profile.get(
                    "frontier_recipe_piggybacks_stored", 0
                )) == 1, evidence
                assert int(borrower_profile.get(
                    "frontier_holder_materialize_members", 0
                )) == NUM_TASKS, evidence

        failure_wall_ns = time.time_ns()
        failure_start = time.perf_counter()

        # Remove the entire owner node. The executor node is deliberately not
        # removed, so this isolates owner-node loss from executor-node loss.
        cluster.remove_node(owner_node, allow_graceful=False)

        recovered_object_id, value = ray.get(
            borrowers[0].read.remote(TARGET_INDEX),
            timeout=GET_TIMEOUT_S,
        )
        failure_to_result_s = time.perf_counter() - failure_start

        assert recovered_object_id == object_ids[TARGET_INDEX]
        assert value["token"] == tokens[TARGET_INDEX]
        assert len(value["payload"]) == PAYLOAD_BYTES

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

        print("PASS: Recovery Frontier + Succession non-leader owner-node failure")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  W                         = {W}")
        if args.initial_piggyback_k:
            print(f"  verified recipe piggybacks = {R}; separate install RPCs = 0")
        print(f"  initial manifest builds   = {initial_manifest_builds}")
        print(f"  candidate reports recv    = {reports_received}")
        print(f"  candidate reports accept  = {reports_accepted}")
        print(f"  holder admissions         = {admissions}")
        print(f"  max non-owner holders     = {max_holders}")
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
