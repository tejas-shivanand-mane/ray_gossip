# Shared immutable holder recipe experiment

Baseline before this experiment: `4a4ec13927091ae870ae5ecf7fc6f6720b8381b0`.
The earlier pre-ACK baseline remains `289bce5f99940f1b96990b725e1bdc1f0b34d624`.
Changes are forward-only; revert the experiment commit with `git revert -s`
and rebuild if necessary. The flag is OFF by default.

The flag `enable_recovery_succession_shared_holder_recipe` lets Succession
Frontier holder task state share the immutable recipe already retained by the
group, rather than copying it again. Initial piggybacks, install RPCs and dynamic
appends use the same helper. Unclean recipes fall back to the existing sanitized
copy. Manifests and provisional/committed/reservation state remain separate.
Replay and admission create mutable copies with the current manifest as before.
Reset, replacement and tombstoning release the shared reference; the recipe
lives until its last group/task reference is released. Fixed-R and K=1 do not
use the sharing shortcut. The wrapper adds a shared pointer to task-state storage,
so net memory savings require measurement rather than inference from bytes alone.

After rebuilding, run:

```bash
python gossip_benchmarks/10_shared_holder_recipe_performance.py
```

This uses the same 32-case paired design as experiment 09: four repetitions,
Fixed-R/Succession at K=1/K=32, R=W=B=2, 1 KiB objects and separate 1 KiB recipe
padding. OFF/ON order alternates. Batch ACKs are explicitly OFF in both arms.
Fixed-R and K=1 are controls; only Succession K>1 takes the sharing path.
JSON records the actual native flag, paired CSVs report changes within each
method/K, and Student-t 95% intervals summarize those paired changes.
No plots or README changes are included.

For service counters and CPU/task, run existing profiling separately:

```bash
RAY_RECOVERY_WITNESS_BATCH_ACK=0 RAY_RECOVERY_SHARED_HOLDER_RECIPE=0 python gossip_benchmarks/03_profile.py --ks 1 32
RAY_RECOVERY_WITNESS_BATCH_ACK=0 RAY_RECOVERY_SHARED_HOLDER_RECIPE=1 python gossip_benchmarks/03_profile.py --ks 1 32
```

Read the raw service JSON or `service/all_counters.csv`, not only `service.log`.
New exported fields:

- `shared_holder_recipe_enabled`: native selection (K=1 can report selected but
  has no Frontier holder copies to share).
- `holder_recipe_copies_avoided`: successful assignments of a shared group recipe.
- `holder_recipe_fallback_copies`: copied assignments, including the flag-OFF arm.
- `shared_holder_recipes_current`: task states currently sharing a recipe.
- `shared_holder_recipe_bytes_current`: serialized size of those referenced
  recipes; this is NOT heap memory or RSS saved. Gauges are recomputed when
  profiling snapshots are requested; event counts reset with the usual reset.

Compare holder materialization time, service CPU/task, throughput, protection
delay, and owner-failure latency. The recipe still exists in the group, so wire
bytes and recipe parsing are unchanged. No speedup is established.

Existing correctness and owner-node diagnostics can select the experiment:

```bash
RAY_RECOVERY_WITNESS_BATCH_ACK=0 RAY_RECOVERY_SHARED_HOLDER_RECIPE=1 python gossip_benchmarks/05_succession_correctness.py
RAY_RECOVERY_WITNESS_BATCH_ACK=0 RAY_RECOVERY_SHARED_HOLDER_RECIPE=1 python gossip_benchmarks/04_owner_failure_throughput.py --k 32
```

Use new result directories for separate runs. The owner-failure workload is a
paced diagnostic. The code was manually reviewed only: no builds, tests, lint,
benchmarks or rendering were executed by the coding agent.
