# Bounded owner witness acknowledgement experiment

## Baseline and rollback

Pre-experiment `main`, verified through GitHub on 2026-09-07:

`289bce5f99940f1b96990b725e1bdc1f0b34d624`

This SHA is the historical baseline, not a claim that the experiment works or
improves performance. The experiment is opt-in on the new native build:

```python
_system_config={"enable_recovery_witness_batch_ack": True}
```

The native default is `false`. Benchmark helpers accept
`RAY_RECOVERY_WITNESS_BATCH_ACK=0` or `1`; unset retains the default.
Use flag OFF for the first same-build comparison. To undo the implementation,
revert the experiment commit with `git revert -s <experiment-commit-sha>` on
`main`, preserving subsequent history, and rebuild. Do not reset `main` to
the baseline SHA. The delivery message supplies the experiment commit SHA.

## What changes

The existing witness client still sends its first item immediately, allows
one physical batch in flight per client, and sends queued batches of at most
32 items. Requests, replies, receiver validation and receiver locking are
unchanged. There are no sequence numbers, cumulative wire prefixes or timers.

When enabled, ordinary publications carry a local owner handler and completion
context instead of a separate transport callback. On receipt, adjacent results
for the same handler are passed together as views into the physical reply.
One owner-wide mutex protects publication counter updates for that segment.
The legacy path takes one publication-state mutex per logical reply.

Every publication still has its own witness count, successes, completion count,
newest conflicting manifest, and exactly one local completion callback.
Ordinary Succession succeeds on its first successful witness acknowledgement.
Fixed-R recipe installation waits for all selected witness-holders and requires
all of them to succeed. Failure results and malformed reply lengths fail closed.
Stored results from unrelated records never satisfy another record's threshold.

Admission, rollback and group-prefix commitment use their existing continuations,
which execute outside the shared mutex. Tombstones, certificate publication,
certificate-mode ordinary publication, and Fixed-R recovery claims retain their
legacy callback paths. Provisional-holder witness confirmation and replay are
unchanged. Mixed batches retain item order by splitting at legacy callbacks or
different handlers. This is not a change to replay deduplication or side effects.

The experiment removes per-item transport callback dispatch and reply swapping
on this path and amortizes owner publication locking. It does not remove record
validation, installation, per-publication outcomes, or admission transitions.
It adds local completion contexts, temporary result/completion vectors and shared
locking across witness clients. These costs may outweigh the savings, especially
for singleton batches. Batch bookkeeping precedes its individual continuations,
so protection latency must be checked even though no timer was added.

## Throughput comparison

Rebuild the native fork first, then run:

```bash
python gossip_benchmarks/09_witness_batch_ack_performance.py
```

Default: 4 repetitions, 32 fresh-cluster cases. Each repetition compares flag OFF
and ON for Fixed-R K=1/K=32 and Succession K=1/K=32. R=W=B=2, six logical nodes
on one physical host, 1 KiB objects, separate 1 KiB recipe padding, burst 32,
128 in-flight producer pipelines, 5-second warm-up, 1-second settling interval,
20-second measurement. Profiling is OFF. Producer and both borrower reads must
complete for one pipeline to count. This reuses experiment 01's timed workload.

Each pair runs consecutively; its OFF/ON order alternates across repetitions.
Variant order is seeded and shuffled per repetition. No disabled case is needed
to measure this within-method change, and the summary does not report overhead
relative to disabled. Every child checks its actual native ACK flag outside the
timed window. A fresh output directory prevents combining incompatible runs.

Outputs: per-case JSON/logs, `experiment.json` with the historical baseline and
installed Ray build metadata, `witness_batch_ack_runs.csv`,
`witness_batch_ack_paired.csv`, and `witness_batch_ack_summary.csv`.
Throughput changes are computed within each OFF/ON pair before averaging;
pointwise Student-t 95% intervals are reported. No plots are generated.

## Profiling and protection checks

Use the existing profiler separately for each mode, with new output directories:

```bash
RAY_RECOVERY_WITNESS_BATCH_ACK=0 python gossip_benchmarks/03_profile.py --ks 1 32
RAY_RECOVERY_WITNESS_BATCH_ACK=1 python gossip_benchmarks/03_profile.py --ks 1 32
```

The service profiler retains all exported owner/borrower counters in JSON and
`all_counters.csv`. New counters (reset by the normal profile reset) are:

| Counter | Interpretation |
|---|---|
| `witness_batch_ack_enabled` | Handler enabled in this CoreWorker; available even with profiling OFF |
| `witness_ack_batches_processed` | Adjacent result segments processed; one bookkeeping lock each |
| `witness_ack_batch_items_processed` | Logical witness results in those segments |
| `witness_ack_batch_bookkeeping_time_ns` | Batch bookkeeping elapsed time, including lock waiting; excludes per-item profiling updates and admission continuations |
| `witness_ack_batch_lock_wait_time_ns` | Portion spent waiting for the shared owner mutex |

Timing/count counters are collected only with native profiling ON. The existing
logical callback time includes amortized batch bookkeeping plus each item's
continuation. Existing physical/logical RPC, admission, publication, and transport
timings remain available. Timings are elapsed service measurements, not process
CPU, and overlapping categories must not be summed. Compare actual CPU/task
using system-profile output, and use the unprofiled paired run for throughput.

Check items/batches to establish whether batching is actually exercised. Compare
admission/publication delay and completed protection counts as well as CPU/task;
application completion alone does not prove that R holders were installed.
The fixed-size K=32 service workload can generate few independent publications;
use `--tasks 1024` with service mode to inspect more groups if needed.

Run the existing correctness and owner-node failure cases with the flag enabled:

```bash
RAY_RECOVERY_WITNESS_BATCH_ACK=1 python gossip_benchmarks/05_succession_correctness.py
RAY_RECOVERY_WITNESS_BATCH_ACK=1 python gossip_benchmarks/06_fixed_r_correctness.py
RAY_RECOVERY_WITNESS_BATCH_ACK=1 python gossip_benchmarks/04_owner_failure_throughput.py
```

Use fresh output paths for OFF/ON owner-failure comparisons. Its paced finite
backlog is a recovery diagnostic, not a maximum-throughput measurement.

Source review only at implementation time. No builds, tests, lint, benchmarks,
or plot rendering were executed by the coding agent; no speedup is established.
