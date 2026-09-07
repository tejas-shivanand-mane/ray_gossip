# Recovery benchmark suite

Eight public commands replace the old numbered/phase experiments. Run from the
repository root using your rebuilt Ray Python environment. Plotting requires
`matplotlib`. No command builds Ray, changes kernel settings, or invokes sudo.

| Entry point | Purpose |
| --- | --- |
| `01_frontier_performance.py` | Disabled vs Fixed-R and Succession at K=1,2,4,8,16,32; throughput and overhead plots |
| `02_object_size_performance.py` | Returned object-size sweep: disabled plus both methods at K=1 and K=32; throughput and overhead plots |
| `03_profile.py` | Both methods: native service/control counters, process/thread CPU, optional perf stacks |
| `04_owner_failure_throughput.py` | Owner-worker failure: time-binned successful reads and fraction of unfinished objects recovered, against disabled |
| `05_succession_correctness.py` | Succession admission, owner failure, commit gap, provisional confirmation, appends, failover, concurrency, retries, late borrowers |
| `06_fixed_r_correctness.py` | Fixed-R owner/node failure, grouping/rollover, lifecycle/cleanup, concurrent claims, witness failure/stall, acting-owner handoff |
| `07_borrower_count_performance.py` | Disabled vs Fixed-R K=32 and Succession K=32 across application borrower counts, including B=1 < R/W |
| `08_replication_count_performance.py` | Disabled vs Fixed-R K=32 and Succession K=32 at R=W=1,2,3; fixed borrower count and topology |

`plotting/` contains the editable benchmark-specific plot files.
`_support/` contains workload code, shared plot utilities and isolated correctness fixtures;
these are implementation modules, not additional experiments to choose from.
The old standalone experiments, patch utility scripts, phase directory, and
tracked historical result snapshots have been removed. Git history retains them.
New results are ignored by Git; this change does not run a cleanup of your local
untracked results.

## Editing plots: complete Matplotlib control

Open the file for the benchmark in **`gossip_benchmarks/plotting/`** and edit
its **`draw()` function**.
Each file contains all figure creation, drawing, axis formatting, legends,
annotations, spacing, and export calls for that benchmark. There is no shared
appearance settings schema or rendering helper applying formatting afterward.
The former `plot_settings.py` has been removed; its committed defaults are
now written directly into these files.

| Benchmark | File under `gossip_benchmarks/plotting/` |
| --- | --- |
| 01: K comparison | `plot_01_frontier_performance.py` |
| 02: object-size comparison | `plot_02_object_size_performance.py` |
| 04: owner failure | `plot_04_owner_failure_throughput.py` |
| 07: borrower counts | `plot_07_borrower_count_performance.py` |
| 08: R/W counts | `plot_08_replication_count_performance.py` |

Generated figures use the same benchmark stems:
`01_frontier_performance_padding_<bytes>.png/.pdf`,
`02_object_size_performance.png/.pdf`,
`04_owner_failure_throughput.png/.pdf`,
`07_borrower_count_performance.png/.pdf`, and
`08_replication_count_performance.png/.pdf`.
Existing figures with older names are left in place by replotting.

03 writes profiling data/logs rather than figures. Correctness suites 05–06
have no plots.

Each drawing function is organized in this order:

1. **Figure defaults:** a local `plt.rc_context` for any rcParams.
2. **Figure and axes:** direct `plt.subplots` calls. Replace these with
   GridSpec, subfigures, another panel arrangement, or additional axes.
3. **Curves/error bars/bars:** direct `ax.errorbar`, `ax.plot`, or
   `ax.bar` calls. Styles are ordinary Matplotlib keyword arguments;
   edit calls or split the loop to make each curve/panel different.
4. **First axis:** direct scale, tick, label, limit, grid and spine calls.
5. **Second axis:** independent formatting calls.
6. **Legends, titles and annotations:** ordinary Matplotlib artists.
7. **Layout:** edit or replace `tight_layout`; use `subplots_adjust` if preferred.
8. **Final custom edits:** add any Matplotlib operations here. No subsequent
   helper resets labels, ticks, limits, or layout.
9. **Saving:** independent PNG/PDF `savefig` calls. Edit filenames,
   transparency, DPI, bounding boxes, metadata, formats, or add more exports.

All axes and artist handles are accessible in that function:
`fig`, `ax_throughput`, `ax_overhead` (or `ax_recovery`),
`throughput_artists`, and the legend/title/footer handles.
Add secondary axes, insets, custom locators/formatters, hatching, shaded regions,
broken-axis layouts, or entirely different plot types directly in the file.
You do not need to add support to another module first.

For example, in Benchmark 02's final-custom-edits section:

```python
from matplotlib.ticker import MultipleLocator, StrMethodFormatter

ax_throughput.set_xticks(
    [1024, 16384, 262144, 1048576],
    ["1 KiB", "16 KiB", "256 KiB", "1 MiB"],
    rotation=0,
)
ax_throughput.set_xlabel("Object size", fontsize=14, labelpad=10)
ax_throughput.set_ylim(0, 3200)
ax_throughput.yaxis.set_major_locator(MultipleLocator(500))
ax_throughput.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
ax_throughput.tick_params(axis="both", direction="in", length=6, width=1.2)
ax_throughput.spines["left"].set_linewidth(1.5)

# Edit a specific curve and its error-bar artists independently.
curve = throughput_artists["succession_k32"]
curve.lines[0].set_linewidth(3)
for cap in curve.lines[1]:
    cap.set_color("black")
for bars in curve.lines[2]:
    bars.set_alpha(0.4)

throughput_legend.remove()
throughput_legend = ax_throughput.legend(
    loc="upper right", frameon=False, fontsize=11,
)
```

Coordinates are:

| Plot | X coordinates |
| --- | --- |
| 01 | Positions 0–5 for the usual K=1,2,4,8,16,32 |
| 02 | Object sizes in bytes; default scale is log base 2 |
| 04 timeline | Seconds relative to owner kill |
| 04 recovery bars | Positions 0,1,2 for disabled, Fixed-R, Succession |
| 07 | Actual borrower counts; default scale is log base 2 |
| 08 | Actual equal R/W values, 1,2,3 |

Set limits after ticks, since Matplotlib may expand limits when setting ticks.
If you change subplot arrangement, also update the named axes and later layout
calls. When changing statistics displayed or removing error bars, update the
figure's explanatory text to match.

The first part of each file prepares plotting arrays from the supplied results;
`draw()` owns appearance. Shared `plots.py` only loads Matplotlib,
formats size text, validates/extracts data, and routes existing entry points.
Aggregation, benchmark execution and saved-data schemas stay in their existing
runner modules.

After editing, use the same saved-result commands:

```bash
python gossip_benchmarks/01_frontier_performance.py plot \
    --output-dir gossip_benchmarks/results/59_recovery_frontier_fixed_vs_succession_performance

python gossip_benchmarks/02_object_size_performance.py plot \
    --output-dir gossip_benchmarks/results/object_sizes_upto_1mib

python gossip_benchmarks/04_owner_failure_throughput.py plot \
    --output-dir PATH_TO_SAVED_OWNER_FAILURE_RUN

python gossip_benchmarks/07_borrower_count_performance.py plot \
    --output-dir gossip_benchmarks/results/borrower_counts

python gossip_benchmarks/08_replication_count_performance.py plot \
    --output-dir gossip_benchmarks/results/replication_counts
```

These commands do not run benchmark cases. Existing figures are replaced unless
you change the export filenames. 02 still supports `--exclude-object-sizes`.
04 also regenerates derived bucket/summary CSVs, as before; its trial JSON stays
intact. Its `--bucket-seconds` changes aggregation, not merely tick spacing.

## 1. Full K comparison

```bash
python gossip_benchmarks/01_frontier_performance.py \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 20 --overwrite
```

Default: 39 fresh-cluster cases. Output:
`gossip_benchmarks/results/frontier_performance/`.

The workload and raw CSV columns are retained from Benchmark 59. Both methods
use R=2/W=2, two independent borrowers, burst size 32, and 128 in-flight tasks.
K=1 disables Frontier; K>1 uses the same K for both methods. Native profiling is
OFF. Producer objects are 1 KiB by default; TaskSpec padding is a separate 1 KiB.

PNG and PDF plots show:
- application throughput, including the shared disabled reference;
- overhead vs the paired disabled run at each K;
- pointwise 95% Student-t intervals across repetitions.

These measure application completion, not completion of every holder admission.
Admission may overlap application work. The paired CSV also reports Succession
vs Fixed-R at equal K. A confidence interval crossing zero does not establish
which method is faster. Three repetitions are a screen, not a guarantee of
precision; 13 gives complete cyclic position balance if a later study needs it.

Replot your existing B59 results without another Ray run:

```bash
python gossip_benchmarks/01_frontier_performance.py plot \
    --output-dir gossip_benchmarks/results/59_recovery_frontier_fixed_vs_succession_performance
```

For new results, the same `plot` command without `--output-dir` uses the new default.

## 2. Object-size effect

```bash
python gossip_benchmarks/02_object_size_performance.py \
    --object-sizes 1KiB 16KiB 256KiB 1MiB 4MiB \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 20 --overwrite
```

Default: 75 fresh-cluster cases. Output:
`gossip_benchmarks/results/object_sizes/`, including
`02_object_size_performance.png` and `.pdf`, raw/summary/paired CSVs, settings,
source/build provenance, and case logs.

Five curves: disabled, Fixed-R K=1, Fixed-R K=32, Succession K=1, Succession K=32.
Only returned producer-object size changes; recipe padding and other workload
parameters stay fixed. Both borrowers read each object. Overhead is relative
to disabled at the **same size and repetition**. Resume requires identical
settings and provenance.

```bash
python gossip_benchmarks/02_object_size_performance.py plot
```

To omit 4 MiB from the saved plots without rerunning any cases:

```bash
python gossip_benchmarks/02_object_size_performance.py plot \
    --exclude-object-sizes 4MiB
```

This regenerates the PNG/PDF and prints the comparison for the remaining sizes.
Raw results, configuration, and summary/paired CSVs retain every measured size.
Use `plot` without exclusions to restore the full-size plots.

## 3. Profiling

```bash
python gossip_benchmarks/03_profile.py
```

Default: service snapshots and system CPU at all six K values, both methods.
System profiling uses three repetitions per method/K. Use
`--ks 1 32` or `--modes service` to narrow an investigation.

Service output includes every exported native counter per owner/borrower in
JSON and `all_counters.csv`, plus interpreted stage timings and count/coverage
checks: registration/export, recipe encoding/materialization, copies/bytes,
candidate/admission work, RPC counts, witness queue/CQ/main-loop timing, H2
readiness, and available lifecycle/current/peak counters. Unexercised paths
remain zero; this does not claim to exercise every recovery path.

System output includes process-class and thread-group CPU, gRPC/CQ,
CoreWorker I/O, context switches and endpoint churn. Ended processes/threads
may be missed, so affected estimates are explicitly reported as lower bounds.
Service elapsed time includes preemption/locks; it is not process CPU.
Nested timings and inclusive categories overlap and must not be summed.

Optional native stacks, through the same entry point:

```bash
python gossip_benchmarks/03_profile.py --modes native --ks 1 32
```

Native mode requires Linux perf and user-space perf permission. Symbol coverage
and exited-thread samples are retained. Profiling throughput is diagnostic;
use experiment 01 or 02 for performance conclusions. Logs and raw files use
a fresh timestamped directory under `results/profile/`.

## 4. Owner-node failure and resumption

```bash
python gossip_benchmarks/04_owner_failure_throughput.py
```

Default: one fresh-cluster case for disabled and each method, K=1, R=2/W=2.
Use `--k 32` for grouped recovery and `--trials 3` for repetitions.

The actual owner creates 32 distinct objects, retains their references and
exports them directly to two independent borrowers. Original executions are
gated. Eight objects are released/read before the entire owner Ray node is terminated
ungracefully (`cluster.remove_node(..., allow_graceful=False)`);
the remaining original gates stay closed. Successful post-failure reads must
retain the ObjectID and have an observed recovery replay. No replacement owner
submits a new workload. The head/GCS, executor and both borrower nodes survive. These are seven logical
Ray nodes on one physical host; the injected failure removes the dedicated
owner node, not the host.

The plot starts at time zero immediately before the first pre-failure gate is
released (after cluster/protection setup). The x-axis is time since simulation
start, in seconds. Throughput bins are recomputed from saved elapsed read events;
colored vertical lines mark each trial's actual owner failure time. The default 60-second
post-failure observation budget begins immediately before node termination and
includes node-removal time; reads begin as soon
as removal returns, without waiting for GCS to mark the node dead. The run
verifies node death before saving a successful result. JSON records the failure
type, node ID, removal-completion time and final node-death confirmation time
(the latter is not the initial failure-detection latency). Reads remain paced
at 0.25 seconds with a 30-second per-read timeout and a 64 KiB payload.

This is **paced read throughput over a finite backlog**, not steady-state task
production throughput. Both borrowers must succeed for an object to count.
Plots end at observation/backlog completion; failures and unattempted objects
are reported separately. Disabled is expected to lose unfinished objects,
but the benchmark records observed behavior rather than drawing a forced zero.
Protection counters are enabled for setup evidence, so this is diagnostic.

Output: timestamped `results/owner_failure/`, raw events/protection/replay
evidence in JSON, bucket/summary CSVs, `04_owner_failure_throughput.png` and `.pdf`.
Fixed-R uses a thick blue solid line with hollow squares; Succession uses a
thin orange dashed line with smaller filled dots. Both remain visible when
their data coincide, without shifting measured values. All style code remains
editable in `plotting/plot_04_owner_failure_throughput.py::draw()`.
Replotting legacy worker-failure JSON keeps the owner-worker label; it cannot
convert an old run into a node-failure experiment. Mixed failure types are rejected.

```bash
python gossip_benchmarks/04_owner_failure_throughput.py plot --output-dir PATH_TO_SAVED_RUN
```

## 5–6. Correctness

```bash
python gossip_benchmarks/05_succession_correctness.py
python gossip_benchmarks/06_fixed_r_correctness.py
```

Each scenario has its own subprocess, cluster, timeout, log and assertions.
Suites stop on the first failure. Do not use Python `-O`.
Use `--list` to inspect coverage and `--cases NAME ...` for selected cases.

Succession has 23 scenarios by default: owner-node loss, witness-ACK/local-commit
gap, and blocked provisional confirmation at each K=1,2,4,8,16,32; partial
groups; dynamic append; failed-append atomicity; and a three-case ordinary K=1
failover/concurrent-recovery/retry-budget fixture with late owner exports.
That last fixture reports each constituent result and exits nonzero on failure.
The commit-gap fixture deliberately stops after the first provisional holder's
witness publication: configured R stays 2, but it does not claim two completed
admissions in that fault window.

Fixed-R has 10 scenarios: K=1 regression, nonleader owner-node failure,
K=4 full group plus rollover, lifecycle, terminal cleanup, concurrent claims,
authoritative witness loss, live-witness stall without unsafe promotion,
acting borrower death, and an in-flight acting-owner handoff. Adversarial
witness cases now use R=2/W=2; they are not an R=3 performance baseline.

These are broad regression suites, not a proof or an exhaustive fault-state
enumeration. The new arrangement and fixture adaptations still need execution
on your system. No local build, test, benchmark or plot execution was performed
when this consolidation was prepared.

## 7. Borrower-count effect, including fewer borrowers than R/W

Compare **disabled**, **Fixed-R K=32**, and **Succession K=32** with a configurable
number B of application borrowers per producer object. Default counts are
**1, 2, 4, 8, 16**; R=2 and W=2 remain fixed, including at B=1.

```bash
python gossip_benchmarks/07_borrower_count_performance.py \
    --borrower-counts 1 2 4 8 16 \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 30
```

Default: **45 fresh-cluster cases**, profiling OFF, object payload 1 KiB, separate
TaskSpec padding 1 KiB, burst size 32, and 128 in-flight producer pipelines.
All three variants use the same application workload at a given B.
Three repetitions balance each variant across the three execution positions
within each borrower count. Use six repetitions for two full cycles; this
does not guarantee narrow confidence intervals.

**What B=1 means:** it is one real downstream application borrower, not two
borrowers disguised as one, and neither R nor W is lowered. The executor is not
automatically admitted as a Succession holder: the current implementation admits
downstream borrower candidates. With fewer than R borrowers, Succession can
remain below its target R while Fixed-R retains its witness-holder replication
policy. This measures the application cost of each policy under low fan-out;
it must not be presented as a comparison at equal achieved durability.
No extra holder admissions or dummy application consumers are forced.

The owner exports every object directly to each borrower. A pipeline counts as
complete only after **all B borrowers return the correct object value**. The
denominator is producer pipelines, not individual borrower reads: one pipeline
at B=16 includes sixteen consumes. In-flight producer count stays fixed, so
outstanding borrower calls grow with B. Completion can overlap holder admission;
this benchmark does not measure durable coverage or recovery.

Topology is one head/owner node, one producer/executor node, B distinct borrower
nodes, and two additional witness nodes (**B+4 logical Ray nodes**, for every
variant). All are created locally by Ray's Cluster helper. R/W do not grow with B,
but local process count, configured CPU resources, and memory footprint do.
Node-distinct placement is not a claim of distinct physical machines. Higher B
therefore measures fan-out under this topology, not pure borrower metadata cost
on a fixed-size cluster. Existing 01/02 defaults remain two borrowers.

Output directory: `results/borrower_counts/` under `gossip_benchmarks/`:

- `borrower_count_runs.csv`: every completed case, completion/latency counts,
  actual borrower count, target holders/witnesses, and variant execution position.
- `borrower_count_summary.csv`: throughput and latency statistics by B/variant.
- `borrower_count_paired.csv`: overhead relative to disabled at the **same B and
  repetition**, plus Succession's percentage speedup over Fixed-R K=32.
- `07_borrower_count_performance.png/.pdf`: throughput and paired overhead with
  pointwise 95% Student-t intervals.
- `run_config.json` and per-case logs: settings, source/native provenance.
  The native extension SHA-256 identifies the binary even when Ray's build
  commit is a placeholder; it does not recover the missing build commit.

Completed cases are journaled after each success. Repeat the identical run
command to resume. Settings, source commit/content, and native binary must match.
Use a fresh `--output-dir` to retain another run; `--overwrite` starts again
by removing only this benchmark's named outputs in the selected directory.

To run only the below-R case:

```bash
python gossip_benchmarks/07_borrower_count_performance.py \
    --borrower-counts 1 \
    --output-dir gossip_benchmarks/results/borrowers_one
```

For plot edits, edit `draw()` in `plotting/plot_07_borrower_count_performance.py`, then:

```bash
python gossip_benchmarks/07_borrower_count_performance.py plot \
    --output-dir gossip_benchmarks/results/borrower_counts
```

Replotting reads the saved journal/configuration, requires all configured cases
to be complete, and replaces only the figures. It does not run Ray cases or
rewrite raw/summary/paired CSVs. Tick positions are actual B values; default
x spacing is log base 2. Larger layout changes belong in
`plotting/plot_07_borrower_count_performance.py`.

Implementation was manually source/diff-reviewed; no build, test, benchmark,
lint, or plot rendering was run while preparing this benchmark.


## 8. Replication-count effect: R=W=1,2,3

```bash
python gossip_benchmarks/08_replication_count_performance.py \
    --rw-values 1 2 3 \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 30
```

Compare disabled, Fixed-R K=32, and Succession K=32 while varying **R and W
together**. R is the target number of non-owner lineage holders; W is the
configured witness count. This sweep does not isolate the cost of changing
only R or only W.

Default: **27 fresh-cluster cases**, profiling OFF. Application fan-out stays
at **three borrowers** at every R/W value. Each case also has the same **eight
local Ray nodes**: head/owner, producer/executor, three distinct borrower nodes,
and three additional witness-capable nodes. Only the configured R/W changes;
the extra witness-capable nodes remain present at R/W=1 and 2. Three configured
witness-capable nodes do not mean W is forced to three.

Object payload and separate TaskSpec padding are both 1 KiB; burst size is 32,
in-flight producer pipelines 128, and timed duration 30 seconds. Every borrower
must consume the correct result before a pipeline counts as completed.
No failures are injected, and holder admission can overlap application
completion. Having at least R borrower candidates is not a measurement of
achieved durable coverage.

Disabled is rerun in every R/W/repetition block with recovery OFF and the
same topology. Its row's R/W identifies the comparison block, not active
replication. Overheads use that block's disabled run; Succession vs Fixed-R
ratios are also paired within the block. Three repetitions balance both
variant positions and, for the full three-value sweep, R/W block positions.
Printed results and CSVs include means, CVs, pointwise 95% Student-t intervals,
and the equal-K method comparison. Six repetitions give two balanced cycles.

Use `--borrowers N` to choose a different **fixed** fan-out for the whole sweep.
The default of three supplies enough downstream candidates for the largest R.
One borrower is allowed if deliberately studying B<R; in that case Succession
can remain below its target R and these are not equal-achieved-durability
comparisons. Physical-host failure independence is not established by this
local multi-node benchmark.

The existing runtime has some optimized paths specialized for R=2/W=2.
This benchmark uses the current implementation unchanged, including its general
paths at other R/W values. Differences can reflect both replication cost and
which implementation paths apply. No Frontier algorithm change is made.
Only this benchmark opts into varying R/W; benchmarks 01–07 keep their existing
R=2/W=2 behavior.

Outputs under `gossip_benchmarks/results/replication_counts/`:

- `replication_count_runs.csv`: completed-case journal including R, W, fixed
  borrower count, provisioned witness-node count, completion/latency counts,
  and execution position.
- `replication_count_summary.csv` and `replication_count_paired.csv`:
  statistics separated by R/W and method.
- `08_replication_count_performance.png/.pdf`: application throughput and paired
  overhead across R/W.
- `run_config.json` and case logs: settings plus source/native fingerprints.

Repeat the same command to resume interrupted runs. Resume requires matching
configuration, source commit/content, and native binary. Use a different output
directory to preserve another run, or `--overwrite` to replace this benchmark's
named outputs.

Edit `draw()` in `plotting/plot_08_replication_count_performance.py` for complete control over
this figure, then regenerate only figures:

```bash
python gossip_benchmarks/08_replication_count_performance.py plot \
    --output-dir gossip_benchmarks/results/replication_counts
```

The saved configuration and all configured cases are validated before plotting.
Replotting does not execute cases or rewrite CSVs. Larger layout changes belong
in `plotting/plot_08_replication_count_performance.py`; tick coordinates are the actual R/W values.

This addition was manually source/diff-reviewed only. No build, test, lint,
benchmark, or plot rendering was run.

## Benchmark 04 failure diagnostics

An unexpected borrower worker death aborts the case. It is not counted as an
ordinary read timeout or a completed measurement. On a Python exception, the
child attempts to save `diagnostics/trial_<N>_<method>/` under its result
directory before shutting down the cluster and removing execution markers.

`failure.json` records the traceback, experiment phase, settings, object IDs,
read events and last observed protection profile. `markers/` preserves tails
of execution-start records. Session folders contain up to 64 KiB per stderr,
native core-worker and raylet log; `manifest.json` maps copies to original paths
and records copy errors. These are bounded snapshots taken before shutdown,
not complete logs. A forcibly terminated driver may not capture diagnostics.
Failure JSON files are separate from the successful trial files used by plots.

The parent prints the child log tail on a nonzero exit or timeout. For an EOF /
`ActorDiedError`, inspect the failed borrower's native log and stderr; the generic
message alone does not establish OOM versus a native crash. This diagnostic
change does not fix the underlying recovery runtime failure.

Manual source review only; no tests, builds, lint, benchmarks or plot rendering
were run for this change.
