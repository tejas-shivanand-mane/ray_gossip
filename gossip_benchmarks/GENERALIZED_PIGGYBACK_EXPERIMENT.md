# Generalized Succession piggyback paths

Baseline: `a1e5ed7c146fe29b6836e79d01e8503e1b0b3cd2`, the preceding shared-recipe
experiment. This follow-up replaces the R=W=2 restriction on full initial
Frontier piggybacks with positive configured R and W. Sender and receiver require
matching configured counts and a complete selected witness list. Cases outside
the shortcut retain ordinary installation. K=1 uses an R-send budget instead of
a two-send boolean. Transport attempts never count as distinct admitted holders.

Witness acknowledgement semantics, provisional confirmation, group append
barriers, retry accounting, tombstones and replay identity are unchanged.
Full-group repeated exports have no send quota; a duplicate export cannot deny
another eligible failure domain its recipe. K=1 candidates beyond the send
budget use ordinary installation. Enough eligible nodes/borrowers are still
required to reach the target R; no missing redundancy is invented.

Fixed-R already selects R witness-holders and waits for all of them. Its actual
witness count is R by design; arbitrary equal R/W does not need this change.
Independent W!=R for Fixed-R would be a different algorithm. H2-readiness
profiling remains R=2-specific because it measures a two-holder event, not a
protocol restriction.

After rebuilding:

```bash
python gossip_benchmarks/11_generalized_succession_correctness.py
```

Default: 48 cases over (R,W)=(1,1),(1,3),(2,2),(2,3),(3,1),(3,3), shared holder
recipes OFF and ON. Each setting runs K=1 owner-node failure,
a full K=32 group with duplicate first-borrower export and owner-node failure,
witness-ACK/commit-gap recovery, and blocked holder confirmation that must
prevent replay. Successful full-group cases require R actual admissions,
R piggyback admissions and zero separate install RPCs. Shared-ON full-group
cases also require all recipes to be present through shared storage. Commit-gap
cases deliberately have one provisional holder and do not claim R-holder durability.

Narrow or extend the matrix:

```bash
python gossip_benchmarks/11_generalized_succession_correctness.py --rw 3:1 1:3 --shared-recipes 1
python gossip_benchmarks/11_generalized_succession_correctness.py --rw 4:2 --k 4
```

The node-failure and commit-gap fixtures accept `--holders` and `--witness-count`
directly; defaults remain 2/2 for existing suites. Use benchmark 08 for the
existing equal-R/W throughput sweep and benchmark 10 for paired shared-recipe
performance at R=W=2. Correctness cases are not throughput evidence.

README remains unchanged pending validation. Manual source review only;
no builds, tests, lint, benchmarks or rendering executed. No performance or
correctness pass is claimed until these cases run on the rebuilt native fork.
