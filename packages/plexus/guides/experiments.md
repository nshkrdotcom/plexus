# Experiments and measured results

Artifacts and runnable scripts are included in the repository and Hex package. `artifacts/environment.json` records the kernel commit, runtime and script hashes. These are single-machine observations, not scale guarantees.

## Node birth

`mix run experiments/node_birth.exs` runs 1k, 10k and 100k actors across 12/24/48 partitions, with no edges, a child star, and child plus two typed edges. All 27 configurations completed on Elixir 1.20.3 / OTP 29.0.5 with 24 online schedulers.

At 100,000 actors:

| Partitions | Topology | Actors/second | p50 / p95 / p99 birth latency (µs) |
| --- | --- | ---: | ---: |
| 12 | none | 16,560 | 45 / 116 / 315 |
| 12 | child | 16,006 | 46 / 124 / 379 |
| 12 | typed | 13,581 | 54 / 156 / 443 |
| 24 | none | 19,869 | 39 / 91 / 272 |
| 24 | child | 14,347 | 51 / 140 / 433 |
| 24 | typed | 12,583 | 58 / 165 / 473 |
| 48 | none | 17,879 | 43 / 103 / 319 |
| 48 | child | 16,420 | 46 / 116 / 337 |
| 48 | typed | 14,139 | 54 / 140 / 385 |

`artifacts/benchmarks/node-birth.jsonl` also retains scheduler utilization, owner/registry/partition mailbox lengths, total memory, incremental bytes per actor, and node/class/edge ETS memory. Births are issued sequentially; concurrent correctness is tested separately. Memory deltas include runtime overhead and are not GC-normalized. This is one trial per configuration; no million-actor claim is made. Historical partial diagnostic files show the earlier class/edge index bottlenecks; interrupted configurations are not results.

## Measurement throughput

`mix run experiments/measurement.exs` completed 384 configurations, each with 128 logical requests, with zero result errors. It sweeps batch size 1/8/32/64, delay 0/1/10/25 ms, SDK concurrency 1/8, in-flight batches 1/4, duplicate rates 0/25/75% and cache rates 0/50%.

The JSONL artifact separates logical throughput, physical HTTP requests, physical batch items, batch calls, request reduction and p50/p95 latency. It uses `TypeSafeSDK.Test`, preserving SDK serialization/validation but removing real network latency and provider limits. In SDK 0.4.0 a batch invokes individual item requests; batching alone does not collapse them into one HTTP request. Dedupe/cache reduce requests. These measurements characterize framework overhead, not provider capacity. A real provider rate-limit/error sweep remains deployment-specific and was not performed.

## Live calibration

With `TYPESAFE_API_KEY` configured, `mix run experiments/calibration.exs` records three equivalent primality questions over 60 deterministically selected labeled integers. There are 30 calibration and 30 held-out integers, balanced by exact primality labels. The retained run completed all 60 live requests (180 probabilities) in 3.643 seconds.

Held-out metrics (five reliability bins):

| Phrasing | Raw ECE | Isotonic ECE | Raw Brier | Isotonic Brier |
| --- | ---: | ---: | ---: | ---: |
| composite | 0.1280 | 0.0000 | 0.0448 | 0.0000 |
| prime | 0.1920 | 0.0000 | 0.0669 | 0.0000 |
| divisors | 0.2057 | 0.0000 | 0.0897 | 0.0000 |
| pooled | 0.1752 | 0.0111 | 0.0671 | 0.0056 |

The report retains log loss, fitted maps, labels, splits, raw probabilities and contract fingerprint. Fit uses only calibration examples. Pooled phrasing observations are correlated, not independent samples. The zero errors from individual fitted maps are results on a tiny, balanced arithmetic sample, not evidence of universal calibration. The explicit application decision is rank/monotone aggregation, without Bayesian graph multiplication.

To regenerate plots from retained data:

```bash
python3 -m venv /tmp/plexus-plots
/tmp/plexus-plots/bin/pip install matplotlib
/tmp/plexus-plots/bin/python experiments/plot_calibration.py
```

PNG/SVG reliability plots and CSV bin tables are under `artifacts/calibration/`.

## Controlled async/BSP replay

`mix run experiments/replay.exs` records eight live responses, freezes the contract/input/topology identity, writes a durable response file, then checks fresh async and BSP runs. To reuse the checked-in store without credentials or new sampling:

```bash
mix run experiments/replay.exs --replay-only
```

Both replay runs make zero transport requests and match the recorded final values. `artifacts/replay/report.json` includes event order, rounds, completion/observed-score curves, useful work, elapsed time and CPU runtime. The example uses independent nodes with fixed neighbor topology. It demonstrates controlled replay and termination; it does not establish an iterative convergence advantage or benchmark bounded async/priority scheduling. Further strategies should reuse this control before interpreting scheduling effects.
