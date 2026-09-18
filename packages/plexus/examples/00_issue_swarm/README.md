# 00 — SWE-bench issue swarm

Turns **SWE-bench Verified** into an asynchronous software-maintenance population. Each issue actor asks TypeSafe/Jev to predict the *shape* of the eventual fix (`source_code`, `tests`, `documentation`, `configuration_build`, or `mixed`) plus a difficulty score, then routes itself to a long-lived repository/fix-shape actor.

The benchmark patch is never included in semantic state and never determines the model's available answer vocabulary. After inference, changed paths in the known patch are reduced to the same fixed fix-shape labels for an interpretable post-hoc accuracy check.

This demonstrates semantic measurements driving actor-to-actor routing, typed topology, long-lived aggregation actors, bounded measurement budgets, completion/quiescence, and an outcome checked against benchmark ground truth.

```bash
mix run examples/00_issue_swarm/fetch.exs
TYPESAFE_API_KEY=... mix run examples/00_issue_swarm/run.exs -- --limit 50
```

Use `--limit 500` for all SWE-bench Verified instances. Downloaded data lives under `.plexus-data/swe-bench-verified/` by default and is ignored by Git.

Source: `SWE-bench/SWE-bench_Verified` on Hugging Face. The example fetcher uses the Hugging Face datasets-server rows service and stores JSONL locally. The fetcher does not vendor the dataset into Plexus; consult SWE-bench and the source repositories for current terms.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
