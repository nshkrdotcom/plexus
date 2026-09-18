# 02 — GAIA incident commander

Runs an **actor-native root-cause investigation** over the CloudWise GAIA / MicroSS AIOps dataset. GAIA contains metrics, business logs, traces, and recorded fault injections. Injection records remain separate as evaluation ground truth.

This example is intentionally different from a fan-out/classify/aggregate pipeline. The outer script does not know the final hypothesis population and does not advance the investigation through depth barriers.

## Actor-native control flow

1. GAIA traces/logs are reduced into a bounded evidence universe and a typed service call graph.
2. Evidence actors publish their local summaries and remain addressable by hypothesis actors.
3. Only a small seed hypothesis population starts initially.
4. Each hypothesis requests evidence from the relevant evidence actor, makes one managed TypeSafe/Jev measurement, and receives a semantic `next_action`.
5. That semantic result may cause the hypothesis actor itself to spawn narrower child hypotheses along caller/callee edges.
6. Every hypothesis birth must first reserve one credit from a shared `Plexus.Budget.Accounts` population grant. Exhausting that grant stops further expansion locally without an orchestrator knowing how many actors will eventually exist.
7. Parent/child topology records the investigation tree; `:consults`, `:investigates`, and trace-derived `:calls` edges preserve why each branch exists.
8. The run terminates on Plexus quiescence, not `await_class_complete!(known_count)` or a layer-synchronous search loop.

A child can begin requesting evidence while unrelated siblings are still evaluating. The final population therefore emerges from runtime semantic decisions plus a finite shared credit pool.

```text
seed hypothesis
   |
   +-- request evidence actor
   |
   +-- TypeSafe root-cause + next-action measurement
   |
   +-- stop
   |
   `-- reserve shared hypothesis credit
         |
         `-- spawn child hypothesis
               |
               `-- request different evidence actor ...
```

## Run it

```bash
mix run examples/02_incident_commander/fetch.exs
TYPESAFE_API_KEY=... mix run examples/02_incident_commander/run.exs --day 2021-07-01
```

GAIA is large and uses Git LFS. `fetch.exs` downloads the real `MicroSS/**` corpus into `.plexus-data/gaia/GAIA-DataSet`; it requires `git-lfs` plus `7z`/`7zz` to extract the standard MicroSS split archives. You may instead pass an existing checkout with `--source-dir /path/to/MicroSS`.

Useful controls:

- `--max-rows N` — maximum rows sampled per dataset family for the selected day.
- `--max-services N` — maximum evidence-service universe retained from the dataset. This is **not** the hypothesis count.
- `--seed-services N` — number of initial hypothesis actors.
- `--max-hypotheses N` — shared actor-growth credit cap and maximum semantic root-cause calls.
- `--max-depth N` — maximum parent/child investigation depth.
- `--branch-width N` — maximum child candidates one hypothesis may attempt to create.
- `--token-budget N` — explicit hard token ledger cap instead of the workload-scaled default.
- `--timeout-ms N` — quiescence timeout.

The report prints seed count, total/dynamic hypothesis population, maximum investigation depth, shared credits consumed, semantic measurements, ranked investigation paths, and whether the top candidate matches an injected service.

## What this demonstrates

The value of this example is the control flow, not merely that BEAM can execute model calls. Semantic output changes future population topology before global synchronization; the final actor count is unknown at startup; shared credits bound endogenous growth; evidence is requested actor-to-actor; and termination emerges from quiescence.

The deterministic test suite exercises the same actor modules with `TypeSafeSDK.Test`, including a case where one semantic result spawns the next hypothesis and a case where exhausted shared credits prevent further growth.

Source: `CloudWise-OpenSource/GAIA-DataSet`. The upstream README and repository metadata currently expose conflicting license signals, so Plexus downloads from upstream instead of redistributing GAIA data; review the upstream `LICENSE` and dataset documentation before redistribution.
