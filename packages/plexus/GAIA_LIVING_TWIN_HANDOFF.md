# GAIA Living System Twin — implementation handoff

Date: 2026-09-18

## Objective

Finish and certify `examples/02_incident_commander` as Plexus's actor-model reference application. The application must investigate the real CloudWise GAIA / MicroSS corpus while telemetry is still arriving. It must not regress into `load all data -> summarize -> fan out jobs -> gather results`.

The architectural test is simple: if the essential state model and control flow can be replaced by a normal centralized queue plus `asyncio.gather`, the reference application has failed.

## What is implemented in this overlay

### Core runtime extensions

`lib/plexus/run.ex`

- `Plexus.Run.start_actor/2` accepts `activity_mode: :work | :resident`.
- `:work` remains the default and participates in the active-actor quiescence counter exactly as before.
- `:resident` remains alive/addressable but idle existence does not count as unfinished work.
- Managed messages, command envelopes, timers, TypeSafe work, and expansion still contribute to quiescence through their existing counters.
- actor start/stop telemetry includes class/activity mode metadata so the living-twin progress collector can track live populations.

`lib/plexus/provenance.ex`

- `invalidate/3` accepts `notify: true` while preserving `invalidate/2` compatibility.
- Dependent live actors receive `{:plexus, :invalidated, upstream_id, epoch}` through `Run.cast/3`.
- Successful `repair/3` removes the actor's queued repair item.

### GAIA raw chronology

`examples/02_incident_commander/chronology.exs`

- Opens trace/business CSVs lazily using the existing multiline-safe CSV parser.
- Keeps one suspended source head per file.
- Merges source heads by normalized event time with deterministic ties.
- Does not construct finished service summaries.
- If an upstream file itself contains late records, they remain late evidence rather than forcing the entire corpus into an in-memory sort.

### Live topology

`examples/02_incident_commander/topology.exs`

- ETS span index keyed by `{trace_id, span_id}`.
- Waiting-child bag for child-before-parent arrival.
- `:calls` edges appear when both sides are known.
- Local atomic counter limits simultaneously admitted hypothesis actors.

### Persistent actors

`examples/02_incident_commander/application.exs`

`Service` actors:

- are resident;
- ingest raw trace/business events repeatedly;
- retain a recent local window plus cumulative signal counts;
- expose snapshots to hypotheses through managed actor messages;
- publish one-shot service-change events and invalidate provenance when live topology changes;
- send real anomaly events through the `:gaia_signal` TypeSafe contract.

`Hypothesis` actors:

- are resident;
- persist across multiple evidence epochs;
- maintain explicit phase/revision/probability/evidence state;
- re-arm service wake subscriptions;
- request fresh snapshots actor-to-actor;
- execute repeated `:gaia_hypothesis_update` measurements;
- react to live provenance invalidation and clear stale state only at the matching epoch;
- discover peers through the population index and challenge them directly;
- execute focused `:gaia_challenge` measurements;
- concede/refute through runtime subtree pruning;
- give child hypotheses parent-lineage identities so lifecycle ownership matches the subtree that funded them;
- use hierarchical `Budget.Accounts` grants as local child-investigation capital.

`Replay` actor:

- owns chronological cursor state;
- remains ordinary active work until EOF/max-events;
- supports `--speed max` or numeric historical replay acceleration;
- dispatches each raw event through managed `Run.cast/3` after explicitly ensuring its service actor exists.

### Live observability

`examples/02_incident_commander/progress.exs`

- tracks replay events/EOF, resident actor counts, revisions, challenges, prunes, TypeSafe starts/stops/errors, tokens, retries, and in-flight work;
- starts the semantic throughput clock on the first TypeSafe evaluate start rather than process start;
- prints compact periodic lines rather than provider-request streams.

`examples/support/typesafe_metrics.exs`

- retains aggregate transport accounting;
- prints only a small provider request-id sample (`TYPESAFE_METRICS_REQUEST_SAMPLE`, default 10).

## Tests authored

The overlay adds/changes deterministic tests for:

- resident actor quiescence semantics;
- managed work on a resident actor still preventing quiescence;
- invalid activity-mode rejection;
- live provenance notification and epoch-safe repair cleanup;
- chronological cross-file raw event merge;
- late-parent trace topology;
- the same hypothesis PID performing repeated semantic revisions;
- peer challenge pruning one branch while unrelated actors survive;
- local branch credit preventing child birth;
- source guards against fixed completion barriers and precomputed service evidence;
- compact TypeSafe request-id summary output.

These tests were authored in a container without BEAM and have not executed here.

## First task for the BEAM agent

Run the complete gate from `HANDOFF.md`. Expect formatter adjustments to be possible because this environment cannot run `mix format`. Treat warnings as failures.

If compilation fails, repair production code rather than deleting architectural tests. In particular, preserve:

- resident actors;
- raw chronological replay;
- repeated same-PID hypothesis revision;
- direct challenge messages;
- provenance notification;
- runtime pruning;
- hierarchical child-investigation capital;
- quiescence-only terminal wait.

## Live provider profiles

### Functional real-data profile

This profile permits up to 2,000 distinct semantic evaluations:

```bash
TYPESAFE_API_KEY=... \
mix run examples/02_incident_commander/run.exs -- \
  --day 2021-07-01 \
  --speed max \
  --max-events 0 \
  --max-hypotheses 2000 \
  --max-measurements 2000 \
  --max-depth 8 \
  --branch-width 2 \
  --branch-credits 8 \
  --peer-challenges 2 \
  --signal-every 4 \
  --hypothesis-update-every 2 \
  --batch-size 64 \
  --max-in-flight-batches 4 \
  --max-concurrency 32 \
  --actor-partitions 24 \
  --progress-every 50 \
  --progress-heartbeat-ms 5000 \
  --record-path /tmp/gaia-living-functional.jsonl \
  --timeout-ms 1800000
```

### High-throughput reference profile

This profile permits up to 50,000 distinct semantic evaluations. In TypeSafeSDK 0.4, that means up to roughly 50,000 provider requests before retry effects, minus cache/deduplication reuse. Do not run it without intending that scale.

```bash
TYPESAFE_API_KEY=... \
TYPESAFE_METRICS_REQUEST_SAMPLE=10 \
mix run examples/02_incident_commander/run.exs -- \
  --day 2021-07-01 \
  --speed max \
  --max-events 0 \
  --max-hypotheses 20000 \
  --max-measurements 50000 \
  --max-depth 12 \
  --branch-width 3 \
  --branch-credits 12 \
  --peer-challenges 2 \
  --signal-every 1 \
  --hypothesis-update-every 1 \
  --incident-window-seconds 30 \
  --batch-size 64 \
  --batch-delay-ms 10 \
  --max-in-flight-batches 8 \
  --max-concurrency 64 \
  --actor-partitions 24 \
  --progress-every 100 \
  --progress-heartbeat-ms 5000 \
  --record-path /tmp/gaia-living-50000.jsonl \
  --timeout-ms 1800000
```

Do not assert beforehand that this profile will consume all 50,000 evaluations. The number must emerge from real GAIA events and actor decisions. If the corpus/contract trajectory produces far fewer useful measurements, report that rather than manufacturing calls.

## Live acceptance evidence to capture

Retain terminal output plus a `Plexus.Record` export or equivalent event analysis proving:

1. `first TypeSafe start` occurs before `gaia_replay_finished` with `reason: :eof`.
2. At least one service actor ID/PID handles many telemetry events.
3. At least one hypothesis actor ID/PID records two or more `gaia_hypothesis_revision` events.
4. At least one `gaia_challenge_result` exists between distinct live hypothesis actors.
5. At least one `gaia_hypothesis_invalidated` event is followed by a later revision at the newer epoch.
6. At least one `gaia_topology_changed` event occurs after hypotheses have already been born.
7. At least one `gaia_hypothesis_tombstone` / `subtree_pruned` event occurs while unrelated hypotheses remain alive.
8. Branch-credit exhaustion or returned unused credit is visible in deterministic tests and, preferably, in the live record.
9. Replay reaches EOF and the managed quiescence snapshot reaches zero without an expected-count barrier.
10. TypeSafe telemetry reports real 2xx responses, provider request IDs, returned model, input/output tokens, retries, and exceptions.
11. Fault-injection labels appear only in the post-run scoring/report path.

## Scientific comparison

After the actor application is certified, build a conventional centralized baseline against the same raw event stream and the same recorded semantic answers. Do not cripple it.

The actor version earns its reference status only if preserving the experiment forces the baseline to reconstruct per-entity persistent state machines, mailboxes/subscriptions, cancellation ownership, stale-epoch handling, branch ownership/credit, and failure isolation. If a simple queue/gather program remains equivalent, record that result and redesign again.

## Known handoff risks

Because this source was not compiled in the authoring container, scrutinize these areas first:

- Elixir formatting and unused aliases/variables under `--warnings-as-errors`;
- TypeSafe prepared-contract option syntax against installed `typesafe_sdk 0.4.x`;
- race behavior when two services attempt to prepare the same hypothesis concurrently;
- the prepare-then-async-spawn window: if a parent branch is pruned after a child account/counter is reserved but before the queued spawn command executes, verify that cleanup cannot retain orphaned local credit or a hypothesis-count slot;
- `Budget.Accounts.close/2` during nested subtree teardown;
- actual GAIA timestamp/source ordering across extracted split CSVs;
- provider concurrency/rate behavior at the high-throughput profile.

Do not mask those risks with fixtures. Fix them in the real implementation, keep deterministic regression coverage, then repeat the live run.
