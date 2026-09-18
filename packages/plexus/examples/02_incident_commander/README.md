# 02 — GAIA living incident commander

This is Plexus's actor-model reference application. It replays the real CloudWise GAIA / MicroSS trace and business-log corpus as chronological telemetry while a persistent system twin investigates the incident in real time.

The run/fault-injection records are kept out of the live computation. They are read only after Plexus reaches quiescence and are used only to score the surviving hypotheses.

## Why this is not a batch search tree

The application does not build service summaries before actor execution and does not walk a known hypothesis tree. The computation is alive while evidence arrives:

1. A replay actor lazily merges raw trace and business CSV streams by event time.
2. Service actors are created explicitly on first observation and remain resident for the run.
3. Service actors keep rolling local telemetry state and update `:calls` topology as spans arrive, including late parent spans.
4. Real anomalous events trigger focused Jev signal measurements.
5. Relevant signals create or update resident hypothesis actors keyed by incident, service, and failure mode.
6. The same hypothesis can wake, request fresh service state, revise its belief repeatedly, become stale after provenance invalidation, and re-evaluate at a newer epoch.
7. Hypotheses discover competitors through Plexus population indexes, create `:contradicts` edges, and send direct challenge messages to peer mailboxes.
8. A conceded or refuted hypothesis uses runtime pruning, which cancels its managed work and terminates its child subtree while unrelated actors continue. Child investigations use parent-lineage IDs so a subtree never owns a hypothesis shared by an unrelated branch.
9. Hierarchical `Plexus.Budget.Accounts` grants use the `:expand` meter as local branch capital. Child investigation must receive credit from its parent branch.
10. The replay actor completes only at EOF. Resident service/hypothesis existence does not prevent settlement; their managed messages, timers, measurements, and other work still do.
11. The run waits only for Plexus quiescence. There is no expected hypothesis count or depth-round barrier.

A simple `while queue: await gather(...)` implementation does not preserve these semantics: entities survive across many evidence epochs, receive independent messages, revise in response to later topology/evidence changes, challenge peers, and can have live subtrees cancelled while the rest of the investigation continues.

## Data flow

```text
real GAIA trace/business CSVs
          |
          v
 chronological Replay actor
          |
          +----------------------------+
          |                            |
          v                            v
 resident Service actors       live :calls topology
          |                            |
          | signal measurements       | invalidation
          v                            v
 resident Hypothesis actors <----------+
          |
          +--> service wakeups / snapshots
          +--> repeated Jev revisions
          +--> peer challenge messages
          +--> child investigation grants
          `--> runtime subtree pruning

EOF + zero managed activity
          |
          v
       quiescence
          |
          v
post-run fault-label scoring
```

## Fetch the real corpus

```bash
mix run examples/02_incident_commander/fetch.exs
```

`fetch.exs` downloads the official GAIA `release-v1.0` MicroSS corpus beneath `.plexus-data/gaia/GAIA-DataSet`. It requires Git LFS plus `7z` or `7zz`. You can instead pass an existing MicroSS checkout with `--source-dir`.

## Run

A normal live run:

```bash
TYPESAFE_API_KEY=... \
mix run examples/02_incident_commander/run.exs -- \
  --day 2021-07-01 \
  --speed max \
  --max-hypotheses 20000 \
  --max-measurements 50000
```

`--max-measurements 50000` permits up to 50,000 managed semantic evaluations. With TypeSafeSDK 0.4, distinct uncached evaluations remain distinct provider requests; Plexus coalescing controls concurrency and deduplicates identical states rather than turning many distinct evaluations into one HTTP request.

Important controls:

- `--max-events N` — replay at most N raw events; `0` means all selected-day trace/business events.
- `--speed max|N` — replay as fast as the actor system can accept work, or at N-times historical speed.
- `--max-hypotheses N` — maximum simultaneously admitted hypothesis actors.
- `--max-depth N` — maximum hypothesis child depth (service/replay roots are accounted for separately).
- `--branch-width N` — maximum topology neighbors considered by one hypothesis revision.
- `--branch-credits N` — local investigation capital granted to a root hypothesis and recursively divided across child branches.
- `--peer-challenges N` — maximum peer hypotheses challenged after one revision.
- `--signal-every N` — perform one service signal measurement per N raw anomaly signals.
- `--hypothesis-update-every N` — request one hypothesis revision per N received evidence packets.
- `--incident-window-seconds N` — temporal bucket used to associate competing hypotheses with the same incident.
- `--max-measurements N` — run-level semantic evaluation ceiling.
- `--token-budget N` — token-accounting ledger reference. Observed tokens are consumed after responses, so this is not a pre-request stop.
- `--batch-size N`, `--batch-delay-ms N`, `--max-in-flight-batches N`, `--max-concurrency N` — TypeSafe coordination/concurrency controls.
- `--actor-partitions N` — actor supervisor partitions. BEAM schedulers still decide where runnable processes execute.
- `--timeout-ms N` — total quiescence wait timeout.
- `--progress-every N` / `--progress-heartbeat-ms N` — compact live progress cadence.
- `--record-path PATH` — write the privacy-safe Plexus event record as JSONL before run teardown.
- `--trigger-probability P` — minimum Jev anomaly relevance needed to create/update a hypothesis.

## What live progress means

The terminal prints compact status such as:

```text
[GAIA] events=183240 eof=false services=10 hypotheses=614 revisions=2201 challenges=311 pruned=74 | Jev done=9412/50000 in_flight=128 2xx=9409 err=3 tokens=... retries=... rate=.../s
```

The semantic throughput clock begins at the first TypeSafe evaluation, not at process start. The final TypeSafe report keeps aggregate counts and prints only a small provider-request-id sample.

## Required live acceptance evidence

A release-quality run should establish all of the following from `Plexus.Record`, graph state, and TypeSafe telemetry:

- the first TypeSafe evaluation happens before telemetry EOF;
- at least one service actor handles many historical events without being replaced;
- at least one hypothesis actor performs multiple semantic revisions;
- at least one direct peer challenge is processed;
- later telemetry/topology invalidates an already-live hypothesis;
- the invalidated hypothesis re-evaluates at a newer epoch;
- topology changes while hypotheses already exist;
- at least one hypothesis subtree is pruned while unrelated actors remain alive;
- local branch credit refuses at least one attempted child or returns unused credit during subtree teardown;
- the run reaches quiescence without a known-population completion barrier;
- run/fault labels are consulted only after the live computation settles;
- TypeSafe telemetry confirms real HTTP responses, provider request ids, models, token totals, retries, and errors.

The deterministic test fixtures under `test/fixtures/examples/gaia_living/` test parsing/runtime behavior only. The runnable application has no synthetic anomaly or fixture fallback.

## Source and terms

Source: `CloudWise-OpenSource/GAIA-DataSet`. The upstream repository has conflicting license signals across its metadata/documentation, so Plexus downloads from upstream instead of redistributing the corpus. Review the upstream `LICENSE` and dataset documentation before redistribution.
