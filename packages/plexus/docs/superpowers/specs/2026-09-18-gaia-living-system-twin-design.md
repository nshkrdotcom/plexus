# GAIA Living System Twin Design

## Purpose

Replace the current GAIA incident-commander search-tree demo with a chronological, continuously evolving system twin that makes actor identity, mailboxes, local state, wakeups, invalidation, pruning, and quiescence materially relevant to the computation.

The real CloudWise GAIA / MicroSS trace and business-log files are the live input. Run/fault-injection rows remain hidden evaluation truth and are consulted only after the investigation settles.

## Architectural criterion

The example fails if its essential behavior can be preserved by replacing the actor population with a single `while queue` / `asyncio.gather` loop.

The same service and hypothesis actors must survive across many evidence epochs. Work is triggered by independently arriving messages, not by a coordinator walking a precomputed DAG.

## Data plane

Raw trace and business-log CSV files are read lazily. A k-way chronological cursor keeps only the next row from each input stream in memory. No finished service summaries are created before Plexus starts.

A single replay actor is responsible only for historical event-time playback. It does not hold investigation state or decide which hypotheses exist. It remains active until EOF so quiescence cannot be declared while telemetry is still arriving.

Each normalized event contains:

- stable event id
- source kind (`trace` or `business`)
- event timestamp
- service name
- original raw row

Run/fault records never enter semantic state.

## Persistent infrastructure actors

A service actor is created explicitly on first observation and stays resident for the run. Resident actors do not keep the run non-quiescent merely by existing; their managed messages, timers, semantic measurements, and expansion work remain visible to quiescence accounting.

Each service actor maintains a rolling local window with:

- total events
- trace failures
- log error signals
- recent raw evidence excerpts
- last event time
- anomaly epoch
- incident key
- local caller/callee topology

Trace span parentage updates typed `:calls` edges as spans arrive. Late parent spans are resolved through run-local ETS indexes owned by the application.

## Semantic signal measurements

A service actor performs small Jev measurements on real anomalous events at an operator-selected cadence. The signal contract returns:

- anomaly relevance (`noul`)
- failure mode (`choice`)
- diagnostic strength (`score`)

A sufficiently relevant signal either creates a new long-lived hypothesis actor or sends the evidence to an existing actor for the same service / incident / failure mode.

## Hypothesis lifecycle

Hypotheses are resident actors with explicit phases:

- `observing`
- `investigating`
- `challenging`
- `stale`
- `reconsidering`
- `refuted`
- `confirmed`

The same hypothesis actor can process many pieces of evidence and perform many semantic revisions. It subscribes to service-change events, re-arms the one-shot Plexus subscription after every wake, and requests snapshots directly from service actors through managed messages.

A hypothesis update contract returns:

- root-cause plausibility (`noul`)
- next action (`choice`)
- evidence strength (`score`)

Next actions may keep observing, investigate callers, investigate callees, investigate both directions, challenge peers, confirm, or refute.

## Peer conflict

Hypotheses for the same incident can establish `:contradicts` edges and send direct `{:challenge, ...}` messages. The receiving actor performs a focused Jev challenge measurement against its current evidence. A failed challenge can cause voluntary concession and runtime pruning of that hypothesis subtree.

Challenge ids are stable hashes of the participants and evidence epoch, preventing uncontrolled ping-pong.

## Provenance and invalidation

Hypotheses depend on the service actors whose evidence they use. Significant service or topology changes call `Plexus.Provenance.invalidate/3` with notification enabled.

Invalidation:

1. marks dependent nodes stale and advances their epoch,
2. queues repair work,
3. sends a managed invalidation message to live dependent actors.

A hypothesis re-evaluates against fresh evidence and clears stale state only if its expected epoch still matches. Repair completion removes the stale repair queue entry.

## Investigation credits

Hierarchical `Plexus.Budget.Accounts` credits use the `:expand` meter as logical branch capital. This remains separate from the run-level physical measurement meter.

Each root hypothesis receives an account grant. Child hypotheses receive sub-grants from the parent account. Creating a child therefore consumes local branch capital. Weak branches cannot create unlimited descendants. When a pruned subtree terminates leaves-first, hypothesis termination closes leaf accounts and returns unused local credit upward.

## Pruning and cancellation

Hypothesis pruning must go through `Plexus.Run.prune/2` / `{:prune, actor_id}` so queued scheduling work, measurements, expansion work, actor processes, and graph topology are removed through the kernel path.

The live acceptance run must demonstrate pruning while unrelated investigation continues.

## Quiescence

The replay actor is ordinary active work and completes only at EOF. Service and hypothesis actors are resident. The run settles only after:

- replay reached EOF,
- managed messages drained,
- managed timers drained,
- semantic measurements completed or were cancelled,
- expansion work completed or was cancelled,
- no ordinary active work actor remains.

There is no expected hypothesis count and no class-completion barrier.

## Progress and observability

The example prints compact progress only:

- replay events read and dispatched
- resident service / hypothesis counts
- hypothesis revisions / challenges / prunes
- Jev starts / completions / errors
- input/output token totals
- semantic throughput measured from the first API start, not process start

The final transport report prints aggregate counts plus a small sample of request ids rather than one line per request. An optional `--record-path` writes the privacy-safe Plexus event history as JSONL before run teardown so temporal acceptance claims remain inspectable.

## Live profiles

The CLI exposes workload controls rather than hard-wiring a small demonstration:

- `--day`
- `--max-events` (`0` means all selected-day events)
- `--speed` (`max` or numeric historical replay multiplier)
- `--max-hypotheses`
- `--max-depth`
- `--branch-width`
- `--signal-every`
- `--hypothesis-update-every`
- `--max-measurements`
- `--batch-size`
- `--batch-delay-ms`
- `--max-concurrency`
- `--actor-partitions`
- `--record-path`
- `--timeout-ms`

A large run may legitimately produce tens of thousands of Jev evaluations, but no code loops merely to manufacture request count.

## Acceptance requirements

A qualifying real-data run must show all of the following:

1. First Jev call occurs before telemetry EOF.
2. At least one service actor handles many historical events.
3. At least one hypothesis actor performs multiple semantic revisions.
4. At least one peer challenge is delivered actor-to-actor.
5. At least one premise invalidation reaches a live hypothesis.
6. The invalidated hypothesis re-evaluates at a newer epoch.
7. Topology changes while hypotheses already exist.
8. At least one hypothesis subtree is pruned while unrelated actors continue.
9. Hierarchical branch credits prevent at least one attempted child birth or visibly return unused credit.
10. No expected-count completion barrier appears in the run path.
11. EOF plus zero managed activity reaches quiescence.
12. Fault-injection labels are consulted only for post-run scoring.
13. TypeSafe telemetry shows real HTTP responses, provider request ids, returned model, tokens, retries, and errors.
14. The final report distinguishes live surviving graph state from pruned-theory history retained in `Plexus.Record`.

## Non-goals

- No synthetic anomaly generator in the runnable example.
- No claim that Python cannot implement actor semantics.
- No claim that one TypeSafe batch is one provider request; distinct uncached evaluations remain distinct transport requests in TypeSafeSDK 0.4.
- No distributed multi-node Plexus work in this change.
