# GAIA Living System Twin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the GAIA search-tree example with a chronological system twin whose persistent actors revise, challenge, invalidate, and prune work while real telemetry is still arriving.

**Architecture:** Add resident actor lifecycle support to Plexus, managed provenance notifications, a lazy chronological GAIA replay layer, persistent service actors, long-lived hypothesis actors, peer conflict, hierarchical branch credits, live pruning, and compact progress reporting. The run/fault dataset remains post-run truth only.

**Tech Stack:** Elixir/OTP, Plexus, TypeSafeSDK 0.4, Pristine transport, ETS, CloudWise GAIA / MicroSS.

**Spec:** `docs/superpowers/specs/2026-09-18-gaia-living-system-twin-design.md`

## Global Constraints

- The real GAIA / MicroSS trace and business-log files are the runnable example input.
- No precomputed semantic service summaries before actor execution.
- No expected-count hypothesis barrier.
- Distinct TypeSafe evaluations are treated as distinct provider requests unless cache/deduplication proves otherwise.
- Run/fault rows are hidden until post-run scoring.
- All actor-to-framework effects use managed Plexus paths where a managed path exists.
- The current package version remains unchanged; this is an Unreleased change.

---

### Task 1: Resident actor lifecycle

**Files:**
- Modify: `lib/plexus/run.ex`
- Modify: `test/plexus/runtime_acceptance_test.exs`

**Interfaces:**
- Consumes: `Plexus.Run.start_actor/2`, `Plexus.Schedule.Quiescence`
- Produces: `activity_mode: :work | :resident` start option

- [ ] Write a test that starts a resident actor and proves existence alone does not keep quiescence false.
- [ ] Write a test that sends a managed blocking message to that resident actor and proves the message is still counted as active work.
- [ ] Add validation for `activity_mode` and store it in graph metadata.
- [ ] Register only `:work` actors in the active-actor quiescence table.
- [ ] Run runtime acceptance tests on a BEAM-capable machine.

### Task 2: Live provenance notification

**Files:**
- Modify: `lib/plexus/provenance.ex`
- Modify: `test/plexus/provenance_test.exs`

**Interfaces:**
- Produces: `Plexus.Provenance.invalidate(run_id, upstream_id, notify: true)` delivering `{:plexus, :invalidated, upstream_id, epoch}`
- Produces: `repair/3` removes the repaired actor's queued repair entry

- [ ] Write a live actor test that depends on an upstream graph node.
- [ ] Verify invalidation marks the actor stale, increments epoch, and sends the managed notification.
- [ ] Verify successful repair removes the corresponding repair queue entry.
- [ ] Preserve existing `invalidate/2` behavior.

### Task 3: Chronological raw GAIA cursor

**Files:**
- Create: `examples/02_incident_commander/chronology.exs`
- Create: `test/fixtures/examples/gaia_living/trace.csv`
- Create: `test/fixtures/examples/gaia_living/business.csv`
- Create: `test/fixtures/examples/gaia_living/run.csv`
- Modify: `test/plexus/incident_commander_example_test.exs`

**Interfaces:**
- Produces: `Chronology.open!/3`, `Chronology.next/1`
- Emits normalized maps with `event_id`, `source`, `event_time_us`, `service`, `raw`

- [ ] Add fixture streams with interleaved timestamps and multiline-compatible CSV.
- [ ] Write a failing test asserting a cross-file chronological merge.
- [ ] Implement suspended-enumerable cursors so only one head row per file is retained.
- [ ] Verify day filtering and stable tie ordering.

### Task 4: Dynamic trace topology

**Files:**
- Create: `examples/02_incident_commander/topology.exs`
- Modify: `test/plexus/incident_commander_example_test.exs`

**Interfaces:**
- Produces: `Topology.new/0`, `Topology.observe_trace/4`, `Topology.neighbors/3`

- [ ] Write a test where child span arrives before parent span.
- [ ] Implement concurrent ETS span index, waiting-child bag, and seen-edge set.
- [ ] Add a single typed `:calls` edge when the relation becomes known.
- [ ] Return whether topology changed so service actors can invalidate dependent hypotheses.

### Task 5: Persistent service and hypothesis actors

**Files:**
- Replace: `examples/02_incident_commander/application.exs`
- Modify: `test/plexus/incident_commander_example_test.exs`

**Interfaces:**
- Service actor accepts `{:telemetry, event}`, `{:request_snapshot, requester}`.
- Hypothesis actor accepts evidence, service wake events, peer challenges, and provenance invalidations.

- [ ] Write a test proving the same service actor processes multiple telemetry events.
- [ ] Write a TypeSafe Test-client scenario proving the same hypothesis actor performs at least two semantic revisions.
- [ ] Implement rolling service state and semantic anomaly measurements on real event payloads.
- [ ] Create/reuse hypotheses by service + incident + failure mode.
- [ ] Re-arm one-shot wake subscriptions after every event.
- [ ] Persist service/incident/mode fields in graph attributes for indexed peer discovery.

### Task 6: Peer conflict, branch credits, and pruning

**Files:**
- Modify: `examples/02_incident_commander/application.exs`
- Modify: `test/plexus/incident_commander_example_test.exs`

**Interfaces:**
- Uses `Budget.Accounts` `:expand` grants for local investigation capital.
- Uses `:contradicts` and `:investigates` graph edges.
- Uses runtime `{:prune, actor_id}` for rejected hypothesis branches.

- [ ] Write a test proving a challenge reaches a peer and can trigger concession.
- [ ] Write a test proving child-account exhaustion prevents additional child hypothesis birth.
- [ ] Write a test proving a pruned branch disappears while an unrelated resident actor stays alive.
- [ ] Close hypothesis accounts from `terminate/2` so leaves return unused credit upward.

### Task 7: Replay actor, live progress, and final scoring

**Files:**
- Create: `examples/02_incident_commander/progress.exs`
- Replace: `examples/02_incident_commander/run.exs`
- Modify: `examples/support/typesafe_metrics.exs`
- Modify: `test/plexus/examples_typesafe_metrics_test.exs`

**Interfaces:**
- Replay actor owns cursor state and remains ordinary active work until EOF.
- Progress collector reports replay and TypeSafe phases separately.

- [ ] Write source-level guards against pre-run summary construction and class-completion barriers.
- [ ] Implement paced replay with `--speed max` or numeric multiplier.
- [ ] Print compact progress with API throughput clock beginning on the first evaluate start.
- [ ] Limit final request-id detail to a small sample while retaining aggregate counts.
- [ ] Add optional JSONL export of the privacy-safe Plexus event record before teardown.
- [ ] Score surviving hypotheses against run/fault rows only after quiescence.

### Task 8: Documentation and handoff

**Files:**
- Modify: `examples/02_incident_commander/README.md`
- Modify: `examples/README.md`
- Modify: `guides/examples.md`
- Modify: `guides/actor-runtime.md`
- Modify: `guides/graph-and-subtrees.md`
- Modify: `CHANGELOG.md`
- Modify: `HANDOFF.md`
- Create: `GAIA_LIVING_TWIN_HANDOFF.md`

- [ ] Document the architecture and CLI profiles without claiming Python impossibility or HTTP batch collapsing.
- [ ] Document resident actor semantics and live provenance notification.
- [ ] Document exact runtime acceptance commands and required live evidence.
- [ ] Record that the artifact-authoring environment lacks Elixir/OTP and therefore cannot certify compile/test results.

### Task 9: BEAM verification handoff

**Runtime commands:**

```bash
mix format
mix format --check-formatted
mix compile --warnings-as-errors
MIX_ENV=test mix compile --warnings-as-errors
mix test test/plexus/runtime_acceptance_test.exs
mix test test/plexus/provenance_test.exs
mix test test/plexus/incident_commander_example_test.exs
mix test
mix credo --strict
mix dialyzer
mix docs
mix hex.build
```

Then fetch/reuse GAIA and run a live profile with explicit measurement and token expectations. Confirm every acceptance requirement in the design spec from run telemetry and `Plexus.Record`.
