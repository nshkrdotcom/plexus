# Plexus example applications

These are application-scale examples of Plexus as a semantic actor runtime. They are deliberately not one-example-per-primitive tutorials: each workload has a different data source, actor topology, semantic-call density, and coordination problem.

All six use recognized external datasets or APIs. Downloaded data is **not** committed to Plexus; fetchers write beneath `.plexus-data/` by default and `.gitignore` protects that directory. Pass `--data-dir` to keep datasets elsewhere.

## Prerequisites

- Elixir/OTP supported by Plexus and `mix deps.get` completed.
- `TYPESAFE_API_KEY` for the run scripts. `TYPESAFE_BASE_URL` is optional. `TYPESAFE_MODEL` is accepted as an example-specific override; the SDK's standard `TYPESAFE_DEFAULT_MODEL` environment variable is also honored.
- Internet access for fetch scripts.
- `curl` is preferred for large downloads; the helper falls back to Erlang HTTP for small files.
- `git-lfs` and `7z`/`7zz` are required only for the GAIA example.

The examples use real TypeSafe/Jev calls. There is no fixture-mode fallback in `examples/`; deterministic fixtures live only in `test/fixtures/examples/` for offline parser/release tests.

## Catalog

| Example | Standard data | Actor computation | TypeSafe/Jev density |
| --- | --- | --- | --- |
| [`00_issue_swarm`](00_issue_swarm/README.md) | SWE-bench Verified | issue actors route into repository/fix-shape populations; gold patches score a fixed prediction vocabulary post hoc | one request per issue |
| [`01_city_signal_tracker`](01_city_signal_tracker/README.md) | NYC 311 | report actors feed spatiotemporal incident clusters | one request per selected dense cluster |
| [`02_incident_commander`](02_incident_commander/README.md) | GAIA / MicroSS | evidence + competing root-cause hypothesis population with provenance | one request per candidate service |
| [`03_dependency_upgrade_search`](03_dependency_upgrade_search/README.md) | deps.dev v3 | resolved dependency graph diff + semantic risk measurements + pruned migration-order beam search | only changed dependency nodes |
| [`04_research_evidence_graph`](04_research_evidence_graph/README.md) | SciFact | claim/evidence graph with typed support/contradiction edges and gold labels | one request per claim/document pair |
| [`05_alert_swarm`](05_alert_swarm/README.md) | NOAA Storm Events | large dormant event population awakened by historical day events | only highest-impact state/day groups |

## Typical workflow

Each directory has a `fetch.exs` and `run.exs`:

```bash
mix run examples/00_issue_swarm/fetch.exs
TYPESAFE_API_KEY=... mix run examples/00_issue_swarm/run.exs --limit 50
```

Fetch once, then rerun from the local cache. The run scripts expose workload controls rather than substituting toy datasets for the real source.

## Data and cost discipline

The examples separate **dataset scale** from **semantic-call scale**. A 10,000-actor run need not make 10,000 model requests. The run summary prints the logical actor population and Plexus measurement budget usage so the relationship is visible.

Before a large run, inspect the example README and choose its call-driving control (`--limit`, `--max-clusters`, `--max-services`, `--claims`, or `--semantic-groups`). Token ledgers scale with the selected semantic workload rather than using a fixed demo ceiling; every run also accepts `--token-budget N` for an explicit hard limit. TypeSafeSDK 0.4 batches bound concurrency; distinct uncached inputs are still distinct transport requests.

## Runtime-only experiments stay in `experiments/`

Plexus already has controlled characterization for actor birth, measurement/coalescing, calibration, and scheduling replay. In particular, `experiments/node_birth.exs` has retained measurements through 100,000 actors. The application examples do not duplicate that experiment merely to advertise an actor count.

## UI boundary

These examples exercise Plexus directly. They intentionally contain no Phoenix, LiveView, web UI, or visualization layer. Interactive visualization belongs in the separate Plexus LiveView poncho repository.

See [DATASETS.md](DATASETS.md) for source/provenance details.

## Live TypeSafe transport evidence

Every dataset-backed run creates its TypeSafe client through `examples/support/runtime.exs`.
The shared runtime installs a privacy-safe TypeSafe telemetry collector before semantic work begins.

At process exit each run prints a `TYPESAFE LIVE TRANSPORT SUMMARY` containing the selected endpoint,
requested model and transport, evaluate counts, confirmed HTTP response counts, HTTP status distribution,
returned model distribution, input/output token totals, retry counts, and provider request IDs.

A response counts as a **confirmed HTTP response** only when TypeSafeSDK telemetry contains both an HTTP
status and a non-empty provider request ID. The collector never records API keys, semantic state, prompts,
request bodies, response bodies, authorization headers, or other customer content.

Application-level `semantic measurements` and confirmed TypeSafe HTTP responses are deliberately reported
separately: the former demonstrates Plexus scheduling/accounting; the latter proves that the remote TypeSafe
service actually answered the semantic request.
