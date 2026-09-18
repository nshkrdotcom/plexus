# 02 — GAIA incident commander

Runs a root-cause hypothesis population over the **CloudWise GAIA / MicroSS** AIOps dataset. GAIA contains metrics, business logs, traces, and recorded fault injections. The example keeps the injection records separate as evaluation ground truth and asks TypeSafe/Jev to judge candidate service hypotheses from trace and log evidence.

Hypothesis actors depend on evidence actors through Plexus provenance edges. Trace span parentage is also reduced into a real service call topology and preserved as typed `:calls` edges between service evidence actors. The example therefore exercises semantic beliefs, graph structure derived from traces, bounded measurement, provenance, and inspectable root-cause ranking on a dataset with known injected faults.

```bash
mix run examples/02_incident_commander/fetch.exs
TYPESAFE_API_KEY=... mix run examples/02_incident_commander/run.exs --day 2021-07-01
```

GAIA is large and uses Git LFS. `fetch.exs` intentionally downloads the real `MicroSS/**` corpus into `.plexus-data/gaia/GAIA-DataSet`; it requires `git-lfs` plus `7z`/`7zz` to extract the standard MicroSS split archives. You may instead pass an existing checkout with `--source-dir /path/to/MicroSS`.

Useful controls: `--max-rows`, `--max-services`, `--day`.

Source: `CloudWise-OpenSource/GAIA-DataSet`. The upstream README and repository metadata currently expose conflicting license signals, so this example downloads from upstream instead of redistributing GAIA data; review the upstream `LICENSE` and dataset documentation before redistribution.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
