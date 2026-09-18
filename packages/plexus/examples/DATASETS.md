# Dataset sources and local-data policy

Plexus does not vendor the external corpora used by the examples. Fetch scripts acquire them from their standard upstream source into `.plexus-data/`, which is ignored by Git. Users remain responsible for complying with each upstream dataset's current terms.

## SWE-bench Verified

- Upstream: `SWE-bench/SWE-bench_Verified` on Hugging Face.
- Shape used: repository, issue/problem statement, base commit, gold patch, hints, version and benchmark metadata.
- Plexus use: the patch is used only after inference to derive a fixed post-hoc fix-shape label (`source_code`, `tests`, `documentation`, `configuration_build`, or `mixed`). Patch content is never included in semantic state or used to construct the answer vocabulary.
- Fetch path: Hugging Face datasets-server rows API.

## NYC 311 Service Requests from 2020 to Present

- Upstream dataset identifier: `erm2-nwe9` on NYC Open Data.
- Shape used: request id/time, agency, problem, problem detail, location type, borough, latitude/longitude, status.
- Plexus use: large report population + spatiotemporal cluster actors; semantic work occurs at selected cluster boundaries.
- Optional `SOCRATA_APP_TOKEN` can be supplied for API identification/rate handling.

## GAIA / MicroSS

- Upstream: `CloudWise-OpenSource/GAIA-DataSet`.
- Shape used: MicroSS trace, business-log and run/fault-injection records.
- Plexus use: evidence summaries become provenance-linked root-cause hypotheses; trace parent spans are reduced into typed service-call topology; upstream anomaly-injection records are retained separately as evaluation truth.
- Acquisition: the official `release-v1.0` Git ref via Git + Git LFS, followed by extraction of the standard MicroSS split archives with 7-Zip. The corpus is intentionally not copied into the package.
- Terms: the upstream repository has conflicting license signals across its README/GitHub metadata; the fetcher downloads from upstream rather than redistributing GAIA data. Review the upstream `LICENSE` and dataset documentation before redistribution.

## deps.dev

- Upstream: Google Open Source Insights, stable v3 JSON API.
- Shape used: source and target resolved dependency graphs plus selected target-version metadata/advisories.
- Plexus use: every target graph node is an actor; topology follows the resolved graph; exact `(system, name, version)` target keys absent from the source graph consume semantic measurement budget. Package-level collapsing happens only when constructing human-readable migration-order search steps.

## SciFact

- Upstream: AllenAI SciFact release, `https://scifact.s3-us-west-2.amazonaws.com/release/latest/data.tar.gz`.
- Shape used: `claims_dev.jsonl` and `corpus.jsonl`.
- Plexus use: claim/document pair actors create predicted support/contradiction/insufficient-evidence edges and are evaluated against public dev labels.
- Upstream license documentation states claims/evidence annotations are CC BY 4.0 and corpus abstracts are from S2ORC under ODC-By 1.0.

## NOAA Storm Events

- Upstream: NOAA/NCEI Storm Events bulk CSV directory.
- Shape used: yearly `StormEvents_details` files, including event identity/type/time/state, casualties/damage and event narrative.
- Plexus use: every selected historical event becomes a dormant actor subscribed to its day. State/day actors aggregate awakened cohorts; only the highest-impact groups invoke TypeSafe/Jev.

## What is committed

Only code, documentation and tiny test fixtures are committed. These paths are ignored:

```text
.plexus-data/
examples/**/data/
examples/**/.data/
examples/**/cache/
examples/**/downloads/
```

`test/fixtures/examples/` is intentionally tracked; those records test local parsers and never stand in for the public example datasets.
