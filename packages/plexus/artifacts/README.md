# Retained experiment evidence

See [methodology and results](../guides/experiments.md) and [environment](environment.json).

- `benchmarks/node-birth.jsonl`: final 27 configurations, up to 100,000 actors.
- `benchmarks/measurement.jsonl`: 384 fixture configurations, zero result errors.
- `benchmarks/*before*.jsonl`: partial diagnostic runs before index fixes; only completed cases appear.
- `calibration/report.json`: live responses, labels, disjoint splits, maps and held-out metrics.
- `calibration/reliability.{csv,svg,png}`: retained reliability-bin tables and plots.
- `replay/responses.plexus`: exact live semantic responses and compatibility manifest.
- `replay/report.json`: original recording plus fresh async/BSP zero-network comparisons.

The replay payload includes public toy inputs and SDK response metadata. It contains no API credentials. Its checksum is integrity detection, not a signature. No provider load test or million-actor result is claimed.
