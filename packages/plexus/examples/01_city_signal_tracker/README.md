# 01 — NYC 311 city signal tracker

Streams real **NYC 311 Service Requests** into a spatiotemporal actor population. Individual request actors route themselves to geographic/time-cell actors. Only sufficiently dense cells make TypeSafe/Jev calls; the model judges whether a cluster looks like one coherent incident, its operational theme, and signal strength.

The dataset can contain tens of millions of rows; the example intentionally scales the *run* with `--max-clusters`, `--min-cluster`, and the fetch window instead of replacing the source with toy data.

```bash
mix run examples/01_city_signal_tracker/fetch.exs --days 7 --limit 50000
TYPESAFE_API_KEY=... mix run examples/01_city_signal_tracker/run.exs --max-clusters 50
```

Set `SOCRATA_APP_TOKEN` if you have one. Downloaded rows are cached under `.plexus-data/nyc-311/` and ignored by Git.

Source: NYC Open Data dataset `erm2-nwe9`, **311 Service Requests from 2020 to Present**.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
