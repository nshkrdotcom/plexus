# 05 — NOAA Storm Events alert swarm

Replays the standard **NOAA/NCEI Storm Events** dataset as a large event-driven Plexus population. Every historical storm row becomes an actor that subscribes to its event day and otherwise remains dormant. Publishing a day wakes exactly that cohort; event actors route themselves into state/day actors and complete.

Only the highest-impact state/day groups spend TypeSafe/Jev budget to interpret compound operational impact from structured losses plus NOAA narratives. This is intentionally different from `experiments/node_birth.exs`: raw actor birth is already characterized up to 100,000 actors; this example exercises wake subscriptions, selective fan-out, typed edges, aggregation, sparse semantic work, and quiescence on real data.

```bash
mix run examples/05_alert_swarm/fetch.exs --year 2025
TYPESAFE_API_KEY=... mix run examples/05_alert_swarm/run.exs --limit-events 10000 --semantic-groups 25
```

`--limit-events 0` processes the full downloaded year. Increase the limit to exercise larger populations. The fetched `.csv.gz` and expanded CSV live under `.plexus-data/noaa-storm-events/` and are ignored by Git.

Source: NOAA/NCEI Storm Events bulk CSV archive.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
