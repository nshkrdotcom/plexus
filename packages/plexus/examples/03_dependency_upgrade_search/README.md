# 03 — deps.dev dependency upgrade search

Builds two real resolved dependency graphs from the stable **deps.dev v3 API**, compares a source and target package version, preserves every target graph node and edge as Plexus topology, and asks TypeSafe/Jev only about exact target version keys absent from the source graph. This deliberately avoids assuming a dependency graph contains only one node per package name/version.

Those semantic risk/attention measurements then drive a bounded actor **beam search** over migration order. Search actors respect changed-dependency prerequisites, branch across multiple ready choices, retain only the best beam, and prune losing plan actors. The output therefore includes both a semantic migration-risk frontier and concrete dependency-aware migration orders rather than a flat graph diff.

```bash
mix run examples/03_dependency_upgrade_search/fetch.exs \
  --system NPM --package eslint --from 8.57.0 --to 9.35.0

TYPESAFE_API_KEY=... mix run examples/03_dependency_upgrade_search/run.exs \
  --search-items 8 --beam-width 6 --branch-width 4
```

Useful controls: `--max-nodes`, `--search-items`, `--beam-width`, `--branch-width`. Larger graph slices increase the dependency actor population; `--search-items` bounds the combinatorial migration-order search separately. The search presents human-sized package-level migration steps, so duplicate changed graph nodes for one package are collapsed only at the planning stage; the underlying actor graph and exact version-key diff remain intact.

Supported dependency graphs follow deps.dev coverage (notably npm, Cargo, Maven, and PyPI for `GetDependencies`). Downloaded JSON is stored under `.plexus-data/deps-dev-*` and ignored by Git.

Source: Google Open Source Insights / deps.dev v3 API.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
