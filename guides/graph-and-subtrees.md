# Graph and subtrees

Plexus stores population structure in two per-run ETS tables:

- node attributes in a `:set`
- typed edges in a `:bag`

Every edge is indexed both outgoing and incoming:

```elixir
Plexus.Graph.add_edge(run_id, :supports, hypothesis_a, claim_b, 0.82, %{source: :measure_17})
Plexus.Graph.outgoing(run_id, hypothesis_a, :supports)
Plexus.Graph.incoming(run_id, claim_b, :supports)
```

`:child` is conventional and powers `children/2` and `subtree/2`, but the graph is not limited to a tree.

## Pruning

Call `Plexus.prune/2` or emit `{:prune, actor_id}`. Runtime pruning is ordered:

1. remove/cancel queued measurement and expansion work for subtree actors
2. cancel physical TypeSafe batch tokens when no waiters remain
3. terminate actor processes leaves-first
4. remove node/edge metadata

Do not call `Graph.delete_subtree/2` when runtime cancellation is required; that helper is metadata-only.

## Provenance

Use `Plexus.Provenance.depend/4` for derived → upstream dependency edges. Invalidating an upstream node walks incoming `:depends_on` edges and marks dependent nodes stale.
