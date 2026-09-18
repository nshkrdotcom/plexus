defmodule Plexus.Provenance do
  @moduledoc "Epoch/staleness invalidation over typed `:depends_on` edges."

  alias Plexus.Graph

  @spec depend(term(), term(), term(), term()) :: :ok
  def depend(run_id, derived_id, upstream_id, provenance \\ nil) do
    Graph.add_edge(run_id, :depends_on, derived_id, upstream_id, 1.0, provenance)
  end

  @spec invalidate(term(), term()) :: [term()]
  def invalidate(run_id, upstream_id) do
    do_invalidate(run_id, upstream_id, MapSet.new()) |> MapSet.to_list()
  end

  @spec repair(term(), term()) :: :ok | {:error, :not_found}
  def repair(run_id, actor_id) do
    Graph.update(run_id, actor_id, fn attrs ->
      attrs |> Map.put(:stale, false) |> Map.update(:epoch, 1, &(&1 + 1))
    end)
  end

  defp do_invalidate(run_id, upstream_id, seen) do
    if MapSet.member?(seen, upstream_id) do
      seen
    else
      seen = MapSet.put(seen, upstream_id)

      Graph.incoming(run_id, upstream_id, :depends_on)
      |> Enum.reduce(seen, fn edge, acc ->
        Graph.update(run_id, edge.node, &Map.put(&1, :stale, true))
        do_invalidate(run_id, edge.node, acc)
      end)
    end
  end
end
