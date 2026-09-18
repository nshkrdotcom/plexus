defmodule Plexus.Provenance do
  @moduledoc "Epoch/staleness invalidation over typed `:depends_on` edges."

  alias Plexus.{Graph, Record}
  alias Plexus.Run.Config

  @spec depend(term(), term(), term(), term()) :: :ok
  def depend(run_id, derived_id, upstream_id, provenance \\ nil) do
    source = Graph.get(run_id, upstream_id) || %{}

    provenance = %{
      schema_version: 1,
      upstream_epoch: Map.get(source, :epoch, 0),
      evidence: provenance
    }

    Graph.add_edge(run_id, :depends_on, derived_id, upstream_id, 1.0, provenance)
  end

  @spec invalidate(term(), term()) :: [term()]
  def invalidate(run_id, upstream_id) do
    do_invalidate(run_id, upstream_id, %{}) |> Map.keys()
  end

  @spec repair(term(), term(), non_neg_integer() | nil) :: :ok | {:error, term()}
  def repair(run_id, actor_id, expected_epoch \\ nil) do
    case Graph.get_and_update(run_id, actor_id, &clear_stale(&1, expected_epoch)) do
      {:ok, attrs} ->
        if is_nil(expected_epoch) or attrs.epoch == expected_epoch,
          do: :ok,
          else: {:error, :stale_epoch}

      error ->
        error
    end
  end

  defp clear_stale(attrs, expected_epoch) do
    if is_nil(expected_epoch) or attrs.epoch == expected_epoch,
      do: Map.put(attrs, :stale, false),
      else: attrs
  end

  @doc "Take the highest-priority pending repair. Complete using repair/3 with its epoch."
  @spec next_repair(term()) :: {:ok, map()} | :empty
  def next_repair(run_id) do
    table = Config.fetch!(run_id).tables.repairs

    case :ets.first(table) do
      :"$end_of_table" -> :empty
      key -> take_repair(run_id, table, key)
    end
  end

  defp take_repair(run_id, table, key) do
    case :ets.take(table, key) do
      [{_key, actor_id}] ->
        case Graph.get(run_id, actor_id) do
          %{stale: true, epoch: epoch} -> {:ok, %{actor_id: actor_id, epoch: epoch}}
          _ -> next_repair(run_id)
        end

      [] ->
        next_repair(run_id)
    end
  end

  defp enqueue(run_id, actor_id) do
    Graph.update(run_id, actor_id, fn attrs ->
      attrs |> Map.put(:stale, true) |> Map.update(:epoch, 1, &(&1 + 1))
    end)

    case Graph.get(run_id, actor_id) do
      nil ->
        :ok

      attrs ->
        priority = Map.get(attrs, :repair_priority, 0)

        :ets.insert(
          Config.fetch!(run_id).tables.repairs,
          {{-priority, :erlang.term_to_binary(actor_id)}, actor_id}
        )

        Record.append(run_id, :repair_queued, %{
          actor_id: actor_id,
          epoch: attrs.epoch,
          priority: priority
        })
    end
  end

  @spec do_invalidate(term(), term(), map()) :: map()
  defp do_invalidate(run_id, upstream_id, seen) do
    if Map.has_key?(seen, upstream_id) do
      seen
    else
      seen = Map.put(seen, upstream_id, true)

      Graph.incoming(run_id, upstream_id, :depends_on)
      |> Enum.reduce(seen, &invalidate_edge(run_id, &1, &2))
    end
  end

  @spec invalidate_edge(term(), map(), map()) :: map()
  defp invalidate_edge(run_id, edge, seen) do
    unless Map.has_key?(seen, edge.node), do: enqueue(run_id, edge.node)
    do_invalidate(run_id, edge.node, seen)
  end
end
