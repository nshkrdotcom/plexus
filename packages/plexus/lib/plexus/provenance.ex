defmodule Plexus.Provenance do
  @moduledoc "Epoch/staleness invalidation over typed `:depends_on` edges."

  alias Plexus.{Graph, Record, Run}
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

  @spec invalidate(term(), term(), keyword()) :: [term()]
  def invalidate(run_id, upstream_id, opts \\ []) do
    {invalidated, notify} =
      do_invalidate(run_id, upstream_id, %{}, %{})

    invalidated = Map.keys(invalidated)

    if Keyword.get(opts, :notify, false),
      do: notify_invalidated(run_id, upstream_id, Map.keys(notify))

    invalidated
  end

  defp notify_invalidated(run_id, upstream_id, invalidated) do
    invalidated
    |> Enum.reject(&(&1 == upstream_id))
    |> Enum.each(&notify_invalidated_actor(run_id, upstream_id, &1))
  end

  defp notify_invalidated_actor(run_id, upstream_id, actor_id) do
    case Graph.get(run_id, actor_id) do
      %{epoch: epoch} ->
        _ = Run.cast(run_id, actor_id, {:plexus, :invalidated, upstream_id, epoch})

      _ ->
        :ok
    end
  end

  @spec repair(term(), term(), non_neg_integer() | nil) :: :ok | {:error, term()}
  def repair(run_id, actor_id, expected_epoch \\ nil) do
    case Graph.get_and_update(run_id, actor_id, &clear_stale(&1, expected_epoch)) do
      {:ok, attrs} ->
        if is_nil(expected_epoch) or attrs.epoch == expected_epoch do
          discard_repair(run_id, actor_id)
          :ok
        else
          {:error, :stale_epoch}
        end

      error ->
        error
    end
  end

  defp discard_repair(run_id, actor_id) do
    table = Config.fetch!(run_id).tables.repairs
    encoded = :erlang.term_to_binary(actor_id)
    :ets.match_delete(table, {{:_, encoded}, actor_id})
    :ok
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
      attrs
      |> Map.put(:stale, true)
      |> Map.update(:epoch, 1, &(&1 + 1))
    end)

    case Graph.get(run_id, actor_id) do
      nil ->
        false

      attrs ->
        priority = Map.get(attrs, :repair_priority, 0)
        encoded = :erlang.term_to_binary(actor_id)
        key = {-priority, encoded}
        table = Config.fetch!(run_id).tables.repairs

        newly_pending? = :ets.insert_new(table, {key, actor_id})

        if newly_pending? do
          Record.append(run_id, :repair_queued, %{
            actor_id: actor_id,
            epoch: attrs.epoch,
            priority: priority
          })
        end

        newly_pending?
    end
  end

  @spec do_invalidate(term(), term(), map(), map()) :: {map(), map()}
  defp do_invalidate(run_id, upstream_id, seen, notify) do
    if Map.has_key?(seen, upstream_id) do
      {seen, notify}
    else
      seen = Map.put(seen, upstream_id, true)

      Graph.incoming(run_id, upstream_id, :depends_on)
      |> Enum.reduce({seen, notify}, fn edge, {seen, notify} ->
        invalidate_edge(run_id, edge, seen, notify)
      end)
    end
  end

  @spec invalidate_edge(term(), map(), map(), map()) :: {map(), map()}
  defp invalidate_edge(run_id, edge, seen, notify) do
    if Map.has_key?(seen, edge.node) do
      {seen, notify}
    else
      notify =
        if enqueue(run_id, edge.node),
          do: Map.put(notify, edge.node, true),
          else: notify

      do_invalidate(run_id, edge.node, seen, notify)
    end
  end
end
