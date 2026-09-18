defmodule Plexus.Graph do
  @moduledoc """
  Per-run lock-free node and typed-edge graph.

  Nodes live in an ETS `:set`. Edges live in an ETS `:ordered_set` and are stored in
  both outgoing and incoming directions, avoiding children-list read/modify/write
  races and the global graph GenServer from the original scaffold.
  """

  alias Plexus.Population.Index
  alias Plexus.Run.Config

  @type edge_type :: atom()

  @spec put(term(), term(), keyword() | map()) :: :ok
  def put(run_id, actor_id, attrs) do
    config = Config.fetch!(run_id)
    attrs = attrs |> Enum.into(%{}) |> Map.put_new(:epoch, 0) |> Map.put_new(:stale, false)

    old_class =
      case :ets.lookup(config.tables.nodes, actor_id) do
        [{^actor_id, existing}] -> Map.get(existing, :class)
        [] -> nil
      end

    new_class = Map.get(attrs, :class)

    if old_class != nil and old_class != new_class do
      :ets.delete(config.tables.node_classes, class_key(old_class, actor_id))
    end

    if new_class != nil do
      :ets.insert(config.tables.node_classes, {class_key(new_class, actor_id), actor_id})
    end

    :ets.insert(config.tables.nodes, {actor_id, attrs})
    Index.update(config, actor_id, attrs)
    :ok
  end

  @spec update(term(), term(), (map() -> map())) :: :ok | {:error, :not_found}
  def update(run_id, actor_id, fun) when is_function(fun, 1) do
    config = Config.fetch!(run_id)

    case update_row(config, actor_id, fun) do
      {:ok, _previous} -> :ok
      error -> error
    end
  end

  @doc false
  def get_and_update(run_id, actor_id, fun), do: update_row(Config.fetch!(run_id), actor_id, fun)

  defp update_row(config, actor_id, fun) do
    case :ets.lookup(config.tables.nodes, actor_id) do
      [] ->
        {:error, :not_found}

      [{^actor_id, attrs}] ->
        updated = fun.(attrs)

        match = update_match(actor_id, attrs, updated)

        case :ets.select_replace(config.tables.nodes, match) do
          0 ->
            update_row(config, actor_id, fun)

          1 ->
            index_class(config, actor_id, updated)
            Index.update(config, actor_id, updated)
            {:ok, attrs}
        end
    end
  end

  defp update_match(actor_id, attrs, updated) do
    if match_variable?(actor_id) do
      [
        {{:"$1", :"$2"}, [{:"=:=", :"$1", {:const, actor_id}}, {:"=:=", :"$2", {:const, attrs}}],
         [{{:"$1", {:const, updated}}}]}
      ]
    else
      [{{actor_id, :"$1"}, [{:"=:=", :"$1", {:const, attrs}}], [{:const, {actor_id, updated}}]}]
    end
  end

  defp match_variable?(value) when is_atom(value),
    do: value == :_ or String.starts_with?(Atom.to_string(value), "$")

  defp match_variable?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.any?(&match_variable?/1)

  defp match_variable?(value) when is_list(value), do: Enum.any?(value, &match_variable?/1)
  defp match_variable?(value) when is_map(value), do: true
  defp match_variable?(_), do: false

  defp index_class(config, actor_id, attrs) do
    if class = Map.get(attrs, :class),
      do: :ets.insert(config.tables.node_classes, {class_key(class, actor_id), actor_id})
  end

  @doc false
  def class_key(class, actor_id),
    do: {:erlang.term_to_binary(class), :erlang.term_to_binary(actor_id)}

  @spec get(term(), term()) :: map() | nil
  def get(run_id, actor_id) do
    config = Config.fetch!(run_id)

    case :ets.lookup(config.tables.nodes, actor_id) do
      [{^actor_id, attrs}] -> attrs
      [] -> nil
    end
  end

  @spec nodes(term()) :: [{term(), map()}]
  def nodes(run_id) do
    config = Config.fetch!(run_id)
    :ets.tab2list(config.tables.nodes)
  end

  @spec by_class(term(), term()) :: [{term(), map()}]
  def by_class(run_id, class) do
    config = Config.fetch!(run_id)

    :ets.match_object(config.tables.node_classes, {{:erlang.term_to_binary(class), :_}, :_})
    |> Enum.flat_map(fn {_key, actor_id} ->
      case :ets.lookup(config.tables.nodes, actor_id) do
        [{^actor_id, %{class: ^class} = attrs}] -> [{actor_id, attrs}]
        [_] -> []
        [] -> []
      end
    end)
  end

  @spec count(term()) :: non_neg_integer()
  def count(run_id) do
    config = Config.fetch!(run_id)
    :ets.info(config.tables.nodes, :size)
  end

  @spec add_edge(term(), edge_type(), term(), term(), number(), term()) :: :ok
  def add_edge(run_id, type, from, to, weight \\ 1.0, provenance \\ nil)
      when is_atom(type) and is_number(weight) do
    config = Config.fetch!(run_id)
    edge = config.tables.edges

    :ets.insert(
      edge,
      {edge_key(:out, from, type, to, weight, provenance), {type, to, weight, provenance}}
    )

    :ets.insert(
      edge,
      {edge_key(:in, to, type, from, weight, provenance), {type, from, weight, provenance}}
    )

    :ok
  end

  @spec attach_child(term(), term(), term()) :: :ok
  def attach_child(run_id, parent_id, child_id), do: add_edge(run_id, :child, parent_id, child_id)

  @spec outgoing(term(), term(), edge_type() | :all) :: [map()]
  def outgoing(run_id, actor_id, type \\ :all), do: edges(run_id, :out, actor_id, type)

  @spec incoming(term(), term(), edge_type() | :all) :: [map()]
  def incoming(run_id, actor_id, type \\ :all), do: edges(run_id, :in, actor_id, type)

  @spec children(term(), term()) :: [term()]
  def children(run_id, actor_id) do
    run_id
    |> outgoing(actor_id, :child)
    |> Enum.map(& &1.node)
  end

  @spec subtree(term(), term()) :: [term()]
  def subtree(run_id, actor_id),
    do: do_subtree(run_id, actor_id, MapSet.new()) |> MapSet.to_list()

  @spec delete_node(term(), term()) :: :ok
  def delete_node(run_id, actor_id) do
    config = Config.fetch!(run_id)

    case :ets.lookup(config.tables.nodes, actor_id) do
      [{^actor_id, %{class: class}}] ->
        :ets.delete(config.tables.node_classes, class_key(class, actor_id))

      _ ->
        :ok
    end

    :ets.delete(config.tables.nodes, actor_id)
    delete_edges_for(config.tables.edges, actor_id)
    :ok
  end

  @spec delete_subtree(term(), term()) :: :ok
  def delete_subtree(run_id, actor_id) do
    Enum.each(subtree(run_id, actor_id), &delete_node(run_id, &1))
    :ok
  end

  @doc "Prune through the runtime so evaluations/processes are stopped before metadata is removed."
  @spec prune(term(), term()) :: :ok
  def prune(run_id, actor_id), do: Plexus.Run.prune(run_id, actor_id)

  defp edges(run_id, direction, actor_id, type) do
    table = Config.fetch!(run_id).tables.edges

    edge_rows(table, direction, actor_id, type)
    |> Enum.map(fn {_key, {type, node, weight, provenance}} ->
      %{type: type, node: node, weight: weight, provenance: provenance}
    end)
  end

  defp edge_rows(table, direction, actor_id, type) do
    encoded_type = if type == :all, do: :_, else: :erlang.term_to_binary(type)

    :ets.match_object(
      table,
      {{direction, :erlang.term_to_binary(actor_id), encoded_type, :_}, :_}
    )
  end

  defp edge_key(direction, actor_id, type, node, weight, provenance) do
    {direction, :erlang.term_to_binary(actor_id), :erlang.term_to_binary(type),
     :erlang.term_to_binary({node, weight, provenance})}
  end

  defp do_subtree(run_id, actor_id, visited) do
    if MapSet.member?(visited, actor_id) do
      visited
    else
      Enum.reduce(children(run_id, actor_id), MapSet.put(visited, actor_id), fn child, acc ->
        do_subtree(run_id, child, acc)
      end)
    end
  end

  defp delete_edges_for(table, actor_id) do
    for {direction, reverse} <- [out: :in, in: :out],
        {key, {type, node, weight, provenance}} <- edge_rows(table, direction, actor_id, :all) do
      :ets.delete(table, key)
      :ets.delete(table, edge_key(reverse, node, type, actor_id, weight, provenance))
    end

    :ok
  end
end
