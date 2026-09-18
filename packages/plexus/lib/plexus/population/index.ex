defmodule Plexus.Population.Index do
  @moduledoc false
  alias Plexus.Graph
  alias Plexus.Run.Config

  def create(run_id, field) do
    config = Config.fetch!(run_id)
    :ets.insert(config.tables.index_fields, {field})
    Enum.each(Graph.nodes(run_id), fn {id, attrs} -> update(config, id, attrs) end)
    :ok
  end

  def update(config, actor_id, attrs) do
    for {field} <- :ets.tab2list(config.tables.index_fields), Map.has_key?(attrs, field) do
      key = {encode(field), encode(Map.fetch!(attrs, field)), encode(actor_id)}
      :ets.insert(config.tables.population_indexes, {key, actor_id})
    end

    :ok
  end

  def lookup(run_id, field, value) do
    config = Config.fetch!(run_id)

    :ets.match_object(config.tables.population_indexes, {{encode(field), encode(value), :_}, :_})
    |> Enum.flat_map(fn {_key, id} -> current(run_id, id, field, value) end)
  end

  defp current(run_id, id, field, value) do
    case Graph.get(run_id, id) do
      nil -> []
      attrs -> if Map.fetch(attrs, field) == {:ok, value}, do: [{id, attrs}], else: []
    end
  end

  defp encode(value), do: :erlang.term_to_binary(value, [:deterministic])
end
