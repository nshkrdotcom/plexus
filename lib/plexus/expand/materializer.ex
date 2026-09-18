defmodule Plexus.Expand.Materializer do
  @moduledoc "Converts schema-validated proposal objects into interpreter commands."

  @spec commands(map(), (String.t() -> module()), keyword()) :: [Plexus.Actor.Command.t()]
  def commands(response, module_resolver, opts \\ []) when is_function(module_resolver, 1) do
    proposals = Map.get(response, :proposals) || Map.get(response, "proposals") || []
    default_parent = Keyword.get(opts, :parent_id)

    Enum.flat_map(proposals, fn proposal ->
      id = fetch(proposal, :id)
      class_name = fetch(proposal, :class)
      content = fetch(proposal, :content)
      parent = value(proposal, :parent, default_parent)
      module = module_resolver.(class_name)
      class = class_atom(class_name)

      spawn =
        {:spawn, class, module, %{content: content}, actor_id: id, parent_id: parent, metadata: %{proposal: true}}

      edges =
        proposal
        |> value(:edges, [])
        |> Enum.map(fn edge ->
          type = edge |> fetch(:type) |> class_atom()
          to = fetch(edge, :to)
          weight = value(edge, :weight, 1.0)
          {:edge, type, id, to, weight, %{source: :expansion}}
        end)

      [spawn | edges]
    end)
  end

  defp fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.fetch!(map, Atom.to_string(key))
    end
  end

  defp value(map, key, default) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  # Class/type strings come from schema-validated application output. Use
  # existing atoms only so remote data cannot grow the atom table.
  defp class_atom(value) when is_atom(value), do: value
  defp class_atom(value) when is_binary(value), do: String.to_existing_atom(value)
end
