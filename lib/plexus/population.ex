defmodule Plexus.Population do
  @moduledoc "Population queries and selection operators over the per-run node table."

  alias Plexus.Graph

  @spec all(term()) :: [{term(), map()}]
  def all(run_id), do: Graph.nodes(run_id)

  @spec by_class(term(), term()) :: [{term(), map()}]
  def by_class(run_id, class), do: Graph.by_class(run_id, class)

  @spec where(term(), (term(), map() -> boolean())) :: [{term(), map()}]
  def where(run_id, predicate) when is_function(predicate, 2) do
    Enum.filter(all(run_id), fn {id, attrs} -> predicate.(id, attrs) end)
  end

  @spec top_k(term(), term(), non_neg_integer(), (map() -> term())) :: [{term(), map()}]
  def top_k(run_id, class, k, score_fun) when is_integer(k) and k >= 0 and is_function(score_fun, 1) do
    run_id
    |> by_class(class)
    |> Enum.sort_by(fn {_id, attrs} -> score_fun.(attrs) end, :desc)
    |> Enum.take(k)
  end

  @spec pareto(term(), term(), [(map() -> number())]) :: [{term(), map()}]
  def pareto(run_id, class, objectives) when is_list(objectives) do
    nodes = by_class(run_id, class)
    Enum.reject(nodes, fn {_id, attrs} -> Enum.any?(nodes, fn {_oid, other} -> dominates?(other, attrs, objectives) end) end)
  end

  defp dominates?(left, right, objectives) do
    pairs = Enum.map(objectives, &{&1.(left), &1.(right)})
    Enum.all?(pairs, fn {a, b} -> a >= b end) and Enum.any?(pairs, fn {a, b} -> a > b end)
  end
end
