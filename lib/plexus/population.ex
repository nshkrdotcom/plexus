defmodule Plexus.Population do
  @moduledoc "Population queries and selection operators over the per-run node table."

  alias Plexus.{Actor, Graph}
  alias Plexus.Population.Index

  @doc """
  Index a top-level node attribute in run-owned ETS. Queries validate candidate
  rows against current attributes. Historical index values remain until run
  teardown, avoiding lost entries under concurrent updates; index low-cardinality
  metadata, not unbounded counters. Concurrent queries are not snapshots.
  """
  @spec index(term(), term()) :: :ok
  defdelegate index(run_id, field), to: Index, as: :create

  @doc "Find current nodes with an indexed attribute equal to the requested value."
  @spec lookup(term(), term(), term()) :: [{term(), map()}]
  defdelegate lookup(run_id, field, value), to: Index

  @spec all(term()) :: [{term(), map()}]
  def all(run_id), do: Graph.nodes(run_id)

  @spec by_class(term(), term()) :: [{term(), map()}]
  def by_class(run_id, class), do: Graph.by_class(run_id, class)

  @spec where(term(), (term(), map() -> boolean())) :: [{term(), map()}]
  def where(run_id, predicate) when is_function(predicate, 2) do
    Enum.filter(all(run_id), fn {id, attrs} -> predicate.(id, attrs) end)
  end

  @spec top_k(term(), term(), non_neg_integer(), (map() -> term())) :: [{term(), map()}]
  def top_k(run_id, class, k, score_fun)
      when is_integer(k) and k >= 0 and is_function(score_fun, 1) do
    run_id
    |> by_class(class)
    |> Enum.sort_by(fn {_id, attrs} -> score_fun.(attrs) end, :desc)
    |> Enum.take(k)
  end

  @spec pareto(term(), term(), [(map() -> number())]) :: [{term(), map()}]
  def pareto(run_id, class, objectives) when is_list(objectives) do
    nodes = by_class(run_id, class)

    Enum.reject(nodes, fn {_id, attrs} ->
      Enum.any?(nodes, fn {_oid, other} -> dominates?(other, attrs, objectives) end)
    end)
  end

  @doc "Weighted sampling with replacement and an explicit reproducible seed."
  @spec resample(term(), term(), non_neg_integer(), (map() -> number()), integer()) :: [
          {term(), map()}
        ]
  def resample(run_id, class, count, weight, seed) when is_integer(count) and count >= 0 do
    weighted =
      by_class(run_id, class)
      |> Enum.sort()
      |> Enum.map(fn {_id, attrs} = row -> {row, weight.(attrs)} end)

    unless Enum.all?(weighted, fn {_, w} -> is_number(w) and w >= 0 end),
      do: raise(ArgumentError, "weights must be non-negative")

    total = Enum.sum(Enum.map(weighted, &elem(&1, 1)))

    if count == 0 or total == 0 do
      []
    else
      rng = :rand.seed_s(:exsss, {seed, seed + 1, seed + 2})

      Enum.map_reduce(1..count, rng, fn _, rng ->
        {p, next} = :rand.uniform_s(rng)
        {pick(weighted, p * total), next}
      end)
      |> elem(0)
    end
  end

  @spec tournament(term(), term(), pos_integer(), (map() -> number()), integer()) ::
          {term(), map()} | nil
  def tournament(run_id, class, size, score, seed) when size > 0 do
    resample(run_id, class, size, fn _ -> 1 end, seed)
    |> Enum.max_by(fn {_id, attrs} -> score.(attrs) end, fn -> nil end)
  end

  @doc "Replicate initial application input; pass init_arg in opts to supply a current snapshot."
  def replicate(context, source, target, opts \\ []),
    do: Actor.dispatch(context, {:population, {:replicate, source, target, opts}})

  @doc "Replace a leaf with siblings using application-supplied input maps. Failed admission rolls back births."
  def split(context, source, children),
    do: Actor.dispatch(context, {:population, {:split, source, children}})

  @doc "Replace compatible sibling leaves using an application-merged input map."
  def merge(context, sources, target, init_arg),
    do: Actor.dispatch(context, {:population, {:merge, sources, target, init_arg}})

  @doc "Move a leaf to another run using an explicit application snapshot; mailbox contents are not migrated."
  def migrate(context, source, destination, init_arg),
    do: Actor.dispatch(context, {:population, {:migrate, source, destination, init_arg}})

  defp pick([{row, weight} | _], target) when target <= weight and weight > 0, do: row
  defp pick([{_, weight} | rest], target), do: pick(rest, target - weight)

  defp dominates?(left, right, objectives) do
    pairs = Enum.map(objectives, &{&1.(left), &1.(right)})
    Enum.all?(pairs, fn {a, b} -> a >= b end) and Enum.any?(pairs, fn {a, b} -> a > b end)
  end
end
