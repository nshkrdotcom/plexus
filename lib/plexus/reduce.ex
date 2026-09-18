defmodule Plexus.Reduce do
  @moduledoc "Population reducers for explicit run answers."

  alias Plexus.Population

  @spec top_k(term(), term(), non_neg_integer(), (map() -> term())) :: [{term(), map()}]
  def top_k(run_id, class, k, score_fun), do: Population.top_k(run_id, class, k, score_fun)

  @spec one(term(), term(), (map() -> term())) :: {term(), map()} | nil
  def one(run_id, class, score_fun) do
    run_id |> Population.top_k(class, 1, score_fun) |> List.first()
  end

  @spec collect(term(), (term(), map() -> term())) :: [term()]
  def collect(run_id, mapper), do: Enum.map(Population.all(run_id), fn {id, attrs} -> mapper.(id, attrs) end)
end
