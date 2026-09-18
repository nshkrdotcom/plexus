defmodule Plexus.Stop do
  @moduledoc "Composable run stop predicates."

  alias Plexus.{Budget, Graph}
  alias Plexus.Run.Config
  alias Plexus.Schedule.Quiescence

  @type predicate :: (term() -> boolean())

  @spec quiescent() :: predicate()
  def quiescent, do: fn run_id -> Quiescence.quiescent?(Config.fetch!(run_id).quiescence) end

  @spec budget_exhausted(Plexus.Budget.meter()) :: predicate()
  def budget_exhausted(meter) do
    fn run_id -> Budget.remaining(Config.fetch!(run_id).budget, meter) == 0 end
  end

  @spec population_at_most(non_neg_integer()) :: predicate()
  def population_at_most(max), do: fn run_id -> Graph.count(run_id) <= max end

  @spec any([predicate()]) :: predicate()
  def any(predicates), do: fn run_id -> Enum.any?(predicates, & &1.(run_id)) end

  @spec all([predicate()]) :: predicate()
  def all(predicates), do: fn run_id -> Enum.all?(predicates, & &1.(run_id)) end
end
