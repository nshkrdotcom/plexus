defmodule Plexus.Stop do
  @moduledoc "Composable run stop predicates."

  alias Plexus.{Budget, Graph, Record}
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

  @doc "Record one comparable objective value per completed round (larger is better)."
  @spec observe(term(), number()) :: non_neg_integer()
  def observe(run_id, value) when is_number(value),
    do: Record.append(run_id, :stop_observation, %{value: value})

  @doc "An explicit confidence projection; callers choose calibrated values or a justified score."
  @spec confidence(number(), (map() -> number())) :: predicate()
  def confidence(threshold, score) when is_function(score, 1) do
    fn run_id ->
      Enum.any?(Graph.nodes(run_id), fn {_id, attrs} -> score.(attrs) > threshold end)
    end
  end

  @doc "Deadline in monotonic milliseconds; use System.monotonic_time(:millisecond) + duration."
  @spec deadline(integer()) :: predicate()
  def deadline(at) when is_integer(at), do: fn _ -> System.monotonic_time(:millisecond) >= at end

  @spec no_improvement(pos_integer()) :: predicate()
  def no_improvement(k) when is_integer(k) and k > 0 do
    fn run_id ->
      values = observations(run_id)
      {previous, recent} = Enum.split(values, max(length(values) - k, 0))
      previous != [] and Enum.max(recent) <= Enum.max(previous)
    end
  end

  @spec stable(number(), pos_integer()) :: predicate()
  def stable(epsilon, k) when epsilon >= 0 and is_integer(k) and k > 0 do
    fn run_id ->
      values = Enum.take(observations(run_id), -(k + 1))
      length(values) == k + 1 and Enum.max(values) - Enum.min(values) <= epsilon
    end
  end

  @spec oscillation(pos_integer(), pos_integer(), number()) :: predicate()
  def oscillation(period, cycles, epsilon \\ 0.0)
      when is_integer(period) and period > 1 and is_integer(cycles) and cycles > 1 and
             epsilon >= 0 do
    fn run_id ->
      values = Enum.take(observations(run_id), -(period * cycles))
      length(values) == period * cycles and repeating?(values, period, epsilon)
    end
  end

  defp repeating?(values, period, epsilon) do
    pattern = Enum.take(values, period)

    Enum.max(pattern) - Enum.min(pattern) > epsilon and
      Enum.with_index(values)
      |> Enum.all?(fn {value, i} -> abs(value - Enum.at(pattern, rem(i, period))) <= epsilon end)
  end

  defp observations(run_id) do
    Record.events(run_id)
    |> Enum.filter(&(&1.type == :stop_observation))
    |> Enum.map(& &1.data.value)
  end

  @spec any([predicate()]) :: predicate()
  def any(predicates), do: fn run_id -> Enum.any?(predicates, & &1.(run_id)) end

  @spec all([predicate()]) :: predicate()
  def all(predicates), do: fn run_id -> Enum.all?(predicates, & &1.(run_id)) end
end
