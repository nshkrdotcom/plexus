defmodule Plexus.Schedule.Quiescence do
  @moduledoc """
  Lock-free counters used by stop conditions and barrier experiments.
  """

  @counters [:actors, :messages, :measurements, :expansions, :timers]
  @index @counters |> Enum.with_index(1) |> Map.new()

  @spec new() :: reference()
  def new, do: :counters.new(length(@counters), [:write_concurrency])

  @spec add(reference(), atom(), integer()) :: :ok
  def add(ref, counter, delta) when is_integer(delta) do
    :counters.add(ref, Map.fetch!(@index, counter), delta)
    :ok
  end

  @spec get(reference(), atom()) :: integer()
  def get(ref, counter), do: :counters.get(ref, Map.fetch!(@index, counter))

  @spec snapshot(reference()) :: map()
  def snapshot(ref), do: Map.new(@counters, &{&1, get(ref, &1)})

  @spec quiescent?(reference()) :: boolean()
  def quiescent?(ref) do
    snapshot(ref)
    |> Map.values()
    |> Enum.all?(&(&1 == 0))
  end
end
