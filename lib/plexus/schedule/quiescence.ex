defmodule Plexus.Schedule.Quiescence do
  @moduledoc """
  Lock-free counters used by stop conditions and barrier experiments.
  """

  @counters [:actors, :messages, :measurements, :expansions, :timers]
  @index @counters |> Enum.with_index(1) |> Map.new()

  @type t :: :counters.counters_ref()

  @spec new() :: t()
  def new, do: :counters.new(length(@counters), [:write_concurrency])

  @spec add(t(), atom(), integer()) :: :ok
  def add(ref, counter, delta) when is_integer(delta) do
    :counters.add(ref, Map.fetch!(@index, counter), delta)
    :ok
  end

  @doc false
  def retire_actor(config, lifecycle_ref) do
    case :ets.take(config.tables.active_actors, lifecycle_ref) do
      [] -> :ok
      [_] -> add(config.quiescence, :actors, -1)
    end
  end

  @spec get(t(), atom()) :: integer()
  def get(ref, counter), do: :counters.get(ref, Map.fetch!(@index, counter))

  @spec snapshot(t()) :: map()
  def snapshot(ref), do: Map.new(@counters, &{&1, get(ref, &1)})

  @spec quiescent?(t()) :: boolean()
  def quiescent?(ref) do
    snapshot(ref)
    |> Map.values()
    |> Enum.all?(&(&1 == 0))
  end
end
