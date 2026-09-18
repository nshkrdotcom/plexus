defmodule Plexus.Record do
  @moduledoc """
  Append-only run event log plus a fixed-response replay store.
  """

  alias Plexus.Run.Config

  @spec append(term(), atom() | tuple(), map()) :: non_neg_integer()
  def append(run_id, type, data \\ %{}) when is_map(data) do
    config = Config.fetch!(run_id)
    sequence = :atomics.add_get(config.event_sequence, 1, 1)

    event = %{
      sequence: sequence,
      type: type,
      monotonic_time: System.monotonic_time(),
      system_time: System.system_time(),
      data: data
    }

    :ets.insert(config.tables.events, {sequence, event})
    sequence
  end

  @spec events(term()) :: [map()]
  def events(run_id) do
    config = Config.fetch!(run_id)

    :ets.tab2list(config.tables.events)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  @spec replay_put(term(), String.t(), term()) :: :ok
  def replay_put(run_id, key, value) do
    config = Config.fetch!(run_id)
    :ets.insert(config.tables.replay, {key, value})
    :ok
  end

  @spec replay_fetch(term(), String.t()) :: {:ok, term()} | :error
  def replay_fetch(run_id, key) do
    config = Config.fetch!(run_id)

    case :ets.lookup(config.tables.replay, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :error
    end
  end

  @spec replay_entries(term()) :: [{String.t(), term()}]
  def replay_entries(run_id) do
    config = Config.fetch!(run_id)

    :ets.tab2list(config.tables.replay)
    |> Enum.sort_by(&elem(&1, 0))
  end

  @spec load_replay(term(), Enumerable.t()) :: :ok
  def load_replay(run_id, entries) do
    config = Config.fetch!(run_id)
    true = :ets.insert(config.tables.replay, Enum.to_list(entries))
    :ok
  end
end
