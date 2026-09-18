defmodule Plexus.Cache do
  @moduledoc """
  Bounded per-run ETS response cache keyed by `Plexus.Contract.memo_key/2`.
  """

  alias Plexus.Run.Config

  @spec get(term(), term()) :: {:ok, term()} | :miss
  def get(run_id, key) do
    config = Config.fetch!(run_id)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(config.tables.cache, key) do
      [{^key, :infinity, value}] ->
        {:ok, value}

      [{^key, expires_at, value}] when expires_at > now ->
        {:ok, value}

      [{^key, _expires_at, _value}] ->
        :ets.delete(config.tables.cache, key)
        :miss

      [] ->
        :miss
    end
  end

  @spec put(term(), term(), term(), keyword()) :: :ok
  def put(run_id, key, value, opts \\ []) do
    config = Config.fetch!(run_id)
    cache_opts = Keyword.merge(config.cache, opts)
    ttl_ms = Keyword.get(cache_opts, :ttl_ms, 60_000)
    max_entries = Keyword.get(cache_opts, :max_entries, 50_000)

    expires_at =
      case ttl_ms do
        :infinity -> :infinity
        value when is_integer(value) and value > 0 -> System.monotonic_time(:millisecond) + value
        _ -> raise ArgumentError, "cache ttl_ms must be a positive integer or :infinity"
      end

    :ets.insert(config.tables.cache, {key, expires_at, value})
    trim(config.tables.cache, max_entries)
    :ok
  end

  @spec delete(term(), term()) :: :ok
  def delete(run_id, key) do
    config = Config.fetch!(run_id)
    :ets.delete(config.tables.cache, key)
    :ok
  end

  defp trim(_table, :infinity), do: :ok

  defp trim(table, max_entries) when is_integer(max_entries) and max_entries > 0 do
    extra = :ets.info(table, :size) - max_entries

    if extra > 0 do
      table
      |> :ets.tab2list()
      |> Enum.take(extra)
      |> Enum.each(fn {key, _, _} -> :ets.delete(table, key) end)
    end

    :ok
  end
end
