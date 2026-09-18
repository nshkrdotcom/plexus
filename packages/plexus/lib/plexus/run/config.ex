defmodule Plexus.Run.Config do
  @moduledoc """
  Lock-free read access to run-scoped resources.

  The global directory contains only `run_id -> config_table` pointers. The
  actual client, ETS table ids, limits and counters live in a table owned by the
  run's owner process and disappear when that run terminates.
  """

  alias Plexus.Run.Directory

  @spec fetch(term()) :: {:ok, map()} | {:error, :run_not_found}
  def fetch(run_id) do
    with {:ok, table} <- Directory.fetch(run_id),
         [{:config, config}] <- :ets.lookup(table, :config) do
      {:ok, config}
    else
      _ -> {:error, :run_not_found}
    end
  rescue
    ArgumentError -> {:error, :run_not_found}
  end

  @spec fetch!(term()) :: map()
  def fetch!(run_id) do
    case fetch(run_id) do
      {:ok, config} -> config
      {:error, :run_not_found} -> raise ArgumentError, "unknown Plexus run: #{inspect(run_id)}"
    end
  end

  @spec update(term(), (map() -> map())) :: :ok | {:error, :run_not_found}
  def update(run_id, fun) when is_function(fun, 1) do
    with {:ok, table} <- Directory.fetch(run_id),
         [{:config, config}] <- :ets.lookup(table, :config) do
      :ets.insert(table, {:config, fun.(config)})
      :ok
    else
      _ -> {:error, :run_not_found}
    end
  rescue
    ArgumentError -> {:error, :run_not_found}
  end
end
