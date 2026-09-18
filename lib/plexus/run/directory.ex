defmodule Plexus.Run.Directory do
  @moduledoc false
  use GenServer

  @table :plexus_run_directory

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    table =
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, table}
  end

  @spec put(term(), :ets.tid()) :: true
  def put(run_id, config_table), do: :ets.insert(@table, {run_id, config_table})

  @spec fetch(term()) :: {:ok, :ets.tid()} | :error
  def fetch(run_id) do
    case :ets.lookup(@table, run_id) do
      [{^run_id, table}] -> {:ok, table}
      [] -> :error
    end
  end

  @spec delete(term()) :: true
  def delete(run_id), do: :ets.delete(@table, run_id)
end
