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

    {:ok, %{table: table, monitors: %{}}}
  end

  @spec put(term(), :ets.tid()) :: true
  def put(run_id, config_table) do
    true = :ets.insert(@table, {run_id, config_table})
    GenServer.cast(__MODULE__, {:monitor_owner, run_id, config_table, self()})
    true
  end

  @impl true
  def handle_cast({:monitor_owner, run_id, table, owner}, state) do
    ref = Process.monitor(owner)
    {:noreply, %{state | monitors: Map.put(state.monitors, ref, {run_id, table})}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {{run_id, table}, monitors} ->
        :ets.delete_object(@table, {run_id, table})
        {:noreply, %{state | monitors: monitors}}
    end
  end

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
