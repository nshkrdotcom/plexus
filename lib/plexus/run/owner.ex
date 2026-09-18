defmodule Plexus.Run.Owner do
  @moduledoc false
  use GenServer

  alias Plexus.{Budget, Schedule, Telemetry}
  alias Plexus.Run.{Directory, Names}
  alias Plexus.Schedule.Quiescence

  def start_link(opts) do
    run_id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: Names.owner(run_id))
  end

  @impl true
  def init(opts) do
    run_id = Keyword.fetch!(opts, :id)
    client = Keyword.fetch!(opts, :client)

    tables = %{
      config: table(:set),
      nodes: table(:set),
      node_classes: table(:bag),
      edges: table(:bag),
      cache: table(:set),
      contracts: table(:set),
      events: table(:ordered_set),
      replay: table(:set),
      waiters: table(:bag)
    }

    max_population = Keyword.get(opts, :max_population, :infinity)
    budget_opts = Keyword.get(opts, :budgets, [])

    budget_opts =
      case budget_opts do
        values when is_list(values) ->
          if max_population != :infinity and not Keyword.has_key?(values, :population),
            do: Keyword.put(values, :population, max_population),
            else: values

        values when is_map(values) ->
          if max_population != :infinity, do: Map.put_new(values, :population, max_population), else: values
      end

    budget = Budget.new(budget_opts)
    quiescence = Quiescence.new()
    event_sequence = :atomics.new(1, signed: false)

    config = %{
      run_id: run_id,
      client: client,
      tables: tables,
      budget: budget,
      quiescence: quiescence,
      event_sequence: event_sequence,
      task_supervisor: Names.task_supervisor(run_id),
      expand_task_supervisor: Names.expand_task_supervisor(run_id),
      actor_supervisors: Names.actor_supervisors(run_id),
      measure_supervisor: Names.measure_supervisor(run_id),
      schedule_server: Names.schedule_server(run_id),
      expand_queue: Names.expand_queue(run_id),
      max_depth: Keyword.get(opts, :max_depth, :infinity),
      max_population: max_population,
      default_max_in_flight:
        Keyword.get(
          opts,
          :default_max_in_flight,
          Application.get_env(:plexus, :default_max_in_flight, 16)
        ),
      default_batch: Keyword.get(opts, :batch, Application.get_env(:plexus, :default_batch, [])),
      cache: Keyword.get(opts, :cache, Application.get_env(:plexus, :default_cache, [])),
      replay: Keyword.get(opts, :replay, :off),
      schedule: Schedule.normalize(Keyword.get(opts, :schedule, :async)),
      inference_client: Keyword.get(opts, :inference_client),
      expand_adapter: Keyword.get(opts, :expand_adapter),
      expand: Keyword.get(opts, :expand, [])
    }

    :ets.insert(tables.config, {:config, config})
    true = Directory.put(run_id, tables.config)

    {:ok, %{run_id: run_id, tables: tables}, {:continue, :attach_telemetry}}
  end

  @impl true
  def handle_continue(:attach_telemetry, state) do
    Telemetry.attach_typesafe(state.run_id)
    Telemetry.emit(state.run_id, [:run, :start], %{system_time: System.system_time()}, %{})
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    Telemetry.emit(state.run_id, [:run, :stop], %{system_time: System.system_time()}, %{})
    Telemetry.detach_typesafe(state.run_id)
    Directory.delete(state.run_id)
    :ok
  end

  defp table(kind) do
    :ets.new(:plexus_run_table, [
      kind,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end
end
