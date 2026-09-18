defmodule Plexus.Run do
  @moduledoc """
  Run-scoped coordination without a hot coordinator mailbox.

  A run is a supervised resource island. Its immutable/rarely-mutated config is
  read from per-run ETS, actor processes are partitioned across dynamic
  supervisors, and graph/cache/record operations go directly to per-run ETS.
  """

  alias Plexus.Actor.Activity
  alias Plexus.{Budget, Graph, Measure, Record, Registry, Schedule, Telemetry}
  alias Plexus.Expand.Queue, as: ExpandQueue
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

  @type t :: pid() | term()

  @spec start_run(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_run(opts) do
    run_id = Keyword.get(opts, :id, make_ref())
    opts = Keyword.put(opts, :id, run_id)

    opts =
      if Keyword.get(opts, :inference_client),
        do: Keyword.put_new(opts, :expand_adapter, Plexus.Expand.InferenceAdapter),
        else: opts

    case DynamicSupervisor.start_child(Plexus.RunSupervisor, {Plexus.Run.Supervisor, opts}) do
      {:ok, _supervisor} -> Registry.lookup_run(run_id)
      {:error, {:already_started, _pid}} -> {:error, {:already_started, run_id}}
      other -> other
    end
  end

  @spec stop_run(t(), term()) :: :ok | {:error, :not_found}
  def stop_run(run, reason \\ :normal) do
    run_id = run_id(run)
    _ = Record.append(run_id, :run_stop_requested, %{reason: bounded_reason(reason)})
    _ = Config.update(run_id, &Map.put(&1, :stopping, true))

    case GenServer.whereis(Names.run_supervisor(run_id)) do
      nil -> {:error, :not_found}
      supervisor -> DynamicSupervisor.terminate_child(Plexus.RunSupervisor, supervisor)
    end
  rescue
    ArgumentError -> {:error, :not_found}
  end

  @spec run_id(t()) :: term()
  def run_id(run) when is_pid(run) do
    case Registry.run_id(run) do
      {:ok, id} ->
        id

      {:error, :not_found} ->
        raise ArgumentError, "pid is not a Plexus run owner: #{inspect(run)}"
    end
  end

  def run_id(run_id), do: run_id

  @spec start_actor(t(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_actor(run, opts) do
    run_id = run_id(run)
    config = Config.fetch!(run_id)
    module = Keyword.fetch!(opts, :module)
    actor_id = Keyword.fetch!(opts, :actor_id)
    parent_id = Keyword.get(opts, :parent_id)
    class = Keyword.get(opts, :class, module)
    metadata = Keyword.get(opts, :metadata, %{})
    depth = depth(run_id, parent_id)

    with :ok <- ensure_actor_absent(run_id, actor_id),
         :ok <- admit_depth(config.max_depth, depth),
         :ok <- admit_population(config.max_population, run_id),
         :ok <- Budget.reserve(config.budget, :population, 1) do
      do_start_actor(config, module, actor_id, parent_id, class, metadata, depth, opts)
    end
  end

  @spec actor_pid(t(), term()) :: {:ok, pid()} | {:error, :not_found}
  def actor_pid(run, actor_id), do: Registry.lookup(run_id(run), actor_id)

  @spec cast(t(), term(), term()) :: :ok | {:error, :not_found}
  def cast(run, actor_id, message) do
    run_id = run_id(run)

    case Registry.lookup(run_id, actor_id) do
      {:ok, pid} ->
        ticket = Activity.begin(run_id, actor_id)
        if not Process.alive?(pid), do: Activity.finish(ticket)
        GenServer.cast(pid, {:plexus_tracked, ticket, message})
        :ok

      {:error, :not_found} = error ->
        error
    end
  end

  @spec call(t(), term(), term(), timeout()) :: term()
  def call(run, actor_id, message, timeout \\ 5_000) do
    with {:ok, pid} <- Registry.lookup(run_id(run), actor_id) do
      ticket = Activity.begin(run_id(run), actor_id)
      if not Process.alive?(pid), do: Activity.finish(ticket)
      GenServer.call(pid, {:plexus_tracked, ticket, message}, timeout)
    end
  end

  @spec terminate_actor(t(), term()) :: :ok | {:error, :not_found}
  def terminate_actor(run, actor_id) do
    run_id = run_id(run)
    config = Config.fetch!(run_id)

    with {:ok, pid} <- Registry.lookup(run_id, actor_id) do
      Schedule.cancel_actor(run_id, actor_id)
      Measure.cancel_actor(run_id, actor_id)
      ExpandQueue.cancel_actor(run_id, actor_id)
      supervisor = actor_partition(config, actor_id)
      terminate_actor_child(supervisor, pid, config, run_id, actor_id)
    end
  end

  defp terminate_actor_child(supervisor, pid, config, run_id, actor_id) do
    case DynamicSupervisor.terminate_child(supervisor, pid) do
      :ok ->
        unregister_actor(config, run_id, actor_id, pid)

      {:error, :not_found} = error ->
        error
    end
  end

  @doc false
  def actor_down(run_id, actor_id, pid) do
    case Graph.get(run_id, actor_id) do
      %{pid: ^pid} ->
        if GenServer.whereis(Names.measure_supervisor(run_id)),
          do: Measure.cancel_actor(run_id, actor_id)

        if GenServer.whereis(Names.expand_queue(run_id)),
          do: ExpandQueue.cancel_actor(run_id, actor_id)

        unregister_actor(Config.fetch!(run_id), run_id, actor_id, pid)

      _ ->
        :ok
    end
  end

  defp unregister_actor(config, run_id, actor_id, expected_pid \\ nil) do
    with_actor_lock(config, actor_id, fn ->
      unregister_locked(config, run_id, actor_id, expected_pid)
    end)
  end

  defp unregister_locked(config, run_id, actor_id, expected_pid) do
    case Graph.get(run_id, actor_id) do
      nil ->
        :ok

      attrs ->
        if Map.get(attrs, :pid) == expected_pid,
          do: remove_actor(config, run_id, actor_id, attrs),
          else: :ok
    end
  end

  defp remove_actor(config, run_id, actor_id, attrs) do
    Schedule.cancel_actor(run_id, actor_id)
    :ets.match_delete(config.tables.waiters, {:_, actor_id})
    Graph.delete_node(run_id, actor_id)
    Budget.refund(config.budget, :population, 1)
    Quiescence.retire_actor(config, attrs.lifecycle_ref)
    Record.append(run_id, :actor_death, %{actor_id: actor_id})
    Telemetry.emit(run_id, [:actor, :stop], %{}, %{actor_id: actor_id})
    :ok
  end

  defp with_actor_lock(config, actor_id, fun) do
    table = config.tables.lifecycle_locks

    if :ets.insert_new(table, {actor_id, self()}) do
      try do
        fun.()
      after
        :ets.delete_object(table, {actor_id, self()})
      end
    else
      await_actor_lock(config, actor_id, fun)
    end
  end

  defp await_actor_lock(config, actor_id, fun) do
    case :ets.lookup(config.tables.lifecycle_locks, actor_id) do
      [{^actor_id, owner}] when owner == self() ->
        fun.()

      [{^actor_id, owner}] ->
        if Process.alive?(owner),
          do: Process.sleep(1),
          else: :ets.delete_object(config.tables.lifecycle_locks, {actor_id, owner})

        with_actor_lock(config, actor_id, fun)

      [] ->
        with_actor_lock(config, actor_id, fun)
    end
  end

  @doc "Cancel work and terminate every process in the child subtree before deleting topology."
  @spec prune(t(), term()) :: :ok
  def prune(run, actor_id) do
    run_id = run_id(run)
    ids = Graph.subtree(run_id, actor_id)

    Enum.each(ids, fn id ->
      Schedule.cancel_actor(run_id, id)
      Measure.cancel_actor(run_id, id)
      ExpandQueue.cancel_actor(run_id, id)
    end)

    ids
    |> Enum.sort_by(&node_depth(run_id, &1), :desc)
    |> Enum.each(fn id ->
      case terminate_actor(run_id, id) do
        :ok -> :ok
        {:error, :not_found} -> Graph.delete_node(run_id, id)
      end
    end)

    Record.append(run_id, :subtree_pruned, %{root: actor_id, count: length(ids)})
    :ok
  end

  @spec config(t()) :: map()
  def config(run), do: Config.fetch!(run_id(run))

  defp do_start_actor(config, module, actor_id, parent_id, class, metadata, depth, opts) do
    telemetry_metadata =
      opts
      |> Keyword.get(:evaluation_options, [])
      |> Keyword.get(:telemetry_metadata, %{})
      |> Map.merge(%{
        plexus_run_id: config.run_id,
        plexus_actor_id: actor_id,
        plexus_class: class
      })

    evaluation_options =
      opts
      |> Keyword.get(:evaluation_options, [])
      |> Keyword.put(:telemetry_metadata, telemetry_metadata)

    init_arg =
      opts
      |> Keyword.get(:init_arg, %{})
      |> Map.merge(%{
        run: config.run_id,
        run_id: config.run_id,
        actor_id: actor_id,
        parent_id: parent_id,
        class: class,
        metadata: metadata
      })

    child_opts = [
      id: {module, actor_id},
      name: Registry.via(config.run_id, actor_id),
      client: config.client,
      task_supervisor: config.task_supervisor,
      max_in_flight: Keyword.get(opts, :max_in_flight, config.default_max_in_flight),
      evaluation_options: evaluation_options,
      init_arg: init_arg
    ]

    attrs = %{
      module: module,
      init_arg: Keyword.get(opts, :init_arg, %{}),
      class: class,
      parent: parent_id,
      depth: depth,
      metadata: metadata,
      status: :active,
      lifecycle_ref: make_ref(),
      epoch: 0,
      stale: false,
      started_at: System.monotonic_time()
    }

    with_actor_lock(config, actor_id, fn -> insert_actor(config, actor_id, attrs, child_opts) end)
  end

  defp insert_actor(config, actor_id, attrs, child_opts) do
    if :ets.insert_new(config.tables.nodes, {actor_id, attrs}) do
      :ets.insert(config.tables.node_classes, {Graph.class_key(attrs.class, actor_id), actor_id})
      if attrs.parent != nil, do: Graph.attach_child(config.run_id, attrs.parent, actor_id)
      Quiescence.add(config.quiescence, :actors, 1)
      :ets.insert(config.tables.active_actors, {attrs.lifecycle_ref})

      start_registered_actor(
        config,
        attrs.module,
        actor_id,
        attrs.parent,
        attrs.class,
        child_opts
      )
    else
      Budget.refund(config.budget, :population, 1)
      {:error, :already_registered}
    end
  end

  defp start_registered_actor(config, module, actor_id, parent_id, class, child_opts) do
    spec = module.child_spec(child_opts) |> Map.put(:restart, :temporary)
    result = DynamicSupervisor.start_child(actor_partition(config, actor_id), spec)

    case result do
      {:ok, pid} ->
        Graph.update(config.run_id, actor_id, &Map.put(&1, :pid, pid))
        GenServer.cast(Names.owner(config.run_id), {:monitor_actor, actor_id, pid})

        Record.append(config.run_id, :actor_birth, %{
          actor_id: actor_id,
          parent_id: parent_id,
          class: class
        })

        Telemetry.emit(config.run_id, [:actor, :start], %{}, %{actor_id: actor_id, class: class})
        {:ok, pid}

      other ->
        unregister_actor(config, config.run_id, actor_id)
        other
    end
  rescue
    error ->
      unregister_actor(config, config.run_id, actor_id)
      reraise error, __STACKTRACE__
  end

  defp actor_partition(config, actor_id) do
    {:via, PartitionSupervisor, {config.actor_supervisors, actor_id}}
  end

  defp ensure_actor_absent(run_id, actor_id) do
    case Registry.lookup(run_id, actor_id) do
      {:error, :not_found} -> :ok
      {:ok, pid} -> {:error, {:already_registered, pid}}
    end
  end

  defp admit_depth(:infinity, _depth), do: :ok
  defp admit_depth(max, depth) when is_integer(max) and depth <= max, do: :ok
  defp admit_depth(_max, _depth), do: {:error, :max_depth}

  defp admit_population(:infinity, _run_id), do: :ok

  defp admit_population(max, run_id) when is_integer(max) do
    if Graph.count(run_id) < max, do: :ok, else: {:error, :max_population}
  end

  defp depth(_run_id, nil), do: 0

  defp depth(run_id, parent_id) do
    case Graph.get(run_id, parent_id) do
      %{depth: parent_depth} when is_integer(parent_depth) -> parent_depth + 1
      _ -> 1
    end
  end

  defp node_depth(run_id, actor_id) do
    case Graph.get(run_id, actor_id) do
      %{depth: depth} -> depth
      _ -> 0
    end
  end

  defp bounded_reason(reason) when reason in [:normal, :shutdown], do: reason
  defp bounded_reason({:shutdown, _}), do: :shutdown
  defp bounded_reason(_reason), do: :other
end
