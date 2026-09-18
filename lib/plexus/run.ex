defmodule Plexus.Run do
  @moduledoc """
  Run-scoped coordination without a hot coordinator mailbox.

  A run is a supervised resource island. Its immutable/rarely-mutated config is
  read from per-run ETS, actor processes are partitioned across dynamic
  supervisors, and graph/cache/record operations go directly to per-run ETS.
  """

  alias Plexus.{Budget, Graph, Record, Registry, Telemetry}
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

  @type t :: pid() | term()

  @spec start_run(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_run(opts) do
    run_id = Keyword.get(opts, :id, make_ref())
    opts = Keyword.put(opts, :id, run_id)

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
        GenServer.cast(pid, message)
        :ok

      {:error, :not_found} = error ->
        error
    end
  end

  @spec call(t(), term(), term(), timeout()) :: term()
  def call(run, actor_id, message, timeout \\ 5_000) do
    with {:ok, pid} <- Registry.lookup(run_id(run), actor_id) do
      GenServer.call(pid, message, timeout)
    end
  end

  @spec terminate_actor(t(), term()) :: :ok | {:error, :not_found}
  def terminate_actor(run, actor_id) do
    run_id = run_id(run)
    config = Config.fetch!(run_id)

    with {:ok, pid} <- Registry.lookup(run_id, actor_id) do
      supervisor = actor_partition(config, actor_id)

      case DynamicSupervisor.terminate_child(supervisor, pid) do
        :ok ->
          was_active? =
            case Graph.get(run_id, actor_id) do
              %{status: :complete} -> false
              _ -> true
            end

          :ets.match_delete(config.tables.waiters, {:_, actor_id})
          Graph.delete_node(run_id, actor_id)
          Budget.refund(config.budget, :population, 1)
          if was_active?, do: safe_counter_add(config.quiescence, :actors, -1)
          Record.append(run_id, :actor_death, %{actor_id: actor_id})
          Telemetry.emit(run_id, [:actor, :stop], %{}, %{actor_id: actor_id})
          :ok

        {:error, :not_found} = error ->
          error
      end
    end
  end

  @doc "Cancel work and terminate every process in the child subtree before deleting topology."
  @spec prune(t(), term()) :: :ok
  def prune(run, actor_id) do
    run_id = run_id(run)
    ids = Graph.subtree(run_id, actor_id)

    Enum.each(ids, fn id ->
      Plexus.Measure.cancel_actor(run_id, id)
      Plexus.Expand.Queue.cancel_actor(run_id, id)
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

    result =
      DynamicSupervisor.start_child(actor_partition(config, actor_id), {module, child_opts})

    case result do
      {:ok, pid} ->
        :ok =
          Graph.put(config.run_id, actor_id,
            module: module,
            class: class,
            parent: parent_id,
            depth: depth,
            metadata: metadata,
            status: :active,
            started_at: System.monotonic_time()
          )

        if parent_id != nil, do: Graph.attach_child(config.run_id, parent_id, actor_id)
        safe_counter_add(config.quiescence, :actors, 1)

        Record.append(config.run_id, :actor_birth, %{
          actor_id: actor_id,
          parent_id: parent_id,
          class: class
        })

        Telemetry.emit(config.run_id, [:actor, :start], %{}, %{actor_id: actor_id, class: class})
        {:ok, pid}

      other ->
        Budget.refund(config.budget, :population, 1)
        other
    end
  rescue
    error ->
      Budget.refund(config.budget, :population, 1)
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

  defp safe_counter_add(ref, counter, delta) do
    current = Quiescence.get(ref, counter)
    if delta >= 0 or current + delta >= 0, do: Quiescence.add(ref, counter, delta), else: :ok
  end

  defp bounded_reason(reason) when reason in [:normal, :shutdown], do: reason
  defp bounded_reason({:shutdown, _}), do: :shutdown
  defp bounded_reason(_reason), do: :other
end
