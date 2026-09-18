defmodule Plexus.Run do
  @moduledoc """
  Per-run coordinator.

  A run owns:

  - one shared `TypeSafeSDK.Client`
  - one `Task.Supervisor` used by all semantic actors in the run
  - one `DynamicSupervisor` for actor processes
  - graph metadata for parent/child relationships
  """

  use GenServer

  alias Plexus.{Graph, Registry}

  @type t :: pid()

  defstruct [:id, :client, :task_supervisor, :actor_supervisor]

  @spec start_run(keyword()) :: DynamicSupervisor.on_start_child()
  def start_run(opts) do
    spec = {__MODULE__, opts}
    DynamicSupervisor.start_child(Plexus.RunSupervisor, spec)
  end

  @spec stop_run(pid() | atom(), term()) :: :ok
  def stop_run(run, reason \\ :normal), do: GenServer.stop(run, reason)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :client)
    run_id = Keyword.get(opts, :id, make_ref())

    {:ok, task_supervisor} =
      Task.Supervisor.start_link(Keyword.get(opts, :actor_task_supervisor_opts, max_children: 64))

    {:ok, actor_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    {:ok,
     %__MODULE__{
       id: run_id,
       client: client,
       task_supervisor: task_supervisor,
       actor_supervisor: actor_supervisor
     }}
  end

  @spec start_actor(pid() | atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_actor(run, opts), do: GenServer.call(run, {:start_actor, opts}, 30_000)

  @spec actor_pid(pid() | atom(), term()) :: {:ok, pid()} | {:error, :not_found}
  def actor_pid(run, actor_id), do: GenServer.call(run, {:actor_pid, actor_id})

  @spec run_id(pid() | term()) :: term()
  def run_id(run) when is_pid(run), do: GenServer.call(run, :run_id)
  def run_id(run_id), do: run_id

  @spec cast(pid() | atom(), term(), term()) :: :ok
  def cast(run, actor_id, message) do
    with {:ok, pid} <- actor_pid(run, actor_id) do
      GenServer.cast(pid, message)
    end

    :ok
  end

  @spec call(pid() | atom(), term(), term(), timeout()) :: term()
  def call(run, actor_id, message, timeout \\ 5_000) do
    with {:ok, pid} <- actor_pid(run, actor_id) do
      GenServer.call(pid, message, timeout)
    end
  end

  @impl true
  def handle_call(:run_id, _from, state) do
    {:reply, state.id, state}
  end

  def handle_call({:actor_pid, actor_id}, _from, state) do
    {:reply, Registry.lookup(state.id, actor_id), state}
  end

  def handle_call({:start_actor, opts}, _from, state) do
    module = Keyword.fetch!(opts, :module)
    actor_id = Keyword.fetch!(opts, :actor_id)
    parent_id = Keyword.get(opts, :parent_id)
    init_arg = Keyword.get(opts, :init_arg, %{})

    max_in_flight =
      Keyword.get(opts, :max_in_flight, Application.get_env(:plexus, :default_max_in_flight, 16))

    evaluation_options = Keyword.get(opts, :evaluation_options, [])

    child_opts = [
      id: {module, actor_id},
      name: Registry.via(state.id, actor_id),
      client: state.client,
      task_supervisor: state.task_supervisor,
      max_in_flight: max_in_flight,
      evaluation_options: evaluation_options,
      init_arg:
        Map.merge(init_arg, %{
          run: self(),
          run_id: state.id,
          actor_id: actor_id,
          parent_id: parent_id
        })
    ]

    result = DynamicSupervisor.start_child(state.actor_supervisor, {module, child_opts})

    case result do
      {:ok, pid} ->
        :ok = Graph.put(state.id, actor_id, module: module, parent: parent_id, children: [])
        if parent_id, do: :ok = Graph.attach_child(state.id, parent_id, actor_id)
        {:reply, {:ok, pid}, state}

      other ->
        {:reply, other, state}
    end
  end
end
