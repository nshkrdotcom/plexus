defmodule Plexus.Run.Supervisor do
  @moduledoc false
  use Supervisor

  alias Plexus.Run.Names

  def start_link(opts) do
    run_id = Keyword.fetch!(opts, :id)
    Supervisor.start_link(__MODULE__, opts, name: Names.run_supervisor(run_id))
  end

  @impl true
  def init(opts) do
    run_id = Keyword.fetch!(opts, :id)

    task_limit =
      Keyword.get(opts, :task_limit, Application.get_env(:plexus, :default_task_limit, 64))

    actor_partitions = Keyword.get(opts, :actor_partitions, max(System.schedulers_online(), 1))
    expand_concurrency = Keyword.get(opts, :expand_concurrency, 4)

    children = [
      {Plexus.Run.Owner, opts},
      {Task.Supervisor, name: Names.task_supervisor(run_id), max_children: task_limit},
      {Task.Supervisor,
       name: Names.expand_task_supervisor(run_id), max_children: max(expand_concurrency, 1)},
      {PartitionSupervisor,
       child_spec: DynamicSupervisor,
       name: Names.actor_supervisors(run_id),
       partitions: max(actor_partitions, 1)},
      {DynamicSupervisor, name: Names.measure_supervisor(run_id), strategy: :one_for_one},
      {Plexus.Schedule.Server, run_id: run_id, regime: Keyword.get(opts, :schedule, :async)},
      {Plexus.Expand.Queue,
       run_id: run_id,
       max_concurrency: expand_concurrency,
       adapter: Keyword.get(opts, :expand_adapter),
       client: Keyword.get(opts, :inference_client),
       required_capabilities:
         Keyword.get(Keyword.get(opts, :expand, []), :required_capabilities, [])}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
