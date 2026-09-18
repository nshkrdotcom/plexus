import Config

config :plexus,
  default_max_in_flight: 16,
  default_batch_concurrency: 8,
  graph_table: :plexus_graph,
  registry: Plexus.Registry,
  actor_supervisor: Plexus.ActorSupervisor,
  run_supervisor: Plexus.RunSupervisor,
  task_supervisor: Plexus.TaskSupervisor

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:module, :run_id, :actor_id]
