import Config

config :plexus,
  default_max_in_flight: 16,
  default_task_limit: 64,
  default_batch: [max: 64, delay_ms: 10, max_in_flight_batches: 4, max_concurrency: 8],
  default_cache: [ttl_ms: 60_000, max_entries: 50_000]

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:module, :run_id, :actor_id]
