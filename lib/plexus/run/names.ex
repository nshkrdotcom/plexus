defmodule Plexus.Run.Names do
  @moduledoc false

  @registry Plexus.Registry

  def run_supervisor(run_id), do: via({:run_supervisor, run_id})
  def owner(run_id), do: via({:run_owner, run_id})
  def task_supervisor(run_id), do: via({:task_supervisor, run_id})
  def expand_task_supervisor(run_id), do: via({:expand_task_supervisor, run_id})
  def actor_supervisors(run_id), do: via({:actor_supervisors, run_id})
  def measure_supervisor(run_id), do: via({:measure_supervisor, run_id})
  def schedule_server(run_id), do: via({:schedule_server, run_id})
  def expand_queue(run_id), do: via({:expand_queue, run_id})
  def coalescer(run_id, key), do: via({:measure_coalescer, run_id, key})

  def actor(run_id, actor_id), do: via({:actor, run_id, actor_id})

  def via(key), do: {:via, Registry, {@registry, key}}
end
