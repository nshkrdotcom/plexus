defmodule Plexus.Bench.Actor do
  use Plexus.Actor
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Bench.Birth do
  alias Plexus.{Graph, Run}

  def run(count, partitions, topology) do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, actor_partitions: partitions)
    id = Run.run_id(run)
    before_memory = :erlang.memory(:total)
    :erlang.system_flag(:scheduler_wall_time, true)
    before_schedulers = :erlang.statistics(:scheduler_wall_time)
    started = System.monotonic_time(:microsecond)

    latencies =
      for n <- 1..count do
        birth = System.monotonic_time(:microsecond)
        opts = [module: Plexus.Bench.Actor, actor_id: n]
        opts = if topology != :none and n > 1, do: Keyword.put(opts, :parent_id, 1), else: opts
        {:ok, _} = Run.start_actor(run, opts)

        if topology == :typed and n > 1 do
          Graph.add_edge(id, :supports, n, 1)
          Graph.add_edge(id, :neighbor, n, n - 1)
        end

        System.monotonic_time(:microsecond) - birth
      end

    elapsed = System.monotonic_time(:microsecond) - started
    config = Run.config(run)
    partitions_pids = Enum.map(Supervisor.which_children(config.actor_supervisors), &elem(&1, 1))
    queue = fn pid -> elem(Process.info(pid, :message_queue_len), 1) end

    queues = %{
      owner: queue.(run),
      partitions: Enum.map(partitions_pids, queue),
      registry:
        Process.registered()
        |> Enum.filter(&String.starts_with?(Atom.to_string(&1), "Elixir.Plexus.Registry"))
        |> Enum.map(&(Process.whereis(&1) |> queue.()))
    }

    # Drain monitor registration before measuring retained memory.
    :sys.get_state(run)

    after_schedulers =
      Map.new(:erlang.statistics(:scheduler_wall_time), fn {i, a, t} -> {i, {a, t}} end)

    utilization =
      Enum.map(before_schedulers, fn {i, a, t} ->
        {aa, tt} = Map.fetch!(after_schedulers, i)
        %{scheduler: i, utilization: if(tt == t, do: 0.0, else: (aa - a) / (tt - t))}
      end)

    sorted = Enum.sort(latencies)
    percentile = fn p -> Enum.at(sorted, min(trunc(count * p), count - 1)) end
    memory = :erlang.memory(:total)

    result = %{
      count: count,
      partitions: partitions,
      topology: topology,
      elapsed_us: elapsed,
      births_per_second: count * 1_000_000 / elapsed,
      latency_us: %{p50: percentile.(0.5), p95: percentile.(0.95), p99: percentile.(0.99)},
      total_memory_bytes: memory,
      incremental_bytes_per_actor: (memory - before_memory) / count,
      ets_bytes:
        Map.new(
          [:nodes, :node_classes, :edges],
          &{&1, :ets.info(config.tables[&1], :memory) * :erlang.system_info(:wordsize)}
        ),
      mailboxes: queues,
      scheduler_utilization: utilization
    }

    Plexus.stop_run(run)
    TypeSafeSDK.Test.close(client)
    result
  end
end

File.mkdir_p!("artifacts/benchmarks")
schedulers = System.schedulers_online()
path = "artifacts/benchmarks/node-birth.jsonl"
File.write!(path, "")

for count <- [1_000, 10_000, 100_000],
    partitions <- Enum.uniq([max(div(schedulers, 2), 1), schedulers, schedulers * 2]),
    topology <- [:none, :child, :typed] do
  result = Plexus.Bench.Birth.run(count, partitions, topology)
  File.write!(path, Jason.encode!(result) <> "\n", [:append])

  IO.puts(
    "#{count} actors / #{partitions} partitions / #{topology}: #{Float.round(result.births_per_second)} births/s"
  )
end
