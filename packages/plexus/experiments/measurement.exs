defmodule Plexus.Bench.MeasureActor do
  use Plexus.Actor
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_cast({:plexus, :measurement, tag, result}, state) do
    send(state.owner, {:result, tag, System.monotonic_time(:microsecond), elem(result, 0)})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Bench.Measurement do
  alias Plexus.{Actor, Cache, Contract, Record, Run}

  def run(batch, delay, concurrency, in_flight, duplicate, cache_rate) do
    count = 128
    client = TypeSafeSDK.Test.client() |> TypeSafeSDK.Test.stub(flag: {:noul, 0.8})
    prepared = TypeSafeSDK.prepare!(flag: TypeSafeSDK.noul("Flag?"))

    {:ok, run} =
      Plexus.start_run(
        client: client,
        actor_partitions: 1,
        batch: [
          max: batch,
          delay_ms: delay,
          max_concurrency: concurrency,
          max_in_flight_batches: in_flight
        ]
      )

    id = Run.run_id(run)

    {:ok, _} =
      Run.start_actor(run,
        module: Plexus.Bench.MeasureActor,
        actor_id: :probe,
        init_arg: %{owner: self()}
      )

    unique = max(round(count * (1 - duplicate)), 1)
    {:ok, fixture} = TypeSafeSDK.evaluate(client, %{i: 0}, prepared)
    cached = round(unique * cache_rate)

    if cached > 0 do
      for n <- 0..(cached - 1),
          do: Cache.put(id, Contract.memo_key(%{i: n}, prepared), {:ok, fixture})
    end

    before_requests = TypeSafeSDK.Test.stats(client).total
    started = System.monotonic_time(:microsecond)

    submissions =
      Map.new(0..(count - 1), fn tag ->
        at = System.monotonic_time(:microsecond)

        Actor.dispatch(
          %{run_id: id, actor_id: :probe},
          {:measure, tag, %{i: rem(tag, unique)}, prepared, []}
        )

        {tag, at}
      end)

    responses =
      for _ <- 1..count do
        receive do
          {:result, tag, at, status} -> {at - Map.fetch!(submissions, tag), status}
        after
          10_000 -> raise "lost benchmark waiter"
        end
      end

    elapsed = System.monotonic_time(:microsecond) - started
    physical = TypeSafeSDK.Test.stats(client).total - before_requests
    latencies = responses |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    batches = Record.events(id) |> Enum.filter(&(&1.type == :measurement_batch_start))

    result = %{
      batch: batch,
      delay_ms: delay,
      internal_concurrency: concurrency,
      in_flight_batches: in_flight,
      duplicate_rate: duplicate,
      cache_rate: cache_rate,
      logical_count: count,
      physical_http_requests: physical,
      physical_batch_items: Enum.sum(Enum.map(batches, & &1.data.size)),
      batch_calls: length(batches),
      elapsed_us: elapsed,
      logical_per_second: count * 1_000_000 / elapsed,
      requests_per_second: physical * 1_000_000 / elapsed,
      reduction: 1 - physical / count,
      p50_us: Enum.at(latencies, div(count, 2)),
      p95_us: Enum.at(latencies, trunc(count * 0.95)),
      errors: Enum.count(responses, &(elem(&1, 1) == :error)),
      transport: "TypeSafeSDK.Test; no provider rate limit or network latency"
    }

    Plexus.stop_run(run)
    TypeSafeSDK.Test.close(client)
    result
  end
end

File.mkdir_p!("artifacts/benchmarks")
path = "artifacts/benchmarks/measurement.jsonl"
File.write!(path, "")

for batch <- [1, 8, 32, 64],
    delay <- [0, 1, 10, 25],
    concurrency <- [1, 8],
    in_flight <- [1, 4],
    duplicate <- [0.0, 0.25, 0.75],
    cache_rate <- [0.0, 0.5] do
  result =
    Plexus.Bench.Measurement.run(batch, delay, concurrency, in_flight, duplicate, cache_rate)

  File.write!(path, Jason.encode!(result) <> "\n", [:append])
end

IO.puts("Completed 384 fixture throughput configurations.")
