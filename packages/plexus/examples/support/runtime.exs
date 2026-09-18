Code.require_file("typesafe_metrics.exs", __DIR__)

defmodule Plexus.Examples.Support.Runtime do
  @moduledoc false

  alias Plexus.{Graph, Run, Stop}
  alias Plexus.Schedule.Quiescence

  def root, do: Path.expand("../..", __DIR__)

  def cli_args, do: normalize_cli_args(System.argv())

  def normalize_cli_args(["--" | rest]), do: rest
  def normalize_cli_args(args) when is_list(args), do: args

  def data_dir(slug, opts \\ []) do
    Keyword.get(opts, :data_dir) ||
      Path.join([root(), ".plexus-data", slug])
  end

  def client! do
    api_key = System.fetch_env!("TYPESAFE_API_KEY")

    opts = [api_key: api_key]
    opts = maybe_put_env(opts, :base_url, "TYPESAFE_BASE_URL")
    opts = maybe_put_first_env(opts, :model, ["TYPESAFE_MODEL", "TYPESAFE_DEFAULT_MODEL"])
    client = TypeSafeSDK.new_client(opts)
    Plexus.Examples.Support.TypeSafeMetrics.register_client(client)
    client
  end

  def token_budget(opts, expected_calls, config \\ []) do
    floor = Keyword.get(config, :floor, 250_000)
    per_call = Keyword.get(config, :per_call, 8_000)
    requested = Keyword.get(opts, :token_budget)
    budget = requested || max(floor, max(expected_calls, 1) * per_call)

    if not is_integer(budget) or budget < 1 do
      raise ArgumentError, "--token-budget must be a positive integer"
    end

    budget
  end

  def start_run!(opts \\ []) do
    client = Keyword.get_lazy(opts, :client, &client!/0)

    defaults = [
      client: client,
      max_population: 100_000,
      budgets: [measure: 5_000, expand: 100, tokens: 2_000_000],
      batch: [max: 32, delay_ms: 10, max_concurrency: 8],
      cache: [ttl_ms: 300_000, max_entries: 100_000],
      schedule: :async,
      replay: :record
    ]

    case Plexus.start_run(Keyword.merge(defaults, Keyword.delete(opts, :client))) do
      {:ok, run} -> run
      other -> raise "could not start Plexus run: #{inspect(other)}"
    end
  end

  def await_class_complete!(run, class, expected, timeout_ms \\ 120_000) do
    run_id = Run.run_id(run)

    await!(
      fn ->
        Graph.by_class(run_id, class)
        |> Enum.count(fn {_id, attrs} -> attrs[:status] == :complete end)
        |> Kernel.>=(expected)
      end,
      timeout_ms,
      "#{inspect(class)} actors to complete"
    )
  end

  def await_quiescent!(run, timeout_ms \\ 120_000) do
    run_id = Run.run_id(run)
    predicate = Stop.quiescent()
    await!(fn -> predicate.(run_id) end, timeout_ms, "run to reach quiescence")
  end

  def await_messages_drained!(run, timeout_ms \\ 120_000) do
    quiescence = Run.config(run).quiescence

    await!(
      fn -> Quiescence.get(quiescence, :messages) == 0 end,
      timeout_ms,
      "managed messages to drain"
    )
  end

  def await_actor_ids_complete!(run, actor_ids, timeout_ms \\ 120_000) do
    run_id = Run.run_id(run)

    await!(
      fn ->
        Enum.all?(actor_ids, fn actor_id ->
          case Graph.get(run_id, actor_id) do
            %{status: :complete} -> true
            _ -> false
          end
        end)
      end,
      timeout_ms,
      "selected actors to complete"
    )
  end

  def await!(predicate, timeout_ms, label) when is_function(predicate, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await(predicate, deadline, label)
  end

  def stop(run) do
    case Plexus.stop_run(run) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end

  def top_complete(run, class, k, score_fun) do
    Run.run_id(run)
    |> Graph.by_class(class)
    |> Enum.filter(fn {_id, attrs} -> attrs[:status] == :complete end)
    |> Enum.sort_by(fn {_id, attrs} -> score_fun.(attrs[:result] || %{}) end, :desc)
    |> Enum.take(k)
  end

  defp do_await(predicate, deadline, label) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "timed out waiting for #{label}"

      true ->
        Process.sleep(25)
        do_await(predicate, deadline, label)
    end
  end

  defp maybe_put_env(opts, key, env) do
    case System.get_env(env) do
      nil -> opts
      "" -> opts
      value -> Keyword.put(opts, key, value)
    end
  end

  defp maybe_put_first_env(opts, _key, []), do: opts

  defp maybe_put_first_env(opts, key, [env | rest]) do
    case System.get_env(env) do
      nil -> maybe_put_first_env(opts, key, rest)
      "" -> maybe_put_first_env(opts, key, rest)
      value -> Keyword.put(opts, key, value)
    end
  end
end
