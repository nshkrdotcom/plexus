defmodule Plexus.Measure do
  @moduledoc """
  Framework-managed local measurement.

  Requests are resolved through the contract registry, cache/replay layers and a
  per-contract coalescer. The coalescer is the normal path; `batch: false`
  simply makes a one-item/zero-delay coalescer rather than bypassing policy.
  """

  alias Plexus.{Budget, Cache, Contract, Record, Run}
  alias Plexus.Contract.Registry, as: ContractRegistry
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

  @spec submit(term(), map(), term(), term(), term(), keyword()) :: :ok
  def submit(run_id, context, tag, state, contract_ref, opts \\ []) do
    config = Config.fetch!(run_id)

    case ContractRegistry.resolve(run_id, contract_ref) do
      {:ok, entry} ->
        memo_key = Contract.memo_key(state, entry.prepared, opts)

        case replay_or_cache(run_id, config, memo_key) do
          {:hit, result, source} ->
            Record.append(run_id, :measurement_reused, %{
              actor_id: context.actor_id,
              source: source
            })

            deliver(run_id, context.actor_id, tag, result)

          :miss ->
            admit(run_id, context, tag, state, entry, memo_key, opts)
        end

      {:error, :not_found} ->
        deliver(run_id, context.actor_id, tag, {:error, {:contract_not_found, contract_ref}})
    end

    :ok
  end

  defp admit(run_id, context, tag, state, entry, memo_key, opts) do
    config = Config.fetch!(run_id)

    case Budget.reserve(config.budget, :measure, 1) do
      :ok ->
        Quiescence.add(config.quiescence, :measurements, 1)
        submit_coalesced(run_id, context, tag, state, entry, memo_key, opts)

      {:error, :budget_exhausted} ->
        deliver(run_id, context.actor_id, tag, {:error, {:budget_exhausted, :measure}})
    end
  end

  @spec cancel_actor(term(), term()) :: :ok
  def cancel_actor(run_id, actor_id) do
    config = Config.fetch!(run_id)

    for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(config.measure_supervisor),
        is_pid(pid) do
      :ok = GenServer.call(pid, {:cancel_actor, actor_id}, :infinity)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp replay_or_cache(run_id, %{replay: :replay}, memo_key) do
    case Record.replay_fetch(run_id, memo_key) do
      {:ok, value} -> {:hit, value, :replay}
      :error -> {:hit, {:error, {:replay_miss, memo_key}}, :replay_miss}
    end
  end

  defp replay_or_cache(run_id, _config, memo_key) do
    case Cache.get(run_id, memo_key) do
      {:ok, value} -> {:hit, value, :cache}
      :miss -> :miss
    end
  end

  defp submit_coalesced(run_id, context, tag, state, entry, memo_key, opts) do
    batch = effective_batch(entry.batch, opts)
    eval_opts = Keyword.drop(opts, [:batch])
    key = coalescer_key(entry.fingerprint, eval_opts, batch)
    pid = ensure_coalescer(run_id, key, entry, eval_opts, batch)

    GenServer.cast(pid, {:submit, memo_key, state, %{actor_id: context.actor_id, tag: tag}})
  end

  defp effective_batch(contract_batch, opts) do
    requested = Keyword.get(opts, :batch, [])

    cond do
      requested == false -> [max: 1, delay_ms: 0, max_in_flight_batches: 1, max_concurrency: 1]
      is_list(requested) -> Keyword.merge(contract_batch, requested)
      true -> contract_batch
    end
  end

  defp ensure_coalescer(run_id, key, entry, eval_opts, batch) do
    name = Names.coalescer(run_id, key)

    case GenServer.whereis(name) do
      pid when is_pid(pid) ->
        pid

      nil ->
        config = Config.fetch!(run_id)

        spec =
          {Plexus.Measure.Coalescer,
           run_id: run_id, key: key, entry: entry, evaluation_options: eval_opts, batch: batch}

        case DynamicSupervisor.start_child(config.measure_supervisor, spec) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
          {:error, reason} -> raise "failed to start measurement coalescer: #{inspect(reason)}"
        end
    end
  end

  defp coalescer_key(fingerprint, opts, batch) do
    stable_opts = opts |> Keyword.drop([:cancellation, :telemetry_metadata]) |> Enum.sort()

    :crypto.hash(:sha256, :erlang.term_to_binary({fingerprint, stable_opts, batch}))
    |> Base.encode16(case: :lower)
  end

  defp deliver(run_id, actor_id, tag, result) do
    Run.cast(run_id, actor_id, {:plexus, :measurement, tag, result})
  end
end
