defmodule Plexus.Measure.Coalescer do
  @moduledoc """
  Dedupe + bounded-window batcher backed by `TypeSafeSDK.evaluate_many/4`.
  """

  use GenServer

  alias Plexus.{Budget, Cache, Record, Run, Telemetry}
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

  def start_link(opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    key = Keyword.fetch!(opts, :key)
    GenServer.start_link(__MODULE__, opts, name: Names.coalescer(run_id, key))
  end

  @impl true
  def init(opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    entry = Keyword.fetch!(opts, :entry)
    config = Config.fetch!(run_id)
    batch = Keyword.get(opts, :batch, [])

    max_batch = Keyword.get(batch, :max, Keyword.get(config.default_batch, :max, 64))
    delay_ms = Keyword.get(batch, :delay_ms, Keyword.get(config.default_batch, :delay_ms, 10))

    max_in_flight_batches =
      Keyword.get(batch, :max_in_flight_batches, Keyword.get(config.default_batch, :max_in_flight_batches, 4))

    max_concurrency =
      Keyword.get(batch, :max_concurrency, Keyword.get(config.default_batch, :max_concurrency, 8))

    unless is_integer(max_batch) and max_batch > 0, do: raise(ArgumentError, "batch max must be positive")
    unless is_integer(delay_ms) and delay_ms >= 0, do: raise(ArgumentError, "batch delay_ms must be non-negative")

    unless is_integer(max_in_flight_batches) and max_in_flight_batches > 0,
      do: raise(ArgumentError, "max_in_flight_batches must be positive")

    unless is_integer(max_concurrency) and max_concurrency in 1..1024,
      do: raise(ArgumentError, "max_concurrency must be in 1..1024")

    {:ok,
     %{
       run_id: run_id,
       key: Keyword.fetch!(opts, :key),
       prepared: entry.prepared,
       fingerprint: entry.fingerprint,
       evaluation_options: Keyword.get(opts, :evaluation_options, []),
       batch: batch,
       max_batch: max_batch,
       delay_ms: delay_ms,
       max_in_flight_batches: max_in_flight_batches,
       max_concurrency: max_concurrency,
       pending: %{},
       order: [],
       timer: nil,
       in_flight: %{}
     }}
  end

  @impl true
  def handle_cast({:submit, memo_key, state_input, waiter}, state) do
    config = Config.fetch!(state.run_id)

    {state, duplicate?} =
      case Map.fetch(state.pending, memo_key) do
        {:ok, entry} ->
          entry = %{entry | waiters: [waiter | entry.waiters]}
          {%{state | pending: Map.put(state.pending, memo_key, entry)}, true}

        :error ->
          entry = %{memo_key: memo_key, state: state_input, waiters: [waiter]}
          {%{state | pending: Map.put(state.pending, memo_key, entry), order: state.order ++ [memo_key]}, false}
      end

    if duplicate?, do: Budget.refund(config.budget, :measure, 1)

    state = ensure_timer(state)

    if length(state.order) >= state.max_batch and map_size(state.in_flight) < state.max_in_flight_batches do
      send(self(), :flush)
    end

    {:noreply, state}
  end

  def handle_cast({:cancel_actor, actor_id}, state) do
    {:noreply, cancel_actor_state(state, actor_id)}
  end

  @impl true
  def handle_call({:cancel_actor, actor_id}, _from, state) do
    {:reply, :ok, cancel_actor_state(state, actor_id)}
  end

  @impl true
  def handle_info(:flush, state) do
    state = %{state | timer: nil}

    cond do
      state.order == [] -> {:noreply, state}
      map_size(state.in_flight) >= state.max_in_flight_batches -> {:noreply, ensure_timer(state)}
      true -> {:noreply, launch_batch(state)}
    end
  end

  def handle_info({ref, results}, state) when is_reference(ref) do
    case Map.pop(state.in_flight, ref) do
      {nil, _} ->
        {:noreply, state}

      {batch, in_flight} ->
        Process.demonitor(ref, [:flush])
        complete_batch(state.run_id, state.fingerprint, batch.entries, results)
        state = %{state | in_flight: in_flight}
        if state.order != [], do: send(self(), :flush)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.in_flight, ref) do
      {nil, _} -> {:noreply, state}
      {batch, in_flight} ->
        results = List.duplicate({:error, {:batch_task_exit, sanitize_reason(reason)}}, length(batch.entries))
        complete_batch(state.run_id, state.fingerprint, batch.entries, results)
        state = %{state | in_flight: in_flight}
        if state.order != [], do: send(self(), :flush)
        {:noreply, state}
    end
  end

  defp launch_batch(state) do
    {keys, rest} = Enum.split(state.order, state.max_batch)
    entries = Enum.map(keys, &Map.fetch!(state.pending, &1))
    cancellation = Pristine.Cancellation.new()
    config = Config.fetch!(state.run_id)

    telemetry_metadata = %{
      plexus_run_id: state.run_id,
      plexus_contract_fingerprint: state.fingerprint
    }

    opts =
      state.evaluation_options
      |> Keyword.drop([:cancellation, :telemetry_metadata])
      |> Keyword.put(:cancellation, cancellation)
      |> Keyword.put(:telemetry_metadata, telemetry_metadata)
      |> Keyword.put(:max_concurrency, state.max_concurrency)
      |> Keyword.put(:ordered, true)
      |> Keyword.put(:on_error, :collect)

    case start_batch_task(config, entries, state.prepared, opts) do
      {:ok, task} ->
        pending = Map.drop(state.pending, keys)

        Record.append(state.run_id, :measurement_batch_start, %{
          fingerprint: state.fingerprint,
          size: length(entries),
          logical_waiters: Enum.sum(Enum.map(entries, &length(&1.waiters)))
        })

        batch = %{entries: entries, cancellation: cancellation, started_at: System.monotonic_time()}

        state
        |> Map.put(:pending, pending)
        |> Map.put(:order, rest)
        |> Map.update!(:in_flight, &Map.put(&1, task.ref, batch))
        |> ensure_timer()

      {:error, :task_supervisor_saturated} ->
        Pristine.Cancellation.cancel(cancellation)
        %{state | timer: Process.send_after(self(), :flush, max(state.delay_ms, 1))}
    end
  end

  defp start_batch_task(config, entries, prepared, opts) do
    task =
      Task.Supervisor.async_nolink(config.task_supervisor, fn ->
        TypeSafeSDK.evaluate_many(config.client, Enum.map(entries, & &1.state), prepared, opts)
      end)

    {:ok, task}
  catch
    :exit, _reason -> {:error, :task_supervisor_saturated}
  end

  defp complete_batch(run_id, fingerprint, entries, results) do
    padded = pad_results(results, length(entries))

    Enum.zip(entries, padded)
    |> Enum.each(fn {entry, result} ->
      if match?({:ok, _}, result) do
        Cache.put(run_id, entry.memo_key, result)
      else
        refund_measurements(run_id, 1)
      end

      if Config.fetch!(run_id).replay == :record do
        Record.replay_put(run_id, entry.memo_key, result)
      end

      Enum.each(entry.waiters, fn waiter ->
        Run.cast(run_id, waiter.actor_id, {:plexus, :measurement, waiter.tag, result})
      end)

      decrement_measurements(run_id, length(entry.waiters))

      Record.append(run_id, :measurement_response, %{
        fingerprint: fingerprint,
        waiter_count: length(entry.waiters),
        outcome: if(match?({:ok, _}, result), do: :ok, else: :error)
      })
    end)

    Telemetry.emit(run_id, [:measure, :batch, :stop], %{items: length(entries)}, %{fingerprint: fingerprint})
  end

  defp pad_results(results, size) when is_list(results) do
    missing = max(size - length(results), 0)
    Enum.take(results, size) ++ List.duplicate({:error, :cancelled}, missing)
  end

  defp pad_results(_other, size), do: List.duplicate({:error, :invalid_batch_result}, size)

  defp ensure_timer(%{order: []} = state), do: state
  defp ensure_timer(%{timer: timer} = state) when is_reference(timer), do: state

  defp ensure_timer(state) do
    delay = max(state.delay_ms, 0)
    %{state | timer: Process.send_after(self(), :flush, delay)}
  end

  defp cancel_actor_state(state, actor_id) do
    {pending, cancelled_pending, pending_refunds} = remove_actor_from_pending(state.pending, actor_id)

    {in_flight, cancelled_inflight} =
      Enum.map_reduce(state.in_flight, 0, fn {ref, batch}, count ->
        {entries, removed} = remove_actor_from_entries(batch.entries, actor_id)
        batch = %{batch | entries: entries}

        if Enum.all?(entries, &(&1.waiters == [])) do
          Pristine.Cancellation.cancel(batch.cancellation)
        end

        {{ref, batch}, count + removed}
      end)
      |> then(fn {pairs, count} -> {Map.new(pairs), count} end)

    cancelled = cancelled_pending + cancelled_inflight
    decrement_measurements(state.run_id, cancelled)
    refund_measurements(state.run_id, pending_refunds)

    order = Enum.filter(state.order, &Map.has_key?(pending, &1))
    %{state | pending: pending, order: order, in_flight: in_flight}
  end

  defp remove_actor_from_pending(pending, actor_id) do
    Enum.reduce(pending, {%{}, 0, 0}, fn {key, entry}, {acc, count, refunds} ->
      {kept, removed} = Enum.split_with(entry.waiters, &(&1.actor_id != actor_id))
      emptied? = removed != [] and kept == []
      acc = if kept == [], do: acc, else: Map.put(acc, key, %{entry | waiters: kept})
      {acc, count + length(removed), refunds + if(emptied?, do: 1, else: 0)}
    end)
  end

  defp remove_actor_from_entries(entries, actor_id) do
    Enum.map_reduce(entries, 0, fn entry, count ->
      {kept, removed} = Enum.split_with(entry.waiters, &(&1.actor_id != actor_id))
      {%{entry | waiters: kept}, count + length(removed)}
    end)
  end

  defp decrement_measurements(_run_id, 0), do: :ok

  defp decrement_measurements(run_id, count) do
    config = Config.fetch!(run_id)
    current = Quiescence.get(config.quiescence, :measurements)
    Quiescence.add(config.quiescence, :measurements, -min(current, count))
  rescue
    ArgumentError -> :ok
  end

  defp refund_measurements(_run_id, 0), do: :ok

  defp refund_measurements(run_id, count) do
    Budget.refund(Config.fetch!(run_id).budget, :measure, count)
  rescue
    ArgumentError -> :ok
  end

  defp sanitize_reason(:normal), do: :normal
  defp sanitize_reason(:shutdown), do: :shutdown
  defp sanitize_reason({:shutdown, _}), do: :shutdown
  defp sanitize_reason(_), do: :task_exit
end
