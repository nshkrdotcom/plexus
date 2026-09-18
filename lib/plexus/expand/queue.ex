defmodule Plexus.Expand.Queue do
  @moduledoc "Bounded, priority-ordered queue for expensive generative expansion."

  use GenServer

  alias Plexus.{Budget, Record, Run, Telemetry}
  alias Plexus.Run.{Config, Names}

  def start_link(opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    GenServer.start_link(__MODULE__, opts, name: Names.expand_queue(run_id))
  end

  @spec submit(term(), map(), term(), term(), keyword()) :: :ok
  def submit(run_id, context, tag, spec, opts \\ []) do
    GenServer.cast(Names.expand_queue(run_id), {:submit, context, tag, spec, opts})
  end

  @spec cancel_actor(term(), term()) :: :ok
  def cancel_actor(run_id, actor_id) do
    case GenServer.whereis(Names.expand_queue(run_id)) do
      nil -> :ok
      pid -> GenServer.call(pid, {:cancel_actor, actor_id}, :infinity)
    end
  end

  @impl true
  def init(opts) do
    adapter = Keyword.get(opts, :adapter)
    client = Keyword.get(opts, :client)

    capabilities =
      if adapter && Code.ensure_loaded?(adapter) && function_exported?(adapter, :capabilities, 1) do
        safe_capabilities(adapter, client)
      else
        %{}
      end

    required = Keyword.get(opts, :required_capabilities, [])

    cond do
      required != [] and is_nil(adapter) ->
        {:stop, {:expand_preflight_failed, :adapter_not_configured}}

      not capabilities_supported?(capabilities, required) ->
        {:stop, {:expand_preflight_failed, {:unsupported_capabilities, required}}}

      true ->
        {:ok,
         %{
           run_id: Keyword.fetch!(opts, :run_id),
           adapter: adapter,
           client: client,
           capabilities: capabilities,
           max_concurrency: max(Keyword.get(opts, :max_concurrency, 4), 1),
           sequence: 0,
           pending: [],
           active: %{}
         }}
    end
  end

  @impl true
  def handle_cast({:submit, context, tag, spec, opts}, state) do
    cond do
      is_nil(state.adapter) ->
        refund(state.run_id)
        decrement_expansions(state.run_id, 1)
        deliver(state.run_id, context.actor_id, tag, {:error, :expand_adapter_not_configured})
        {:noreply, state}

      not capabilities_supported?(
        state.capabilities,
        Keyword.get(opts, :required_capabilities, [])
      ) ->
        refund(state.run_id)
        decrement_expansions(state.run_id, 1)
        deliver(state.run_id, context.actor_id, tag, {:error, :unsupported_expansion_capability})
        {:noreply, state}

      true ->
        sequence = state.sequence + 1

        item = %{
          sequence: sequence,
          priority: Keyword.get(opts, :priority, 0),
          actor_id: context.actor_id,
          tag: tag,
          spec: spec,
          opts: Keyword.drop(opts, [:priority, :required_capabilities])
        }

        pending = [item | state.pending] |> Enum.sort_by(&{-&1.priority, &1.sequence})
        {:noreply, dispatch(%{state | sequence: sequence, pending: pending})}
    end
  end

  def handle_cast({:cancel_actor, actor_id}, state) do
    {:noreply, cancel_actor_state(state, actor_id)}
  end

  @impl true
  def handle_call({:cancel_actor, actor_id}, _from, state) do
    {:reply, :ok, cancel_actor_state(state, actor_id)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.active, ref) do
      {nil, _} ->
        {:noreply, state}

      {entry, active} ->
        Process.demonitor(ref, [:flush])
        finish(state.run_id, entry.item, result)
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _} ->
        {:noreply, state}

      {entry, active} ->
        finish(state.run_id, entry.item, {:error, :expand_task_exit})
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  defp cancel_actor_state(state, actor_id) do
    {cancelled_pending, pending} = Enum.split_with(state.pending, &(&1.actor_id == actor_id))

    Enum.each(cancelled_pending, fn item ->
      refund(state.run_id)
      decrement_expansions(state.run_id, 1)
      deliver(state.run_id, item.actor_id, item.tag, {:error, :cancelled})
    end)

    {active, cancelled_active} =
      Enum.reduce(state.active, {%{}, []}, fn {ref, entry}, {kept, cancelled} ->
        if entry.item.actor_id == actor_id do
          Task.shutdown(entry.task, :brutal_kill)
          {kept, [entry | cancelled]}
        else
          {Map.put(kept, ref, entry), cancelled}
        end
      end)

    Enum.each(cancelled_active, fn entry ->
      refund(state.run_id)
      decrement_expansions(state.run_id, 1)
      deliver(state.run_id, entry.item.actor_id, entry.item.tag, {:error, :cancelled})
    end)

    dispatch(%{state | pending: pending, active: active})
  end

  defp dispatch(state) when map_size(state.active) >= state.max_concurrency, do: state
  defp dispatch(%{pending: []} = state), do: state

  defp dispatch(%{pending: [item | rest]} = state) do
    config = Config.fetch!(state.run_id)
    adapter = state.adapter
    client = state.client

    task =
      Task.Supervisor.async_nolink(config.expand_task_supervisor, fn ->
        adapter.expand(client, item.spec, item.opts)
      end)

    Record.append(state.run_id, :expansion_start, %{
      actor_id: item.actor_id,
      priority: item.priority
    })

    Telemetry.emit(state.run_id, [:expand, :start], %{}, %{actor_id: item.actor_id})

    entry = %{item: item, task: task, started_at: System.monotonic_time()}
    state = %{state | pending: rest, active: Map.put(state.active, task.ref, entry)}
    dispatch(state)
  end

  defp finish(run_id, item, result) do
    result = normalize_result(result)
    if match?({:error, _}, result), do: refund(run_id)
    decrement_expansions(run_id, 1)

    Record.append(run_id, :expansion_stop, %{
      actor_id: item.actor_id,
      outcome: if(match?({:ok, _}, result), do: :ok, else: :error)
    })

    Telemetry.emit(run_id, [:expand, :stop], %{}, %{actor_id: item.actor_id})
    deliver(run_id, item.actor_id, item.tag, result)
  end

  defp normalize_result({:ok, _} = result), do: result
  defp normalize_result({:error, _} = result), do: result
  defp normalize_result(_other), do: {:error, :invalid_expand_adapter_result}

  defp deliver(run_id, actor_id, tag, result),
    do: Run.cast(run_id, actor_id, {:plexus, :expansion, tag, result})

  defp decrement_expansions(run_id, count) do
    config = Config.fetch!(run_id)
    current = Plexus.Schedule.Quiescence.get(config.quiescence, :expansions)
    Plexus.Schedule.Quiescence.add(config.quiescence, :expansions, -min(current, count))
  rescue
    ArgumentError -> :ok
  end

  defp refund(run_id) do
    Budget.refund(Config.fetch!(run_id).budget, :expand, 1)
  rescue
    ArgumentError -> :ok
  end

  defp safe_capabilities(adapter, client) do
    case adapter.capabilities(client) do
      caps when is_map(caps) -> caps
      _ -> %{}
    end
  rescue
    _ -> %{}
  end

  defp capabilities_supported?(_caps, []), do: true

  defp capabilities_supported?(caps, required) do
    Enum.all?(required, fn capability -> Map.get(caps, capability, :unknown) == :supported end)
  end
end
