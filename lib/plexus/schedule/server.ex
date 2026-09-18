defmodule Plexus.Schedule.Server do
  @moduledoc false
  use GenServer

  alias Plexus.Actor.{Activity, Interpreter}
  alias Plexus.{Graph, Record, Run, Schedule}
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

  def start_link(opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    GenServer.start_link(__MODULE__, opts, name: Names.schedule_server(run_id))
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       run_id: Keyword.fetch!(opts, :run_id),
       regime: Schedule.normalize(Keyword.get(opts, :regime, :async)),
       round: 0,
       progress: %{},
       queued: []
     }}
  end

  @impl true
  def handle_cast({:dispatch, envelope}, %{regime: {:bsp, _}} = state) do
    {:noreply, %{state | queued: [envelope | state.queued]}}
  end

  def handle_cast({:dispatch, envelope}, %{regime: {:priority, fun}} = state) do
    queued = (state.queued ++ [envelope]) |> Enum.sort_by(fun)
    send(self(), :drain_one)
    {:noreply, %{state | queued: queued}}
  end

  def handle_cast({:dispatch, envelope}, %{regime: {:bounded_async, _}} = state) do
    {:noreply, drain_bounded(%{state | queued: state.queued ++ [envelope]})}
  end

  def handle_cast({:dispatch, envelope}, state) do
    Interpreter.execute_now(state.run_id, envelope)
    {:noreply, state}
  end

  @impl true
  def handle_info({:wake_actor, token}, state) do
    config = Config.fetch!(state.run_id)

    case :ets.take(config.tables.timers, token) do
      [{^token, actor_id, _ref}] ->
        Run.cast(state.run_id, actor_id, {:plexus, :wake})
        Quiescence.add(config.quiescence, :timers, -1)

      [] ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:drain_one, %{queued: []} = state), do: {:noreply, state}

  def handle_info(:drain_one, %{regime: {:priority, _}, queued: [next | rest]} = state) do
    Interpreter.execute_now(state.run_id, next)
    if rest != [], do: send(self(), :drain_one)
    {:noreply, %{state | queued: rest}}
  end

  def handle_info(:drain_one, state), do: {:noreply, state}

  @impl true
  def handle_call(:barrier, _from, %{regime: {:bounded_async, _}} = state) do
    before = length(state.queued)
    state = drain_bounded(state)
    released = before - length(state.queued)
    round = state.round + 1
    Record.append(state.run_id, :barrier, %{round: round, released: released})
    {:reply, {:ok, round, released}, %{state | round: round}}
  end

  def handle_call(:barrier, _from, state) do
    queued =
      if match?({:bsp, _}, state.regime), do: Enum.reverse(state.queued), else: state.queued

    Enum.each(queued, &Interpreter.execute_now(state.run_id, &1))
    round = state.round + 1
    Record.append(state.run_id, :barrier, %{round: round, released: length(queued)})
    {:reply, {:ok, round, length(queued)}, %{state | round: round, queued: []}}
  end

  def handle_call({:set_regime, regime}, _from, state) do
    if state.regime == regime do
      {:reply, :ok, state}
    else
      released =
        if match?({:bsp, _}, state.regime), do: Enum.reverse(state.queued), else: state.queued

      Enum.each(released, &Interpreter.execute_now(state.run_id, &1))
      round = state.round + if(released == [], do: 0, else: 1)

      Record.append(state.run_id, :schedule_transition, %{
        round: round,
        released: length(released),
        regime: inspect(regime)
      })

      Config.update(state.run_id, &Map.put(&1, :schedule, regime))
      {:reply, :ok, %{state | regime: regime, round: round, queued: [], progress: %{}}}
    end
  end

  defp drain_bounded(state) do
    queued = Enum.filter(state.queued, fn envelope -> Activity.active?(envelope.activity) end)
    state = %{state | queued: queued}

    case Enum.find_index(state.queued, &eligible?(state, &1)) do
      nil ->
        state

      index ->
        {envelope, queued} = List.pop_at(state.queued, index)
        actor = envelope.context.actor_id
        Interpreter.execute_now(state.run_id, envelope)

        Record.append(state.run_id, :scheduled_update, %{
          actor_id: actor,
          update: Map.get(state.progress, actor, 0) + 1
        })

        drain_bounded(%{
          state
          | queued: queued,
            progress: Map.update(state.progress, actor, 1, &(&1 + 1))
        })
    end
  end

  defp eligible?(%{regime: {:bounded_async, k}} = state, envelope) do
    active =
      Graph.nodes(state.run_id) |> Enum.filter(fn {_id, attrs} -> attrs[:status] == :active end)

    floor =
      active
      |> Enum.map(fn {id, _} -> Map.get(state.progress, id, 0) end)
      |> Enum.min(fn -> 0 end)

    Map.get(state.progress, envelope.context.actor_id, 0) < floor + k
  end
end
