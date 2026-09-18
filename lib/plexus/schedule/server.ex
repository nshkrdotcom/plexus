defmodule Plexus.Schedule.Server do
  @moduledoc false
  use GenServer

  alias Plexus.Actor.Interpreter
  alias Plexus.{Record, Run, Schedule}
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
       queued: []
     }}
  end

  @impl true
  def handle_cast({:dispatch, envelope}, %{regime: {:bsp, _}} = state) do
    {:noreply, %{state | queued: [envelope | state.queued]}}
  end

  def handle_cast({:dispatch, envelope}, %{regime: {:priority, fun}} = state) do
    queued = [envelope | state.queued] |> Enum.sort_by(fun)
    send(self(), :drain_one)
    {:noreply, %{state | queued: queued}}
  end

  def handle_cast({:dispatch, envelope}, state) do
    # bounded-async currently preserves the central policy seam while executing
    # effects promptly; measurement/expansion concurrency remains bounded by
    # their dedicated queues.
    Interpreter.execute_now(state.run_id, envelope)
    {:noreply, state}
  end

  @impl true
  def handle_info({:wake_actor, actor_id}, state) do
    Run.cast(state.run_id, actor_id, {:plexus, :wake})
    config = Config.fetch!(state.run_id)
    current = Quiescence.get(config.quiescence, :timers)
    if current > 0, do: Quiescence.add(config.quiescence, :timers, -1)
    {:noreply, state}
  end

  @impl true
  def handle_info(:drain_one, %{queued: []} = state), do: {:noreply, state}

  def handle_info(:drain_one, %{queued: [next | rest]} = state) do
    Interpreter.execute_now(state.run_id, next)
    if rest != [], do: send(self(), :drain_one)
    {:noreply, %{state | queued: rest}}
  end

  @impl true
  def handle_call(:barrier, _from, state) do
    queued = Enum.reverse(state.queued)
    Enum.each(queued, &Interpreter.execute_now(state.run_id, &1))
    round = state.round + 1
    Record.append(state.run_id, :barrier, %{round: round, released: length(queued)})
    {:reply, {:ok, round, length(queued)}, %{state | round: round, queued: []}}
  end

  def handle_call({:set_regime, regime}, _from, %{regime: {:bsp, _}, queued: queued} = state) do
    if queued != [] and not match?({:bsp, _}, regime) do
      released = Enum.reverse(queued)
      Enum.each(released, &Interpreter.execute_now(state.run_id, &1))
      round = state.round + 1

      Record.append(state.run_id, :barrier, %{
        round: round,
        released: length(released),
        reason: :regime_change
      })

      {:reply, :ok, %{state | regime: regime, round: round, queued: []}}
    else
      {:reply, :ok, %{state | regime: regime}}
    end
  end

  def handle_call({:set_regime, regime}, _from, state) do
    {:reply, :ok, %{state | regime: regime}}
  end
end
