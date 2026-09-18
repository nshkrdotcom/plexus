defmodule Plexus.Examples.IncidentCommander.Progress do
  @moduledoc false
  use GenServer

  @typesafe_events [
    [:typesafe_sdk, :evaluate, :start],
    [:typesafe_sdk, :evaluate, :stop],
    [:typesafe_sdk, :evaluate, :exception]
  ]

  @plexus_events [
    [:plexus, :actor, :start],
    [:plexus, :actor, :stop]
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def replay_event(pid), do: GenServer.cast(pid, :replay_event)
  def replay_eof(pid), do: GenServer.cast(pid, :replay_eof)
  def hypothesis_revision(pid), do: GenServer.cast(pid, :hypothesis_revision)
  def challenge(pid), do: GenServer.cast(pid, :challenge)
  def prune(pid, count \\ 1), do: GenServer.cast(pid, {:prune, count})
  def snapshot(pid), do: GenServer.call(pid, :snapshot)
  def stop(pid), do: GenServer.stop(pid, :normal)

  @impl true
  def init(opts) do
    now = System.monotonic_time(:millisecond)
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        @typesafe_events ++ @plexus_events,
        &__MODULE__.handle/4,
        self()
      )

    heartbeat_ms = Keyword.get(opts, :heartbeat_ms, 5_000)
    Process.send_after(self(), :heartbeat, heartbeat_ms)

    state = %{
      handler_id: handler_id,
      run_id: Keyword.get(opts, :run_id),
      max_measurements: Keyword.get(opts, :max_measurements, 0),
      every: max(Keyword.get(opts, :every, 100), 1),
      heartbeat_ms: heartbeat_ms,
      started_at: now,
      first_api_at: nil,
      replay_events: 0,
      replay_eof: false,
      services: 0,
      hypotheses: 0,
      revisions: 0,
      challenges: 0,
      pruned: 0,
      starts: 0,
      stops: 0,
      exceptions: 0,
      http_2xx: 0,
      errors: 0,
      input_tokens: 0,
      output_tokens: 0,
      retries: 0,
      last_status: nil,
      last_model: nil
    }

    IO.puts(
      "[GAIA] chronological replay started; Jev calls begin only when live actors request them"
    )

    {:ok, state}
  end

  @impl true
  def handle_cast(:replay_event, state),
    do: {:noreply, %{state | replay_events: state.replay_events + 1}}

  def handle_cast(:replay_eof, state), do: {:noreply, %{state | replay_eof: true}}

  def handle_cast(:hypothesis_revision, state),
    do: {:noreply, %{state | revisions: state.revisions + 1}}

  def handle_cast(:challenge, state), do: {:noreply, %{state | challenges: state.challenges + 1}}
  def handle_cast({:prune, count}, state), do: {:noreply, %{state | pruned: state.pruned + count}}

  def handle_cast({:telemetry, [:typesafe_sdk, :evaluate, :start], _m, metadata}, state) do
    if relevant?(state, metadata) do
      now = System.monotonic_time(:millisecond)
      first_api_at = state.first_api_at || now
      starts = state.starts + 1
      state = %{state | starts: starts, first_api_at: first_api_at}

      if starts == 1 do
        IO.puts(
          "[GAIA] first Jev evaluation started after #{elapsed(state.started_at, now)}s of live replay"
        )
      end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:telemetry, [:typesafe_sdk, :evaluate, :stop], measurements, metadata}, state) do
    if relevant?(state, metadata) do
      status = metadata[:status]
      success? = is_integer(status) and status >= 200 and status < 300
      input_tokens = integer(measurements[:input_tokens])
      output_tokens = integer(measurements[:output_tokens])
      stops = state.stops + 1

      state = %{
        state
        | stops: stops,
          http_2xx: state.http_2xx + if(success?, do: 1, else: 0),
          errors: state.errors + if(success?, do: 0, else: 1),
          input_tokens: state.input_tokens + input_tokens,
          output_tokens: state.output_tokens + output_tokens,
          retries: state.retries + integer(metadata[:retries]),
          last_status: status,
          last_model: metadata[:model] || state.last_model
      }

      if stops <= 5 or rem(stops, state.every) == 0, do: print_line(state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:telemetry, [:typesafe_sdk, :evaluate, :exception], _m, metadata}, state) do
    if relevant?(state, metadata) do
      state = %{state | exceptions: state.exceptions + 1, errors: state.errors + 1}
      print_line(state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:telemetry, [:plexus, :actor, :start], _m, metadata}, state) do
    if metadata[:run_id] == state.run_id do
      state =
        case metadata[:class] do
          :service -> %{state | services: state.services + 1}
          :hypothesis -> %{state | hypotheses: state.hypotheses + 1}
          _ -> state
        end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:telemetry, [:plexus, :actor, :stop], _m, metadata}, state) do
    if metadata[:run_id] == state.run_id and metadata[:class] == :hypothesis do
      {:noreply, %{state | hypotheses: max(state.hypotheses - 1, 0)}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, state, state}

  @impl true
  def handle_info(:heartbeat, state) do
    print_line(state)
    Process.send_after(self(), :heartbeat, state.heartbeat_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    :telemetry.detach(state.handler_id)
    :ok
  end

  def handle(event, measurements, metadata, pid) do
    if Process.alive?(pid), do: GenServer.cast(pid, {:telemetry, event, measurements, metadata})
    :ok
  end

  defp print_line(state) do
    now = System.monotonic_time(:millisecond)
    tokens = state.input_tokens + state.output_tokens
    done = state.stops + state.exceptions
    in_flight = max(state.starts - done, 0)

    api_rate =
      case state.first_api_at do
        nil -> 0.0
        first -> Float.round(done / max((now - first) / 1_000, 0.001), 2)
      end

    ceiling = if state.max_measurements > 0, do: "/#{state.max_measurements}", else: ""

    IO.puts(
      "[GAIA] events=#{state.replay_events} eof=#{state.replay_eof} " <>
        "services=#{state.services} hypotheses=#{state.hypotheses} revisions=#{state.revisions} " <>
        "challenges=#{state.challenges} pruned=#{state.pruned} | " <>
        "Jev done=#{done}#{ceiling} in_flight=#{in_flight} 2xx=#{state.http_2xx} " <>
        "err=#{state.errors} tokens=#{tokens} retries=#{state.retries} rate=#{api_rate}/s"
    )
  end

  defp relevant?(state, metadata) do
    caller = metadata[:caller] || %{}
    state.run_id == nil or caller[:plexus_run_id] == state.run_id
  end

  defp elapsed(started_at, now), do: Float.round((now - started_at) / 1_000, 1)
  defp integer(value) when is_integer(value), do: value
  defp integer(_), do: 0
end
