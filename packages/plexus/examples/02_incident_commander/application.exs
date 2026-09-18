defmodule Plexus.Examples.IncidentCommander.Service do
  use Plexus.Actor

  alias Plexus.Actor
  alias Plexus.Examples.IncidentCommander
  alias Plexus.Examples.IncidentCommander.Topology
  alias Plexus.{Event, Graph, Provenance, Record}

  @error_pattern ~r/(error|warning|fail|timeout|exception|refused|reset|unavailable)/i

  @impl true
  def init(args) do
    context = Actor.context(args)

    state = %{
      context: context,
      service: args.service,
      topology: args.topology,
      progress: args[:progress],
      signal_every: max(args[:signal_every] || 1, 1),
      trigger_probability: args[:trigger_probability] || 0.6,
      hypothesis_update_every: max(args[:hypothesis_update_every] || 1, 1),
      branch_width: max(args[:branch_width] || 2, 1),
      branch_credits: max(args[:branch_credits] || 8, 0),
      peer_challenges: max(args[:peer_challenges] || 2, 0),
      max_hypotheses: max(args[:max_hypotheses] || 20_000, 1),
      incident_window_us: max(args[:incident_window_us] || 30_000_000, 1),
      recent_limit: max(args[:recent_limit] || 24, 1),
      event_count: 0,
      trace_failures: 0,
      log_signals: 0,
      signal_count: 0,
      recent: [],
      pending_signals: %{},
      last_event_time_us: nil,
      semantic_signature: nil
    }

    Graph.update(context.run_id, context.actor_id, fn attrs ->
      attrs
      |> Map.put(:service, state.service)
      |> Map.put(:event_count, 0)
      |> Map.put(:trace_failures, 0)
      |> Map.put(:log_signals, 0)
      |> Map.put(:last_event_time_us, nil)
    end)

    {:ok, state}
  end

  @impl true
  def handle_cast({:telemetry, event}, state) do
    {state, signal?} = ingest(state, event)

    topology_changes =
      Topology.observe_trace(state.context.run_id, state.topology, state.service, event)

    if topology_changes != [] do
      Enum.each(topology_changes, fn {caller, callee} ->
        _ = Provenance.invalidate(state.context.run_id, {:service, caller}, notify: true)
        _ = Provenance.invalidate(state.context.run_id, {:service, callee}, notify: true)
      end)

      Record.append(state.context.run_id, :gaia_topology_changed, %{
        service: state.service,
        edges: topology_changes,
        event_id: event.event_id
      })
    end

    if signal? and IncidentCommander.measurement_credit_available?(state.context.run_id) do
      Event.publish(
        state.context.run_id,
        IncidentCommander.service_event(state.service),
        snapshot(state)
      )

      state =
        if rem(state.signal_count - 1, state.signal_every) == 0 do
          Actor.dispatch(
            state.context,
            {:measure, {:signal, event.event_id}, signal_state(state, event), :gaia_signal, []}
          )

          %{state | pending_signals: Map.put(state.pending_signals, event.event_id, event)}
        else
          state
        end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:request_snapshot, requester}, state) do
    Actor.dispatch(
      state.context,
      {:send, requester, {:service_snapshot, state.context.actor_id, snapshot(state)}}
    )

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, {:signal, event_id}, {:ok, response}}, state) do
    {event, pending_signals} = Map.pop(state.pending_signals, event_id)
    state = %{state | pending_signals: pending_signals}
    relevance = Plexus.Belief.from(response, :anomaly_relevance)
    mode = Plexus.Belief.from(response, :failure_mode)
    strength = Plexus.Belief.from(response, :diagnostic_strength)
    failure_mode = to_string(mode.value)

    state =
      maybe_invalidate_semantic_state(
        state,
        event,
        relevance,
        failure_mode,
        strength.value
      )

    if not is_nil(event) and Plexus.Belief.probability(relevance) >= state.trigger_probability do
      incident = IncidentCommander.incident_key(event.event_time_us, state.incident_window_us)
      hypothesis_id = IncidentCommander.hypothesis_id(incident, state.service, failure_mode)

      opts = [
        service: state.service,
        incident: incident,
        failure_mode: failure_mode,
        topology: state.topology,
        progress: state.progress,
        hypothesis_update_every: state.hypothesis_update_every,
        branch_width: state.branch_width,
        branch_credits: state.branch_credits,
        peer_challenges: state.peer_challenges,
        max_hypotheses: state.max_hypotheses,
        parent_account: :root,
        evidence_strength: strength.value
      ]

      case IncidentCommander.prepare_hypothesis(state.context.run_id, hypothesis_id, opts) do
        {:existing, _actor_id} ->
          Actor.dispatch(state.context, {:send, hypothesis_id, {:evidence, event}})

        {:new, init_arg} ->
          Actor.dispatch(state.context, [
            {:spawn, :hypothesis, Plexus.Examples.IncidentCommander.Hypothesis, init_arg,
             [
               actor_id: hypothesis_id,
               activity_mode: :resident,
               metadata: %{incident: incident, service: state.service, failure_mode: failure_mode}
             ]},
            {:edge, :investigates, state.context.actor_id, hypothesis_id, 1.0,
             %{event_id: event_id, diagnostic_strength: strength.value}},
            {:send, hypothesis_id, {:evidence, event}}
          ])

        {:error, reason} ->
          Record.append(state.context.run_id, :gaia_hypothesis_admission_denied, %{
            service: state.service,
            incident: incident,
            failure_mode: failure_mode,
            reason: inspect(reason)
          })
      end
    end

    {:noreply, state}
  end

  def handle_cast(
        {:plexus, :measurement, {:signal, event_id}, {:error, {:budget_exhausted, :measure}}},
        state
      ) do
    {:noreply, %{state | pending_signals: Map.delete(state.pending_signals, event_id)}}
  end

  def handle_cast({:plexus, :measurement, {:signal, event_id}, {:error, error}}, state) do
    Record.append(state.context.run_id, :gaia_signal_error, %{
      service: state.service,
      event_id: event_id,
      error: inspect(error)
    })

    {:noreply, %{state | pending_signals: Map.delete(state.pending_signals, event_id)}}
  end

  def handle_cast({:plexus, :command_error, {:spawn, actor_id}, reason}, state) do
    IncidentCommander.cleanup_prepared_hypothesis(state.context.run_id, state.topology, actor_id)

    Record.append(state.context.run_id, :gaia_hypothesis_spawn_error, %{
      actor_id: actor_id,
      reason: inspect(reason)
    })

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  def snapshot(state) do
    run_id = state.context.run_id

    %{
      "service" => state.service,
      "event_count" => state.event_count,
      "trace_failures" => state.trace_failures,
      "log_signals" => state.log_signals,
      "last_event_time_us" => state.last_event_time_us,
      "callers" => Topology.neighbors(run_id, state.service, :callers),
      "callees" => Topology.neighbors(run_id, state.service, :callees),
      "recent_events" => Enum.reverse(Enum.map(state.recent, &compact_event/1))
    }
  end

  defp ingest(state, event) do
    trace_failure = event.source == :trace and trace_failure?(event.raw["status_code"])

    log_signal =
      event.source == :business and Regex.match?(@error_pattern, event.raw["message"] || "")

    signal? = trace_failure or log_signal

    recent = [event | state.recent] |> Enum.take(state.recent_limit)

    state = %{
      state
      | event_count: state.event_count + 1,
        trace_failures: state.trace_failures + if(trace_failure, do: 1, else: 0),
        log_signals: state.log_signals + if(log_signal, do: 1, else: 0),
        signal_count: state.signal_count + if(signal?, do: 1, else: 0),
        recent: recent,
        last_event_time_us: event.event_time_us
    }

    Graph.update(state.context.run_id, state.context.actor_id, fn attrs ->
      attrs
      |> Map.put(:event_count, state.event_count)
      |> Map.put(:trace_failures, state.trace_failures)
      |> Map.put(:log_signals, state.log_signals)
      |> Map.put(:last_event_time_us, state.last_event_time_us)
    end)

    {state, signal?}
  end

  defp signal_state(state, event) do
    %{
      "service" => state.service,
      "source" => Atom.to_string(event.source),
      "event_time_us" => event.event_time_us,
      "raw_event" => compact_event(event),
      "local_window" => snapshot(state)
    }
  end

  defp compact_event(event) do
    raw = event.raw

    %{
      "event_id" => event.event_id,
      "source" => Atom.to_string(event.source),
      "event_time_us" => event.event_time_us,
      "timestamp" => Map.get(event, :timestamp) || raw["timestamp"] || raw["datetime"],
      "message" => raw["message"],
      "status_code" => raw["status_code"],
      "url" => raw["url"],
      "trace_id" => raw["trace_id"],
      "span_id" => raw["span_id"]
    }
  end

  defp maybe_invalidate_semantic_state(state, nil, _relevance, _failure_mode, _strength),
    do: state

  defp maybe_invalidate_semantic_state(state, event, relevance, failure_mode, strength) do
    if Plexus.Belief.probability(relevance) >= state.trigger_probability do
      signature = {failure_mode, strength}

      case state.semantic_signature do
        nil ->
          %{state | semantic_signature: signature}

        ^signature ->
          state

        previous ->
          invalidated =
            Provenance.invalidate(
              state.context.run_id,
              {:service, state.service},
              notify: true
            )

          Record.append(state.context.run_id, :gaia_service_semantic_state_changed, %{
            service: state.service,
            event_id: event.event_id,
            previous: inspect(previous),
            current: inspect(signature),
            dependent_nodes: max(length(invalidated) - 1, 0)
          })

          %{state | semantic_signature: signature}
      end
    else
      state
    end
  end

  defp trace_failure?(nil), do: false
  defp trace_failure?(""), do: false
  defp trace_failure?(status) when status in [200, "200"], do: false

  defp trace_failure?(status) do
    case Integer.parse(to_string(status)) do
      {code, _} -> code < 200 or code >= 300
      :error -> true
    end
  end
end

defmodule Plexus.Examples.IncidentCommander.Hypothesis do
  use Plexus.Actor

  alias Plexus.Actor
  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander
  alias Plexus.Examples.IncidentCommander.{Progress, Topology}
  alias Plexus.{Graph, Population, Provenance, Record}

  @impl true
  def init(args) do
    context = Actor.context(args)
    account = Map.get(args, :account, account_id(context.actor_id))

    state = %{
      context: context,
      service: args.service,
      incident: args.incident,
      failure_mode: args.failure_mode,
      topology: args.topology,
      progress: args[:progress],
      account: account,
      branch_width: max(args[:branch_width] || 2, 1),
      branch_credits: max(args[:branch_credits] || 0, 0),
      peer_challenges: max(args[:peer_challenges] || 2, 0),
      max_hypotheses: max(args[:max_hypotheses] || 20_000, 1),
      update_every: max(args[:hypothesis_update_every] || 1, 1),
      confirm_probability: args[:confirm_probability] || 0.9,
      refute_probability: args[:refute_probability] || 0.15,
      phase: :observing,
      revisions: 0,
      evidence_count: 0,
      recent_evidence: [],
      root_probability: 0.5,
      next_action: "observe",
      evidence_strength: args[:evidence_strength] || 0,
      pending_update: false,
      pending_update_epoch: nil,
      pending_repair_epoch: nil,
      seen_challenges: MapSet.new()
    }

    Graph.update(context.run_id, context.actor_id, fn attrs ->
      attrs
      |> Map.put(:service, state.service)
      |> Map.put(:incident, state.incident)
      |> Map.put(:failure_mode, state.failure_mode)
      |> Map.put(:phase, state.phase)
      |> Map.put(:revisions, 0)
      |> Map.put(:root_probability, state.root_probability)
      |> Map.put(:repair_priority, 100)
    end)

    Provenance.depend(context.run_id, context.actor_id, {:service, state.service}, %{
      incident: state.incident,
      failure_mode: state.failure_mode
    })

    Actor.dispatch(context, {:wake_on, IncidentCommander.service_event(state.service)})
    {:ok, state}
  end

  def account_id(actor_id), do: {:gaia_hypothesis_account, actor_id}

  @impl true
  def handle_cast({:evidence, event}, state) do
    state = add_evidence(state, event)
    {:noreply, maybe_request_update(state)}
  end

  def handle_cast({:investigate_from, parent_id, packet}, state) do
    state = add_evidence(state, Map.put(packet, "from_hypothesis", inspect(parent_id)))
    {:noreply, request_snapshot(state)}
  end

  def handle_cast({:plexus, :event, event, _payload}, state) do
    service_event = IncidentCommander.service_event(state.service)

    if event == service_event do
      Actor.dispatch(state.context, {:wake_on, service_event})
      {:noreply, request_snapshot(%{state | phase: :investigating})}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:plexus, :invalidated, upstream_id, epoch}, state) do
    Record.append(state.context.run_id, :gaia_hypothesis_invalidated, %{
      actor_id: state.context.actor_id,
      upstream_id: inspect(upstream_id),
      epoch: epoch
    })

    state = %{state | phase: :stale, pending_repair_epoch: epoch}
    update_graph_state(state)
    {:noreply, request_snapshot(state)}
  end

  def handle_cast({:service_snapshot, _service_id, snapshot}, state) do
    if state.pending_update do
      {:noreply, state}
    else
      revision = state.revisions + 1
      attrs = Graph.get(state.context.run_id, state.context.actor_id) || %{}
      evidence_epoch = attrs[:epoch] || 0

      semantic_state = %{
        "hypothesis_service" => state.service,
        "incident" => state.incident,
        "failure_mode" => state.failure_mode,
        "revision" => revision,
        "evidence_epoch" => evidence_epoch,
        "current_root_probability" => state.root_probability,
        "service_snapshot" => snapshot,
        "recent_evidence" => Enum.reverse(state.recent_evidence)
      }

      Actor.dispatch(
        state.context,
        {:measure, {:hypothesis_update, revision, evidence_epoch}, semantic_state,
         :gaia_hypothesis_update, []}
      )

      {:noreply,
       %{
         state
         | pending_update: true,
           pending_update_epoch: evidence_epoch,
           phase: :investigating
       }}
    end
  end

  def handle_cast(
        {:plexus, :measurement, {:hypothesis_update, revision, evidence_epoch}, {:ok, response}},
        state
      ) do
    attrs = Graph.get(state.context.run_id, state.context.actor_id) || %{}
    current_epoch = attrs[:epoch] || 0

    if current_epoch != evidence_epoch do
      Record.append(state.context.run_id, :gaia_hypothesis_superseded_result, %{
        actor_id: state.context.actor_id,
        revision: revision,
        measurement_epoch: evidence_epoch,
        current_epoch: current_epoch
      })

      state = %{
        state
        | pending_update: false,
          pending_update_epoch: nil,
          phase: :stale
      }

      {:noreply, request_snapshot(state)}
    else
      root = Plexus.Belief.from(response, :root_cause)
      action = Plexus.Belief.from(response, :next_action)
      strength = Plexus.Belief.from(response, :evidence_strength)
      probability = Plexus.Belief.probability(root)
      next_action = to_string(action.value)

      state = %{
        state
        | revisions: max(revision, state.revisions + 1),
          root_probability: probability,
          next_action: next_action,
          evidence_strength: strength.value,
          pending_update: false,
          pending_update_epoch: nil,
          phase: phase_for(probability, next_action, state)
      }

      update_graph_state(state)
      if is_pid(state.progress), do: Progress.hypothesis_revision(state.progress)

      Record.append(state.context.run_id, :gaia_hypothesis_revision, %{
        actor_id: state.context.actor_id,
        revision: state.revisions,
        evidence_epoch: evidence_epoch,
        root_probability: state.root_probability,
        next_action: state.next_action,
        evidence_strength: state.evidence_strength
      })

      case repair_if_needed(state, evidence_epoch) do
        {:ok, state} -> {:noreply, apply_next_action(state)}
        {:retry, state} -> {:noreply, state}
      end
    end
  end

  def handle_cast(
        {:plexus, :measurement, {:hypothesis_update, _revision, _evidence_epoch},
         {:error, {:budget_exhausted, :measure}}},
        state
      ) do
    {:noreply, %{state | pending_update: false, pending_update_epoch: nil, phase: :observing}}
  end

  def handle_cast(
        {:plexus, :measurement, {:hypothesis_update, revision, evidence_epoch}, {:error, error}},
        state
      ) do
    Record.append(state.context.run_id, :gaia_hypothesis_measurement_error, %{
      actor_id: state.context.actor_id,
      revision: revision,
      evidence_epoch: evidence_epoch,
      error: inspect(error)
    })

    {:noreply, %{state | pending_update: false, pending_update_epoch: nil, phase: :observing}}
  end

  def handle_cast({:challenge, challenger_id, packet}, state) do
    challenge_id =
      packet["challenge_id"] || stable_challenge_id(challenger_id, state.context.actor_id, packet)

    if MapSet.member?(state.seen_challenges, challenge_id) do
      {:noreply, state}
    else
      if is_pid(state.progress), do: Progress.challenge(state.progress)

      Actor.dispatch(state.context, [
        {:edge, :contradicts, challenger_id, state.context.actor_id, 1.0,
         %{challenge_id: challenge_id}},
        {:measure, {:challenge, challenge_id, challenger_id}, challenge_state(state, packet),
         :gaia_challenge, []}
      ])

      {:noreply,
       %{
         state
         | seen_challenges: MapSet.put(state.seen_challenges, challenge_id),
           phase: :challenging
       }}
    end
  end

  def handle_cast(
        {:plexus, :measurement, {:challenge, challenge_id, challenger_id}, {:ok, response}},
        state
      ) do
    survives = Plexus.Belief.from(response, :survives_challenge)
    disposition = Plexus.Belief.from(response, :disposition)
    probability = Plexus.Belief.probability(survives)

    Record.append(state.context.run_id, :gaia_challenge_result, %{
      actor_id: state.context.actor_id,
      challenger_id: challenger_id,
      challenge_id: challenge_id,
      survives: probability,
      disposition: to_string(disposition.value)
    })

    if probability < 0.35 or to_string(disposition.value) == "concede" do
      tombstone(state, :challenge_conceded)
      if is_pid(state.progress), do: Progress.prune(state.progress)
      Actor.dispatch(state.context, {:prune, state.context.actor_id})
      {:noreply, %{state | phase: :refuted}}
    else
      {:noreply, %{state | phase: :observing}}
    end
  end

  def handle_cast(
        {:plexus, :measurement, {:challenge, _challenge_id, _challenger_id},
         {:error, {:budget_exhausted, :measure}}},
        state
      ) do
    {:noreply, %{state | phase: :observing}}
  end

  def handle_cast(
        {:plexus, :measurement, {:challenge, challenge_id, challenger_id}, {:error, error}},
        state
      ) do
    Record.append(state.context.run_id, :gaia_challenge_error, %{
      actor_id: state.context.actor_id,
      challenger_id: challenger_id,
      challenge_id: challenge_id,
      error: inspect(error)
    })

    {:noreply, %{state | phase: :observing}}
  end

  def handle_cast({:plexus, :command_error, {:spawn, child_id}, reason}, state) do
    IncidentCommander.cleanup_prepared_hypothesis(state.context.run_id, state.topology, child_id)

    Record.append(state.context.run_id, :gaia_hypothesis_spawn_error, %{
      actor_id: state.context.actor_id,
      child_id: child_id,
      reason: inspect(reason)
    })

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = Accounts.close(state.context.run_id, state.account)
    Topology.release_hypothesis(state.topology)
    :ok
  rescue
    _ -> :ok
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp add_evidence(state, event) do
    evidence = compact_evidence(event)

    %{
      state
      | evidence_count: state.evidence_count + 1,
        recent_evidence: [evidence | state.recent_evidence] |> Enum.take(12)
    }
  end

  defp maybe_request_update(state) do
    if rem(state.evidence_count, state.update_every) == 0,
      do: request_snapshot(state),
      else: state
  end

  defp request_snapshot(%{pending_update: true} = state), do: state

  defp request_snapshot(state) do
    if IncidentCommander.measurement_credit_available?(state.context.run_id) do
      Actor.dispatch(
        state.context,
        {:send, {:service, state.service}, {:request_snapshot, state.context.actor_id}}
      )

      %{state | phase: :investigating}
    else
      %{state | phase: :observing}
    end
  end

  defp repair_if_needed(state, evidence_epoch) do
    case Graph.get(state.context.run_id, state.context.actor_id) do
      %{stale: true, epoch: ^evidence_epoch} ->
        case Provenance.repair(state.context.run_id, state.context.actor_id, evidence_epoch) do
          :ok ->
            {:ok, %{state | pending_repair_epoch: nil}}

          {:error, :stale_epoch} ->
            {:retry,
             request_snapshot(%{
               state
               | pending_update: false,
                 pending_update_epoch: nil,
                 phase: :stale
             })}

          _ ->
            {:ok, state}
        end

      %{stale: true} ->
        {:retry,
         request_snapshot(%{
           state
           | pending_update: false,
             pending_update_epoch: nil,
             phase: :stale
         })}

      _ ->
        {:ok, %{state | pending_repair_epoch: nil}}
    end
  end

  defp apply_next_action(state) do
    cond do
      state.root_probability <= state.refute_probability or state.next_action == "refute" ->
        tombstone(state, :semantic_refutation)
        if is_pid(state.progress), do: Progress.prune(state.progress)
        Actor.dispatch(state.context, {:prune, state.context.actor_id})
        %{state | phase: :refuted}

      state.next_action in ["investigate_callers", "investigate_callees", "investigate_both"] ->
        state |> spawn_children() |> challenge_peers()

      state.next_action == "challenge_peers" ->
        challenge_peers(state)

      state.root_probability >= state.confirm_probability or state.next_action == "confirm" ->
        challenge_peers(%{state | phase: :confirmed})

      true ->
        %{state | phase: :observing}
    end
  end

  defp spawn_children(state) do
    direction =
      case state.next_action do
        "investigate_callers" -> :callers
        "investigate_callees" -> :callees
        "investigate_both" -> :both
      end

    targets =
      Topology.neighbors(state.context.run_id, state.service, direction)
      |> Enum.take(state.branch_width)

    Enum.each(targets, fn target -> spawn_child(state, target) end)
    state
  end

  defp spawn_child(state, target) do
    child_id =
      IncidentCommander.child_hypothesis_id(
        state.context.actor_id,
        state.incident,
        target,
        state.failure_mode
      )

    if child_id == state.context.actor_id do
      :ok
    else
      child_grant = child_grant(state)

      opts = [
        service: target,
        incident: state.incident,
        failure_mode: state.failure_mode,
        topology: state.topology,
        progress: state.progress,
        hypothesis_update_every: state.update_every,
        branch_width: state.branch_width,
        branch_credits: child_grant,
        peer_challenges: state.peer_challenges,
        max_hypotheses: state.max_hypotheses,
        parent_account: state.account
      ]

      case IncidentCommander.prepare_hypothesis(state.context.run_id, child_id, opts) do
        {:existing, _} ->
          Actor.dispatch(state.context, [
            {:edge, :investigates, state.context.actor_id, child_id, 1.0,
             %{reason: state.next_action, revision: state.revisions}},
            {:send, child_id, {:investigate_from, state.context.actor_id, child_packet(state)}}
          ])

        {:new, init_arg} ->
          Actor.dispatch(state.context, [
            {:spawn, :hypothesis, __MODULE__, init_arg,
             [
               actor_id: child_id,
               activity_mode: :resident,
               metadata: %{
                 incident: state.incident,
                 service: target,
                 failure_mode: state.failure_mode
               }
             ]},
            {:edge, :investigates, state.context.actor_id, child_id, 1.0,
             %{reason: state.next_action, revision: state.revisions}},
            {:send, child_id, {:investigate_from, state.context.actor_id, child_packet(state)}}
          ])

        {:error, reason} ->
          Record.append(state.context.run_id, :gaia_branch_credit_denied, %{
            actor_id: state.context.actor_id,
            child_id: child_id,
            target_service: target,
            reason: inspect(reason),
            revision: state.revisions
          })
      end
    end
  end

  defp challenge_peers(%{peer_challenges: 0} = state), do: state

  defp challenge_peers(state) do
    peers =
      Population.lookup(state.context.run_id, :incident, state.incident)
      |> Enum.filter(fn {id, attrs} ->
        id != state.context.actor_id and attrs[:class] == :hypothesis and
          attrs[:phase] != :refuted
      end)
      |> Enum.take(state.peer_challenges)

    Enum.each(peers, fn {peer_id, _attrs} ->
      packet = %{
        "challenge_id" =>
          stable_challenge_id(state.context.actor_id, peer_id, %{
            "revision" => state.revisions,
            "incident" => state.incident
          }),
        "service" => state.service,
        "failure_mode" => state.failure_mode,
        "root_probability" => state.root_probability,
        "revision" => state.revisions,
        "evidence" => List.first(state.recent_evidence) || %{}
      }

      Actor.dispatch(state.context, [
        {:edge, :contradicts, state.context.actor_id, peer_id, 1.0, %{revision: state.revisions}},
        {:send, peer_id, {:challenge, state.context.actor_id, packet}}
      ])
    end)

    state
  end

  defp update_graph_state(state) do
    Graph.update(state.context.run_id, state.context.actor_id, fn attrs ->
      attrs
      |> Map.put(:phase, state.phase)
      |> Map.put(:revisions, state.revisions)
      |> Map.put(:root_probability, state.root_probability)
      |> Map.put(:next_action, state.next_action)
      |> Map.put(:evidence_strength, state.evidence_strength)
      |> Map.put(:evidence_count, state.evidence_count)
    end)

    :ok
  end

  defp tombstone(state, reason) do
    Record.append(state.context.run_id, :gaia_hypothesis_tombstone, %{
      actor_id: state.context.actor_id,
      service: state.service,
      incident: state.incident,
      failure_mode: state.failure_mode,
      reason: reason,
      revisions: state.revisions,
      root_probability: state.root_probability,
      evidence_count: state.evidence_count
    })
  end

  defp phase_for(probability, action, state) do
    cond do
      probability <= state.refute_probability or action == "refute" ->
        :refuted

      probability >= state.confirm_probability or action == "confirm" ->
        :confirmed

      action == "challenge_peers" ->
        :challenging

      action in ["investigate_callers", "investigate_callees", "investigate_both"] ->
        :investigating

      true ->
        :observing
    end
  end

  defp child_grant(state) do
    cond do
      state.branch_credits <= 0 -> 0
      state.branch_width <= 1 -> state.branch_credits
      true -> max(div(state.branch_credits, state.branch_width), 1)
    end
  end

  defp child_packet(state) do
    %{
      "source" => "hypothesis",
      "parent" => inspect(state.context.actor_id),
      "revision" => state.revisions,
      "root_probability" => state.root_probability,
      "failure_mode" => state.failure_mode,
      "message" => "parent hypothesis requested neighbor investigation"
    }
  end

  defp challenge_state(state, packet) do
    %{
      "challenger" => packet,
      "defending_hypothesis" => %{
        "service" => state.service,
        "incident" => state.incident,
        "failure_mode" => state.failure_mode,
        "root_probability" => state.root_probability,
        "revision" => state.revisions,
        "recent_evidence" => Enum.reverse(state.recent_evidence)
      }
    }
  end

  defp compact_evidence(%{event_id: _} = event) do
    raw = event.raw

    %{
      "event_id" => event.event_id,
      "source" => Atom.to_string(event.source),
      "event_time_us" => event.event_time_us,
      "message" => raw["message"],
      "status_code" => raw["status_code"],
      "url" => raw["url"]
    }
  end

  defp compact_evidence(%{} = packet) do
    packet
    |> Enum.map(fn {key, value} -> {to_string(key), semantic_value(value)} end)
    |> Map.new()
  end

  defp semantic_value(value) when is_atom(value), do: Atom.to_string(value)
  defp semantic_value(value) when is_list(value), do: Enum.map(value, &semantic_value/1)

  defp semantic_value(%{} = value) do
    Map.new(value, fn {key, nested} -> {to_string(key), semantic_value(nested)} end)
  end

  defp semantic_value(value), do: value

  defp stable_challenge_id(from, to, packet) do
    :crypto.hash(:sha256, :erlang.term_to_binary({from, to, packet}, [:deterministic]))
    |> Base.encode16(case: :lower)
  end
end

defmodule Plexus.Examples.IncidentCommander.Replay do
  use Plexus.Actor

  alias Plexus.Actor
  alias Plexus.Examples.IncidentCommander
  alias Plexus.Examples.IncidentCommander.{Chronology, Progress}
  alias Plexus.{Record, Run}

  @impl true
  def init(args) do
    cursor = Chronology.open!(args.trace_files, args.business_files, day: args.day)
    context = Actor.context(args)

    state = %{
      context: context,
      owner: args[:owner],
      cursor: cursor,
      progress: args[:progress],
      service_opts: args.service_opts,
      speed: args[:speed] || :max,
      max_events: args[:max_events] || 0,
      chunk_size: max(args[:chunk_size] || 256, 1),
      emitted: 0,
      last_time_us: nil,
      pending: nil
    }

    Actor.dispatch(context, {:sleep, 0})
    {:ok, state}
  end

  @impl true
  def handle_cast({:plexus, :wake}, state) do
    {:noreply, pump(state)}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp pump(state) do
    cond do
      state.max_events > 0 and state.emitted >= state.max_events ->
        finish(state, :max_events)

      state.speed == :max ->
        pump_fast(state, state.chunk_size)

      true ->
        pump_paced(state)
    end
  end

  defp pump_fast(state, 0) do
    Actor.dispatch(state.context, {:sleep, 0})
    state
  end

  defp pump_fast(state, remaining) do
    if state.max_events > 0 and state.emitted >= state.max_events do
      finish(state, :max_events)
    else
      case Chronology.next(state.cursor) do
        :eof ->
          finish(state, :eof)

        {:ok, event, cursor} ->
          state
          |> dispatch_event(event, cursor)
          |> pump_fast(remaining - 1)
      end
    end
  end

  defp pump_paced(%{pending: event} = state) when not is_nil(event) do
    state = dispatch_event(%{state | pending: nil}, event, state.cursor)
    Actor.dispatch(state.context, {:sleep, 0})
    state
  end

  defp pump_paced(state) do
    case Chronology.next(state.cursor) do
      :eof ->
        finish(state, :eof)

      {:ok, event, cursor} ->
        delay_ms = replay_delay_ms(state.last_time_us, event.event_time_us, state.speed)

        if delay_ms > 0 do
          Actor.dispatch(state.context, {:sleep, delay_ms})
          %{state | cursor: cursor, pending: event}
        else
          state = dispatch_event(state, event, cursor)
          Actor.dispatch(state.context, {:sleep, 0})
          state
        end
    end
  end

  defp dispatch_event(state, event, cursor) do
    case IncidentCommander.ensure_service(state.context.run_id, event.service, state.service_opts) do
      {:ok, _pid} ->
        :ok = Run.cast(state.context.run_id, {:service, event.service}, {:telemetry, event})
        if is_pid(state.progress), do: Progress.replay_event(state.progress)

      {:error, reason} ->
        Record.append(state.context.run_id, :gaia_service_start_error, %{
          service: event.service,
          event_id: event.event_id,
          reason: inspect(reason)
        })
    end

    %{state | cursor: cursor, emitted: state.emitted + 1, last_time_us: event.event_time_us}
  end

  defp finish(state, reason) do
    if reason == :eof and is_pid(state.progress), do: Progress.replay_eof(state.progress)

    Record.append(state.context.run_id, :gaia_replay_finished, %{
      reason: reason,
      events: state.emitted
    })

    if is_pid(state.owner) do
      send(state.owner, {:gaia_replay_finished, self(), reason, state.emitted})
    end

    Actor.dispatch(state.context, {:complete, %{reason: reason, events: state.emitted}})
    state
  end

  defp replay_delay_ms(nil, _event_time_us, _speed), do: 0

  defp replay_delay_ms(last_time_us, event_time_us, speed) when is_number(speed) and speed > 0 do
    delta_us = max(event_time_us - last_time_us, 0)
    trunc(delta_us / 1_000 / speed)
  end
end

defmodule Plexus.Examples.IncidentCommander do
  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander.{Hypothesis, Progress, Replay, Service, Topology}
  alias Plexus.Examples.Support.{Data, Runtime}
  alias Plexus.{Budget, Graph, Population, Record, Run}

  @signal_modes [
    dependency_failure: "Upstream or downstream dependency failure",
    request_path_error: "Application request/RPC path failure",
    resource_exhaustion: "CPU, memory, connection pool, queue or worker exhaustion",
    configuration: "Deployment or configuration fault",
    network: "Network transport or reachability fault",
    data_store: "Database or cache fault",
    unknown: "Evidence is insufficient to identify a failure mode"
  ]

  def run(opts) do
    source = source_dir(opts)
    day = opts[:day] || "2021-07-01"
    files = Path.wildcard(Path.join(source, "**/*.csv"))
    trace_files = Enum.filter(files, &path_kind?(&1, "trace"))
    business_files = Enum.filter(files, &path_kind?(&1, "business"))
    run_files = Enum.filter(files, &path_kind?(&1, "run"))

    if trace_files == [] or business_files == [] do
      raise "GAIA MicroSS trace/business CSV files were not found under #{source}"
    end

    max_hypotheses = opts[:max_hypotheses] || 20_000
    max_measurements = opts[:max_measurements] || 50_000
    branch_credits = opts[:branch_credits] || 12
    actor_partitions = opts[:actor_partitions] || max(System.schedulers_online(), 1)
    max_population = max_hypotheses + 20_000
    run_id = {:gaia_living, System.unique_integer([:positive, :monotonic])}

    token_ledger =
      Runtime.token_budget(opts, max_measurements, per_call: 4_000, floor: 1_000_000)

    run =
      Runtime.start_run!(
        id: run_id,
        max_population: max_population,
        max_depth: (opts[:max_depth] || 8) + 2,
        actor_partitions: actor_partitions,
        budgets: [
          measure: max_measurements,
          expand: max(max_hypotheses * max(branch_credits, 1), max_hypotheses),
          population: max_population,
          tokens: token_ledger
        ],
        batch: [
          max: opts[:batch_size] || 64,
          delay_ms: opts[:batch_delay_ms] || 10,
          max_in_flight_batches: opts[:max_in_flight_batches] || 8,
          max_concurrency: opts[:max_concurrency] || 32
        ],
        cache: [ttl_ms: 300_000, max_entries: 250_000],
        schedule: :async,
        replay: :record
      )

    topology = Topology.new()

    {:ok, progress} =
      Progress.start_link(
        run_id: Run.run_id(run),
        max_measurements: max_measurements,
        every: opts[:progress_every] || 100,
        heartbeat_ms: opts[:progress_heartbeat_ms] || 5_000
      )

    try do
      register_contracts!(run)
      prepare_indexes!(run)

      service_opts = [
        topology: topology,
        progress: progress,
        signal_every: opts[:signal_every] || 1,
        trigger_probability: opts[:trigger_probability] || 0.6,
        hypothesis_update_every: opts[:hypothesis_update_every] || 1,
        branch_width: opts[:branch_width] || 2,
        branch_credits: branch_credits,
        peer_challenges: opts[:peer_challenges] || 2,
        max_hypotheses: max_hypotheses,
        incident_window_us: trunc((opts[:incident_window_seconds] || 30) * 1_000_000),
        recent_limit: opts[:recent_limit] || 24
      ]

      {:ok, replay_pid} =
        Run.start_actor(run,
          module: Replay,
          actor_id: :gaia_replay,
          class: :replay,
          init_arg: %{
            owner: self(),
            trace_files: trace_files,
            business_files: business_files,
            day: day,
            progress: progress,
            service_opts: service_opts,
            speed: opts[:speed] || :max,
            max_events: opts[:max_events] || 0,
            chunk_size: opts[:chunk_size] || 256
          }
        )

      timeout_ms = opts[:timeout_ms] || 1_800_000
      replay_result = await_replay_terminal!(replay_pid, timeout_ms)

      if (opts[:max_events] || 0) == 0 and replay_result.reason != :eof do
        raise "GAIA replay terminated with #{inspect(replay_result.reason)} before EOF"
      end

      Runtime.await_quiescent!(run, timeout_ms)
      truth = truth_services(run_files, day)
      summary = summarize(run, topology, truth, day, replay_result)
      report(summary)

      if path = opts[:record_path] do
        :ok = write_event_log!(run, path)
        IO.puts("event record                #{path}")
      end

      summary
    after
      _ = Progress.stop(progress)
      Topology.close(topology)
      Runtime.stop(run)
    end
  end

  @doc false
  def await_replay_terminal!(replay_pid, timeout_ms)
      when is_pid(replay_pid) and is_integer(timeout_ms) and timeout_ms > 0 do
    monitor = Process.monitor(replay_pid)

    receive do
      {:gaia_replay_finished, ^replay_pid, reason, events} ->
        Process.demonitor(monitor, [:flush])
        %{reason: reason, events: events}

      {:DOWN, ^monitor, :process, ^replay_pid, reason} ->
        raise "GAIA replay actor terminated before completion: #{inspect(reason)}"
    after
      timeout_ms ->
        Process.demonitor(monitor, [:flush])
        raise "timed out waiting for GAIA replay terminal result"
    end
  end

  @doc false
  def measurement_credit_available?(run_id) do
    case Budget.remaining(Run.config(run_id).budget, :measure) do
      0 -> false
      _ -> true
    end
  rescue
    ArgumentError -> false
  end

  def register_contracts!(run) do
    signal =
      TypeSafeSDK.prepare!(
        anomaly_relevance:
          TypeSafeSDK.noul(
            "Does this raw telemetry event provide meaningful evidence of an operational anomaly for the named service?"
          ),
        failure_mode:
          TypeSafeSDK.choice(
            "Which failure mode best matches this specific event and its local telemetry window?",
            @signal_modes
          ),
        diagnostic_strength:
          TypeSafeSDK.score(
            "How diagnostically useful is this event for root-cause investigation?",
            ["weak", "limited", "useful", "strong"]
          )
      )

    hypothesis =
      TypeSafeSDK.prepare!(
        root_cause:
          TypeSafeSDK.noul(
            "Given the evolving service evidence, is this named service/failure-mode hypothesis plausibly causal rather than merely symptomatic?"
          ),
        next_action:
          TypeSafeSDK.choice(
            "What should this live hypothesis do next?",
            observe: "Wait for more evidence",
            investigate_callers: "Investigate upstream callers",
            investigate_callees: "Investigate downstream callees",
            investigate_both: "Investigate both topology directions",
            challenge_peers: "Challenge competing hypotheses",
            confirm: "Evidence is strong enough to mark confirmed",
            refute: "Evidence refutes the hypothesis"
          ),
        evidence_strength:
          TypeSafeSDK.score(
            "How strong is the current evidence for this hypothesis?",
            ["weak", "limited", "useful", "strong"]
          )
      )

    challenge =
      TypeSafeSDK.prepare!(
        survives_challenge:
          TypeSafeSDK.noul(
            "Does the defending hypothesis remain plausible after considering the challenger's evidence and temporal claim?"
          ),
        disposition:
          TypeSafeSDK.choice(
            "How should the defending hypothesis respond?",
            stand: "Retain the hypothesis",
            concede: "Concede and remove this hypothesis branch"
          )
      )

    :ok = Plexus.register_contract(run, :gaia_signal, signal, version: 1)
    :ok = Plexus.register_contract(run, :gaia_hypothesis_update, hypothesis, version: 1)
    :ok = Plexus.register_contract(run, :gaia_challenge, challenge, version: 1)
    :ok
  end

  def prepare_indexes!(run) do
    run_id = Run.run_id(run)
    :ok = Population.index(run_id, :incident)
    :ok = Population.index(run_id, :service)
    :ok = Population.index(run_id, :failure_mode)
    :ok
  end

  def ensure_service(run, service, opts) when is_binary(service) do
    run_id = Run.run_id(run)

    case Run.actor_pid(run_id, {:service, service}) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, :not_found} ->
        case Run.start_actor(run_id,
               module: Service,
               actor_id: {:service, service},
               class: :service,
               activity_mode: :resident,
               metadata: %{service: service},
               init_arg: Map.new(Keyword.merge(opts, service: service))
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_registered, pid}} when is_pid(pid) -> {:ok, pid}
          other -> other
        end
    end
  end

  def prepare_hypothesis(run_id, actor_id, opts) do
    topology = Keyword.fetch!(opts, :topology)
    max_hypotheses = Keyword.get(opts, :max_hypotheses, 20_000)
    account = Hypothesis.account_id(actor_id)
    parent_account = Keyword.get(opts, :parent_account, :root)
    branch_credits = max(Keyword.get(opts, :branch_credits, 0), 0)

    case Run.actor_pid(run_id, actor_id) do
      {:ok, _pid} ->
        {:existing, actor_id}

      {:error, :not_found} when parent_account != :root and branch_credits <= 0 ->
        {:error, :branch_credit_exhausted}

      {:error, :not_found} ->
        with :ok <- Topology.reserve_hypothesis(topology, max_hypotheses),
             :ok <- grant_account(run_id, parent_account, account, branch_credits) do
          init_arg =
            opts
            |> Keyword.drop([:parent_account])
            |> Map.new()
            |> Map.put(:account, account)

          {:new, init_arg}
        else
          {:error, :account_exists} ->
            Topology.release_hypothesis(topology)

            case Run.actor_pid(run_id, actor_id) do
              {:ok, _pid} -> {:existing, actor_id}
              _ -> {:error, :account_exists}
            end

          error ->
            Topology.release_hypothesis(topology)
            error
        end
    end
  end

  def start_hypothesis(run, actor_id, opts) do
    parent_id = Keyword.get(opts, :parent_id)

    case prepare_hypothesis(Run.run_id(run), actor_id, opts) do
      {:existing, _} ->
        Run.actor_pid(run, actor_id)

      {:new, init_arg} ->
        case Run.start_actor(run,
               module: Hypothesis,
               actor_id: actor_id,
               parent_id: parent_id,
               class: :hypothesis,
               activity_mode: :resident,
               metadata: %{
                 incident: init_arg.incident,
                 service: init_arg.service,
                 failure_mode: init_arg.failure_mode
               },
               init_arg: init_arg
             ) do
          {:ok, pid} ->
            {:ok, pid}

          error ->
            cleanup_prepared_hypothesis(Run.run_id(run), init_arg.topology, actor_id)
            error
        end

      error ->
        error
    end
  end

  def cleanup_prepared_hypothesis(run_id, topology, actor_id) do
    _ = Accounts.close(run_id, Hypothesis.account_id(actor_id))
    Topology.release_hypothesis(topology)
    :ok
  rescue
    _ -> :ok
  end

  def hypothesis_id(incident, service, failure_mode),
    do: {:hypothesis, incident, service, failure_mode}

  def child_hypothesis_id(parent_id, incident, service, failure_mode) do
    lineage =
      :crypto.hash(:sha256, :erlang.term_to_binary(parent_id, [:deterministic]))
      |> Base.encode16(case: :lower)

    {:hypothesis, incident, service, failure_mode, lineage}
  end

  def service_event(service), do: {:gaia_service_changed, service}

  def incident_key(event_time_us, window_us) do
    bucket = div(event_time_us, max(window_us, 1))
    "incident-#{bucket}"
  end

  defp grant_account(run_id, parent, child, branch_credits) do
    Accounts.grant(run_id, parent, child, expand: branch_credits)
  end

  defp source_dir(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("gaia")
    source = opts[:source_dir] || Path.join([data_dir, "GAIA-DataSet", "MicroSS"])

    unless File.dir?(source) do
      raise "missing GAIA MicroSS directory #{source}; run fetch.exs first or pass --source-dir"
    end

    source
  end

  defp truth_services(run_files, day) do
    run_files
    |> Stream.flat_map(fn path -> Data.csv_maps!(path) end)
    |> Stream.filter(fn row ->
      timestamp = row["timestamp"] || row["datetime"] || ""
      String.starts_with?(timestamp, day) and Regex.match?(~r/anomal/i, row["message"] || "")
    end)
    |> Stream.map(&(&1["service"] || &1["service_name"]))
    |> Stream.reject(&(&1 in [nil, ""]))
    |> Enum.uniq()
  end

  defp summarize(run, topology, truth, day, replay_result) do
    run_id = Run.run_id(run)
    hypotheses = Graph.by_class(run_id, :hypothesis)
    services = Graph.by_class(run_id, :service)
    events = Record.events(run_id)
    transport = transport_from_events(events)
    acceptance = acceptance_from_events(events)

    top =
      hypotheses
      |> Enum.sort_by(
        fn {_id, attrs} ->
          {attrs[:root_probability] || 0.0, attrs[:evidence_strength] || 0,
           attrs[:revisions] || 0}
        end,
        :desc
      )
      |> Enum.take(10)

    predicted =
      case List.first(top) do
        {_id, attrs} -> attrs[:service]
        nil -> nil
      end

    max_service_events =
      services
      |> Enum.map(fn {_id, attrs} -> attrs[:event_count] || 0 end)
      |> Enum.max(fn -> 0 end)

    %{
      day: day,
      raw_events: replay_result[:events] || 0,
      replay_eof: replay_result[:reason] == :eof,
      replay_reason: replay_result[:reason],
      services: length(services),
      max_service_events: max_service_events,
      live_hypotheses: length(hypotheses),
      tracked_live_hypotheses: Topology.hypothesis_count(topology),
      hypothesis_revisions: event_count(events, :gaia_hypothesis_revision),
      challenges: event_count(events, :gaia_challenge_result),
      pruned: event_count(events, :subtree_pruned),
      semantic_starts: transport.starts,
      semantic_stops: transport.stops,
      semantic_exceptions: transport.exceptions,
      input_tokens: transport.input_tokens,
      output_tokens: transport.output_tokens,
      truth_services: truth,
      top_predicted_service: predicted,
      top_hits_truth: not is_nil(predicted) and predicted in truth,
      acceptance: acceptance,
      top: top
    }
  end

  defp report(summary) do
    IO.puts("")
    IO.puts("GAIA living system twin — #{summary.day}")
    IO.puts("raw telemetry events         #{summary.raw_events}")
    IO.puts("telemetry EOF                #{summary.replay_eof}")
    IO.puts("replay finish reason         #{summary.replay_reason || "<none>"}")
    IO.puts("resident services            #{summary.services}")
    IO.puts("max events on one service    #{summary.max_service_events}")
    IO.puts("live hypotheses              #{summary.live_hypotheses}")
    IO.puts("tracked live hypotheses      #{summary.tracked_live_hypotheses}")
    IO.puts("hypothesis revisions         #{summary.hypothesis_revisions}")
    IO.puts("peer challenges              #{summary.challenges}")
    IO.puts("pruned hypotheses/subtrees   #{summary.pruned}")
    IO.puts("TypeSafe evaluate starts     #{summary.semantic_starts}")
    IO.puts("TypeSafe evaluate stops      #{summary.semantic_stops}")
    IO.puts("TypeSafe exceptions          #{summary.semantic_exceptions}")
    IO.puts("TypeSafe input tokens        #{summary.input_tokens}")
    IO.puts("TypeSafe output tokens       #{summary.output_tokens}")
    IO.puts("known injected services      #{Enum.join(summary.truth_services, ", ")}")
    IO.puts("top predicted service        #{summary.top_predicted_service || "<none>"}")
    IO.puts("top hits injected service    #{summary.top_hits_truth}")

    IO.puts("")
    IO.puts("Actor-semantics evidence:")
    IO.puts("  semantic before EOF        #{summary.acceptance.semantic_before_eof}")
    IO.puts("  hypotheses revised 2+      #{summary.acceptance.multi_revision_hypotheses}")
    IO.puts("  challenge results          #{summary.acceptance.challenge_results}")
    IO.puts("  invalidations              #{summary.acceptance.invalidations}")
    IO.puts("  topology changes           #{summary.acceptance.topology_changes}")
    IO.puts("  topology after hypothesis  #{summary.acceptance.topology_after_hypothesis_birth}")
    IO.puts("  subtree prunes             #{summary.acceptance.subtree_prunes}")
    IO.puts("  branch-credit denials      #{summary.acceptance.branch_credit_denials}")

    IO.puts("")
    IO.puts("Top surviving hypotheses:")

    Enum.each(summary.top, fn {id, attrs} ->
      IO.inspect(%{
        actor_id: id,
        service: attrs[:service],
        incident: attrs[:incident],
        failure_mode: attrs[:failure_mode],
        phase: attrs[:phase],
        revisions: attrs[:revisions],
        root_probability: attrs[:root_probability],
        evidence_strength: attrs[:evidence_strength]
      })
    end)
  end

  def acceptance_evidence(run) do
    run
    |> Run.run_id()
    |> Record.events()
    |> acceptance_from_events()
  end

  defp acceptance_from_events(events) do
    first_semantic =
      first_sequence(events, fn event ->
        event.type == {:typesafe, {:typesafe_sdk, :evaluate, :start}}
      end)

    eof_sequence =
      first_sequence(events, fn event ->
        event.type == :gaia_replay_finished and event.data[:reason] == :eof
      end)

    first_hypothesis_birth =
      first_sequence(events, fn event ->
        event.type == :actor_birth and event.data[:class] == :hypothesis
      end)

    revision_counts =
      events
      |> Enum.filter(&(&1.type == :gaia_hypothesis_revision))
      |> Enum.frequencies_by(& &1.data.actor_id)

    topology_sequences =
      events
      |> Enum.filter(&(&1.type == :gaia_topology_changed))
      |> Enum.map(& &1.sequence)

    %{
      semantic_before_eof:
        is_integer(first_semantic) and is_integer(eof_sequence) and first_semantic < eof_sequence,
      multi_revision_hypotheses: Enum.count(revision_counts, fn {_id, count} -> count >= 2 end),
      challenge_results: event_count(events, :gaia_challenge_result),
      invalidations: event_count(events, :gaia_hypothesis_invalidated),
      topology_changes: length(topology_sequences),
      topology_after_hypothesis_birth:
        is_integer(first_hypothesis_birth) and
          Enum.any?(topology_sequences, &(&1 > first_hypothesis_birth)),
      subtree_prunes: event_count(events, :subtree_pruned),
      branch_credit_denials: event_count(events, :gaia_branch_credit_denied)
    }
  end

  def write_event_log!(run, path) when is_binary(path) do
    run_id = Run.run_id(run)
    directory = Path.dirname(path)
    if directory not in ["", "."], do: File.mkdir_p!(directory)

    File.open!(path, [:write, :utf8], fn io ->
      Record.events(run_id)
      |> Enum.each(fn event ->
        IO.write(io, Jason.encode!(json_safe(event)))
        IO.write(io, "\n")
      end)
    end)

    :ok
  end

  defp transport_from_events(events) do
    Enum.reduce(
      events,
      %{starts: 0, stops: 0, exceptions: 0, input_tokens: 0, output_tokens: 0},
      fn event, acc ->
        case event.type do
          {:typesafe, {:typesafe_sdk, :evaluate, :start}} ->
            %{acc | starts: acc.starts + 1}

          {:typesafe, {:typesafe_sdk, :evaluate, :stop}} ->
            measurements = event.data[:measurements] || %{}

            %{
              acc
              | stops: acc.stops + 1,
                input_tokens: acc.input_tokens + integer_value(measurements[:input_tokens]),
                output_tokens: acc.output_tokens + integer_value(measurements[:output_tokens])
            }

          {:typesafe, {:typesafe_sdk, :evaluate, :exception}} ->
            %{acc | exceptions: acc.exceptions + 1}

          _ ->
            acc
        end
      end
    )
  end

  defp integer_value(value) when is_integer(value), do: value
  defp integer_value(_value), do: 0

  defp event_count(events, type), do: Enum.count(events, &(&1.type == type))

  defp first_sequence(events, predicate) do
    case Enum.find(events, predicate) do
      nil -> nil
      event -> event.sequence
    end
  end

  defp json_safe(value)
       when is_nil(value) or is_boolean(value) or is_number(value) or is_binary(value),
       do: value

  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)

  defp json_safe(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&json_safe/1)

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)

  defp json_safe(%{} = value) do
    Map.new(value, fn {key, nested} -> {json_key(key), json_safe(nested)} end)
  end

  defp json_safe(value), do: inspect(value)

  defp json_key(key) when is_binary(key), do: key
  defp json_key(key) when is_atom(key), do: Atom.to_string(key)
  defp json_key(key), do: inspect(key)

  defp path_kind?(path, kind),
    do: path |> String.downcase() |> String.contains?("/#{kind}/")
end
