defmodule Plexus.Actor.Interpreter do
  @moduledoc """
  Interpreter for `Plexus.Actor.Command` effects.

  Cross-cutting policy belongs here instead of in strategy modules.
  """

  alias Plexus.{Budget, Event, Graph, Record, Run, Schedule}
  alias Plexus.Run.Config

  @spec dispatch(map(), [Plexus.Actor.Command.t()] | Plexus.Actor.Command.t()) :: :ok
  def dispatch(context, commands) do
    context = normalize_context(context)
    Schedule.dispatch(context.run_id, %{context: context, commands: List.wrap(commands)})
  end

  @doc false
  @spec execute_now(term(), map()) :: :ok
  def execute_now(run_id, %{context: context, commands: commands}) do
    Enum.each(commands, &execute(run_id, context, &1))
    :ok
  end

  defp execute(run_id, context, {:spawn, class, module, init_arg, opts}) do
    actor_id = Keyword.get_lazy(opts, :actor_id, fn -> auto_actor_id(context.actor_id, class) end)

    opts =
      opts
      |> Keyword.put(:module, module)
      |> Keyword.put(:actor_id, actor_id)
      |> Keyword.put_new(:parent_id, context.actor_id)
      |> Keyword.put(:class, class)
      |> Keyword.put(:init_arg, init_arg)

    case Run.start_actor(run_id, opts) do
      {:ok, _pid} -> :ok
      {:error, reason} -> deliver(run_id, context.actor_id, {:plexus, :command_error, {:spawn, actor_id}, reason})
    end
  end

  defp execute(run_id, _context, {:edge, type, from, to, weight}) do
    Graph.add_edge(run_id, type, from, to, weight)
  end

  defp execute(run_id, _context, {:edge, type, from, to, weight, provenance}) do
    Graph.add_edge(run_id, type, from, to, weight, provenance)
  end

  defp execute(run_id, _context, {:send, actor_id, message}), do: Run.cast(run_id, actor_id, message)

  defp execute(run_id, context, {:measure, tag, state, contract, opts}) do
    config = Config.fetch!(run_id)

    case Budget.reserve(config.budget, :measure, 1) do
      :ok -> Plexus.Measure.submit(run_id, context, tag, state, contract, opts)
      {:error, :budget_exhausted} -> deliver_measure(run_id, context.actor_id, tag, {:error, {:budget_exhausted, :measure}})
    end
  end

  defp execute(run_id, context, {:expand, tag, spec, opts}) do
    config = Config.fetch!(run_id)

    case Budget.reserve(config.budget, :expand, 1) do
      :ok ->
        Plexus.Schedule.Quiescence.add(config.quiescence, :expansions, 1)
        Plexus.Expand.Queue.submit(run_id, context, tag, spec, opts)

      {:error, :budget_exhausted} ->
        deliver_expand(run_id, context.actor_id, tag, {:error, {:budget_exhausted, :expand}})
    end
  end

  defp execute(run_id, context, {:belief, belief}) do
    Graph.update(run_id, context.actor_id, fn attrs ->
      attrs
      |> Map.put(:belief, belief)
      |> Map.update(:epoch, 1, &(&1 + 1))
      |> Map.put(:stale, false)
    end)

    Graph.outgoing(run_id, context.actor_id, :all)
    |> Enum.filter(&(&1.type in [:neighbor, :supports, :contradicts, :implies]))
    |> Enum.each(fn edge -> deliver(run_id, edge.node, {:plexus, :belief, context.actor_id, belief}) end)
  end

  defp execute(run_id, _context, {:budget, action, meter, amount}) when action in [:reserve, :refund] do
    budget = Config.fetch!(run_id).budget
    if action == :reserve, do: Budget.reserve(budget, meter, amount), else: Budget.refund(budget, meter, amount)
  end

  defp execute(run_id, _context, {:prune, actor_id}), do: Run.prune(run_id, actor_id)

  defp execute(run_id, context, {:sleep, timeout}) when is_integer(timeout) and timeout >= 0 do
    config = Config.fetch!(run_id)
    Plexus.Schedule.Quiescence.add(config.quiescence, :timers, 1)
    Process.send_after(config.schedule_server, {:wake_actor, context.actor_id}, timeout)
    :ok
  end

  defp execute(run_id, context, {:wake_on, event}) do
    Event.subscribe(run_id, event, context.actor_id)
    Record.append(run_id, :wake_subscription, %{
      actor_id: context.actor_id,
      event_kind: if(is_atom(event), do: event, else: :opaque)
    })
  end

  defp execute(run_id, context, {:complete, result}) do
    case Graph.get(run_id, context.actor_id) do
      %{status: :complete} ->
        :ok

      nil ->
        :ok

      _attrs ->
        Graph.update(run_id, context.actor_id, fn attrs ->
          attrs |> Map.put(:result, result) |> Map.put(:status, :complete)
        end)

        config = Config.fetch!(run_id)
        current = Plexus.Schedule.Quiescence.get(config.quiescence, :actors)
        if current > 0, do: Plexus.Schedule.Quiescence.add(config.quiescence, :actors, -1)
        Record.append(run_id, :actor_complete, %{actor_id: context.actor_id, has_result: not is_nil(result)})
    end
  end

  defp execute(run_id, context, command) do
    deliver(run_id, context.actor_id, {:plexus, :command_error, command, :unsupported_command})
  end

  defp normalize_context(%{run_id: run_id} = context) do
    %{
      run_id: run_id,
      actor_id: Map.get(context, :actor_id),
      parent_id: Map.get(context, :parent_id),
      class: Map.get(context, :class),
      metadata: Map.get(context, :metadata, %{})
    }
  end

  defp normalize_context(other), do: raise(ArgumentError, "invalid Plexus actor context: #{inspect(other)}")

  defp auto_actor_id(parent_id, class) do
    {parent_id, class, System.unique_integer([:positive, :monotonic])}
  end

  defp deliver_measure(run_id, actor_id, tag, result), do: deliver(run_id, actor_id, {:plexus, :measurement, tag, result})
  defp deliver_expand(run_id, actor_id, tag, result), do: deliver(run_id, actor_id, {:plexus, :expansion, tag, result})

  defp deliver(_run_id, nil, _message), do: :ok
  defp deliver(run_id, actor_id, message), do: Run.cast(run_id, actor_id, message)
end
