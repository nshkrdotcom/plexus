defmodule Plexus.Examples.IntakeCoordinator do
  @moduledoc """
  Example root actor demonstrating interpreter-mediated recursive work.

  The coordinator measures a ticket, then declares child births/messages. It
  never calls `Plexus.Run.start_actor/2` from inside its callback.
  """

  use Plexus.Actor

  alias Plexus.Actor
  alias TypeSafeSDK.Response

  @impl true
  def init(%{text: text} = args) do
    prepared =
      TypeSafeSDK.prepare!(
        department:
          TypeSafeSDK.choice(
            "Route the issue to the primary department.",
            billing: "Invoice, charges, payment, or subscription issues.",
            technical: "Application bugs, outages, or broken workflows.",
            sales: "Pricing, plan, or procurement requests."
          ),
        urgent: TypeSafeSDK.noul("Does this need urgent human attention?"),
        evidence:
          TypeSafeSDK.noul("Does the ticket contain multiple distinct evidence statements?")
      )

    {:ok,
     %{
       text: text,
       context: Actor.context(args),
       prepared: prepared,
       classification: nil,
       children: []
     }}
  end

  @impl true
  def handle_cast(:classify, state) do
    :ok =
      Actor.dispatch(
        state.context,
        {:measure, :classify, %{ticket: state.text}, state.prepared, []}
      )

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :classify, {:ok, response}}, state) do
    snippets = evidence_snippets(response, state.text)

    classification = %{
      response: response,
      department: Response.fetch(response, :department),
      urgent: Response.fetch(response, :urgent),
      evidence_snippets: snippets
    }

    {commands, children} = child_commands(state.context.actor_id, snippets)
    if commands != [], do: Actor.dispatch(state.context, commands)

    {:noreply, %{state | classification: classification, children: children}}
  end

  def handle_cast({:plexus, :measurement, :classify, {:error, error}}, state) do
    {:noreply, %{state | classification: {:error, error}}}
  end

  @impl true
  def handle_call(:classification, _from, state), do: {:reply, state.classification, state}

  @impl true
  def handle_call(:children, _from, state), do: {:reply, state.children, state}

  @impl true
  def handle_evaluation(result, tag, state) do
    {:noreply, Map.put(state, :last_direct_evaluation, {tag, result})}
  end

  defp evidence_snippets(response, text) do
    case Response.fetch(response, :evidence) do
      {:ok, %{noul: p}} when p >= 0.5 ->
        text
        |> String.split([";", "\n", " and "], trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> Enum.take(3)

      _ ->
        []
    end
  end

  defp child_commands(parent_id, snippets) do
    snippets
    |> Enum.with_index(1)
    |> Enum.reduce({[], []}, fn {snippet, index}, {commands, ids} ->
      actor_id = {parent_id, :evidence, index}

      commands =
        commands ++
          [
            {:spawn, :evidence, Plexus.Examples.EvidenceWorker, %{text: snippet},
             actor_id: actor_id},
            {:send, actor_id, :analyze}
          ]

      {commands, ids ++ [actor_id]}
    end)
  end
end
