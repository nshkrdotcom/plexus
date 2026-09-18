defmodule Plexus.Examples.IntakeCoordinator do
  @moduledoc """
  Example root actor demonstrating recursive TypeSafe use.

  Flow:

  1. classify the incoming ticket
  2. derive candidate evidence sentences
  3. spawn leaf workers for each sentence
  4. each worker performs its own TypeSafe evaluation
  """

  use Plexus.Actor

  alias Plexus.Run
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
          TypeSafeSDK.noul(
            "Return up to three compact evidence statements separated by semicolons."
          )
      )

    {:ok,
     %{
       text: text,
       context: Map.take(args, [:run, :run_id, :actor_id, :parent_id]),
       prepared: prepared,
       classification: nil,
       children: []
     }}
  end

  @impl true
  def handle_cast(:classify, state) do
    {:evaluate, {:classify, %{ticket: state.text}, state.prepared}, state}
  end

  def handle_cast({:spawn_evidence_workers, snippets}, state) do
    children =
      Enum.map(Enum.with_index(snippets, 1), fn {snippet, index} ->
        actor_id = {state.context.actor_id, :evidence, index}

        {:ok, _pid} =
          Run.start_actor(state.context.run,
            module: Plexus.Examples.EvidenceWorker,
            actor_id: actor_id,
            parent_id: state.context.actor_id,
            init_arg: %{text: snippet}
          )

        Run.cast(state.context.run, actor_id, :analyze)
        actor_id
      end)

    {:noreply, %{state | children: children}}
  end

  @impl true
  def handle_call(:classification, _from, state) do
    {:reply, state.classification, state}
  end

  @impl true
  def handle_call(:children, _from, state) do
    {:reply, state.children, state}
  end

  @impl true
  def handle_evaluation({:ok, response}, :classify, state) do
    snippets =
      case Response.fetch(response, :evidence) do
        {:ok, %{noul: p}} when p >= 0.5 ->
          state.text
          |> String.split([";", "\n", " and "], trim: true)
          |> Enum.map(&String.trim/1)
          |> Enum.reject(&(&1 == ""))
          |> Enum.take(3)

        _ ->
          []
      end

    classification = %{
      response: response,
      department: Response.fetch(response, :department),
      urgent: Response.fetch(response, :urgent),
      evidence_snippets: snippets
    }

    if snippets == [] do
      {:noreply, %{state | classification: classification}}
    else
      {:noreply, %{state | classification: classification},
       {:continue, {:spawn_evidence_workers, snippets}}}
    end
  end

  def handle_evaluation({:error, error}, :classify, state) do
    {:noreply, %{state | classification: {:error, error}}}
  end

  @impl true
  def handle_continue({:spawn_evidence_workers, snippets}, state) do
    handle_cast({:spawn_evidence_workers, snippets}, state)
  end
end
