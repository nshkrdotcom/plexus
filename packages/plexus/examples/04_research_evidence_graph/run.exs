Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Data, Metrics, Runtime}


defmodule Plexus.Examples.ResearchGraph.Claim do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok, %{context: Actor.context(args), claim: args.claim, expected: args.expected, received: 0, predictions: []}}
  end

  @impl true
  def handle_cast({:evidence_result, result}, state) do
    next = %{state | received: state.received + 1, predictions: [result | state.predictions]}

    if next.received == next.expected do
      counts = next.predictions |> Enum.map(& &1.predicted) |> Enum.frequencies()
      Actor.dispatch(next.context, {:complete, %{claim_id: state.claim["id"], claim: state.claim["claim"], relation_counts: counts}})
    end

    {:noreply, next}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end


defmodule Plexus.Examples.ResearchGraph.EvidencePair do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       claim_id: args.claim_id,
       claim_actor: args.claim_actor,
       claim_text: args.claim_text,
       document: args.document,
       gold: args.gold,
       contract: args.contract
     }}
  end

  @impl true
  def handle_cast(:evaluate, state) do
    input = %{
      scientific_claim: state.claim_text,
      paper_title: state.document["title"],
      paper_abstract: state.document["abstract"]
    }

    Actor.dispatch(state.context, {:measure, :evidence_relation, input, state.contract, []})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :evidence_relation, {:ok, response}}, state) do
    relation = Plexus.Belief.from(response, :relation)
    strength = Plexus.Belief.from(response, :strength)
    predicted = relation.value
    edge = edge_type(predicted)
    result = %{
      claim_id: state.claim_id,
      document_id: state.document["doc_id"],
      predicted: predicted,
      gold: state.gold,
      correct: predicted == state.gold,
      strength: strength.value
    }

    Actor.dispatch(state.context, [
      {:belief, relation},
      {:edge, edge, state.context.actor_id, state.claim_actor, 1.0, %{strength: strength.value, dataset: :scifact}},
      {:send, state.claim_actor, {:evidence_result, result}},
      {:complete, result}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :evidence_relation, {:error, error}}, state) do
    result = %{claim_id: state.claim_id, document_id: state.document["doc_id"], gold: state.gold, error: inspect(error)}
    Actor.dispatch(state.context, [{:send, state.claim_actor, {:evidence_result, Map.put(result, :predicted, :error)}}, {:complete, result}])
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp edge_type(:support), do: :supports
  defp edge_type(:contradict), do: :contradicts
  defp edge_type(_), do: :insufficient_evidence
end


defmodule Plexus.Examples.ResearchGraph do
  alias Plexus.Examples.ResearchGraph.{Claim, EvidencePair}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Run}

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("scifact")
    root = locate_root!(data_dir)
    claim_limit = opts[:claims] || 50
    if claim_limit < 1, do: raise(ArgumentError, "--claims must be at least 1")

    claims = Data.jsonl!(Path.join(root, "claims_dev.jsonl")) |> Enum.take(claim_limit)
    needed_ids = claims |> Enum.flat_map(&(&1["cited_doc_ids"] || [])) |> MapSet.new()

    corpus =
      Data.jsonl!(Path.join(root, "corpus.jsonl"))
      |> Stream.filter(&MapSet.member?(needed_ids, &1["doc_id"]))
      |> Map.new(&{&1["doc_id"], &1})

    pairs = Enum.flat_map(claims, &pairs_for_claim(&1, corpus))
    run = Runtime.start_run!(
      max_population: length(claims) + length(pairs) + 100,
      budgets: [
        measure: length(pairs),
        population: length(claims) + length(pairs) + 100,
        tokens: Runtime.token_budget(opts, length(pairs), per_call: 12_000, floor: 500_000)
      ]
    )

    try do
      prepared = TypeSafeSDK.prepare!(
        relation:
          TypeSafeSDK.choice(
            "What relation does the supplied scientific abstract have to the claim?",
            support: "The abstract provides evidence supporting the claim",
            contradict: "The abstract provides evidence contradicting the claim",
            not_enough_info: "The abstract does not provide enough evidence either way"
          ),
        strength:
          TypeSafeSDK.score(
            "How directly does this abstract bear on the claim?",
            ["incidental", "weak", "moderate", "direct"]
          )
      )
      :ok = Plexus.register_contract(run, :scifact_relation, prepared, version: 1)

      Enum.each(claims, fn claim ->
        claim_pairs = Enum.filter(pairs, &(&1.claim_id == claim["id"]))
        claim_id = {:claim, claim["id"]}
        {:ok, _} = Plexus.start_actor(run, module: Claim, actor_id: claim_id, class: :claim,
          init_arg: %{claim: claim, expected: max(length(claim_pairs), 1)})

        Enum.each(claim_pairs, fn pair ->
          pair_id = {:evidence, claim["id"], pair.document["doc_id"]}
          {:ok, _} = Plexus.start_actor(run, module: EvidencePair, actor_id: pair_id, class: :evidence_pair,
            init_arg: %{
              claim_id: claim["id"], claim_actor: claim_id, claim_text: claim["claim"],
              document: pair.document, gold: pair.gold, contract: :scifact_relation
            })
          Plexus.cast({run, pair_id}, :evaluate)
        end)

        if claim_pairs == [], do: Plexus.cast({run, claim_id}, {:evidence_result, %{predicted: :not_enough_info}})
      end)

      Runtime.await_class_complete!(run, :evidence_pair, length(pairs), opts[:timeout_ms] || 240_000)
      Runtime.await_class_complete!(run, :claim, length(claims), 30_000)
      Runtime.await_quiescent!(run, 30_000)
      report(run, claims, pairs)
    after
      Runtime.stop(run)
    end
  end

  defp pairs_for_claim(claim, corpus) do
    gold = gold_labels(claim)

    (claim["cited_doc_ids"] || [])
    |> Enum.flat_map(fn doc_id ->
      case Map.get(corpus, doc_id) do
        nil -> []
        document -> [%{claim_id: claim["id"], document: document, gold: Map.get(gold, to_string(doc_id), :not_enough_info)}]
      end
    end)
  end

  defp gold_labels(claim) do
    (claim["evidence"] || %{})
    |> Map.new(fn {doc_id, rationales} ->
      label =
        case List.first(rationales) do
          %{} = rationale -> rationale["label"]
          _ -> nil
        end

      value =
        case label do
          "SUPPORT" -> :support
          "CONTRADICT" -> :contradict
          _ -> :not_enough_info
        end

      {to_string(doc_id), value}
    end)
  end

  defp report(run, claims, pairs) do
    id = Run.run_id(run)
    results =
      Graph.by_class(id, :evidence_pair)
      |> Enum.flat_map(fn {_id, attrs} -> if is_map(attrs[:result]), do: [attrs.result], else: [] end)
    correct = Enum.count(results, & &1[:correct])

    IO.puts("\nSciFact research evidence graph")
    Metrics.print_table([
      {"claims", length(claims)},
      {"claim/document pairs", length(pairs)},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"pair relation accuracy", :io_lib.format("~.1f%", [Metrics.pct(correct, length(results))]) |> IO.iodata_to_binary()},
      {"typed graph nodes", Graph.count(id)}
    ])

    IO.puts("\nExample completed claim graphs:")
    Graph.by_class(id, :claim) |> Enum.take(8) |> Enum.each(fn {_id, attrs} -> IO.inspect(attrs.result) end)
  end

  defp locate_root!(data_dir) do
    [Path.join(data_dir, "data"), data_dir]
    |> Enum.find(&File.exists?(Path.join(&1, "corpus.jsonl"))) ||
      raise("SciFact data not found under #{data_dir}; run fetch.exs first")
  end
end

{opts, _, _} = OptionParser.parse(System.argv(), strict: [data_dir: :string, claims: :integer, token_budget: :integer, timeout_ms: :integer])
Plexus.Examples.ResearchGraph.run(opts)
