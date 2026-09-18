Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Data, Metrics, Runtime}

defmodule Plexus.Examples.IssueSwarm.Bucket do
  use Plexus.Actor

  alias Plexus.Actor
  alias Plexus.Examples.Support.Metrics

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       count: 0,
       correct: 0,
       difficulty: []
     }}
  end

  @impl true
  def handle_cast({:issue_result, difficulty, correct?}, state) do
    {:noreply,
     %{
       state
       | count: state.count + 1,
         correct: state.correct + if(correct?, do: 1, else: 0),
         difficulty: [difficulty | state.difficulty]
     }}
  end

  def handle_cast(:finish, state) do
    Actor.dispatch(
      state.context,
      {:complete,
       %{
         count: state.count,
         correct: state.correct,
         mean_difficulty: Metrics.mean(state.difficulty)
       }}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.IssueSwarm.Issue do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       instance: args.instance,
       actual_fix_shape: args.actual_fix_shape,
       bucket_actor_ids: args.bucket_actor_ids
     }}
  end

  @impl true
  def handle_cast(:analyze, state) do
    input = %{
      repository: state.instance["repo"],
      problem_statement: state.instance["problem_statement"],
      hints: state.instance["hints_text"],
      release_version: state.instance["version"]
    }

    Actor.dispatch(state.context, {:measure, :triage, input, :issue_swarm, []})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :triage, {:ok, response}}, state) do
    fix_shape = Plexus.Belief.from(response, :fix_shape)
    difficulty = Plexus.Belief.from(response, :difficulty).value || 0.0
    predicted = fix_shape.value
    bucket_actor = Map.fetch!(state.bucket_actor_ids, predicted)
    correct? = predicted == state.actual_fix_shape

    Actor.dispatch(state.context, [
      {:belief, fix_shape},
      {:edge, :routes_to, state.context.actor_id, bucket_actor, 1.0, %{predicted: true}},
      {:send, bucket_actor, {:issue_result, difficulty, correct?}},
      {:complete,
       %{
         predicted_fix_shape: predicted,
         actual_fix_shape: state.actual_fix_shape,
         difficulty: difficulty,
         correct: correct?
       }}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :triage, {:error, error}}, state) do
    Actor.dispatch(state.context, {:complete, %{error: inspect(error)}})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.IssueSwarm do
  alias Plexus.Examples.IssueSwarm.{Bucket, Issue}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Run}

  @fix_shapes [:source_code, :tests, :documentation, :configuration_build, :mixed]

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("swe-bench-verified")
    path = Path.join(data_dir, "swe_bench_verified.jsonl")
    unless File.exists?(path), do: raise("missing #{path}; run fetch.exs first")

    limit = opts[:limit] || 50
    if limit < 1, do: raise(ArgumentError, "--limit must be at least 1")

    rows = Data.jsonl!(path) |> Enum.take(limit)

    annotated =
      Enum.map(rows, fn row ->
        actual = actual_fix_shape(row["patch"])

        row
        |> Map.drop(["patch", "test_patch", "eval_script", "FAIL_TO_PASS", "PASS_TO_PASS"])
        |> Map.put("_actual_fix_shape", actual)
      end)

    repos = annotated |> Enum.map(& &1["repo"]) |> Enum.uniq()
    population_limit = length(annotated) + length(repos) * length(@fix_shapes) + 100

    run =
      Runtime.start_run!(
        max_population: max(2_000, population_limit),
        budgets: [
          measure: length(annotated),
          population: max(2_000, population_limit),
          tokens: Runtime.token_budget(opts, length(annotated), per_call: 20_000, floor: 500_000)
        ]
      )

    try do
      register_contract(run)
      bucket_ids = start_bucket_actors(run, repos)

      Enum.each(annotated, fn instance ->
        repo = instance["repo"]
        ids = Map.new(@fix_shapes, &{&1, {repo, :fix_shape, &1}})
        actor_id = {:issue, instance["instance_id"]}

        {:ok, _} =
          Plexus.start_actor(run,
            module: Issue,
            actor_id: actor_id,
            class: :issue,
            init_arg: %{
              instance: instance,
              actual_fix_shape: instance["_actual_fix_shape"],
              bucket_actor_ids: ids
            }
          )

        Plexus.cast({run, actor_id}, :analyze)
      end)

      Runtime.await_class_complete!(run, :issue, length(annotated), opts[:timeout_ms] || 180_000)
      Runtime.await_messages_drained!(run)
      Enum.each(Map.values(bucket_ids), &Plexus.cast({run, &1}, :finish))
      Runtime.await_quiescent!(run, 30_000)
      report(run, annotated)
    after
      Runtime.stop(run)
    end
  end

  defp register_contract(run) do
    prepared =
      TypeSafeSDK.prepare!(
        fix_shape:
          TypeSafeSDK.choice(
            "Which broad shape is the eventual fix most likely to have?",
            source_code: "Primarily implementation/source-code changes",
            tests: "Primarily tests, fixtures, or test infrastructure",
            documentation: "Primarily documentation or examples",
            configuration_build: "Primarily build, packaging, CI, or configuration files",
            mixed: "A cross-cutting fix spanning more than one of these categories"
          ),
        difficulty:
          TypeSafeSDK.score(
            "How difficult is this issue likely to be to resolve correctly?",
            ["small/localized", "moderate", "substantial", "deep/cross-cutting"]
          )
      )

    :ok = Plexus.register_contract(run, :issue_swarm, prepared, version: 1)
  end

  defp start_bucket_actors(run, repos) do
    Enum.reduce(repos, %{}, fn repo, acc ->
      Enum.reduce(@fix_shapes, acc, fn fix_shape, inner ->
        id = {repo, :fix_shape, fix_shape}

        {:ok, _} =
          Plexus.start_actor(run,
            module: Bucket,
            actor_id: id,
            class: :fix_bucket,
            init_arg: %{}
          )

        Map.put(inner, {repo, fix_shape}, id)
      end)
    end)
  end

  defp report(run, rows) do
    id = Run.run_id(run)
    completed = Graph.by_class(id, :issue)
    results = for {_actor, %{result: result}} <- completed, is_map(result), do: result
    correct = Enum.count(results, & &1[:correct])

    IO.puts("\nSWE-bench Verified issue swarm")

    Metrics.print_table([
      {"instances", length(rows)},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"fix-shape accuracy", percentage(correct, length(results))},
      {"graph nodes", Graph.count(id)}
    ])

    IO.puts("\nBusiest repository/fix-shape buckets:")

    Runtime.top_complete(run, :fix_bucket, 12, &Map.get(&1, :count, 0))
    |> Enum.each(fn {actor_id, attrs} -> IO.inspect({actor_id, attrs.result}) end)
  end

  defp actual_fix_shape(patch) when is_binary(patch) do
    categories =
      Regex.scan(~r/^diff --git a\/(.+?) b\//m, patch, capture: :all_but_first)
      |> List.flatten()
      |> Enum.map(&path_category/1)
      |> Enum.uniq()

    case categories do
      [one] -> one
      [] -> :mixed
      _ -> :mixed
    end
  end

  defp actual_fix_shape(_), do: :mixed

  defp path_category(path) do
    down = String.downcase(path)
    basename = Path.basename(down)

    cond do
      Regex.match?(~r/(^|\/)(test|tests|spec|specs)(\/|$)/, down) ->
        :tests

      String.ends_with?(down, ".md") or String.starts_with?(down, "docs/") ->
        :documentation

      String.starts_with?(down, ".github/") ->
        :configuration_build

      basename in [
        "mix.exs",
        "mix.lock",
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "package.json",
        "package-lock.json",
        "tox.ini",
        "dockerfile"
      ] ->
        :configuration_build

      String.ends_with?(down, [".yml", ".yaml", ".toml", ".ini", ".cfg"]) ->
        :configuration_build

      true ->
        :source_code
    end
  end

  defp percentage(_part, 0), do: "n/a"

  defp percentage(part, whole) do
    :io_lib.format("~.1f%", [Metrics.pct(part, whole)]) |> IO.iodata_to_binary()
  end
end

{opts, _, _} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [data_dir: :string, limit: :integer, token_budget: :integer, timeout_ms: :integer]
  )

Plexus.Examples.IssueSwarm.run(opts)
