defmodule Plexus.Population.Operators do
  @moduledoc false
  alias Plexus.{Graph, Record, Run}

  def execute(run_id, {:replicate, source, target, opts}) do
    with {:ok, attrs} <- fetch(run_id, source) do
      birth(run_id, attrs, target, Keyword.get(opts, :init_arg, attrs.init_arg), opts)
    end
  end

  def execute(run_id, {:split, source, children}) do
    with {:ok, attrs} <- leaf(run_id, source),
         :ok <- validate_targets(children, [source]) do
      case birth_all(run_id, attrs, children, []) do
        {:ok, ids} ->
          Run.prune(run_id, source)
          Record.append(run_id, :population_split, %{source: source, children: ids})
          :ok

        error ->
          error
      end
    end
  end

  def execute(run_id, {:merge, sources, target, init_arg}) when sources != [] do
    with {:ok, attrs} <- merge_sources(run_id, sources),
         :ok <- validate_targets([{target, init_arg}], sources),
         {:ok, _pid} <- birth(run_id, attrs, target, init_arg, []) do
      Enum.each(sources, &Run.prune(run_id, &1))
      Record.append(run_id, :population_merge, %{sources: sources, target: target})
      :ok
    end
  end

  def execute(run_id, {:migrate, source, destination, init_arg}) do
    destination = Run.run_id(destination)

    with :ok <- different_run(run_id, destination),
         {:ok, attrs} <- leaf(run_id, source),
         {:ok, _pid} <- birth(destination, attrs, source, init_arg, parent_id: nil) do
      Run.prune(run_id, source)
      Record.append(destination, :population_migration, %{actor_id: source, source_run: run_id})
      :ok
    end
  end

  def execute(_run_id, _), do: {:error, :invalid_population_operation}

  defp birth(run_id, attrs, id, init_arg, opts) do
    options =
      [
        module: attrs.module,
        actor_id: id,
        class: attrs.class,
        parent_id: attrs.parent,
        metadata: attrs.metadata,
        init_arg: init_arg
      ]
      |> Keyword.merge(opts)

    Run.start_actor(run_id, options)
  end

  defp birth_all(_run_id, _attrs, [], born), do: {:ok, Enum.reverse(born)}

  defp birth_all(run_id, attrs, [{id, init_arg} | rest], born) do
    case birth(run_id, attrs, id, init_arg, []) do
      {:ok, _} ->
        birth_all(run_id, attrs, rest, [id | born])

      error ->
        Enum.each(born, &Run.prune(run_id, &1))
        error
    end
  end

  defp merge_sources(run_id, sources) do
    results = Enum.map(sources, &leaf(run_id, &1))

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> compatible_sources(results)
      error -> error
    end
  end

  defp compatible_sources([{:ok, first} | rest]) do
    if Enum.all?(rest, fn {:ok, attrs} ->
         {attrs.module, attrs.class, attrs.parent} == {first.module, first.class, first.parent}
       end), do: {:ok, first}, else: {:error, :incompatible_sources}
  end

  defp leaf(run_id, id) do
    with {:ok, attrs} <- fetch(run_id, id) do
      if Graph.children(run_id, id) == [], do: {:ok, attrs}, else: {:error, :requires_leaf}
    end
  end

  defp fetch(run_id, id) do
    case Graph.get(run_id, id) do
      %{module: _, init_arg: _} = attrs -> {:ok, attrs}
      _ -> {:error, :not_found}
    end
  end

  defp validate_targets(children, sources) do
    ids = Enum.map(children, &elem(&1, 0))

    if children != [] and length(Enum.uniq(ids)) == length(ids) and
         Enum.all?(children, fn {id, arg} -> id not in sources and is_map(arg) end),
       do: :ok,
       else: {:error, :invalid_targets}
  end

  defp different_run(id, id), do: {:error, :same_run}
  defp different_run(_, _), do: :ok
end
