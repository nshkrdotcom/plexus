defmodule Plexus.Contract.Registry do
  @moduledoc """
  Named, versioned per-run prepared contracts.
  """

  alias Plexus.Contract
  alias Plexus.Run.Config
  alias TypeSafeSDK.Prepared

  @spec put(term(), term(), Prepared.t() | keyword(), keyword()) :: :ok
  def put(run_id, name, prepared_or_questions, opts \\ []) do
    config = Config.fetch!(run_id)
    prepared = Prepared.new!(prepared_or_questions)
    version = Keyword.get(opts, :version, 1)
    batch = Keyword.get(opts, :batch, [])

    entry = %{
      name: name,
      version: version,
      prepared: prepared,
      fingerprint: Contract.fingerprint(prepared),
      batch: batch
    }

    :ets.insert(config.tables.contracts, {{name, version}, entry})
    :ets.insert(config.tables.contracts, {{:latest, name}, version})
    :ok
  end

  @spec fetch(term(), term(), term()) :: {:ok, map()} | {:error, :not_found}
  def fetch(run_id, name, version \\ :latest) do
    config = Config.fetch!(run_id)

    version =
      case version do
        :latest ->
          case :ets.lookup(config.tables.contracts, {:latest, name}) do
            [{{:latest, ^name}, value}] -> value
            [] -> nil
          end

        value ->
          value
      end

    case version && :ets.lookup(config.tables.contracts, {name, version}) do
      [{{^name, ^version}, entry}] -> {:ok, entry}
      _ -> {:error, :not_found}
    end
  end

  @spec resolve(term(), term()) :: {:ok, map()} | {:error, :not_found}
  def resolve(_run_id, %Prepared{} = prepared) do
    {:ok, %{prepared: prepared, fingerprint: Contract.fingerprint(prepared), batch: []}}
  end

  def resolve(_run_id, questions) when is_list(questions) do
    prepared = Prepared.new!(questions)
    {:ok, %{prepared: prepared, fingerprint: Contract.fingerprint(prepared), batch: []}}
  end

  def resolve(run_id, {name, version}), do: fetch(run_id, name, version)
  def resolve(run_id, name), do: fetch(run_id, name, :latest)
end
