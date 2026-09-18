defmodule Plexus.Record.File do
  @moduledoc """
  Versioned, checksummed fixed-response replay files.

  The JSON envelope holds an uncompressed ETF payload decoded with `:safe`;
  only inert scalars, lists, tuples and maps are accepted. PIDs, references,
  ports and functions are rejected. Existing atoms only: importing a contract
  with application atoms requires loading that application's modules first.
  Files are capped at 64 MiB and nesting at 128 levels. SHA-256 detects damage,
  not malicious replacement; files should come from a trusted experiment.

  Manifests freeze engine/kernel versions, registered contract fingerprints and
  admission limits. Scheduling and replay mode are intentionally excluded.
  Use `replay_identity` at run startup to freeze strategy, dataset and evaluation
  options for a scientific comparison. Import is all-or-nothing after validation.
  """
  alias Plexus.Record
  alias Plexus.Run.Config

  @max_bytes 64 * 1024 * 1024

  @spec write(term(), Path.t()) :: :ok | {:error, term()}
  def write(run_id, path) do
    payload = %{manifest: manifest(run_id), entries: Record.replay_entries(run_id)}

    if safe?(payload, 0) do
      bytes = :erlang.term_to_binary(payload, [:deterministic])

      envelope =
        Jason.encode!(%{version: 1, sha256: checksum(bytes), payload: Base.encode64(bytes)})

      if byte_size(envelope) <= @max_bytes,
        do: atomic_write(path, envelope),
        else: {:error, :file_too_large}
    else
      {:error, :unsafe_replay_term}
    end
  end

  @spec load(term(), Path.t()) :: :ok | {:error, term()}
  def load(run_id, path) do
    with {:ok, stat} <- File.stat(path),
         :ok <- check(stat.size <= @max_bytes, :file_too_large),
         {:ok, json} <- File.read(path),
         {:ok, envelope} <- Jason.decode(json),
         {:ok, payload} <- decode(envelope),
         :ok <- check(payload.manifest == manifest(run_id), :incompatible_manifest),
         :ok <- validate_entries(payload.entries) do
      Record.load_replay(run_id, payload.entries)
    end
  rescue
    _ in [ArgumentError, KeyError, BadMapError] -> {:error, :invalid_replay_file}
  end

  @spec manifest(term()) :: map()
  def manifest(run_id) do
    config = Config.fetch!(run_id)

    contracts =
      config.tables.contracts
      |> :ets.tab2list()
      |> Enum.flat_map(fn
        {_key, %{name: name, version: version, fingerprint: fingerprint}} ->
          [{name, version, fingerprint}]

        _ ->
          []
      end)
      |> Enum.sort()

    %{
      kernel: to_string(Application.spec(:plexus, :vsn)),
      engine: to_string(Application.spec(:typesafe_sdk, :vsn)),
      contracts: contracts,
      config:
        checksum(
          :erlang.term_to_binary(
            {config.max_depth, config.max_population, config.budget.limits,
             config.replay_identity},
            [:deterministic]
          )
        )
    }
  end

  defp decode(%{"version" => 1, "sha256" => expected, "payload" => encoded}) do
    with {:ok, bytes} <- Base.decode64(encoded),
         :ok <- check(checksum(bytes) == expected, :checksum_mismatch),
         :ok <- check(not match?(<<131, 80, _::binary>>, bytes), :compressed_payload_forbidden) do
      {payload, used} = :erlang.binary_to_term(bytes, [:safe, :used])

      if used == byte_size(bytes) and safe?(payload, 0),
        do: {:ok, payload},
        else: {:error, :unsafe_replay_term}
    else
      :error -> {:error, :invalid_payload}
      error -> error
    end
  end

  defp decode(_), do: {:error, :unsupported_replay_format}

  defp validate_entries(entries) when is_list(entries) do
    valid =
      Enum.all?(entries, fn
        {key, {status, _}} when is_binary(key) and status in [:ok, :error] -> true
        _ -> false
      end)

    check(valid, :invalid_replay_entries)
  end

  defp validate_entries(_), do: {:error, :invalid_replay_entries}

  defp safe?(_, depth) when depth > 128, do: false
  defp safe?(value, _) when is_atom(value) or is_number(value) or is_binary(value), do: true
  defp safe?(value, depth) when is_list(value), do: Enum.all?(value, &safe?(&1, depth + 1))
  defp safe?(value, depth) when is_tuple(value), do: safe?(Tuple.to_list(value), depth + 1)
  defp safe?(value, depth) when is_map(value), do: safe?(Map.to_list(value), depth + 1)
  defp safe?(_, _), do: false

  defp check(true, _), do: :ok
  defp check(false, reason), do: {:error, reason}
  defp checksum(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp atomic_write(path, bytes) do
    temporary = path <> ".#{System.unique_integer([:positive])}.tmp"

    with :ok <- File.write(temporary, bytes, [:exclusive]), do: File.rename(temporary, path)
  end
end
