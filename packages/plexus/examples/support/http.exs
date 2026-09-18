defmodule Plexus.Examples.Support.HTTP do
  @moduledoc false

  def get_json!(url, headers \\ []) do
    url |> get!(headers) |> Jason.decode!()
  end

  def post_json!(url, body, headers \\ []) do
    ensure_http!()
    payload = Jason.encode!(body)
    request_headers = [{~c"content-type", ~c"application/json"} | normalize_headers(headers)]

    request = {String.to_charlist(url), request_headers, ~c"application/json", payload}

    case :httpc.request(:post, request, http_opts(), body_format: :binary) do
      {:ok, {{_, status, _}, _response_headers, response}} when status in 200..299 ->
        Jason.decode!(response)

      {:ok, {{_, status, _}, _response_headers, response}} ->
        raise "HTTP #{status} for #{url}: #{String.slice(response, 0, 1_000)}"

      {:error, reason} ->
        raise "HTTP request failed for #{url}: #{inspect(reason)}"
    end
  end

  def get!(url, headers \\ []) do
    ensure_http!()
    request = {String.to_charlist(url), normalize_headers(headers)}

    case :httpc.request(:get, request, http_opts(), body_format: :binary) do
      {:ok, {{_, status, _}, _response_headers, response}} when status in 200..299 ->
        response

      {:ok, {{_, status, _}, _response_headers, response}} ->
        raise "HTTP #{status} for #{url}: #{String.slice(response, 0, 1_000)}"

      {:error, reason} ->
        raise "HTTP request failed for #{url}: #{inspect(reason)}"
    end
  end

  def download!(url, path) do
    File.mkdir_p!(Path.dirname(path))

    case System.find_executable("curl") do
      nil ->
        File.write!(path, get!(url))

      curl ->
        {_, status} =
          System.cmd(curl, ["-fL", "--retry", "3", "--continue-at", "-", "-o", path, url],
            stderr_to_stdout: true,
            into: IO.stream(:stdio, :line)
          )

        if status != 0, do: raise("curl failed downloading #{url}")
    end

    path
  end

  def encode_path(value),
    do: value |> to_string() |> URI.encode_www_form() |> String.replace("+", "%20")

  def query(url, params) do
    separator = if String.contains?(url, "?"), do: "&", else: "?"
    url <> separator <> URI.encode_query(params)
  end

  defp ensure_http! do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end

  defp http_opts, do: [autoredirect: true, timeout: 60_000, connect_timeout: 15_000]

  defp normalize_headers(headers) do
    Enum.map(headers, fn {key, value} ->
      {String.to_charlist(to_string(key)), String.to_charlist(to_string(value))}
    end)
  end
end
