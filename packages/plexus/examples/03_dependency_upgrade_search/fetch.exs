Code.require_file("../support/http.exs", __DIR__)
Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.{HTTP, Runtime}

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [
      data_dir: :string,
      system: :string,
      package: :string,
      from: :string,
      to: :string,
      metadata_limit: :integer
    ]
  )

system = String.upcase(opts[:system] || "NPM")
package = opts[:package] || "eslint"
from_version = opts[:from] || "8.57.0"
to_version = opts[:to] || "9.35.0"
metadata_limit = opts[:metadata_limit] || 100

slug =
  "deps-dev-#{String.downcase(system)}-#{package |> String.replace(~r/[^A-Za-z0-9_.-]+/, "-")}"

data_dir = opts[:data_dir] || Runtime.data_dir(slug)
File.mkdir_p!(data_dir)

base =
  "https://api.deps.dev/v3/systems/#{HTTP.encode_path(system)}/packages/#{HTTP.encode_path(package)}/versions"

from_graph = HTTP.get_json!("#{base}/#{HTTP.encode_path(from_version)}:dependencies")
to_graph = HTTP.get_json!("#{base}/#{HTTP.encode_path(to_version)}:dependencies")

File.write!(Path.join(data_dir, "from_graph.json"), Jason.encode!(from_graph, pretty: true))
File.write!(Path.join(data_dir, "to_graph.json"), Jason.encode!(to_graph, pretty: true))

version_tuple = fn key -> {key["system"], key["name"], key["version"]} end
metadata_key = fn key -> Enum.join([key["system"], key["name"], key["version"]], "\u001F") end

from_keys =
  from_graph["nodes"]
  |> Enum.map(fn node -> version_tuple.(node["versionKey"]) end)
  |> MapSet.new()

changed =
  to_graph["nodes"]
  |> Enum.map(fn node -> node["versionKey"] end)
  |> Enum.reject(&MapSet.member?(from_keys, version_tuple.(&1)))
  |> Enum.uniq_by(version_tuple)
  |> Enum.take(metadata_limit)

metadata =
  Map.new(changed, fn key ->
    url =
      "https://api.deps.dev/v3/systems/#{HTTP.encode_path(key["system"])}/packages/#{HTTP.encode_path(key["name"])}/versions/#{HTTP.encode_path(key["version"])}"

    {metadata_key.(key), HTTP.get_json!(url)}
  end)

File.write!(Path.join(data_dir, "metadata.json"), Jason.encode!(metadata, pretty: true))

File.write!(
  Path.join(data_dir, "scenario.json"),
  Jason.encode!(
    %{
      system: system,
      package: package,
      from: from_version,
      to: to_version,
      metadata_limit: metadata_limit
    },
    pretty: true
  )
)

IO.puts(
  "Saved deps.dev upgrade scenario #{package} #{from_version} -> #{to_version} to #{data_dir}"
)

IO.puts(
  "Target graph nodes: #{length(to_graph["nodes"])}; metadata fetched for #{map_size(metadata)} changed packages"
)
