Code.require_file("../support/http.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.{Data, HTTP, Runtime}

{opts, _, _} = OptionParser.parse(Runtime.cli_args(), strict: [data_dir: :string])
data_dir = opts[:data_dir] || Runtime.data_dir("scifact")
archive = Path.join(data_dir, "scifact-data.tar.gz")
File.mkdir_p!(data_dir)

HTTP.download!("https://scifact.s3-us-west-2.amazonaws.com/release/latest/data.tar.gz", archive)
Data.extract_tar_gz!(archive, data_dir)

root =
  [Path.join(data_dir, "data"), data_dir]
  |> Enum.find(&File.exists?(Path.join(&1, "corpus.jsonl"))) ||
    raise("SciFact archive extracted but corpus.jsonl was not found")

IO.puts("SciFact corpus available at #{root}")
