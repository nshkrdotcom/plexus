Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.Runtime

{opts, _, _} = OptionParser.parse(System.argv(), strict: [data_dir: :string])
data_dir = opts[:data_dir] || Runtime.data_dir("gaia")
repo = Path.join(data_dir, "GAIA-DataSet")
micross = Path.join(repo, "MicroSS")
File.mkdir_p!(data_dir)

git = System.find_executable("git") || raise "git is required to acquire the GAIA dataset"

seven_zip =
  System.find_executable("7z") || System.find_executable("7zz") ||
    raise "7z/7zz is required to extract GAIA MicroSS split archives (Ubuntu: apt install p7zip-full)"

unless File.dir?(repo) do
  IO.puts("Cloning the official GAIA release-v1.0 metadata into #{repo}")

  {_, status} =
    System.cmd(
      git,
      [
        "clone",
        "--depth",
        "1",
        "--branch",
        "release-v1.0",
        "https://github.com/CloudWise-OpenSource/GAIA-DataSet.git",
        repo
      ],
      env: [{"GIT_LFS_SKIP_SMUDGE", "1"}],
      stderr_to_stdout: true,
      into: IO.stream(:stdio, :line)
    )

  if status != 0, do: raise("git clone failed")
end

case System.cmd(git, ["-C", repo, "lfs", "version"], stderr_to_stdout: true) do
  {_out, 0} -> :ok
  _ -> raise "git-lfs is required for GAIA's large data files"
end

IO.puts("Fetching GAIA MicroSS LFS objects. The real corpus is large.")

{_, status} =
  System.cmd(
    git,
    ["-C", repo, "lfs", "pull", "--include=MicroSS/**"],
    stderr_to_stdout: true,
    into: IO.stream(:stdio, :line)
  )

if status != 0, do: raise("git lfs pull failed")

archives = [
  Path.join([micross, "business", "business_split.zip"]),
  Path.join([micross, "run", "run.zip"]),
  Path.join([micross, "trace", "trace_split.zip"])
]

Enum.each(archives, fn archive ->
  unless File.exists?(archive), do: raise("expected GAIA archive is missing: #{archive}")

  destination = Path.dirname(archive)
  already_extracted? = Path.wildcard(Path.join(destination, "**/*.csv")) != []

  unless already_extracted? do
    IO.puts("Extracting #{archive}")

    {_, extract_status} =
      System.cmd(
        seven_zip,
        ["x", "-y", Path.basename(archive), "-o#{destination}"],
        cd: destination,
        stderr_to_stdout: true,
        into: IO.stream(:stdio, :line)
      )

    if extract_status != 0, do: raise("7z extraction failed for #{archive}")
  end
end)

csv_count = Path.wildcard(Path.join(micross, "**/*.csv")) |> length()
if csv_count == 0, do: raise("GAIA acquisition completed but no MicroSS CSV files were extracted")

IO.puts("GAIA MicroSS is ready at #{micross} (#{csv_count} CSV files)")
