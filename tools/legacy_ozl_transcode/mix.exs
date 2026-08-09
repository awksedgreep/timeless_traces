defmodule LegacyOzlTranscode.MixProject do
  use Mix.Project

  # Standalone on purpose. It pins ex_openzl 0.4.6, which is the last release
  # whose vendored OpenZL (0.1.x) can still decode blocks written before
  # ex_openzl 0.4.7. The parent application pins 0.4.16 and cannot load both,
  # so this must never become a dependency of timeless_traces.
  #
  # The dependency is taken from git with submodules because the Hex package
  # does not ship the vendored OpenZL source the NIF needs to build from
  # source.

  def project do
    [
      app: :legacy_ozl_transcode,
      version: "0.1.0",
      elixir: "~> 1.15",
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:ex_openzl, github: "awksedgreep/ex_openzl", tag: "v0.4.6", submodules: true}
    ]
  end
end
