defmodule SpanStream.MixProject do
  use Mix.Project

  def project do
    [
      app: :span_stream,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Embedded OpenTelemetry span storage and compression for Elixir applications.",
      source_url: "https://github.com/awksedgreep/span_stream",
      homepage_url: "https://github.com/awksedgreep/span_stream",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {SpanStream.Application, []}
    ]
  end

  defp deps do
    [
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Matt Cotner"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/awksedgreep/span_stream"},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"]
    ]
  end
end
