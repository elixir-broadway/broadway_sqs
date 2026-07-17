defmodule BroadwaySQSExample.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_sqs_example,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {BroadwaySQSExample.Application, []}
    ]
  end

  defp deps do
    [
      {:broadway_sqs, path: "../.."},
      {:hackney, "~> 4.0"},
      {:httpoison, "~> 0.13.0"}
    ]
  end
end
