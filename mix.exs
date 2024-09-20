defmodule BroadwaySqs.MixProject do
  use Mix.Project

  @version "0.7.4"
  @description "A SQS connector for Broadway"

  def project do
    [
      app: :broadway_sqs,
      version: @version,
      elixir: "~> 1.7",
      name: "BroadwaySQS",
      description: @description,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:ex_aws_sqs, "~> 3.2.1 or ~> 3.3", optional: true},
      {:aws_credentials, "~> 0.3", optional: true},
      {:aws, "~> 1.0", optional: true},
      {:nimble_options, "~> 0.3.7 or ~> 0.4 or ~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},
      {:saxy, "~> 1.1", optional: true},
      {:hackney, "~> 1.9", only: [:dev, :test]},
      {:bypass, "~> 2.1.0", only: :test},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwaySQS.Producer",
      source_ref: "v#{@version}",
      source_url: "https://github.com/dashbitco/broadway_sqs"
    ]
  end

  defp package do
    %{
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/dashbitco/broadway_sqs"}
    }
  end
end
