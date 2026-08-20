defmodule BroadwaySQS.ReqClient.Request do
  @moduledoc false

  @content_type "application/x-amz-json-1.0"

  @type options :: [
          credentials: map() | keyword(),
          endpoint: String.t(),
          plug: term(),
          region: String.t(),
          queue_url: String.t()
        ]

  @doc """
  Sends one JSON SQS API request.

  The request is signed with AWS Signature Version 4. Credentials can be
  supplied in `opts` or are loaded from `aws_credentials`.
  """
  @spec call(String.t(), map(), options()) :: {:ok, map()} | {:error, term()}
  def call(action, payload, opts \\ []) when is_binary(action) and is_map(payload) do
    with {:ok, queue_url} <- queue_url(payload, opts),
         {:ok, credentials} <- credentials(opts),
         {:ok, region} <- region(opts, credentials),
         req = request(opts, credentials, region, queue_url),
         {:ok, response} <-
           Req.post(req,
             headers: [{"x-amz-target", action}],
             json: payload,
             decode_body: false
           ),
         {:ok, body} <- decode_body(response) do
      if response.status in 200..299 do
        {:ok, body}
      else
        {:error, {:http_error, response.status, body}}
      end
    end
  end

  defp request(opts, credentials, region, queue_url) do
    headers = [
      {"content-type", @content_type}
    ]

    headers =
      case credentials[:token] do
        nil -> headers
        token -> [{"x-amz-security-token", token} | headers]
      end

    Req.new(
      url: Keyword.get(opts, :endpoint, queue_url),
      plug: Keyword.get(opts, :plug),
      headers: headers,
      aws_sigv4: [
        access_key_id: credentials[:access_key_id],
        secret_access_key: credentials[:secret_access_key],
        region: region,
        service: :sqs
      ]
    )
  end

  defp credentials(opts) do
    credentials =
      case Keyword.fetch(opts, :credentials) do
        {:ok, credentials} -> credentials
        :error -> aws_credentials()
      end

    credentials = normalize_credentials(credentials)

    if credentials[:access_key_id] && credentials[:secret_access_key] do
      {:ok, credentials}
    else
      {:error, :aws_credentials_not_found}
    end
  end

  defp aws_credentials do
    case :aws_credentials.get_credentials() do
      credentials when is_map(credentials) -> credentials
      _ -> %{}
    end
  end

  defp normalize_credentials(credentials) when is_list(credentials), do: Map.new(credentials)
  defp normalize_credentials(credentials) when is_map(credentials), do: credentials
  defp normalize_credentials(_credentials), do: %{}

  defp queue_url(payload, opts) do
    case Keyword.get(opts, :queue_url, payload["QueueUrl"]) do
      queue_url when is_binary(queue_url) and queue_url != "" -> {:ok, queue_url}
      _ -> {:error, :queue_url_not_found}
    end
  end

  defp region(opts, credentials) do
    case Keyword.get(opts, :region) || credentials[:region] do
      region when is_binary(region) and region != "" -> {:ok, region}
      _ -> {:error, :aws_region_not_found}
    end
  end

  defp decode_body(%{body: body}) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, {:invalid_json, reason, body}}
    end
  end

  defp decode_body(%{body: body}), do: {:error, {:invalid_body, body}}
end
