if Code.ensure_loaded?(AWS) do
  defmodule BroadwaySQS.AwsElixirClient do
    @moduledoc """
    Alternative SQS client used by `BroadwaySQS.Producer` to communicate with AWS
    SQS service.

    This client uses the `AWS.SQS` library and implements the
    `BroadwaySQS.SQSClient` and `Broadway.Acknowledger` behaviours which define
    callbacks for receiving and acknowledging messages.
    """

    alias Broadway.{Message, Acknowledger}
    require Logger

    @behaviour BroadwaySQS.SQSClient
    @behaviour Acknowledger

    @max_num_messages_allowed_by_aws 10

    @aws_sqs_client_args [:access_key_id, :secret_access_key, :token, :region]

    @impl true
    def init(opts) do
      opts_map = opts |> Enum.into(%{ack_ref: opts[:broadway][:name]})

      {:ok, opts_map}
    end

    @impl true
    def receive_messages(demand, opts) do
      receive_messages_request =
        build_receive_messages_opts(opts, demand)

      opts.config
      |> build_client()
      |> AWS.SQS.receive_message(receive_messages_request)
      |> wrap_received_messages(opts)
    end

    @impl Acknowledger
    def ack(ack_ref, successful, failed) do
      ack_options = :persistent_term.get(ack_ref)

      messages =
        Enum.filter(successful, &ack?(&1, ack_options, :on_success)) ++
          Enum.filter(failed, &ack?(&1, ack_options, :on_failure))

      messages
      |> Enum.chunk_every(@max_num_messages_allowed_by_aws)
      |> Enum.each(fn messages -> delete_messages(messages, ack_options) end)
    end

    defp ack?(message, ack_options, option) do
      {_, _, message_ack_options} = message.acknowledger
      (message_ack_options[option] || Map.fetch!(ack_options, option)) == :ack
    end

    @impl Acknowledger
    def configure(_ack_ref, ack_data, options) do
      {:ok, Map.merge(ack_data, Map.new(options))}
    end

    defp delete_messages(messages, ack_options) do
      delete_message_batch_request =
        build_delete_message_opts(ack_options, messages)

      ack_options.config
      |> build_client()
      |> AWS.SQS.delete_message_batch(delete_message_batch_request)
    end

    defp wrap_received_messages({:ok, %{"Messages" => raw_messages}, _}, %{ack_ref: ack_ref}) do
      raw_messages
      |> Enum.map(&build_message/1)
      |> Enum.map(fn message ->
        metadata = Map.delete(message, :body)
        acknowledger = build_acknowledger(message, ack_ref)
        %Message{data: message.body, metadata: metadata, acknowledger: acknowledger}
      end)
    end

    defp wrap_received_messages({:ok, %{}, _}, %{ack_ref: _ack_ref}), do: []

    defp wrap_received_messages(
           {:error,
            {:unexpected_response, %{body: body, headers: _headers, status_code: _status_code}}},
           %{queue_url: queue_url}
         ) do
      decoded = Jason.decode!(body)
      # The documentation specifies that this key will be `message`, but I have
      # observed `Message` in error responses. I am unsure if the documentation
      # is incorrect, or the JSON API response occasionally does not conform to
      # the specification.
      reason = decoded["message"] || decoded["Message"]

      Logger.error(
        "Unable to fetch events from AWS queue #{queue_url}. Reason: #{inspect(reason)}"
      )

      []
    end

    defp wrap_received_messages({:error, reason}, %{queue_url: queue_url}) do
      Logger.error(
        "Unable to fetch events from AWS queue #{queue_url}. Reason: #{inspect(reason)}"
      )

      []
    end

    defp build_acknowledger(message, ack_ref) do
      receipt = %{id: message.message_id, receipt_handle: message.receipt_handle}
      {__MODULE__, ack_ref, %{receipt: receipt}}
    end

    defp build_receive_messages_opts(opts, demand) do
      max_number_of_messages = min(demand, opts[:max_number_of_messages])

      %{
        "QueueUrl" => opts.queue_url,
        "MaxNumberOfMessages" => max_number_of_messages,
        "WaitTimeSeconds" => opts[:wait_time_seconds],
        "VisibilityTimeout" => opts[:visibility_timeout],
        "AttributeNames" => opts[:attribute_names],
        "MessageAttributeNames" => opts[:message_attribute_names]
      }
      |> Enum.filter(fn {_, value} -> value end)
      |> Enum.into(%{})
    end

    defp extract_message_receipt(message) do
      {_, _, %{receipt: receipt}} = message.acknowledger
      receipt
    end

    defp build_message(raw_message) do
      %{
        attributes: raw_message["Attributes"],
        body: raw_message["Body"],
        md5_of_body: raw_message["MD5OfBody"],
        md5_of_message_attributes: raw_message["MD5OfMessageAttributes"],
        message_attributes: raw_message["MessageAttributes"],
        message_id: raw_message["MessageId"],
        receipt_handle: raw_message["ReceiptHandle"]
      }
      |> Enum.filter(fn {_, value} -> value end)
      |> Enum.into(%{})
    end

    defp build_delete_message_opts(opts, messages) do
      receipts =
        messages
        |> Enum.map(&extract_message_receipt/1)
        |> Enum.map(&build_delete_message_batch_request_entry/1)

      %{"QueueUrl" => opts.queue_url, "Entries" => receipts}
    end

    defp build_delete_message_batch_request_entry(receipt) do
      %{"Id" => receipt.id, "ReceiptHandle" => receipt.receipt_handle}
    end

    defp build_client(opts) do
      options =
        if opts[:use_aws_credentials_providers] do
          Keyword.merge(get_sanitized_aws_credentials(), opts)
        else
          opts
        end

      client =
        AWS.Client.create(
          options[:access_key_id],
          options[:secret_access_key],
          options[:token],
          options[:region]
        )
        |> put_endpoint(options[:endpoint])
        |> put_http_client(options[:http_client])

      options
      |> Keyword.drop(@aws_sqs_client_args ++ [:endpoint, :http_client])
      |> Enum.reduce(client, fn {k, v}, client -> Map.put(client, k, v) end)
    end

    defp put_endpoint(client, nil), do: client
    defp put_endpoint(client, endpoint), do: AWS.Client.put_endpoint(client, endpoint)

    defp put_http_client(client, nil), do: client
    defp put_http_client(client, http_client), do: AWS.Client.put_http_client(client, http_client)

    defp get_sanitized_aws_credentials() do
      creds = :aws_credentials.get_credentials()

      # Rename the token field, and sanitize the output.
      creds
      |> Map.put(:session_token, creds[:token])
      |> Map.take(@aws_sqs_client_args)
      |> Enum.into(Keyword.new())
    end
  end
end
