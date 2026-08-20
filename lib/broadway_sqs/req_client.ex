defmodule BroadwaySQS.ReqClient do
  @moduledoc """
  SQS client backed by `BroadwaySQS.ReqClient.Request`.

  This module adapts the SQS JSON API to the Broadway producer and
  acknowledger behaviours.
  """

  alias Broadway.{Acknowledger, Message}
  alias BroadwaySQS.ReqClient.SQS
  require Logger

  @behaviour BroadwaySQS.SQSClient
  @behaviour Acknowledger

  @max_num_messages_allowed_by_aws 10

  @impl true
  def init(opts) do
    {:ok, Map.put(Map.new(opts), :ack_ref, opts[:broadway][:name])}
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_options = %{opts | max_number_of_messages: min(demand, opts.max_number_of_messages)}

    case SQS.receive_message(opts.queue_url, receive_options, request_options(opts)) do
      {:ok, %{"Messages" => messages}} ->
        wrap_received_messages(messages, opts.ack_ref)

      {:ok, _response} ->
        []

      {:error, reason} ->
        Logger.error(
          "Unable to fetch events from AWS queue #{opts.queue_url}. Reason: #{inspect(reason)}"
        )

        []
    end
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    ack_options = :persistent_term.get(ack_ref)

    messages_to_delete =
      Enum.filter(successful, &ack?(&1, ack_options, :on_success)) ++
        Enum.filter(failed, &ack?(&1, ack_options, :on_failure))

    messages_to_nack_with_timeout =
      Enum.flat_map(successful, &nack(&1, ack_options, :on_success)) ++
        Enum.flat_map(failed, &nack(&1, ack_options, :on_failure))

    messages_to_delete
    |> Enum.chunk_every(@max_num_messages_allowed_by_aws)
    |> Enum.each(&delete_messages(&1, ack_options))

    messages_to_nack_with_timeout
    |> Enum.chunk_every(@max_num_messages_allowed_by_aws)
    |> Enum.each(&change_message_visibilities(&1, ack_options))
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    {:ok, Map.merge(ack_data, Map.new(options))}
  end

  defp ack?(message, ack_options, option) do
    {_, _, message_ack_options} = message.acknowledger
    (message_ack_options[option] || Map.fetch!(ack_options, option)) == :ack
  end

  defp nack(message, ack_options, option) do
    {_, _, message_ack_options} = message.acknowledger

    case message_ack_options[option] || Map.fetch!(ack_options, option) do
      {:nack, timeout} -> [{message, timeout}]
      _ -> []
    end
  end

  defp delete_messages(messages, opts) do
    entries = Enum.map(messages, &delete_entry/1)

    request!(
      SQS.delete_message_batch(opts.queue_url, entries, request_options(opts)),
      :delete_message_batch
    )
  end

  defp change_message_visibilities(messages_with_timeouts, opts) do
    entries =
      Enum.map(messages_with_timeouts, fn {message, timeout} ->
        message
        |> receipt()
        |> Map.put("VisibilityTimeout", timeout)
      end)

    request!(
      SQS.change_message_visibility_batch(opts.queue_url, entries, request_options(opts)),
      :change_message_visibility_batch
    )
  end

  defp request!({:ok, response}, _function), do: response

  defp request!({:error, reason}, function) do
    raise "SQS request #{function} failed: #{inspect(reason)}"
  end

  defp delete_entry(message) do
    receipt = receipt(message)
    %{"Id" => receipt["Id"], "ReceiptHandle" => receipt["ReceiptHandle"]}
  end

  defp receipt(message) do
    {_, _, %{receipt: receipt}} = message.acknowledger
    %{"Id" => receipt.id, "ReceiptHandle" => receipt.receipt_handle}
  end

  defp wrap_received_messages(messages, ack_ref) do
    Enum.map(messages, fn message ->
      message = SQS.normalize_message(message)

      %Message{
        data: message.data,
        metadata: message.metadata,
        acknowledger: build_acknowledger(message, ack_ref)
      }
    end)
  end

  defp build_acknowledger(message, ack_ref) do
    {__MODULE__, ack_ref, %{receipt: message.receipt}}
  end

  defp request_options(opts) do
    config = option_get(opts, :config, [])
    credentials = credentials_from_config(config)

    []
    |> put_option(:region, option_get(config, :region))
    |> put_option(:credentials, credentials)
    |> put_option(:endpoint, option_get(config, :endpoint))
    |> put_option(:plug, option_get(config, :plug))
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp credentials_from_config(config) do
    credentials = [
      access_key_id: option_get(config, :access_key_id),
      secret_access_key: option_get(config, :secret_access_key),
      token: option_get(config, :token)
    ]

    if credentials[:access_key_id] && credentials[:secret_access_key], do: credentials
  end

  defp put_option(options, _key, nil), do: options
  defp put_option(options, key, value), do: Keyword.put(options, key, value)

  defp option_get(options, key, default \\ nil)

  defp option_get(options, key, default) when is_list(options),
    do: Keyword.get(options, key, default)

  defp option_get(options, key, default) when is_map(options),
    do: Map.get(options, key, default)
end
