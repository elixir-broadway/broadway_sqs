defmodule BroadwaySQS.ExAwsClient do
  @moduledoc """
  Default SQS client used by `BroadwaySQS.Producer` to communicate with AWS
  SQS service.

  This client uses the `ExAws.SQS` library and implements the
  `BroadwaySQS.SQSClient` and `Broadway.Acknowledger` behaviours which define
  callbacks for receiving and acknowledging messages.
  """

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour BroadwaySQS.SQSClient
  @behaviour Acknowledger

  @max_num_messages_allowed_by_aws 10

  @impl true
  def init(opts) do
    opts_map = opts |> Enum.into(%{ack_ref: opts[:broadway][:name]})

    {:ok, opts_map}
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_messages_opts = build_receive_messages_opts(opts, demand)

    opts.queue_url
    |> ExAws.SQS.receive_message(receive_messages_opts)
    |> ExAws.request(opts.config)
    |> wrap_received_messages(opts)
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

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    {:ok, Map.merge(ack_data, Map.new(options))}
  end

  defp delete_messages(messages, ack_options) do
    receipts = Enum.map(messages, &extract_message_receipt/1)

    ack_options.queue_url
    |> ExAws.SQS.delete_message_batch(receipts)
    |> ExAws.request!(ack_options.config)
  end

  defp change_message_visibilities(messages_with_timeouts, ack_options) do
    entries =
      Enum.map(messages_with_timeouts, fn {message, timeout} ->
        message
        |> extract_message_receipt()
        |> Map.put(:visibility_timeout, timeout)
      end)

    ack_options.queue_url
    |> ExAws.SQS.change_message_visibility_batch(entries)
    |> ExAws.request!(ack_options.config)
  end

  defp wrap_received_messages({:ok, %{body: body}}, %{ack_ref: ack_ref}) do
    Enum.map(body.messages, fn message ->
      metadata = Map.delete(message, :body)
      acknowledger = build_acknowledger(message, ack_ref)
      %Message{data: message.body, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp wrap_received_messages({:error, reason}, %{queue_url: queue_url}) do
    Logger.error("Unable to fetch events from AWS queue #{queue_url}. Reason: #{inspect(reason)}")
    []
  end

  defp build_acknowledger(message, ack_ref) do
    receipt = %{id: message.message_id, receipt_handle: message.receipt_handle}
    {__MODULE__, ack_ref, %{receipt: receipt}}
  end

  defp build_receive_messages_opts(opts, demand) do
    max_number_of_messages = min(demand, opts[:max_number_of_messages])

    [
      max_number_of_messages: max_number_of_messages,
      wait_time_seconds: opts[:wait_time_seconds],
      visibility_timeout: opts[:visibility_timeout],
      attribute_names: opts[:attribute_names],
      message_attribute_names: opts[:message_attribute_names]
    ]
    |> Enum.filter(fn {_, value} -> value end)
  end

  defp extract_message_receipt(message) do
    {_, _, %{receipt: receipt}} = message.acknowledger
    receipt
  end
end
