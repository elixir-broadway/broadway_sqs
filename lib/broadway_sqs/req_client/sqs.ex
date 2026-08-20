defmodule BroadwaySQS.ReqClient.SQS do
  @moduledoc false

  alias BroadwaySQS.ReqClient.Request

  def receive_message(queue_url, options, request_options) do
    payload =
      %{
        "QueueUrl" => queue_url,
        "MaxNumberOfMessages" => options.max_number_of_messages
      }
      |> put_if_present("WaitTimeSeconds", options[:wait_time_seconds])
      |> put_if_present("VisibilityTimeout", options[:visibility_timeout])
      |> put_if_present("AttributeNames", attribute_names(options[:attribute_names]))
      |> put_if_present("MessageAttributeNames", options[:message_attribute_names])

    Request.call("AmazonSQS.ReceiveMessage", payload, request_options)
  end

  def delete_message_batch(queue_url, entries, request_options) do
    Request.call(
      "AmazonSQS.DeleteMessageBatch",
      %{"QueueUrl" => queue_url, "Entries" => entries},
      request_options
    )
  end

  def change_message_visibility_batch(queue_url, entries, request_options) do
    Request.call(
      "AmazonSQS.ChangeMessageVisibilityBatch",
      %{"QueueUrl" => queue_url, "Entries" => entries},
      request_options
    )
  end

  def normalize_message(message) do
    %{
      data: message["Body"],
      metadata: message_metadata(message),
      receipt: %{
        id: message["MessageId"],
        receipt_handle: message["ReceiptHandle"]
      }
    }
  end

  defp put_if_present(payload, _key, nil), do: payload
  defp put_if_present(payload, key, value), do: Map.put(payload, key, value)

  defp attribute_names(nil), do: nil
  defp attribute_names(:all), do: ["All"]
  defp attribute_names(names), do: Enum.map(names, &attribute_name/1)

  defp attribute_name(:sender_id), do: "SenderId"
  defp attribute_name(:sent_timestamp), do: "SentTimestamp"
  defp attribute_name(:approximate_receive_count), do: "ApproximateReceiveCount"

  defp attribute_name(:approximate_first_receive_timestamp),
    do: "ApproximateFirstReceiveTimestamp"

  defp attribute_name(:sequence_number), do: "SequenceNumber"
  defp attribute_name(:message_deduplication_id), do: "MessageDeduplicationId"
  defp attribute_name(:message_group_id), do: "MessageGroupId"
  defp attribute_name(:aws_trace_header), do: "AWSTraceHeader"
  defp attribute_name(name) when is_binary(name), do: name

  defp message_metadata(message) do
    %{
      message_id: message["MessageId"],
      receipt_handle: message["ReceiptHandle"],
      md5_of_body: message["MD5OfBody"],
      attributes: convert_attributes(message["Attributes"]),
      message_attributes: convert_message_attributes(message["MessageAttributes"])
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp convert_attributes(nil), do: []

  defp convert_attributes(attributes) when is_map(attributes) do
    Map.new(attributes, fn {name, value} ->
      {metadata_attribute_name(name), parse_integer(value)}
    end)
  end

  defp convert_attributes(attributes), do: attributes

  defp convert_message_attributes(nil), do: []

  defp convert_message_attributes(attributes) when is_map(attributes) do
    Map.new(attributes, fn {name, attribute} ->
      value = attribute["StringValue"] || attribute["BinaryValue"] || ""

      {name,
       %{
         name: name,
         data_type: attribute["DataType"],
         string_value: attribute["StringValue"] || "",
         binary_value: attribute["BinaryValue"] || "",
         value: value
       }}
    end)
  end

  defp convert_message_attributes(attributes), do: attributes

  defp metadata_attribute_name("SenderId"), do: "sender_id"
  defp metadata_attribute_name("SentTimestamp"), do: "sent_timestamp"
  defp metadata_attribute_name("ApproximateReceiveCount"), do: "approximate_receive_count"

  defp metadata_attribute_name("ApproximateFirstReceiveTimestamp"),
    do: "approximate_first_receive_timestamp"

  defp metadata_attribute_name("SequenceNumber"), do: "sequence_number"
  defp metadata_attribute_name("MessageDeduplicationId"), do: "message_deduplication_id"
  defp metadata_attribute_name("MessageGroupId"), do: "message_group_id"
  defp metadata_attribute_name("AWSTraceHeader"), do: "aws_trace_header"
  defp metadata_attribute_name(name), do: name

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> value
    end
  end

  defp parse_integer(value), do: value
end
