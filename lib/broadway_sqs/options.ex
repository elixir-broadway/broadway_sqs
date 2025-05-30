defmodule BroadwaySQS.Options do
  @moduledoc false

  def definition() do
    [
      queue_url: [
        required: true,
        type: {
          :custom,
          __MODULE__,
          :type_non_empty_string,
          [[{:name, :queue_url}]]
        },
        doc: """
        The url for the SQS queue. *Note this does not have to be a
        regional endpoint*. For example, `https://sqs.amazonaws.com/0000000000/my_queue`.
        """
      ],
      sqs_client: [
        doc: """
        A module that implements the `BroadwaySQS.SQSClient`
        behaviour. This module is responsible for fetching and acknowledging the
        messages. Pay attention that all options passed to the producer will be forwarded
        to the client.
        """,
        default: BroadwaySQS.ExAwsClient
      ],
      receive_interval: [
        type: :non_neg_integer,
        doc: """
        The duration (in milliseconds) for which the producer
        waits before making a request for more messages.
        """,
        default: 5000
      ],
      on_success: [
        type: {:custom, __MODULE__, :type_ack_action, [[{:name, :on_success}]]},
        default: :ack,
        doc: """
        configures the acking behaviour for successful messages. See the
        "Acknowledgments" section below for all the possible values.
        """
      ],
      on_failure: [
        type: {:custom, __MODULE__, :type_ack_action, [[{:name, :on_failure}]]},
        default: :noop,
        doc: """
        configures the acking behaviour for failed messages. See the
        "Acknowledgments" section below for all the possible values.
        """
      ],
      config: [
        type: :keyword_list,
        default: [],
        doc: """
        A set of options that overrides the default ExAws configuration
        options. The most commonly used options are: `:access_key_id`, `:secret_access_key`,
        `:scheme`, `:region` and `:port`. For a complete list of configuration options and
        their default values, please see the `ExAws` documentation.
        """
      ],
      max_number_of_messages: [
        type: {
          :custom,
          __MODULE__,
          :type_bounded_integer,
          [[{:name, :max_number_of_messages}, {:min, 1}, {:max, 10}]]
        },
        default: 10,
        doc: """
        The maximum number of messages to be fetched
        per request. This value must be between `1` and `10`, which is the maximum number
        allowed by AWS.
        """
      ],
      wait_time_seconds: [
        type: {
          :custom,
          __MODULE__,
          :type_bounded_integer,
          [[{:name, :wait_time_seconds}, {:min, 0}, {:max, 20}]]
        },
        doc: """
        The duration (in seconds) for which the call waits
        for a message to arrive in the queue before returning. This value must be
        between `0` and `20`, which is the maximum number allowed by AWS. For more
        information see ["WaitTimeSeconds" on the Amazon SQS documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html).
        """
      ],
      visibility_timeout: [
        type: {
          :custom,
          __MODULE__,
          :type_bounded_integer,
          [[{:name, :visibility_timeout}, {:min, 0}, {:max, 43200}]]
        },
        doc: """
        The time period (in seconds) that a message will
        remain _invisible_ to other consumers whilst still on the queue and not acknowledged.
        This is passed to SQS when the message (or messages) are read.
        This value must be between 0 and 43200 (12 hours).
        """
      ],
      attribute_names: [
        type: {
          :custom,
          __MODULE__,
          :type_list_limited_member_or_all_atom,
          [
            [
              {:name, :attribute_names},
              {:allowed_members,
               [
                 :sender_id,
                 :sent_timestamp,
                 :approximate_receive_count,
                 :approximate_first_receive_timestamp,
                 :sequence_number,
                 :message_deduplication_id,
                 :message_group_id,
                 :aws_trace_header
               ]}
            ]
          ]
        },
        doc: """
        A list containing the names of attributes that should be
        attached to the response and appended to the `metadata`
        field of the message. You can also use `:all` instead of
        a list if you want to retrieve all attributes. Supported
        values are:

            * `:sender_id`
            * `:sent_timestamp`
            * `:approximate_receive_count`
            * `:approximate_first_receive_timestamp`
            * `:sequence_number`
            * `:message_deduplication_id`
            * `:message_group_id`
            * `:aws_trace_header`

        """
      ],
      message_attribute_names: [
        type: {
          :custom,
          __MODULE__,
          :type_array_non_empty_string_or_all_atom,
          [[{:name, :queue_url}]]
        },
        doc: """
        A list containing the names of custom message attributes
        that should be attached to the response and appended to the `metadata` field of the
        message. Wildcards `[".*"]` and prefixes `["bar.*"]` will retrieve multiple fields.
        You can also use `:all` instead of the list if you want to retrieve all attributes.
        """
      ],
      test_pid: [
        type: :pid,
        doc: false
      ],
      message_server: [
        type: :pid,
        doc: false
      ]
    ]
  end

  def type_bounded_integer(value, [{:name, _}, {:min, min}, {:max, max}])
      when is_integer(value) and value >= min and value <= max do
    {:ok, value}
  end

  def type_bounded_integer(value, [{:name, name}, {:min, min}, {:max, max}]) do
    {:error,
     "expected :#{name} to be an integer between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_array_non_empty_string_or_all_atom(:all, _) do
    {:ok, :all}
  end

  def type_array_non_empty_string_or_all_atom(value, [{:name, name}]) do
    invalid_members =
      value
      |> Enum.filter(&(is_nil(&1) || &1 == "" || !is_binary(&1)))

    if invalid_members == [] do
      {:ok, value}
    else
      {:error,
       "expected :#{name} to be a list with non-empty strings, got: #{inspect(invalid_members)}"}
    end
  end

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: \"\""}
  end

  def type_non_empty_string(value, _)
      when not is_nil(value) and is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}
  end

  def type_list_limited_member_or_all_atom(:all, _) do
    {:ok, :all}
  end

  def type_list_limited_member_or_all_atom(value, [
        {:name, name},
        {:allowed_members, allowed_members}
      ]) do
    if value -- allowed_members == [] do
      {:ok, value}
    else
      {:error,
       "expected :#{name} to be a list with possible members #{inspect(allowed_members)}, got: #{inspect(value)}"}
    end
  end

  def type_ack_action(:ack, _opts), do: {:ok, :ack}
  def type_ack_action(:noop, _opts), do: {:ok, :noop}

  def type_ack_action({:nack, timeout}, [{:name, _name}])
      when is_integer(timeout) and timeout >= 0 and timeout <= 43_200 do
    {:ok, {:nack, timeout}}
  end

  def type_ack_action(value, [{:name, name}]) do
    {:error, "expected :#{name} to be :ack, :noop, or {:nack, timeout}, got: #{inspect(value)}"}
  end
end
