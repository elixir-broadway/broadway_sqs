defmodule BroadwaySQS.ExAwsClientTest do
  use ExUnit.Case

  alias BroadwaySQS.ExAwsClient
  alias Broadway.Message
  import ExUnit.CaptureLog

  defmodule FakeHttpClient do
    @behaviour ExAws.Request.HttpClient

    def request(:post, url, "Action=ReceiveMessage" <> _ = body, _, _) do
      send(self(), {:http_request_called, %{url: url, body: body}})

      response_body = """
      <ReceiveMessageResponse>
        <ReceiveMessageResult>
          <Message>
            <MessageId>Id_1</MessageId>
            <ReceiptHandle>ReceiptHandle_1</ReceiptHandle>
            <MD5OfBody>fake_md5</MD5OfBody>
            <Body>Message 1</Body>
            <Attribute>
              <Name>SenderId</Name>
              <Value>13</Value>
            </Attribute>
            <Attribute>
              <Name>ApproximateReceiveCount</Name>
              <Value>5</Value>
            </Attribute>
            <MessageAttribute>
              <Name>TestStringAttribute</Name>
              <Value>
                <StringValue>Test</StringValue>
                <DataType>String</DataType>
              </Value>
            </MessageAttribute>
          </Message>
          <Message>
            <MessageId>Id_2</MessageId>
            <ReceiptHandle>ReceiptHandle_2</ReceiptHandle>
            <Body>Message 2</Body>
          </Message>
        </ReceiveMessageResult>
      </ReceiveMessageResponse>
      """

      {:ok, %{status_code: 200, body: response_body}}
    end

    def request(:post, url, "Action=DeleteMessageBatch" <> _ = body, _, _) do
      send(self(), {:http_request_called, %{url: url, body: body}})

      {:ok, %{status_code: 200, body: "<DeleteMessageBatchResponse />"}}
    end

    def request(:post, url, "Action=ChangeMessageVisibilityBatch" <> _ = body, _, _) do
      send(self(), {:http_request_called, %{url: url, body: body}})

      {:ok, %{status_code: 200, body: "<ChangeMessageVisibilityBatchResponse />"}}
    end
  end

  defmodule FakeHttpClientWithError do
    @behaviour ExAws.Request.HttpClient

    def request(:post, _url, "Action=ReceiveMessage" <> _, _, _) do
      {:error, %{reason: "Fake error"}}
    end
  end

  describe "receive_messages/2" do
    setup do
      %{
        opts: [
          # will be injected by broadway at runtime
          broadway: [name: :Broadway3],
          queue_url: "my_queue",
          config: [
            http_client: FakeHttpClient,
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY",
            retries: [max_attempts: 0]
          ]
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      [message1, message2] = ExAwsClient.receive_messages(10, opts)

      assert message1.data == "Message 1"
      assert message2.data == "Message 2"

      assert message1.acknowledger ==
               {ExAwsClient, opts.ack_ref,
                %{receipt: %{id: "Id_1", receipt_handle: "ReceiptHandle_1"}}}
    end

    test "add message_id, receipt_handle and md5_of_body to metadata", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      [%{metadata: metadata} | _] = ExAwsClient.receive_messages(10, opts)

      assert metadata.message_id == "Id_1"
      assert metadata.receipt_handle == "ReceiptHandle_1"
      assert metadata.md5_of_body == "fake_md5"
    end

    test "add attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1.attributes == %{"sender_id" => 13, "approximate_receive_count" => 5}
      assert metadata_2.attributes == []
    end

    test "add message_attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :message_attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1.message_attributes == %{
               "TestStringAttribute" => %{
                 name: "TestStringAttribute",
                 data_type: "String",
                 string_value: "Test",
                 binary_value: "",
                 value: "Test"
               }
             }

      assert metadata_2.message_attributes == []
    end

    test "if the request fails, returns an empty list and log the error", %{opts: base_opts} do
      {:ok, opts} =
        base_opts
        |> put_in([:config, :http_client], FakeHttpClientWithError)
        |> ExAwsClient.init()

      assert capture_log(fn ->
               assert ExAwsClient.receive_messages(10, opts) == []
             end) =~
               "[error] Unable to fetch events from AWS queue my_queue. Reason: \"Fake error\""
    end

    test "send a SQS/ReceiveMessage request with default options", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == "Action=ReceiveMessage&MaxNumberOfMessages=10&QueueUrl=my_queue"
      assert url == "https://sqs.us-east-1.amazonaws.com/"
    end

    test "request with custom :wait_time_seconds", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:wait_time_seconds, 0) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "WaitTimeSeconds=0"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "MaxNumberOfMessages=5"
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          scheme: "http://",
          host: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> ExAwsClient.init()

      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/"
    end
  end

  describe "ack/3" do
    setup do
      %{
        opts: [
          # will be injected by broadway at runtime
          broadway: [name: :Broadway3],
          queue_url: "my_queue",
          config: [
            http_client: FakeHttpClient,
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY"
          ],
          on_success: :ack,
          on_error: :noop
        ]
      }
    end

    test "send a SQS/DeleteMessageBatch request", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      ack_data_1 = %{receipt: %{id: "1", receipt_handle: "abc"}}
      ack_data_2 = %{receipt: %{id: "2", receipt_handle: "def"}}

      fill_persistent_term(opts.ack_ref, base_opts)

      ExAwsClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_1}, data: nil},
          %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_2}, data: nil}
        ],
        []
      )

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body ==
               "Action=DeleteMessageBatch" <>
                 "&DeleteMessageBatchRequestEntry.1.Id=1&DeleteMessageBatchRequestEntry.1.ReceiptHandle=abc" <>
                 "&DeleteMessageBatchRequestEntry.2.Id=2&DeleteMessageBatchRequestEntry.2.ReceiptHandle=def&QueueUrl=my_queue"

      assert url == "https://sqs.us-east-1.amazonaws.com/"
    end

    test "request with custom :on_success and :on_failure", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts ++ [on_success: :noop, on_failure: :ack])

      :persistent_term.put(opts.ack_ref, %{
        queue_url: opts[:queue_url],
        config: opts[:config],
        on_success: opts[:on_success],
        on_failure: opts[:on_failure]
      })

      ack_data_1 = %{receipt: %{id: "1", receipt_handle: "abc"}}
      ack_data_2 = %{receipt: %{id: "2", receipt_handle: "def"}}
      ack_data_3 = %{receipt: %{id: "3", receipt_handle: "ghi"}}
      ack_data_4 = %{receipt: %{id: "4", receipt_handle: "jkl"}}

      message1 = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_1}, data: nil}
      message2 = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_2}, data: nil}
      message3 = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_3}, data: nil}
      message4 = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_4}, data: nil}

      ExAwsClient.ack(
        opts.ack_ref,
        [
          message1,
          message2 |> Message.configure_ack(on_success: :ack)
        ],
        [
          message3,
          message4 |> Message.configure_ack(on_failure: :noop)
        ]
      )

      assert_received {:http_request_called, %{body: body}}

      assert body ==
               "Action=DeleteMessageBatch" <>
                 "&DeleteMessageBatchRequestEntry.1.Id=2&DeleteMessageBatchRequestEntry.1.ReceiptHandle=def" <>
                 "&DeleteMessageBatchRequestEntry.2.Id=3&DeleteMessageBatchRequestEntry.2.ReceiptHandle=ghi&QueueUrl=my_queue"
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          scheme: "http://",
          host: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> ExAwsClient.init()

      :persistent_term.put(opts.ack_ref, %{
        queue_url: opts[:queue_url],
        config: opts[:config],
        on_success: opts[:on_success],
        on_failure: opts[:on_failure]
      })

      ack_data = %{receipt: %{id: "1", receipt_handle: "abc"}}
      message = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data}, data: nil}

      ExAwsClient.ack(opts.ack_ref, [message], [])

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/"
    end

    test "request with :nack strategy", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts ++ [on_failure: {:nack, 10}])

      :persistent_term.put(opts.ack_ref, %{
        queue_url: opts[:queue_url],
        config: opts[:config],
        on_success: opts[:on_success],
        on_failure: opts[:on_failure]
      })

      ack_data = %{receipt: %{id: "1", receipt_handle: "abc"}}
      message = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data}, data: nil}

      ExAwsClient.ack(opts.ack_ref, [], [message])

      assert_received {:http_request_called, %{body: body}}

      assert body ==
               "Action=ChangeMessageVisibilityBatch" <>
                 "&ChangeMessageVisibilityBatchRequestEntry.1.Id=1" <>
                 "&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=abc" <>
                 "&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=10" <>
                 "&QueueUrl=my_queue"
    end
  end

  defp fill_persistent_term(ack_ref, base_opts) do
    :persistent_term.put(ack_ref, %{
      queue_url: base_opts[:queue_url],
      config: base_opts[:config],
      on_success: base_opts[:on_success] || :ack,
      on_failure: base_opts[:on_failure] || :noop
    })
  end
end
