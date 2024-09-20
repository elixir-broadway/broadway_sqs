defmodule BroadwaySQS.AwsElixirClientTest do
  use ExUnit.Case

  alias BroadwaySQS.AwsElixirClient
  alias Broadway.Message
  import ExUnit.CaptureLog

  defmodule FakeHttpClient do
    @behaviour AWS.HTTPClient

    def request(:post, url, body, headers, options) do
      {"X-Amz-Target", "AmazonSQS." <> action} =
        Enum.find(headers, fn {k, _} -> k == "X-Amz-Target" end)

      request(action, :post, url, body, headers, options)
    end

    def request("ReceiveMessage", :post, url, body, headers, _) do
      send(self(), {:http_request_called, %{url: url, body: body, headers: headers}})

      response_body = """
      {
        "Messages": [
          {
            "MessageId": "Id_1",
            "ReceiptHandle": "ReceiptHandle_1",
            "MD5OfBody": "fake_md5",
            "Body": "Message 1",
            "Attributes": {
              "SenderId": "13",
              "ApproximateReceiveCount": "5"
            },
            "MessageAttributes": {
              "TestStringAttribute": {
                "StringValue": "Test",
                "DataType": "String"
              }
            }
          },
          {
            "MessageId": "Id_2",
            "ReceiptHandle": "ReceiptHandle_22",
            "Body": "Message 2"
          }
        ]
      }
      """

      {:ok, %{status_code: 200, body: response_body}}
    end

    def request("DeleteMessageBatch", :post, url, body, headers, _) do
      send(self(), {:http_request_called, %{url: url, body: body, headers: headers}})

      response_body = """
      {
        "Successful": [
          {
            "Id": "1"
          },
          {
            "Id": "2"
          }
        ]
      }
      """

      {:ok, %{status_code: 200, body: response_body}}
    end
  end

  defmodule FakeHttpClientWithError do
    @behaviour AWS.HTTPClient

    def request(:post, url, body, headers, options) do
      {"X-Amz-Target", "AmazonSQS." <> action} =
        Enum.find(headers, fn {k, _} -> k == "X-Amz-Target" end)

      request(action, :post, url, body, headers, options)
    end

    def request("ReceiveMessage", :post, _url, _body, _headers, _options) do
      body = %{
        "__type" => "com.amazonaws.sqs#queuedoesnotexist",
        "message" => "The specified queue does not exist."
      }

      {:error,
       {:unexpected_response, %{body: Jason.encode!(body), headers: [], status_code: 404}}}
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
            http_client: {FakeHttpClient, []},
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY",
            region: "us-east-1"
          ]
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{opts: base_opts} do
      {:ok, opts} = AwsElixirClient.init(base_opts)
      [message1, message2] = AwsElixirClient.receive_messages(10, opts)

      assert message1.data == "Message 1"
      assert message2.data == "Message 2"

      assert message1.acknowledger ==
               {AwsElixirClient, opts.ack_ref,
                %{receipt: %{id: "Id_1", receipt_handle: "ReceiptHandle_1"}}}
    end

    test "add message_id, receipt_handle and md5_of_body to metadata", %{opts: base_opts} do
      {:ok, opts} = AwsElixirClient.init(base_opts)
      [%{metadata: metadata} | _] = AwsElixirClient.receive_messages(10, opts)

      assert metadata.message_id == "Id_1"
      assert metadata.receipt_handle == "ReceiptHandle_1"
      assert metadata.md5_of_body == "fake_md5"
    end

    test "add attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :attribute_names, :all) |> AwsElixirClient.init()

      [%{metadata: metadata_1}, %{metadata: _metadata_2} | _] =
        AwsElixirClient.receive_messages(10, opts)

      assert metadata_1.attributes == %{"SenderId" => "13", "ApproximateReceiveCount" => "5"}
    end

    test "add message_attributes to metadata", %{opts: base_opts} do
      {:ok, opts} =
        Keyword.put(base_opts, :message_attribute_names, :all) |> AwsElixirClient.init()

      [%{metadata: metadata_1}, %{metadata: _metadata_2} | _] =
        AwsElixirClient.receive_messages(10, opts)

      assert metadata_1.message_attributes == %{
               "TestStringAttribute" => %{
                 "DataType" => "String",
                 "StringValue" => "Test"
               }
             }
    end

    test "if the request fails, returns an empty list and log the error", %{opts: base_opts} do
      {:ok, opts} =
        base_opts
        |> put_in([:config, :http_client], {FakeHttpClientWithError, []})
        |> AwsElixirClient.init()

      assert capture_log(fn ->
               assert AwsElixirClient.receive_messages(10, opts) == []
             end) =~
               "[error] Unable to fetch events from AWS queue my_queue. Reason: \"The specified queue does not exist.\""
    end

    test "send a SQS/ReceiveMessage request with default options", %{opts: base_opts} do
      {:ok, opts} = AwsElixirClient.init(base_opts)
      AwsElixirClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: url, headers: headers}}
      assert body == "{\"MaxNumberOfMessages\":10,\"QueueUrl\":\"my_queue\"}"
      assert url == "https://sqs.us-east-1.amazonaws.com/"

      assert {"X-Amz-Target", "AmazonSQS.ReceiveMessage"} ==
               Enum.find(headers, fn {k, _} -> k == "X-Amz-Target" end)
    end

    test "request with custom :wait_time_seconds", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:wait_time_seconds, 0) |> AwsElixirClient.init()
      AwsElixirClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "WaitTimeSeconds\":0"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> AwsElixirClient.init()
      AwsElixirClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "MaxNumberOfMessages\":5"
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          proto: "http",
          endpoint: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> AwsElixirClient.init()

      AwsElixirClient.receive_messages(10, opts)

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
            http_client: {FakeHttpClient, []},
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY",
            region: "us-east-1"
          ],
          on_success: :ack,
          on_error: :noop
        ]
      }
    end

    test "send a SQS/DeleteMessageBatch request", %{opts: base_opts} do
      {:ok, opts} = AwsElixirClient.init(base_opts)
      ack_data_1 = %{receipt: %{id: "1", receipt_handle: "abc"}}
      ack_data_2 = %{receipt: %{id: "2", receipt_handle: "def"}}

      fill_persistent_term(opts.ack_ref, base_opts)

      AwsElixirClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_1}, data: nil},
          %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_2}, data: nil}
        ],
        []
      )

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body ==
               ~s({"Entries":[{"Id":"1","ReceiptHandle":"abc"},{"Id":"2","ReceiptHandle":"def"}],"QueueUrl":"my_queue"})

      assert url == "https://sqs.us-east-1.amazonaws.com/"
    end

    test "request with custom :on_success and :on_failure", %{opts: base_opts} do
      {:ok, opts} = AwsElixirClient.init(base_opts ++ [on_success: :noop, on_failure: :ack])

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

      message1 = %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_1}, data: nil}
      message2 = %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_2}, data: nil}
      message3 = %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_3}, data: nil}
      message4 = %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data_4}, data: nil}

      AwsElixirClient.ack(
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
               ~s({"Entries":[{"Id":"2","ReceiptHandle":"def"},{"Id":"3","ReceiptHandle":"ghi"}],"QueueUrl":"my_queue"})
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          proto: "http",
          endpoint: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> AwsElixirClient.init()

      :persistent_term.put(opts.ack_ref, %{
        queue_url: opts[:queue_url],
        config: opts[:config],
        on_success: opts[:on_success],
        on_failure: opts[:on_failure]
      })

      ack_data = %{receipt: %{id: "1", receipt_handle: "abc"}}
      message = %Message{acknowledger: {AwsElixirClient, opts.ack_ref, ack_data}, data: nil}

      AwsElixirClient.ack(opts.ack_ref, [message], [])

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/"
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
