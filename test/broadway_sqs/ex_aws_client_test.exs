defmodule BroadwaySQS.ExAwsClientTest do
  use ExUnit.Case

  alias BroadwaySQS.ExAwsClient
  alias Broadway.Message
  import ExUnit.CaptureLog

  defmodule FakeHttpClient do
    @behaviour ExAws.Request.HttpClient

    def request(:post, url, body, headers, _) do
      case List.keyfind(headers, "x-amz-target", 0) do
        {"x-amz-target", action} -> handle_request(action, url, body)
        false -> assert false, "No x-amz-target found for action"
      end
    end

    defp handle_request("AmazonSQS.ReceiveMessage" = action, url, body) do
      send(self(), {:http_request_called, %{url: url, body: body, action: action}})

      response_body =
        Jason.encode!(%{
          "Messages" => [
            %{
              "MessageId" => "Id_1",
              "ReceiptHandle" => "ReceiptHandle_1",
              "MD5OfBody" => "fake_md5",
              "Body" => "Message 1",
              "Attributes" => %{
                "SenderId" => "13",
                "ApproximateReceiveCount" => "5"
              },
              "MessageAttributes" => %{
                "TestStringAttribute" => %{
                  "StringValue" => "Test",
                  "DataType" => "String"
                }
              }
            },
            %{
              "MessageId" => "Id_2",
              "ReceiptHandle" => "ReceiptHandle_2",
              "Body" => "Message 2"
            }
          ]
        })

      {:ok, %{status_code: 200, body: response_body}}
    end

    defp handle_request("AmazonSQS.DeleteMessageBatch" = action, url, body) do
      send(self(), {:http_request_called, %{url: url, body: body, action: action}})

      {:ok, %{status_code: 200, body: ~s({"Successful":[],"Failed":[]})}}
    end

    defp handle_request("AmazonSQS.ChangeMessageVisibilityBatch" = action, url, body) do
      send(self(), {:http_request_called, %{url: url, body: body, action: action}})

      {:ok, %{status_code: 200, body: ~s({"Successful":[],"Failed":[]})}}
    end
  end

  defmodule FakeHttpClientWithError do
    @behaviour ExAws.Request.HttpClient

    def request(:post, _url, _body, headers, _) do
      {_, "AmazonSQS.ReceiveMessage"} = List.keyfind(headers, "x-amz-target", 0)

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

      assert metadata["MessageId"] == "Id_1"
      assert metadata["ReceiptHandle"] == "ReceiptHandle_1"
      assert metadata["MD5OfBody"] == "fake_md5"
    end

    test "add attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1["Attributes"] == %{"SenderId" => "13", "ApproximateReceiveCount" => "5"}
      assert metadata_2["Attributes"] == nil
    end

    test "add message_attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :message_attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1["MessageAttributes"] == %{
               "TestStringAttribute" => %{"DataType" => "String", "StringValue" => "Test"}
             }

      assert metadata_2["MessageAttributes"] == nil
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
      assert %{"MaxNumberOfMessages" => 10, "QueueUrl" => "my_queue"} = Jason.decode!(body)
      assert url == "https://sqs.us-east-1.amazonaws.com/"
    end

    test "request with custom :wait_time_seconds", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:wait_time_seconds, 0) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert %{"WaitTimeSeconds" => 0} = Jason.decode!(body)
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert %{"MaxNumberOfMessages" => 5} = Jason.decode!(body)
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

      assert Jason.decode!(body) ==
               %{
                 "Entries" => [
                   %{"Id" => "1", "ReceiptHandle" => "abc"},
                   %{"Id" => "2", "ReceiptHandle" => "def"}
                 ],
                 "QueueUrl" => "my_queue"
               }

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

      assert Jason.decode!(body) ==
               %{
                 "Entries" => [
                   %{"Id" => "2", "ReceiptHandle" => "def"},
                   %{"Id" => "3", "ReceiptHandle" => "ghi"}
                 ],
                 "QueueUrl" => "my_queue"
               }
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

      assert_received {:http_request_called, %{body: body, action: action}}

      assert action == "AmazonSQS.ChangeMessageVisibilityBatch"

      assert %{
               "Entries" => [
                 %{"Id" => "1", "ReceiptHandle" => "abc", "VisibilityTimeout" => 10}
               ],
               "QueueUrl" => "my_queue"
             } == Jason.decode!(body)
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
