defmodule BroadwaySQSExample.Helpers do
  def send_strings_sqs(queue, msg, amount) do
    Enum.each(1..amount, fn _x ->
      Task.async(fn ->
        request("AmazonSQS.SendMessage", %{"QueueUrl" => queue, "MessageBody" => msg})
      end)
    end)
  end

  def send_ints_sqs(queue, amount) do
    Enum.each(1..amount, fn x ->
      Task.async(fn ->
        request("AmazonSQS.SendMessage", %{
          "QueueUrl" => queue,
          "MessageBody" => to_string(x)
        })
      end)
    end)
  end

  def create_sqs_queue(queue) do
    request("AmazonSQS.CreateQueue", %{"QueueName" => queue})
  end

  def create_default_queues() do
    string_queue = Application.get_env(:broadway_sqs_example, :string_queue)
    IO.inspect("creating string_queue with name: #{string_queue}")
    create_sqs_queue(string_queue)

    int_queue = Application.get_env(:broadway_sqs_example, :int_queue)
    IO.inspect("creating int_queue with name: #{int_queue}")
    create_sqs_queue(int_queue)
  end

  def send_ints() do
    int_queue = Application.get_env(:broadway_sqs_example, :int_queue)
    send_ints_sqs(int_queue, 100)
  end

  def send_strings() do
    string_queue = Application.get_env(:broadway_sqs_example, :string_queue)
    send_strings_sqs(string_queue, "testing", 100)
  end

  defp request(action, payload) do
    credentials = :aws_credentials.get_credentials()
    region = Application.get_env(:broadway_sqs_example, :region, "us-east-2")

    BroadwaySQS.ReqClient.Request.call(action, payload,
      credentials: credentials,
      region: region,
      endpoint: Application.get_env(:broadway_sqs_example, :sqs_endpoint),
      queue_url: Application.get_env(:broadway_sqs_example, :sqs_endpoint)
    )
  end
end
