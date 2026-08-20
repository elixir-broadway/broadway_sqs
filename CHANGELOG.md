# Changelog

## Unreleased

  * Replace the default `BroadwaySQS.ExAwsClient` with the Req-based
    `BroadwaySQS.ReqClient`.
  * Make `BroadwaySQS.ReqClient` as default sqs_client.
  * Implement the SQS JSON API requests used by Broadway SQS with Req and AWS
    * `ReceiveMessage`
    * `DeleteMessageBatch`
    * `ChangeMessageVisibilityBatch`
  * Support credentials discovered through `aws_credentials`
  * Remove the `ex_aws_sqs`, `ex_aws`, `hackney`, and `saxy` dependencies.
  * Update the documentation and example application to use the Req-based
    client.

### Breaking changes

  * `BroadwaySQS.ExAwsClient` has been removed. The default client is now
    `BroadwaySQS.ReqClient`.
  * ExAws configuration is no longer used. Configure the AWS region with the
    producer `:config` option, and provide credentials through
    `aws_credentials` or the producer configuration options.
  * Applications using `BroadwaySQS.ExAwsClient` directly or relying on
    `ex_aws` application configuration must migrate to
    `BroadwaySQS.ReqClient` and the new credential configuration.

## v0.7.4 (2024-06-21)

  * Forward compatibility with Broadway v1.1

## v0.7.3 (2023-06-16)

  * Relax `nimble_options` dependency to accept `~> 1.0`

## v0.7.2 (2022-11-12)

  * Relax `nimble_options` dependency to accept `~> 0.5.0`

## v0.7.1 (2022-03-27)

  * Relax `nimble_options` dependency to accept `~> 0.4.0`

## v0.7.0 (2021-08-30)

  * Add the following telemetry events:
    * `[:broadway_sqs, :receive_messages, :start]`
    * `[:broadway_sqs, :receive_messages, :stop]`
    * `[:broadway_sqs, :receive_messages, :exception]`
  * Require Broadway 1.0

## v0.6.1 (2020-04-14)

  * Depend on ex_aws_sqs with the faster Saxy support

## v0.6.0 (2020-02-19)

  * Implement `prepare_for_draining/1` to make sure no more messages will be fetched after draining
  * Add `:on_success` and `:on_failure` options
  * Crash on ack error
  * Update to Broadway v0.6.0

## v0.5.0 (2019-11-05)

  * Update to Broadway v0.5.0

## v0.4.0 (2019-09-26)

  * Replace option `:queue_name` with `:queue_url` to keep compatibility with ex_aws_sqs >= v3.0.0

## v0.3.0 (2019-09-18)

  * Update `ex_aws` dependency to `~> 3.0`
  * Update `broadway` dependency to `~> 0.4.0`

## v0.2.0 (2019-04-26)

  * Automatically add `message_id`, `receipt_handle` and `md5_of_body` to the message's metadata
  * New option `:attribute_names`
  * New option `:message_attribute_names`
  * New option `:visibility_timeout`

## v0.1.0 (2019-02-19)

  * Initial release
