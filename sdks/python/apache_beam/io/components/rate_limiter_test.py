#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from unittest import mock

import grpc
from google.protobuf.duration_pb2 import Duration

from apache_beam.io.components import rate_limit_pb2
from apache_beam.io.components import rate_limiter

RateLimitResponse = rate_limit_pb2.RateLimitResponse


class EnvoyRateLimiterTest(unittest.TestCase):
  def setUp(self):
    self.service_address = 'localhost:8081'
    self.domain = 'test_domain'
    self.descriptors = [{'key': 'value'}]
    self.limiter = rate_limiter.EnvoyRateLimiter(
        self.service_address,
        self.domain,
        self.descriptors,
        timeout=0.1,  # Fast timeout for tests
        block_until_allowed=False,
        retries=2,
        namespace='test_namespace')

  @mock.patch('grpc.insecure_channel')
  def test_allow_success(self, mock_channel):
    # Mock successful OK response
    mock_stub = mock.Mock()
    mock_response = RateLimitResponse(overall_code=RateLimitResponse.OK)
    mock_stub.ShouldRateLimit.return_value = mock_response

    # Inject mock stub
    self.limiter._stub = mock_stub

    allowed = self.limiter.allow()

    self.assertTrue(allowed)
    mock_stub.ShouldRateLimit.assert_called_once()

  @mock.patch('grpc.insecure_channel')
  def test_allow_over_limit_retries_exceeded(self, mock_channel):
    # Mock OVER_LIMIT response
    mock_stub = mock.Mock()
    mock_response = RateLimitResponse(overall_code=RateLimitResponse.OVER_LIMIT)
    mock_stub.ShouldRateLimit.return_value = mock_response

    self.limiter._stub = mock_stub
    # block_until_allowed is False, so it should eventually return False

    # We mock time.sleep to run fast
    with mock.patch('time.sleep'):
      allowed = self.limiter.allow()

    self.assertFalse(allowed)
    # Should be called 1 (initial) + 2 (retries) + 1 (last check > retries
    # logic depends on loop)
    # Logic: attempt starts at 0.
    # Loop 1: attempt 0. status OVER_LIMIT. sleep. attempt becomes 1.
    # Loop 2: attempt 1. status OVER_LIMIT. sleep. attempt becomes 2.
    # Loop 3: attempt 2. status OVER_LIMIT. sleep. attempt becomes 3.
    # Loop 4: attempt 3 > retries(2). Break.
    # Total calls: 3
    self.assertEqual(mock_stub.ShouldRateLimit.call_count, 3)

  @mock.patch('grpc.insecure_channel')
  def test_allow_rpc_error_retry(self, mock_channel):
    # Mock RpcError then Success
    mock_stub = mock.Mock()
    mock_response = RateLimitResponse(overall_code=RateLimitResponse.OK)

    # Side effect: Error, Error, Success
    error = grpc.RpcError()
    mock_stub.ShouldRateLimit.side_effect = [error, error, mock_response]

    self.limiter._stub = mock_stub

    with mock.patch('time.sleep'):
      allowed = self.limiter.allow()

    self.assertTrue(allowed)
    self.assertEqual(mock_stub.ShouldRateLimit.call_count, 3)

  @mock.patch('grpc.insecure_channel')
  def test_allow_rpc_error_fail(self, mock_channel):
    # Mock Persistent RpcError
    mock_stub = mock.Mock()
    error = grpc.RpcError()
    mock_stub.ShouldRateLimit.side_effect = error

    self.limiter._stub = mock_stub

    with mock.patch('time.sleep'):
      with self.assertRaises(grpc.RpcError):
        self.limiter.allow()

    # The inner loop tries 5 times for connection errors
    self.assertEqual(mock_stub.ShouldRateLimit.call_count, 5)

  @mock.patch('grpc.insecure_channel')
  @mock.patch('random.uniform', return_value=0.0)
  def test_extract_duration_from_response(self, mock_random, mock_channel):
    # Mock OVER_LIMIT with specific duration
    mock_stub = mock.Mock()

    # Valid until 5 seconds
    status = RateLimitResponse.DescriptorStatus(
        code=RateLimitResponse.OVER_LIMIT,
        duration_until_reset=Duration(seconds=5))
    mock_response = RateLimitResponse(
        overall_code=RateLimitResponse.OVER_LIMIT, statuses=[status])

    mock_stub.ShouldRateLimit.return_value = mock_response
    self.limiter._stub = mock_stub
    self.limiter.retries = 0  # Single attempt

    with mock.patch('time.sleep') as mock_sleep:
      self.limiter.allow()
      # Should sleep for 5 seconds (jitter is 0.0)
      mock_sleep.assert_called_with(5.0)


class RateLimitWireFormatTest(unittest.TestCase):
  """Pins the on-the-wire layout of the vendored rate_limit_pb2 messages.

  Wire compatibility with a real Envoy Rate Limit Service depends solely on
  field numbers and types (protobuf carries neither message nor package names
  on the wire), so these must stay in lockstep with Envoy's rls.proto and
  ratelimit.proto. The mock-based tests above would pass even if a field were
  renumbered; these golden-byte assertions fail if that ever happens.
  """
  def test_request_wire_layout(self):
    request = rate_limit_pb2.RateLimitRequest(
        domain='d',
        descriptors=[
            rate_limit_pb2.RateLimitDescriptor(
                entries=[
                    rate_limit_pb2.RateLimitDescriptor.Entry(
                        key='k', value='v')
                ])
        ],
        hits_addend=1)
    # domain=1 (LEN "d"); descriptors=2 (LEN {entries=1 (LEN {key=1 "k",
    # value=2 "v"})}); hits_addend=3 (VARINT 1).
    self.assertEqual(
        request.SerializeToString().hex(), '0a016412080a060a016b1201761801')

  def test_descriptor_status_wire_layout(self):
    status = rate_limit_pb2.RateLimitResponse.DescriptorStatus(
        code=rate_limit_pb2.RateLimitResponse.OVER_LIMIT,
        duration_until_reset=Duration(seconds=5))
    # code=1 (VARINT OVER_LIMIT=2); duration_until_reset=4 (LEN
    # Duration{seconds=1 (VARINT 5)}).
    self.assertEqual(status.SerializeToString().hex(), '080222020805')

  def test_response_code_enum_values(self):
    self.assertEqual(int(rate_limit_pb2.RateLimitResponse.UNKNOWN), 0)
    self.assertEqual(int(rate_limit_pb2.RateLimitResponse.OK), 1)
    self.assertEqual(int(rate_limit_pb2.RateLimitResponse.OVER_LIMIT), 2)


if __name__ == '__main__':
  unittest.main()
