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
import base64
import sys
import unittest
from typing import Tuple
from typing import Union

import grpc
from grpc import Channel
from grpclib.exceptions import GRPCError, Status
import urllib3

from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import SetupTeardown
from apache_beam.io.requestresponseio import UserCodeExecutionException
from apache_beam.io.requestresponseio import UserCodeQuotaException
from apache_beam.io.requestresponseio import UserCodeTimeoutException
from apache_beam.options.pipeline_options import PipelineOptions

try:
  from apache_beam.io.mock_apis.proto.echo.v1.echo_pb2 import EchoRequest
  from apache_beam.io.mock_apis.proto.echo.v1.echo_pb2 import EchoResponse
  from apache_beam.io.mock_apis.proto.echo.v1.echo_pb2_grpc import EchoServiceStub
except ImportError:
  raise unittest.SkipTest(
      'proto.echo requirement missing. Make sure you '
      'run `sdks/python/gen_protos.py`')

_CHANNEL_CREDENTIALS = grpc.local_channel_credentials()
_HTTP_PATH = "/v1/echo"
_PAYLOAD = base64.b64encode(bytes('payload', 'utf-8'))


class EchoITOptions(PipelineOptions):
  """Shared options for running integration tests on a deployed
      ``EchoServiceGrpc`` See https://github.com/apache/beam/tree/master/.test
      -infra/mock-apis#integration for details on how to acquire values
      required by ``EchoITOptions``.
      """
  @classmethod
  def _add_argparse_args(cls, parser) -> None:
    parser.add_argument(
        '--gRPCEndpointAddress',
        required=True,
        dest='grpc_endpoint_address',
        help='The gRPC address of the Echo API endpoint, typically of the '
        'form <host>:<port>.')
    parser.add_argument(
        '--httpEndpointAddress',
        required=True,
        dest='http_endpoint_address',
        help='The HTTP address of the Echo API endpoint; must being with '
        'http(s)://')
    parser.add_argument(
        '--neverExceedQuotaId',
        default='echo-should-never-exceed-quota',
        dest='never_exceed_quota_id',
        help='The ID for an allocated quota that should never exceed.')
    parser.add_argument(
        '--shouldExceedQuotaId',
        default='echo-should-exceed-quota',
        dest='should_exceed_quota_id',
        help='The ID for an allocated quota that should exceed.')


class EchoGRPCCaller(Caller, SetupTeardown):
  """Implements ``Caller`` and ``SetupTeardown`` to call ``EchoServiceGrpc``.
    The purpose of ``EchoGRPCCaller`` is to support integration tests.
    """
  def __init__(self, address: str):
    self.address = address
    self.channel: Union[Channel, None] = None
    self.client: Union[EchoServiceStub, None] = None

  def call(self, request: EchoRequest) -> EchoResponse:
    assert self.client is not None
    try:
      return self.client.Echo(request)
    except grpc.RpcError as e:
      code: grpc.StatusCode = e.code()
      if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise UserCodeQuotaException(e)
      if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise UserCodeTimeoutException(e)
      raise UserCodeExecutionException(e)

  def setup(self) -> None:
    self.channel = grpc.insecure_channel(self.address)
    self.client = EchoServiceStub(self.channel)

  def teardown(self) -> None:
    if self.channel is not None:
      self.channel.close()


class EchoGRPCCallerTestIT(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[EchoGRPCCaller, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    cls.client = EchoGRPCCaller(cls.options.grpc_endpoint_address)
    cls.client.setup()

  @classmethod
  def tearDownClass(cls) -> None:
    if cls.client is not None:
      cls.client.teardown()

  @classmethod
  def _get_client_and_options(cls) -> Tuple[EchoGRPCCaller, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def setUp(self) -> None:
    client, options = EchoGRPCCallerTestIT._get_client_and_options()

    req = EchoRequest(id=options.should_exceed_quota_id, payload=_PAYLOAD)

    try:
      # The following is needed to exceed the API
      client.call(req)
      client.call(req)
      client.call(req)
    except UserCodeExecutionException as e:
      if not isinstance(e, UserCodeQuotaException):
        raise e

  def test_given_valid_request_receives_response(self):
    client, options = EchoGRPCCallerTestIT._get_client_and_options()

    try:
      req = EchoRequest(id=options.never_exceed_quota_id, payload=_PAYLOAD)

      response: EchoResponse = client.call(req)
      self.assertEqual(req.id, response.id)
      self.assertEqual(req.payload, response.payload)

    except UserCodeExecutionException:
      raise

  def test_given_exceeded_quota_should_raise(self):
    client, options = EchoGRPCCallerTestIT._get_client_and_options()

    req = EchoRequest(id=options.should_exceed_quota_id, payload=_PAYLOAD)

    self.assertRaises(UserCodeQuotaException, lambda: client.call(req))

  def test_not_found_should_raise(self):
    client, _ = EchoGRPCCallerTestIT._get_client_and_options()

    req = EchoRequest(id='i-dont-exist-quota-id', payload=_PAYLOAD)

    self.assertRaisesRegex(
        UserCodeExecutionException,
        "source not found",
        lambda: client.call(req))


class EchoHTTPCaller(Caller):
  """Implements ``Caller`` to call the ``EchoServiceGrpc``'s HTTP handler.
    The purpose of ``EchoHTTPCaller`` is to support integration tests.
    """
  def __init__(self, url: str):
    self.url = url + _HTTP_PATH

  def call(self, request: EchoRequest) -> EchoResponse:
    """Overrides ``Caller``'s call method invoking the
        ``EchoServiceGrpc``'s HTTP handler with an ``EchoRequest``, returning
        either a successful ``EchoResponse`` or throwing either a
        ``UserCodeExecutionException``, ``UserCodeTimeoutException``,
        or a ``UserCodeQuotaException``.
        """

    try:
      resp = urllib3.request(
          "POST",
          self.url,
          json={
              "id": request.id, "payload": str(request.payload, 'utf-8')
          },
          retries=False)

      if resp.status < 300:
        resp_body = resp.json()
        resp_id = resp_body['id']
        payload = resp_body['payload']
        return EchoResponse(id=resp_id, payload=bytes(payload, 'utf-8'))

      if resp.status == 429:  # Too Many Requests
        raise UserCodeQuotaException(resp.reason)

      raise UserCodeExecutionException(resp.reason)

    except urllib3.exceptions.HTTPError as e:
      raise UserCodeExecutionException(e)


class EchoHTTPCallerTestIT(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[EchoHTTPCaller, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    cls.client = EchoHTTPCaller(cls.options.http_endpoint_address)

  def setUp(self) -> None:
    client, options = EchoHTTPCallerTestIT._get_client_and_options()

    req = EchoRequest(id=options.should_exceed_quota_id, payload=_PAYLOAD)
    try:
      # The following is needed to exceed the API
      client.call(req)
      client.call(req)
      client.call(req)
    except UserCodeExecutionException as e:
      if not isinstance(e, UserCodeQuotaException):
        raise e

  @classmethod
  def _get_client_and_options(cls) -> Tuple[EchoHTTPCaller, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def test_given_valid_request_receives_response(self):
    client, options = EchoHTTPCallerTestIT._get_client_and_options()

    req = EchoRequest(id=options.never_exceed_quota_id, payload=_PAYLOAD)

    response: EchoResponse = client.call(req)

    self.assertEqual(req.id, response.id)
    self.assertEqual(req.payload, response.payload)

  def test_given_exceeded_quota_should_raise(self):
    client, options = EchoHTTPCallerTestIT._get_client_and_options()

    req = EchoRequest(id=options.should_exceed_quota_id, payload=_PAYLOAD)

    self.assertRaises(UserCodeQuotaException, lambda: client.call(req))

  def test_not_found_should_raise(self):
    client, _ = EchoHTTPCallerTestIT._get_client_and_options()

    req = EchoRequest(id='i-dont-exist-quota-id', payload=_PAYLOAD)
    self.assertRaisesRegex(
        UserCodeExecutionException, "Not Found", lambda: client.call(req))


if __name__ == '__main__':
  unittest.main(argv=sys.argv[:1])
