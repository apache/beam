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
import typing
import unittest
from dataclasses import dataclass
from typing import Tuple
from typing import Union

import urllib3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.io.requestresponse import Caller
  from apache_beam.io.requestresponse import RequestResponseIO
  from apache_beam.io.requestresponse import UserCodeExecutionException
  from apache_beam.io.requestresponse import UserCodeQuotaException
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')

_HTTP_PATH = '/v1/echo'
_PAYLOAD = base64.b64encode(bytes('payload', 'utf-8'))
_HTTP_ENDPOINT_ADDRESS_FLAG = '--httpEndpointAddress'


class EchoITOptions(PipelineOptions):
  """Shared options for running integration tests on a deployed
      ``EchoServiceGrpc`` See https://github.com/apache/beam/tree/master/.test
      -infra/mock-apis#integration for details on how to acquire values
      required by ``EchoITOptions``.
      """
  @classmethod
  def _add_argparse_args(cls, parser) -> None:
    parser.add_argument(
        _HTTP_ENDPOINT_ADDRESS_FLAG,
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


@dataclass
class EchoResponse:
  id: str
  payload: bytes


# TODO(riteshghorse,damondouglas) replace Echo(Request|Response) with proto
#   generated classes from .test-infra/mock-apis:
class Request(typing.NamedTuple):
  id: str
  payload: bytes


class EchoHTTPCaller(Caller[Request, EchoResponse]):
  """Implements ``Caller`` to call the ``EchoServiceGrpc``'s HTTP handler.
    The purpose of ``EchoHTTPCaller`` is to support integration tests.
    """
  def __init__(self, url: str):
    self.url = url + _HTTP_PATH

  def __call__(self, request: Request, *args, **kwargs) -> EchoResponse:
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
      else:
        raise UserCodeExecutionException(resp.status, resp.reason, request)

    except urllib3.exceptions.HTTPError as e:
      raise UserCodeExecutionException(e)


class EchoHTTPCallerTestIT(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[EchoHTTPCaller, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    http_endpoint_address = cls.options.http_endpoint_address
    if not http_endpoint_address or http_endpoint_address == '':
      raise unittest.SkipTest(f'{_HTTP_ENDPOINT_ADDRESS_FLAG} is required.')

    cls.client = EchoHTTPCaller(http_endpoint_address)

  def setUp(self) -> None:
    client, options = EchoHTTPCallerTestIT._get_client_and_options()

    req = Request(id=options.should_exceed_quota_id, payload=_PAYLOAD)
    try:
      # The following is needed to exceed the API
      client(req)
      client(req)
      client(req)
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

    req = Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)

    response: EchoResponse = client(req)

    self.assertEqual(req.id, response.id)
    self.assertEqual(req.payload, response.payload)

  def test_given_exceeded_quota_should_raise(self):
    client, options = EchoHTTPCallerTestIT._get_client_and_options()

    req = Request(id=options.should_exceed_quota_id, payload=_PAYLOAD)

    self.assertRaises(UserCodeQuotaException, lambda: client(req))

  def test_not_found_should_raise(self):
    client, _ = EchoHTTPCallerTestIT._get_client_and_options()

    req = Request(id='i-dont-exist-quota-id', payload=_PAYLOAD)
    self.assertRaisesRegex(
        UserCodeExecutionException, "Not Found", lambda: client(req))

  def test_request_response_io(self):
    client, options = EchoHTTPCallerTestIT._get_client_and_options()
    req = Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      output = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'RRIO Transform' >> RequestResponseIO(client))
      self.assertIsNotNone(output)


if __name__ == '__main__':
  unittest.main(argv=sys.argv[:1])
