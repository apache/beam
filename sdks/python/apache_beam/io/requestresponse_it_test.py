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
import logging
import sys
import typing
import unittest
from dataclasses import dataclass
from typing import Tuple
from typing import Union

import pytest
import urllib3

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from testcontainers.redis import RedisContainer
  from apache_beam.io.requestresponse import Caller
  from apache_beam.io.requestresponse import RedisCache
  from apache_beam.io.requestresponse import RequestResponseIO
  from apache_beam.io.requestresponse import UserCodeExecutionException
  from apache_beam.io.requestresponse import UserCodeQuotaException
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')

_HTTP_PATH = '/v1/echo'
_PAYLOAD = base64.b64encode(bytes('payload', 'utf-8'))
_HTTP_ENDPOINT_ADDRESS_FLAG = '--httpEndpointAddress'

_LOGGER = logging.getLogger(__name__)


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
        default='http://10.138.0.32:8080',
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
      http = urllib3.PoolManager()
      resp = http.request(
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


class ValidateResponse(beam.DoFn):
  """Validates response received from Mock API server."""
  def process(self, element, *args, **kwargs):
    if (element.id != 'echo-should-never-exceed-quota' or
        element.payload != _PAYLOAD):
      raise ValueError(
          'got EchoResponse(id: %s, payload: %s), want '
          'EchoResponse(id: echo-should-never-exceed-quota, '
          'payload: %s' % (element.id, element.payload, _PAYLOAD))


@pytest.mark.uses_mock_api
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

  @classmethod
  def _get_client_and_options(cls) -> Tuple[EchoHTTPCaller, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def test_request_response_io(self):
    client, options = EchoHTTPCallerTestIT._get_client_and_options()
    req = Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      output = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'RRIO Transform' >> RequestResponseIO(client)
          | 'Validate' >> beam.ParDo(ValidateResponse()))
      self.assertIsNotNone(output)


class ValidateCacheResponses(beam.DoFn):
  """Validates that the responses are fetched from the cache."""
  def process(self, element, *args, **kwargs):
    if not element[1] or 'cached-' not in element[1]:
      raise ValueError(
          'responses not fetched from cache even though cache '
          'entries are present.')


class ValidateCallerResponses(beam.DoFn):
  """Validates that the responses are fetched from the caller."""
  def process(self, element, *args, **kwargs):
    if not element[1] or 'ACK-' not in element[1]:
      raise ValueError('responses not fetched from caller when they should.')


class FakeCallerForCache(Caller[str, str]):
  def __init__(self, use_cache: bool = False):
    self.use_cache = use_cache

  def __enter__(self):
    pass

  def __call__(self, element, *args, **kwargs):
    if self.use_cache:
      return None, None

    return element, 'ACK-{element}'

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass


@pytest.mark.uses_testcontainer
class TestRedisCache(unittest.TestCase):
  def setUp(self) -> None:
    self.retries = 3
    self._start_container()

  def test_rrio_cache_all_miss(self):
    """Cache is empty so all responses are fetched from caller."""
    caller = FakeCallerForCache()
    req = ['redis', 'cachetools', 'memcache']
    cache = RedisCache(
        self.host,
        self.port,
        time_to_live=30,
        request_coder=coders.StrUtf8Coder(),
        response_coder=coders.StrUtf8Coder())
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | beam.Create(req)
          | RequestResponseIO(caller, cache=cache)
          | beam.ParDo(ValidateCallerResponses()))

  def test_rrio_cache_all_hit(self):
    """Validate that records are fetched from cache."""
    caller = FakeCallerForCache()
    requests = ['foo', 'bar']
    responses = ['cached-foo', 'cached-bar']
    coder = coders.StrUtf8Coder()
    for i in range(len(requests)):
      enc_req = coder.encode(requests[i])
      enc_resp = coder.encode(responses[i])
      self.client.setex(enc_req, 120, enc_resp)
    cache = RedisCache(
        self.host,
        self.port,
        time_to_live=30,
        request_coder=coders.StrUtf8Coder(),
        response_coder=coders.StrUtf8Coder())
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | beam.Create(requests)
          | RequestResponseIO(caller, cache=cache)
          | beam.ParDo(ValidateCacheResponses()))

  def test_rrio_cache_miss_and_hit(self):
    """Run two back-to-back pipelines, one with pulling the data from caller
    and other from the cache."""
    caller = FakeCallerForCache()
    requests = ['beam', 'flink', 'spark']
    cache = RedisCache(
        self.host,
        self.port,
        request_coder=coders.StrUtf8Coder(),
        response_coder=coders.StrUtf8Coder())
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | beam.Create(requests)
          | RequestResponseIO(caller, cache=cache)
          | beam.ParDo(ValidateCallerResponses()))

    caller = FakeCallerForCache(use_cache=True)
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | beam.Create(requests)
          | RequestResponseIO(caller, cache=cache)
          | beam.ParDo(ValidateCallerResponses()))

  def test_rrio_no_coder_exception(self):
    caller = FakeCallerForCache()
    requests = ['beam', 'flink', 'spark']
    cache = RedisCache(self.host, self.port)
    with self.assertRaises(ValueError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | beam.Create(requests)
          | RequestResponseIO(caller, cache=cache))
      res = test_pipeline.run()
      res.wait_until_finish()

  def tearDown(self) -> None:
    self.container.stop()

  def _start_container(self):
    for i in range(self.retries):
      try:
        self.container = RedisContainer(image='redis:7.2.4')
        self.container.start()
        self.host = self.container.get_container_host_ip()
        self.port = self.container.get_exposed_port(6379)
        self.client = self.container.get_client()
        break
      except Exception as e:
        if i == self.retries - 1:
          _LOGGER.error('Unable to start redis container for RRIO tests.')
          raise e


if __name__ == '__main__':
  unittest.main(argv=sys.argv[:1])
