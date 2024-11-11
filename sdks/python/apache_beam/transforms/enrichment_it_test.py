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
import time
import unittest
from typing import NamedTuple
from typing import Union

import pytest
import urllib3

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException

# pylint: disable=ungrouped-imports
try:
  from apache_beam.io.requestresponse import UserCodeExecutionException
  from apache_beam.io.requestresponse import UserCodeQuotaException
  from apache_beam.io.requestresponse_it_test import _PAYLOAD
  from apache_beam.io.requestresponse_it_test import EchoITOptions
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment import EnrichmentSourceHandler
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')


class Request(NamedTuple):
  id: str
  payload: bytes


def _custom_join(left, right):
  """custom_join returns the id and resp_payload along with a timestamp"""
  right['timestamp'] = time.time()
  return beam.Row(**right)


class SampleHTTPEnrichment(EnrichmentSourceHandler[Request, beam.Row]):
  """Implements ``EnrichmentSourceHandler`` to call the ``EchoServiceGrpc``'s
  HTTP handler.
  """
  def __init__(self, url: str):
    self.url = url + '/v1/echo'  # append path to the mock API.

  def __call__(self, request: Request, *args, **kwargs):
    """Overrides ``Caller``'s call method invoking the
    ``EchoServiceGrpc``'s HTTP handler with an `dict`, returning
    either a successful ``tuple[dict,dict]`` or throwing either a
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
        return (
            request, beam.Row(id=resp_id, resp_payload=bytes(payload, 'utf-8')))

      if resp.status == 429:  # Too Many Requests
        raise UserCodeQuotaException(resp.reason)
      elif resp.status != 200:
        raise UserCodeExecutionException(resp.status, resp.reason, request)

    except urllib3.exceptions.HTTPError as e:
      raise UserCodeExecutionException(e)


class ValidateFields(beam.DoFn):
  """ValidateFields validates if a PCollection of `beam.Row`
  has certain fields."""
  def __init__(self, n_fields: int, fields: list[str]):
    self.n_fields = n_fields
    self._fields = fields

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(element_dict.keys()) != self.n_fields:
      raise BeamAssertException(
          "Expected %d fields in enriched PCollection:"
          " id, payload and resp_payload" % self.n_fields)

    for field in self._fields:
      if field not in element_dict or element_dict[field] is None:
        raise BeamAssertException(f"Expected a not None field: {field}")


@pytest.mark.uses_mock_api
class TestEnrichment(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[SampleHTTPEnrichment, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    http_endpoint_address = cls.options.http_endpoint_address
    if not http_endpoint_address or http_endpoint_address == '':
      raise unittest.SkipTest('HTTP_ENDPOINT_ADDRESS is required.')
    cls.client = SampleHTTPEnrichment(http_endpoint_address)

  @classmethod
  def _get_client_and_options(
      cls) -> tuple[SampleHTTPEnrichment, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def test_http_enrichment(self):
    """Tests Enrichment Transform against the Mock-API HTTP endpoint
    with the default cross join."""
    client, options = TestEnrichment._get_client_and_options()
    req = Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    fields = ['id', 'payload', 'resp_payload']
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client)
          | 'Assert Fields' >> beam.ParDo(
              ValidateFields(len(fields), fields=fields)))

  def test_http_enrichment_custom_join(self):
    """Tests Enrichment Transform against the Mock-API HTTP endpoint
    with a custom join function."""
    client, options = TestEnrichment._get_client_and_options()
    req = Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    fields = ['id', 'resp_payload', 'timestamp']
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client, join_fn=_custom_join)
          | 'Assert Fields' >> beam.ParDo(
              ValidateFields(len(fields), fields=fields)))


if __name__ == '__main__':
  unittest.main()
