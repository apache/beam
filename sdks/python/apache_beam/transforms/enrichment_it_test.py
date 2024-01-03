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
from typing import Tuple
from typing import Union

import pytest
import urllib3

import apache_beam as beam
from apache_beam.io.requestresponse import UserCodeExecutionException
from apache_beam.io.requestresponse import UserCodeQuotaException
from apache_beam.io.requestresponse_it_test import _PAYLOAD
from apache_beam.io.requestresponse_it_test import EchoITOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment import EnrichmentSourceHandler


class _Request(NamedTuple):
  """Simple request type to store id and payload for requests."""
  id: str  # mock API quota id
  payload: bytes  # byte payload


def _custom_join(element):
  """custom_join returns the id and resp_payload along with a timestamp"""
  right_dict = element[1].as_dict()
  right_dict['timestamp'] = time.time()
  return beam.Row(**right_dict)


class SampleHTTPEnrichment(EnrichmentSourceHandler[_Request, beam.Row]):
  """Implements ``EnrichmentSourceHandler`` to call the ``EchoServiceGrpc``'s
  HTTP handler.
  """
  def __init__(self, url: str):
    self.url = url + '/v1/echo'  # append path to the mock API.

  def __call__(self, request: _Request, *args, **kwargs):
    """Overrides ``Caller``'s call method invoking the
    ``EchoServiceGrpc``'s HTTP handler with an ``_Request``, returning
    either a successful ``Tuple[beam.Row,beam.Row]`` or throwing either a
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
        yield (
            beam.Row(id=request.id, payload=request.payload),
            beam.Row(id=resp_id, resp_payload=bytes(payload, 'utf-8')))

      if resp.status == 429:  # Too Many Requests
        raise UserCodeQuotaException(resp.reason)
      elif resp.status != 200:
        raise UserCodeExecutionException(resp.status, resp.reason, request)

    except urllib3.exceptions.HTTPError as e:
      raise UserCodeExecutionException(e)


class ValidateFields(beam.DoFn):
  """ValidateFields validates if a PCollection of `beam.Row`
  has certain fields."""
  def __init__(self, fields):
    self._fields = fields

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(element_dict.keys()) != 3:
      raise BeamAssertException(
          "Expected three fields in enriched PCollection:"
          " id, payload and resp_payload")

    for field in self._fields:
      if field not in element_dict or element_dict[field] is None:
        raise BeamAssertException(f"Expected a not None field: {field}")


@pytest.mark.it_postcommit
class TestEnrichment(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[SampleHTTPEnrichment, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    http_endpoint_address = 'http://10.138.0.32:8080'
    cls.client = SampleHTTPEnrichment(http_endpoint_address)

  @classmethod
  def _get_client_and_options(
      cls) -> Tuple[SampleHTTPEnrichment, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def test_http_enrichment(self):
    """Tests Enrichment Transform against the Mock-API HTTP endpoint
    with the default cross join."""
    client, options = TestEnrichment._get_client_and_options()
    req = _Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    fields = ['id', 'payload', 'resp_payload']
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client)
          | 'Assert Fields' >> beam.ParDo(ValidateFields(fields=fields)))

  def test_http_enrichment_custom_join(self):
    """Tests Enrichment Transform against the Mock-API HTTP endpoint
    with a custom join function."""
    client, options = TestEnrichment._get_client_and_options()
    req = _Request(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    fields = ['id', 'resp_payload', 'timestamp']
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client, join_fn=_custom_join)
          | 'Assert Fields' >> beam.ParDo(ValidateFields(fields=fields)))


if __name__ == '__main__':
  unittest.main()
