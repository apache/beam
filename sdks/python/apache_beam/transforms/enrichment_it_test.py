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
from typing import Tuple
from typing import Union

import urllib3

import apache_beam as beam
from apache_beam.io.requestresponseio import UserCodeExecutionException
from apache_beam.io.requestresponseio import UserCodeQuotaException
from apache_beam.io.requestresponseio_it_test import _PAYLOAD
from apache_beam.io.requestresponseio_it_test import EchoITOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment import EnrichmentSourceHandler


class SampleHTTPEnrichment(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """Implements ``EnrichmentSourceHandler`` to call the ``EchoServiceGrpc``'s
  HTTP handler.
  """
  def __init__(self, url: str):
    self.url = url + '/v1/echo'  # append path to the mock API.

  def __call__(self, request: beam.Row, *args,
               **kwargs) -> Tuple[beam.Row, beam.Row]:
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
        return (
            beam.Row(id=request.id, payload=request.payload),
            beam.Row(id=resp_id, resp_payload=bytes(payload, 'utf-8')))

      if resp.status == 429:  # Too Many Requests
        raise UserCodeQuotaException(resp.reason)
      elif resp.status != 200:
        raise UserCodeExecutionException(resp.status, resp.reason, request)

    except urllib3.exceptions.HTTPError as e:
      raise UserCodeExecutionException(e)


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
    client, options = TestEnrichment._get_client_and_options()
    req = beam.Row(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      output = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client))
      self.assertIsNotNone(output)


if __name__ == '__main__':
  unittest.main()
