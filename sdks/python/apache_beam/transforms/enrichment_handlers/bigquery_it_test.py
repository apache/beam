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
import logging
import unittest
from unittest.mock import MagicMock

import pytest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import BadRequest
  from testcontainers.redis import RedisContainer
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery import \
    BigQueryEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store_it_test import \
    ValidateResponse
except ImportError:
  raise unittest.SkipTest(
      'Google Cloud BigQuery dependencies are not installed.')

_LOGGER = logging.getLogger(__name__)


def query_fn(row: beam.Row):
  query = (
      "SELECT * FROM "
      "`apache-beam-testing.my_ecommerce.product_details`"
      " WHERE id = '{}'".format(row.id))  # type: ignore[attr-defined]
  return query


def condition_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


@pytest.mark.uses_testcontainer
class TestBigQueryEnrichmentIT(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'
    self.condition_template = "id = '{}'"
    self.table_name = "`apache-beam-testing.my_ecommerce.product_details`"
    self.retries = 3
    self._start_container()

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
          _LOGGER.error(
              'Unable to start redis container for BigQuery '
              ' enrichment tests.')
          raise e

  def tearDown(self) -> None:
    self.container.stop()
    self.client = None

  def test_bigquery_enrichment(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    fields = ['id']
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        fields=fields,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_query_fn(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(project=self.project, query_fn=query_fn)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_condition_value_fn(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_condition_without_batch(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_bad_request(self):
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        column_names=['wrong_column'],
        condition_value_fn=condition_value_fn,
    )
    with self.assertRaises(BadRequest):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_bigquery_enrichment_with_redis(self):
    """
    In this test, we run two pipelines back to back.

    In the first pipeline, we run a simple BigQuery enrichment pipeline
    with zero cache records. Therefore, it makes call to the source
    and ultimately writes to the cache with a TTL of 300 seconds.

    For the second pipeline, we mock the
    `BigQueryEnrichmentHandler`'s `__call__` method to always
    return a `None` response. However, this change won't impact the second
    pipeline because the Enrichment transform first checks the cache to fulfill
    requests. Since all requests are cached, it will return from there without
    making calls to the BigQuery service.
    """
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port)
          | beam.ParDo(ValidateResponse(expected_fields)))

    # manually check cache entry
    c = coders.StrUtf8Coder()
    for req in requests:
      key = handler.get_cache_key(req)
      response = self.client.get(c.encode(key))
      if not response:
        raise ValueError("No cache entry found for %s" % key)

    actual = BigQueryEnrichmentHandler.__call__
    BigQueryEnrichmentHandler.__call__ = MagicMock(
        return_value=(
            beam.Row(
                id='15816',
                name='low profile dyed cotton twill cap - putty w39s55d',
                quantity=1),
            beam.Row()))

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port)
          | beam.ParDo(ValidateResponse(expected_fields)))
    BigQueryEnrichmentHandler.__call__ = actual


if __name__ == '__main__':
  unittest.main()
