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
from apache_beam.testing.util import BeamAssertException

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import NotFound
  from testcontainers.redis import RedisContainer
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import \
    VertexAIFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import \
    VertexAIFeatureStoreLegacyEnrichmentHandler
except ImportError:
  raise unittest.SkipTest(
      'VertexAI Feature Store test dependencies '
      'are not installed.')

_LOGGER = logging.getLogger(__name__)


class ValidateResponse(beam.DoFn):
  """ValidateResponse validates if a PCollection of `beam.Row`
  has the required fields."""
  def __init__(self, expected_fields):
    self.expected_fields = expected_fields

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(self.expected_fields) != len(element_dict.keys()):
      raise BeamAssertException(
          "Expected %d fields in enriched PCollection:" %
          len(self.expected_fields))
    for field in self.expected_fields:
      if field not in element_dict:
        raise BeamAssertException(
            f"Expected to fetch field: {field}"
            f"from feature store")


@pytest.mark.uses_testcontainer
class TestVertexAIFeatureStoreHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'
    self.location = 'us-central1'
    self.feature_store_name = "the_look_demo_unique"
    self.feature_view_name = "registry_product"
    self.entity_type_name = "entity_id"
    self.api_endpoint = "us-central1-aiplatform.googleapis.com"
    self.feature_ids = ['title', 'genres']
    self.retries = 3
    self._start_container()

  def _start_container(self):
    for i in range(3):
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

  def tearDown(self) -> None:
    self.container.stop()
    self.client = None

  def test_vertex_ai_feature_store_bigtable_serving_enrichment(self):
    requests = [
        beam.Row(entity_id="847", name='cardigan jacket'),
        beam.Row(entity_id="16050", name='stripe t-shirt'),
    ]
    expected_fields = [
        'entity_id',
        'bad_order_count',
        'good_order_count',
        'feature_timestamp',
        'category',
        'cost',
        'brand',
        'retail_price',
        'name'
    ]
    handler = VertexAIFeatureStoreEnrichmentHandler(
        project=self.project,
        location=self.location,
        api_endpoint=self.api_endpoint,
        feature_store_name=self.feature_store_name,
        feature_view_name=self.feature_view_name,
        row_key=self.entity_type_name,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_vertex_ai_feature_store_wrong_name(self):
    requests = [
        beam.Row(entity_id="847", name='cardigan jacket'),
        beam.Row(entity_id="16050", name='stripe t-shirt'),
    ]

    with self.assertRaises(NotFound):
      handler = VertexAIFeatureStoreEnrichmentHandler(
          project=self.project,
          location=self.location,
          api_endpoint=self.api_endpoint,
          feature_store_name="incorrect_name",
          feature_view_name=self.feature_view_name,
          row_key=self.entity_type_name,
      )
      test_pipeline = beam.Pipeline()
      _ = (test_pipeline | beam.Create(requests) | Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_vertex_ai_feature_store_bigtable_serving_enrichment_bad(self):
    requests = [
        beam.Row(entity_id="ui", name="fred perry men\'s sharp stripe t-shirt")
    ]
    handler = VertexAIFeatureStoreEnrichmentHandler(
        project=self.project,
        location=self.location,
        api_endpoint=self.api_endpoint,
        feature_store_name=self.feature_store_name,
        feature_view_name=self.feature_view_name,
        row_key=self.entity_type_name,
        exception_level=ExceptionLevel.RAISE,
    )
    with self.assertRaises(ValueError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrich w/ VertexAI" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_vertex_ai_legacy_feature_store_enrichment(self):
    requests = [
        beam.Row(entity_id="movie_02", title="The Shining"),
        beam.Row(entity_id="movie_04", title='The Dark Knight'),
    ]
    expected_fields = ['entity_id', 'title', 'genres']
    feature_store_id = "movie_prediction_unique"
    entity_type_id = "movies"
    handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
        project=self.project,
        location=self.location,
        api_endpoint=self.api_endpoint,
        feature_store_id=feature_store_id,
        entity_type_id=entity_type_id,
        feature_ids=self.feature_ids,
        row_key=self.entity_type_name,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_vertex_ai_legacy_feature_store_enrichment_bad(self):
    requests = [
        beam.Row(entity_id="12345", title="The Shining"),
    ]
    feature_store_id = "movie_prediction_unique"
    entity_type_id = "movies"
    handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
        project=self.project,
        location=self.location,
        api_endpoint=self.api_endpoint,
        feature_store_id=feature_store_id,
        entity_type_id=entity_type_id,
        feature_ids=self.feature_ids,
        row_key=self.entity_type_name,
        exception_level=ExceptionLevel.RAISE,
    )

    with self.assertRaises(ValueError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_vertex_ai_legacy_feature_store_invalid_featurestore(self):
    requests = [
        beam.Row(entity_id="movie_02", title="The Shining"),
    ]
    feature_store_id = "invalid_name"
    entity_type_id = "movies"

    with self.assertRaises(NotFound):
      handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
          project=self.project,
          location=self.location,
          api_endpoint=self.api_endpoint,
          feature_store_id=feature_store_id,
          entity_type_id=entity_type_id,
          feature_ids=self.feature_ids,
          row_key=self.entity_type_name,
          exception_level=ExceptionLevel.RAISE,
      )
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_feature_store_enrichment_with_redis(self):
    """
    In this test, we run two pipelines back to back.

    In the first pipeline, we run a simple feature store enrichment pipeline
    with zero cache records. Therefore, it makes call to the source
    and ultimately writes to the cache with a TTL of 300 seconds.

    For the second pipeline, we mock the
    `VertexAIFeatureStoreEnrichmentHandler`'s `__call__` method to always
    return a `None` response. However, this change won't impact the second
    pipeline because the Enrichment transform first checks the cache to fulfill
    requests. Since all requests are cached, it will return from there without
    making calls to the feature store instance.
    """
    expected_fields = ['entity_id', 'title', 'genres']
    requests = [
        beam.Row(entity_id="movie_02", title="The Shining"),
        beam.Row(entity_id="movie_04", title="The Dark Knight"),
    ]
    feature_store_id = "movie_prediction_unique"
    entity_type_id = "movies"
    handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
        project=self.project,
        location=self.location,
        api_endpoint=self.api_endpoint,
        feature_store_id=feature_store_id,
        entity_type_id=entity_type_id,
        feature_ids=self.feature_ids,
        row_key=self.entity_type_name,
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

    actual = VertexAIFeatureStoreLegacyEnrichmentHandler.__call__
    VertexAIFeatureStoreLegacyEnrichmentHandler.__call__ = MagicMock(
        return_value=(
            beam.Row(entity_id="movie_02", title="The Shining"), beam.Row()))

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port)
          | beam.ParDo(ValidateResponse(expected_fields)))
    VertexAIFeatureStoreLegacyEnrichmentHandler.__call__ = actual


if __name__ == '__main__':
  unittest.main()
