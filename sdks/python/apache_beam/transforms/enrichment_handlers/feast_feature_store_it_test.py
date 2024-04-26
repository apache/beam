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

"""Tests Feast feature store enrichment handler for enrichment transform.

See https://s.apache.org/feast-enrichment-test-setup
to set up test feast feature repository.
"""

import unittest
from typing import Any
from typing import Mapping

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.feast_feature_store import \
    FeastFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store_it_test import ValidateResponse  # pylint: disable=line-too-long
except ImportError:
  raise unittest.SkipTest(
      'Feast feature store test dependencies are not installed.')


def _entity_row_fn(request: beam.Row) -> Mapping[str, Any]:
  entity_value = request.user_id  # type: ignore[attr-defined]
  return {'user_id': entity_value}


@pytest.mark.uses_feast
class TestFeastEnrichmentHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.feature_store_yaml_file = (
        'gs://apache-beam-testing-enrichment/'
        'feast-feature-store/repos/ecommerce/'
        'feature_repo/feature_store.yaml')
    self.feature_service_name = 'demograph_service'

  def test_feast_enrichment(self):
    requests = [
        beam.Row(user_id=2, product_id=1),
        beam.Row(user_id=6, product_id=2),
        beam.Row(user_id=9, product_id=3),
    ]
    expected_fields = [
        'user_id', 'product_id', 'state', 'country', 'gender', 'age'
    ]
    handler = FeastFeatureStoreEnrichmentHandler(
        entity_id='user_id',
        feature_store_yaml_path=self.feature_store_yaml_file,
        feature_service_name=self.feature_service_name,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_feast_enrichment_bad_feature_service_name(self):
    """Test raising an error when a bad feature service name is given."""
    requests = [
        beam.Row(user_id=1, product_id=1),
    ]
    handler = FeastFeatureStoreEnrichmentHandler(
        entity_id='user_id',
        feature_store_yaml_path=self.feature_store_yaml_file,
        feature_service_name="bad_name",
    )

    with self.assertRaises(RuntimeError):
      test_pipeline = beam.Pipeline()
      _ = (test_pipeline | beam.Create(requests) | Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_feast_enrichment_with_lambda(self):
    requests = [
        beam.Row(user_id=2, product_id=1),
        beam.Row(user_id=6, product_id=2),
        beam.Row(user_id=9, product_id=3),
    ]
    expected_fields = [
        'user_id', 'product_id', 'state', 'country', 'gender', 'age'
    ]
    handler = FeastFeatureStoreEnrichmentHandler(
        feature_store_yaml_path=self.feature_store_yaml_file,
        feature_service_name=self.feature_service_name,
        entity_row_fn=_entity_row_fn,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))


if __name__ == '__main__':
  unittest.main()
