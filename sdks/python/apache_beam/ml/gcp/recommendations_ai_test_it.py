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

"""Integration tests for Recommendations AI transforms."""

from __future__ import absolute_import

import random
import unittest
from datetime import datetime

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_not_empty

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import recommendationengine
  from apache_beam.ml.gcp import recommendations_ai
except ImportError:
  recommendationengine = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

GCP_TEST_PROJECT = 'apache-beam-testing'

CATALOG_ITEM = {
    "id": f"aitest-{int(datetime.now().timestamp())}-"
    f"{int(random.randint(1,10000))}",
    "title": "Sample laptop",
    "description": "Indisputably the most fantastic laptop ever created.",
    "language_code": "en",
    "category_hierarchies": [{
        "categories": ["Electronic", "Computers"]
    }]
}


def extract_id(response):
  yield response["id"]


def extract_event_type(response):
  yield response["event_type"]


def extract_prediction(response):
  yield response[0]["results"]


@pytest.mark.it_postcommit
@unittest.skipIf(
    recommendationengine is None,
    "Recommendations AI dependencies not installed.")
class RecommendationAIIT(unittest.TestCase):
  test_ran = False

  def test_create_catalog_item(self):

    with TestPipeline(is_integration_test=True) as p:
      RecommendationAIIT.test_ran = True
      output = (
          p | 'Create data' >> beam.Create([CATALOG_ITEM])
          | 'Create CatalogItem' >>
          recommendations_ai.CreateCatalogItem(project=GCP_TEST_PROJECT)
          | beam.ParDo(extract_id) | beam.combiners.ToList())

      assert_that(output, equal_to([[CATALOG_ITEM["id"]]]))

  def test_create_user_event(self):
    USER_EVENT = {"event_type": "page-visit", "user_info": {"visitor_id": "1"}}

    with TestPipeline(is_integration_test=True) as p:
      RecommendationAIIT.test_ran = True
      output = (
          p | 'Create data' >> beam.Create([USER_EVENT]) | 'Create UserEvent' >>
          recommendations_ai.WriteUserEvent(project=GCP_TEST_PROJECT)
          | beam.ParDo(extract_event_type) | beam.combiners.ToList())

      assert_that(output, equal_to([[USER_EVENT["event_type"]]]))

  def test_predict(self):
    USER_EVENT = {"event_type": "page-visit", "user_info": {"visitor_id": "1"}}

    with TestPipeline(is_integration_test=True) as p:
      RecommendationAIIT.test_ran = True
      output = (
          p | 'Create data' >> beam.Create([USER_EVENT])
          | 'Predict UserEvent' >> recommendations_ai.PredictUserEvent(
              project=GCP_TEST_PROJECT, placement_id="recently_viewed_default")
          | beam.ParDo(extract_prediction))

      assert_that(output, is_not_empty())

  @classmethod
  def tearDownClass(cls):
    if not cls.test_ran:
      raise unittest.SkipTest('all test skipped')

    client = recommendationengine.CatalogServiceClient()
    parent = (
        f'projects/{GCP_TEST_PROJECT}/locations/'
        'global/catalogs/default_catalog')
    for item in list(client.list_catalog_items(parent=parent)):
      client.delete_catalog_item(
          name=f"projects/{GCP_TEST_PROJECT}/locations/global/catalogs/"
          f"default_catalog/catalogItems/{item.id}")


if __name__ == '__main__':
  unittest.main()
