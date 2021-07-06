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

from nose.plugins.attrib import attr

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


def extract_id(response):
  yield response["id"]


def extract_event_type(response):
  yield response["event_type"]


def extract_prediction(response):
  yield response[0]["results"]


@attr('IT')
@unittest.skipIf(
    recommendationengine is None,
    "Recommendations AI dependencies not installed.")
class RecommendationAIIT(unittest.TestCase):
  def test_create_catalog_item(self):

    CATALOG_ITEM = {
        "id": str(int(random.randrange(100000))),
        "title": "Sample laptop",
        "description": "Indisputably the most fantastic laptop ever created.",
        "language_code": "en",
        "category_hierarchies": [{
            "categories": ["Electronic", "Computers"]
        }]
    }

    with TestPipeline(is_integration_test=True) as p:
      output = (
          p | 'Create data' >> beam.Create([CATALOG_ITEM])
          | 'Create CatalogItem' >>
          recommendations_ai.CreateCatalogItem(project=GCP_TEST_PROJECT)
          | beam.ParDo(extract_id) | beam.combiners.ToList())

      assert_that(output, equal_to([[CATALOG_ITEM["id"]]]))

  def test_create_user_event(self):
    USER_EVENT = {"event_type": "page-visit", "user_info": {"visitor_id": "1"}}

    with TestPipeline(is_integration_test=True) as p:
      output = (
          p | 'Create data' >> beam.Create([USER_EVENT]) | 'Create UserEvent' >>
          recommendations_ai.WriteUserEvent(project=GCP_TEST_PROJECT)
          | beam.ParDo(extract_event_type) | beam.combiners.ToList())

      assert_that(output, equal_to([[USER_EVENT["event_type"]]]))

  def test_predict(self):
    USER_EVENT = {"event_type": "page-visit", "user_info": {"visitor_id": "1"}}

    with TestPipeline(is_integration_test=True) as p:
      output = (
          p | 'Create data' >> beam.Create([USER_EVENT])
          | 'Predict UserEvent' >> recommendations_ai.PredictUserEvent(
              project=GCP_TEST_PROJECT, placement_id="recently_viewed_default")
          | beam.ParDo(extract_prediction))

      assert_that(output, is_not_empty())


if __name__ == '__main__':
  print(recommendationengine.CatalogItem.__module__)
  unittest.main()
