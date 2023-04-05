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

"""Unit tests for Recommendations AI transforms."""

from __future__ import absolute_import

import unittest

import mock

import apache_beam as beam
from apache_beam.metrics import MetricsFilter

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import recommendationengine
  from apache_beam.ml.gcp import recommendations_ai
except ImportError:
  recommendationengine = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


@unittest.skipIf(
    recommendationengine is None,
    "Recommendations AI dependencies not installed.")
class RecommendationsAICatalogItemTest(unittest.TestCase):
  def setUp(self):
    self._mock_client = mock.Mock()
    self._mock_client.create_catalog_item.return_value = (
        recommendationengine.CatalogItem())
    self.m2 = mock.Mock()
    self.m2.result.return_value = None
    self._mock_client.import_catalog_items.return_value = self.m2

    self._catalog_item = {
        "id": "12345",
        "title": "Sample laptop",
        "description": "Indisputably the most fantastic laptop ever created.",
        "language_code": "en",
        "category_hierarchies": [{
            "categories": ["Electronic", "Computers"]
        }]
    }

  def test_CreateCatalogItem(self):
    expected_counter = 1
    with mock.patch.object(recommendations_ai,
                           'get_recommendation_catalog_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()

      _ = (
          p | "Create data" >> beam.Create([self._catalog_item])
          | "Create CatalogItem" >>
          recommendations_ai.CreateCatalogItem(project="test"))

      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('api_calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_ImportCatalogItems(self):
    expected_counter = 1
    with mock.patch.object(recommendations_ai,
                           'get_recommendation_catalog_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()

      _ = (
          p | "Create data" >> beam.Create([
              (self._catalog_item["id"], self._catalog_item),
              (self._catalog_item["id"], self._catalog_item)
          ]) | "Create CatalogItems" >>
          recommendations_ai.ImportCatalogItems(project="test"))

      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('api_calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)


@unittest.skipIf(
    recommendationengine is None,
    "Recommendations AI dependencies not installed.")
class RecommendationsAIUserEventTest(unittest.TestCase):
  def setUp(self):
    self._mock_client = mock.Mock()
    self._mock_client.write_user_event.return_value = (
        recommendationengine.UserEvent())
    self.m2 = mock.Mock()
    self.m2.result.return_value = None
    self._mock_client.import_user_events.return_value = self.m2

    self._user_event = {
        "event_type": "page-visit", "user_info": {
            "visitor_id": "1"
        }
    }

  def test_CreateUserEvent(self):
    expected_counter = 1
    with mock.patch.object(recommendations_ai,
                           'get_recommendation_user_event_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()

      _ = (
          p | "Create data" >> beam.Create([self._user_event])
          | "Create UserEvent" >>
          recommendations_ai.WriteUserEvent(project="test"))

      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('api_calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_ImportUserEvents(self):
    expected_counter = 1
    with mock.patch.object(recommendations_ai,
                           'get_recommendation_user_event_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()

      _ = (
          p | "Create data" >> beam.Create([
              (self._user_event["user_info"]["visitor_id"], self._user_event),
              (self._user_event["user_info"]["visitor_id"], self._user_event)
          ]) | "Create UserEvents" >>
          recommendations_ai.ImportUserEvents(project="test"))

      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('api_calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)


@unittest.skipIf(
    recommendationengine is None,
    "Recommendations AI dependencies not installed.")
class RecommendationsAIPredictTest(unittest.TestCase):
  def setUp(self):
    self._mock_client = mock.Mock()
    self._mock_client.predict.return_value = [
        recommendationengine.PredictResponse()
    ]

    self._user_event = {
        "event_type": "page-visit", "user_info": {
            "visitor_id": "1"
        }
    }

  def test_Predict(self):
    expected_counter = 1
    with mock.patch.object(recommendations_ai,
                           'get_recommendation_prediction_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()

      _ = (
          p | "Create data" >> beam.Create([self._user_event])
          | "Prediction UserEvents" >> recommendations_ai.PredictUserEvent(
              project="test", placement_id="recently_viewed_default"))

      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('api_calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)


if __name__ == '__main__':
  unittest.main()
