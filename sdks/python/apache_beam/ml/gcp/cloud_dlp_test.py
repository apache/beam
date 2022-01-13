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

"""Unit tests for Google Cloud Video Intelligence API transforms."""

import logging
import unittest

import mock

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments with google-cloud-dlp unavailable.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import dlp_v2
except ImportError:
  dlp_v2 = None
else:
  from apache_beam.ml.gcp.cloud_dlp import InspectForDetails
  from apache_beam.ml.gcp.cloud_dlp import MaskDetectedDetails
  from apache_beam.ml.gcp.cloud_dlp import _DeidentifyFn
  from apache_beam.ml.gcp.cloud_dlp import _InspectFn
  from google.cloud.dlp_v2.types import dlp
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

_LOGGER = logging.getLogger(__name__)


@unittest.skipIf(dlp_v2 is None, 'GCP dependencies are not installed')
class TestDeidentifyText(unittest.TestCase):
  def test_exception_raised_when_no_config_is_provided(self):
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p | MaskDetectedDetails()


@unittest.skipIf(dlp_v2 is None, 'GCP dependencies are not installed')
class TestDeidentifyFn(unittest.TestCase):
  def test_deidentify_called(self):
    class ClientMock(object):
      def deidentify_content(self, *args, **kwargs):
        # Check that we can marshal a valid request.
        dlp.DeidentifyContentRequest(kwargs['request'])

        called = Metrics.counter('test_deidentify_text', 'called')
        called.inc()
        operation = mock.Mock()
        item = mock.Mock()
        item.value = [None]
        operation.item = item
        return operation

      def common_project_path(self, *args):
        return 'test'

    with mock.patch('google.cloud.dlp_v2.DlpServiceClient', ClientMock):
      p = TestPipeline()
      config = {
          "deidentify_config": {
              "info_type_transformations": {
                  "transformations": [{
                      "primitive_transformation": {
                          "character_mask_config": {
                              "masking_character": '#'
                          }
                      }
                  }]
              }
          }
      }
      # pylint: disable=expression-not-assigned
      (
          p
          | beam.Create(['mary.sue@example.com', 'john.doe@example.com'])
          | beam.ParDo(_DeidentifyFn(config=config)))
      result = p.run()
      result.wait_until_finish()
    called = result.metrics().query()['counters'][0]
    self.assertEqual(called.result, 2)


@unittest.skipIf(dlp_v2 is None, 'GCP dependencies are not installed')
class TestInspectText(unittest.TestCase):
  def test_exception_raised_then_no_config_provided(self):
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        #pylint: disable=expression-not-assigned
        p | InspectForDetails()


@unittest.skipIf(dlp_v2 is None, 'GCP dependencies are not installed')
class TestInspectFn(unittest.TestCase):
  def test_inspect_called(self):
    class ClientMock(object):
      def inspect_content(self, *args, **kwargs):
        # Check that we can marshal a valid request.
        dlp.InspectContentRequest(kwargs['request'])

        called = Metrics.counter('test_inspect_text', 'called')
        called.inc()
        operation = mock.Mock()
        operation.result = mock.Mock()
        operation.result.findings = [None]
        return operation

      def common_project_path(self, *args):
        return 'test'

    with mock.patch('google.cloud.dlp_v2.DlpServiceClient', ClientMock):
      p = TestPipeline()
      config = {"inspect_config": {"info_types": [{"name": "EMAIL_ADDRESS"}]}}
      # pylint: disable=expression-not-assigned
      (
          p
          | beam.Create(['mary.sue@example.com', 'john.doe@example.com'])
          | beam.ParDo(_InspectFn(config=config)))
      result = p.run()
      result.wait_until_finish()
      called = result.metrics().query()['counters'][0]
      self.assertEqual(called.result, 2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
