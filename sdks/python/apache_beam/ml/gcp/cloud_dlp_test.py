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

from __future__ import absolute_import

import logging
import unittest

import mock

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.ml.gcp.cloud_dlp import InspectForDetails
from apache_beam.ml.gcp.cloud_dlp import MaskDetectedDetails
from apache_beam.ml.gcp.cloud_dlp import _DeidentifyFn
from apache_beam.ml.gcp.cloud_dlp import _InspectFn
from apache_beam.testing.test_pipeline import TestPipeline

_LOGGER = logging.getLogger(__name__)


class TestDeidentifyText(unittest.TestCase):
  def test_exception_raised_when_both_template_and_config_are_provided(self):
    deidentify_config = {
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
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p | MaskDetectedDetails('template-name-1', deidentify_config)

  def test_exception_raised_when_no_config_is_provided(self):
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p | MaskDetectedDetails()


class TestDeidentifyFn(unittest.TestCase):
  def test_deidentify_called(self):
    class ClientMock(object):
      def deidentify_content(self, *args, **kwargs):
        called = Metrics.counter('test_deidentify_text', 'called')
        called.inc()
        operation = mock.Mock()
        item = mock.Mock()
        item.value = [None]
        operation.item = item
        return operation

      def project_path(self, *args):
        return 'test'

    with mock.patch('google.cloud.dlp_v2.DlpServiceClient', ClientMock):
      p = TestPipeline()
      deidentify_config = {
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
      # pylint: disable=expression-not-assigned
      (
          p
          | beam.Create(['mary.sue@example.com', 'john.doe@example.com'])
          | beam.ParDo(_DeidentifyFn(config=deidentify_config)))
      result = p.run()
      result.wait_until_finish()
    called = result.metrics().query()['counters'][0]
    self.assertEqual(called.result, 2)


class TestInspectText(unittest.TestCase):
  def test_exception_raised_when_both_template_and_config_are_provided(self):
    inspect_config = {"info_types": [{"name": "EMAIL_ADDRESS"}]}
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p | InspectForDetails('template-name-1', inspect_config)

  def test_exception_raised_then_no_config_provided(self):
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        #pylint: disable=expression-not-assigned
        p | InspectForDetails()


class TestDeidentifyFn(unittest.TestCase):
  def test_inspect_called(self):
    class ClientMock(object):
      def inspect_content(self, *args, **kwargs):
        called = Metrics.counter('test_inspect_text', 'called')
        called.inc()
        operation = mock.Mock()
        operation.result = mock.Mock()
        operation.result.findings = [None]
        return operation

      def project_path(self, *args):
        return 'test'

    with mock.patch('google.cloud.dlp_v2.DlpServiceClient', ClientMock):
      p = TestPipeline()
      inspect_config = {"info_types": [{"name": "EMAIL_ADDRESS"}]}
      # pylint: disable=expression-not-assigned
      (
          p
          | beam.Create(['mary.sue@example.com', 'john.doe@example.com'])
          | beam.ParDo(_InspectFn(config=inspect_config)))
      result = p.run()
      result.wait_until_finish()
    called = result.metrics().query()['counters'][0]
    self.assertEqual(called.result, 2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
