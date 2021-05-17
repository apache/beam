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

"""Integration tests for Google Cloud Video Intelligence API transforms."""

import logging
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments with google-cloud-dlp unavailable.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import dlp_v2
except ImportError:
  dlp_v2 = None
else:
  from apache_beam.ml.gcp.cloud_dlp import InspectForDetails
  from apache_beam.ml.gcp.cloud_dlp import MaskDetectedDetails
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

_LOGGER = logging.getLogger(__name__)

INSPECT_CONFIG = {"info_types": [{"name": "EMAIL_ADDRESS"}]}

DEIDENTIFY_CONFIG = {
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


def extract_inspection_results(response):
  yield beam.pvalue.TaggedOutput('info_type', response[0].info_type.name)


@unittest.skipIf(dlp_v2 is None, 'GCP dependencies are not installed')
class CloudDLPIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

  @pytest.mark.it_postcommit
  def test_deidentification(self):
    with TestPipeline(is_integration_test=True) as p:
      output = (
          p | beam.Create(["mary.sue@example.com"])
          | MaskDetectedDetails(
              project=self.project,
              deidentification_config=DEIDENTIFY_CONFIG,
              inspection_config=INSPECT_CONFIG))
      assert_that(output, equal_to(['####################']))

  @pytest.mark.it_postcommit
  def test_inspection(self):
    with TestPipeline(is_integration_test=True) as p:
      output = (
          p | beam.Create(["mary.sue@example.com"])
          | InspectForDetails(
              project=self.project, inspection_config=INSPECT_CONFIG)
          | beam.ParDo(extract_inspection_results).with_outputs(
              'quote', 'info_type'))
      assert_that(output.info_type, equal_to(['EMAIL_ADDRESS']), 'Type matches')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.WARN)
  unittest.main()
