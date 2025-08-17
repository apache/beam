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

"""End-to-End test for Gemini Remote Inference"""

import logging
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.examples.inference import gemini_text_classification
except ImportError as e:
  raise unittest.SkipTest("Gemini model handler dependencies are not installed")

_OUTPUT_DIR = "gs://apache-beam-ml/testing/outputs/gemini"
_TEST_PROJECT = "apache-beam-testing"
_TEST_REGION = "us-central1"


class GeminiInference(unittest.TestCase):
  @pytest.mark.gemini_postcommit
  def test_gemini_text_classification(self):
    output_file = '/'.join([_OUTPUT_DIR, str(uuid.uuid4()), 'output.txt'])

    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {
        'output': output_file,
        'cloud_project': _TEST_PROJECT,
        'cloud_region': _TEST_REGION
    }
    gemini_text_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts))
    self.assertEqual(FileSystems().exists(output_file), True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
