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

"""End-to-End test for Vertex AI Remote Inference"""

import logging
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.examples.inference import vertex_ai_image_classification
  from apache_beam.examples.inference import vertex_ai_llm_text_classification
except ImportError as e:
  raise unittest.SkipTest(
      "Vertex AI model handler dependencies are not installed")

_INPUT = "gs://apache-beam-ml/testing/inputs/vertex_images/*/*.jpg"
_OUTPUT_DIR = "gs://apache-beam-ml/testing/outputs/vertex_images"
_FLOWER_ENDPOINT_ID = "5384055553544683520"
_LLM_ENDPOINT_ID = "9157860935048626176"
_ENDPOINT_PROJECT = "apache-beam-testing"
_ENDPOINT_REGION = "us-central1"
_ENDPOINT_NETWORK = "projects/844138762903/global/networks/beam-test-vpc"
# pylint: disable=line-too-long
_SUBNETWORK = "https://www.googleapis.com/compute/v1/projects/apache-beam-testing/regions/us-central1/subnetworks/beam-test-vpc"


class VertexAIInference(unittest.TestCase):
  @pytest.mark.uses_vertex_ai
  @pytest.mark.it_postcommit
  def test_vertex_ai_run_flower_image_classification(self):
    output_file = '/'.join([_OUTPUT_DIR, str(uuid.uuid4()), 'output.txt'])

    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {
        'input': _INPUT,
        'output': output_file,
        'endpoint_id': _FLOWER_ENDPOINT_ID,
        'endpoint_project': _ENDPOINT_PROJECT,
        'endpoint_region': _ENDPOINT_REGION,
        'endpoint_network': _ENDPOINT_NETWORK,
        'private': "True",
        'subnetwork': _SUBNETWORK,
    }
    vertex_ai_image_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts))
    self.assertEqual(FileSystems().exists(output_file), True)

  @pytest.mark.uses_vertex_ai
  @pytest.mark.it_postcommit
  def test_vertex_ai_run_llm_text_classification(self):
    output_file = '/'.join([_OUTPUT_DIR, str(uuid.uuid4()), 'output.txt'])

    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {
        'output': output_file,
        'endpoint_id': _LLM_ENDPOINT_ID,
        'endpoint_project': _ENDPOINT_PROJECT,
        'endpoint_region': _ENDPOINT_REGION
    }
    vertex_ai_llm_text_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts))
    self.assertEqual(FileSystems().exists(output_file), True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
