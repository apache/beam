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

"""End-to-End test for vLLM Inference"""

import logging
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import pytorch_inference_it_test
from apache_beam.testing.test_pipeline import TestPipeline

from apache_beam.examples.inference import vllm_text_completion


@pytest.mark.uses_vllm
@pytest.mark.it_postcommit
@pytest.mark.timeout(1800)
class HuggingFaceInference(unittest.TestCase):
  def test_vllm_text_completion(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_name = 'facebook/opt-125m'

    extra_opts = {
        'model': model_name,
        'output': output_file,
        'machine_type': 'n1-standard-4',
        'dataflow_service_options': ['worker_accelerator=type:nvidia-tesla-t4;count:1;install-nvidia-driver:5xx'],
        'worker_harness_container_image': 'gcr.io/apache-beam-testing/beam-ml/vllm:latest'
    }
    vllm_text_completion.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = pytorch_inference_it_test.process_outputs(
        filepath=output_file)

    self.assertEqual(len(predictions), 5, f'Expected 5 strings, received: {predictions}')

  def test_vllm_chat(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_name = 'facebook/opt-125m'

    extra_opts = {
        'model': model_name,
        'output': output_file,
        'chat': True
    }
    vllm_text_completion.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = pytorch_inference_it_test.process_outputs(
        filepath=output_file)

    self.assertEqual(len(predictions), 5, f'Expected 5 strings, received: {predictions}')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
