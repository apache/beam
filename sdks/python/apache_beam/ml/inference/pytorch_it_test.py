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

# pylint: skip-file

"""End-to-End test for MNIST classification example"""
import tempfile

import pytest
import shutil
import unittest
import uuid
from typing import List

from apache_beam.ml.inference.examples import pytorch_image_classification
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None


class PyTorchInference(unittest.TestCase):
  def setUp(self) -> None:
    self._temp_dir = None

  def tearDown(self) -> None:
    if self._temp_dir:
      shutil.rmtree(self._temp_dir)

  def make_temp_dir(self):
    if self._temp_dir is None:
      self._temp_dir = tempfile.mkdtemp()
    return tempfile.mkdtemp(dir=self._temp_dir)

  def create_temp_file(self, path, contents: List[str]):
    with open(path, 'w') as f:
      for content in contents:
        f.write(content + '\n')
      return f.name

  @pytest.mark.examples_postcommit
  def test_predictions_output_file(self):
    requirements_cache_dir = self.make_temp_dir()
    requirements_file = self.create_temp_file(
        path=os.path.join(requirements_cache_dir, 'requirements.txt'),
        contents=['torch', 'torchvision'])
    test_pipeline = TestPipeline(is_integration_test=True)
    # setup file with expected content
    output_file_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/outputs'
    output = '/'.join([output_file_dir, str(uuid.uuid4()), 'result'])
    input_file_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs'

    model_path = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/models/mobilenet_v2.pt'
    extra_opts = {
        'input': input_file_dir,
        'output': output,
        'model_path': model_path,
        'requirements_file': requirements_file
    }

    gcs = gcsio.GcsIO()
    pytorch_image_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts))
    self.assertTrue(gcs.exists(output))
