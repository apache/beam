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

"""End-to-End test for Pytorch Inference"""

import logging
import pytest
import unittest
import uuid

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

try:
  import torch
  from apache_beam.ml.inference.examples import pytorch_image_classification
except ImportError:
  torch = None

_EXPECTED_OUTPUTS = {
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005001.JPEG': '681',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005002.JPEG': '333',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005003.JPEG': '711',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005004.JPEG': '286',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005005.JPEG': '433',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005006.JPEG': '290',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005007.JPEG': '890',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005008.JPEG': '592',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005009.JPEG': '406',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005010.JPEG': '996',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005011.JPEG': '327',
    'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/ILSVRC2012_val_00005012.JPEG': '573'
}


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


@unittest.skipIf(torch is None, 'torch is not installed')
class PyTorchInference(unittest.TestCase):
  @pytest.mark.uses_pytorch
  @pytest.mark.it_postcommit
  @pytest.mark.sickbay_direct
  @pytest.mark.sickbay_spark
  @pytest.mark.sickbay_flink
  def test_predictions_output_file(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    output_file_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/outputs'
    output = '/'.join([output_file_dir, str(uuid.uuid4()), 'result'])
    input_file_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs/imagenet_samples.csv'
    images_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/inputs'

    model_path = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/models/mobilenet_v2.pt'
    extra_opts = {
        'input': input_file_dir,
        'output': output,
        'model_path': model_path,
        'images_dir': images_dir,
    }
    pytorch_image_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts))

    output_file = output + '.txt'
    self.assertEqual(FileSystems().exists(output_file), True)
    outputs = process_outputs(filepath=output_file)

    for output in outputs:
      filename, prediction = output.split(',')
      self.assertEqual(_EXPECTED_OUTPUTS[filename], prediction)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
