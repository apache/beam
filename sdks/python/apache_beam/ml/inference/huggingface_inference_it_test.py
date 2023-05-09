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

"""End-to-End test for HuggingFace Model Handler with Run Inference"""

import unittest
import uuid
import logging
import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.examples.inference import huggingface_image_segmentation


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


class HuggingFaceInference(unittest.TestCase):
  @pytest.mark.timeout(4200)
  def test_hf_imagenet_image_segmentation(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # text files containing absolute path to the coco validation data on GCS
    file_of_image_names = 'gs://apache-beam-ml/testing/inputs/it_coco_validation_inputs.txt'  # pylint: disable=line-too-long
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    repo_id = 'facebook/detr-resnet-50'
    model = 'pytorch_model.bin'
    images_dir = 'gs://apache-beam-ml/datasets/coco/raw-data/val2017'
    extra_opts = {
        'input': file_of_image_names,
        'output': output_file,
        'repo_id': repo_id,
        'model': model,
        'images_dir': images_dir,
    }
    huggingface_image_segmentation.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = process_outputs(filepath=output_file)
    actuals_file = 'gs://apache-beam-ml/testing/expected_outputs/test_torch_run_inference_coco_maskrcnn_resnet50_fpn_actuals.txt'  # pylint: disable=line-too-long
    actuals = process_outputs(filepath=actuals_file)

    predictions_dict = {}
    for prediction in predictions:
      filename, prediction_labels = prediction.split(';')
      predictions_dict[filename] = prediction_labels

    for actual in actuals:
      filename, actual_labels = actual.split(';')
      prediction_labels = predictions_dict[filename]
      self.assertEqual(actual_labels, prediction_labels)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
