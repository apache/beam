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

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.examples.inference import huggingface_image_segmentation


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


class HuggingFaceInference(unittest.TestCase):
  def test_hf_imagenet_image_segmentation(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = (
        'gs://apache-beam-ml/testing/inputs/it_imagenet_input_labels.txt')
    image_dir = (
        'https://storage.googleapis.com/download.tensorflow.org/example_images/'
    )
    output_file_dir = 'gs://apache-beam-ml/testing/outputs'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    extra_opts = {
        'input': input_file,
        'output': output_file,
        'repo_id': 'alexsu52/mobilenet_v2_imagenette',
        'filename': 'tf_model.h5',
        'image_dir': image_dir,
    }
    huggingface_image_segmentation.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = 'gs://apache-beam-ml/testing/expected_outputs/test_tf_imagenet_image_segmentation.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)
    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    for true_label, predicted_label in zip(expected_outputs, predicted_outputs):
      self.assertEqual(true_label, predicted_label)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
