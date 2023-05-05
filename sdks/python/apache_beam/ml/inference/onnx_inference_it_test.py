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

"""End-to-End test for Onnx Inference"""

import logging
import os
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  import onnx
  from apache_beam.examples.inference import onnx_sentiment_classification
except ImportError as e:
  onnx = None


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


@unittest.skipIf(
    os.getenv('FORCE_ONNX_IT') is None and onnx is None,
    'Missing dependencies. '
    'Test depends on onnx and transformers')
class OnnxInference(unittest.TestCase):
  @pytest.mark.uses_onnx
  @pytest.mark.it_postcommit
  def test_onnx_run_inference_roberta_sentiment_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    file_of_sentences = (
        'gs://apache-beam-ml/testing/inputs/onnx/'
        'sentiment_classification_input.txt')
    output_file_dir = 'local/sentiment_classification/output'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_uri = (
        'gs://apache-beam-ml/models/'
        'roberta_sentiment_classification.onnx')
    extra_opts = {
        'input': file_of_sentences,
        'output': output_file,
        'model_uri': model_uri,
    }
    onnx_sentiment_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = process_outputs(filepath=output_file)
    actuals_file = (
        'gs://apache-beam-ml/testing/expected_outputs/'
        'test_onnx_run_inference_roberta_sentiment'
        '_classification_actuals.txt')
    actuals = process_outputs(filepath=actuals_file)

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text

    for actual in actuals:
      text, actual_predicted_text = actual.split(';')
      predicted_predicted_text = predictions_dict[text]
      self.assertEqual(actual_predicted_text, predicted_predicted_text)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
