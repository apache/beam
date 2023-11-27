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

"""End-to-End test for Hugging Face Inference"""

import logging
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from apache_beam.examples.inference import huggingface_language_modeling
  from apache_beam.examples.inference import huggingface_question_answering
  from apache_beam.ml.inference import pytorch_inference_it_test
except ImportError:
  raise unittest.SkipTest(
      "transformers dependencies are not installed. "
      "Check if transformers, torch, and tensorflow "
      "is installed.")


@pytest.mark.uses_transformers
@pytest.mark.it_postcommit
@pytest.mark.timeout(1800)
class HuggingFaceInference(unittest.TestCase):
  def test_hf_language_modeling(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    file_of_sentences = 'gs://apache-beam-ml/datasets/custom/hf_sentences.txt'
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_name = 'stevhliu/my_awesome_eli5_mlm_model'

    extra_opts = {
        'input': file_of_sentences,
        'output': output_file,
        'model_name': model_name,
    }
    huggingface_language_modeling.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = pytorch_inference_it_test.process_outputs(
        filepath=output_file)
    actuals_file = 'gs://apache-beam-ml/testing/expected_outputs/test_hf_run_inference_for_masked_lm_actuals.txt'  # pylint: disable=line-too-long
    actuals = pytorch_inference_it_test.process_outputs(filepath=actuals_file)

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text.strip().lower()

    for actual in actuals:
      text, actual_predicted_text = actual.split(';')
      predicted_predicted_text = predictions_dict[text]
      self.assertEqual(actual_predicted_text, predicted_predicted_text)

  def test_hf_language_modeling_large_model(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    file_of_sentences = 'gs://apache-beam-ml/datasets/custom/hf_sentences.txt'
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_name = 'stevhliu/my_awesome_eli5_mlm_model'

    extra_opts = {
        'input': file_of_sentences,
        'output': output_file,
        'model_name': model_name,
        'large_model': True,
    }
    huggingface_language_modeling.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = pytorch_inference_it_test.process_outputs(
        filepath=output_file)
    actuals_file = 'gs://apache-beam-ml/testing/expected_outputs/test_hf_run_inference_for_masked_lm_actuals.txt'  # pylint: disable=line-too-long
    actuals = pytorch_inference_it_test.process_outputs(filepath=actuals_file)

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text.strip().lower()

    for actual in actuals:
      text, actual_predicted_text = actual.split(';')
      predicted_predicted_text = predictions_dict[text]
      self.assertEqual(actual_predicted_text, predicted_predicted_text)

  def test_hf_pipeline(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some questions and context
    input_file = 'gs://apache-beam-ml/datasets/custom/questions.txt'
    output_file_dir = 'gs://apache-beam-ml/hf/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'revision': 'deedc3e42208524e0df3d9149d1f26aa6934f05f',
    }
    huggingface_question_answering.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)
    predictions = pytorch_inference_it_test.process_outputs(
        filepath=output_file)
    actuals_file = (
        'gs://apache-beam-ml/testing/expected_outputs/'
        'test_hf_pipeline_answers.txt')
    actuals = pytorch_inference_it_test.process_outputs(filepath=actuals_file)

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text.strip()

    for actual in actuals:
      text, actual_predicted_text = actual.split(';')
      predicted_predicted_text = predictions_dict[text]
      self.assertEqual(actual_predicted_text, predicted_predicted_text)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
