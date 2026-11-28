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

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.ml.inference.huggingface_inference import PipelineTask
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apache_beam.examples.inference import huggingface_language_modeling
  from apache_beam.examples.inference import huggingface_question_answering
  from apache_beam.ml.inference import pytorch_inference_it_test
except ImportError:
  raise unittest.SkipTest(
      "transformers dependencies are not installed. "
      "Check if transformers, torch, and tensorflow "
      "is installed.")

_EXPECTED_MASKED_LM_OUTPUTS = {
    'The capital of France is Paris .': 'paris',
    'It is raining cats and dogs .': 'dogs',
    'Today is Monday and tomorrow is Tuesday .': 'tuesday',
    'There are 5 coconuts on this palm tree .': 'tree',
    'The strongest person in the world is not famous .': 'me',
    'The secret ingredient to his wonderful life was gratitude .': 'love',
    'The biggest animal in the world is the whale .': 'elephant',
}


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

    model_name = 'FacebookAI/roberta-base'

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

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text.strip().lower()

    self.assertEqual(_EXPECTED_MASKED_LM_OUTPUTS, predictions_dict)

  def test_hf_language_modeling_large_model(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Path to text file containing some sentences
    file_of_sentences = 'gs://apache-beam-ml/datasets/custom/hf_sentences.txt'
    output_file_dir = 'gs://apache-beam-ml/testing/predictions'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])

    model_name = 'FacebookAI/roberta-base'

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

    predictions_dict = {}
    for prediction in predictions:
      text, predicted_text = prediction.split(';')
      predictions_dict[text] = predicted_text.strip().lower()

    self.assertEqual(_EXPECTED_MASKED_LM_OUTPUTS, predictions_dict)

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

  def test_hf_fill_mask_pipeline(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    model_handler = HuggingFacePipelineModelHandler(
        task=PipelineTask.FillMask,
        model='FacebookAI/roberta-base',
    )
    with test_pipeline as pipeline:
      predictions = (
          pipeline
          | 'CreateMaskedSentences' >> beam.Create([
              'The capital of France is <mask> .',
              'It is raining cats and <mask> .',
          ])
          | 'RunFillMask' >> RunInference(
              model_handler, inference_args={'top_k': 1})
          | 'ExtractTopToken' >> beam.Map(
              lambda r: (
                  r.example, (
                      r.inference[0]['token_str']
                      if isinstance(r.inference, list) else r.inference[
                          'token_str']).strip().lower())))
      assert_that(
          predictions,
          equal_to([
              ('The capital of France is <mask> .', 'paris'),
              ('It is raining cats and <mask> .', 'dogs'),
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
