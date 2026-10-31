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
# pytype: skip-file

import unittest

import numpy

import apache_beam as beam
from apache_beam.io.filesystem import FileMetadata
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class WatchFilePatternTest(unittest.TestCase):
  def test_latest_file_by_timestamp_default_value(self):
    # match continuously returns the files in sorted timestamp order.
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 20),
        FileMetadata(
            'path2.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 10)
    ]
    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['', '']))

  def test_latest_file_with_timestamp_after_pipeline_construction_time(self):
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 10)
    ]
    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['path1.py']))

  def test_emitting_singleton_output(self):
    # match continuously returns the files in sorted timestamp order.
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 20),
        # returns default
        FileMetadata(
            'path2.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 10),
        # returns default
        FileMetadata(
            'path3.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 10),
        FileMetadata(
            'path4.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 20)
    ]
    # returns path3.py

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.ParDo(utils._ConvertIterToSingleton())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['', 'path3.py', 'path4.py']))


class _ModelHandlerWithMissingPredictions(ModelHandler[int,
                                                       PredictionResult,
                                                       None]):
  """Returns a prediction for every input except the last one in a batch."""
  def load_model(self):
    return None

  def run_inference(self, batch, model, inference_args=None):
    return utils._convert_to_result(batch, [x * 10 for x in batch[:-1]])


class ConvertToResultTest(unittest.TestCase):
  def test_pairs_each_input_with_its_prediction(self):
    self.assertEqual(
        utils._convert_to_result(['a', 'b'], [1, 2], model_id='m'),
        [PredictionResult('a', 1, 'm'), PredictionResult('b', 2, 'm')])

  def test_splits_batched_array_into_one_prediction_per_input(self):
    results = utils._convert_to_result(['a', 'b', 'c'],
                                       numpy.array([[1, 2], [3, 4], [5, 6]]))
    self.assertEqual([r.inference.tolist() for r in results],
                     [[1, 2], [3, 4], [5, 6]])

  def test_splits_dict_predictions_into_one_dict_per_input(self):
    results = utils._convert_to_result(['a', 'b'], {
        'label': [0, 1], 'score': [0.9, 0.8]
    })
    self.assertEqual([r.inference for r in results], [{
        'label': 0, 'score': 0.9
    }, {
        'label': 1, 'score': 0.8
    }])

  def test_accepts_predictions_from_a_generator(self):
    results = utils._convert_to_result(['a', 'b'], (x for x in [1, 2]))
    self.assertEqual([r.inference for r in results], [1, 2])

  def test_raises_when_there_are_fewer_predictions_than_inputs(self):
    # For example, a model that returns a tuple of two outputs for a batch of
    # five inputs: zip() would pair the inputs with the outputs and drop the
    # last three inputs.
    with self.assertRaisesRegex(ValueError,
                                'got 2 predictions for a batch of 5 inputs'):
      utils._convert_to_result(
          list(range(5)), ('logits', 'scores'), model_id='m')

  def test_raises_when_a_generator_yields_too_few_predictions(self):
    with self.assertRaisesRegex(ValueError,
                                'got 1 prediction for a batch of 3 inputs'):
      utils._convert_to_result(['a', 'b', 'c'], (x for x in [1]))

  def test_raises_naming_dict_outputs_without_one_entry_per_input(self):
    # For example, a Hugging Face causal LM returns past_key_values with one
    # entry per layer rather than one per input.
    predictions = {
        'logits': [[0.1], [0.2], [0.3]],
        'past_key_values': [('k1', 'v1'), ('k2', 'v2')]
    }
    with self.assertRaisesRegex(
        ValueError,
        "output 'past_key_values' has 2 entries for a batch of 3 inputs"):
      utils._convert_to_result(['a', 'b', 'c'], predictions)

  def test_extra_predictions_are_ignored(self):
    # Only missing predictions raise, so pipelines in which every input
    # already gets a result keep working.
    results = utils._convert_to_result(['a'], ('logits', 'scores'))
    self.assertEqual([r.inference for r in results], ['logits'])

  def test_run_inference_sends_inputs_without_predictions_to_failed_output(
      self):
    with TestPipeline() as p:
      main, other = (
          p
          | beam.Create([1, 2, 3, 4])
          | RunInference(_ModelHandlerWithMissingPredictions()
                         ).with_exception_handling())
      assert_that(main, equal_to([]), label='CheckNoResults')
      failed_inputs = (
          other.failed_inferences | beam.FlatMap(lambda failure: failure[0]))
      assert_that(
          failed_inputs, equal_to([1, 2, 3, 4]), label='CheckFailedInputs')


if __name__ == '__main__':
  unittest.main()
