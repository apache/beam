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
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _file(path, seconds_from_start):
  return FileMetadata(
      path,
      10,
      last_updated_in_seconds=utils._START_TIME_STAMP + seconds_from_start)


def _model_ids(pcoll, label='ModelIds'):
  return pcoll | label >> beam.Map(lambda model: model.model_id)


class WatchFilePatternTest(unittest.TestCase):
  def test_latest_file_combine_fn_picks_newest_regardless_of_order(self):
    older = _file('older.py', 10)
    newer = _file('newer.py', 50)
    initial = _file('initial.py', -5)
    combine = utils._LatestFileCombineFn()

    def accumulate(files):
      accumulator = combine.create_accumulator()
      for metadata in files:
        accumulator = combine.add_input(accumulator, metadata)
      return accumulator

    merged = combine.merge_accumulators(
        [accumulate([older]), accumulate([newer, initial])])
    self.assertEqual('newer.py', combine.extract_output(merged).path)
    swapped = combine.merge_accumulators(
        [accumulate([newer, initial]), accumulate([older])])
    self.assertEqual('newer.py', combine.extract_output(swapped).path)

  def test_latest_file_combine_fn_breaks_timestamp_ties_by_path(self):
    combine = utils._LatestFileCombineFn()
    first = combine.add_input(combine.create_accumulator(), _file('b.py', 10))
    second = combine.add_input(combine.create_accumulator(), _file('a.py', 10))
    merged = combine.merge_accumulators([first, second])
    self.assertEqual('b.py', combine.extract_output(merged).path)

  def test_files_at_or_before_pipeline_start_emit_configured_model_once(self):
    # Several pre-start files are one poll. They publish the configured model
    # once (an empty model id), not one value per file.
    files = [_file('path1.py', -20), _file('path2.py', -10), _file('now.py', 0)]
    with TestPipeline() as p:
      emitted = _model_ids(p | beam.Create(files) | utils._SelectLatestModel())
      assert_that(emitted, equal_to(['']))

  def test_configured_model_then_a_newer_file(self):
    files = [_file('initial.py', -5), _file('newer.py', 50)]
    with TestPipeline() as p:
      emitted = _model_ids(
          p
          | beam.Create(files)
          | beam.Map(lambda metadata: (utils._GLOBAL_MODEL_KEY, metadata))
          | beam.ParDo(utils._EmitLatestModel()),
          label='ConfiguredThenUpdated')
      assert_that(emitted, equal_to(['', 'newer.py']))

  def test_file_newer_than_pipeline_start_is_emitted(self):
    with TestPipeline() as p:
      emitted = _model_ids(
          p | beam.Create([_file('path1.py', 10)]) | utils._SelectLatestModel())
      assert_that(emitted, equal_to(['path1.py']))

  def test_one_poll_emits_only_the_newest_model(self):
    # Newest file is not last, and another post-start file is present, so
    # neither input order nor "every new path" can select the right model.
    files = [
        _file('path4.py', 20),
        _file('path1.py', -20),
        _file('path3.py', 10),
        _file('path2.py', -10),
    ]
    with TestPipeline() as p:
      updates = p | 'Files' >> beam.Create(files) | utils._SelectLatestModel()
      assert_that(_model_ids(updates), equal_to(['path4.py']), label='emitted')
      singleton = (
          p
          | 'Probe' >> beam.Create([None])
          | beam.Map(
              lambda _, model: model.model_id,
              beam.pvalue.AsSingleton(updates)))
      assert_that(singleton, equal_to(['path4.py']), label='singleton')

  def test_older_update_does_not_replace_a_newer_model(self):
    # Shared key, processed in input order. The second file is still newer
    # than pipeline start, but older than the model already accepted.
    files = [_file('newer.py', 50), _file('older.py', 10)]
    with TestPipeline() as p:
      emitted = _model_ids(
          p
          | beam.Create(files)
          | beam.Map(lambda metadata: (utils._GLOBAL_MODEL_KEY, metadata))
          | beam.ParDo(utils._EmitLatestModel()))
      assert_that(emitted, equal_to(['newer.py']))

  def test_newer_update_replaces_an_older_model(self):
    files = [_file('older.py', 10), _file('newer.py', 50)]
    with TestPipeline() as p:
      emitted = _model_ids(
          p
          | beam.Create(files)
          | beam.Map(lambda metadata: (utils._GLOBAL_MODEL_KEY, metadata))
          | beam.ParDo(utils._EmitLatestModel()),
          label='UpdatedModelIds')
      assert_that(emitted, equal_to(['older.py', 'newer.py']))

  def test_later_poll_does_not_emit_an_older_model(self):
    options = PipelineOptions(flags=['--streaming'])
    with TestPipeline(options=options) as p:
      stream = (
          TestStream().advance_watermark_to(0).add_elements(
              [_file('newer.py', 50)]).advance_processing_time(5).add_elements([
                  _file('older.py', 10)
              ]).advance_processing_time(5).advance_watermark_to_infinity())
      emitted = _model_ids(
          p | stream | utils._SelectLatestModel(), label='PolledModelIds')
      assert_that(emitted, equal_to(['newer.py']))


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
    with self.assertRaisesRegex(ValueError, 'got 2 for a batch of size 5'):
      utils._convert_to_result(
          list(range(5)), ('logits', 'scores'), model_id='m')

  def test_raises_when_a_generator_yields_too_few_predictions(self):
    with self.assertRaisesRegex(ValueError, 'got 1 for a batch of size 3'):
      utils._convert_to_result(['a', 'b', 'c'], (x for x in [1]))

  def test_raises_when_a_dict_output_has_too_few_entries(self):
    # For example, a Hugging Face causal LM returns past_key_values with one
    # entry per layer rather than one per input.
    predictions = {
        'logits': [[0.1], [0.2], [0.3]],
        'past_key_values': [('k1', 'v1'), ('k2', 'v2')]
    }
    with self.assertRaisesRegex(ValueError, 'got 2 for a batch of size 3'):
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
