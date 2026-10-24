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

import apache_beam as beam
from apache_beam.io.filesystem import FileMetadata
from apache_beam.ml.inference import utils
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

  def test_files_at_or_before_pipeline_start_are_not_emitted(self):
    files = [_file('path1.py', -20), _file('path2.py', -10), _file('now.py', 0)]
    with TestPipeline() as p:
      emitted = _model_ids(p | beam.Create(files) | utils._SelectLatestModel())
      assert_that(emitted, equal_to([]))

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


if __name__ == '__main__':
  unittest.main()
