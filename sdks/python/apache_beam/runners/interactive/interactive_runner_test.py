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

"""Tests for google3.pipeline.dataflow.python.interactive.interactive_runner.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch


def print_with_message(msg):
  def printer(elem):
    print(msg, elem)
    return elem

  return printer

def _build_a_test_stream_pipeline():
  test_stream = (TestStream()
                 .advance_watermark_to(0)
                 .add_elements([TimestampedValue('a', 1)])
                 .advance_processing_time(5)
                 .advance_watermark_to_infinity())
  p = beam.Pipeline(runner=interactive_runner.InteractiveRunner())
  events = p | test_stream
  ib.watch(locals())
  return p


class InteractiveRunnerTest(unittest.TestCase):

  def setUp(self):
    ie.new_env()

  def test_basic(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))
    ib.watch({'p': p})
    p.run().wait_until_finish()
    pc0 = (
        p | 'read' >> beam.Create([1, 2, 3])
        | 'Print1.1' >> beam.Map(print_with_message('Run1.1')))
    pc = pc0 | 'Print1.2' >> beam.Map(print_with_message('Run1.2'))
    ib.watch(locals())
    p.run().wait_until_finish()
    _ = pc | 'Print2' >> beam.Map(print_with_message('Run2'))
    p.run().wait_until_finish()
    _ = pc0 | 'Print3' >> beam.Map(print_with_message('Run3'))
    p.run().wait_until_finish()

  def test_wordcount(self):
    class WordExtractingDoFn(beam.DoFn):

      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))

    # Count the occurrences of each word.
    counts = (
        p
        | beam.Create(['to be or not to be that is the question'])
        | 'split' >> beam.ParDo(WordExtractingDoFn())
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda wordones: (wordones[0], sum(wordones[1]))))

    # Watch the local scope for Interactive Beam so that counts will be cached.
    ib.watch(locals())

    result = p.run()
    result.wait_until_finish()

    actual = dict(result.get(counts))
    self.assertDictEqual(
        actual, {
            'to': 2,
            'be': 2,
            'or': 1,
            'not': 1,
            'that': 1,
            'is': 1,
            'the': 1,
            'question': 1
        })

  def test_session(self):
    class MockPipelineRunner(object):
      def __init__(self):
        self._in_session = False

      def __enter__(self):
        self._in_session = True

      def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_session = False

    underlying_runner = MockPipelineRunner()
    runner = interactive_runner.InteractiveRunner(underlying_runner)
    runner.start_session()
    self.assertTrue(underlying_runner._in_session)
    runner.end_session()
    self.assertFalse(underlying_runner._in_session)

  # A patch is needed until the boundedness check considers a TestStream as
  # an unbounded source.
  @patch('apache_beam.runners.interactive.pipeline_instrument.'
         'PipelineInstrument.has_unbounded_sources', True)
  def test_background_caching_job_starts_when_none_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    p.run()
    self.assertIsNotNone(
        ie.current_env().pipeline_result(p, is_main_job=False))

  @patch('apache_beam.runners.interactive.pipeline_instrument.'
         'PipelineInstrument.has_unbounded_sources', False)
  def test_background_caching_job_not_start_for_batch_pipeline(self):
    p = _build_a_test_stream_pipeline()
    p.run()
    self.assertIsNone(
        ie.current_env().pipeline_result(p, is_main_job=False))

  @patch('apache_beam.runners.interactive.pipeline_instrument.'
         'PipelineInstrument.has_unbounded_sources', True)
  def test_background_caching_job_not_start_when_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    a_running_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(p, a_running_result, is_main_job=False)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(a_running_result,
                  ie.current_env().pipeline_result(p, is_main_job=False))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result,
                  ie.current_env().pipeline_result(p))

  @patch('apache_beam.runners.interactive.pipeline_instrument.'
         'PipelineInstrument.has_unbounded_sources', True)
  def test_background_caching_job_not_start_when_such_job_is_done(self):
    p = _build_a_test_stream_pipeline()
    a_done_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(p, a_done_result, is_main_job=False)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(a_done_result,
                  ie.current_env().pipeline_result(p, is_main_job=False))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result,
                  ie.current_env().pipeline_result(p))


if __name__ == '__main__':
  unittest.main()
