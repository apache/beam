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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import unittest

import pandas as pd

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfo
from apache_beam.utils.windowed_value import PaneInfoTiming
from apache_beam.utils.windowed_value import WindowedValue

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch  # type: ignore[misc]


def print_with_message(msg):
  def printer(elem):
    print(msg, elem)
    return elem

  return printer


class InteractiveRunnerTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
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

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
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

    actual = list(result.get(counts))
    self.assertSetEqual(
        set(actual),
        set([
            ('or', 1),
            ('that', 1),
            ('be', 2),
            ('is', 1),
            ('question', 1),
            ('to', 2),
            ('the', 1),
            ('not', 1),
        ]))

    # Truncate the precision to millis because the window coder uses millis
    # as units then gets upcast to micros.
    end_of_window = (GlobalWindow().max_timestamp().micros // 1000) * 1000
    df_counts = ib.collect(counts, include_window_info=True)
    df_expected = pd.DataFrame({
        0: [e[0] for e in actual],
        1: [e[1] for e in actual],
        'event_time': [end_of_window for _ in actual],
        'windows': [[GlobalWindow()] for _ in actual],
        'pane_info': [
            PaneInfo(True, True, PaneInfoTiming.ON_TIME, 0, 0) for _ in actual
        ]
    },
                               columns=[
                                   0, 1, 'event_time', 'windows', 'pane_info'
                               ])

    pd.testing.assert_frame_equal(df_expected, df_counts)

    actual_reified = result.get(counts, include_window_info=True)
    expected_reified = [
        WindowedValue(
            e,
            Timestamp(micros=end_of_window), [GlobalWindow()],
            PaneInfo(True, True, PaneInfoTiming.ON_TIME, 0, 0)) for e in actual
    ]
    self.assertEqual(actual_reified, expected_reified)

  @unittest.skipIf(
      sys.version_info < (3, 5, 3),
      'The tests require at least Python 3.6 to work.')
  def test_streaming_wordcount(self):
    class WordExtractingDoFn(beam.DoFn):
      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    # Add the TestStream so that it can be cached.
    ib.options.capturable_sources.add(TestStream)

    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(),
        options=StandardOptions(streaming=True))

    data = (
        p
        | TestStream()
            .advance_watermark_to(0)
            .advance_processing_time(1)
            .add_elements(['to', 'be', 'or', 'not', 'to', 'be'])
            .advance_watermark_to(20)
            .advance_processing_time(1)
            .add_elements(['that', 'is', 'the', 'question'])
        | beam.WindowInto(beam.window.FixedWindows(10))) # yapf: disable

    counts = (
        data
        | 'split' >> beam.ParDo(WordExtractingDoFn())
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda wordones: (wordones[0], sum(wordones[1]))))

    # Watch the local scope for Interactive Beam so that referenced PCollections
    # will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    # Create a fake limiter that cancels the BCJ once the main job receives the
    # expected amount of results.
    class FakeLimiter:
      def __init__(self, p, pcoll):
        self.p = p
        self.pcoll = pcoll

      def is_triggered(self):
        result = ie.current_env().pipeline_result(self.p)
        if result:
          try:
            results = result.get(self.pcoll)
          except ValueError:
            return False
          return len(results) >= 10
        return False

    # This sets the limiters to stop reading when the test receives 10 elements.
    ie.current_env().options.capture_control.set_limiters_for_test(
        [FakeLimiter(p, data)])

    # This tests that the data was correctly cached.
    pane_info = PaneInfo(True, True, PaneInfoTiming.UNKNOWN, 0, 0)
    expected_data_df = pd.DataFrame([
        ('to', 0, [IntervalWindow(0, 10)], pane_info),
        ('be', 0, [IntervalWindow(0, 10)], pane_info),
        ('or', 0, [IntervalWindow(0, 10)], pane_info),
        ('not', 0, [IntervalWindow(0, 10)], pane_info),
        ('to', 0, [IntervalWindow(0, 10)], pane_info),
        ('be', 0, [IntervalWindow(0, 10)], pane_info),
        ('that', 20000000, [IntervalWindow(20, 30)], pane_info),
        ('is', 20000000, [IntervalWindow(20, 30)], pane_info),
        ('the', 20000000, [IntervalWindow(20, 30)], pane_info),
        ('question', 20000000, [IntervalWindow(20, 30)], pane_info)
    ], columns=[0, 'event_time', 'windows', 'pane_info']) # yapf: disable

    data_df = ib.collect(data, include_window_info=True)
    pd.testing.assert_frame_equal(expected_data_df, data_df)

    # This tests that the windowing was passed correctly so that all the data
    # is aggregated also correctly.
    pane_info = PaneInfo(True, False, PaneInfoTiming.ON_TIME, 0, 0)
    expected_counts_df = pd.DataFrame([
        ('be', 2, 9999999, [IntervalWindow(0, 10)], pane_info),
        ('not', 1, 9999999, [IntervalWindow(0, 10)], pane_info),
        ('or', 1, 9999999, [IntervalWindow(0, 10)], pane_info),
        ('to', 2, 9999999, [IntervalWindow(0, 10)], pane_info),
        ('is', 1, 29999999, [IntervalWindow(20, 30)], pane_info),
        ('question', 1, 29999999, [IntervalWindow(20, 30)], pane_info),
        ('that', 1, 29999999, [IntervalWindow(20, 30)], pane_info),
        ('the', 1, 29999999, [IntervalWindow(20, 30)], pane_info),
    ], columns=[0, 1, 'event_time', 'windows', 'pane_info']) # yapf: disable

    counts_df = ib.collect(counts, include_window_info=True)

    # The group by key has no guarantee of order. So we post-process the DF by
    # sorting so we can test equality.
    sorted_counts_df = (counts_df
                        .sort_values(['event_time', 0], ascending=True)
                        .reset_index(drop=True)) # yapf: disable
    pd.testing.assert_frame_equal(expected_counts_df, sorted_counts_df)

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

  @unittest.skipIf(
      not ie.current_env().is_interactive_ready,
      '[interactive] dependency is not installed.')
  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_mark_pcollection_completed_after_successful_run(self, cell):
    with cell:  # Cell 1
      p = beam.Pipeline(interactive_runner.InteractiveRunner())
      ib.watch({'p': p})

    with cell:  # Cell 2
      # pylint: disable=range-builtin-not-iterating
      init = p | 'Init' >> beam.Create(range(5))

    with cell:  # Cell 3
      square = init | 'Square' >> beam.Map(lambda x: x * x)
      cube = init | 'Cube' >> beam.Map(lambda x: x**3)

    ib.watch(locals())
    result = p.run()
    self.assertTrue(init in ie.current_env().computed_pcollections)
    self.assertEqual({0, 1, 2, 3, 4}, set(result.get(init)))
    self.assertTrue(square in ie.current_env().computed_pcollections)
    self.assertEqual({0, 1, 4, 9, 16}, set(result.get(square)))
    self.assertTrue(cube in ie.current_env().computed_pcollections)
    self.assertEqual({0, 1, 8, 27, 64}, set(result.get(cube)))


if __name__ == '__main__':
  unittest.main()
