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

import sys
import unittest
from typing import NamedTuple
from unittest.mock import patch

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
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


def print_with_message(msg):
  def printer(elem):
    print(msg, elem)
    return elem

  return printer


class Record(NamedTuple):
  name: str
  age: int
  height: int


class InteractiveRunnerTest(unittest.TestCase):
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

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

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
    df_counts = ib.collect(counts, include_window_info=True, n=10)
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

  def test_streaming_wordcount(self):
    class WordExtractingDoFn(beam.DoFn):
      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    # Add the TestStream so that it can be cached.
    ib.options.recordable_sources.add(TestStream)

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
            .advance_watermark_to(30)
            .advance_processing_time(1)
            .advance_watermark_to(40)
            .advance_processing_time(1)
            .advance_watermark_to(50)
            .advance_processing_time(1)
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

    data_df = ib.collect(data, n=10, include_window_info=True)
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

    counts_df = ib.collect(counts, n=8, include_window_info=True)

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
      # pylint: disable=bad-option-value
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

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))
    data = p | beam.Create(
        [1, 2, 3]) | beam.Map(lambda x: beam.Row(square=x * x, cube=x * x * x))
    df = to_dataframe(data)

    # Watch the local scope for Interactive Beam so that values will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    df_expected = pd.DataFrame({'square': [1, 4, 9], 'cube': [1, 8, 27]})
    pd.testing.assert_frame_equal(
        df_expected, ib.collect(df, n=10).reset_index(drop=True))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_with_grouped_index(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))

    data = [
        Record('a', 20, 170),
        Record('a', 30, 170),
        Record('b', 22, 180),
        Record('c', 18, 150)
    ]

    aggregate = lambda df: df.groupby('height').mean()

    deferred_df = aggregate(to_dataframe(p | beam.Create(data)))
    df_expected = aggregate(pd.DataFrame(data))

    # Watch the local scope for Interactive Beam so that values will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    pd.testing.assert_frame_equal(df_expected, ib.collect(deferred_df, n=10))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_with_multi_index(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))

    data = [
        Record('a', 20, 170),
        Record('a', 30, 170),
        Record('b', 22, 180),
        Record('c', 18, 150)
    ]

    aggregate = lambda df: df.groupby(['name', 'height']).mean()

    deferred_df = aggregate(to_dataframe(p | beam.Create(data)))
    df_expected = aggregate(pd.DataFrame(data))

    # Watch the local scope for Interactive Beam so that values will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    pd.testing.assert_frame_equal(df_expected, ib.collect(deferred_df, n=10))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_with_multi_index_get_result(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))

    data = [
        Record('a', 20, 170),
        Record('a', 30, 170),
        Record('b', 22, 180),
        Record('c', 18, 150)
    ]

    aggregate = lambda df: df.groupby(['name', 'height']).mean()['age']

    deferred_df = aggregate(to_dataframe(p | beam.Create(data)))
    df_expected = aggregate(pd.DataFrame(data))

    # Watch the local scope for Interactive Beam so that values will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    pd.testing.assert_series_equal(df_expected, ib.collect(deferred_df, n=10))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_same_cell_twice(self):
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.DirectRunner()))
    data = p | beam.Create(
        [1, 2, 3]) | beam.Map(lambda x: beam.Row(square=x * x, cube=x * x * x))
    df = to_dataframe(data)

    # Watch the local scope for Interactive Beam so that values will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    df_expected = pd.DataFrame({'square': [1, 4, 9], 'cube': [1, 8, 27]})
    pd.testing.assert_series_equal(
        df_expected['square'],
        ib.collect(df['square'], n=10).reset_index(drop=True))
    pd.testing.assert_series_equal(
        df_expected['cube'],
        ib.collect(df['cube'], n=10).reset_index(drop=True))

  @unittest.skipIf(
      not ie.current_env().is_interactive_ready,
      '[interactive] dependency is not installed.')
  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_dataframe_caching(self, cell):

    # Create a pipeline that exercises the DataFrame API. This will also use
    # caching in the background.
    with cell:  # Cell 1
      p = beam.Pipeline(interactive_runner.InteractiveRunner())
      ib.watch({'p': p})

    with cell:  # Cell 2
      data = p | beam.Create([
          1, 2, 3
      ]) | beam.Map(lambda x: beam.Row(square=x * x, cube=x * x * x))

      with beam.dataframe.allow_non_parallel_operations():
        df = to_dataframe(data).reset_index(drop=True)

      ib.collect(df)

    with cell:  # Cell 3
      df['output'] = df['square'] * df['cube']
      ib.collect(df)

    with cell:  # Cell 4
      df['output'] = 0
      ib.collect(df)

    # We use a trace through the graph to perform an isomorphism test. The end
    # output should look like a linear graph. This indicates that the dataframe
    # transform was correctly broken into separate pieces to cache. If caching
    # isn't enabled, all the dataframe computation nodes are connected to a
    # single shared node.
    trace = []

    # Only look at the top-level transforms for the isomorphism. The test
    # doesn't care about the transform implementations, just the overall shape.
    class TopLevelTracer(beam.pipeline.PipelineVisitor):
      def _find_root_producer(self, node: beam.pipeline.AppliedPTransform):
        if node is None or not node.full_label:
          return None

        parent = self._find_root_producer(node.parent)
        if parent is None:
          return node

        return parent

      def _add_to_trace(self, node, trace):
        if '/' not in str(node):
          if node.inputs:
            producer = self._find_root_producer(node.inputs[0].producer)
            producer_name = producer.full_label if producer else ''
            trace.append((producer_name, node.full_label))

      def visit_transform(self, node: beam.pipeline.AppliedPTransform):
        self._add_to_trace(node, trace)

      def enter_composite_transform(
          self, node: beam.pipeline.AppliedPTransform):
        self._add_to_trace(node, trace)

    p.visit(TopLevelTracer())

    # Do the isomorphism test which states that the topological sort of the
    # graph yields a linear graph.
    trace_string = '\n'.join(str(t) for t in trace)
    prev_producer = ''
    for producer, consumer in trace:
      self.assertEqual(producer, prev_producer, trace_string)
      prev_producer = consumer


if __name__ == '__main__':
  unittest.main()
