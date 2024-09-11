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

"""Tests for interactive utilities without explicitly using InteractiveRunner.
"""

# pytype: skip-file

import importlib
import sys
import unittest
from collections import defaultdict
from typing import NamedTuple

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive.testing.mock_env import isolated_env
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import PaneInfo
from apache_beam.utils.windowed_value import PaneInfoTiming


def print_with_message(msg):
  def printer(elem):
    print(msg, elem)
    return elem

  return printer


class Record(NamedTuple):
  name: str
  age: int
  height: int


_side_effects = defaultdict(int)


def cause_side_effect(elem):
  mod = importlib.import_module(__name__)
  mod._side_effects[elem] += 1
  return elem


def count_side_effects(elem):
  mod = importlib.import_module(__name__)
  return mod._side_effects[elem]


def clear_side_effect():
  mod = importlib.import_module(__name__)
  mod._side_effects.clear()


@isolated_env
class NonInteractiveRunnerTest(unittest.TestCase):
  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_basic(self):
    clear_side_effect()
    p = beam.Pipeline(direct_runner.DirectRunner())

    # Initial collection runs the pipeline.
    pcoll1 = p | beam.Create(['a', 'b', 'c']) | beam.Map(cause_side_effect)
    collected1 = ib.collect(pcoll1)
    self.assertEqual(set(collected1[0]), set(['a', 'b', 'c']))
    self.assertEqual(count_side_effects('a'), 1)

    # Collecting the PCollection again uses the cache.
    collected1again = ib.collect(pcoll1)
    self.assertEqual(set(collected1again[0]), set(['a', 'b', 'c']))
    self.assertEqual(count_side_effects('a'), 1)

    # Using the PCollection uses the cache.
    pcoll2 = pcoll1 | beam.Map(str.upper)
    collected2 = ib.collect(pcoll2)
    self.assertEqual(set(collected2[0]), set(['A', 'B', 'C']))
    self.assertEqual(count_side_effects('a'), 1)

    # Force re-computation.
    collected2 = ib.collect(pcoll2, force_compute=True)
    self.assertEqual(set(collected2[0]), set(['A', 'B', 'C']))
    self.assertEqual(count_side_effects('a'), 2)

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_multiple_collect(self):
    clear_side_effect()
    p = beam.Pipeline(direct_runner.DirectRunner())

    # Initial collection runs the pipeline.
    pcollA = p | 'A' >> beam.Create(['a']) | 'As' >> beam.Map(cause_side_effect)
    pcollB = p | 'B' >> beam.Create(['b']) | 'Bs' >> beam.Map(cause_side_effect)
    collectedA, collectedB = ib.collect(pcollA, pcollB)
    self.assertEqual(set(collectedA[0]), set(['a']))
    self.assertEqual(set(collectedB[0]), set(['b']))
    self.assertEqual(count_side_effects('a'), 1)
    self.assertEqual(count_side_effects('b'), 1)

    # Collecting the PCollection again uses the cache.
    collectedA, collectedB = ib.collect(pcollA, pcollB)
    self.assertEqual(set(collectedA[0]), set(['a']))
    self.assertEqual(set(collectedB[0]), set(['b']))
    self.assertEqual(count_side_effects('a'), 1)
    self.assertEqual(count_side_effects('b'), 1)

    # Using the PCollection uses the cache.
    pcollAA = pcollA | beam.Map(
        lambda x: 2 * x) | 'AAs' >> beam.Map(cause_side_effect)
    collectedA, collectedB, collectedAA = ib.collect(pcollA, pcollB, pcollAA)
    self.assertEqual(set(collectedA[0]), set(['a']))
    self.assertEqual(set(collectedB[0]), set(['b']))
    self.assertEqual(set(collectedAA[0]), set(['aa']))
    self.assertEqual(count_side_effects('a'), 1)
    self.assertEqual(count_side_effects('b'), 1)
    self.assertEqual(count_side_effects('aa'), 1)

    # Duplicates are only computed once.
    pcollBB = pcollB | beam.Map(
        lambda x: 2 * x) | 'BBs' >> beam.Map(cause_side_effect)
    collectedAA, collectedAAagain, collectedBB, collectedBBagain = ib.collect(
        pcollAA, pcollAA, pcollBB, pcollBB)
    self.assertEqual(set(collectedAA[0]), set(['aa']))
    self.assertEqual(set(collectedAAagain[0]), set(['aa']))
    self.assertEqual(set(collectedBB[0]), set(['bb']))
    self.assertEqual(set(collectedBBagain[0]), set(['bb']))
    self.assertEqual(count_side_effects('aa'), 1)
    self.assertEqual(count_side_effects('bb'), 1)

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_wordcount(self):
    class WordExtractingDoFn(beam.DoFn):
      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    p = beam.Pipeline(runner=direct_runner.DirectRunner())

    # Count the occurrences of each word.
    counts = (
        p
        | beam.Create(['to be or not to be that is the question'])
        | 'split' >> beam.ParDo(WordExtractingDoFn())
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda wordones: (wordones[0], sum(wordones[1]))))

    actual = ib.collect(counts)
    self.assertSetEqual(
        set(zip(actual[0], actual[1])),
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
        0: list(actual[0]),
        1: list(actual[1]),
        'event_time': [end_of_window] * len(actual),
        'windows': [[GlobalWindow()]] * len(actual),
        'pane_info': [PaneInfo(True, True, PaneInfoTiming.ON_TIME, 0, 0)] *
        len(actual)
    },
                               )

    pd.testing.assert_frame_equal(df_expected, df_counts)

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes(self):
    p = beam.Pipeline(runner=direct_runner.DirectRunner())
    data = p | beam.Create(
        [1, 2, 3]) | beam.Map(lambda x: beam.Row(square=x * x, cube=x * x * x))
    df = to_dataframe(data)

    df_expected = pd.DataFrame({'square': [1, 4, 9], 'cube': [1, 8, 27]})
    pd.testing.assert_frame_equal(
        df_expected, ib.collect(df, n=10).reset_index(drop=True))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_with_grouped_index(self):
    p = beam.Pipeline(runner=direct_runner.DirectRunner())

    data = [
        Record('a', 20, 170),
        Record('a', 30, 170),
        Record('b', 22, 180),
        Record('c', 18, 150)
    ]

    aggregate = lambda df: df.groupby('height').mean(numeric_only=True)

    deferred_df = aggregate(to_dataframe(p | beam.Create(data)))
    df_expected = aggregate(pd.DataFrame(data))

    pd.testing.assert_frame_equal(df_expected, ib.collect(deferred_df, n=10))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_with_multi_index(self):
    p = beam.Pipeline(runner=direct_runner.DirectRunner())

    data = [
        Record('a', 20, 170),
        Record('a', 30, 170),
        Record('b', 22, 180),
        Record('c', 18, 150)
    ]

    aggregate = lambda df: df.groupby(['name', 'height']).mean()

    deferred_df = aggregate(to_dataframe(p | beam.Create(data)))
    df_input = pd.DataFrame(data)
    df_input.name = df_input.name.astype(pd.StringDtype())
    df_expected = aggregate(df_input)

    pd.testing.assert_frame_equal(df_expected, ib.collect(deferred_df, n=10))

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_dataframes_same_cell_twice(self):
    p = beam.Pipeline(runner=direct_runner.DirectRunner())
    data = p | beam.Create(
        [1, 2, 3]) | beam.Map(lambda x: beam.Row(square=x * x, cube=x * x * x))
    df = to_dataframe(data)

    df_expected = pd.DataFrame({'square': [1, 4, 9], 'cube': [1, 8, 27]})
    pd.testing.assert_series_equal(
        df_expected['square'],
        ib.collect(df['square'], n=10).reset_index(drop=True))
    pd.testing.assert_series_equal(
        df_expected['cube'],
        ib.collect(df['cube'], n=10).reset_index(drop=True))


if __name__ == '__main__':
  unittest.main()
