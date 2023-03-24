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

"""Unit tests for the windowing classes."""
# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.runners import pipeline_context
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import CombinePerKey
from apache_beam.transforms import Create
from apache_beam.transforms import FlatMapTuple
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import Map
from apache_beam.transforms import MapTuple
from apache_beam.transforms import WindowInto
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import NonMergingWindowFn
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP


def context(element, timestamp):
  return WindowFn.AssignContext(timestamp, element)


class ReifyWindowsFn(core.DoFn):
  def process(self, element, window=core.DoFn.WindowParam):
    key, values = element
    yield "%s @ %s" % (key, window), values


class TestCustomWindows(NonMergingWindowFn):
  """A custom non merging window fn which assigns elements into interval windows
  [0, 3), [3, 5) and [5, element timestamp) based on the element timestamps.
  """
  def assign(self, context):
    timestamp = context.timestamp
    if timestamp < 3:
      return [IntervalWindow(0, 3)]
    elif timestamp < 5:
      return [IntervalWindow(3, 5)]
    else:
      return [IntervalWindow(5, timestamp)]

  def get_window_coder(self):
    return coders.IntervalWindowCoder()


class WindowTest(unittest.TestCase):
  def test_timestamped_value_cmp(self):
    self.assertEqual(TimestampedValue('a', 2), TimestampedValue('a', 2))
    self.assertEqual(TimestampedValue('a', 2), TimestampedValue('a', 2.0))
    self.assertNotEqual(TimestampedValue('a', 2), TimestampedValue('a', 2.1))
    self.assertNotEqual(TimestampedValue('a', 2), TimestampedValue('b', 2))

  def test_global_window(self):
    self.assertEqual(GlobalWindow(), GlobalWindow())
    self.assertNotEqual(
        GlobalWindow(), IntervalWindow(MIN_TIMESTAMP, MAX_TIMESTAMP))
    self.assertNotEqual(
        IntervalWindow(MIN_TIMESTAMP, MAX_TIMESTAMP), GlobalWindow())
    self.assertTrue(GlobalWindow().max_timestamp() < MAX_TIMESTAMP)

  def test_fixed_windows(self):
    # Test windows with offset: 2, 7, 12, 17, ...
    windowfn = FixedWindows(size=5, offset=2)
    self.assertEqual([IntervalWindow(7, 12)], windowfn.assign(context('v', 7)))
    self.assertEqual([IntervalWindow(7, 12)], windowfn.assign(context('v', 11)))
    self.assertEqual([IntervalWindow(12, 17)],
                     windowfn.assign(context('v', 12)))

    # Test windows without offset: 0, 5, 10, 15, ...
    windowfn = FixedWindows(size=5)
    self.assertEqual([IntervalWindow(5, 10)], windowfn.assign(context('v', 5)))
    self.assertEqual([IntervalWindow(5, 10)], windowfn.assign(context('v', 9)))
    self.assertEqual([IntervalWindow(10, 15)],
                     windowfn.assign(context('v', 10)))

    # Test windows with offset out of range.
    windowfn = FixedWindows(size=5, offset=12)
    self.assertEqual([IntervalWindow(7, 12)], windowfn.assign(context('v', 11)))

  def test_sliding_windows_assignment(self):
    windowfn = SlidingWindows(size=15, period=5, offset=2)
    expected = [
        IntervalWindow(7, 22), IntervalWindow(2, 17), IntervalWindow(-3, 12)
    ]
    self.assertEqual(expected, windowfn.assign(context('v', 7)))
    self.assertEqual(expected, windowfn.assign(context('v', 8)))
    self.assertEqual(expected, windowfn.assign(context('v', 11)))

  def test_sliding_windows_assignment_fraction(self):
    windowfn = SlidingWindows(size=3.5, period=2.5, offset=1.5)
    self.assertEqual([IntervalWindow(1.5, 5.0), IntervalWindow(-1.0, 2.5)],
                     windowfn.assign(context('v', 1.7)))
    self.assertEqual([IntervalWindow(1.5, 5.0)],
                     windowfn.assign(context('v', 3)))

  def test_sliding_windows_assignment_fraction_large_offset(self):
    windowfn = SlidingWindows(size=3.5, period=2.5, offset=4.0)
    self.assertEqual([IntervalWindow(1.5, 5.0), IntervalWindow(-1.0, 2.5)],
                     windowfn.assign(context('v', 1.7)))
    self.assertEqual([IntervalWindow(4.0, 7.5), IntervalWindow(1.5, 5.0)],
                     windowfn.assign(context('v', 4.5)))

  def test_sessions_merging(self):
    windowfn = Sessions(10)

    def merge(*timestamps):
      windows = [windowfn.assign(context(None, t)) for t in timestamps]
      running = set()

      class TestMergeContext(WindowFn.MergeContext):
        def __init__(self):
          super().__init__(running)

        def merge(self, to_be_merged, merge_result):
          for w in to_be_merged:
            if w in running:
              running.remove(w)
          running.add(merge_result)

      for ws in windows:
        running.update(ws)
        windowfn.merge(TestMergeContext())
      windowfn.merge(TestMergeContext())
      return sorted(running)

    self.assertEqual([IntervalWindow(2, 12)], merge(2))
    self.assertEqual([IntervalWindow(2, 12), IntervalWindow(19, 29)],
                     merge(2, 19))

    self.assertEqual([IntervalWindow(2, 19)], merge(2, 9))
    self.assertEqual([IntervalWindow(2, 19)], merge(9, 2))

    self.assertEqual([IntervalWindow(2, 19), IntervalWindow(19, 29)],
                     merge(2, 9, 19))
    self.assertEqual([IntervalWindow(2, 19), IntervalWindow(19, 29)],
                     merge(19, 9, 2))

    self.assertEqual([IntervalWindow(2, 25)], merge(2, 15, 10))

  def timestamped_key_values(self, pipeline, key, *timestamps):
    return (
        pipeline | 'start' >> Create(timestamps)
        | Map(lambda x: WindowedValue((key, x), x, [GlobalWindow()])))

  def test_sliding_windows(self):
    with TestPipeline() as p:
      pcoll = self.timestamped_key_values(p, 'key', 1, 2, 3)
      result = (
          pcoll
          | 'w' >> WindowInto(SlidingWindows(period=2, size=4))
          | GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs)))
          | beam.ParDo(ReifyWindowsFn()))
      expected = [('key @ [-2.0, 2.0)', [1]), ('key @ [0.0, 4.0)', [1, 2, 3]),
                  ('key @ [2.0, 6.0)', [2, 3])]
      assert_that(result, equal_to(expected))

  def test_sessions(self):
    with TestPipeline() as p:
      pcoll = self.timestamped_key_values(p, 'key', 1, 2, 3, 20, 35, 27)
      sort_values = Map(lambda k_vs: (k_vs[0], sorted(k_vs[1])))
      result = (
          pcoll
          | 'w' >> WindowInto(Sessions(10))
          | GroupByKey()
          | sort_values
          | beam.ParDo(ReifyWindowsFn()))
      expected = [('key @ [1.0, 13.0)', [1, 2, 3]),
                  ('key @ [20.0, 45.0)', [20, 27, 35])]
      assert_that(result, equal_to(expected))

  def test_timestamped_value(self):
    with TestPipeline() as p:
      result = (
          p
          | 'start' >> Create([(k, k) for k in range(10)])
          | Map(lambda x_t: TimestampedValue(x_t[0], x_t[1]))
          | 'w' >> WindowInto(FixedWindows(5))
          | Map(lambda v: ('key', v))
          | GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          result,
          equal_to([('key', [0, 1, 2, 3, 4]), ('key', [5, 6, 7, 8, 9])]))

  def test_rewindow(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([(k, k) for k in range(10)])
          | Map(lambda x_t1: TimestampedValue(x_t1[0], x_t1[1]))
          | 'window' >> WindowInto(SlidingWindows(period=2, size=6))
          # Per the model, each element is now duplicated across
          # three windows. Rewindowing must preserve this duplication.
          | 'rewindow' >> WindowInto(FixedWindows(5))
          | 'rewindow2' >> WindowInto(FixedWindows(5))
          | Map(lambda v: ('key', v))
          | GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          result,
          equal_to([('key', sorted([0, 1, 2, 3, 4] * 3)),
                    ('key', sorted([5, 6, 7, 8, 9] * 3))]))

  def test_rewindow_regroup(self):
    with TestPipeline() as p:
      grouped = (
          p
          | Create(range(5))
          | Map(lambda t: TimestampedValue(('key', t), t))
          | 'window' >> WindowInto(FixedWindows(5, offset=3))
          | GroupByKey()
          | MapTuple(lambda k, vs: (k, sorted(vs))))
      # Both of these group-and-ungroup sequences should be idempotent.
      regrouped1 = (
          grouped
          | 'w1' >> WindowInto(FixedWindows(5, offset=3))
          | 'g1' >> GroupByKey()
          | FlatMapTuple(lambda k, vs: [(k, v) for v in vs]))
      regrouped2 = (
          grouped
          | FlatMapTuple(lambda k, vs: [(k, v) for v in vs])
          | 'w2' >> WindowInto(FixedWindows(5, offset=3))
          | 'g2' >> GroupByKey()
          | MapTuple(lambda k, vs: (k, sorted(vs))))
      with_windows = Map(lambda e, w=beam.DoFn.WindowParam: (e, w))
      expected = [(('key', [0, 1, 2]), IntervalWindow(-2, 3)),
                  (('key', [3, 4]), IntervalWindow(3, 8))]

      assert_that(grouped | 'ww' >> with_windows, equal_to(expected))
      assert_that(
          regrouped1 | 'ww1' >> with_windows, equal_to(expected), label='r1')
      assert_that(
          regrouped2 | 'ww2' >> with_windows, equal_to(expected), label='r2')

  def test_timestamped_with_combiners(self):
    with TestPipeline() as p:
      result = (
          p
          # Create some initial test values.
          | 'start' >> Create([(k, k) for k in range(10)])
          # The purpose of the WindowInto transform is to establish a
          # FixedWindows windowing function for the PCollection.
          # It does not bucket elements into windows since the timestamps
          # from Create are not spaced 5 ms apart and very likely they all
          # fall into the same window.
          | 'w' >> WindowInto(FixedWindows(5))
          # Generate timestamped values using the values as timestamps.
          # Now there are values 5 ms apart and since Map propagates the
          # windowing function from input to output the output PCollection
          # will have elements falling into different 5ms windows.
          | Map(lambda x_t2: TimestampedValue(x_t2[0], x_t2[1]))
          # We add a 'key' to each value representing the index of the
          # window. This is important since there is no guarantee of
          # order for the elements of a PCollection.
          | Map(lambda v: (v // 5, v)))
      # Sum all elements associated with a key and window. Although it
      # is called CombinePerKey it is really CombinePerKeyAndWindow the
      # same way GroupByKey is really GroupByKeyAndWindow.
      sum_per_window = result | CombinePerKey(sum)
      # Compute mean per key and window.
      mean_per_window = result | combiners.Mean.PerKey()
      assert_that(
          sum_per_window, equal_to([(0, 10), (1, 35)]), label='assert:sum')
      assert_that(
          mean_per_window, equal_to([(0, 2.0), (1, 7.0)]), label='assert:mean')

  def test_custom_windows(self):
    with TestPipeline() as p:
      pcoll = self.timestamped_key_values(p, 'key', 0, 1, 2, 3, 4, 5, 6)
      # pylint: disable=abstract-class-instantiated
      result = (
          pcoll
          | 'custom window' >> WindowInto(TestCustomWindows())
          | GroupByKey()
          | 'sort values' >> MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          result,
          equal_to([('key', [0, 1, 2]), ('key', [3, 4]), ('key', [5]),
                    ('key', [6])]))

  def test_window_assignment_idempotency(self):
    with TestPipeline() as p:
      pcoll = self.timestamped_key_values(p, 'key', 0, 2, 4)
      result = (
          pcoll
          | 'window' >> WindowInto(FixedWindows(2))
          | 'same window' >> WindowInto(FixedWindows(2))
          | 'same window again' >> WindowInto(FixedWindows(2))
          | GroupByKey())

      assert_that(result, equal_to([('key', [0]), ('key', [2]), ('key', [4])]))

  def test_window_assignment_through_multiple_gbk_idempotency(self):
    with TestPipeline() as p:
      pcoll = self.timestamped_key_values(p, 'key', 0, 2, 4)
      result = (
          pcoll
          | 'window' >> WindowInto(FixedWindows(2))
          | 'gbk' >> GroupByKey()
          | 'same window' >> WindowInto(FixedWindows(2))
          | 'another gbk' >> GroupByKey()
          | 'same window again' >> WindowInto(FixedWindows(2))
          | 'gbk again' >> GroupByKey())

      assert_that(
          result,
          equal_to([('key', [[[0]]]), ('key', [[[2]]]), ('key', [[[4]]])]))


class RunnerApiTest(unittest.TestCase):
  def test_windowfn_encoding(self):
    for window_fn in (GlobalWindows(),
                      FixedWindows(37),
                      SlidingWindows(2, 389),
                      Sessions(5077)):
      context = pipeline_context.PipelineContext()
      self.assertEqual(
          window_fn,
          WindowFn.from_runner_api(window_fn.to_runner_api(context), context))

  def test_windowing_encoding(self):
    for windowing in (Windowing(GlobalWindows()),
                      Windowing(
                          FixedWindows(1, 3),
                          AfterCount(6),
                          accumulation_mode=AccumulationMode.ACCUMULATING),
                      Windowing(
                          SlidingWindows(10, 15, 21),
                          AfterCount(28),
                          timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                          accumulation_mode=AccumulationMode.DISCARDING)):
      context = pipeline_context.PipelineContext()
      self.assertEqual(
          windowing,
          Windowing.from_runner_api(windowing.to_runner_api(context), context))


if __name__ == '__main__':
  unittest.main()
