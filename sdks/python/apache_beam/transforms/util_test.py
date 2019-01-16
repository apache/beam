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

"""Unit tests for the transform.util classes."""

from __future__ import absolute_import
from __future__ import division

import logging
import random
import time
import unittest
from builtins import object
from builtins import range

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import contains_in_any_order
from apache_beam.testing.util import equal_to
from apache_beam.transforms import util
from apache_beam.transforms import window
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.windowed_value import WindowedValue


class FakeClock(object):

  def __init__(self):
    self._now = time.time()

  def __call__(self):
    return self._now

  def sleep(self, duration):
    self._now += duration


class BatchElementsTest(unittest.TestCase):

  def test_constant_batch(self):
    # Assumes a single bundle...
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(35))
          | util.BatchElements(min_batch_size=10, max_batch_size=10)
          | beam.Map(len))
      assert_that(res, equal_to([10, 10, 10, 5]))

  def test_grows_to_max_batch(self):
    # Assumes a single bundle...
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(164))
          | util.BatchElements(
              min_batch_size=1, max_batch_size=50, clock=FakeClock())
          | beam.Map(len))
      assert_that(res, equal_to([1, 1, 2, 4, 8, 16, 32, 50, 50]))

  def test_windowed_batches(self):
    # Assumes a single bundle, in order...
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(47))
          | beam.Map(lambda t: window.TimestampedValue(t, t))
          | beam.WindowInto(window.FixedWindows(30))
          | util.BatchElements(
              min_batch_size=5, max_batch_size=10, clock=FakeClock())
          | beam.Map(len))
      assert_that(res, equal_to([
          5, 5, 10, 10,  # elements in [0, 30)
          10, 7,         # elements in [30, 47)
      ]))

  def test_target_duration(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(
        target_batch_overhead=None, target_batch_duration_secs=10, clock=clock)
    batch_duration = lambda batch_size: 1 + .7 * batch_size
    # 1 + 12 * .7 is as close as we can get to 10 as possible.
    expected_sizes = [1, 2, 4, 8, 12, 12, 12]
    actual_sizes = []
    for _ in range(len(expected_sizes)):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        clock.sleep(batch_duration(actual_sizes[-1]))
    self.assertEqual(expected_sizes, actual_sizes)

  def test_target_overhead(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(
        target_batch_overhead=.05, target_batch_duration_secs=None, clock=clock)
    batch_duration = lambda batch_size: 1 + .7 * batch_size
    # At 27 items, a batch takes ~20 seconds with 5% (~1 second) overhead.
    expected_sizes = [1, 2, 4, 8, 16, 27, 27, 27]
    actual_sizes = []
    for _ in range(len(expected_sizes)):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        clock.sleep(batch_duration(actual_sizes[-1]))
    self.assertEqual(expected_sizes, actual_sizes)

  def test_variance(self):
    clock = FakeClock()
    variance = 0.25
    batch_estimator = util._BatchSizeEstimator(
        target_batch_overhead=.05, target_batch_duration_secs=None,
        variance=variance, clock=clock)
    batch_duration = lambda batch_size: 1 + .7 * batch_size
    expected_target = 27
    actual_sizes = []
    for _ in range(util._BatchSizeEstimator._MAX_DATA_POINTS - 1):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        clock.sleep(batch_duration(actual_sizes[-1]))
    # Check that we're testing a good range of values.
    stable_set = set(actual_sizes[-20:])
    self.assertGreater(len(stable_set), 3)
    self.assertGreater(
        min(stable_set), expected_target - expected_target * variance)
    self.assertLess(
        max(stable_set), expected_target + expected_target * variance)

  def _run_regression_test(self, linear_regression_fn, test_outliers):
    xs = [random.random() for _ in range(10)]
    ys = [2*x + 1 for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 1)
    self.assertAlmostEqual(b, 2)

    xs = [1 + random.random() for _ in range(100)]
    ys = [7*x + 5 + 0.01 * random.random() for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 5, delta=0.01)
    self.assertAlmostEqual(b, 7, delta=0.01)

    # Test repeated xs
    xs = [1 + random.random()] * 100
    ys = [7 * x + 5 + 0.01 * random.random() for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 0, delta=0.01)
    self.assertAlmostEqual(b, sum(ys)/(len(ys) * xs[0]), delta=0.01)

    if test_outliers:
      xs = [1 + random.random() for _ in range(100)]
      ys = [2*x + 1 for x in xs]
      a, b = linear_regression_fn(xs, ys)
      self.assertAlmostEqual(a, 1)
      self.assertAlmostEqual(b, 2)

      # An outlier or two doesn't affect the result.
      for _ in range(2):
        xs += [10]
        ys += [30]
        a, b = linear_regression_fn(xs, ys)
        self.assertAlmostEqual(a, 1)
        self.assertAlmostEqual(b, 2)

      # But enough of them, and they're no longer outliers.
      xs += [10] * 10
      ys += [30] * 10
      a, b = linear_regression_fn(xs, ys)
      self.assertLess(a, 0.5)
      self.assertGreater(b, 2.5)

  def test_no_numpy_regression(self):
    self._run_regression_test(
        util._BatchSizeEstimator.linear_regression_no_numpy, False)

  def test_numpy_regression(self):
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      import numpy as _
    except ImportError:
      self.skipTest('numpy not available')
    self._run_regression_test(
        util._BatchSizeEstimator.linear_regression_numpy, True)


class IdentityWindowTest(unittest.TestCase):

  def test_window_preserved(self):
    expected_timestamp = timestamp.Timestamp(5)
    expected_window = window.IntervalWindow(1.0, 2.0)

    class AddWindowDoFn(beam.DoFn):
      def process(self, element):
        yield WindowedValue(
            element, expected_timestamp, [expected_window])

    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_windows = [
        TestWindowedValue(kv, expected_timestamp, [expected_window])
        for kv in data]
    before_identity = (pipeline
                       | 'start' >> beam.Create(data)
                       | 'add_windows' >> beam.ParDo(AddWindowDoFn()))
    assert_that(before_identity, equal_to(expected_windows),
                label='before_identity', reify_windows=True)
    after_identity = (before_identity
                      | 'window' >> beam.WindowInto(
                          beam.transforms.util._IdentityWindowFn(
                              coders.IntervalWindowCoder())))
    assert_that(after_identity, equal_to(expected_windows),
                label='after_identity', reify_windows=True)
    pipeline.run()

  def test_no_window_context_fails(self):
    expected_timestamp = timestamp.Timestamp(5)
    # Assuming the default window function is window.GlobalWindows.
    expected_window = window.GlobalWindow()

    class AddTimestampDoFn(beam.DoFn):
      def process(self, element):
        yield window.TimestampedValue(element, expected_timestamp)

    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_windows = [
        TestWindowedValue(kv, expected_timestamp, [expected_window])
        for kv in data]
    before_identity = (pipeline
                       | 'start' >> beam.Create(data)
                       | 'add_timestamps' >> beam.ParDo(AddTimestampDoFn()))
    assert_that(before_identity, equal_to(expected_windows),
                label='before_identity', reify_windows=True)
    after_identity = (before_identity
                      | 'window' >> beam.WindowInto(
                          beam.transforms.util._IdentityWindowFn(
                              coders.GlobalWindowCoder()))
                      # This DoFn will return TimestampedValues, making
                      # WindowFn.AssignContext passed to IdentityWindowFn
                      # contain a window of None. IdentityWindowFn should
                      # raise an exception.
                      | 'add_timestamps2' >> beam.ParDo(AddTimestampDoFn()))
    assert_that(after_identity, equal_to(expected_windows),
                label='after_identity', reify_windows=True)
    with self.assertRaisesRegexp(ValueError, r'window.*None.*add_timestamps2'):
      pipeline.run()


class ReshuffleTest(unittest.TestCase):

  def test_reshuffle_contents_unchanged(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
    result = (pipeline
              | beam.Create(data)
              | beam.Reshuffle())
    assert_that(result, equal_to(data))
    pipeline.run()

  def test_reshuffle_after_gbk_contents_unchanged(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
    expected_result = [(1, [1, 2, 3]), (2, [1, 2]), (3, [1])]

    after_gbk = (pipeline
                 | beam.Create(data)
                 | beam.GroupByKey())
    assert_that(after_gbk, equal_to(expected_result), label='after_gbk')
    after_reshuffle = after_gbk | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_result),
                label='after_reshuffle')
    pipeline.run()

  def test_reshuffle_timestamps_unchanged(self):
    pipeline = TestPipeline()
    timestamp = 5
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
    expected_result = [TestWindowedValue(v, timestamp, [GlobalWindow()])
                       for v in data]
    before_reshuffle = (pipeline
                        | 'start' >> beam.Create(data)
                        | 'add_timestamp' >> beam.Map(
                            lambda v: beam.window.TimestampedValue(v,
                                                                   timestamp)))
    assert_that(before_reshuffle, equal_to(expected_result),
                label='before_reshuffle', reify_windows=True)
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_result),
                label='after_reshuffle', reify_windows=True)
    pipeline.run()

  def test_reshuffle_windows_unchanged(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_data = [TestWindowedValue(v, t, [w]) for (v, t, w) in [
        ((1, contains_in_any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)),
        ((2, contains_in_any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)),
        ((3, [1]), 3.0, IntervalWindow(1.0, 3.0)),
        ((1, [4]), 6.0, IntervalWindow(4.0, 6.0))]]
    before_reshuffle = (pipeline
                        | 'start' >> beam.Create(data)
                        | 'add_timestamp' >> beam.Map(
                            lambda v: beam.window.TimestampedValue(v, v[1]))
                        | 'window' >> beam.WindowInto(Sessions(gap_size=2))
                        | 'group_by_key' >> beam.GroupByKey())
    assert_that(before_reshuffle, equal_to(expected_data),
                label='before_reshuffle', reify_windows=True)
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_data),
                label='after reshuffle', reify_windows=True)
    pipeline.run()

  def test_reshuffle_window_fn_preserved(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_windows = [TestWindowedValue(v, t, [w]) for (v, t, w) in [
        ((1, 1), 1.0, IntervalWindow(1.0, 3.0)),
        ((2, 1), 1.0, IntervalWindow(1.0, 3.0)),
        ((3, 1), 1.0, IntervalWindow(1.0, 3.0)),
        ((1, 2), 2.0, IntervalWindow(2.0, 4.0)),
        ((2, 2), 2.0, IntervalWindow(2.0, 4.0)),
        ((1, 4), 4.0, IntervalWindow(4.0, 6.0))]]
    expected_merged_windows = [TestWindowedValue(v, t, [w]) for (v, t, w) in [
        ((1, contains_in_any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)),
        ((2, contains_in_any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)),
        ((3, [1]), 3.0, IntervalWindow(1.0, 3.0)),
        ((1, [4]), 6.0, IntervalWindow(4.0, 6.0))]]
    before_reshuffle = (pipeline
                        | 'start' >> beam.Create(data)
                        | 'add_timestamp' >> beam.Map(
                            lambda v: TimestampedValue(v, v[1]))
                        | 'window' >> beam.WindowInto(Sessions(gap_size=2)))
    assert_that(before_reshuffle, equal_to(expected_windows),
                label='before_reshuffle', reify_windows=True)
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_windows),
                label='after_reshuffle', reify_windows=True)
    after_group = after_reshuffle | beam.GroupByKey()
    assert_that(after_group, equal_to(expected_merged_windows),
                label='after_group', reify_windows=True)
    pipeline.run()

  def test_reshuffle_global_window(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])]
    before_reshuffle = (pipeline
                        | beam.Create(data)
                        | beam.WindowInto(GlobalWindows())
                        | beam.GroupByKey())
    assert_that(before_reshuffle, equal_to(expected_data),
                label='before_reshuffle')
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_data),
                label='after reshuffle')
    pipeline.run()

  def test_reshuffle_sliding_window(self):
    pipeline = TestPipeline()
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    window_size = 2
    expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])] * window_size
    before_reshuffle = (pipeline
                        | beam.Create(data)
                        | beam.WindowInto(SlidingWindows(
                            size=window_size, period=1))
                        | beam.GroupByKey())
    assert_that(before_reshuffle, equal_to(expected_data),
                label='before_reshuffle')
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    # If Reshuffle applies the sliding window function a second time there
    # should be extra values for each key.
    assert_that(after_reshuffle, equal_to(expected_data),
                label='after reshuffle')
    pipeline.run()

  def test_reshuffle_streaming_global_window(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    pipeline = TestPipeline(options=options)
    data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
    expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])]
    before_reshuffle = (pipeline
                        | beam.Create(data)
                        | beam.WindowInto(GlobalWindows())
                        | beam.GroupByKey())
    assert_that(before_reshuffle, equal_to(expected_data),
                label='before_reshuffle')
    after_reshuffle = before_reshuffle | beam.Reshuffle()
    assert_that(after_reshuffle, equal_to(expected_data),
                label='after reshuffle')
    pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
