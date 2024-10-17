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

# pytype: skip-file

import collections
import importlib
import logging
import math
import random
import re
import time
import unittest
import warnings
from datetime import datetime
from typing import Mapping

import pytest
import pytz

import apache_beam as beam
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import WindowInto
from apache_beam.coders import coders
from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.pvalue import AsList
from apache_beam.pvalue import AsSingleton
from apache_beam.runners import pipeline_context
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import SortLists
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import contains_in_any_order
from apache_beam.testing.util import equal_to
from apache_beam.transforms import trigger
from apache_beam.transforms import util
from apache_beam.transforms import window
from apache_beam.transforms.core import FlatMapTuple
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import typehints
from apache_beam.typehints.sharded_key_type import ShardedKeyType
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.windowed_value import WindowedValue

warnings.filterwarnings(
    'ignore', category=FutureWarning, module='apache_beam.transform.util_test')


class CoGroupByKeyTest(unittest.TestCase):
  def test_co_group_by_key_on_tuple(self):
    with TestPipeline() as pipeline:
      pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2),
                                                     ('b', 3), ('c', 4)])
      pcoll_2 = pipeline | 'Start 2' >> beam.Create([('a', 5), ('a', 6),
                                                     ('c', 7), ('c', 8)])
      result = (pcoll_1, pcoll_2) | beam.CoGroupByKey() | SortLists
      assert_that(
          result,
          equal_to([('a', ([1, 2], [5, 6])), ('b', ([3], [])),
                    ('c', ([4], [7, 8]))]))

  def test_co_group_by_key_on_iterable(self):
    with TestPipeline() as pipeline:
      pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2),
                                                     ('b', 3), ('c', 4)])
      pcoll_2 = pipeline | 'Start 2' >> beam.Create([('a', 5), ('a', 6),
                                                     ('c', 7), ('c', 8)])
      result = iter([pcoll_1, pcoll_2]) | beam.CoGroupByKey() | SortLists
      assert_that(
          result,
          equal_to([('a', ([1, 2], [5, 6])), ('b', ([3], [])),
                    ('c', ([4], [7, 8]))]))

  def test_co_group_by_key_on_list(self):
    with TestPipeline() as pipeline:
      pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2),
                                                     ('b', 3), ('c', 4)])
      pcoll_2 = pipeline | 'Start 2' >> beam.Create([('a', 5), ('a', 6),
                                                     ('c', 7), ('c', 8)])
      result = [pcoll_1, pcoll_2] | beam.CoGroupByKey() | SortLists
      assert_that(
          result,
          equal_to([('a', ([1, 2], [5, 6])), ('b', ([3], [])),
                    ('c', ([4], [7, 8]))]))

  def test_co_group_by_key_on_dict(self):
    with TestPipeline() as pipeline:
      pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2),
                                                     ('b', 3), ('c', 4)])
      pcoll_2 = pipeline | 'Start 2' >> beam.Create([('a', 5), ('a', 6),
                                                     ('c', 7), ('c', 8)])
      result = {'X': pcoll_1, 'Y': pcoll_2} | beam.CoGroupByKey() | SortLists
      assert_that(
          result,
          equal_to([('a', {
              'X': [1, 2], 'Y': [5, 6]
          }), ('b', {
              'X': [3], 'Y': []
          }), ('c', {
              'X': [4], 'Y': [7, 8]
          })]))

  def test_co_group_by_key_on_dict_with_tuple_keys(self):
    with TestPipeline() as pipeline:
      key = ('a', ('b', 'c'))
      pcoll_1 = pipeline | 'Start 1' >> beam.Create([(key, 1)])
      pcoll_2 = pipeline | 'Start 2' >> beam.Create([(key, 2)])
      result = {'X': pcoll_1, 'Y': pcoll_2} | beam.CoGroupByKey() | SortLists
      assert_that(result, equal_to([(key, {'X': [1], 'Y': [2]})]))

  def test_co_group_by_key_on_empty(self):
    with TestPipeline() as pipeline:
      assert_that(
          tuple() | 'EmptyTuple' >> beam.CoGroupByKey(pipeline=pipeline),
          equal_to([]),
          label='AssertEmptyTuple')
      assert_that([] | 'EmptyList' >> beam.CoGroupByKey(pipeline=pipeline),
                  equal_to([]),
                  label='AssertEmptyList')
      assert_that(
          iter([]) | 'EmptyIterable' >> beam.CoGroupByKey(pipeline=pipeline),
          equal_to([]),
          label='AssertEmptyIterable')
      assert_that({} | 'EmptyDict' >> beam.CoGroupByKey(pipeline=pipeline),
                  equal_to([]),
                  label='AssertEmptyDict')

  def test_co_group_by_key_on_one(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | beam.Create([('a', 1), ('b', 2)])
      expected = [('a', ([1], )), ('b', ([2], ))]
      assert_that((pcoll, ) | 'OneTuple' >> beam.CoGroupByKey(),
                  equal_to(expected),
                  label='AssertOneTuple')
      assert_that([pcoll] | 'OneList' >> beam.CoGroupByKey(),
                  equal_to(expected),
                  label='AssertOneList')
      assert_that(
          iter([pcoll]) | 'OneIterable' >> beam.CoGroupByKey(),
          equal_to(expected),
          label='AssertOneIterable')
      assert_that({'tag': pcoll}
                  | 'OneDict' >> beam.CoGroupByKey()
                  | beam.MapTuple(lambda k, v: (k, (v['tag'], ))),
                  equal_to(expected),
                  label='AssertOneDict')


class FakeClock(object):
  def __init__(self, now=time.time()):
    self._now = now

  def __call__(self):
    return self._now

  def sleep(self, duration):
    self._now += duration


class BatchElementsTest(unittest.TestCase):
  NUM_ELEMENTS = 10
  BATCH_SIZE = 5

  @staticmethod
  def _create_test_data():
    scientists = [
        "Einstein",
        "Darwin",
        "Copernicus",
        "Pasteur",
        "Curie",
        "Faraday",
        "Newton",
        "Bohr",
        "Galilei",
        "Maxwell"
    ]

    data = []
    for i in range(BatchElementsTest.NUM_ELEMENTS):
      index = i % len(scientists)
      data.append(scientists[index])
    return data

  def test_constant_batch(self):
    # Assumes a single bundle...
    p = TestPipeline()
    output = (
        p
        | beam.Create(range(35))
        | util.BatchElements(min_batch_size=10, max_batch_size=10)
        | beam.Map(len))
    assert_that(output, equal_to([10, 10, 10, 5]))
    res = p.run()
    res.wait_until_finish()
    metrics = res.metrics()
    results = metrics.query(MetricsFilter().with_name("batch_size"))
    self.assertEqual(len(results["distributions"]), 1)

  def test_constant_batch_no_metrics(self):
    p = TestPipeline()
    output = (
        p
        | beam.Create(range(35))
        | util.BatchElements(
            min_batch_size=10, max_batch_size=10, record_metrics=False)
        | beam.Map(len))
    assert_that(output, equal_to([10, 10, 10, 5]))
    res = p.run()
    res.wait_until_finish()
    metrics = res.metrics()
    results = metrics.query(MetricsFilter().with_name("batch_size"))
    self.assertEqual(len(results["distributions"]), 0)

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
          | beam.Create(range(47), reshuffle=False)
          | beam.Map(lambda t: window.TimestampedValue(t, t))
          | beam.WindowInto(window.FixedWindows(30))
          | util.BatchElements(
              min_batch_size=5, max_batch_size=10, clock=FakeClock())
          | beam.Map(len))
      assert_that(
          res,
          equal_to([
              5,
              5,
              10,
              10,  # elements in [0, 30)
              10,
              7,  # elements in [30, 47)
          ]))

  def test_global_batch_timestamps(self):
    # Assumes a single bundle
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(3), reshuffle=False)
          | util.BatchElements(min_batch_size=2, max_batch_size=2)
          | beam.Map(
              lambda batch,
              timestamp=beam.DoFn.TimestampParam: (len(batch), timestamp)))
      assert_that(
          res,
          equal_to([
              (2, GlobalWindow().max_timestamp()),
              (1, GlobalWindow().max_timestamp()),
          ]))

  def test_sized_batches(self):
    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              'a', 'a',                # First batch.
              'aaaaaaaaaa',            # Second batch.
              'aaaaa', 'aaaaa',        # Third batch.
              'a', 'aaaaaaa', 'a', 'a' # Fourth batch.
              ], reshuffle=False)
          | util.BatchElements(
              min_batch_size=10, max_batch_size=10, element_size_fn=len)
          | beam.Map(lambda batch: ''.join(batch))
          | beam.Map(len))
      assert_that(res, equal_to([2, 10, 10, 10]))

  def test_sized_windowed_batches(self):
    # Assumes a single bundle, in order...
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(1, 8), reshuffle=False)
          | beam.Map(lambda t: window.TimestampedValue('a' * t, t))
          | beam.WindowInto(window.FixedWindows(3))
          | util.BatchElements(
              min_batch_size=11,
              max_batch_size=11,
              element_size_fn=len,
              clock=FakeClock())
          | beam.Map(lambda batch: ''.join(batch)))
      assert_that(
          res,
          equal_to([
              'a' * (1+2), # Elements in [1, 3)
              'a' * (3+4), # Elements in [3, 6)
              'a' * 5,
              'a' * 6, # Elements in [6, 9)
              'a' * 7,
          ]))

  def test_target_duration(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(
        target_batch_overhead=None, target_batch_duration_secs=10, clock=clock)
    batch_duration = lambda batch_size: 1 + .7 * batch_size
    # 14 * .7 is as close as we can get to 10 as possible.
    expected_sizes = [1, 2, 4, 8, 14, 14, 14]
    actual_sizes = []
    for _ in range(len(expected_sizes)):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        clock.sleep(batch_duration(actual_sizes[-1]))
    self.assertEqual(expected_sizes, actual_sizes)

  def test_target_duration_including_fixed_cost(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(
        target_batch_overhead=None,
        target_batch_duration_secs_including_fixed_cost=10,
        clock=clock)
    batch_duration = lambda batch_size: 1 + .7 * batch_size
    # 1 + 14 * .7 is as close as we can get to 10 as possible.
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
        target_batch_overhead=.05,
        target_batch_duration_secs=None,
        variance=variance,
        clock=clock)
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

  def test_ignore_first_n_batch_size(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(
        clock=clock, ignore_first_n_seen_per_batch_size=2)

    expected_sizes = [
        1, 1, 1, 2, 2, 2, 4, 4, 4, 8, 8, 8, 16, 16, 16, 32, 32, 32, 64, 64, 64
    ]
    actual_sizes = []
    for i in range(len(expected_sizes)):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        if i % 3 == 2:
          clock.sleep(0.01)
        else:
          clock.sleep(1)

    self.assertEqual(expected_sizes, actual_sizes)

    # Check we only record the third timing.
    expected_data_batch_sizes = [1, 2, 4, 8, 16, 32, 64]
    actual_data_batch_sizes = [x[0] for x in batch_estimator._data]
    self.assertEqual(expected_data_batch_sizes, actual_data_batch_sizes)
    expected_data_timing = [0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01]
    for i in range(len(expected_data_timing)):
      self.assertAlmostEqual(
          expected_data_timing[i], batch_estimator._data[i][1])

  def test_ignore_next_timing(self):
    clock = FakeClock()
    batch_estimator = util._BatchSizeEstimator(clock=clock)
    batch_estimator.ignore_next_timing()

    expected_sizes = [1, 1, 2, 4, 8, 16]
    actual_sizes = []
    for i in range(len(expected_sizes)):
      actual_sizes.append(batch_estimator.next_batch_size())
      with batch_estimator.record_time(actual_sizes[-1]):
        if i == 0:
          clock.sleep(1)
        else:
          clock.sleep(0.01)

    self.assertEqual(expected_sizes, actual_sizes)

    # Check the first record_time was skipped.
    expected_data_batch_sizes = [1, 2, 4, 8, 16]
    actual_data_batch_sizes = [x[0] for x in batch_estimator._data]
    self.assertEqual(expected_data_batch_sizes, actual_data_batch_sizes)
    expected_data_timing = [0.01, 0.01, 0.01, 0.01, 0.01]
    for i in range(len(expected_data_timing)):
      self.assertAlmostEqual(
          expected_data_timing[i], batch_estimator._data[i][1])

  def _run_regression_test(self, linear_regression_fn, test_outliers):
    xs = [random.random() for _ in range(10)]
    ys = [2 * x + 1 for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 1)
    self.assertAlmostEqual(b, 2)

    xs = [1 + random.random() for _ in range(100)]
    ys = [7 * x + 5 + 0.01 * random.random() for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 5, delta=0.02)
    self.assertAlmostEqual(b, 7, delta=0.02)

    # Test repeated xs
    xs = [1 + random.random()] * 100
    ys = [7 * x + 5 + 0.01 * random.random() for x in xs]
    a, b = linear_regression_fn(xs, ys)
    self.assertAlmostEqual(a, 0, delta=0.02)
    self.assertAlmostEqual(b, sum(ys) / (len(ys) * xs[0]), delta=0.02)

    if test_outliers:
      xs = [1 + random.random() for _ in range(100)]
      ys = [2 * x + 1 for x in xs]
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

  def test_stateful_constant_batch(self):
    # Assumes a single bundle...
    p = TestPipeline()
    output = (
        p
        | beam.Create(range(35))
        | util.BatchElements(
            min_batch_size=10, max_batch_size=10, max_batch_duration_secs=100)
        | beam.Map(len))
    assert_that(output, equal_to([10, 10, 10, 5]))
    res = p.run()
    res.wait_until_finish()

  def test_stateful_in_global_window(self):
    with TestPipeline() as pipeline:
      collection = pipeline \
                   | beam.Create(
                     BatchElementsTest._create_test_data()) \
                   | util.BatchElements(
                     min_batch_size=BatchElementsTest.BATCH_SIZE,
                     max_batch_size=BatchElementsTest.BATCH_SIZE,
                     max_batch_duration_secs=100)
      num_batches = collection | beam.combiners.Count.Globally()
      assert_that(
          num_batches,
          equal_to([
              int(
                  math.ceil(
                      BatchElementsTest.NUM_ELEMENTS /
                      BatchElementsTest.BATCH_SIZE))
          ]))

  def test_stateful_buffering_timer_in_fixed_window_streaming(self):
    window_duration = 6
    max_buffering_duration_secs = 100

    start_time = timestamp.Timestamp(0)
    test_stream = (
        TestStream().add_elements([
            TimestampedValue(value, start_time + i) for i,
            value in enumerate(BatchElementsTest._create_test_data())
        ]).advance_processing_time(150).advance_watermark_to(
            start_time + window_duration).advance_watermark_to(
                start_time + window_duration +
                1).advance_watermark_to_infinity())

    with TestPipeline(options=StandardOptions(streaming=True)) as pipeline:
      # To trigger the processing time timer, use a fake clock with start time
      # being Timestamp(0).
      fake_clock = FakeClock(now=start_time)

      num_elements_per_batch = (
          pipeline | test_stream
          | "fixed window" >> WindowInto(FixedWindows(window_duration))
          | util.BatchElements(
              min_batch_size=BatchElementsTest.BATCH_SIZE,
              max_batch_size=BatchElementsTest.BATCH_SIZE,
              max_batch_duration_secs=max_buffering_duration_secs,
              clock=fake_clock)
          | "count elements in batch" >> Map(lambda x: (None, len(x)))
          | GroupByKey()
          | "global window" >> WindowInto(GlobalWindows())
          | FlatMapTuple(lambda k, vs: vs))

      # Window duration is 6 and batch size is 5, so output batch size
      # should be 5 (flush because of batch size reached).
      expected_0 = 5
      # There is only one element left in the window so batch size
      # should be 1 (flush because of max buffering duration reached).
      expected_1 = 1
      # Collection has 10 elements, there are only 4 left, so batch size should
      # be 4 (flush because of end of window reached).
      expected_2 = 4
      assert_that(
          num_elements_per_batch,
          equal_to([expected_0, expected_1, expected_2]),
          "assert2")

  def test_stateful_buffering_timer_in_global_window_streaming(self):
    max_buffering_duration_secs = 42

    start_time = timestamp.Timestamp(0)
    test_stream = TestStream().advance_watermark_to(start_time)
    for i, value in enumerate(BatchElementsTest._create_test_data()):
      test_stream.add_elements(
          [TimestampedValue(value, start_time + i)]) \
        .advance_processing_time(5)
    test_stream.advance_watermark_to(
        start_time + BatchElementsTest.NUM_ELEMENTS + 1) \
      .advance_watermark_to_infinity()

    with TestPipeline(options=StandardOptions(streaming=True)) as pipeline:
      # Set a batch size larger than the total number of elements.
      # Since we're in a global window, we would have been waiting
      # for all the elements to arrive without the buffering time limit.
      batch_size = BatchElementsTest.NUM_ELEMENTS * 2

      # To trigger the processing time timer, use a fake clock with start time
      # being Timestamp(0). Since the fake clock never really advances during
      # the pipeline execution, meaning that the timer is always set to the same
      # value, the timer will be fired on every element after the first firing.
      fake_clock = FakeClock(now=start_time)

      num_elements_per_batch = (
          pipeline | test_stream
          | WindowInto(
              GlobalWindows(),
              trigger=Repeatedly(AfterCount(1)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | util.BatchElements(
              min_batch_size=batch_size,
              max_batch_size=batch_size,
              max_batch_duration_secs=max_buffering_duration_secs,
              clock=fake_clock)
          | 'count elements in batch' >> Map(lambda x: (None, len(x)))
          | GroupByKey()
          | FlatMapTuple(lambda k, vs: vs))

      # We will flush twice when the max buffering duration is reached and when
      # the global window ends.
      assert_that(num_elements_per_batch, equal_to([9, 1]))

  def test_stateful_grows_to_max_batch(self):
    # Assumes a single bundle...
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(range(164))
          | util.BatchElements(
              min_batch_size=1,
              max_batch_size=50,
              max_batch_duration_secs=100,
              clock=FakeClock())
          | beam.Map(len))
      assert_that(res, equal_to([1, 1, 2, 4, 8, 16, 32, 50, 50]))


class IdentityWindowTest(unittest.TestCase):
  def test_window_preserved(self):
    expected_timestamp = timestamp.Timestamp(5)
    expected_window = window.IntervalWindow(1.0, 2.0)

    class AddWindowDoFn(beam.DoFn):
      def process(self, element):
        yield WindowedValue(element, expected_timestamp, [expected_window])

    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_windows = [
          TestWindowedValue(kv, expected_timestamp, [expected_window])
          for kv in data
      ]
      before_identity = (
          pipeline
          | 'start' >> beam.Create(data)
          | 'add_windows' >> beam.ParDo(AddWindowDoFn()))
      assert_that(
          before_identity,
          equal_to(expected_windows),
          label='before_identity',
          reify_windows=True)
      after_identity = (
          before_identity
          | 'window' >> beam.WindowInto(
              beam.transforms.util._IdentityWindowFn(
                  coders.IntervalWindowCoder())))
      assert_that(
          after_identity,
          equal_to(expected_windows),
          label='after_identity',
          reify_windows=True)

  def test_no_window_context_fails(self):
    expected_timestamp = timestamp.Timestamp(5)
    # Assuming the default window function is window.GlobalWindows.
    expected_window = window.GlobalWindow()

    class AddTimestampDoFn(beam.DoFn):
      def process(self, element):
        yield window.TimestampedValue(element, expected_timestamp)

    with self.assertRaisesRegex(ValueError, r'window.*None.*add_timestamps2'):
      with TestPipeline() as pipeline:
        data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
        expected_windows = [
            TestWindowedValue(kv, expected_timestamp, [expected_window])
            for kv in data
        ]
        before_identity = (
            pipeline
            | 'start' >> beam.Create(data)
            | 'add_timestamps' >> beam.ParDo(AddTimestampDoFn()))
        assert_that(
            before_identity,
            equal_to(expected_windows),
            label='before_identity',
            reify_windows=True)
        after_identity = (
            before_identity
            | 'window' >> beam.WindowInto(
                beam.transforms.util._IdentityWindowFn(
                    coders.GlobalWindowCoder()))
            # This DoFn will return TimestampedValues, making
            # WindowFn.AssignContext passed to IdentityWindowFn
            # contain a window of None. IdentityWindowFn should
            # raise an exception.
            | 'add_timestamps2' >> beam.ParDo(AddTimestampDoFn()))
        assert_that(
            after_identity,
            equal_to(expected_windows),
            label='after_identity',
            reify_windows=True)


class ReshuffleTest(unittest.TestCase):
  def test_reshuffle_contents_unchanged(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
      result = (pipeline | beam.Create(data) | beam.Reshuffle())
      assert_that(result, equal_to(data))

  def test_reshuffle_contents_unchanged_with_buckets(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
      buckets = 2
      result = (pipeline | beam.Create(data) | beam.Reshuffle(buckets))
      assert_that(result, equal_to(data))

  def test_reshuffle_contents_unchanged_with_wrong_buckets(self):
    wrong_buckets = [0, -1, "wrong", 2.5]
    for wrong_bucket in wrong_buckets:
      with self.assertRaisesRegex(ValueError,
                                  'If `num_buckets` is set, it has to be an '
                                  'integer greater than 0, got %s' %
                                  wrong_bucket):
        beam.Reshuffle(wrong_bucket)

  def test_reshuffle_after_gbk_contents_unchanged(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
      expected_result = [(1, [1, 2, 3]), (2, [1, 2]), (3, [1])]

      after_gbk = (
          pipeline
          | beam.Create(data)
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(after_gbk, equal_to(expected_result), label='after_gbk')
      after_reshuffle = after_gbk | beam.Reshuffle()
      assert_that(
          after_reshuffle, equal_to(expected_result), label='after_reshuffle')

  def test_reshuffle_timestamps_unchanged(self):
    with TestPipeline() as pipeline:
      timestamp = 5
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)]
      expected_result = [
          TestWindowedValue(v, timestamp, [GlobalWindow()]) for v in data
      ]
      before_reshuffle = (
          pipeline
          | 'start' >> beam.Create(data)
          | 'add_timestamp' >>
          beam.Map(lambda v: beam.window.TimestampedValue(v, timestamp)))
      assert_that(
          before_reshuffle,
          equal_to(expected_result),
          label='before_reshuffle',
          reify_windows=True)
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      assert_that(
          after_reshuffle,
          equal_to(expected_result),
          label='after_reshuffle',
          reify_windows=True)

  def test_reshuffle_windows_unchanged(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_data = [
          TestWindowedValue(v, t - .001, [w])
          for (v, t, w) in [((1, contains_in_any_order([2, 1])),
                             4.0,
                             IntervalWindow(1.0, 4.0)),
                            ((2, contains_in_any_order([2, 1])),
                             4.0,
                             IntervalWindow(1.0, 4.0)), (
                                 (3, [1]), 3.0, IntervalWindow(1.0, 3.0)), (
                                     (1, [4]), 6.0, IntervalWindow(4.0, 6.0))]
      ]
      before_reshuffle = (
          pipeline
          | 'start' >> beam.Create(data)
          | 'add_timestamp' >>
          beam.Map(lambda v: beam.window.TimestampedValue(v, v[1]))
          | 'window' >> beam.WindowInto(Sessions(gap_size=2))
          | 'group_by_key' >> beam.GroupByKey())
      assert_that(
          before_reshuffle,
          equal_to(expected_data),
          label='before_reshuffle',
          reify_windows=True)
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      assert_that(
          after_reshuffle,
          equal_to(expected_data),
          label='after reshuffle',
          reify_windows=True)

  def test_reshuffle_window_fn_preserved(self):
    any_order = contains_in_any_order
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_windows = [
          TestWindowedValue(v, t, [w])
          for (v, t, w) in [((1, 1), 1.0, IntervalWindow(1.0, 3.0)), (
              (2, 1), 1.0, IntervalWindow(1.0, 3.0)), (
                  (3, 1), 1.0, IntervalWindow(1.0, 3.0)), (
                      (1, 2), 2.0, IntervalWindow(2.0, 4.0)), (
                          (2, 2), 2.0,
                          IntervalWindow(2.0, 4.0)), ((1, 4),
                                                      4.0,
                                                      IntervalWindow(4.0, 6.0))]
      ]
      expected_merged_windows = [
          TestWindowedValue(v, t - .001, [w])
          for (v, t,
               w) in [((1, any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)), (
                   (2, any_order([2, 1])), 4.0, IntervalWindow(1.0, 4.0)), (
                       (3, [1]), 3.0,
                       IntervalWindow(1.0, 3.0)), ((1, [4]),
                                                   6.0,
                                                   IntervalWindow(4.0, 6.0))]
      ]
      before_reshuffle = (
          pipeline
          | 'start' >> beam.Create(data)
          | 'add_timestamp' >> beam.Map(lambda v: TimestampedValue(v, v[1]))
          | 'window' >> beam.WindowInto(Sessions(gap_size=2)))
      assert_that(
          before_reshuffle,
          equal_to(expected_windows),
          label='before_reshuffle',
          reify_windows=True)
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      assert_that(
          after_reshuffle,
          equal_to(expected_windows),
          label='after_reshuffle',
          reify_windows=True)
      after_group = after_reshuffle | beam.GroupByKey()
      assert_that(
          after_group,
          equal_to(expected_merged_windows),
          label='after_group',
          reify_windows=True)

  def test_reshuffle_global_window(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])]
      before_reshuffle = (
          pipeline
          | beam.Create(data)
          | beam.WindowInto(GlobalWindows())
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          before_reshuffle, equal_to(expected_data), label='before_reshuffle')
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      assert_that(
          after_reshuffle, equal_to(expected_data), label='after reshuffle')

  def test_reshuffle_sliding_window(self):
    with TestPipeline() as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      window_size = 2
      expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])] * window_size
      before_reshuffle = (
          pipeline
          | beam.Create(data)
          | beam.WindowInto(SlidingWindows(size=window_size, period=1))
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          before_reshuffle, equal_to(expected_data), label='before_reshuffle')
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      # If Reshuffle applies the sliding window function a second time there
      # should be extra values for each key.
      assert_that(
          after_reshuffle, equal_to(expected_data), label='after reshuffle')

  def test_reshuffle_streaming_global_window(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])]
      before_reshuffle = (
          pipeline
          | beam.Create(data)
          | beam.WindowInto(GlobalWindows())
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          before_reshuffle, equal_to(expected_data), label='before_reshuffle')
      after_reshuffle = before_reshuffle | beam.Reshuffle()
      assert_that(
          after_reshuffle, equal_to(expected_data), label='after reshuffle')

  def test_reshuffle_streaming_global_window_with_buckets(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as pipeline:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_data = [(1, [1, 2, 4]), (2, [1, 2]), (3, [1])]
      buckets = 2
      before_reshuffle = (
          pipeline
          | beam.Create(data)
          | beam.WindowInto(GlobalWindows())
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(
          before_reshuffle, equal_to(expected_data), label='before_reshuffle')
      after_reshuffle = before_reshuffle | beam.Reshuffle(buckets)
      assert_that(
          after_reshuffle, equal_to(expected_data), label='after reshuffle')

  @pytest.mark.it_validatesrunner
  def test_reshuffle_preserves_timestamps(self):
    with TestPipeline() as pipeline:

      # Create a PCollection and assign each element with a different timestamp.
      before_reshuffle = (
          pipeline
          | beam.Create([
              {
                  'name': 'foo', 'timestamp': MIN_TIMESTAMP
              },
              {
                  'name': 'foo', 'timestamp': 0
              },
              {
                  'name': 'bar', 'timestamp': 33
              },
              {
                  'name': 'bar', 'timestamp': 0
              },
          ])
          | beam.Map(
              lambda element: beam.window.TimestampedValue(
                  element, element['timestamp'])))

      # Reshuffle the PCollection above and assign the timestamp of an element
      # to that element again.
      after_reshuffle = before_reshuffle | beam.Reshuffle()

      # Given an element, emits a string which contains the timestamp and the
      # name field of the element.
      def format_with_timestamp(element, timestamp=beam.DoFn.TimestampParam):
        t = str(timestamp)
        if timestamp == MIN_TIMESTAMP:
          t = 'MIN_TIMESTAMP'
        elif timestamp == MAX_TIMESTAMP:
          t = 'MAX_TIMESTAMP'
        return '{} - {}'.format(t, element['name'])

      # Combine each element in before_reshuffle with its timestamp.
      formatted_before_reshuffle = (
          before_reshuffle
          | "Get before_reshuffle timestamp" >> beam.Map(format_with_timestamp))

      # Combine each element in after_reshuffle with its timestamp.
      formatted_after_reshuffle = (
          after_reshuffle
          | "Get after_reshuffle timestamp" >> beam.Map(format_with_timestamp))

      expected_data = [
          'MIN_TIMESTAMP - foo',
          'Timestamp(0) - foo',
          'Timestamp(33) - bar',
          'Timestamp(0) - bar'
      ]

      # Can't compare formatted_before_reshuffle and formatted_after_reshuffle
      # directly, because they are deferred PCollections while equal_to only
      # takes a concrete argument.
      assert_that(
          formatted_before_reshuffle,
          equal_to(expected_data),
          label="formatted_before_reshuffle")
      assert_that(
          formatted_after_reshuffle,
          equal_to(expected_data),
          label="formatted_after_reshuffle")


class WithKeysTest(unittest.TestCase):
  def setUp(self):
    self.l = [1, 2, 3]

  def test_constant_k(self):
    with TestPipeline() as p:
      pc = p | beam.Create(self.l)
      with_keys = pc | util.WithKeys('k')
      assert_that(with_keys, equal_to([('k', 1), ('k', 2), ('k', 3)], ))

  def test_callable_k(self):
    with TestPipeline() as p:
      pc = p | beam.Create(self.l)
      with_keys = pc | util.WithKeys(lambda x: x * x)
      assert_that(with_keys, equal_to([(1, 1), (4, 2), (9, 3)]))

  @staticmethod
  def _test_args_kwargs_fn(x, multiply, subtract):
    return x * multiply - subtract

  def test_args_kwargs_k(self):
    with TestPipeline() as p:
      pc = p | beam.Create(self.l)
      with_keys = pc | util.WithKeys(
          WithKeysTest._test_args_kwargs_fn, 2, subtract=1)
      assert_that(with_keys, equal_to([(1, 1), (3, 2), (5, 3)]))

  def test_sideinputs(self):
    with TestPipeline() as p:
      pc = p | beam.Create(self.l)
      si1 = AsList(p | "side input 1" >> beam.Create([1, 2, 3]))
      si2 = AsSingleton(p | "side input 2" >> beam.Create([10]))
      with_keys = pc | util.WithKeys(
          lambda x,
          the_list,
          the_singleton: x + sum(the_list) + the_singleton,
          si1,
          the_singleton=si2)
      assert_that(with_keys, equal_to([(17, 1), (18, 2), (19, 3)]))


class GroupIntoBatchesTest(unittest.TestCase):
  NUM_ELEMENTS = 10
  BATCH_SIZE = 5

  @staticmethod
  def _create_test_data():
    scientists = [
        "Einstein",
        "Darwin",
        "Copernicus",
        "Pasteur",
        "Curie",
        "Faraday",
        "Newton",
        "Bohr",
        "Galilei",
        "Maxwell"
    ]

    data = []
    for i in range(GroupIntoBatchesTest.NUM_ELEMENTS):
      index = i % len(scientists)
      data.append(("key", scientists[index]))
    return data

  def test_in_global_window(self):
    with TestPipeline() as pipeline:
      collection = pipeline \
                   | beam.Create(GroupIntoBatchesTest._create_test_data()) \
                   | util.GroupIntoBatches(GroupIntoBatchesTest.BATCH_SIZE)
      num_batches = collection | beam.combiners.Count.Globally()
      assert_that(
          num_batches,
          equal_to([
              int(
                  math.ceil(
                      GroupIntoBatchesTest.NUM_ELEMENTS /
                      GroupIntoBatchesTest.BATCH_SIZE))
          ]))

  def test_in_global_window_with_synthetic_source(self):
    with beam.Pipeline() as pipeline:
      collection = (
          pipeline
          | beam.io.Read(
              SyntheticSource({
                  "numRecords": 10, "keySizeBytes": 1, "valueSizeBytes": 1
              }))
          | "identical keys" >> beam.Map(lambda x: (None, x[1]))
          | "Group key" >> beam.GroupIntoBatches(2)
          | "count size" >> beam.Map(lambda x: len(x[1])))
      assert_that(collection, equal_to([2, 2, 2, 2, 2]))

  def test_with_sharded_key_in_global_window(self):
    with TestPipeline() as pipeline:
      collection = (
          pipeline
          | beam.Create(GroupIntoBatchesTest._create_test_data())
          | util.GroupIntoBatches.WithShardedKey(
              GroupIntoBatchesTest.BATCH_SIZE))
      num_batches = collection | beam.combiners.Count.Globally()
      assert_that(
          num_batches,
          equal_to([
              int(
                  math.ceil(
                      GroupIntoBatchesTest.NUM_ELEMENTS /
                      GroupIntoBatchesTest.BATCH_SIZE))
          ]))

  def test_buffering_timer_in_fixed_window_streaming(self):
    window_duration = 6
    max_buffering_duration_secs = 100

    start_time = timestamp.Timestamp(0)
    test_stream = (
        TestStream().add_elements([
            TimestampedValue(value, start_time + i) for i,
            value in enumerate(GroupIntoBatchesTest._create_test_data())
        ]).advance_processing_time(150).advance_watermark_to(
            start_time + window_duration).advance_watermark_to(
                start_time + window_duration +
                1).advance_watermark_to_infinity())

    with TestPipeline(options=StandardOptions(streaming=True)) as pipeline:
      # To trigger the processing time timer, use a fake clock with start time
      # being Timestamp(0).
      fake_clock = FakeClock(now=start_time)

      num_elements_per_batch = (
          pipeline | test_stream
          | "fixed window" >> WindowInto(FixedWindows(window_duration))
          | util.GroupIntoBatches(
              GroupIntoBatchesTest.BATCH_SIZE,
              max_buffering_duration_secs,
              fake_clock)
          | "count elements in batch" >> Map(lambda x: (None, len(x[1])))
          | GroupByKey()
          | "global window" >> WindowInto(GlobalWindows())
          | FlatMapTuple(lambda k, vs: vs))

      # Window duration is 6 and batch size is 5, so output batch size
      # should be 5 (flush because of batch size reached).
      expected_0 = 5
      # There is only one element left in the window so batch size
      # should be 1 (flush because of max buffering duration reached).
      expected_1 = 1
      # Collection has 10 elements, there are only 4 left, so batch size should
      # be 4 (flush because of end of window reached).
      expected_2 = 4
      assert_that(
          num_elements_per_batch,
          equal_to([expected_0, expected_1, expected_2]),
          "assert2")

  def test_buffering_timer_in_global_window_streaming(self):
    max_buffering_duration_secs = 42

    start_time = timestamp.Timestamp(0)
    test_stream = TestStream().advance_watermark_to(start_time)
    for i, value in enumerate(GroupIntoBatchesTest._create_test_data()):
      test_stream.add_elements(
          [TimestampedValue(value, start_time + i)]) \
        .advance_processing_time(5)
    test_stream.advance_watermark_to(
        start_time + GroupIntoBatchesTest.NUM_ELEMENTS + 1) \
      .advance_watermark_to_infinity()

    with TestPipeline(options=StandardOptions(streaming=True)) as pipeline:
      # Set a batch size larger than the total number of elements.
      # Since we're in a global window, we would have been waiting
      # for all the elements to arrive without the buffering time limit.
      batch_size = GroupIntoBatchesTest.NUM_ELEMENTS * 2

      # To trigger the processing time timer, use a fake clock with start time
      # being Timestamp(0). Since the fake clock never really advances during
      # the pipeline execution, meaning that the timer is always set to the same
      # value, the timer will be fired on every element after the first firing.
      fake_clock = FakeClock(now=start_time)

      num_elements_per_batch = (
          pipeline | test_stream
          | WindowInto(
              GlobalWindows(),
              trigger=Repeatedly(AfterCount(1)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | util.GroupIntoBatches(
              batch_size, max_buffering_duration_secs, fake_clock)
          | 'count elements in batch' >> Map(lambda x: (None, len(x[1])))
          | GroupByKey()
          | FlatMapTuple(lambda k, vs: vs))

      # We will flush twice when the max buffering duration is reached and when
      # the global window ends.
      assert_that(num_elements_per_batch, equal_to([9, 1]))

  def test_output_typehints(self):
    transform = util.GroupIntoBatches.WithShardedKey(
        GroupIntoBatchesTest.BATCH_SIZE)
    unused_input_type = typehints.Tuple[str, str]
    output_type = transform.infer_output_type(unused_input_type)
    self.assertTrue(isinstance(output_type, typehints.TupleConstraint))
    k, v = output_type.tuple_types
    self.assertTrue(isinstance(k, ShardedKeyType))
    self.assertTrue(isinstance(v, typehints.IterableTypeConstraint))

    with TestPipeline() as pipeline:
      collection = (
          pipeline
          | beam.Create([((1, 2), 'a'), ((2, 3), 'b')])
          | util.GroupIntoBatches.WithShardedKey(
              GroupIntoBatchesTest.BATCH_SIZE))
      self.assertTrue(
          collection.element_type,
          typehints.Tuple[
              ShardedKeyType[typehints.Tuple[int, int]],  # type: ignore[misc]
              typehints.Iterable[str]])

  def test_runtime_type_check(self):
    options = PipelineOptions()
    options.view_as(TypeOptions).runtime_type_check = True
    with TestPipeline(options=options) as pipeline:
      collection = (
          pipeline
          | beam.Create(GroupIntoBatchesTest._create_test_data())
          | util.GroupIntoBatches(GroupIntoBatchesTest.BATCH_SIZE))
      num_batches = collection | beam.combiners.Count.Globally()
      assert_that(
          num_batches,
          equal_to([
              int(
                  math.ceil(
                      GroupIntoBatchesTest.NUM_ELEMENTS /
                      GroupIntoBatchesTest.BATCH_SIZE))
          ]))

  def _test_runner_api_round_trip(self, transform, urn):
    context = pipeline_context.PipelineContext()
    proto = transform.to_runner_api(context)
    self.assertEqual(urn, proto.urn)
    payload = (
        proto_utils.parse_Bytes(
            proto.payload, beam_runner_api_pb2.GroupIntoBatchesPayload))
    self.assertEqual(transform.params.batch_size, payload.batch_size)
    self.assertEqual(
        transform.params.max_buffering_duration_secs * 1000,
        payload.max_buffering_duration_millis)

    transform_from_proto = (
        transform.__class__.from_runner_api_parameter(None, payload, None))
    self.assertIsInstance(transform_from_proto, transform.__class__)
    self.assertEqual(transform.params, transform_from_proto.params)

  def test_runner_api(self):
    batch_size = 10
    max_buffering_duration_secs = [None, 0, 5]

    for duration in max_buffering_duration_secs:
      self._test_runner_api_round_trip(
          util.GroupIntoBatches(batch_size, duration),
          common_urns.group_into_batches_components.GROUP_INTO_BATCHES.urn)
    self._test_runner_api_round_trip(
        util.GroupIntoBatches(batch_size),
        common_urns.group_into_batches_components.GROUP_INTO_BATCHES.urn)

    for duration in max_buffering_duration_secs:
      self._test_runner_api_round_trip(
          util.GroupIntoBatches.WithShardedKey(batch_size, duration),
          common_urns.composites.GROUP_INTO_BATCHES_WITH_SHARDED_KEY.urn)
    self._test_runner_api_round_trip(
        util.GroupIntoBatches.WithShardedKey(batch_size),
        common_urns.composites.GROUP_INTO_BATCHES_WITH_SHARDED_KEY.urn)


class ToStringTest(unittest.TestCase):
  def test_tostring_elements(self):
    with TestPipeline() as p:
      result = (p | beam.Create([1, 1, 2, 3]) | util.ToString.Element())
      assert_that(result, equal_to(["1", "1", "2", "3"]))

  def test_tostring_iterables(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create([("one", "two", "three"), ("four", "five", "six")])
          | util.ToString.Iterables())
      assert_that(result, equal_to(["one,two,three", "four,five,six"]))

  def test_tostring_iterables_with_delimeter(self):
    with TestPipeline() as p:
      data = [("one", "two", "three"), ("four", "five", "six")]
      result = (p | beam.Create(data) | util.ToString.Iterables("\t"))
      assert_that(result, equal_to(["one\ttwo\tthree", "four\tfive\tsix"]))

  def test_tostring_kvs(self):
    with TestPipeline() as p:
      result = (p | beam.Create([("one", 1), ("two", 2)]) | util.ToString.Kvs())
      assert_that(result, equal_to(["one,1", "two,2"]))

  def test_tostring_kvs_delimeter(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create([("one", 1), ("two", 2)]) | util.ToString.Kvs("\t"))
      assert_that(result, equal_to(["one\t1", "two\t2"]))

  def test_tostring_kvs_empty_delimeter(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create([("one", 1), ("two", 2)]) | util.ToString.Kvs(""))
      assert_that(result, equal_to(["one1", "two2"]))


class LogElementsTest(unittest.TestCase):
  @pytest.fixture(scope="function")
  def _capture_stdout_log(request, capsys):
    with TestPipeline() as p:
      result = (
          p | beam.Create([
              TimestampedValue(
                  "event",
                  datetime(2022, 10, 1, 0, 0, 0, 0,
                           tzinfo=pytz.UTC).timestamp()),
              TimestampedValue(
                  "event",
                  datetime(2022, 10, 2, 0, 0, 0, 0,
                           tzinfo=pytz.UTC).timestamp()),
          ])
          | beam.WindowInto(FixedWindows(60))
          | util.LogElements(
              prefix='prefix_', with_window=True, with_timestamp=True))

    request.captured_stdout = capsys.readouterr().out
    return result

  @pytest.mark.usefixtures("_capture_stdout_log")
  def test_stdout_logs(self):
    assert self.captured_stdout == \
      ("prefix_event, timestamp='2022-10-01T00:00:00Z', "
       "window(start=2022-10-01T00:00:00Z, end=2022-10-01T00:01:00Z)\n"
       "prefix_event, timestamp='2022-10-02T00:00:00Z', "
       "window(start=2022-10-02T00:00:00Z, end=2022-10-02T00:01:00Z)\n"), \
      f'Received from stdout: {self.captured_stdout}'

  def test_ptransform_output(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(['a', 'b', 'c'])
          | util.LogElements(prefix='prefix_'))
      assert_that(result, equal_to(['a', 'b', 'c']))

  @pytest.fixture(scope="function")
  def _capture_logs(request, caplog):
    with caplog.at_level(logging.INFO):
      with TestPipeline() as p:
        _ = (
            p | "info" >> beam.Create(["element"])
            | "I" >> beam.LogElements(prefix='info_', level=logging.INFO))
        _ = (
            p | "warning" >> beam.Create(["element"])
            | "W" >> beam.LogElements(prefix='warning_', level=logging.WARNING))
        _ = (
            p | "error" >> beam.Create(["element"])
            | "E" >> beam.LogElements(prefix='error_', level=logging.ERROR))

    request.captured_log = caplog.text

  @pytest.mark.usefixtures("_capture_logs")
  def test_setting_level_uses_appropriate_log_channel(self):
    self.assertTrue(
        re.compile('INFO(.*)info_element').search(self.captured_log))
    self.assertTrue(
        re.compile('WARNING(.*)warning_element').search(self.captured_log))
    self.assertTrue(
        re.compile('ERROR(.*)error_element').search(self.captured_log))


class ReifyTest(unittest.TestCase):
  def test_timestamp(self):
    l = [
        TimestampedValue('a', 100),
        TimestampedValue('b', 200),
        TimestampedValue('c', 300)
    ]
    expected = [
        TestWindowedValue('a', 100, [GlobalWindow()]),
        TestWindowedValue('b', 200, [GlobalWindow()]),
        TestWindowedValue('c', 300, [GlobalWindow()])
    ]
    with TestPipeline() as p:
      # Map(lambda x: x) PTransform is added after Create here, because when
      # a PCollection of TimestampedValues is created with Create PTransform,
      # the timestamps are not assigned to it. Adding a Map forces the
      # PCollection to go through a DoFn so that the PCollection consists of
      # the elements with timestamps assigned to them instead of a PCollection
      # of TimestampedValue(element, timestamp).
      pc = p | beam.Create(l) | beam.Map(lambda x: x)
      reified_pc = pc | util.Reify.Timestamp()
      assert_that(reified_pc, equal_to(expected), reify_windows=True)

  def test_window(self):
    l = [
        GlobalWindows.windowed_value('a', 100),
        GlobalWindows.windowed_value('b', 200),
        GlobalWindows.windowed_value('c', 300)
    ]
    expected = [
        TestWindowedValue(('a', 100, GlobalWindow()), 100, [GlobalWindow()]),
        TestWindowedValue(('b', 200, GlobalWindow()), 200, [GlobalWindow()]),
        TestWindowedValue(('c', 300, GlobalWindow()), 300, [GlobalWindow()])
    ]
    with TestPipeline() as p:
      pc = p | beam.Create(l)
      # Map(lambda x: x) PTransform is added after Create here, because when
      # a PCollection of WindowedValues is created with Create PTransform,
      # the windows are not assigned to it. Adding a Map forces the
      # PCollection to go through a DoFn so that the PCollection consists of
      # the elements with timestamps assigned to them instead of a PCollection
      # of WindowedValue(element, timestamp, window).
      pc = pc | beam.Map(lambda x: x)
      reified_pc = pc | util.Reify.Window()
      assert_that(reified_pc, equal_to(expected), reify_windows=True)

  def test_timestamp_in_value(self):
    l = [
        TimestampedValue(('a', 1), 100),
        TimestampedValue(('b', 2), 200),
        TimestampedValue(('c', 3), 300)
    ]
    expected = [
        TestWindowedValue(('a', TimestampedValue(1, 100)),
                          100, [GlobalWindow()]),
        TestWindowedValue(('b', TimestampedValue(2, 200)),
                          200, [GlobalWindow()]),
        TestWindowedValue(('c', TimestampedValue(3, 300)),
                          300, [GlobalWindow()])
    ]
    with TestPipeline() as p:
      pc = p | beam.Create(l) | beam.Map(lambda x: x)
      reified_pc = pc | util.Reify.TimestampInValue()
      assert_that(reified_pc, equal_to(expected), reify_windows=True)

  def test_window_in_value(self):
    l = [
        GlobalWindows.windowed_value(('a', 1), 100),
        GlobalWindows.windowed_value(('b', 2), 200),
        GlobalWindows.windowed_value(('c', 3), 300)
    ]
    expected = [
        TestWindowedValue(('a', (1, 100, GlobalWindow())),
                          100, [GlobalWindow()]),
        TestWindowedValue(('b', (2, 200, GlobalWindow())),
                          200, [GlobalWindow()]),
        TestWindowedValue(('c', (3, 300, GlobalWindow())),
                          300, [GlobalWindow()])
    ]
    with TestPipeline() as p:
      # Map(lambda x: x) hack is used for the same reason here.
      # Also, this makes the typehint on Reify.WindowInValue work.
      pc = p | beam.Create(l) | beam.Map(lambda x: x)
      reified_pc = pc | util.Reify.WindowInValue()
      assert_that(reified_pc, equal_to(expected), reify_windows=True)


class RegexTest(unittest.TestCase):
  def test_find(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["aj", "xj", "yj", "zj"])
          | util.Regex.find("[xyz]"))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_find_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("[xyz]")
      result = (p | beam.Create(["aj", "xj", "yj", "zj"]) | util.Regex.find(rc))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_find_group(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["aj", "xj", "yj", "zj"])
          | util.Regex.find("([xyz])j", group=1))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_find_empty(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "b", "c", "d"])
          | util.Regex.find("[xyz]"))
      assert_that(result, equal_to([]))

  def test_find_group_name(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["aj", "xj", "yj", "zj"])
          | util.Regex.find("(?P<namedgroup>[xyz])j", group="namedgroup"))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_find_group_name_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("(?P<namedgroup>[xyz])j")
      result = (
          p | beam.Create(["aj", "xj", "yj", "zj"])
          | util.Regex.find(rc, group="namedgroup"))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_find_all_groups(self):
    data = ["abb ax abbb", "abc qwerty abcabcd xyz"]
    with TestPipeline() as p:
      pcol = (p | beam.Create(data))

      assert_that(
          pcol | 'with default values' >> util.Regex.find_all('a(b*)'),
          equal_to([['abb', 'a', 'abbb'], ['ab', 'ab', 'ab']]),
          label='CheckWithDefaultValues')

      assert_that(
          pcol | 'group 1' >> util.Regex.find_all('a(b*)', 1),
          equal_to([['b', 'b', 'b'], ['bb', '', 'bbb']]),
          label='CheckWithGroup1')

      assert_that(
          pcol | 'group 1 non empty' >> util.Regex.find_all(
              'a(b*)', 1, outputEmpty=False),
          equal_to([['b', 'b', 'b'], ['bb', 'bbb']]),
          label='CheckGroup1NonEmpty')

      assert_that(
          pcol | 'named group' >> util.Regex.find_all(
              'a(?P<namedgroup>b*)', 'namedgroup'),
          equal_to([['b', 'b', 'b'], ['bb', '', 'bbb']]),
          label='CheckNamedGroup')

      assert_that(
          pcol | 'all groups' >> util.Regex.find_all(
              'a(?P<namedgroup>b*)', util.Regex.ALL),
          equal_to([[('ab', 'b'), ('ab', 'b'), ('ab', 'b')],
                    [('abb', 'bb'), ('a', ''), ('abbb', 'bbb')]]),
          label='CheckAllGroups')

      assert_that(
          pcol | 'all non empty groups' >> util.Regex.find_all(
              'a(b*)', util.Regex.ALL, outputEmpty=False),
          equal_to([[('ab', 'b'), ('ab', 'b'), ('ab', 'b')],
                    [('abb', 'bb'), ('abbb', 'bbb')]]),
          label='CheckAllNonEmptyGroups')

  def test_find_kv(self):
    with TestPipeline() as p:
      pcol = (p | beam.Create(['a b c d']))
      assert_that(
          pcol | 'key 1' >> util.Regex.find_kv(
              'a (b) (c)',
              1,
          ),
          equal_to([('b', 'a b c')]),
          label='CheckKey1')

      assert_that(
          pcol | 'key 1 group 1' >> util.Regex.find_kv('a (b) (c)', 1, 2),
          equal_to([('b', 'c')]),
          label='CheckKey1Group1')

  def test_find_kv_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("a (b) (c)")
      result = (p | beam.Create(["a b c"]) | util.Regex.find_kv(rc, 1, 2))
      assert_that(result, equal_to([("b", "c")]))

  def test_find_kv_none(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["x y z"])
          | util.Regex.find_kv("a (b) (c)", 1, 2))
      assert_that(result, equal_to([]))

  def test_match(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "x", "y", "z"])
          | util.Regex.matches("[xyz]"))
      assert_that(result, equal_to(["x", "y", "z"]))

    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "ax", "yby", "zzc"])
          | util.Regex.matches("[xyz]"))
      assert_that(result, equal_to(["y", "z"]))

  def test_match_entire_line(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "x", "y", "ay", "zz"])
          | util.Regex.matches("[xyz]$"))
      assert_that(result, equal_to(["x", "y"]))

  def test_match_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("[xyz]")
      result = (p | beam.Create(["a", "x", "y", "z"]) | util.Regex.matches(rc))
      assert_that(result, equal_to(["x", "y", "z"]))

  def test_match_none(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "b", "c", "d"])
          | util.Regex.matches("[xyz]"))
      assert_that(result, equal_to([]))

  def test_match_group(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "x xxx", "x yyy", "x zzz"])
          | util.Regex.matches("x ([xyz]*)", 1))
      assert_that(result, equal_to(("xxx", "yyy", "zzz")))

  def test_match_group_name(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "x xxx", "x yyy", "x zzz"])
          | util.Regex.matches("x (?P<namedgroup>[xyz]*)", 'namedgroup'))
      assert_that(result, equal_to(("xxx", "yyy", "zzz")))

  def test_match_group_name_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("x (?P<namedgroup>[xyz]*)")
      result = (
          p | beam.Create(["a", "x xxx", "x yyy", "x zzz"])
          | util.Regex.matches(rc, 'namedgroup'))
      assert_that(result, equal_to(("xxx", "yyy", "zzz")))

  def test_match_group_empty(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a", "b", "c", "d"])
          | util.Regex.matches("x (?P<namedgroup>[xyz]*)", 'namedgroup'))
      assert_that(result, equal_to([]))

  def test_all_matched(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a x", "x x", "y y", "z z"])
          | util.Regex.all_matches("([xyz]) ([xyz])"))
      expected_result = [["x x", "x", "x"], ["y y", "y", "y"],
                         ["z z", "z", "z"]]
      assert_that(result, equal_to(expected_result))

  def test_all_matched_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("([xyz]) ([xyz])")
      result = (
          p | beam.Create(["a x", "x x", "y y", "z z"])
          | util.Regex.all_matches(rc))
      expected_result = [["x x", "x", "x"], ["y y", "y", "y"],
                         ["z z", "z", "z"]]
      assert_that(result, equal_to(expected_result))

  def test_match_group_kv(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a b c"])
          | util.Regex.matches_kv("a (b) (c)", 1, 2))
      assert_that(result, equal_to([("b", "c")]))

  def test_match_group_kv_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("a (b) (c)")
      pcol = (p | beam.Create(["a b c"]))
      assert_that(
          pcol | 'key 1' >> util.Regex.matches_kv(rc, 1),
          equal_to([("b", "a b c")]),
          label="CheckKey1")

      assert_that(
          pcol | 'key 1 group 2' >> util.Regex.matches_kv(rc, 1, 2),
          equal_to([("b", "c")]),
          label="CheckKey1Group2")

  def test_match_group_kv_none(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["x y z"])
          | util.Regex.matches_kv("a (b) (c)", 1, 2))
      assert_that(result, equal_to([]))

  def test_match_kv_group_names(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["a b c"]) | util.Regex.matches_kv(
              "a (?P<keyname>b) (?P<valuename>c)", 'keyname', 'valuename'))
      assert_that(result, equal_to([("b", "c")]))

  def test_match_kv_group_names_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("a (?P<keyname>b) (?P<valuename>c)")
      result = (
          p | beam.Create(["a b c"])
          | util.Regex.matches_kv(rc, 'keyname', 'valuename'))
      assert_that(result, equal_to([("b", "c")]))

  def test_match_kv_group_name_none(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["x y z"]) | util.Regex.matches_kv(
              "a (?P<keyname>b) (?P<valuename>c)", 'keyname', 'valuename'))
      assert_that(result, equal_to([]))

  def test_replace_all(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["xj", "yj", "zj"])
          | util.Regex.replace_all("[xyz]", "new"))
      assert_that(result, equal_to(["newj", "newj", "newj"]))

  def test_replace_all_mixed(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["abc", "xj", "yj", "zj", "def"])
          | util.Regex.replace_all("[xyz]", 'new'))
      assert_that(result, equal_to(["abc", "newj", "newj", "newj", "def"]))

  def test_replace_all_mixed_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("[xyz]")
      result = (
          p | beam.Create(["abc", "xj", "yj", "zj", "def"])
          | util.Regex.replace_all(rc, 'new'))
      assert_that(result, equal_to(["abc", "newj", "newj", "newj", "def"]))

  def test_replace_first(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["xjx", "yjy", "zjz"])
          | util.Regex.replace_first("[xyz]", 'new'))
      assert_that(result, equal_to(["newjx", "newjy", "newjz"]))

  def test_replace_first_mixed(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(["abc", "xjx", "yjy", "zjz", "def"])
          | util.Regex.replace_first("[xyz]", 'new'))
      assert_that(result, equal_to(["abc", "newjx", "newjy", "newjz", "def"]))

  def test_replace_first_mixed_pattern(self):
    with TestPipeline() as p:
      rc = re.compile("[xyz]")
      result = (
          p | beam.Create(["abc", "xjx", "yjy", "zjz", "def"])
          | util.Regex.replace_first(rc, 'new'))
      assert_that(result, equal_to(["abc", "newjx", "newjy", "newjz", "def"]))

  def test_split(self):
    with TestPipeline() as p:
      data = ["The  quick   brown fox jumps over    the lazy dog"]
      result = (p | beam.Create(data) | util.Regex.split("\\W+"))
      expected_result = [[
          "The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"
      ]]
      assert_that(result, equal_to(expected_result))

  def test_split_pattern(self):
    with TestPipeline() as p:
      data = ["The  quick   brown fox jumps over    the lazy dog"]
      rc = re.compile("\\W+")
      result = (p | beam.Create(data) | util.Regex.split(rc))
      expected_result = [[
          "The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"
      ]]
      assert_that(result, equal_to(expected_result))

  def test_split_with_empty(self):
    with TestPipeline() as p:
      data = ["The  quick   brown fox jumps over    the lazy dog"]
      result = (p | beam.Create(data) | util.Regex.split("\\s", True))
      expected_result = [[
          'The',
          '',
          'quick',
          '',
          '',
          'brown',
          'fox',
          'jumps',
          'over',
          '',
          '',
          '',
          'the',
          'lazy',
          'dog'
      ]]
      assert_that(result, equal_to(expected_result))

  def test_split_without_empty(self):
    with TestPipeline() as p:
      data = ["The  quick   brown fox jumps over    the lazy dog"]
      result = (p | beam.Create(data) | util.Regex.split("\\s", False))
      expected_result = [[
          "The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"
      ]]
      assert_that(result, equal_to(expected_result))


class TeeTest(unittest.TestCase):
  _side_effects: Mapping[str, int] = collections.defaultdict(int)

  def test_tee(self):
    # The imports here are to avoid issues with the class (and its attributes)
    # possibly being pickled rather than referenced.
    def cause_side_effect(element):
      importlib.import_module(__name__).TeeTest._side_effects[element] += 1

    def count_side_effects(element):
      return importlib.import_module(__name__).TeeTest._side_effects[element]

    with TestPipeline() as p:
      result = (
          p
          | beam.Create(['a', 'b', 'c'])
          | 'TeePTransform' >> beam.Tee(beam.Map(cause_side_effect))
          | 'TeeCallable' >> beam.Tee(
              lambda pcoll: pcoll | beam.Map(
                  lambda element: cause_side_effect('X' + element))))
      assert_that(result, equal_to(['a', 'b', 'c']))

    self.assertEqual(count_side_effects('a'), 1)
    self.assertEqual(count_side_effects('Xa'), 1)


class WaitOnTest(unittest.TestCase):
  def test_find(self):
    # We need shared reference that survives pickling.
    def increment_global_counter():
      try:
        value = getattr(beam, '_WAIT_ON_TEST_COUNTER', 0)
        return value
      finally:
        setattr(beam, '_WAIT_ON_TEST_COUNTER', value + 1)

    def record(tag):
      return f'Record({tag})' >> beam.Map(
          lambda x: (x[0], tag, increment_global_counter()))

    with TestPipeline() as p:
      start = p | beam.Create([(None, ), (None, )])
      x = start | record('x')
      y = start | 'WaitForX' >> util.WaitOn(x) | record('y')
      z = start | 'WaitForY' >> util.WaitOn(y) | record('z')
      result = x | 'WaitForYZ' >> util.WaitOn(y, z) | record('result')
      assert_that(x, equal_to([(None, 'x', 0), (None, 'x', 1)]), label='x')
      assert_that(y, equal_to([(None, 'y', 2), (None, 'y', 3)]), label='y')
      assert_that(z, equal_to([(None, 'z', 4), (None, 'z', 5)]), label='z')
      assert_that(
          result,
          equal_to([(None, 'result', 6), (None, 'result', 7)]),
          label='result')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
