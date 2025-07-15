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

"""Unit tests for the PTransform and descendants."""

# pytype: skip-file

import inspect
import logging
import random
import time
import unittest

from parameterized import parameterized

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.periodicsequence import PeriodicSequence
from apache_beam.transforms.periodicsequence import RebaseMode
from apache_beam.transforms.periodicsequence import _sequence_backlog_bytes
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned


class PeriodicSequenceTest(unittest.TestCase):
  def test_periodicsequence_outputs_valid_sequence(self):
    start_offset = 1
    start_time = time.time() + start_offset
    duration = 1
    end_time = start_time + duration
    interval = 0.25

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(start_time, end_time, interval)])
          | 'ImpulseSeqGen' >> PeriodicSequence())

      k = [
          start_time + x * interval
          for x in range(0, int(duration / interval), 1)
      ]
      self.assertEqual(result.is_bounded, False)
      assert_that(result, equal_to(k))

  def test_periodicsequence_outputs_valid_sequence_in_past(self):
    start_offset = -10000
    it = time.time() + start_offset
    duration = 5
    et = it + duration
    interval = 1

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(it, et, interval)])
          | 'ImpulseSeqGen' >> PeriodicSequence())

      k = [it + x * interval for x in range(0, int(duration / interval), 1)]
      self.assertEqual(result.is_bounded, False)
      assert_that(result, equal_to(k))

  def test_periodicsequence_output_size(self):
    element = [0, 1000000000, 10]
    self.assertEqual(
        _sequence_backlog_bytes(element, 100, OffsetRange(10, 100000000)), 0)
    self.assertEqual(
        _sequence_backlog_bytes(element, 100, OffsetRange(9, 100000000)), 8)
    self.assertEqual(
        _sequence_backlog_bytes(element, 100, OffsetRange(8, 100000000)), 16)
    self.assertEqual(
        _sequence_backlog_bytes(element, 101, OffsetRange(9, 100000000)), 8)
    self.assertEqual(
        _sequence_backlog_bytes(element, 10000, OffsetRange(0, 100000000)),
        8 * 10000 / 10)
    self.assertEqual(
        _sequence_backlog_bytes(element, 10000, OffsetRange(1002, 1003)), 0)
    self.assertEqual(
        _sequence_backlog_bytes(element, 10100, OffsetRange(1002, 1003)), 8)


class PeriodicImpulseTest(unittest.TestCase):
  def test_windowing_on_si(self):
    start_offset = -15
    it = time.time() + start_offset
    duration = 15
    et = it + duration
    interval = 5

    with TestPipeline() as p:
      si = (
          p
          | 'PeriodicImpulse' >> PeriodicImpulse(it, et, interval, True)
          | 'AddKey' >> beam.Map(lambda v: ('key', v))
          | 'GBK' >> beam.GroupByKey()
          | 'SortGBK' >> beam.MapTuple(lambda k, vs: (k, sorted(vs))))

      actual = si
      k = [('key', [it + x * interval])
           for x in range(0, int(duration / interval), 1)]
      assert_that(actual, equal_to(k))

  def test_default_start(self):
    default_parameters = inspect.signature(PeriodicImpulse.__init__).parameters
    it = default_parameters["start_timestamp"].default
    duration = 1
    et = it + duration
    interval = 0.5

    # Check default `stop_timestamp` is the same type `start_timestamp`
    is_same_type = isinstance(
        it, type(default_parameters["stop_timestamp"].default))
    error = "'start_timestamp' and 'stop_timestamp' have different type"
    assert is_same_type, error

    with TestPipeline() as p:
      result = p | 'PeriodicImpulse' >> PeriodicImpulse(it, et, interval)

      k = [it + x * interval for x in range(0, int(duration / interval))]
      self.assertEqual(result.is_bounded, False)
      assert_that(result, equal_to(k))

  @unittest.skip("hard to determine warm-up time and threshold for runners.")
  def test_processing_time(self):
    warmup_time = 3
    threshold = 0.5
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp.now() + warmup_time,
              data=[10, 20, 30],
              fire_interval=2)
          | beam.Map(lambda _: time.time())
          | beam.WindowInto(
              window.GlobalWindows(),
              trigger=trigger.Repeatedly(trigger.AfterCount(3)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING,
          )
          | beam.GroupBy()
          | beam.FlatMap(lambda x: [v - min(x[1]) for v in x[1]]))
      expected = [0, 2, 4]
      assert_that(ret, equal_to(expected, lambda x, y: abs(x - y) < threshold))

  @parameterized.expand([0.5, 1, 2, 10])
  def test_stop_over_by_epsilon(self, interval):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp(seconds=1),
              stop_timestamp=Timestamp(seconds=1, micros=1),
              data=[1, 2],
              fire_interval=interval)
          | beam.WindowInto(FixedWindows(interval))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [
          (0, [1]),
      ]
      assert_that(ret, equal_to(expected))

  @parameterized.expand([1, 2])
  def test_stop_over_by_interval(self, interval):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp(seconds=1),
              stop_timestamp=Timestamp(seconds=1 + interval),
              data=[1, 2],
              fire_interval=interval)
          | beam.WindowInto(FixedWindows(interval))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1])]
      assert_that(ret, equal_to(expected))

  @parameterized.expand([1, 2])
  def test_stop_over_by_interval_and_epsilon(self, interval):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp(seconds=1),
              stop_timestamp=Timestamp(seconds=1 + interval, micros=1),
              data=[1, 2],
              fire_interval=interval)
          | beam.WindowInto(FixedWindows(interval))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1]), (0, [2])]
      assert_that(ret, equal_to(expected))

  def test_interval(self):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(data=[1, 2, 3, 4], fire_interval=0.5)
          | beam.WindowInto(FixedWindows(0.5))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1]), (0, [2]), (0, [3]), (0, [4])]
      assert_that(ret, equal_to(expected))

  def test_repeat(self):
    now = Timestamp.now()
    with self.assertWarnsRegex(UserWarning, "not enough to span"):
      with TestPipeline() as p:
        ret = (
            p | PeriodicImpulse(
                start_timestamp=now,
                stop_timestamp=now + 2.6,
                data=[1, 2, 3, 4],
                fire_interval=0.5)
            | beam.WindowInto(FixedWindows(0.5))
            | beam.WithKeys(0)
            | beam.GroupByKey())
        expected = [(0, [1]), (0, [2]), (0, [3]), (0, [4]), (0, [1]), (0, [2])]
        assert_that(ret, equal_to(expected))

  def test_timestamped_value(self):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              data=[(Timestamp(1), 1), (Timestamp(3), 2), (Timestamp(2), 3),
                    (Timestamp(1), 4)],
              fire_interval=0.5)
          | beam.WindowInto(FixedWindows(0.5))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1, 4]), (0, [2]), (0, [3])]
      assert_that(ret, equal_to(expected))

  def test_not_enough_timestamped_value(self):
    now = Timestamp.now()
    data = [(Timestamp(1), 1), (Timestamp(2), 2), (Timestamp(3), 3)]
    with self.assertRaisesRegex(ValueError, "not enough to span"):
      with TestPipeline() as p:
        _ = (
            p | PeriodicImpulse(
                start_timestamp=now,
                stop_timestamp=now + 2.6,
                data=data,
                fire_interval=0.5))

  def test_fuzzy_length_and_interval(self):
    times = 30
    for _ in range(times):
      seed = int(time.time() * 1000)
      random.seed(seed)
      n = int(random.randint(1, 100))
      data = list(range(n))
      m = random.randint(1, 1000)
      interval = m / 1e6
      now = Timestamp.now()
      try:
        with TestPipeline() as p:
          ret = (
              p | PeriodicImpulse(
                  start_timestamp=now, data=data, fire_interval=interval))
          assert_that(ret, equal_to(data))
      except Exception as e:  # pylint: disable=broad-except
        logging.error("Error occurred at random seed=%d", seed)
        raise e

  def test_fuzzy_length_at_minimal_interval(self):
    times = 30
    for _ in range(times):
      seed = int(time.time() * 1000)
      random.seed(seed)
      n = int(random.randint(1, 100))
      data = list(range(n))
      interval = 1e-6
      now = Timestamp.now()
      try:
        with TestPipeline() as p:
          ret = (
              p | PeriodicImpulse(
                  start_timestamp=now, data=data, fire_interval=interval))
          assert_that(ret, equal_to(data))
      except Exception as e:  # pylint: disable=broad-except
        logging.error("Error occurred at random seed=%d", seed)
        raise e

  def test_int_type_input(self):
    # This test is to verify that if input timestamps and interval are integers,
    # the generated timestamped values are also integers.
    # This is necessary for the following test to pass:
    # apache_beam.examples.snippets.snippets_test.SlowlyChangingSideInputsTest
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=1, stop_timestamp=5, fire_interval=1))
      expected = [1, 2, 3, 4]
      assert_that(
          ret, equal_to(expected, lambda x, y: type(x) is type(y) and x == y))

  def test_float_type_input(self):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=1.0, stop_timestamp=5.0, fire_interval=1))
      expected = [1.0, 2.0, 3.0, 4.0]
      assert_that(
          ret, equal_to(expected, lambda x, y: type(x) is type(y) and x == y))

  def test_timestamp_type_input(self):
    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp.of(1),
              stop_timestamp=Timestamp.of(5),
              fire_interval=1))
      expected = [1.0, 2.0, 3.0, 4.0]
      assert_that(
          ret, equal_to(expected, lambda x, y: type(x) is type(y) and x == y))

  def test_rebase_timestamp(self):
    class CheckTimeStamp(beam.DoFn):
      def process(self, elem):
        ts = Timestamp.of(elem)
        now = Timestamp.now()
        # When rebase is enabled, the timestamp should be closer to now than the
        # original start.
        if (ts - Timestamp.of(1)) < (now - ts):
          yield "wrong"

    with TestPipeline() as p:
      ret = (
          p | PeriodicImpulse(
              start_timestamp=Timestamp.of(1),
              stop_timestamp=Timestamp.of(5),
              fire_interval=1,
              rebase=RebaseMode.REBASE_ALL)
          | beam.ParDo(CheckTimeStamp()))
      assert_that(ret, is_empty())

  def test_rebase_timestamp_with_wrong_setting(self):
    with self.assertRaises(Exception):
      # exception is raised because start_timestamp is rebased to the pipeline
      # execution time, but the stop_timestamp is 5 seconds after unix epoch.
      with TestPipeline() as p:
        _ = (
            p | PeriodicImpulse(
                start_timestamp=Timestamp.of(1),
                stop_timestamp=Timestamp.of(5),
                fire_interval=1,
                rebase=RebaseMode.REBASE_START))


if __name__ == '__main__':
  unittest.main()
