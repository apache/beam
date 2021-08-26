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

"""Unit tests for testing utilities."""

# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam import Create
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.testing.util import is_empty
from apache_beam.testing.util import is_not_empty
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP


class UtilTest(unittest.TestCase):
  def test_assert_that_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))

  def test_assert_that_passes_order_does_not_matter(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([2, 1, 3]))

  def test_assert_that_passes_order_does_not_matter_with_negatives(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, -2, 3]), equal_to([-2, 1, 3]))

  def test_assert_that_passes_empty_equal_to(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), equal_to([]))

  def test_assert_that_passes_empty_is_empty(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), is_empty())

  def test_assert_that_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), equal_to([1, 2, 3]))

  def test_assert_missing(self):
    with self.assertRaisesRegex(BeamAssertException,
                                r"missing elements \['c'\]"):
      with TestPipeline() as p:
        assert_that(p | Create(['a', 'b']), equal_to(['a', 'b', 'c']))

  def test_assert_unexpected(self):
    with self.assertRaisesRegex(BeamAssertException,
                                r"unexpected elements \['c', 'd'\]|"
                                r"unexpected elements \['d', 'c'\]"):
      with TestPipeline() as p:
        assert_that(p | Create(['a', 'b', 'c', 'd']), equal_to(['a', 'b']))

  def test_assert_missing_and_unexpected(self):
    with self.assertRaisesRegex(
        BeamAssertException,
        r"unexpected elements \['c'\].*missing elements \['d'\]"):
      with TestPipeline() as p:
        assert_that(p | Create(['a', 'b', 'c']), equal_to(['a', 'b', 'd']))

  def test_assert_with_custom_comparator(self):
    with TestPipeline() as p:
      assert_that(
          p | Create([1, 2, 3]),
          equal_to(['1', '2', '3'], equals_fn=lambda e, a: int(e) == int(a)))

  def test_reified_value_passes(self):
    expected = [
        TestWindowedValue(v, MIN_TIMESTAMP, [GlobalWindow()])
        for v in [1, 2, 3]
    ]
    with TestPipeline() as p:
      assert_that(p | Create([2, 3, 1]), equal_to(expected), reify_windows=True)

  def test_reified_value_assert_fail_unmatched_value(self):
    expected = [
        TestWindowedValue(v + 1, MIN_TIMESTAMP, [GlobalWindow()])
        for v in [1, 2, 3]
    ]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(
            p | Create([2, 3, 1]), equal_to(expected), reify_windows=True)

  def test_reified_value_assert_fail_unmatched_timestamp(self):
    expected = [TestWindowedValue(v, 1, [GlobalWindow()]) for v in [1, 2, 3]]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(
            p | Create([2, 3, 1]), equal_to(expected), reify_windows=True)

  def test_reified_value_assert_fail_unmatched_window(self):
    expected = [
        TestWindowedValue(v, MIN_TIMESTAMP, [IntervalWindow(0, 1)])
        for v in [1, 2, 3]
    ]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(
            p | Create([2, 3, 1]), equal_to(expected), reify_windows=True)

  def test_assert_that_fails_on_empty_input(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([]), equal_to([1, 2, 3]))

  def test_assert_that_fails_on_empty_expected(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 2, 3]), is_empty())

  def test_assert_that_passes_is_not_empty(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), is_not_empty())

  def test_assert_that_fails_on_is_not_empty_expected(self):
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([]), is_not_empty())

  def test_equal_to_per_window_passes(self):
    start = int(MIN_TIMESTAMP.micros // 1e6) - 5
    end = start + 20
    expected = {
        window.IntervalWindow(start, end): [('k', [1])],
    }
    with TestPipeline(options=StandardOptions(streaming=True)) as p:
      assert_that((
          p
          | Create([1])
          | beam.WindowInto(
              FixedWindows(20),
              trigger=trigger.AfterWatermark(),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.Map(lambda x: ('k', x))
          | beam.GroupByKey()),
                  equal_to_per_window(expected),
                  reify_windows=True)

  def test_equal_to_per_window_fail_unmatched_window(self):
    with self.assertRaises(BeamAssertException):
      expected = {
          window.IntervalWindow(50, 100): [('k', [1])],
      }
      with TestPipeline(options=StandardOptions(streaming=True)) as p:
        assert_that((
            p
            | Create([1])
            | beam.WindowInto(
                FixedWindows(20),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | beam.Map(lambda x: ('k', x))
            | beam.GroupByKey()),
                    equal_to_per_window(expected),
                    reify_windows=True)

  def test_equal_to_per_window_fail_unmatched_element(self):
    with self.assertRaises(BeamAssertException):
      start = int(MIN_TIMESTAMP.micros // 1e6) - 5
      end = start + 20
      expected = {
          window.IntervalWindow(start, end): [('k', [1]), ('k', [2])],
      }
      with TestPipeline(options=StandardOptions(streaming=True)) as p:
        assert_that((
            p
            | Create([1])
            | beam.WindowInto(
                FixedWindows(20),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | beam.Map(lambda x: ('k', x))
            | beam.GroupByKey()),
                    equal_to_per_window(expected),
                    reify_windows=True)

  def test_equal_to_per_window_succeeds_no_reify_windows(self):
    start = int(MIN_TIMESTAMP.micros // 1e6) - 5
    end = start + 20
    expected = {
        window.IntervalWindow(start, end): [('k', [1])],
    }
    with TestPipeline(options=StandardOptions(streaming=True)) as p:
      assert_that((
          p
          | Create([1])
          | beam.WindowInto(
              FixedWindows(20),
              trigger=trigger.AfterWatermark(),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.Map(lambda x: ('k', x))
          | beam.GroupByKey()),
                  equal_to_per_window(expected))

  def test_equal_to_per_window_fail_unexpected_element(self):
    with self.assertRaises(BeamAssertException):
      start = int(MIN_TIMESTAMP.micros // 1e6) - 5
      end = start + 20
      expected = {
          window.IntervalWindow(start, end): [('k', [1])],
      }
      with TestPipeline(options=StandardOptions(streaming=True)) as p:
        assert_that((
            p
            | Create([1, 2])
            | beam.WindowInto(
                FixedWindows(20),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | beam.Map(lambda x: ('k', x))
            | beam.GroupByKey()),
                    equal_to_per_window(expected),
                    reify_windows=True)


if __name__ == '__main__':
  unittest.main()
