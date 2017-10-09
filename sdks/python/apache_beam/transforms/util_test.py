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

import time
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import util
from apache_beam.transforms import window


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
