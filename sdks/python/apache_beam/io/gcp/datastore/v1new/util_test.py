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

"""Tests for util.py."""
# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.io.gcp.datastore.v1new import util


class MovingSumTest(unittest.TestCase):

  TIMESTAMP = 1500000000

  def test_bad_bucket_size(self):
    with self.assertRaises(ValueError):
      _ = util.MovingSum(1, 0)

  def test_bad_window_size(self):
    with self.assertRaises(ValueError):
      _ = util.MovingSum(1, 2)

  def test_no_data(self):
    ms = util.MovingSum(10, 1)
    self.assertEqual(0, ms.sum(MovingSumTest.TIMESTAMP))
    self.assertEqual(0, ms.count(MovingSumTest.TIMESTAMP))
    self.assertFalse(ms.has_data(MovingSumTest.TIMESTAMP))

  def test_one_data_point(self):
    ms = util.MovingSum(10, 1)
    ms.add(MovingSumTest.TIMESTAMP, 5)
    self.assertEqual(5, ms.sum(MovingSumTest.TIMESTAMP))
    self.assertEqual(1, ms.count(MovingSumTest.TIMESTAMP))
    self.assertTrue(ms.has_data(MovingSumTest.TIMESTAMP))

  def test_aggregates_within_window(self):
    ms = util.MovingSum(10, 1)
    ms.add(MovingSumTest.TIMESTAMP, 5)
    ms.add(MovingSumTest.TIMESTAMP + 1, 3)
    ms.add(MovingSumTest.TIMESTAMP + 2, 7)
    self.assertEqual(15, ms.sum(MovingSumTest.TIMESTAMP + 3))
    self.assertEqual(3, ms.count(MovingSumTest.TIMESTAMP + 3))

  def test_data_expires_from_moving_window(self):
    ms = util.MovingSum(5, 1)
    ms.add(MovingSumTest.TIMESTAMP, 5)
    ms.add(MovingSumTest.TIMESTAMP + 3, 3)
    ms.add(MovingSumTest.TIMESTAMP + 6, 7)
    self.assertEqual(10, ms.sum(MovingSumTest.TIMESTAMP + 7))
    self.assertEqual(2, ms.count(MovingSumTest.TIMESTAMP + 7))


class DynamicWriteBatcherTest(unittest.TestCase):
  def setUp(self):
    self._batcher = util.DynamicBatchSizer()

  # If possible, keep these test cases aligned with the Java test cases in
  # DatastoreV1Test.java
  def test_no_data(self):
    self.assertEqual(
        util.WRITE_BATCH_INITIAL_SIZE, self._batcher.get_batch_size(0))

  def test_fast_queries(self):
    self._batcher.report_latency(0, 1000, 200)
    self._batcher.report_latency(0, 1000, 200)
    self.assertEqual(util.WRITE_BATCH_MAX_SIZE, self._batcher.get_batch_size(0))

  def test_slow_queries(self):
    self._batcher.report_latency(0, 10000, 200)
    self._batcher.report_latency(0, 10000, 200)
    self.assertEqual(100, self._batcher.get_batch_size(0))

  def test_size_not_below_minimum(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(0, 30000, 50)
    self.assertEqual(util.WRITE_BATCH_MIN_SIZE, self._batcher.get_batch_size(0))

  def test_sliding_window(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(50000, 5000, 200)
    self._batcher.report_latency(100000, 5000, 200)
    self.assertEqual(200, self._batcher.get_batch_size(150000))


if __name__ == '__main__':
  unittest.main()
