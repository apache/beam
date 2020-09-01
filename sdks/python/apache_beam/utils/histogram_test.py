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

"""Unit tests for Histogram."""

from __future__ import absolute_import
from __future__ import division

import unittest

from apache_beam.utils.histogram import Histogram
from apache_beam.utils.histogram import LinearBucket


class HistogramTest(unittest.TestCase):
  def test_out_of_range(self):
    histogram = Histogram(LinearBucket(0, 20, 5))
    with self.assertLogs('apache_beam.utils.histogram', level='WARNING') as cm:
      histogram.record(100)
    self.assertEqual(
        cm.output,
        [
            'WARNING:apache_beam.utils.histogram:' +
            'record is out of upper bound 100: 100'
        ])
    self.assertEqual(histogram.total_count(), 1)

  def test_boundary_buckets(self):
    histogram = Histogram(LinearBucket(0, 20, 5))
    histogram.record(0)
    histogram.record(99.9)
    self.assertEqual(histogram._buckets[0], 1)
    self.assertEqual(histogram._buckets[4], 1)

  def test_fractional_buckets(self):
    histogram1 = Histogram(LinearBucket(0, 10 / 3, 3))
    histogram1.record(3.33)
    histogram1.record(6.66)
    self.assertEqual(histogram1._buckets[0], 1)
    self.assertEqual(histogram1._buckets[1], 1)

    histogram2 = Histogram(LinearBucket(0, 10 / 3, 3))
    histogram2.record(3.34)
    histogram2.record(6.67)
    self.assertEqual(histogram2._buckets[1], 1)
    self.assertEqual(histogram2._buckets[2], 1)

  def test_p50(self):
    histogram1 = Histogram(LinearBucket(0, 0.2, 50))
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    self.assertEqual(histogram1.p50(), 4.2)

    histogram2 = Histogram(LinearBucket(0, 0.02, 50))
    histogram2.record(0, 0, 0)
    self.assertEqual(histogram2.p50(), 0.01)

  def test_p90(self):
    histogram1 = Histogram(LinearBucket(0, 0.2, 50))
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    self.assertEqual(histogram1.p90(), 8.2)

    histogram2 = Histogram(LinearBucket(0, 0.02, 50))
    histogram2.record(0, 0, 0)
    self.assertAlmostEqual(histogram2.p90(), 0.018, places=3)

  def test_p99(self):
    histogram1 = Histogram(LinearBucket(0, 0.2, 50))
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    self.assertEqual(histogram1.p99(), 9.18)

    histogram2 = Histogram(LinearBucket(0, 0.02, 50))
    histogram2.record(0, 0, 0)
    self.assertAlmostEqual(histogram2.p99(), 0.02, places=3)

  def test_p90_negative(self):
    histogram1 = Histogram(LinearBucket(-10, 0.2, 50))
    histogram1.record(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10)
    self.assertEqual(histogram1.p90(), -1.8)

    histogram2 = Histogram(LinearBucket(-1, 0.02, 50))
    histogram2.record(-1, -1, -1)
    self.assertEqual(histogram2.p90(), -0.982)

  def test_p90_negative_to_positive(self):
    histogram1 = Histogram(LinearBucket(-5, 0.2, 50))
    histogram1.record(-1, -2, -3, -4, -5, 0, 1, 2, 3, 4)
    self.assertEqual(histogram1.p90(), 3.2)

    histogram2 = Histogram(LinearBucket(-0.5, 0.02, 50))
    histogram2.record(-0.5, -0.5, -0.5)
    self.assertEqual(histogram2.p90(), -0.482)

  def test_p50_negative_infinity(self):
    histogram = Histogram(LinearBucket(0, 0.2, 50))
    histogram.record(-1, -2, -3, -4, -5, 0, 1, 2, 3, 4)
    self.assertEqual(histogram.p50(), float('-inf'))

  def test_p50_positive_infinity(self):
    histogram = Histogram(LinearBucket(0, 0.2, 50))
    histogram.record(6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
    self.assertEqual(histogram.p50(), float('inf'))

  def test_empty_p99(self):
    histogram = Histogram(LinearBucket(0, 0.2, 50))
    with self.assertRaises(RuntimeError) as cm:
      histogram.p99()
    self.assertEqual(str(cm.exception), 'histogram has no record.')

  def test_clear(self):
    histogram = Histogram(LinearBucket(0, 0.2, 50))
    histogram.record(-1, 1, 2, 3)
    self.assertEqual(histogram.total_count(), 4)
    self.assertEqual(histogram._buckets[5], 1)
    histogram.clear()
    self.assertEqual(histogram.total_count(), 0)
    self.assertEqual(histogram._buckets.get(5, 0), 0)


if __name__ == '__main__':
  unittest.main()
