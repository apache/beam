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

# pytype: skip-file

import threading
import unittest

from apache_beam.internal.metrics.cells import HistogramCell
from apache_beam.internal.metrics.cells import HistogramCellFactory
from apache_beam.internal.metrics.cells import HistogramData
from apache_beam.utils.histogram import Histogram
from apache_beam.utils.histogram import LinearBucket


class TestHistogramCell(unittest.TestCase):
  @classmethod
  def _modify_histogram(cls, d):
    for i in range(cls.NUM_ITERATIONS):
      d.update(i)

  NUM_THREADS = 5
  NUM_ITERATIONS = 100

  def test_parallel_access(self):
    # We create NUM_THREADS threads that concurrently modify the distribution.
    threads = []
    bucket_type = LinearBucket(0, 1, 100)
    d = HistogramCell(bucket_type)
    for _ in range(TestHistogramCell.NUM_THREADS):
      t = threading.Thread(
          target=TestHistogramCell._modify_histogram, args=(d, ))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    histogram = Histogram(bucket_type)
    for _ in range(self.NUM_THREADS):
      for i in range(self.NUM_ITERATIONS):
        histogram.record(i)

    self.assertEqual(d.get_cumulative(), HistogramData(histogram))

  def test_basic_operations(self):
    d = HistogramCellFactory(LinearBucket(0, 1, 10))()
    d.update(10)
    self.assertEqual(
        str(d.get_cumulative()),
        'HistogramData(Total count: 1, P99: >=10, P90: >=10, P50: >=10)')
    d.update(0)
    self.assertEqual(
        str(d.get_cumulative()),
        'HistogramData(Total count: 2, P99: >=10, P90: >=10, P50: 1)')
    d.update(5)
    self.assertEqual(
        str(d.get_cumulative()),
        'HistogramData(Total count: 3, P99: >=10, P90: >=10, P50: 6)')


if __name__ == '__main__':
  unittest.main()
