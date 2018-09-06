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

from __future__ import absolute_import

import threading
import unittest
from builtins import range

from apache_beam.metrics.cells import CellCommitState
from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.cells import GaugeData


class TestCounterCell(unittest.TestCase):
  @classmethod
  def _modify_counter(cls, d):
    for i in range(cls.NUM_ITERATIONS):
      d.inc(i)

  NUM_THREADS = 5
  NUM_ITERATIONS = 100

  def test_parallel_access(self):
    # We create NUM_THREADS threads that concurrently modify the counter.
    threads = []
    c = CounterCell()
    for _ in range(TestCounterCell.NUM_THREADS):
      t = threading.Thread(target=TestCounterCell._modify_counter,
                           args=(c,))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (self.NUM_ITERATIONS
             * (self.NUM_ITERATIONS - 1) // 2
             * self.NUM_THREADS)
    self.assertEqual(c.get_cumulative(), total)

  def test_basic_operations(self):
    c = CounterCell()
    c.inc(2)
    self.assertEqual(c.get_cumulative(), 2)

    c.dec(10)
    self.assertEqual(c.get_cumulative(), -8)

    c.dec()
    self.assertEqual(c.get_cumulative(), -9)

    c.inc()
    self.assertEqual(c.get_cumulative(), -8)


class TestDistributionCell(unittest.TestCase):
  @classmethod
  def _modify_distribution(cls, d):
    for i in range(cls.NUM_ITERATIONS):
      d.update(i)

  NUM_THREADS = 5
  NUM_ITERATIONS = 100

  def test_parallel_access(self):
    # We create NUM_THREADS threads that concurrently modify the distribution.
    threads = []
    d = DistributionCell()
    for _ in range(TestDistributionCell.NUM_THREADS):
      t = threading.Thread(target=TestDistributionCell._modify_distribution,
                           args=(d,))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (self.NUM_ITERATIONS
             * (self.NUM_ITERATIONS - 1) // 2
             * self.NUM_THREADS)

    count = (self.NUM_ITERATIONS * self.NUM_THREADS)

    self.assertEqual(d.get_cumulative(),
                     DistributionData(total, count, 0,
                                      self.NUM_ITERATIONS - 1))

  def test_basic_operations(self):
    d = DistributionCell()
    d.update(10)
    self.assertEqual(d.get_cumulative(),
                     DistributionData(10, 1, 10, 10))

    d.update(2)
    self.assertEqual(d.get_cumulative(),
                     DistributionData(12, 2, 2, 10))

    d.update(900)
    self.assertEqual(d.get_cumulative(),
                     DistributionData(912, 3, 2, 900))

  def test_integer_only(self):
    d = DistributionCell()
    d.update(3.1)
    d.update(3.2)
    d.update(3.3)
    self.assertEqual(d.get_cumulative(),
                     DistributionData(9, 3, 3, 3))


class TestGaugeCell(unittest.TestCase):
  def test_basic_operations(self):
    g = GaugeCell()
    g.set(10)
    self.assertEqual(g.get_cumulative().value, GaugeData(10).value)

    g.set(2)
    self.assertEqual(g.get_cumulative().value, 2)

  def test_integer_only(self):
    g = GaugeCell()
    g.set(3.3)
    self.assertEqual(g.get_cumulative().value, 3)

  def test_combine_appropriately(self):
    g1 = GaugeCell()
    g1.set(3)

    g2 = GaugeCell()
    g2.set(1)

    # THe second Gauge, with value 1 was the most recent, so it should be
    # the final result.
    result = g2.combine(g1)
    self.assertEqual(result.data.value, 1)


class TestCellCommitState(unittest.TestCase):
  def test_basic_path(self):
    ds = CellCommitState()
    # Starts dirty
    self.assertTrue(ds.before_commit())
    ds.after_commit()
    self.assertFalse(ds.before_commit())

    # Make it dirty again
    ds.after_modification()
    self.assertTrue(ds.before_commit())
    ds.after_commit()
    self.assertFalse(ds.before_commit())

    # Dirty again
    ds.after_modification()
    self.assertTrue(ds.before_commit())
    ds.after_modification()
    ds.after_commit()
    self.assertTrue(ds.before_commit())


if __name__ == '__main__':
  unittest.main()
