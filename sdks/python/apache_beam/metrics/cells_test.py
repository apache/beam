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

from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.cells import GaugeData
from apache_beam.metrics.cells import StringSetCell
from apache_beam.metrics.metricbase import MetricName


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
      t = threading.Thread(target=TestCounterCell._modify_counter, args=(c, ))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (
        self.NUM_ITERATIONS * (self.NUM_ITERATIONS - 1) // 2 * self.NUM_THREADS)
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

  def test_start_time_set(self):
    c = CounterCell()
    c.inc(2)

    name = MetricName('namespace', 'name1')
    mi = c.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


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
      t = threading.Thread(
          target=TestDistributionCell._modify_distribution, args=(d, ))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (
        self.NUM_ITERATIONS * (self.NUM_ITERATIONS - 1) // 2 * self.NUM_THREADS)

    count = (self.NUM_ITERATIONS * self.NUM_THREADS)

    self.assertEqual(
        d.get_cumulative(),
        DistributionData(total, count, 0, self.NUM_ITERATIONS - 1))

  def test_basic_operations(self):
    d = DistributionCell()
    d.update(10)
    self.assertEqual(d.get_cumulative(), DistributionData(10, 1, 10, 10))

    d.update(2)
    self.assertEqual(d.get_cumulative(), DistributionData(12, 2, 2, 10))

    d.update(900)
    self.assertEqual(d.get_cumulative(), DistributionData(912, 3, 2, 900))

  def test_integer_only(self):
    d = DistributionCell()
    d.update(3.1)
    d.update(3.2)
    d.update(3.3)
    self.assertEqual(d.get_cumulative(), DistributionData(9, 3, 3, 3))

  def test_start_time_set(self):
    d = DistributionCell()
    d.update(3.1)

    name = MetricName('namespace', 'name1')
    mi = d.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


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

  def test_start_time_set(self):
    g1 = GaugeCell()
    g1.set(3)

    name = MetricName('namespace', 'name1')
    mi = g1.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


class TestStringSetCell(unittest.TestCase):
  def test_not_leak_mutable_set(self):
    c = StringSetCell()
    c.add('test')
    c.add('another')
    s = c.get_cumulative()
    self.assertEqual(s, set(('test', 'another')))
    s.add('yet another')
    self.assertEqual(c.get_cumulative(), set(('test', 'another')))

  def test_combine_appropriately(self):
    s1 = StringSetCell()
    s1.add('1')
    s1.add('2')

    s2 = StringSetCell()
    s2.add('1')
    s2.add('3')

    result = s2.combine(s1)
    self.assertEqual(result.data, set(('1', '2', '3')))


if __name__ == '__main__':
  unittest.main()
