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

import unittest

from apache_beam.metrics.cells import CellCommitState
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.execution import ScopedMetricsContainer
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metricbase import MetricName


class TestMetricsContainer(unittest.TestCase):
  def test_create_new_counter(self):
    mc = MetricsContainer('astep')
    self.assertFalse(MetricName('namespace', 'name') in mc.counters)
    mc.get_counter(MetricName('namespace', 'name'))
    self.assertTrue(MetricName('namespace', 'name') in mc.counters)

  def test_scoped_container(self):
    c1 = MetricsContainer('mystep')
    c2 = MetricsContainer('myinternalstep')
    with ScopedMetricsContainer(c1):
      self.assertEqual(c1, MetricsEnvironment.current_container())
      counter = Metrics.counter('ns', 'name')
      counter.inc(2)

      with ScopedMetricsContainer(c2):
        self.assertEqual(c2, MetricsEnvironment.current_container())
        counter = Metrics.counter('ns', 'name')
        counter.inc(3)
        self.assertEqual(
            list(c2.get_cumulative().counters.items()),
            [(MetricKey('myinternalstep', MetricName('ns', 'name')), 3)])

      self.assertEqual(c1, MetricsEnvironment.current_container())
      counter = Metrics.counter('ns', 'name')
      counter.inc(4)
      self.assertEqual(
          list(c1.get_cumulative().counters.items()),
          [(MetricKey('mystep', MetricName('ns', 'name')), 6)])

  def test_add_to_counter(self):
    mc = MetricsContainer('astep')
    counter = mc.get_counter(MetricName('namespace', 'name'))
    counter.inc()
    counter = mc.get_counter(MetricName('namespace', 'name'))
    self.assertEqual(counter.value, 1)

  def test_get_cumulative_or_updates(self):
    mc = MetricsContainer('astep')

    clean_values = []
    dirty_values = []
    for i in range(1, 11):
      counter = mc.get_counter(MetricName('namespace', 'name{}'.format(i)))
      distribution = mc.get_distribution(
          MetricName('namespace', 'name{}'.format(i)))
      gauge = mc.get_gauge(MetricName('namespace', 'name{}'.format(i)))

      counter.inc(i)
      distribution.update(i)
      gauge.set(i)
      if i % 2 == 0:
        # Some are left to be DIRTY (i.e. not yet committed).
        # Some are left to be CLEAN (i.e. already committed).
        dirty_values.append(i)
        continue
      # Assert: Counter/Distribution is DIRTY or COMMITTING (not CLEAN)
      self.assertEqual(distribution.commit.before_commit(), True)
      self.assertEqual(counter.commit.before_commit(), True)
      self.assertEqual(gauge.commit.before_commit(), True)
      distribution.commit.after_commit()
      counter.commit.after_commit()
      gauge.commit.after_commit()
      # Assert: Counter/Distribution has been committed, therefore it's CLEAN
      self.assertEqual(counter.commit.state, CellCommitState.CLEAN)
      self.assertEqual(distribution.commit.state, CellCommitState.CLEAN)
      self.assertEqual(gauge.commit.state, CellCommitState.CLEAN)
      clean_values.append(i)

    # Retrieve NON-COMMITTED updates.
    logical = mc.get_updates()
    self.assertEqual(len(logical.counters), 5)
    self.assertEqual(len(logical.distributions), 5)
    self.assertEqual(len(logical.gauges), 5)

    self.assertEqual(set(dirty_values),
                     set([v.value for _, v in logical.gauges.items()]))
    self.assertEqual(set(dirty_values),
                     set([v for _, v in logical.counters.items()]))

    # Retrieve ALL updates.
    cumulative = mc.get_cumulative()
    self.assertEqual(len(cumulative.counters), 10)
    self.assertEqual(len(cumulative.distributions), 10)
    self.assertEqual(len(cumulative.gauges), 10)

    self.assertEqual(set(dirty_values + clean_values),
                     set([v for _, v in cumulative.counters.items()]))
    self.assertEqual(set(dirty_values + clean_values),
                     set([v.value for _, v in cumulative.gauges.items()]))


class TestMetricsEnvironment(unittest.TestCase):
  def test_uses_right_container(self):
    c1 = MetricsContainer('step1')
    c2 = MetricsContainer('step2')
    counter = Metrics.counter('ns', 'name')
    MetricsEnvironment.set_current_container(c1)
    counter.inc()
    MetricsEnvironment.set_current_container(c2)
    counter.inc(3)
    MetricsEnvironment.unset_current_container()

    self.assertEqual(
        list(c1.get_cumulative().counters.items()),
        [(MetricKey('step1', MetricName('ns', 'name')), 1)])

    self.assertEqual(
        list(c2.get_cumulative().counters.items()),
        [(MetricKey('step2', MetricName('ns', 'name')), 3)])

  def test_no_container(self):
    self.assertEqual(MetricsEnvironment.current_container(),
                     None)


if __name__ == '__main__':
  unittest.main()
