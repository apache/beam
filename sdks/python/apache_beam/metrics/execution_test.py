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

import unittest
from builtins import range

from apache_beam.metrics.cells import CellCommitState
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.metricbase import MetricName


class TestMetricKey(unittest.TestCase):
  def test_equality_for_key_with_labels(self):
    test_labels = {'label1', 'value1'}
    test_object = MetricKey(
        'step', MetricName('namespace', 'name'), labels=test_labels)
    same_labels = MetricKey(
        'step', MetricName('namespace', 'name'), labels={'label1', 'value1'})
    same_label_reference = MetricKey(
        'step', MetricName('namespace', 'name'), labels=test_labels)
    self.assertEqual(test_object, same_labels)
    self.assertEqual(test_object, same_label_reference)
    self.assertEqual(hash(test_object), hash(same_labels))
    self.assertEqual(hash(test_object), hash(same_label_reference))

  def test_inequality_for_key_with_labels(self):
    test_labels = {'label1', 'value1'}
    test_object = MetricKey(
        'step', MetricName('namespace', 'name'), labels=test_labels)
    no_labels = MetricKey('step', MetricName('namespace', 'name'))
    diff_label_key = MetricKey(
        'step', MetricName('namespace', 'name'), labels={'l1_diff', 'value1'})
    diff_label_value = MetricKey(
        'step', MetricName('namespace', 'name'), labels={'label1', 'v1_diff'})
    self.assertNotEqual(test_object, no_labels)
    self.assertNotEqual(test_object, diff_label_key)
    self.assertNotEqual(test_object, diff_label_value)
    self.assertNotEqual(hash(test_object), hash(no_labels))
    self.assertNotEqual(hash(test_object), hash(diff_label_key))
    self.assertNotEqual(hash(test_object), hash(diff_label_value))

  def test_equality_for_key_with_no_labels(self):
    test_object = MetricKey('step', MetricName('namespace', 'name'))
    same = MetricKey('step', MetricName('namespace', 'name'))
    self.assertEqual(test_object, same)
    self.assertEqual(hash(test_object), hash(same))

    diff_step = MetricKey('step_diff', MetricName('namespace', 'name'))
    diff_namespace = MetricKey('step', MetricName('namespace_diff', 'name'))
    diff_name = MetricKey('step', MetricName('namespace', 'name_diff'))
    self.assertNotEqual(test_object, diff_step)
    self.assertNotEqual(test_object, diff_namespace)
    self.assertNotEqual(test_object, diff_name)
    self.assertNotEqual(hash(test_object), hash(diff_step))
    self.assertNotEqual(hash(test_object), hash(diff_namespace))
    self.assertNotEqual(hash(test_object), hash(diff_name))


class TestMetricsContainer(unittest.TestCase):
  def test_create_new_counter(self):
    mc = MetricsContainer('astep')
    self.assertFalse(MetricName('namespace', 'name') in mc.counters)
    mc.get_counter(MetricName('namespace', 'name'))
    self.assertTrue(MetricName('namespace', 'name') in mc.counters)

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


if __name__ == '__main__':
  unittest.main()
