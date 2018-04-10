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

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.metrics.metricbase import MetricName


class NameTest(unittest.TestCase):
  def test_basic_metric_name(self):
    name = MetricName('namespace1', 'name1')
    self.assertEqual(name.namespace, 'namespace1')
    self.assertEqual(name.name, 'name1')
    self.assertEqual(name, MetricName('namespace1', 'name1'))

    key = MetricKey('step1', name)
    self.assertEqual(key.step, 'step1')
    self.assertEqual(key.metric.namespace, 'namespace1')
    self.assertEqual(key.metric.name, 'name1')
    self.assertEqual(key, MetricKey('step1', MetricName('namespace1', 'name1')))


class MetricResultsTest(unittest.TestCase):

  def test_metric_filter_namespace_matching(self):
    filter = MetricsFilter().with_namespace('ns1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

  def test_metric_filter_name_matching(self):
    filter = MetricsFilter().with_name('name1').with_namespace('ns1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_name('name1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

  def test_metric_filter_step_matching(self):
    filter = MetricsFilter().with_step('Top1/Outer1/Inner1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('Top1/Outer1/Inner1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('step1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Outer1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('Top1/Outer1/Inner1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Inner1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('Top1/Outer1/Inner1', name)
    self.assertFalse(MetricResults.matches(filter, key))


class MetricsTest(unittest.TestCase):
  def test_get_namespace_class(self):
    class MyClass(object):
      pass

    self.assertEqual('{}.{}'.format(MyClass.__module__, MyClass.__name__),
                     Metrics.get_namespace(MyClass))

  def test_get_namespace_string(self):
    namespace = 'MyNamespace'
    self.assertEqual(namespace, Metrics.get_namespace(namespace))

  def test_get_namespace_error(self):
    with self.assertRaises(ValueError):
      Metrics.get_namespace(object())

  def test_counter_empty_name(self):
    with self.assertRaises(ValueError):
      Metrics.counter("namespace", "")

  def test_counter_empty_namespace(self):
    with self.assertRaises(ValueError):
      Metrics.counter("", "names")

  def test_distribution_empty_name(self):
    with self.assertRaises(ValueError):
      Metrics.distribution("namespace", "")

  def test_distribution_empty_namespace(self):
    with self.assertRaises(ValueError):
      Metrics.distribution("", "names")

  def test_create_counter_distribution(self):
    MetricsEnvironment.set_current_container(MetricsContainer('mystep'))
    counter_ns = 'aCounterNamespace'
    distro_ns = 'aDistributionNamespace'
    gauge_ns = 'aGaugeNamespace'
    name = 'a_name'
    counter = Metrics.counter(counter_ns, name)
    distro = Metrics.distribution(distro_ns, name)
    gauge = Metrics.gauge(gauge_ns, name)
    counter.inc(10)
    counter.dec(3)
    distro.update(10)
    distro.update(2)
    gauge.set(10)
    self.assertTrue(isinstance(counter, Metrics.DelegatingCounter))
    self.assertTrue(isinstance(distro, Metrics.DelegatingDistribution))
    self.assertTrue(isinstance(gauge, Metrics.DelegatingGauge))

    del distro
    del counter
    del gauge

    container = MetricsEnvironment.current_container()
    self.assertEqual(
        container.counters[MetricName(counter_ns, name)].get_cumulative(),
        7)
    self.assertEqual(
        container.distributions[MetricName(distro_ns, name)].get_cumulative(),
        DistributionData(12, 2, 2, 10))
    self.assertEqual(
        container.gauges[MetricName(gauge_ns, name)].get_cumulative().value,
        10)


if __name__ == '__main__':
  unittest.main()
