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

import unittest

import hamcrest as hc
import mock
import pytest

import apache_beam as beam
from apache_beam import metrics
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metric import Lineage
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.worker import statesampler
from apache_beam.testing.metric_result_matchers import DistributionMatcher
from apache_beam.testing.metric_result_matchers import MetricResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import counters


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
    name = MetricName('ns1', 'name1')
    filter = MetricsFilter().with_step('Step1')

    key = MetricKey('Step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    key = MetricKey('Step10', name)
    self.assertFalse(MetricResults.matches(filter, key))

    key = MetricKey('Step10/Step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    key = MetricKey('Top1/Outer1/Inner1', name)

    filter = MetricsFilter().with_step('Top1/Outer1/Inner1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Outer1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Outer1/Inner1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Inner1')
    self.assertFalse(MetricResults.matches(filter, key))


class MetricsTest(unittest.TestCase):
  def test_get_namespace_class(self):
    class MyClass(object):
      pass

    self.assertEqual(
        '{}.{}'.format(MyClass.__module__, MyClass.__name__),
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

  # Do not change the behaviour of str(), do tno update/delete this test case
  # if the behaviour of str() is changed. Doing so will
  # break end user beam code which depends on the str() behaviour.
  def test_user_metric_name_str(self):
    mn = MetricName("my_namespace", "my_name")
    expected_str = 'MetricName(namespace=my_namespace, name=my_name)'
    self.assertEqual(str(mn), expected_str)

  def test_general_urn_metric_name_str(self):
    mn = MetricName(
        "my_namespace", "my_name", urn='my_urn', labels={'key': 'value'})
    expected_str = (
        "MetricName(namespace=my_namespace, name=my_name, "
        "urn=my_urn, labels={'key': 'value'})")
    self.assertEqual(str(mn), expected_str)

  @pytest.mark.it_validatesrunner
  def test_user_counter_using_pardo(self):
    class SomeDoFn(beam.DoFn):
      """A custom dummy DoFn using yield."""
      static_counter_elements = metrics.Metrics.counter(
          "SomeDoFn", 'metrics_static_counter_element')

      def __init__(self):
        self.user_counter_elements = metrics.Metrics.counter(
            self.__class__, 'metrics_user_counter_element')

      def process(self, element):
        self.static_counter_elements.inc(2)
        self.user_counter_elements.inc()
        distro = Metrics.distribution(self.__class__, 'element_dist')
        distro.update(element)
        yield element

    pipeline = TestPipeline()
    nums = pipeline | 'Input' >> beam.Create([1, 2, 3, 4])
    results = nums | 'ApplyPardo' >> beam.ParDo(SomeDoFn())
    assert_that(results, equal_to([1, 2, 3, 4]))

    res = pipeline.run()
    res.wait_until_finish()

    # Verify static counter.
    metric_results = (
        res.metrics().query(
            MetricsFilter().with_metric(SomeDoFn.static_counter_elements)))
    outputs_static_counter = metric_results['counters'][0]

    self.assertEqual(
        outputs_static_counter.key.metric.name,
        'metrics_static_counter_element')
    self.assertEqual(outputs_static_counter.committed, 8)

    # Verify user counter.
    metric_results = (
        res.metrics().query(
            MetricsFilter().with_name('metrics_user_counter_element')))
    outputs_user_counter = metric_results['counters'][0]

    self.assertEqual(
        outputs_user_counter.key.metric.name, 'metrics_user_counter_element')
    self.assertEqual(outputs_user_counter.committed, 4)

    # Verify user distribution counter.
    metric_results = res.metrics().query()
    matcher = MetricResultMatcher(
        step='ApplyPardo',
        namespace=hc.contains_string('SomeDoFn'),
        name='element_dist',
        committed=DistributionMatcher(
            sum_value=hc.greater_than_or_equal_to(0),
            count_value=hc.greater_than_or_equal_to(0),
            min_value=hc.greater_than_or_equal_to(0),
            max_value=hc.greater_than_or_equal_to(0)))
    hc.assert_that(
        metric_results['distributions'], hc.contains_inanyorder(matcher))

  def test_create_counter_distribution(self):
    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state1 = sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

    try:
      sampler.start()
      with state1:
        counter_ns = 'aCounterNamespace'
        distro_ns = 'aDistributionNamespace'
        name = 'a_name'
        counter = Metrics.counter(counter_ns, name)
        distro = Metrics.distribution(distro_ns, name)
        counter.inc(10)
        counter.dec(3)
        distro.update(10)
        distro.update(2)
        self.assertTrue(isinstance(counter, Metrics.DelegatingCounter))
        self.assertTrue(isinstance(distro, Metrics.DelegatingDistribution))

        del distro
        del counter

        container = MetricsEnvironment.current_container()
        self.assertEqual(
            container.get_counter(MetricName(counter_ns,
                                             name)).get_cumulative(),
            7)
        self.assertEqual(
            container.get_distribution(MetricName(distro_ns,
                                                  name)).get_cumulative(),
            DistributionData(12, 2, 2, 10))
    finally:
      sampler.stop()


class LineageTest(unittest.TestCase):
  def test_fq_name(self):
    test_cases = {
        "apache-beam": "apache-beam",
        "`apache-beam`": "`apache-beam`",
        "apache.beam": "`apache.beam`",
        "apache:beam": "`apache:beam`",
        "apache beam": "`apache beam`",
        "`apache beam`": "`apache beam`",
        "apache\tbeam": "`apache\tbeam`",
        "apache\nbeam": "`apache\nbeam`"
    }
    for k, v in test_cases.items():
      self.assertEqual("apache:" + v, Lineage.get_fq_name("apache", k))
      self.assertEqual(
          "apache:beam:" + v, Lineage.get_fq_name("apache", k, subtype="beam"))
      self.assertEqual(
          "apache:beam:" + v + '.' + v,
          Lineage.get_fq_name("apache", k, k, subtype="beam"))

  def test_add(self):
    lineage = Lineage(Lineage.SOURCE)
    stringset = set()
    # override
    lineage.metric = stringset
    lineage.add("s", "1", "2")
    lineage.add("s:3.4")
    lineage.add("s", "5", "6.7")
    lineage.add("s", "1", "2", subtype="t")
    self.assertSetEqual(stringset, {"s:1.2", "s:3.4", "s:t:1.2", "s:5.`6.7`"})

  @pytest.mark.no_xdist
  def test_lineage_enable_disable(self):
    mock_metric = mock.Mock()
    try:
      lineage = Lineage(Lineage.SOURCE)
      lineage.metric = mock_metric
      lineage._LINEAGE_DISABLED = False
      lineage.add('beam', 'path')
      mock_metric.add.assert_called_once_with('beam:path')

      options = PipelineOptions.from_dictionary(
          {'experiments': ['disable_lineage']})
      lineage.set_options(options)
      lineage.add('beam', 'path2')
      mock_metric.add.assert_called_once()

      options = PipelineOptions.from_dictionary({'runner': 'DataflowRunner'})
      lineage.set_options(options)
      lineage.add('beam', 'path3')
      mock_metric.add.assert_called_once()

      options = PipelineOptions.from_dictionary({
          'runner': 'DataflowRunner', 'experiments': ['enable_lineage']
      })
      lineage.set_options(options)
      lineage.add('beam', 'path4')
      mock_metric.add.assert_called_with('beam:path4')
    finally:
      lineage._LINEAGE_DISABLED = False


if __name__ == '__main__':
  unittest.main()
