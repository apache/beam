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
import pickle
import re
import unittest

import hamcrest as hc
import pytest

import apache_beam as beam
from apache_beam import metrics
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metric import Lineage
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.metrics.metric import MetricsFlag
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.runners.worker import statesampler
from apache_beam.testing.metric_result_matchers import DistributionMatcher
from apache_beam.testing.metric_result_matchers import MetricResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import counters
from apache_beam.utils.histogram import LinearBucket


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
        step=hc.contains_string('ApplyPardo'),
        namespace=hc.contains_string('SomeDoFn'),
        name='element_dist',
        committed=DistributionMatcher(
            sum_value=hc.greater_than_or_equal_to(0),
            count_value=hc.greater_than_or_equal_to(0),
            min_value=hc.greater_than_or_equal_to(0),
            max_value=hc.greater_than_or_equal_to(0)))
    hc.assert_that(metric_results['distributions'], hc.has_item(matcher))

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


class MetricsFlagTest(unittest.TestCase):
  """Covers the disable*Metrics experiments.

  See https://github.com/apache/beam/issues/38746.
  """
  def setUp(self):
    MetricsFlag.reset()
    self.sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(self.sampler)
    self.state = self.sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))
    self.sampler.start()

  def tearDown(self):
    self.sampler.stop()
    MetricsFlag.reset()

  @staticmethod
  def _set_experiments(*experiments):
    MetricsFlag.set_default_pipeline_options(
        PipelineOptions(['--experiments=%s' % exp for exp in experiments]))

  def test_flags_follow_experiments(self):
    self.assertFalse(MetricsFlag.counter_disabled())
    self.assertFalse(MetricsFlag.string_set_disabled())
    self.assertFalse(MetricsFlag.bounded_trie_disabled())

    for experiment, expected in [
        ('disableCounterMetrics', (True, False, False)),
        ('disableStringSetMetrics', (False, True, False)),
        ('disableBoundedTrieMetrics', (False, False, True)),
    ]:
      MetricsFlag.reset()
      self._set_experiments(experiment)
      self.assertEqual((
          MetricsFlag.counter_disabled(),
          MetricsFlag.string_set_disabled(),
          MetricsFlag.bounded_trie_disabled()),
                       expected,
                       experiment)

    MetricsFlag.reset()
    self._set_experiments(
        'disableCounterMetrics',
        'disableStringSetMetrics',
        'disableBoundedTrieMetrics')
    self.assertTrue(MetricsFlag.counter_disabled())
    self.assertTrue(MetricsFlag.string_set_disabled())
    self.assertTrue(MetricsFlag.bounded_trie_disabled())

  def test_first_call_wins(self):
    self._set_experiments('disableCounterMetrics')
    # Later options, e.g. from user code constructing a Pipeline on a worker,
    # do not change the flags the harness was started with.
    self._set_experiments('disableStringSetMetrics')
    self.assertTrue(MetricsFlag.counter_disabled())
    self.assertFalse(MetricsFlag.string_set_disabled())

  def test_update_call_shapes_keep_working(self):
    # The first attempt at this feature (#38749) was reverted because it
    # changed the signature of DelegatingCounter.inc; the metric objects must
    # stay MetricUpdater callables that accept the value as a keyword too.
    with self.state:
      counter = Metrics.counter('ns', 'counter')
      self.assertIsInstance(counter.inc, MetricUpdater)
      counter.inc()
      counter.inc(4)
      counter.inc(value=5)
      counter.dec()
      counter.dec(2)
      string_set = Metrics.string_set('ns', 'set')
      string_set.add('a')
      string_set.add(value='b')
      container = MetricsEnvironment.current_container()
      self.assertEqual(
          container.get_counter(MetricName('ns', 'counter')).get_cumulative(),
          7)
      self.assertEqual(
          container.get_string_set(MetricName(
              'ns', 'set')).get_cumulative().string_set, {'a', 'b'})

  def test_disabled_counter_is_noop(self):
    with self.state:
      container = MetricsEnvironment.current_container()
      Metrics.counter('ns', 'before').inc()
      self.assertEqual(len(container.metrics), 1)

      self._set_experiments('disableCounterMetrics')
      created_before = Metrics.counter('ns', 'before')
      created_before.inc()
      created_before.inc(value=5)
      created_before.dec()
      Metrics.counter('ns', 'after').inc(3)
      self.assertEqual(len(container.metrics), 1)
      self.assertEqual(
          container.get_counter(MetricName('ns', 'before')).get_cumulative(), 1)

      # Other kinds keep reporting.
      Metrics.distribution('ns', 'dist').update(3)
      Metrics.gauge('ns', 'gauge').set(2)
      Metrics.string_set('ns', 'set').add('x')
      Metrics.bounded_trie('ns', 'trie').add(('x', ))
      self.assertEqual(len(container.metrics), 5)

  def test_disabled_string_set_is_noop(self):
    with self.state:
      container = MetricsEnvironment.current_container()
      Metrics.string_set('ns', 'before').add('seed')
      self.assertEqual(len(container.metrics), 1)

      self._set_experiments('disableStringSetMetrics')
      Metrics.string_set('ns', 'before').add('more')
      Metrics.string_set('ns', 'after').add('value')
      self.assertEqual(len(container.metrics), 1)
      self.assertEqual(
          container.get_string_set(MetricName(
              'ns', 'before')).get_cumulative().string_set, {'seed'})
      Metrics.counter('ns', 'counter').inc()
      self.assertEqual(len(container.metrics), 2)

  def test_disabled_bounded_trie_is_noop(self):
    with self.state:
      container = MetricsEnvironment.current_container()
      Metrics.bounded_trie('ns', 'before').add(('a', ))
      self.assertEqual(len(container.metrics), 1)

      self._set_experiments('disableBoundedTrieMetrics')
      Metrics.bounded_trie('ns', 'before').add(('a', 'b'))
      Metrics.bounded_trie('ns', 'after').add(('c', ))
      self.assertEqual(len(container.metrics), 1)
      self.assertEqual(
          list(
              container.get_bounded_trie(MetricName(
                  'ns', 'before')).get_cumulative().flattened()),
          [('a', False)])

  def test_disabled_process_wide_counter_is_noop(self):
    self._set_experiments('disableCounterMetrics')
    name = MetricName('ns', 'process_wide')
    counter = Metrics.DelegatingCounter(name, process_wide=True)
    counter.inc()
    # The process-wide container is shared with every other test in this
    # process, so only check that this counter never reached it.
    self.assertNotIn(
        MetricKey(None, name),
        MetricsEnvironment.process_wide_container().get_cumulative().counters)

  def test_disabled_flag_applies_to_unpickled_metrics(self):
    # DoFns holding metric objects are pickled at submission time and
    # unpickled on the worker, where the harness sets the flags.
    counter = Metrics.counter('ns', 'pickled')
    counter = pickle.loads(pickle.dumps(counter))
    self.assertIsInstance(counter.inc, MetricUpdater)
    self._set_experiments('disableCounterMetrics')
    with self.state:
      counter.inc()
      self.assertEqual(len(MetricsEnvironment.current_container().metrics), 0)

  def test_disabled_counters_in_pipeline(self):
    class SomeDoFn(beam.DoFn):
      def process(self, element):
        Metrics.counter(self.__class__, 'elements').inc()
        Metrics.distribution(self.__class__, 'element_dist').update(element)
        yield element

    MetricsFlag.reset()
    pipeline = TestPipeline(
        options=PipelineOptions(['--experiments=disableCounterMetrics']))
    results = pipeline | beam.Create([1, 2, 3]) | beam.ParDo(SomeDoFn())
    assert_that(results, equal_to([1, 2, 3]))
    res = pipeline.run()
    res.wait_until_finish()

    self.assertEqual(
        res.metrics().query(MetricsFilter().with_name('elements'))['counters'],
        [])
    distributions = res.metrics().query(
        MetricsFilter().with_name('element_dist'))['distributions']
    self.assertEqual(len(distributions), 1)
    self.assertEqual(
        distributions[0].committed.data, DistributionData(6, 3, 1, 3))


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
    added = set()
    # override
    lineage.metric = added
    lineage.add("s", "1", "2")
    lineage.add("s:3.4")
    lineage.add("s", "5", "6.7")
    lineage.add("s", "1", "2", subtype="t")
    lineage.add("sys", "seg1", "seg2", "seg3/part2/part3", last_segment_sep='/')
    self.assertSetEqual(
        added,
        {('s:', '1.', '2'), ('s:3.4:', ), ('s:', '5.', '6.7'),
         ('s:', 't:', '1.', '2'),
         ('sys:', 'seg1.', 'seg2.', 'seg3/', 'part2/', 'part3')})


class HistogramTest(unittest.TestCase):
  def test_histogram(self):
    class WordExtractingDoFn(beam.DoFn):
      def __init__(self):
        self.word_lengths_dist = Metrics.histogram(
            self.__class__,
            'latency_histogram_ms',
            LinearBucket(0, 1, num_buckets=10))

      def process(self, element):
        text_line = element.strip()
        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        for w in words:
          self.word_lengths_dist.update(len(w))
        return words

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      lines = p | 'read' >> beam.Create(["x x x yyyyyy yyyyyy yyyyyy"])
      _ = (
          lines
          | 'split' >>
          (beam.ParDo(WordExtractingDoFn()).with_output_types(str)))

    result = p.result

    filter = MetricsFilter().with_name('latency_histogram_ms')
    query_result = result.metrics().query(filter)
    histogram = query_result['histograms'][0].committed.histogram
    assert histogram._buckets == {1: 3, 6: 3}
    assert histogram.total_count() == 6
    assert 1 < histogram.get_linear_interpolation(0.50) < 3
    assert histogram.get_linear_interpolation(0.99) > 3


if __name__ == '__main__':
  unittest.main()
