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
import unittest

import apache_beam as beam
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.metrics.metricbase import MetricName
from apache_beam.metrics.metrics_flag import MetricsFlag
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.worker import statesampler
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import counters


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


if __name__ == '__main__':
  unittest.main()
