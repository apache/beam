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
import functools
import logging
import time
import unittest

import apache_beam as beam
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.portability import maptask_executor_runner_test
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker import statesampler
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window

if statesampler.FAST_SAMPLER:
  DEFAULT_SAMPLING_PERIOD_MS = statesampler.DEFAULT_SAMPLING_PERIOD_MS
else:
  DEFAULT_SAMPLING_PERIOD_MS = 0


# Inherit good model test coverage from
# maptask_executor_runner_test.MapTaskExecutorRunnerTest.
class FnApiRunnerTest(
    maptask_executor_runner_test.MapTaskExecutorRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_grpc=False))

  def test_pardo_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b', 'c'])
      side = p | 'side' >> beam.Create(['x', 'y'])
      assert_that(main | beam.FlatMap(cross_product, beam.pvalue.AsList(side)),
                  equal_to([('a', 'x'), ('b', 'x'), ('c', 'x'),
                            ('a', 'y'), ('b', 'y'), ('c', 'y')]))

      # Now with some windowing.
      pcoll = p | beam.Create(range(10)) | beam.Map(
          lambda t: window.TimestampedValue(t, t))
      # Intentionally choosing non-aligned windows to highlight the transition.
      main = pcoll | 'WindowMain' >> beam.WindowInto(window.FixedWindows(5))
      side = pcoll | 'WindowSide' >> beam.WindowInto(window.FixedWindows(7))
      res = main | beam.Map(lambda x, s: (x, sorted(s)),
                            beam.pvalue.AsList(side))
      assert_that(
          res,
          equal_to([
              # The window [0, 5) maps to the window [0, 7).
              (0, range(7)),
              (1, range(7)),
              (2, range(7)),
              (3, range(7)),
              (4, range(7)),
              # The window [5, 10) maps to the window [7, 14).
              (5, range(7, 10)),
              (6, range(7, 10)),
              (7, range(7, 10)),
              (8, range(7, 10)),
              (9, range(7, 10))]),
          label='windowed')

  def test_flattened_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side1 = p | 'side1' >> beam.Create([('a', 1)])
      side2 = p | 'side2' >> beam.Create([('b', 2)])
      side = (side1, side2) | beam.Flatten()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {'a': 1, 'b': 2})]))

  def test_gbk_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side = p | 'side' >> beam.Create([('a', 1)]) | beam.GroupByKey()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {'a': [1]})]))

  def test_assert_that(self):
    # TODO: figure out a way for fn_api_runner to parse and raise the
    # underlying exception.
    with self.assertRaisesRegexp(Exception, 'Failed assert'):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_no_subtransform_composite(self):

    class First(beam.PTransform):
      def expand(self, pcolls):
        return pcolls[0]

    with self.create_pipeline() as p:
      pcoll_a = p | 'a' >> beam.Create(['a'])
      pcoll_b = p | 'b' >> beam.Create(['b'])
      assert_that((pcoll_a, pcoll_b) | First(), equal_to(['a']))

  def test_metrics(self):

    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      self.skipTest('Metrics not supported.')

    counter = beam.metrics.Metrics.counter('ns', 'counter')
    distribution = beam.metrics.Metrics.distribution('ns', 'distribution')
    gauge = beam.metrics.Metrics.gauge('ns', 'gauge')

    pcoll = p | beam.Create(['a', 'zzz'])
    # pylint: disable=expression-not-assigned
    pcoll | 'count1' >> beam.FlatMap(lambda x: counter.inc())
    pcoll | 'count2' >> beam.FlatMap(lambda x: counter.inc(len(x)))
    pcoll | 'dist' >> beam.FlatMap(lambda x: distribution.update(len(x)))
    pcoll | 'gauge' >> beam.FlatMap(lambda x: gauge.set(len(x)))

    res = p.run()
    res.wait_until_finish()
    c1, = res.metrics().query(beam.metrics.MetricsFilter().with_step('count1'))[
        'counters']
    self.assertEqual(c1.committed, 2)
    c2, = res.metrics().query(beam.metrics.MetricsFilter().with_step('count2'))[
        'counters']
    self.assertEqual(c2.committed, 4)
    dist, = res.metrics().query(beam.metrics.MetricsFilter().with_step('dist'))[
        'distributions']
    gaug, = res.metrics().query(
        beam.metrics.MetricsFilter().with_step('gauge'))['gauges']
    self.assertEqual(
        dist.committed.data, beam.metrics.cells.DistributionData(4, 2, 1, 3))
    self.assertEqual(dist.committed.mean, 2.0)
    self.assertEqual(gaug.committed.value, 3)

  def test_progress_metrics(self):
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      self.skipTest('Progress metrics not supported.')

    _ = (p
         | beam.Create([0, 0, 0, 5e-3 * DEFAULT_SAMPLING_PERIOD_MS])
         | beam.Map(time.sleep)
         | beam.Map(lambda x: ('key', x))
         | beam.GroupByKey()
         | 'm_out' >> beam.FlatMap(lambda x: [
             1, 2, 3, 4, 5,
             beam.pvalue.TaggedOutput('once', x),
             beam.pvalue.TaggedOutput('twice', x),
             beam.pvalue.TaggedOutput('twice', x)]))
    res = p.run()
    res.wait_until_finish()
    try:
      self.assertEqual(2, len(res._metrics_by_stage))
      pregbk_metrics, postgbk_metrics = res._metrics_by_stage.values()
      if 'Create/Read' not in pregbk_metrics.ptransforms:
        # The metrics above are actually unordered. Swap.
        pregbk_metrics, postgbk_metrics = postgbk_metrics, pregbk_metrics

      self.assertEqual(
          4,
          pregbk_metrics.ptransforms['Create/Read']
          .processed_elements.measured.output_element_counts['None'])
      self.assertEqual(
          4,
          pregbk_metrics.ptransforms['Map(sleep)']
          .processed_elements.measured.output_element_counts['None'])
      self.assertLessEqual(
          4e-3 * DEFAULT_SAMPLING_PERIOD_MS,
          pregbk_metrics.ptransforms['Map(sleep)']
          .processed_elements.measured.total_time_spent)
      self.assertEqual(
          1,
          postgbk_metrics.ptransforms['GroupByKey/Read']
          .processed_elements.measured.output_element_counts['None'])

      # The actual stage name ends up being something like 'm_out/lamdbda...'
      m_out, = [
          metrics for name, metrics in postgbk_metrics.ptransforms.items()
          if name.startswith('m_out')]
      self.assertEqual(
          5,
          m_out.processed_elements.measured.output_element_counts['None'])
      self.assertEqual(
          1,
          m_out.processed_elements.measured.output_element_counts['once'])
      self.assertEqual(
          2,
          m_out.processed_elements.measured.output_element_counts['twice'])

    except:
      print res._metrics_by_stage
      raise

  # Inherits all tests from maptask_executor_runner.MapTaskExecutorRunner


class FnApiRunnerTestWithGrpc(FnApiRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_grpc=True))


class FnApiRunnerTestWithGrpcMultiThreaded(FnApiRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            use_grpc=True,
            sdk_harness_factory=functools.partial(
                sdk_worker.SdkHarness, worker_count=2)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
