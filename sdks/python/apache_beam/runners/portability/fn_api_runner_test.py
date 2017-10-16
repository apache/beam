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

import logging
import time
import unittest

import apache_beam as beam
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.portability import maptask_executor_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apache_beam.runners.worker.statesampler import DEFAULT_SAMPLING_PERIOD_MS
except ImportError:
  DEFAULT_SAMPLING_PERIOD_MS = 0


class FnApiRunnerTest(
    maptask_executor_runner_test.MapTaskExecutorRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_grpc=False))

  def test_combine_per_key(self):
    # TODO(BEAM-1348): Enable once Partial GBK is supported in fn API.
    pass

  def test_combine_per_key(self):
    # TODO(BEAM-1348): Enable once Partial GBK is supported in fn API.
    pass

  def test_pardo_side_inputs(self):
    # TODO(BEAM-1348): Enable once side inputs are supported in fn API.
    pass

  def test_pardo_unfusable_side_inputs(self):
    # TODO(BEAM-1348): Enable once side inputs are supported in fn API.
    pass

  def test_assert_that(self):
    # TODO: figure out a way for fn_api_runner to parse and raise the
    # underlying exception.
    with self.assertRaisesRegexp(Exception, 'Failed assert'):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_progress_metrics(self):
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      return

    _ = (p
         | beam.Create([0, 0, 0, 2.1e-3 * DEFAULT_SAMPLING_PERIOD_MS])
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
          2e-3 * DEFAULT_SAMPLING_PERIOD_MS,
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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
