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

"""Unit tests for the PipelineRunner and DirectRunner classes.

Note that PipelineRunner and DirectRunner functionality is tested in all
the other unit tests. In this file we choose to test only aspects related to
caching and clearing values that are not tested elsewhere.
"""

import unittest

import hamcrest as hc

import apache_beam as beam
import apache_beam.transforms as ptransform
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metricbase import MetricName
from apache_beam.pipeline import Pipeline
from apache_beam.runners import DirectRunner
from apache_beam.runners import create_runner
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to
from apache_beam.utils.pipeline_options import PipelineOptions


class RunnerTest(unittest.TestCase):
  default_properties = [
      '--dataflow_endpoint=ignored',
      '--job_name=test-job',
      '--project=test-project',
      '--staging_location=ignored',
      '--temp_location=/dev/null',
      '--no_auth=True']

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectRunner'), DirectRunner))
    self.assertRaises(ValueError, create_runner, 'xyz')

  def test_create_runner_shorthand(self):
    self.assertTrue(
        isinstance(create_runner('DiReCtRuNnEr'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('directrunner'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('direct'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('DiReCt'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('Direct'), DirectRunner))

  def test_direct_runner_metrics(self):
    from apache_beam.metrics.metric import Metrics

    class MyDoFn(beam.DoFn):
      def start_bundle(self):
        count = Metrics.counter(self.__class__, 'bundles')
        count.inc()

      def finish_bundle(self):
        count = Metrics.counter(self.__class__, 'finished_bundles')
        count.inc()

      def process(self, element):
        count = Metrics.counter(self.__class__, 'elements')
        count.inc()
        distro = Metrics.distribution(self.__class__, 'element_dist')
        distro.update(element)
        return [element]

    runner = DirectRunner()
    p = Pipeline(runner,
                 options=PipelineOptions(self.default_properties))
    pcoll = (p | ptransform.Create([1, 2, 3, 4, 5])
             | 'Do' >> beam.ParDo(MyDoFn()))
    assert_that(pcoll, equal_to([1, 2, 3, 4, 5]))
    result = p.run()
    result.wait_until_finish()
    metrics = result.metrics().query()
    namespace = '{}.{}'.format(MyDoFn.__module__,
                               MyDoFn.__name__)

    hc.assert_that(
        metrics['counters'],
        hc.contains_inanyorder(
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'elements')),
                5, 5),
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'bundles')),
                1, 1),
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'finished_bundles')),
                1, 1)))
    hc.assert_that(
        metrics['distributions'],
        hc.contains_inanyorder(
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'element_dist')),
                DistributionResult(DistributionData(15, 5, 1, 5)),
                DistributionResult(DistributionData(15, 5, 1, 5)))))


if __name__ == '__main__':
  unittest.main()
