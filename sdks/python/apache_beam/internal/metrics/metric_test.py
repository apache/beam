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

from mock import patch

from apache_beam.internal.metrics.cells import HistogramCellFactory
from apache_beam.internal.metrics.metric import Metrics as InternalMetrics
from apache_beam.internal.metrics.metric import MetricLogger
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.utils import counters
from apache_beam.utils.histogram import LinearBucket


class MetricLoggerTest(unittest.TestCase):
  @patch('apache_beam.internal.metrics.metric._LOGGER')
  def test_log_metrics(self, mock_logger):
    logger = MetricLogger()
    logger.minimum_logging_frequency_msec = -1
    namespace = Metrics.get_namespace(self.__class__)
    metric_name = MetricName(namespace, 'metric_logger_test')
    logger.update(HistogramCellFactory(LinearBucket(0, 1, 10)), metric_name, 1)
    logger.log_metrics()

    class Contains(str):
      def __eq__(self, other):
        return self in other

    mock_logger.info.assert_called_once_with(
        Contains('HistogramData(Total count: 1, P99: 2, P90: 2, P50: 2)'))


class MetricsTest(unittest.TestCase):
  def test_create_process_wide(self):
    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state1 = sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

    try:
      sampler.start()
      with state1:
        urn = "my:custom:urn"
        labels = {'key': 'value'}
        counter = InternalMetrics.counter(
            urn=urn, labels=labels, process_wide=True)
        # Test that if process_wide is set, that it will be set
        # on the process_wide container.
        counter.inc(10)
        self.assertTrue(isinstance(counter, Metrics.DelegatingCounter))

        del counter

        metric_name = MetricName(None, None, urn=urn, labels=labels)
        # Expect a value set on the current container.
        self.assertEqual(
            MetricsEnvironment.process_wide_container().get_counter(
                metric_name).get_cumulative(),
            10)
        # Expect no value set on the current container.
        self.assertEqual(
            MetricsEnvironment.current_container().get_counter(
                metric_name).get_cumulative(),
            0)
    finally:
      sampler.stop()


if __name__ == '__main__':
  unittest.main()
