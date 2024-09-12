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

"""Tests for Throttling Handler of GCSIO."""

import unittest
from unittest.mock import Mock

from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.utils import counters

try:
  from apache_beam.io.gcp import gcsio_retry
  from google.api_core import exceptions as api_exceptions
except ImportError:
  gcsio_retry = None
  api_exceptions = None


@unittest.skipIf((gcsio_retry is None or api_exceptions is None),
                 'GCP dependencies are not installed')
class TestGCSIORetry(unittest.TestCase):
  def test_retry_on_non_retriable(self):
    mock = Mock(side_effect=[
        Exception('Something wrong!'),
    ])
    retry = gcsio_retry.DEFAULT_RETRY_WITH_THROTTLING_COUNTERS
    with self.assertRaises(Exception):
      retry(mock)()

  def test_retry_on_throttling(self):
    mock = Mock(
        side_effect=[
            api_exceptions.TooManyRequests("Slow down!"),
            api_exceptions.TooManyRequests("Slow down again!"),
            12345
        ])
    retry = gcsio_retry.DEFAULT_RETRY_WITH_THROTTLING_COUNTERS

    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state = sampler.scoped_state(
        'my_step', 'my_state', metrics_container=MetricsContainer('my_step'))
    try:
      sampler.start()
      with state:
        container = MetricsEnvironment.current_container()

        self.assertEqual(
            container.get_counter(
                MetricName('gcsio',
                           "cumulativeThrottlingSeconds")).get_cumulative(),
            0)

        self.assertEqual(12345, retry(mock)())

        self.assertGreater(
            container.get_counter(
                MetricName('gcsio',
                           "cumulativeThrottlingSeconds")).get_cumulative(),
            1)
    finally:
      sampler.stop()


if __name__ == '__main__':
  unittest.main()
