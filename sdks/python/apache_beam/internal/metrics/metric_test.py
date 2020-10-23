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

from __future__ import absolute_import

import unittest

from mock import patch

from apache_beam.internal.metrics.cells import HistogramCellFactory
from apache_beam.internal.metrics.metric import MetricLogger
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metricbase import MetricName
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


if __name__ == '__main__':
  unittest.main()
