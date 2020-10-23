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

"""
Metrics API classes for internal use only.

Users should use apache_beam.metrics.metric package instead.

For internal use only. No backwards compatibility guarantees.
"""
# pytype: skip-file
# mypy: disallow-untyped-defs

from __future__ import absolute_import

import datetime
import logging
import threading
import time
from builtins import object
from typing import TYPE_CHECKING
from typing import Dict
from typing import Optional
from typing import Type
from typing import Union

from apache_beam.internal.metrics.cells import HistogramCellFactory
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metric import Metrics as UserMetrics
from apache_beam.metrics.metricbase import Histogram
from apache_beam.metrics.metricbase import MetricName

if TYPE_CHECKING:
  from apache_beam.metrics.cells import MetricCell
  from apache_beam.metrics.cells import MetricCellFactory
  from apache_beam.utils.histogram import BucketType

__all__ = ['Metrics']

_LOGGER = logging.getLogger(__name__)


class Metrics(object):
  @staticmethod
  def histogram(namespace, name, bucket_type, logger=None):
    # type: (Union[Type, str], str, BucketType, Optional[MetricLogger]) -> Metrics.DelegatingHistogram

    """Obtains or creates a Histogram metric.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric
      bucket_type: A type of bucket used in a histogram. A subclass of
        apache_beam.utils.histogram.BucketType
      logger: MetricLogger for logging locally aggregated metric

    Returns:
      A Histogram object.
    """
    namespace = UserMetrics.get_namespace(namespace)
    return Metrics.DelegatingHistogram(
        MetricName(namespace, name), bucket_type, logger)

  class DelegatingHistogram(Histogram):
    """Metrics Histogram that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name, bucket_type, logger):
      # type: (MetricName, BucketType, Optional[MetricLogger]) -> None
      super(Metrics.DelegatingHistogram, self).__init__(metric_name)
      self.metric_name = metric_name
      self.cell_type = HistogramCellFactory(bucket_type)
      self.logger = logger
      self.updater = MetricUpdater(self.cell_type, self.metric_name)

    def update(self, value):
      # type: (object) -> None
      self.updater(value)
      if self.logger:
        self.logger.update(self.cell_type, self.metric_name, value)


class MetricLogger(object):
  """Simple object to locally aggregate and log metrics.

  This class is experimental. No backwards-compatibility guarantees.
  """
  def __init__(self):
    # type: () -> None
    self._metric = dict()  # type: Dict[MetricName, MetricCell]
    self._lock = threading.Lock()
    self._last_logging_millis = int(time.time() * 1000)
    self.minimum_logging_frequency_msec = 180000

  def update(self, cell_type, metric_name, value):
    # type: (Union[Type[MetricCell], MetricCellFactory], MetricName, object) -> None
    cell = self._get_metric_cell(cell_type, metric_name)
    cell.update(value)

  def _get_metric_cell(self, cell_type, metric_name):
    # type: (Union[Type[MetricCell], MetricCellFactory], MetricName) -> MetricCell
    with self._lock:
      if metric_name not in self._metric:
        self._metric[metric_name] = cell_type()
    return self._metric[metric_name]

  def log_metrics(self, reset_after_logging=False):
    # type: (bool) -> None
    if self._lock.acquire(False):
      try:
        current_millis = int(time.time() * 1000)
        if ((current_millis - self._last_logging_millis) >
            self.minimum_logging_frequency_msec):
          logging_metric_info = [
              '[Locally aggregated metrics since %s]' %
              datetime.datetime.fromtimestamp(
                  self._last_logging_millis / 1000.0)
          ]
          for name, cell in self._metric.items():
            logging_metric_info.append('%s: %s' % (name, cell.get_cumulative()))
          _LOGGER.info('\n'.join(logging_metric_info))
          if reset_after_logging:
            self._metric = dict()
          self._last_logging_millis = current_millis
      finally:
        self._lock.release()
