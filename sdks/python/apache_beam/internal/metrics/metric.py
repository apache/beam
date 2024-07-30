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

import datetime
import logging
import threading
import time
from typing import TYPE_CHECKING
from typing import Dict
from typing import Optional
from typing import Type
from typing import Union

from apache_beam.internal.metrics.cells import HistogramCellFactory
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metric import Metrics as UserMetrics
from apache_beam.metrics.metricbase import Histogram
from apache_beam.metrics.metricbase import MetricName

if TYPE_CHECKING:
  from apache_beam.metrics.cells import MetricCell
  from apache_beam.metrics.cells import MetricCellFactory
  from apache_beam.utils.histogram import BucketType

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  pass

__all__ = ['Metrics']

_LOGGER = logging.getLogger(__name__)


class Metrics(object):
  @staticmethod
  def counter(
      urn: str,
      labels: Optional[Dict[str, str]] = None,
      process_wide: bool = False) -> UserMetrics.DelegatingCounter:
    """Obtains or creates a Counter metric.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric
      urn: URN to populate on a MonitoringInfo, when sending to RunnerHarness.
      labels: Labels to populate on a MonitoringInfo
      process_wide: Whether or not the metric is specific to the current bundle
          or should be calculated for the entire process.

    Returns:
      A Counter object.
    """
    return UserMetrics.DelegatingCounter(
        MetricName(namespace=None, name=None, urn=urn, labels=labels),
        process_wide=process_wide)

  @staticmethod
  def histogram(
      namespace: Union[Type, str],
      name: str,
      bucket_type: 'BucketType',
      logger: Optional['MetricLogger'] = None) -> 'Metrics.DelegatingHistogram':
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
    def __init__(
        self,
        metric_name: MetricName,
        bucket_type: 'BucketType',
        logger: Optional['MetricLogger']) -> None:
      super().__init__(metric_name)
      self.metric_name = metric_name
      self.cell_type = HistogramCellFactory(bucket_type)
      self.logger = logger
      self.updater = MetricUpdater(self.cell_type, self.metric_name)

    def update(self, value: object) -> None:
      self.updater(value)
      if self.logger:
        self.logger.update(self.cell_type, self.metric_name, value)


class MetricLogger(object):
  """Simple object to locally aggregate and log metrics."""
  def __init__(self) -> None:
    self._metric: Dict[MetricName, 'MetricCell'] = {}
    self._lock = threading.Lock()
    self._last_logging_millis = int(time.time() * 1000)
    self.minimum_logging_frequency_msec = 180000

  def update(
      self,
      cell_type: Union[Type['MetricCell'], 'MetricCellFactory'],
      metric_name: MetricName,
      value: object) -> None:
    cell = self._get_metric_cell(cell_type, metric_name)
    cell.update(value)

  def _get_metric_cell(
      self,
      cell_type: Union[Type['MetricCell'], 'MetricCellFactory'],
      metric_name: MetricName) -> 'MetricCell':
    with self._lock:
      if metric_name not in self._metric:
        self._metric[metric_name] = cell_type()
    return self._metric[metric_name]

  def log_metrics(self, reset_after_logging: bool = False) -> None:
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
            self._metric = {}
          self._last_logging_millis = current_millis
      finally:
        self._lock.release()


class ServiceCallMetric(object):
  """Metric class which records Service API call metrics.

  This class will capture a request count metric for the specified
  request_count_urn and base_labels.

  When call() is invoked the status must be provided, which will
  be converted to a canonical GCP status code, if possible.

  TODO(ajamato): Add Request latency metric.
  """
  def __init__(
      self,
      request_count_urn: str,
      base_labels: Optional[Dict[str, str]] = None) -> None:
    self.base_labels = base_labels if base_labels else {}
    self.request_count_urn = request_count_urn

  def call(self, status: Union[int, str, 'HttpError']) -> None:
    """Record the status of the call into appropriate metrics."""
    canonical_status = self.convert_to_canonical_status_string(status)
    additional_labels = {monitoring_infos.STATUS_LABEL: canonical_status}

    labels = dict(
        list(self.base_labels.items()) + list(additional_labels.items()))

    request_counter = Metrics.counter(
        urn=self.request_count_urn, labels=labels, process_wide=True)
    request_counter.inc()

  def convert_to_canonical_status_string(
      self, status: Union[int, str, 'HttpError']) -> str:
    """Converts a status to a canonical GCP status cdoe string."""
    http_status_code = None
    if isinstance(status, int):
      http_status_code = status
    elif isinstance(status, str):
      return status.lower()
    elif isinstance(status, HttpError):
      http_status_code = int(status.status_code)
    http_to_canonical_gcp_status = {
        200: 'ok',
        400: 'out_of_range',
        401: 'unauthenticated',
        403: 'permission_denied',
        404: 'not_found',
        409: 'already_exists',
        429: 'resource_exhausted',
        499: 'cancelled',
        500: 'internal',
        501: 'not_implemented',
        503: 'unavailable',
        504: 'deadline_exceeded'
    }
    if (http_status_code is not None and
        http_status_code in http_to_canonical_gcp_status):
      return http_to_canonical_gcp_status[http_status_code]
    return str(http_status_code)

  @staticmethod
  def bigtable_error_code_to_grpc_status_string(
      grpc_status_code: Optional[int]) -> str:
    """
    Converts the bigtable error code to a canonical GCP status code string.

    This Bigtable client library is not using the canonical http status code
    values (i.e. https://cloud.google.com/apis/design/errors)"
    Instead they are numbered using an enum with these values corresponding
    to each status code: https://cloud.google.com/bigtable/docs/status-codes

    Args:
      grpc_status_code: An int that corresponds to an enum of status codes

    Returns:
      A GCP status code string
    """
    grpc_to_canonical_gcp_status = {
        0: 'ok',
        1: 'cancelled',
        2: 'unknown',
        3: 'invalid_argument',
        4: 'deadline_exceeded',
        5: 'not_found',
        6: 'already_exists',
        7: 'permission_denied',
        8: 'resource_exhausted',
        9: 'failed_precondition',
        10: 'aborted',
        11: 'out_of_range',
        12: 'unimplemented',
        13: 'internal',
        14: 'unavailable'
    }
    if grpc_status_code is None:
      # Bigtable indicates this can be retried but itself has exhausted retry
      # timeout or there is no retry policy set for bigtable.
      return grpc_to_canonical_gcp_status[4]
    return grpc_to_canonical_gcp_status.get(
        grpc_status_code, str(grpc_status_code))
