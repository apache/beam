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
User-facing classes for Metrics API.

The classes in this file allow users to define and use metrics to be collected
and displayed as part of their pipeline execution.

- Metrics - This class lets pipeline and transform writers create and access
    metric objects such as counters, distributions, etc.
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
from typing import FrozenSet
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Type
from typing import Union

from apache_beam.metrics import cells
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metricbase import Counter
from apache_beam.metrics.metricbase import Distribution
from apache_beam.metrics.metricbase import Gauge
from apache_beam.metrics.metricbase import Histogram
from apache_beam.metrics.metricbase import MetricName

if TYPE_CHECKING:
  from apache_beam.metrics.cells import MetricCell
  from apache_beam.metrics.cells import MetricCellFactory
  from apache_beam.metrics.execution import MetricKey
  from apache_beam.metrics.metricbase import Metric
  from apache_beam.utils.histogram import BucketType

__all__ = ['Metrics', 'MetricsFilter']

_LOGGER = logging.getLogger(__name__)


class Metrics(object):
  """Lets users create/access metric objects during pipeline execution."""
  @staticmethod
  def get_namespace(namespace):
    # type: (Union[Type, str]) -> str
    if isinstance(namespace, type):
      return '{}.{}'.format(namespace.__module__, namespace.__name__)
    elif isinstance(namespace, str):
      return namespace
    else:
      raise ValueError('Unknown namespace type')

  @staticmethod
  def counter(namespace, name):
    # type: (Union[Type, str], str) -> Metrics.DelegatingCounter

    """Obtains or creates a Counter metric.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A Counter object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingCounter(MetricName(namespace, name))

  @staticmethod
  def distribution(namespace, name):
    # type: (Union[Type, str], str) -> Metrics.DelegatingDistribution

    """Obtains or creates a Distribution metric.

    Distribution metrics are restricted to integer-only distributions.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A Distribution object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingDistribution(MetricName(namespace, name))

  @staticmethod
  def gauge(namespace, name):
    # type: (Union[Type, str], str) -> Metrics.DelegatingGauge

    """Obtains or creates a Gauge metric.

    Gauge metrics are restricted to integer-only values.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A Distribution object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingGauge(MetricName(namespace, name))

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
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingHistogram(
        MetricName(namespace, name), bucket_type, logger)

  class DelegatingCounter(Counter):
    """Metrics Counter that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      # type: (MetricName) -> None
      super(Metrics.DelegatingCounter, self).__init__(metric_name)
      self.inc = MetricUpdater(cells.CounterCell, metric_name, default=1)  # type: ignore[assignment]

  class DelegatingDistribution(Distribution):
    """Metrics Distribution Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      # type: (MetricName) -> None
      super(Metrics.DelegatingDistribution, self).__init__(metric_name)
      self.update = MetricUpdater(cells.DistributionCell, metric_name)  # type: ignore[assignment]

  class DelegatingGauge(Gauge):
    """Metrics Gauge that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      # type: (MetricName) -> None
      super(Metrics.DelegatingGauge, self).__init__(metric_name)
      self.set = MetricUpdater(cells.GaugeCell, metric_name)  # type: ignore[assignment]

  class DelegatingHistogram(Histogram):
    """Metrics Histogram that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name, bucket_type, logger):
      # type: (MetricName, BucketType, Optional[MetricLogger]) -> None
      super(Metrics.DelegatingHistogram, self).__init__(metric_name)
      self.metric_name = metric_name
      self.cell_type = cells.HistogramCellFactory(bucket_type)
      self.logger = logger
      self.updater = MetricUpdater(self.cell_type, self.metric_name)

    def update(self, value):
      # type: (object) -> None
      self.updater(value)
      if self.logger:
        self.logger.update(self.cell_type, self.metric_name, value)


class MetricResults(object):
  COUNTERS = "counters"
  DISTRIBUTIONS = "distributions"
  GAUGES = "gauges"

  @staticmethod
  def _matches_name(filter, metric_key):
    # type: (MetricsFilter, MetricKey) -> bool
    if ((filter.namespaces and
         metric_key.metric.namespace not in filter.namespaces) or
        (filter.names and metric_key.metric.name not in filter.names)):
      return False
    else:
      return True

  @staticmethod
  def _is_sub_list(needle, haystack):
    # type: (List[str], List[str]) -> bool

    """True iff `needle` is a sub-list of `haystack` (i.e. a contiguous slice
    of `haystack` exactly matches `needle`"""
    needle_len = len(needle)
    haystack_len = len(haystack)
    for i in range(0, haystack_len - needle_len + 1):
      if haystack[i:i + needle_len] == needle:
        return True

    return False

  @staticmethod
  def _matches_sub_path(actual_scope, filter_scope):
    # type: (str, str) -> bool

    """True iff the '/'-delimited pieces of filter_scope exist as a sub-list
    of the '/'-delimited pieces of actual_scope"""
    return MetricResults._is_sub_list(
        filter_scope.split('/'), actual_scope.split('/'))

  @staticmethod
  def _matches_scope(filter, metric_key):
    # type: (MetricsFilter, MetricKey) -> bool
    if not filter.steps:
      return True

    for step in filter.steps:
      if MetricResults._matches_sub_path(metric_key.step, step):
        return True

    return False

  @staticmethod
  def matches(filter, metric_key):
    # type: (Optional[MetricsFilter], MetricKey) -> bool
    if filter is None:
      return True

    if (MetricResults._matches_name(filter, metric_key) and
        MetricResults._matches_scope(filter, metric_key)):
      return True
    return False

  def query(self, filter=None):
    # type: (Optional[MetricsFilter]) -> Dict[str, List[MetricResults]]

    """Queries the runner for existing user metrics that match the filter.

    It should return a dictionary, with lists of each kind of metric, and
    each list contains the corresponding kind of MetricResult. Like so:

        {
          "counters": [MetricResult(counter_key, committed, attempted), ...],
          "distributions": [MetricResult(dist_key, committed, attempted), ...],
          "gauges": []  // Empty list if nothing matched the filter.
        }

    The committed / attempted values are DistributionResult / GaugeResult / int
    objects.
    """
    raise NotImplementedError


class MetricsFilter(object):
  """Simple object to filter metrics results.

  This class is experimental. No backwards-compatibility guarantees.

  If filters by matching a result's step-namespace-name with three internal
  sets. No execution/matching logic is added to this object, so that it may
  be used to construct arguments as an RPC request. It is left for runners
  to implement matching logic by themselves.
  """
  def __init__(self):
    # type: () -> None
    self._names = set()  # type: Set[str]
    self._namespaces = set()  # type: Set[str]
    self._steps = set()  # type: Set[str]

  @property
  def steps(self):
    # type: () -> FrozenSet[str]
    return frozenset(self._steps)

  @property
  def names(self):
    # type: () -> FrozenSet[str]
    return frozenset(self._names)

  @property
  def namespaces(self):
    # type: () -> FrozenSet[str]
    return frozenset(self._namespaces)

  def with_metric(self, metric):
    # type: (Metric) -> MetricsFilter
    return (
        self.with_name(metric.metric_name.name).with_namespace(
            metric.metric_name.namespace))

  def with_name(self, name):
    # type: (str) -> MetricsFilter
    return self.with_names([name])

  def with_names(self, names):
    # type: (Iterable[str]) -> MetricsFilter
    if isinstance(names, str):
      raise ValueError('Names must be a collection, not a string')

    self._names.update(names)
    return self

  def with_namespace(self, namespace):
    # type: (Union[Type, str]) -> MetricsFilter
    return self.with_namespaces([namespace])

  def with_namespaces(self, namespaces):
    # type: (Iterable[Union[Type, str]]) -> MetricsFilter
    if isinstance(namespaces, str):
      raise ValueError('Namespaces must be an iterable, not a string')

    self._namespaces.update([Metrics.get_namespace(ns) for ns in namespaces])
    return self

  def with_step(self, step):
    # type: (str) -> MetricsFilter
    return self.with_steps([step])

  def with_steps(self, steps):
    # type: (Iterable[str]) -> MetricsFilter
    if isinstance(steps, str):
      raise ValueError('Steps must be an iterable, not a string')

    self._steps.update(steps)
    return self


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
      if not metric_name in self._metric:
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
