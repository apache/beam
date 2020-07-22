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

from __future__ import absolute_import

import inspect
from builtins import object

from apache_beam.metrics import cells
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metricbase import Counter
from apache_beam.metrics.metricbase import Distribution
from apache_beam.metrics.metricbase import Gauge
from apache_beam.metrics.metricbase import MetricName

__all__ = ['Metrics', 'MetricsFilter']


class Metrics(object):
  """Lets users create/access metric objects during pipeline execution."""
  @staticmethod
  def get_namespace(namespace):
    if inspect.isclass(namespace):
      return '{}.{}'.format(namespace.__module__, namespace.__name__)
    elif isinstance(namespace, str):
      return namespace
    else:
      raise ValueError('Unknown namespace type')

  @staticmethod
  def counter(namespace, name):
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

  class DelegatingCounter(Counter):
    """Metrics Counter that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      super(Metrics.DelegatingCounter, self).__init__()
      self.metric_name = metric_name
      self.inc = MetricUpdater(cells.CounterCell, metric_name, default=1)

  class DelegatingDistribution(Distribution):
    """Metrics Distribution Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      super(Metrics.DelegatingDistribution, self).__init__()
      self.metric_name = metric_name
      self.update = MetricUpdater(cells.DistributionCell, metric_name)

  class DelegatingGauge(Gauge):
    """Metrics Gauge that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name):
      super(Metrics.DelegatingGauge, self).__init__()
      self.metric_name = metric_name
      self.set = MetricUpdater(cells.GaugeCell, metric_name)


class MetricResults(object):
  COUNTERS = "counters"
  DISTRIBUTIONS = "distributions"
  GAUGES = "gauges"

  @staticmethod
  def _matches_name(filter, metric_key):
    if not filter.names and not filter.namespaces:
      return True

    if ((filter.namespaces and metric_key.metric.namespace in filter.namespaces)
        or (filter.names and metric_key.metric.name in filter.names)):
      return True
    return False

  @staticmethod
  def _is_sub_list(needle, haystack):
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
    """True iff the '/'-delimited pieces of filter_scope exist as a sub-list
    of the '/'-delimited pieces of actual_scope"""
    return MetricResults._is_sub_list(
        filter_scope.split('/'), actual_scope.split('/'))

  @staticmethod
  def _matches_scope(filter, metric_key):
    if not filter.steps:
      return True

    for step in filter.steps:
      if MetricResults._matches_sub_path(metric_key.step, step):
        return True

    return False

  @staticmethod
  def matches(filter, metric_key):
    if filter is None:
      return True

    if (MetricResults._matches_name(filter, metric_key) and
        MetricResults._matches_scope(filter, metric_key)):
      return True
    return False

  def query(self, filter=None):
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
    self._names = set()
    self._namespaces = set()
    self._steps = set()

  @property
  def steps(self):
    return frozenset(self._steps)

  @property
  def names(self):
    return frozenset(self._names)

  @property
  def namespaces(self):
    return frozenset(self._namespaces)

  def with_metric(self, metric):
    return (
        self.with_name(metric.metric_name.name).with_namespace(
            metric.metric_name.namespace))

  def with_name(self, name):
    return self.with_names([name])

  def with_names(self, names):
    if isinstance(names, str):
      raise ValueError('Names must be a collection, not a string')

    self._names.update(names)
    return self

  def with_namespace(self, namespace):
    return self.with_namespaces([namespace])

  def with_namespaces(self, namespaces):
    if isinstance(namespaces, str):
      raise ValueError('Namespaces must be an iterable, not a string')

    self._namespaces.update([Metrics.get_namespace(ns) for ns in namespaces])
    return self

  def with_step(self, step):
    return self.with_steps([step])

  def with_steps(self, steps):
    if isinstance(steps, str):
      raise ValueError('Steps must be an iterable, not a string')

    self._steps.update(steps)
    return self
