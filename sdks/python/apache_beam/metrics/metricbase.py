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
The classes in this file are interfaces for metrics. They are not intended
to be subclassed or created directly by users. To work with and access metrics,
users should use the classes and methods exposed in metric.py.

Available classes:

- Metric - Base interface of a metrics object.
- Counter - Counter metric interface. Allows a count to be incremented or
    decremented during pipeline execution.
- Distribution - Distribution Metric interface. Allows statistics about the
    distribution of a variable to be collected during pipeline execution.
- Gauge - Gauge Metric interface. Allows to track the latest value of a
    variable during pipeline execution.
- MetricName - Namespace and name used to refer to a Metric.
"""

# pytype: skip-file

from typing import Optional

__all__ = [
    'Metric',
    'Counter',
    'Distribution',
    'Gauge',
    'StringSet',
    'Histogram',
    'MetricName'
]


class MetricName(object):
  """The name of a metric.

  The name of a metric consists of a namespace and a name. The namespace
  allows grouping related metrics together and also prevents collisions
  between multiple metrics of the same name.
  """
  def __init__(
      self,
      namespace: Optional[str],
      name: Optional[str],
      urn: Optional[str] = None,
      labels: Optional[dict[str, str]] = None) -> None:
    """Initializes ``MetricName``.

    Note: namespace and name should be set for user metrics,
    urn and labels should be set for an arbitrary metric to package into a
    MonitoringInfo.

    Args:
      namespace: A string with the namespace of a metric.
      name: A string with the name of a metric.
      urn: URN to populate on a MonitoringInfo, when sending to RunnerHarness.
      labels: Labels to populate on a MonitoringInfo
    """
    if not urn:
      if not namespace:
        raise ValueError('Metric namespace must be non-empty')
      if not name:
        raise ValueError('Metric name must be non-empty')
    self.namespace = namespace
    self.name = name
    self.urn = urn
    self.labels = labels if labels else {}

  def __eq__(self, other):
    return (
        self.namespace == other.namespace and self.name == other.name and
        self.urn == other.urn and self.labels == other.labels)

  def __str__(self):
    if self.urn:
      return 'MetricName(namespace={}, name={}, urn={}, labels={})'.format(
          self.namespace, self.name, self.urn, self.labels)
    else:  # User counter case.
      return 'MetricName(namespace={}, name={})'.format(
          self.namespace, self.name)

  def __hash__(self):
    return hash((self.namespace, self.name, self.urn) +
                tuple(self.labels.items()))

  def fast_name(self):
    name = self.name or ''
    namespace = self.namespace or ''
    urn = self.urn or ''
    labels = ''
    if self.labels:
      labels = '_'.join(['%s=%s' % (k, v) for (k, v) in self.labels.items()])
    return '%d_%s%s%s%s' % (len(name), name, namespace, urn, labels)


class Metric(object):
  """Base interface of a metric object."""
  def __init__(self, metric_name: MetricName) -> None:
    self.metric_name = metric_name


class Counter(Metric):
  """Counter metric interface. Allows a count to be incremented/decremented
  during pipeline execution."""
  def inc(self, n=1):
    raise NotImplementedError

  def dec(self, n=1):
    self.inc(-n)


class Distribution(Metric):
  """Distribution Metric interface.

  Allows statistics about the distribution of a variable to be collected during
  pipeline execution."""
  def update(self, value):
    raise NotImplementedError


class Gauge(Metric):
  """Gauge Metric interface.

  Allows tracking of the latest value of a variable during pipeline
  execution."""
  def set(self, value):
    raise NotImplementedError


class StringSet(Metric):
  """StringSet Metric interface.

  Reports set of unique string values during pipeline execution.."""
  def add(self, value):
    raise NotImplementedError


class Histogram(Metric):
  """Histogram Metric interface.

  Allows statistics about the percentile of a variable to be collected during
  pipeline execution."""
  def update(self, value):
    raise NotImplementedError
