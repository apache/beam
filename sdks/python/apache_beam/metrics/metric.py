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

import logging
import re
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
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.execution import MetricUpdater
from apache_beam.metrics.metricbase import Counter
from apache_beam.metrics.metricbase import Distribution
from apache_beam.metrics.metricbase import Gauge
from apache_beam.metrics.metricbase import MetricName
from apache_beam.metrics.metricbase import StringSet

if TYPE_CHECKING:
  from apache_beam.metrics.execution import MetricKey
  from apache_beam.metrics.metricbase import Metric

__all__ = ['Metrics', 'MetricsFilter', 'Lineage']

_LOGGER = logging.getLogger(__name__)


class Metrics(object):
  """Lets users create/access metric objects during pipeline execution."""
  @staticmethod
  def get_namespace(namespace: Union[Type, str]) -> str:
    if isinstance(namespace, type):
      return '{}.{}'.format(namespace.__module__, namespace.__name__)
    elif isinstance(namespace, str):
      return namespace
    else:
      raise ValueError('Unknown namespace type')

  @staticmethod
  def counter(
      namespace: Union[Type, str], name: str) -> 'Metrics.DelegatingCounter':
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
  def distribution(
      namespace: Union[Type, str],
      name: str) -> 'Metrics.DelegatingDistribution':
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
  def gauge(
      namespace: Union[Type, str], name: str) -> 'Metrics.DelegatingGauge':
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
  def string_set(
      namespace: Union[Type, str], name: str) -> 'Metrics.DelegatingStringSet':
    """Obtains or creates a String set metric.

    String set metrics are restricted to string values.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A StringSet object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingStringSet(MetricName(namespace, name))

  class DelegatingCounter(Counter):
    """Metrics Counter that Delegates functionality to MetricsEnvironment."""
    def __init__(
        self, metric_name: MetricName, process_wide: bool = False) -> None:
      super().__init__(metric_name)
      self.inc = MetricUpdater(  # type: ignore[method-assign]
          cells.CounterCell,
          metric_name,
          default_value=1,
          process_wide=process_wide)

  class DelegatingDistribution(Distribution):
    """Metrics Distribution Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name: MetricName) -> None:
      super().__init__(metric_name)
      self.update = MetricUpdater(cells.DistributionCell, metric_name)  # type: ignore[method-assign]

  class DelegatingGauge(Gauge):
    """Metrics Gauge that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name: MetricName) -> None:
      super().__init__(metric_name)
      self.set = MetricUpdater(cells.GaugeCell, metric_name)  # type: ignore[method-assign]

  class DelegatingStringSet(StringSet):
    """Metrics StringSet that Delegates functionality to MetricsEnvironment."""
    def __init__(self, metric_name: MetricName) -> None:
      super().__init__(metric_name)
      self.add = MetricUpdater(cells.StringSetCell, metric_name)  # type: ignore[method-assign]


class MetricResults(object):
  COUNTERS = "counters"
  DISTRIBUTIONS = "distributions"
  GAUGES = "gauges"
  STRINGSETS = "string_sets"

  @staticmethod
  def _matches_name(filter: 'MetricsFilter', metric_key: 'MetricKey') -> bool:
    if ((filter.namespaces and
         metric_key.metric.namespace not in filter.namespaces) or
        (filter.names and metric_key.metric.name not in filter.names)):
      return False
    else:
      return True

  @staticmethod
  def _is_sub_list(needle: List[str], haystack: List[str]) -> bool:
    """True iff `needle` is a sub-list of `haystack` (i.e. a contiguous slice
    of `haystack` exactly matches `needle`"""
    needle_len = len(needle)
    haystack_len = len(haystack)
    for i in range(0, haystack_len - needle_len + 1):
      if haystack[i:i + needle_len] == needle:
        return True

    return False

  @staticmethod
  def _matches_sub_path(actual_scope: str, filter_scope: str) -> bool:
    """True iff the '/'-delimited pieces of filter_scope exist as a sub-list
    of the '/'-delimited pieces of actual_scope"""
    return bool(
        actual_scope and MetricResults._is_sub_list(
            filter_scope.split('/'), actual_scope.split('/')))

  @staticmethod
  def _matches_scope(filter: 'MetricsFilter', metric_key: 'MetricKey') -> bool:
    if not filter.steps:
      return True

    for step in filter.steps:
      if MetricResults._matches_sub_path(metric_key.step, step):
        return True

    return False

  @staticmethod
  def matches(
      filter: Optional['MetricsFilter'], metric_key: 'MetricKey') -> bool:
    if filter is None:
      return True

    if (MetricResults._matches_name(filter, metric_key) and
        MetricResults._matches_scope(filter, metric_key)):
      return True
    return False

  def query(
      self,
      filter: Optional['MetricsFilter'] = None
  ) -> Dict[str, List['MetricResult']]:
    """Queries the runner for existing user metrics that match the filter.

    It should return a dictionary, with lists of each kind of metric, and
    each list contains the corresponding kind of MetricResult. Like so:

        {
          "counters": [MetricResult(counter_key, committed, attempted), ...],
          "distributions": [MetricResult(dist_key, committed, attempted), ...],
          "gauges": [],  // Empty list if nothing matched the filter.
          "string_sets": [] [MetricResult(string_set_key, committed, attempted),
                            ...]
        }

    The committed / attempted values are DistributionResult / GaugeResult / int
    / set objects.
    """
    raise NotImplementedError


class MetricsFilter(object):
  """Simple object to filter metrics results.

  If filters by matching a result's step-namespace-name with three internal
  sets. No execution/matching logic is added to this object, so that it may
  be used to construct arguments as an RPC request. It is left for runners
  to implement matching logic by themselves.

  Note: This class only supports user defined metrics.
  """
  def __init__(self) -> None:
    self._names: Set[str] = set()
    self._namespaces: Set[str] = set()
    self._steps: Set[str] = set()

  @property
  def steps(self) -> FrozenSet[str]:
    return frozenset(self._steps)

  @property
  def names(self) -> FrozenSet[str]:
    return frozenset(self._names)

  @property
  def namespaces(self) -> FrozenSet[str]:
    return frozenset(self._namespaces)

  def with_metric(self, metric: 'Metric') -> 'MetricsFilter':
    name = metric.metric_name.name or ''
    namespace = metric.metric_name.namespace or ''
    return self.with_name(name).with_namespace(namespace)

  def with_name(self, name: str) -> 'MetricsFilter':
    return self.with_names([name])

  def with_names(self, names: Iterable[str]) -> 'MetricsFilter':
    if isinstance(names, str):
      raise ValueError('Names must be a collection, not a string')

    self._names.update(names)
    return self

  def with_namespace(self, namespace: Union[Type, str]) -> 'MetricsFilter':
    return self.with_namespaces([namespace])

  def with_namespaces(
      self, namespaces: Iterable[Union[Type, str]]) -> 'MetricsFilter':
    if isinstance(namespaces, str):
      raise ValueError('Namespaces must be an iterable, not a string')

    self._namespaces.update([Metrics.get_namespace(ns) for ns in namespaces])
    return self

  def with_step(self, step: str) -> 'MetricsFilter':
    return self.with_steps([step])

  def with_steps(self, steps: Iterable[str]) -> 'MetricsFilter':
    if isinstance(steps, str):
      raise ValueError('Steps must be an iterable, not a string')

    self._steps.update(steps)
    return self


class Lineage:
  """Standard collection of metrics used to record source and sinks information
  for lineage tracking."""

  LINEAGE_NAMESPACE = "lineage"
  SOURCE = "sources"
  SINK = "sinks"

  _METRICS = {
      SOURCE: Metrics.string_set(LINEAGE_NAMESPACE, SOURCE),
      SINK: Metrics.string_set(LINEAGE_NAMESPACE, SINK)
  }

  def __init__(self, label: str) -> None:
    """Create a Lineage with valid label (:data:`~Lineage.SOURCE` or
    :data:`~Lineage.SINK`)
    """
    self.metric = Lineage._METRICS[label]

  @classmethod
  def sources(cls) -> 'Lineage':
    return cls(Lineage.SOURCE)

  @classmethod
  def sinks(cls) -> 'Lineage':
    return cls(Lineage.SINK)

  _RESERVED_CHARS = re.compile(r'[:\s.]')

  @staticmethod
  def wrap_segment(segment: str) -> str:
    """Wrap segment to valid segment name.

    Specifically, If there are reserved chars (colon, whitespace, dot), escape
    with backtick. If the segment is already wrapped, return the original.
    """
    if segment.startswith("`") and segment.endswith("`"): return segment
    if Lineage._RESERVED_CHARS.search(segment):
      return "`" + segment + "`"
    return segment

  @staticmethod
  def get_fq_name(
      system: str, *segments: str, subtype: Optional[str] = None) -> str:
    """Assemble fully qualified name
    (`FQN <https://cloud.google.com/data-catalog/docs/fully-qualified-names>`_).
    Format:

    - `system:segment1.segment2`
    - `system:subtype:segment1.segment2`
    - `system:`segment1.with.dots:colons`.segment2`

    This helper method is for internal and testing usage only.
    """
    segs = '.'.join(map(Lineage.wrap_segment, segments))
    if subtype:
      return ':'.join((system, subtype, segs))
    return ':'.join((system, segs))

  def add(
      self, system: str, *segments: str, subtype: Optional[str] = None) -> None:
    """
    Adds the given details as Lineage.

    For asset level lineage the resource location should be specified as
    Dataplex FQN, see
    https://cloud.google.com/data-catalog/docs/fully-qualified-names

    Example of adding FQN components:

    - `add("system", "segment1", "segment2")`
    - `add("system", "segment1", "segment2", subtype="subtype")`

    Example of adding a FQN:

    - `add("system:segment1.segment2")`
    - `add("system:subtype:segment1.segment2")`

    The first positional argument serves as system, if full segments are
    provided, or the full FQN if it is provided as a single argument.
    """
    system_or_details = system
    if len(segments) == 0 and subtype is None:
      self.metric.add(system_or_details)
    else:
      self.metric.add(self.get_fq_name(system, *segments, subtype=subtype))

  @staticmethod
  def query(results: MetricResults, label: str) -> Set[str]:
    if not label in Lineage._METRICS:
      raise ValueError("Label {} does not exist for Lineage", label)
    response = results.query(
        MetricsFilter().with_namespace(Lineage.LINEAGE_NAMESPACE).with_name(
            label))[MetricResults.STRINGSETS]
    result = set()
    for metric in response:
      result.update(metric.committed)
      result.update(metric.attempted)
    return result
