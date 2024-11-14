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
This module is for internal use only; no backwards-compatibility guarantees.

The classes in this file keep shared state, and organize metrics information.

Available classes:

- MetricKey - Internal key for a metric.
- MetricResult - Current status of a metric's updates/commits.
- _MetricsEnvironment - Keeps track of MetricsContainer and other metrics
    information for every single execution working thread.
- MetricsContainer - Holds the metrics of a single step and a single
    unit-of-commit (bundle).
"""

# pytype: skip-file

import threading
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import Union
from typing import cast

from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.cells import StringSetCell
from apache_beam.metrics.cells import StringSetData
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.statesampler import get_current_tracker

if TYPE_CHECKING:
  from apache_beam.metrics.cells import GaugeData
  from apache_beam.metrics.cells import DistributionData
  from apache_beam.metrics.cells import MetricCell
  from apache_beam.metrics.cells import MetricCellFactory
  from apache_beam.metrics.metricbase import MetricName
  from apache_beam.portability.api import metrics_pb2


class MetricKey(object):
  """Key used to identify instance of metric cell.

  Metrics are internally keyed by the name of the step they're associated with,
  the name and namespace (if it is a user defined metric) of the metric,
  and any extra label metadata added by the runner specific metric collection
  service.
  """
  def __init__(self, step, metric, labels=None):
    """Initializes ``MetricKey``.

    Args:
      step: A string with the step this metric cell is part of.
      metric: A ``MetricName`` namespace+name that identifies a metric.
      labels: An arbitrary set of labels that also identifies the metric.
    """
    self.step = step
    self.metric = metric
    self.labels = labels if labels else {}

  def __eq__(self, other):
    return (
        self.step == other.step and self.metric == other.metric and
        self.labels == other.labels)

  def __hash__(self):
    return hash((self.step, self.metric, frozenset(self.labels)))

  def __repr__(self):
    return 'MetricKey(step={}, metric={}, labels={})'.format(
        self.step, self.metric, self.labels)


class MetricResult(object):
  """Keeps track of the status of a metric within a single bundle.

  It contains the physical and logical updates to the metric. Physical updates
  are updates that have not necessarily been committed, but that have been made
  during pipeline execution. Logical updates are updates that have been
  committed.

  Attributes:
    key: A ``MetricKey`` that identifies the metric and bundle of this result.
    committed: The committed updates of the metric. This attribute's type is
      of metric type result (e.g. int, DistributionResult, GaugeResult).
    attempted: The logical updates of the metric. This attribute's type is that
      of metric type result (e.g. int, DistributionResult, GaugeResult).
  """
  def __init__(self, key, committed, attempted):
    """Initializes ``MetricResult``.
    Args:
      key: A ``MetricKey`` object.
      committed: Metric data that has been committed (e.g. logical updates)
      attempted: Metric data that has been attempted (e.g. physical updates)
    """
    self.key = key
    self.committed = committed
    self.attempted = attempted

  def __eq__(self, other):
    return (
        self.key == other.key and self.committed == other.committed and
        self.attempted == other.attempted)

  def __hash__(self):
    return hash((self.key, self.committed, self.attempted))

  def __repr__(self):
    return 'MetricResult(key={}, committed={}, attempted={})'.format(
        self.key, str(self.committed), str(self.attempted))

  def __str__(self):
    return repr(self)

  @property
  def result(self):
    """Short-hand for falling back to attempted metrics if it seems that
    committed was not populated (e.g. due to not being supported on a given
    runner"""
    return self.committed if self.committed else self.attempted


class _MetricsEnvironment(object):
  """Holds the MetricsContainer for every thread and other metric information.

  This class is not meant to be instantiated, instead being used to keep
  track of global state.
  """
  def current_container(self):
    """Returns the current MetricsContainer."""
    sampler = statesampler.get_current_tracker()
    if sampler is None:
      return None
    return sampler.current_state().metrics_container

  def process_wide_container(self):
    """Returns the MetricsContainer for process wide metrics, e.g. memory."""
    return PROCESS_WIDE_METRICS_CONTAINER


MetricsEnvironment = _MetricsEnvironment()


class _TypedMetricName(object):
  """Like MetricName, but also stores the cell type of the metric."""
  def __init__(
      self,
      cell_type,  # type: Union[type[MetricCell], MetricCellFactory]
      metric_name  # type: Union[str, MetricName]
  ):
    # type: (...) -> None
    self.cell_type = cell_type
    self.metric_name = metric_name
    if isinstance(metric_name, str):
      self.fast_name = metric_name
    else:
      self.fast_name = metric_name.fast_name()
    # Cached for speed, as this is used as a key for every counter update.
    self._hash = hash((cell_type, self.fast_name))

  def __eq__(self, other):
    return self is other or (
        self.cell_type == other.cell_type and self.fast_name == other.fast_name)

  def __hash__(self):
    return self._hash

  def __str__(self):
    return '%s %s' % (self.cell_type, self.metric_name)

  def __reduce__(self):
    return _TypedMetricName, (self.cell_type, self.metric_name)


_DEFAULT = None  # type: Any


class MetricUpdater(object):
  """A callable that updates the metric as quickly as possible."""
  def __init__(
      self,
      cell_type,  # type: Union[type[MetricCell], MetricCellFactory]
      metric_name,  # type: Union[str, MetricName]
      default_value=None,
      process_wide=False):
    self.process_wide = process_wide
    self.typed_metric_name = _TypedMetricName(cell_type, metric_name)
    self.default_value = default_value

  def __call__(self, value=_DEFAULT):
    # type: (Any) -> None
    if value is _DEFAULT:
      if self.default_value is _DEFAULT:
        raise ValueError(
            'Missing value for update of %s' % self.typed_metric_name.fast_name)
      value = self.default_value
    if self.process_wide:
      MetricsEnvironment.process_wide_container().get_metric_cell(
          self.typed_metric_name).update(value)
    else:
      tracker = get_current_tracker()
      if tracker is not None:
        tracker.update_metric(self.typed_metric_name, value)

  def __reduce__(self):
    return MetricUpdater, (
        self.typed_metric_name.cell_type,
        self.typed_metric_name.metric_name,
        self.default_value)


class MetricsContainer(object):
  """Holds the metrics of a single step and a single bundle.

  Or the metrics associated with the process/SDK harness. I.e. memory usage.
  """
  def __init__(self, step_name):
    self.step_name = step_name
    self.lock = threading.Lock()
    self.metrics = {}  # type: dict[_TypedMetricName, MetricCell]

  def get_counter(self, metric_name):
    # type: (MetricName) -> CounterCell
    return cast(
        CounterCell,
        self.get_metric_cell(_TypedMetricName(CounterCell, metric_name)))

  def get_distribution(self, metric_name):
    # type: (MetricName) -> DistributionCell
    return cast(
        DistributionCell,
        self.get_metric_cell(_TypedMetricName(DistributionCell, metric_name)))

  def get_gauge(self, metric_name):
    # type: (MetricName) -> GaugeCell
    return cast(
        GaugeCell,
        self.get_metric_cell(_TypedMetricName(GaugeCell, metric_name)))

  def get_string_set(self, metric_name):
    # type: (MetricName) -> StringSetCell
    return cast(
        StringSetCell,
        self.get_metric_cell(_TypedMetricName(StringSetCell, metric_name)))

  def get_metric_cell(self, typed_metric_name):
    # type: (_TypedMetricName) -> MetricCell
    cell = self.metrics.get(typed_metric_name, None)
    if cell is None:
      with self.lock:
        cell = self.metrics[typed_metric_name] = typed_metric_name.cell_type()
    return cell

  def get_cumulative(self):
    # type: () -> MetricUpdates

    """Return MetricUpdates with cumulative values of all metrics in container.

    This returns all the cumulative values for all metrics.
    """
    counters = {
        MetricKey(self.step_name, k.metric_name): v.get_cumulative()
        for k,
        v in self.metrics.items() if k.cell_type == CounterCell
    }

    distributions = {
        MetricKey(self.step_name, k.metric_name): v.get_cumulative()
        for k,
        v in self.metrics.items() if k.cell_type == DistributionCell
    }

    gauges = {
        MetricKey(self.step_name, k.metric_name): v.get_cumulative()
        for k,
        v in self.metrics.items() if k.cell_type == GaugeCell
    }

    string_sets = {
        MetricKey(self.step_name, k.metric_name): v.get_cumulative()
        for k,
        v in self.metrics.items() if k.cell_type == StringSetCell
    }

    return MetricUpdates(counters, distributions, gauges, string_sets)

  def to_runner_api(self):
    return [
        cell.to_runner_api_user_metric(key.metric_name) for key,
        cell in self.metrics.items()
    ]

  def to_runner_api_monitoring_infos(self, transform_id):
    # type: (str) -> dict[frozenset, metrics_pb2.MonitoringInfo]

    """Returns a list of MonitoringInfos for the metrics in this container."""
    with self.lock:
      items = list(self.metrics.items())
    all_metrics = [
        cell.to_runner_api_monitoring_info(key.metric_name, transform_id)
        for key,
        cell in items
    ]
    return {
        monitoring_infos.to_key(mi): mi
        for mi in all_metrics if mi is not None
    }

  def reset(self):
    # type: () -> None

    """Reset all metrics in the MetricsContainer. This does not delete added
    metrics.
    """

    for metric in self.metrics.values():
      metric.reset()

  def __reduce__(self):
    raise NotImplementedError


PROCESS_WIDE_METRICS_CONTAINER = MetricsContainer(None)


class MetricUpdates(object):
  """Contains updates for several metrics.

  A metric update is an object containing information to update a metric.
  For Distribution metrics, it is DistributionData, and for Counter metrics,
  it's an int.
  """
  def __init__(
      self,
      counters=None,  # type: Optional[dict[MetricKey, int]]
      distributions=None,  # type: Optional[dict[MetricKey, DistributionData]]
      gauges=None,  # type: Optional[dict[MetricKey, GaugeData]]
      string_sets=None,  # type: Optional[dict[MetricKey, StringSetData]]
  ):
    # type: (...) -> None

    """Create a MetricUpdates object.

    Args:
      counters: Dictionary of MetricKey:MetricUpdate updates.
      distributions: Dictionary of MetricKey:MetricUpdate objects.
      gauges: Dictionary of MetricKey:MetricUpdate objects.
      string_sets: Dictionary of MetricKey:MetricUpdate objects.
    """
    self.counters = counters or {}
    self.distributions = distributions or {}
    self.gauges = gauges or {}
    self.string_sets = string_sets or {}
