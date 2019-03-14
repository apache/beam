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

from __future__ import absolute_import

import threading
from builtins import object
from collections import defaultdict

from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.monitoring_infos import user_metric_urn
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import statesampler


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
    self.labels = labels if labels else dict()

  def __eq__(self, other):
    return (self.step == other.step and
            self.metric == other.metric and
            self.labels == other.labels)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

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
    return (self.key == other.key and
            self.committed == other.committed and
            self.attempted == other.attempted)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

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
  def __init__(self):
    self.METRICS_SUPPORTED = False
    self._METRICS_SUPPORTED_LOCK = threading.Lock()

  def set_metrics_supported(self, supported):
    with self._METRICS_SUPPORTED_LOCK:
      self.METRICS_SUPPORTED = supported

  def current_container(self):
    """Returns the current MetricsContainer."""
    sampler = statesampler.get_current_tracker()
    if sampler is None:
      return None
    return sampler.current_state().metrics_container


MetricsEnvironment = _MetricsEnvironment()


class MetricsContainer(object):
  """Holds the metrics of a single step and a single bundle."""
  def __init__(self, step_name):
    self.step_name = step_name
    self.counters = defaultdict(lambda: CounterCell())
    self.distributions = defaultdict(lambda: DistributionCell())
    self.gauges = defaultdict(lambda: GaugeCell())

  def get_counter(self, metric_name):
    return self.counters[metric_name]

  def get_distribution(self, metric_name):
    return self.distributions[metric_name]

  def get_gauge(self, metric_name):
    return self.gauges[metric_name]

  def _get_updates(self, filter=None):
    """Return cumulative values of metrics filtered according to a lambda.

    This returns all the cumulative values for all metrics after filtering
    then with the filter parameter lambda function. If None is passed in,
    then cumulative values for all metrics are returned.
    """
    if filter is None:
      filter = lambda v: True
    counters = {MetricKey(self.step_name, k): v.get_cumulative()
                for k, v in self.counters.items()
                if filter(v)}

    distributions = {MetricKey(self.step_name, k): v.get_cumulative()
                     for k, v in self.distributions.items()
                     if filter(v)}

    gauges = {MetricKey(self.step_name, k): v.get_cumulative()
              for k, v in self.gauges.items()
              if filter(v)}

    return MetricUpdates(counters, distributions, gauges)

  def get_updates(self):
    """Return cumulative values of metrics that changed since the last commit.

    This returns all the cumulative values for all metrics only if their state
    prior to the function call was COMMITTING or DIRTY.
    """
    return self._get_updates(filter=lambda v: v.commit.before_commit())

  def get_cumulative(self):
    """Return MetricUpdates with cumulative values of all metrics in container.

    This returns all the cumulative values for all metrics regardless of whether
    they have been committed or not.
    """
    return self._get_updates()

  def to_runner_api(self):
    return (
        [beam_fn_api_pb2.Metrics.User(
            metric_name=k.to_runner_api(),
            counter_data=beam_fn_api_pb2.Metrics.User.CounterData(
                value=v.get_cumulative()))
         for k, v in self.counters.items()] +
        [beam_fn_api_pb2.Metrics.User(
            metric_name=k.to_runner_api(),
            distribution_data=v.get_cumulative().to_runner_api())
         for k, v in self.distributions.items()] +
        [beam_fn_api_pb2.Metrics.User(
            metric_name=k.to_runner_api(),
            gauge_data=v.get_cumulative().to_runner_api())
         for k, v in self.gauges.items()]
    )

  def to_runner_api_monitoring_infos(self, transform_id):
    """Returns a list of MonitoringInfos for the metrics in this container."""
    all_user_metrics = []
    for k, v in self.counters.items():
      all_user_metrics.append(monitoring_infos.int64_counter(
          user_metric_urn(k.namespace, k.name),
          v.to_runner_api_monitoring_info(),
          ptransform=transform_id
      ))

    for k, v in self.distributions.items():
      all_user_metrics.append(monitoring_infos.int64_distribution(
          user_metric_urn(k.namespace, k.name),
          v.get_cumulative().to_runner_api_monitoring_info(),
          ptransform=transform_id
      ))

    for k, v in self.gauges.items():
      all_user_metrics.append(monitoring_infos.int64_gauge(
          user_metric_urn(k.namespace, k.name),
          v.get_cumulative().to_runner_api_monitoring_info(),
          ptransform=transform_id
      ))
    return {monitoring_infos.to_key(mi) : mi for mi in all_user_metrics}

  def reset(self):
    for counter in self.counters.values():
      counter.reset()
    for distribution in self.distributions.values():
      distribution.reset()
    for gauge in self.gauges.values():
      gauge.reset()


class MetricUpdates(object):
  """Contains updates for several metrics.

  A metric update is an object containing information to update a metric.
  For Distribution metrics, it is DistributionData, and for Counter metrics,
  it's an int.
  """
  def __init__(self, counters=None, distributions=None, gauges=None):
    """Create a MetricUpdates object.

    Args:
      counters: Dictionary of MetricKey:MetricUpdate updates.
      distributions: Dictionary of MetricKey:MetricUpdate objects.
      gauges: Dictionary of MetricKey:MetricUpdate objects.
    """
    self.counters = counters or {}
    self.distributions = distributions or {}
    self.gauges = gauges or {}
