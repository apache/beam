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
from collections import defaultdict

from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.metricbase import MetricName
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import statesampler


class MetricsContextException(Exception):
  """statesampler is not set up to provide global context.

  Metrics relies on statesampler to provide its global context. If any metrics
  operation is attempted without the global context set up, this exception will
  be thrown.
  """
  pass


class MetricKey(object):
  """Key used to identify instance of metric cell.

  Metrics are internally keyed by the step name they associated with and
  the name of the metric.
  """
  def __init__(self, step, metric):
    """Initializes ``MetricKey``.

    Args:
      step: A string with the step this metric cell is part of.
      metric: A ``MetricName`` that identifies a metric.
    """
    self.step = step
    self.metric = metric

  def __eq__(self, other):
    return (self.step == other.step and
            self.metric == other.metric)

  def __repr__(self):
    return 'MetricKey(step={}, metric={})'.format(
        self.step, self.metric)

  def __hash__(self):
    return hash((self.step, self.metric))

  def to_runner_api(self):
    return beam_fn_api_pb2.Metrics.User.MetricKey(
        step=self.step, namespace=self.metric.namespace, name=self.metric.name)

  @staticmethod
  def from_runner_api(proto):
    return MetricKey(proto.step, MetricName(proto.namespace, proto.name))


class MetricResult(object):
  """Keeps track of the status of a metric within a single bundle.

  It contains the physical and logical updates to the metric. Physical updates
  are updates that have not necessarily been committed, but that have been made
  during pipeline execution. Logical updates are updates that have been
  committed.

  Attributes:
    key: A ``MetricKey`` that identifies the metric and bundle of this result.
    committed: The committed updates of the metric. This attribute's type is
      that of the underlying cell data (e.g. int, DistributionData).
    attempted: The logical updates of the metric. This attribute's type is that
      of the underlying cell data (e.g. int, DistributionData).
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

  def __repr__(self):
    return 'MetricResult(key={}, committed={}, attempted={})'.format(
        self.key, str(self.committed), str(self.attempted))


class MetricsEnvironment(object):
  """Holds the MetricsContainer for every step.

  This class is not meant to be instantiated, instead being used to keep
  track of global state.
  """
  METRICS_SUPPORTED = False

  @classmethod
  def set_metrics_supported(cls, supported):
    cls.METRICS_SUPPORTED = supported

  @classmethod
  def register_container(cls, step_name, container):
    """Register container to a step name."""
    current_tracker = statesampler.EXECUTION_STATE_TRACKERS.current_tracker()
    if not current_tracker:
      raise MetricsContextException(
          'No context manager set for metrics.')
    current_tracker.metrics_container_registry[step_name] = container

  @classmethod
  def current_container(cls):
    current_tracker = statesampler.EXECUTION_STATE_TRACKERS.current_tracker()
    if not current_tracker:
      raise MetricsContextException(
          'No context manager set for metrics.')
    current_step_name = current_tracker.current_state().name.step_name
    container = current_tracker.metrics_container_registry.get(
        current_step_name)
    return container


class MetricsContainer(object):
  """Holds the metrics of a single step and a single bundle."""
  def __init__(self, step_name):
    self.step_name = step_name
    self.counters = defaultdict(lambda: CounterCell())
    self.distributions = defaultdict(lambda: DistributionCell())

  def get_counter(self, metric_name):
    return self.counters[metric_name]

  def get_distribution(self, metric_name):
    return self.distributions[metric_name]

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

    return MetricUpdates(counters, distributions)

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
            key=beam_fn_api_pb2.Metrics.User.MetricKey(
                step=self.step_name, namespace=k.namespace, name=k.name),
            counter_data=beam_fn_api_pb2.Metrics.User.CounterData(
                value=v.get_cumulative()))
         for k, v in self.counters.items()] +
        [beam_fn_api_pb2.Metrics.User(
            key=beam_fn_api_pb2.Metrics.User.MetricKey(
                step=self.step_name, namespace=k.namespace, name=k.name),
            distribution_data=v.get_cumulative().to_runner_api())
         for k, v in self.distributions.items()])


class MetricUpdates(object):
  """Contains updates for several metrics.

  A metric update is an object containing information to update a metric.
  For Distribution metrics, it is DistributionData, and for Counter metrics,
  it's an int.
  """
  def __init__(self, counters=None, distributions=None):
    """Create a MetricUpdates object.

    Args:
      counters: Dictionary of MetricKey:MetricUpdate updates.
      distributions: Dictionary of MetricKey:MetricUpdate objects.
    """
    self.counters = counters or {}
    self.distributions = distributions or {}


class ScopedMetricsContainer(object):
  def enter(self):
    pass

  def exit(self):
    pass

  def __enter__(self):
    self.enter()

  def __exit__(self, type, value, traceback):
    self.exit()
