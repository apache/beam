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
This file contains metric cell classes. A metric cell is used to accumulate
in-memory changes to a metric. It represents a specific metric in a single
context.

Cells depend on a 'dirty-bit' in the CellCommitState class that tracks whether
a cell's updates have been committed.
"""

from __future__ import absolute_import
from __future__ import division

import threading
import time
from builtins import object

from google.protobuf import timestamp_pb2

from apache_beam.metrics.metricbase import Counter
from apache_beam.metrics.metricbase import Distribution
from apache_beam.metrics.metricbase import Gauge
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import metrics_pb2

__all__ = ['DistributionResult', 'GaugeResult']


class CellCommitState(object):
  """For internal use only; no backwards-compatibility guarantees.

  Atomically tracks a cell's dirty/clean commit status.

  Reporting a metric update works in a two-step process: First, updates to the
  metric are received, and the metric is marked as 'dirty'. Later, updates are
  committed, and then the cell may be marked as 'clean'.

  The tracking of a cell's state is done conservatively: A metric may be
  reported DIRTY even if updates have not occurred.

  This class is thread-safe.
  """

  # Indicates that there have been changes to the cell since the last commit.
  DIRTY = 0
  # Indicates that there have NOT been changes to the cell since last commit.
  CLEAN = 1
  # Indicates that a commit of the current value is in progress.
  COMMITTING = 2

  def __init__(self):
    """Initializes ``CellCommitState``.

    A cell is initialized as dirty.
    """
    self._lock = threading.Lock()
    self._state = CellCommitState.DIRTY

  @property
  def state(self):
    with self._lock:
      return self._state

  def after_modification(self):
    """Indicate that changes have been made to the metric being tracked.

    Should be called after modification of the metric value.
    """
    with self._lock:
      self._state = CellCommitState.DIRTY

  def after_commit(self):
    """Mark changes made up to the last call to ``before_commit`` as committed.

    The next call to ``before_commit`` will return ``False`` unless there have
    been changes made.
    """
    with self._lock:
      if self._state == CellCommitState.COMMITTING:
        self._state = CellCommitState.CLEAN

  def before_commit(self):
    """Check the dirty state, and mark the metric as committing.

    After this call, the state is either CLEAN, or COMMITTING. If the state
    was already CLEAN, then we simply return. If it was either DIRTY or
    COMMITTING, then we set the cell as COMMITTING (e.g. in the middle of
    a commit).

    After a commit is successful, ``after_commit`` should be called.

    Returns:
      A boolean, which is false if the cell is CLEAN, and true otherwise.
    """
    with self._lock:
      if self._state == CellCommitState.CLEAN:
        return False
      self._state = CellCommitState.COMMITTING
      return True


class MetricCell(object):
  """For internal use only; no backwards-compatibility guarantees.

  Accumulates in-memory changes to a metric.

  A MetricCell represents a specific metric in a single context and bundle.
  All subclasses must be thread safe, as these are used in the pipeline runners,
  and may be subject to parallel/concurrent updates. Cells should only be used
  directly within a runner.
  """
  def __init__(self):
    self.commit = CellCommitState()
    self._lock = threading.Lock()

  def get_cumulative(self):
    raise NotImplementedError


class CounterCell(Counter, MetricCell):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks the current value and delta of a counter metric.

  Each cell tracks the state of a metric independently per context per bundle.
  Therefore, each metric has a different cell in each bundle, cells are
  aggregated by the runner.

  This class is thread safe.
  """
  def __init__(self, *args):
    super(CounterCell, self).__init__(*args)
    self.value = CounterAggregator.identity_element()

  def reset(self):
    self.commit = CellCommitState()
    self.value = CounterAggregator.identity_element()

  def combine(self, other):
    result = CounterCell()
    result.inc(self.value + other.value)
    return result

  def inc(self, n=1):
    with self._lock:
      self.value += n
      self.commit.after_modification()

  def get_cumulative(self):
    with self._lock:
      return self.value

  def to_runner_api_monitoring_info(self):
    """Returns a Metric with this counter value for use in a MonitoringInfo."""
    # TODO(ajamato): Update this code to be consistent with Gauges
    # and Distributions. Since there is no CounterData class this method
    # was added to CounterCell. Consider adding a CounterData class or
    # removing the GaugeData and DistributionData classes.
    return metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(
            int64_value=self.get_cumulative()
        )
    )


class DistributionCell(Distribution, MetricCell):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks the current value and delta for a distribution metric.

  Each cell tracks the state of a metric independently per context per bundle.
  Therefore, each metric has a different cell in each bundle, that is later
  aggregated.

  This class is thread safe.
  """
  def __init__(self, *args):
    super(DistributionCell, self).__init__(*args)
    self.data = DistributionAggregator.identity_element()

  def reset(self):
    self.commit = CellCommitState()
    self.data = DistributionAggregator.identity_element()

  def combine(self, other):
    result = DistributionCell()
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    with self._lock:
      self.commit.after_modification()
      self._update(value)

  def _update(self, value):
    value = int(value)
    self.data.count += 1
    self.data.sum += value
    self.data.min = (value
                     if self.data.min is None or self.data.min > value
                     else self.data.min)
    self.data.max = (value
                     if self.data.max is None or self.data.max < value
                     else self.data.max)

  def get_cumulative(self):
    with self._lock:
      return self.data.get_cumulative()


class GaugeCell(Gauge, MetricCell):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks the current value and delta for a gauge metric.

  Each cell tracks the state of a metric independently per context per bundle.
  Therefore, each metric has a different cell in each bundle, that is later
  aggregated.

  This class is thread safe.
  """
  def __init__(self, *args):
    super(GaugeCell, self).__init__(*args)
    self.data = GaugeAggregator.identity_element()

  def reset(self):
    self.commit = CellCommitState()
    self.data = GaugeAggregator.identity_element()

  def combine(self, other):
    result = GaugeCell()
    result.data = self.data.combine(other.data)
    return result

  def set(self, value):
    value = int(value)
    with self._lock:
      self.commit.after_modification()
      # Set the value directly without checking timestamp, because
      # this value is naturally the latest value.
      self.data.value = value
      self.data.timestamp = time.time()

  def get_cumulative(self):
    with self._lock:
      return self.data.get_cumulative()


class DistributionResult(object):
  """The result of a Distribution metric."""
  def __init__(self, data):
    self.data = data

  def __eq__(self, other):
    if isinstance(other, DistributionResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    return hash(self.data)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<DistributionResult(sum={}, count={}, min={}, max={})>'.format(
        self.sum,
        self.count,
        self.min,
        self.max)

  @property
  def max(self):
    return self.data.max

  @property
  def min(self):
    return self.data.min

  @property
  def count(self):
    return self.data.count

  @property
  def sum(self):
    return self.data.sum

  @property
  def mean(self):
    """Returns the float mean of the distribution.

    If the distribution contains no elements, it returns None.
    """
    if self.data.count == 0:
      return None
    return self.data.sum / self.data.count


class GaugeResult(object):
  def __init__(self, data):
    self.data = data

  def __eq__(self, other):
    if isinstance(other, GaugeResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    return hash(self.data)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<GaugeResult(value={}, timestamp={})>'.format(
        self.value,
        self.timestamp)

  @property
  def value(self):
    return self.data.value

  @property
  def timestamp(self):
    return self.data.timestamp


class GaugeData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a gauge metric.

  Gauge metrics are restricted to integers only.

  This object is not thread safe, so it's not supposed to be modified
  by other than the GaugeCell that contains it.
  """
  def __init__(self, value, timestamp=None):
    self.value = value
    self.timestamp = timestamp if timestamp is not None else 0

  def __eq__(self, other):
    return self.value == other.value and self.timestamp == other.timestamp

  def __hash__(self):
    return hash((self.value, self.timestamp))

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<GaugeData(value={}, timestamp={})>'.format(
        self.value,
        self.timestamp)

  def get_cumulative(self):
    return GaugeData(self.value, timestamp=self.timestamp)

  def combine(self, other):
    if other is None:
      return self

    if other.timestamp > self.timestamp:
      return other
    else:
      return self

  @staticmethod
  def singleton(value, timestamp=None):
    return GaugeData(value, timestamp=timestamp)

  def to_runner_api(self):
    seconds = int(self.timestamp)
    nanos = int((self.timestamp - seconds) * 10**9)
    gauge_timestamp = timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)
    return beam_fn_api_pb2.Metrics.User.GaugeData(
        value=self.value, timestamp=gauge_timestamp)

  @staticmethod
  def from_runner_api(proto):
    gauge_timestamp = (proto.timestamp.seconds +
                       float(proto.timestamp.nanos) / 10**9)
    return GaugeData(proto.value, timestamp=gauge_timestamp)

  def to_runner_api_monitoring_info(self):
    """Returns a Metric with this value for use in a MonitoringInfo."""
    return metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(
            int64_value=self.value
        )
    )


class DistributionData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a distribution metric.

  Distribution metrics are restricted to distributions of integers only.

  This object is not thread safe, so it's not supposed to be modified
  by other than the DistributionCell that contains it.
  """
  def __init__(self, sum, count, min, max):
    self.sum = sum
    self.count = count
    self.min = min
    self.max = max

  def __eq__(self, other):
    return (self.sum == other.sum and
            self.count == other.count and
            self.min == other.min and
            self.max == other.max)

  def __hash__(self):
    return hash((self.sum, self.count, self.min, self.max))

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<DistributionData(sum={}, count={}, min={}, max={})>'.format(
        self.sum,
        self.count,
        self.min,
        self.max)

  def get_cumulative(self):
    return DistributionData(self.sum, self.count, self.min, self.max)

  def combine(self, other):
    if other is None:
      return self

    new_min = (None if self.min is None and other.min is None else
               min(x for x in (self.min, other.min) if x is not None))
    new_max = (None if self.max is None and other.max is None else
               max(x for x in (self.max, other.max) if x is not None))
    return DistributionData(
        self.sum + other.sum,
        self.count + other.count,
        new_min,
        new_max)

  @staticmethod
  def singleton(value):
    return DistributionData(value, 1, value, value)

  def to_runner_api(self):
    return beam_fn_api_pb2.Metrics.User.DistributionData(
        count=self.count, sum=self.sum, min=self.min, max=self.max)

  @staticmethod
  def from_runner_api(proto):
    return DistributionData(proto.sum, proto.count, proto.min, proto.max)

  def to_runner_api_monitoring_info(self):
    """Returns a Metric with this value for use in a MonitoringInfo."""
    return metrics_pb2.Metric(
        distribution_data=metrics_pb2.DistributionData(
            int_distribution_data=metrics_pb2.IntDistributionData(
                count=self.count, sum=self.sum, min=self.min, max=self.max)))


class MetricAggregator(object):
  """For internal use only; no backwards-compatibility guarantees.

  Base interface for aggregating metric data during pipeline execution."""

  def identity_element(self):
    """Returns the identical element of an Aggregation.

    For the identity element, it must hold that
     Aggregator.combine(any_element, identity_element) == any_element.
    """
    raise NotImplementedError

  def combine(self, updates):
    raise NotImplementedError

  def result(self, x):
    raise NotImplementedError


class CounterAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Counter metric data during pipeline execution.

  Values aggregated should be ``int`` objects.
  """
  @staticmethod
  def identity_element():
    return 0

  def combine(self, x, y):
    return int(x) + int(y)

  def result(self, x):
    return int(x)


class DistributionAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Distribution metric data during pipeline execution.

  Values aggregated should be ``DistributionData`` objects.
  """
  @staticmethod
  def identity_element():
    return DistributionData(0, 0, None, None)

  def combine(self, x, y):
    return x.combine(y)

  def result(self, x):
    return DistributionResult(x.get_cumulative())


class GaugeAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Gauge metric data during pipeline execution.

  Values aggregated should be ``GaugeData`` objects.
  """
  @staticmethod
  def identity_element():
    return GaugeData(None, timestamp=0)

  def combine(self, x, y):
    result = x.combine(y)
    return result

  def result(self, x):
    return GaugeResult(x.get_cumulative())
