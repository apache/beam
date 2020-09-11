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
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import threading
import time
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import SupportsInt

from apache_beam.utils.histogram import Histogram

if TYPE_CHECKING:
  from apache_beam.utils.histogram import BucketType

try:
  import cython
except ImportError:

  class fake_cython:
    compiled = False

  globals()['cython'] = fake_cython

__all__ = ['DistributionResult', 'GaugeResult']


class MetricCell(object):
  """For internal use only; no backwards-compatibility guarantees.

  Accumulates in-memory changes to a metric.

  A MetricCell represents a specific metric in a single context and bundle.
  All subclasses must be thread safe, as these are used in the pipeline runners,
  and may be subject to parallel/concurrent updates. Cells should only be used
  directly within a runner.
  """
  def __init__(self):
    self._lock = threading.Lock()

  def update(self, value):
    raise NotImplementedError

  def get_cumulative(self):
    raise NotImplementedError

  def to_runner_api_monitoring_info(self, name, transform_id):
    raise NotImplementedError

  def reset(self):
    # type: () -> None
    raise NotImplementedError

  def __reduce__(self):
    raise NotImplementedError


class MetricCellFactory(object):
  def __call__(self):
    # type: () -> MetricCell
    raise NotImplementedError


class CounterCell(MetricCell):
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
    # type: () -> None
    self.value = CounterAggregator.identity_element()

  def combine(self, other):
    # type: (CounterCell) -> CounterCell
    result = CounterCell()
    result.inc(self.value + other.value)
    return result

  def inc(self, n=1):
    self.update(n)

  def dec(self, n=1):
    self.update(-n)

  def update(self, value):
    if cython.compiled:
      ivalue = value
      # We hold the GIL, no need for another lock.
      self.value += ivalue
    else:
      with self._lock:
        self.value += value

  def get_cumulative(self):
    # type: () -> int
    with self._lock:
      return self.value

  def to_runner_api_monitoring_info(self, name, transform_id):
    from apache_beam.metrics import monitoring_infos
    return monitoring_infos.int64_user_counter(
        name.namespace,
        name.name,
        self.get_cumulative(),
        ptransform=transform_id)


class DistributionCell(MetricCell):
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
    # type: () -> None
    self.data = DistributionAggregator.identity_element()

  def combine(self, other):
    # type: (DistributionCell) -> DistributionCell
    result = DistributionCell()
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    if cython.compiled:
      # We will hold the GIL throughout the entire _update.
      self._update(value)
    else:
      with self._lock:
        self._update(value)

  def _update(self, value):
    if cython.compiled:
      ivalue = value
    else:
      ivalue = int(value)
    self.data.count = self.data.count + 1
    self.data.sum = self.data.sum + ivalue
    if ivalue < self.data.min:
      self.data.min = ivalue
    if ivalue > self.data.max:
      self.data.max = ivalue

  def get_cumulative(self):
    # type: () -> DistributionData
    with self._lock:
      return self.data.get_cumulative()

  def to_runner_api_monitoring_info(self, name, transform_id):
    from apache_beam.metrics import monitoring_infos
    return monitoring_infos.int64_user_distribution(
        name.namespace,
        name.name,
        self.get_cumulative(),
        ptransform=transform_id)


class GaugeCell(MetricCell):
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
    self.data = GaugeAggregator.identity_element()

  def combine(self, other):
    # type: (GaugeCell) -> GaugeCell
    result = GaugeCell()
    result.data = self.data.combine(other.data)
    return result

  def set(self, value):
    self.update(value)

  def update(self, value):
    # type: (SupportsInt) -> None
    value = int(value)
    with self._lock:
      # Set the value directly without checking timestamp, because
      # this value is naturally the latest value.
      self.data.value = value
      self.data.timestamp = time.time()

  def get_cumulative(self):
    # type: () -> GaugeData
    with self._lock:
      return self.data.get_cumulative()

  def to_runner_api_monitoring_info(self, name, transform_id):
    from apache_beam.metrics import monitoring_infos
    return monitoring_infos.int64_user_gauge(
        name.namespace,
        name.name,
        self.get_cumulative(),
        ptransform=transform_id)


class HistogramCell(MetricCell):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks the current value and delta for a histogram metric.

  Each cell tracks the state of a metric independently per context per bundle.
  Therefore, each metric has a different cell in each bundle, that is later
  aggregated.

  This class is thread safe since underlying histogram object is thread safe.
  """
  def __init__(self, bucket_type):
    self._bucket_type = bucket_type
    self.data = HistogramAggregator(bucket_type).identity_element()

  def reset(self):
    self.data = HistogramAggregator(self._bucket_type).identity_element()

  def combine(self, other):
    # type: (HistogramCell) -> HistogramCell
    result = HistogramCell(self._bucket_type)
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    self.data.histogram.record(value)

  def get_cumulative(self):
    # type: () -> HistogramData
    return self.data.get_cumulative()

  def to_runner_api_monitoring_info(self, name, transform_id):
    return None


class HistogramCellFactory(MetricCellFactory):
  def __init__(self, bucket_type):
    self._bucket_type = bucket_type

  def __call__(self):
    return HistogramCell(self._bucket_type)

  def __eq__(self, other):
    if not isinstance(other, HistogramCellFactory):
      return False
    return self._bucket_type == other._bucket_type

  def __hash__(self):
    return hash(self._bucket_type)


class DistributionResult(object):
  """The result of a Distribution metric."""
  def __init__(self, data):
    # type: (DistributionData) -> None
    self.data = data

  def __eq__(self, other):
    # type: (object) -> bool
    if isinstance(other, DistributionResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    # type: () -> int
    return hash(self.data)

  def __ne__(self, other):
    # type: (object) -> bool
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    # type: () -> str
    return 'DistributionResult(sum={}, count={}, min={}, max={})'.format(
        self.sum, self.count, self.min, self.max)

  @property
  def max(self):
    # type: () -> Optional[int]
    return self.data.max if self.data.count else None

  @property
  def min(self):
    # type: () -> Optional[int]
    return self.data.min if self.data.count else None

  @property
  def count(self):
    # type: () -> Optional[int]
    return self.data.count

  @property
  def sum(self):
    # type: () -> Optional[int]
    return self.data.sum

  @property
  def mean(self):
    # type: () -> Optional[float]

    """Returns the float mean of the distribution.

    If the distribution contains no elements, it returns None.
    """
    if self.data.count == 0:
      return None
    return self.data.sum / self.data.count


class GaugeResult(object):
  def __init__(self, data):
    # type: (GaugeData) -> None
    self.data = data

  def __eq__(self, other):
    # type: (object) -> bool
    if isinstance(other, GaugeResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    # type: () -> int
    return hash(self.data)

  def __ne__(self, other):
    # type: (object) -> bool
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<GaugeResult(value={}, timestamp={})>'.format(
        self.value, self.timestamp)

  @property
  def value(self):
    # type: () -> Optional[int]
    return self.data.value

  @property
  def timestamp(self):
    # type: () -> Optional[int]
    return self.data.timestamp


class HistogramResult(object):
  def __init__(self, data):
    # type: (HistogramData) -> None
    self.data = data

  def __eq__(self, other):
    if isinstance(other, HistogramResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    return hash(self.data)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return '<HistogramResult({})>'.format(
        self.data.histogram.get_percentile_info())

  @property
  def p99(self):
    return self.data.histogram.p99()

  @property
  def p95(self):
    return self.data.histogram.p95()

  @property
  def p90(self):
    return self.data.histogram.p90()


class GaugeData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a gauge metric.

  Gauge metrics are restricted to integers only.

  This object is not thread safe, so it's not supposed to be modified
  by other than the GaugeCell that contains it.
  """
  def __init__(self, value, timestamp=None):
    # type: (Optional[int], Optional[int]) -> None
    self.value = value
    self.timestamp = timestamp if timestamp is not None else 0

  def __eq__(self, other):
    # type: (object) -> bool
    if isinstance(other, GaugeData):
      return self.value == other.value and self.timestamp == other.timestamp
    else:
      return False

  def __hash__(self):
    # type: () -> int
    return hash((self.value, self.timestamp))

  def __ne__(self, other):
    # type: (object) -> bool
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    # type: () -> str
    return '<GaugeData(value={}, timestamp={})>'.format(
        self.value, self.timestamp)

  def get_cumulative(self):
    # type: () -> GaugeData
    return GaugeData(self.value, timestamp=self.timestamp)

  def combine(self, other):
    # type: (Optional[GaugeData]) -> GaugeData
    if other is None:
      return self

    if other.timestamp > self.timestamp:
      return other
    else:
      return self

  @staticmethod
  def singleton(value, timestamp=None):
    # type: (Optional[int], Optional[int]) -> GaugeData
    return GaugeData(value, timestamp=timestamp)


class DistributionData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a distribution metric.

  Distribution metrics are restricted to distributions of integers only.

  This object is not thread safe, so it's not supposed to be modified
  by other than the DistributionCell that contains it.
  """
  def __init__(self, sum, count, min, max):
    # type: (int, int, int, int) -> None
    if count:
      self.sum = sum
      self.count = count
      self.min = min
      self.max = max
    else:
      self.sum = self.count = 0
      self.min = 2**63 - 1
      # Avoid Wimplicitly-unsigned-literal caused by -2**63.
      self.max = -self.min - 1

  def __eq__(self, other):
    # type: (object) -> bool
    if isinstance(other, DistributionData):
      return (
          self.sum == other.sum and self.count == other.count and
          self.min == other.min and self.max == other.max)
    else:
      return False

  def __hash__(self):
    # type: () -> int
    return hash((self.sum, self.count, self.min, self.max))

  def __ne__(self, other):
    # type: (object) -> bool
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    # type: () -> str
    return 'DistributionData(sum={}, count={}, min={}, max={})'.format(
        self.sum, self.count, self.min, self.max)

  def get_cumulative(self):
    # type: () -> DistributionData
    return DistributionData(self.sum, self.count, self.min, self.max)

  def combine(self, other):
    # type: (Optional[DistributionData]) -> DistributionData
    if other is None:
      return self

    return DistributionData(
        self.sum + other.sum,
        self.count + other.count,
        self.min if self.min < other.min else other.min,
        self.max if self.max > other.max else other.max)

  @staticmethod
  def singleton(value):
    # type: (int) -> DistributionData
    return DistributionData(value, 1, value, value)


class HistogramData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a histogram metric.

  This object is not thread safe, so it's not supposed to be modified
  by other than the HistogramCell that contains it.
  """
  def __init__(self, histogram):
    self.histogram = histogram

  def __eq__(self, other):
    return self.histogram == other.histogram

  def __hash__(self):
    return hash(self.histogram)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return 'HistogramData({})'.format(self.histogram.get_percentile_info())

  def get_cumulative(self):
    # type: () -> HistogramData
    return HistogramData(self.histogram)

  def combine(self, other):
    # type: (Optional[HistogramData]) -> HistogramData
    if other is None:
      return self

    return HistogramData(self.histogram.combine(other.histogram))


class MetricAggregator(object):
  """For internal use only; no backwards-compatibility guarantees.

  Base interface for aggregating metric data during pipeline execution."""
  def identity_element(self):
    # type: () -> Any

    """Returns the identical element of an Aggregation.

    For the identity element, it must hold that
     Aggregator.combine(any_element, identity_element) == any_element.
    """
    raise NotImplementedError

  def combine(self, x, y):
    # type: (Any, Any) -> Any
    raise NotImplementedError

  def result(self, x):
    # type: (Any) -> Any
    raise NotImplementedError


class CounterAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Counter metric data during pipeline execution.

  Values aggregated should be ``int`` objects.
  """
  @staticmethod
  def identity_element():
    # type: () -> int
    return 0

  def combine(self, x, y):
    # type: (SupportsInt, SupportsInt) -> int
    return int(x) + int(y)

  def result(self, x):
    # type: (SupportsInt) -> int
    return int(x)


class DistributionAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Distribution metric data during pipeline execution.

  Values aggregated should be ``DistributionData`` objects.
  """
  @staticmethod
  def identity_element():
    # type: () -> DistributionData
    return DistributionData(0, 0, 2**63 - 1, -2**63)

  def combine(self, x, y):
    # type: (DistributionData, DistributionData) -> DistributionData
    return x.combine(y)

  def result(self, x):
    # type: (DistributionData) -> DistributionResult
    return DistributionResult(x.get_cumulative())


class GaugeAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Gauge metric data during pipeline execution.

  Values aggregated should be ``GaugeData`` objects.
  """
  @staticmethod
  def identity_element():
    # type: () -> GaugeData
    return GaugeData(0, timestamp=0)

  def combine(self, x, y):
    # type: (GaugeData, GaugeData) -> GaugeData
    result = x.combine(y)
    return result

  def result(self, x):
    # type: (GaugeData) -> GaugeResult
    return GaugeResult(x.get_cumulative())


class HistogramAggregator(MetricAggregator):
  """For internal use only; no backwards-compatibility guarantees.

  Aggregator for Histogram metric data during pipeline execution.

  Values aggregated should be ``HistogramData`` objects.
  """
  def __init__(self, bucket_type):
    # type: (BucketType) -> None
    self._bucket_type = bucket_type

  def identity_element(self):
    # type: () -> HistogramData
    return HistogramData(Histogram(self._bucket_type))

  def combine(self, x, y):
    # type: (HistogramData, HistogramData) -> HistogramData
    return x.combine(y)

  def result(self, x):
    # type: (HistogramData) -> HistogramResult
    return HistogramResult(x.get_cumulative())
