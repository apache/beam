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
DirectRunner implementation of MetricResults. It is in charge not only of
responding to queries of current metrics, but also of keeping the common
state consistent.
"""
# pytype: skip-file

import threading
from collections import defaultdict
from typing import Any
from typing import SupportsInt

from apache_beam.metrics.cells import BoundedTrieData
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import GaugeData
from apache_beam.metrics.cells import StringSetData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metric import MetricResults


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


class GenericAggregator(MetricAggregator):
  def __init__(self, data_class):
    self._data_class = data_class

  def identity_element(self):
    return self._data_class.identity_element()

  def combine(self, x, y):
    return x.combine(y)

  def result(self, x):
    return x.get_result()


class DirectMetrics(MetricResults):
  def __init__(self):
    self._counters = defaultdict(lambda: DirectMetric(CounterAggregator()))
    self._distributions = defaultdict(
        lambda: DirectMetric(GenericAggregator(DistributionData)))
    self._gauges = defaultdict(
        lambda: DirectMetric(GenericAggregator(GaugeData)))
    self._string_sets = defaultdict(
        lambda: DirectMetric(GenericAggregator(StringSetData)))
    self._bounded_tries = defaultdict(
        lambda: DirectMetric(GenericAggregator(BoundedTrieData)))

  def _apply_operation(self, bundle, updates, op):
    for k, v in updates.counters.items():
      op(self._counters[k], bundle, v)

    for k, v in updates.distributions.items():
      op(self._distributions[k], bundle, v)

    for k, v in updates.gauges.items():
      op(self._gauges[k], bundle, v)

    for k, v in updates.string_sets.items():
      op(self._string_sets[k], bundle, v)

    for k, v in updates.bounded_tries.items():
      op(self._bounded_tries[k], bundle, v)

  def commit_logical(self, bundle, updates):
    op = lambda obj, bundle, update: obj.commit_logical(bundle, update)
    self._apply_operation(bundle, updates, op)

  def commit_physical(self, bundle, updates):
    op = lambda obj, bundle, update: obj.commit_physical(bundle, update)
    self._apply_operation(bundle, updates, op)

  def update_physical(self, bundle, updates):
    op = lambda obj, bundle, update: obj.update_physical(bundle, update)
    self._apply_operation(bundle, updates, op)

  def query(self, filter=None):
    counters = [
        MetricResult(
            MetricKey(k.step, k.metric),
            v.extract_committed(),
            v.extract_latest_attempted()) for k,
        v in self._counters.items() if self.matches(filter, k)
    ]
    distributions = [
        MetricResult(
            MetricKey(k.step, k.metric),
            v.extract_committed(),
            v.extract_latest_attempted()) for k,
        v in self._distributions.items() if self.matches(filter, k)
    ]
    gauges = [
        MetricResult(
            MetricKey(k.step, k.metric),
            v.extract_committed(),
            v.extract_latest_attempted()) for k,
        v in self._gauges.items() if self.matches(filter, k)
    ]
    string_sets = [
        MetricResult(
            MetricKey(k.step, k.metric),
            v.extract_committed(),
            v.extract_latest_attempted()) for k,
        v in self._string_sets.items() if self.matches(filter, k)
    ]
    bounded_tries = [
        MetricResult(
            MetricKey(k.step, k.metric),
            v.extract_committed(),
            v.extract_latest_attempted()) for k,
        v in self._bounded_tries.items() if self.matches(filter, k)
    ]

    return {
        self.COUNTERS: counters,
        self.DISTRIBUTIONS: distributions,
        self.GAUGES: gauges,
        self.STRINGSETS: string_sets,
        self.BOUNDED_TRIES: bounded_tries,
    }


class DirectMetric(object):
  """ Keeps a consistent state for a single metric.

  It keeps track of the metric's physical and logical updates.
  It's thread safe.
  """
  def __init__(self, aggregator):
    self.aggregator = aggregator
    self._attempted_lock = threading.Lock()
    self.finished_attempted = aggregator.identity_element()
    self.inflight_attempted = {}
    self._committed_lock = threading.Lock()
    self.finished_committed = aggregator.identity_element()

  def commit_logical(self, bundle, update):
    with self._committed_lock:
      self.finished_committed = self.aggregator.combine(
          update, self.finished_committed)

  def commit_physical(self, bundle, update):
    with self._attempted_lock:
      self.inflight_attempted[bundle] = update
      self.finished_attempted = self.aggregator.combine(
          update, self.finished_attempted)
      del self.inflight_attempted[bundle]

  def update_physical(self, bundle, update):
    self.inflight_attempted[bundle] = update

  def extract_committed(self):
    return self.aggregator.result(self.finished_committed)

  def extract_latest_attempted(self):
    res = self.finished_attempted
    for _, u in self.inflight_attempted.items():
      res = self.aggregator.combine(res, u)

    return self.aggregator.result(res)
