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
Internal classes for Metrics API.

The classes in this file keep shared state, and organize metrics information.

Available classes:
- MetricsEnvironment - Keeps track of MetricsContainer and other metrics
    information for every single execution working thread.
- MetricsContainer - Holds the metrics of a single step and a single
    unit-of-commit (bundle).
"""
from collections import defaultdict, namedtuple
import threading


MetricName = namedtuple('MetricName', 'namespace name')
MetricKey = namedtuple('MetricKey', 'step metric')


class MetricsEnvironment(object):
  """ Holds the MetricsContainer for every thread and other metric information.

  This class is not meant to be instantiated, instead being used as to keep
  track of global state.
  """
  METRICS_SUPPORTED = False
  _METRICS_SUPPORTED_LOCK = threading.Lock()

  PER_THREAD = threading.local()

  @classmethod
  def set_metrics_supported(cls, supported):
    with cls._METRICS_SUPPORTED_LOCK:
      cls.METRICS_SUPPORTED = supported

  @classmethod
  def current_container(cls):
    try:
      return cls.PER_THREAD.container
    except AttributeError:
      return None

  @classmethod
  def set_current_container(cls, container):
    cls.PER_THREAD.container = container

  @classmethod
  def unset_current_container(cls):
    del cls.PER_THREAD.container


class MetricsContainer(object):
  """ Holds the metrics of a single step and a single bundle.
  """
  def __init__(self, step_name):
    # Import here to avoid the circular dependency
    from apache_beam.metrics.cells import CounterCell, DistributionCell
    self.step_name = step_name
    self.counters = defaultdict(lambda: CounterCell())
    self.distributions = defaultdict(lambda: DistributionCell())

  def get_Counter(self, metric_name):
    return self.counters[metric_name]

  def get_Distribution(self, metric_name):
    return self.distributions[metric_name]

  def _get_updates(self, filter=None):
    """ Return cumulative values of metrics filtered according to a lambda.

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

    return MetricsUpdates(counters, distributions)

  def get_updates(self):
    """ Return cumulative values of metrics that changed since the last commit.

    This returns all the cumulative values for all metrics only if their state
    prior to the function call was COMMITTING or DIRTY.
    """
    return self._get_updates(filter=lambda v: v.dirty.before_commit())

  def get_cumulative(self):
    """ Return MetricUpdates with cumulative values of all metrics in container.

    This returns all the cumulative values for all metrics regardless of whether
    they have been committed or not.
    """
    return self._get_updates()


class MetricsUpdates(object):
  def __init__(self, counters=None, distributions=None):
    self.counters = counters or {}
    self.distributions = distributions or {}
