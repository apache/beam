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

import threading

from apache_beam.metrics.metric import Counter
from apache_beam.metrics.metric import Distribution


class DirtyState(object):
  DIRTY = 0
  CLEAN = 1
  COMMITTING = 2

  def __init__(self):
    self._lock = threading.Lock()
    self._state = DirtyState.DIRTY

  @property
  def state(self):
    return self._state

  def modified(self):
    with self._lock:
      self._state = DirtyState.DIRTY

  def after_commit(self):
    with self._lock:
      if self._state == DirtyState.COMMITTING:
        self._state = DirtyState.CLEAN

  def before_commit(self):
    with self._lock:
      if self._state == DirtyState.CLEAN:
        return False
      else:
        self._state = DirtyState.COMMITTING
        return True


class MetricCell(object):
  def __init__(self):
    self.dirty = DirtyState()
    self._lock = threading.Lock()

  def get_cumulative(self):
    raise NotImplementedError


class CounterCell(Counter, MetricCell):
  def __init__(self, *args):
    super(CounterCell, self).__init__(*args)
    self.value = 0

  def combine(self, other):
    result = CounterCell()
    result.inc(self.value + other.value)
    return result

  def inc(self, n=1):
    with self._lock:
      self.value += n
      self.dirty.modified()

  def get_cumulative(self):
    return self.value


class DistributionCell(Distribution, MetricCell):
  def __init__(self, *args):
    super(DistributionCell, self).__init__(*args)
    self.data = DistributionData(0, 0, None, None)

  def combine(self, other):
    result = DistributionCell()
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    with self._lock:
      self.dirty.modified()
      self._update(value)

  def _update(self, value):
    self.data.count += 1
    self.data.sum += value
    self.data.min = (value
                     if self.data.min is None or self.data.min > value
                     else self.data.min)
    self.data.max = (value
                     if self.data.max is None or self.data.max < value
                     else self.data.max)

  def get_cumulative(self):
    return self.data


class DistributionData(object):
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

  def __neq__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return '<DistributionData({}, {}, {}, {})>'.format(self.sum,
                                                       self.count,
                                                       self.min,
                                                       self.max)

  @property
  def mean(self):
    return self.sum/self.count

  def combine(self, other):
    if other is None:
      return self

    return DistributionData(
        self.sum + other.sum,
        self.count + other.count,
        min(x for x in (self.min, other.min) if x is not None),
        max(x for x in (self.max, other.max) if x is not None))

  @classmethod
  def singleton(cls, value):
    return DistributionData(value, 1, value, value)
