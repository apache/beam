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

# Utility functions & classes that are _not_ specific to the datastore client.
#
# For internal use only; no backwards-compatibility guarantees.

from __future__ import absolute_import
from __future__ import division

import math
from builtins import object
from builtins import range


class MovingSum(object):
  """Class that keeps track of a rolling window sum.

  For use in tracking recent performance of the connector.

  Intended to be similar to
  org.apache.beam.sdk.util.MovingFunction(..., Sum.ofLongs()), but for
  convenience we expose the count of entries as well so this doubles as a
  moving average tracker.
  """

  def __init__(self, window_ms, bucket_ms):
    if window_ms <= bucket_ms or bucket_ms <= 0:
      raise ValueError("window_ms > bucket_ms > 0 please")
    self._num_buckets = int(math.ceil(window_ms / bucket_ms))
    self._bucket_ms = bucket_ms
    self._Reset(now=0)  # initialize the moving window members

  def _Reset(self, now):
    self._current_index = 0  # pointer into self._buckets
    self._current_ms_since_epoch = math.floor(
        now / self._bucket_ms) * self._bucket_ms

    # _buckets is a list where each element is a list [sum, num_samples]
    # This is a circular buffer where
    # [_current_index] represents the time range
    #     [_current_ms_since_epoch, _current_ms_since_epoch+_bucket_ms)
    # [_current_index-1] represents immediatly prior time range
    #     [_current_ms_since_epoch-_bucket_ms, _current_ms_since_epoch)
    # etc, wrapping around from the start to the end of the array, so
    # [_current_index+1] is the element representing the oldest bucket.
    self._buckets = [[0, 0] for _ in range(0, self._num_buckets)]

  def _Flush(self, now):
    """

    Args:
      now: int, milliseconds since epoch
    """
    if now >= (self._current_ms_since_epoch
               + self._bucket_ms * self._num_buckets):
      # Time moved forward so far that all currently held data is outside of
      # the window.  It is faster to simply reset our data.
      self._Reset(now)
      return

    while now > self._current_ms_since_epoch + self._bucket_ms:
      # Advance time by one _bucket_ms, setting the new bucket's counts to 0.
      self._current_ms_since_epoch += self._bucket_ms
      self._current_index = (self._current_index+1) % self._num_buckets
      self._buckets[self._current_index] = [0, 0]
      # Intentional dead reckoning here; we don't care about staying precisely
      # aligned with multiples of _bucket_ms since the epoch, we just need our
      # buckets to represent the most recent _window_ms time window.

  def sum(self, now):
    self._Flush(now)
    return sum(bucket[0] for bucket in self._buckets)

  def add(self, now, inc):
    self._Flush(now)
    bucket = self._buckets[self._current_index]
    bucket[0] += inc
    bucket[1] += 1

  def count(self, now):
    self._Flush(now)
    return sum(bucket[1] for bucket in self._buckets)

  def has_data(self, now):
    return self.count(now) > 0
