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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import math
from builtins import object
from builtins import range

# Constants used in batched mutation RPCs:
WRITE_BATCH_INITIAL_SIZE = 200
# Max allowed Datastore writes per batch, and max bytes per batch.
# Note that the max bytes per batch set here is lower than the 10MB limit
# actually enforced by the API, to leave space for the CommitRequest wrapper
# around the mutations.
# https://cloud.google.com/datastore/docs/concepts/limits
WRITE_BATCH_MAX_SIZE = 500
WRITE_BATCH_MAX_BYTES_SIZE = 9000000
WRITE_BATCH_MIN_SIZE = 10
WRITE_BATCH_TARGET_LATENCY_MS = 5000


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
    if now >= (self._current_ms_since_epoch +
               self._bucket_ms * self._num_buckets):
      # Time moved forward so far that all currently held data is outside of
      # the window.  It is faster to simply reset our data.
      self._Reset(now)
      return

    while now > self._current_ms_since_epoch + self._bucket_ms:
      # Advance time by one _bucket_ms, setting the new bucket's counts to 0.
      self._current_ms_since_epoch += self._bucket_ms
      self._current_index = (self._current_index + 1) % self._num_buckets
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


class DynamicBatchSizer(object):
  """Determines request sizes for future Datastore RPCs."""
  def __init__(self):
    self._commit_time_per_entity_ms = MovingSum(
        window_ms=120000, bucket_ms=10000)

  def get_batch_size(self, now):
    """Returns the recommended size for datastore RPCs at this time."""
    if not self._commit_time_per_entity_ms.has_data(now):
      return WRITE_BATCH_INITIAL_SIZE

    recent_mean_latency_ms = (
        self._commit_time_per_entity_ms.sum(now) //
        self._commit_time_per_entity_ms.count(now))
    return max(
        WRITE_BATCH_MIN_SIZE,
        min(
            WRITE_BATCH_MAX_SIZE,
            WRITE_BATCH_TARGET_LATENCY_MS // max(recent_mean_latency_ms, 1)))

  def report_latency(self, now, latency_ms, num_mutations):
    """Report the latency of a Datastore RPC.

    Args:
      now: double, completion time of the RPC as seconds since the epoch.
      latency_ms: double, the observed latency in milliseconds for this RPC.
      num_mutations: int, number of mutations contained in the RPC.
    """
    self._commit_time_per_entity_ms.add(now, latency_ms / num_mutations)
