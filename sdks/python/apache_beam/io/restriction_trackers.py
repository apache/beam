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

"""`iobase.RestrictionTracker` implementations provided with Apache Beam."""
from __future__ import absolute_import
from __future__ import division

import threading
from builtins import object

from apache_beam.io.iobase import RestrictionProgress
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.range_trackers import OffsetRangeTracker


class OffsetRange(object):

  def __init__(self, start, stop):
    if start > stop:
      raise ValueError(
          'Start offset must be not be larger than the stop offset. '
          'Received %d and %d respectively.' % (start, stop))
    self.start = start
    self.stop = stop

  def __eq__(self, other):
    if not isinstance(other, OffsetRange):
      return False

    return self.start == other.start and self.stop == other.stop

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((type(self), self.start, self.stop))

  def split(self, desired_num_offsets_per_split, min_num_offsets_per_split=1):
    current_split_start = self.start
    max_split_size = max(desired_num_offsets_per_split,
                         min_num_offsets_per_split)
    while current_split_start < self.stop:
      current_split_stop = min(current_split_start + max_split_size, self.stop)
      remaining = self.stop - current_split_stop

      # Avoiding a small split at the end.
      if (remaining < desired_num_offsets_per_split // 4 or
          remaining < min_num_offsets_per_split):
        current_split_stop = self.stop

      yield OffsetRange(current_split_start, current_split_stop)
      current_split_start = current_split_stop

  def new_tracker(self):
    return OffsetRangeTracker(self.start, self.stop)


class OffsetRestrictionTracker(RestrictionTracker):
  """An `iobase.RestrictionTracker` implementations for an offset range.

  Offset range is represented as a pair of integers
  [start_position, stop_position}.
  """

  def __init__(self, start_position, stop_position):
    self._range = OffsetRange(start_position, stop_position)
    self._current_position = None
    self._current_watermark = None
    self._last_claim_attempt = None
    self._deferred_residual = None
    self._checkpointed = False
    self._lock = threading.RLock()

  def check_done(self):
    with self._lock:
      if self._last_claim_attempt < self._range.stop - 1:
        raise ValueError(
            'OffsetRestrictionTracker is not done since work in range [%s, %s) '
            'has not been claimed.'
            % (self._last_claim_attempt if self._last_claim_attempt is not None
               else self._range.start,
               self._range.stop))

  def current_restriction(self):
    with self._lock:
      return (self._range.start, self._range.stop)

  def current_watermark(self):
    return self._current_watermark

  def current_progress(self):
    with self._lock:
      if self._current_position is None:
        fraction = 0.0
      elif self._range.stop == self._range.start:
        # If self._current_position is not None, we must be done.
        fraction = 1.0
      else:
        fraction = (
            float(self._current_position - self._range.start)
            / (self._range.stop - self._range.start))
    return RestrictionProgress(fraction=fraction)

  def start_position(self):
    with self._lock:
      return self._range.start

  def stop_position(self):
    with self._lock:
      return self._range.stop

  def try_claim(self, position):
    with self._lock:
      if self._last_claim_attempt and position <= self._last_claim_attempt:
        raise ValueError(
            'Positions claimed should strictly increase. Trying to claim '
            'position %d while last claim attempt was %d.'
            % (position, self._last_claim_attempt))

      self._last_claim_attempt = position
      if position < self._range.start:
        raise ValueError(
            'Position to be claimed cannot be smaller than the start position '
            'of the range. Tried to claim position %r for the range [%r, %r)'
            % (position, self._range.start, self._range.stop))

      if position >= self._range.start and position < self._range.stop:
        self._current_position = position
        return True

      return False

  def try_split(self, fraction):
    with self._lock:
      if not self._checkpointed:
        if self._current_position is None:
          cur = self._range.start - 1
        else:
          cur = self._current_position
        split_point = cur + int(max(1, (self._range.stop - cur) * fraction))
        if split_point < self._range.stop:
          prev_stop, self._range.stop = self._range.stop, split_point
          return (self._range.start, split_point), (split_point, prev_stop)

  # TODO(SDF): Replace all calls with try_claim(0).
  def checkpoint(self):
    with self._lock:
      # If self._current_position is 'None' no records have been claimed so
      # residual should start from self._range.start.
      if self._current_position is None:
        end_position = self._range.start
      else:
        end_position = self._current_position + 1

      residual_range = (end_position, self._range.stop)

      self._range = OffsetRange(self._range.start, end_position)
      return residual_range

  def defer_remainder(self, watermark=None):
    with self._lock:
      self._deferred_watermark = watermark or self._current_watermark
      self._deferred_residual = self.checkpoint()

  def deferred_status(self):
    if self._deferred_residual:
      return (self._deferred_residual, self._deferred_watermark)
