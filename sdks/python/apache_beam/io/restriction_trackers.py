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
# pytype: skip-file

from typing import Tuple

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

  def __hash__(self):
    return hash((type(self), self.start, self.stop))

  def __repr__(self):
    return 'OffsetRange(start=%s, stop=%s)' % (self.start, self.stop)

  def split(self, desired_num_offsets_per_split, min_num_offsets_per_split=1):
    current_split_start = self.start
    max_split_size = max(
        desired_num_offsets_per_split, min_num_offsets_per_split)
    while current_split_start < self.stop:
      current_split_stop = min(current_split_start + max_split_size, self.stop)
      remaining = self.stop - current_split_stop

      # Avoiding a small split at the end.
      if (remaining < desired_num_offsets_per_split // 4 or
          remaining < min_num_offsets_per_split):
        current_split_stop = self.stop

      yield OffsetRange(current_split_start, current_split_stop)
      current_split_start = current_split_stop

  def split_at(self, split_pos):
    # type: (...) -> Tuple[OffsetRange, OffsetRange]
    return OffsetRange(self.start, split_pos), OffsetRange(split_pos, self.stop)

  def new_tracker(self):
    return OffsetRangeTracker(self.start, self.stop)

  def size(self):
    return self.stop - self.start


class OffsetRestrictionTracker(RestrictionTracker):
  """An `iobase.RestrictionTracker` implementations for an offset range.

  Offset range is represented as OffsetRange.
  """
  def __init__(self, offset_range):
    # type: (OffsetRange) -> None
    assert isinstance(offset_range, OffsetRange), offset_range
    self._range = offset_range
    self._current_position = None
    self._last_claim_attempt = None
    self._checkpointed = False

  def check_done(self):
    if (self._range.start != self._range.stop and
        (self._last_claim_attempt is None or
         self._last_claim_attempt < self._range.stop - 1)):
      raise ValueError(
          'OffsetRestrictionTracker is not done since work in range [%s, %s) '
          'has not been claimed.' % (
              self._last_claim_attempt
              if self._last_claim_attempt is not None else self._range.start,
              self._range.stop))

  def current_restriction(self):
    return self._range

  def current_progress(self):
    # type: () -> RestrictionProgress
    if self._current_position is None:
      fraction = 0.0
    elif self._range.stop == self._range.start:
      # If self._current_position is not None, we must be done.
      fraction = 1.0
    else:
      fraction = (
          float(self._current_position - self._range.start) /
          (self._range.stop - self._range.start))
    return RestrictionProgress(fraction=fraction)

  def start_position(self):
    return self._range.start

  def stop_position(self):
    return self._range.stop

  def try_claim(self, position):
    if (self._last_claim_attempt is not None and
        position <= self._last_claim_attempt):
      raise ValueError(
          'Positions claimed should strictly increase. Trying to claim '
          'position %d while last claim attempt was %d.' %
          (position, self._last_claim_attempt))

    self._last_claim_attempt = position
    if position < self._range.start:
      raise ValueError(
          'Position to be claimed cannot be smaller than the start position '
          'of the range. Tried to claim position %r for the range [%r, %r)' %
          (position, self._range.start, self._range.stop))

    if self._range.start <= position < self._range.stop:
      self._current_position = position
      return True

    return False

  def try_split(self, fraction_of_remainder):
    if not self._checkpointed:
      if self._last_claim_attempt is None:
        cur = self._range.start - 1
      else:
        cur = self._last_claim_attempt
      split_point = (
          cur + int(max(1, (self._range.stop - cur) * fraction_of_remainder)))
      if split_point < self._range.stop:
        if fraction_of_remainder == 0:
          self._checkpointed = True
        self._range, residual_range = self._range.split_at(split_point)
        return self._range, residual_range

  def is_bounded(self):
    return True


class UnsplittableRestrictionTracker(RestrictionTracker):
  """An `iobase.RestrictionTracker` that wraps another but does not split."""
  def __init__(self, underling_tracker):
    self._underling_tracker = underling_tracker

  def try_split(self, fraction_of_remainder):
    return False

  # __getattribute__ is used rather than __getattr__ to override the
  # stubs in the baseclass.
  def __getattribute__(self, name):
    if name.startswith('_') or name in ('try_split', ):
      return super().__getattribute__(name)
    else:
      return getattr(self._underling_tracker, name)
