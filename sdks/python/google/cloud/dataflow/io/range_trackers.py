# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""iobase.RangeTracker implementations provided with Dataflow SDK.
"""

import logging
import math
import threading

from google.cloud.dataflow.io import iobase


class OffsetRangeTracker(iobase.RangeTracker):
  """A 'RangeTracker' for non-negative positions of type 'long'."""

  # Offset corresponding to infinity. This can only be used as the upper-bound
  # of a range, and indicates reading all of the records until the end without
  # specifying exactly what the end is.
  # Infinite ranges cannot be split because it is impossible to estimate
  # progress within them.
  OFFSET_INFINITY = float('inf')

  def __init__(self, start, end):
    self._start_offset = start
    self._stop_offset = end
    self.last_record_start = -1
    self.offset_of_last_split_point = -1
    self.lock = threading.Lock()

  @property
  def start_position(self):
    return self._start_offset

  @property
  def stop_position(self):
    return self._stop_offset

  def try_return_record_at(self, is_at_split_point, record_start):
    with self.lock:
      if self.last_record_start == -1 and not is_at_split_point:
        raise Exception(
            'The first record [starting at %d] must be at a split point' %
            record_start)
      if record_start < self.last_record_start:
        raise Exception(
            'Trying to return a record [starting at %d] which is before the '
            'last-returned record [starting at %d]' %
            (record_start, self.last_record_start))
      if is_at_split_point:
        if (self.offset_of_last_split_point is not -1 and
            record_start is self.offset_of_last_split_point):
          raise Exception(
              'Record at a split point has same offset as the previous split '
              'point: %d' % record_start)
        if record_start >= self.stop_position:
          return False
        self.offset_of_last_split_point = record_start

      self.last_record_start = record_start
      return True

  def try_split_at_position(self, split_offset):
    with self.lock:
      if self._stop_offset == OffsetRangeTracker.OFFSET_INFINITY:
        logging.debug('refusing to split %r at %d: stop position unspecified',
                      self, split_offset)
        return False
      if self.last_record_start == -1:
        logging.debug('Refusing to split %r at %d: unstarted', self,
                      split_offset)
        return False

      if split_offset <= self.last_record_start:
        logging.debug(
            'Refusing to split %r at %d: already past proposed stop offset',
            self, split_offset)
        return False
      if (split_offset < self.start_position
          or split_offset >= self.stop_position):
        logging.debug(
            'Refusing to split %r at %d: proposed split position out of range',
            self, split_offset)
        return False

      logging.debug('Agreeing to split %r at %d', self, split_offset)
      self._stop_offset = split_offset
      return True

  @property
  def fraction_consumed(self):
    with self.lock:
      fraction = ((1.0 * (self.last_record_start - self.start_position + 1) /
                   (self.stop_position - self.start_position)) if
                  self.stop_position != self.start_position else 0.0)

      # self.last_record_start may become larger than self.end_offset when
      # reading the records since any record that starts before the first 'split
      # point' at or after the defined 'stop offset' is considered to be within
      # the range of the OffsetRangeTracker. Hence fraction could be > 1.
      # self.last_record_start is initialized to -1, hence fraction may be < 0.
      # Bounding the to range [0, 1].
      return max(0.0, min(1.0, fraction))

  def get_position_for_fraction_consumed(self, fraction):
    if self.stop_position == OffsetRangeTracker.OFFSET_INFINITY:
      raise Exception(
          'get_position_for_fraction_consumed is not applicable for an '
          'unbounded range')
    return (math.ceil(self.start_position + fraction * (self.stop_position -
                                                        self.start_position)))


class GroupedShuffleRangeTracker(iobase.RangeTracker):
  """A 'RangeTracker' for positions used by'GroupedShuffleReader'.

  These positions roughly correspond to hashes of keys. In case of hash
  collisions, multiple groups can have the same position. In that case, the
  first group at a particular position is considered a split point (because
  it is the first to be returned when reading a position range starting at this
  position), others are not.
  """

  def __init__(self, decoded_start_pos, decoded_stop_pos):
    self._decoded_start_pos = decoded_start_pos
    self._decoded_stop_pos = decoded_stop_pos
    self._decoded_last_group_start = None
    self.last_group_was_at_a_split_point = False
    self.lock = threading.Lock()

  @property
  def start_position(self):
    return self._decoded_start_pos

  @property
  def stop_position(self):
    return self._decoded_stop_pos

  @property
  def last_group_start(self):
    return self._decoded_last_group_start

  def try_return_record_at(self, is_at_split_point, decoded_group_start):
    with self.lock:
      if self.last_group_start is None and not is_at_split_point:
        raise ValueError('The first group [at %r] must be at a split point' %
                         decoded_group_start)

      if (self.start_position
          and decoded_group_start < self.start_position):
        raise ValueError('Trying to return record at %r which is before the'
                         ' starting position at %r' %
                         (decoded_group_start, self.start_position))

      if (self.last_group_start is not None and
          decoded_group_start < self.last_group_start):
        raise ValueError('Trying to return group at %r which is before the'
                         ' last-returned group at %r' %
                         (decoded_group_start, self.last_group_start))
      if is_at_split_point:
        if (self.last_group_start is not None and
            self.last_group_start == decoded_group_start):
          raise ValueError('Trying to return a group at a split point with '
                           'same position as the previous group: both at %r, '
                           'last group was %sat a split point.' %
                           (decoded_group_start,
                            ('' if  self.last_group_was_at_a_split_point
                             else 'not ')))
        if (self.stop_position
            and decoded_group_start >= self.stop_position):
          return False
      elif (self.last_group_start is not None
            and self.last_group_start != decoded_group_start):
        # This case is not a violation of general RangeTracker semantics, but it
        # is contrary to how GroupingShuffleReader in particular works. Hitting
        # it would mean it's behaving unexpectedly.
        raise ValueError('Trying to return a group not at a split point, but '
                         'with a different position than the previous group: '
                         'last group was %r at %r, current at a %s split'
                         ' point.' %
                         (self.last_group_start
                          , decoded_group_start
                          , ('' if self.last_group_was_at_a_split_point
                             else 'non-')))

      self._decoded_last_group_start = decoded_group_start
      self.last_group_was_at_a_split_point = is_at_split_point
      return True

  def try_split_at_position(self, decoded_split_position):
    with self.lock:
      if self.last_group_start is None:
        logging.info('Refusing to split %r at %r: unstarted'
                     , self, decoded_split_position)
        return False

      if decoded_split_position <= self.last_group_start:
        logging.info('Refusing to split %r at %r: already past proposed split '
                     'position'
                     , self, decoded_split_position)
        return False

      if ((self.stop_position
           and decoded_split_position >= self.stop_position)
          or (self.start_position
              and decoded_split_position <= self.start_position)):
        logging.error('Refusing to split %r at %r: proposed split position out '
                      'of range', self, decoded_split_position)
        return False

      logging.info('Agreeing to split %r at %r'
                   , self, decoded_split_position)
      self._decoded_stop_pos = decoded_split_position
      return True

  @property
  def fraction_consumed(self):
    # GroupingShuffle sources have special support on the service and the
    # service will estimate progress from positions for us.
    raise RuntimeError('GroupedShuffleRangeTracker does not measure fraction'
                       ' consumed due to positions being opaque strings'
                       ' that are interpretted by the service')
