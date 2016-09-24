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

"""iobase.RangeTracker implementations provided with Dataflow SDK.
"""

import logging
import math
import threading

from apache_beam.io import iobase


class OffsetRangeTracker(iobase.RangeTracker):
  """A 'RangeTracker' for non-negative positions of type 'long'."""

  # Offset corresponding to infinity. This can only be used as the upper-bound
  # of a range, and indicates reading all of the records until the end without
  # specifying exactly what the end is.
  # Infinite ranges cannot be split because it is impossible to estimate
  # progress within them.
  OFFSET_INFINITY = float('inf')

  def __init__(self, start, end):
    super(OffsetRangeTracker, self).__init__()

    if start is None:
      raise ValueError('Start offset must not be \'None\'')
    if end is None:
      raise ValueError('End offset must not be \'None\'')
    assert isinstance(start, int) or isinstance(start, long)
    if end != self.OFFSET_INFINITY:
      assert isinstance(end, int) or isinstance(end, long)

    assert start <= end

    self._start_offset = start
    self._stop_offset = end

    self._last_record_start = -1
    self._offset_of_last_split_point = -1
    self._lock = threading.Lock()

  def start_position(self):
    return self._start_offset

  def stop_position(self):
    return self._stop_offset

  @property
  def last_record_start(self):
    return self._last_record_start

  def _validate_record_start(self, record_start, split_point):
    # This function must only be called under the lock self.lock.
    if not self._lock.locked():
      raise ValueError(
          'This function must only be called under the lock self.lock.')

    if record_start < self._last_record_start:
      raise ValueError(
          'Trying to return a record [starting at %d] which is before the '
          'last-returned record [starting at %d]' %
          (record_start, self._last_record_start))

    if split_point:
      if (self._offset_of_last_split_point != -1 and
          record_start == self._offset_of_last_split_point):
        raise ValueError(
            'Record at a split point has same offset as the previous split '
            'point: %d' % record_start)
    elif self._last_record_start == -1:
      raise ValueError(
          'The first record [starting at %d] must be at a split point' %
          record_start)

    if (split_point and self._offset_of_last_split_point is not -1 and
        record_start is self._offset_of_last_split_point):
      raise ValueError(
          'Record at a split point has same offset as the previous split '
          'point: %d' % record_start)

    if not split_point and self._last_record_start == -1:
      raise ValueError(
          'The first record [starting at %d] must be at a split point' %
          record_start)

  def try_claim(self, record_start):
    with self._lock:
      self._validate_record_start(record_start, True)
      if record_start >= self.stop_position():
        return False
      self._offset_of_last_split_point = record_start
      self._last_record_start = record_start
      return True

  def set_current_position(self, record_start):
    with self._lock:
      self._validate_record_start(record_start, False)
      self._last_record_start = record_start

  def try_split(self, split_offset):
    assert isinstance(split_offset, (int, long))
    with self._lock:
      if self._stop_offset == OffsetRangeTracker.OFFSET_INFINITY:
        logging.debug('refusing to split %r at %d: stop position unspecified',
                      self, split_offset)
        return
      if self._last_record_start == -1:
        logging.debug('Refusing to split %r at %d: unstarted', self,
                      split_offset)
        return

      if split_offset <= self._last_record_start:
        logging.debug(
            'Refusing to split %r at %d: already past proposed stop offset',
            self, split_offset)
        return
      if (split_offset < self.start_position()
          or split_offset >= self.stop_position()):
        logging.debug(
            'Refusing to split %r at %d: proposed split position out of range',
            self, split_offset)
        return

      logging.debug('Agreeing to split %r at %d', self, split_offset)

      split_fraction = (float(split_offset - self._start_offset) / (
          self._stop_offset - self._start_offset))
      self._stop_offset = split_offset

      return self._stop_offset, split_fraction

  def fraction_consumed(self):
    with self._lock:
      fraction = ((1.0 * (self._last_record_start - self.start_position()) /
                   (self.stop_position() - self.start_position())) if
                  self.stop_position() != self.start_position() else 0.0)

      # self.last_record_start may become larger than self.end_offset when
      # reading the records since any record that starts before the first 'split
      # point' at or after the defined 'stop offset' is considered to be within
      # the range of the OffsetRangeTracker. Hence fraction could be > 1.
      # self.last_record_start is initialized to -1, hence fraction may be < 0.
      # Bounding the to range [0, 1].
      return max(0.0, min(1.0, fraction))

  def position_at_fraction(self, fraction):
    if self.stop_position() == OffsetRangeTracker.OFFSET_INFINITY:
      raise Exception(
          'get_position_for_fraction_consumed is not applicable for an '
          'unbounded range')
    return int(math.ceil(self.start_position() + fraction * (
        self.stop_position() - self.start_position())))


class GroupedShuffleRangeTracker(iobase.RangeTracker):
  """A 'RangeTracker' for positions used by'GroupedShuffleReader'.

  These positions roughly correspond to hashes of keys. In case of hash
  collisions, multiple groups can have the same position. In that case, the
  first group at a particular position is considered a split point (because
  it is the first to be returned when reading a position range starting at this
  position), others are not.
  """

  def __init__(self, decoded_start_pos, decoded_stop_pos):
    super(GroupedShuffleRangeTracker, self).__init__()
    self._decoded_start_pos = decoded_start_pos
    self._decoded_stop_pos = decoded_stop_pos
    self._decoded_last_group_start = None
    self._last_group_was_at_a_split_point = False
    self._lock = threading.Lock()

  def start_position(self):
    return self._decoded_start_pos

  def stop_position(self):
    return self._decoded_stop_pos

  def last_group_start(self):
    return self._decoded_last_group_start

  def _validate_decoded_group_start(self, decoded_group_start, split_point):
    if self.start_position() and decoded_group_start < self.start_position():
      raise ValueError('Trying to return record at %r which is before the'
                       ' starting position at %r' %
                       (decoded_group_start, self.start_position()))

    if (self.last_group_start() and
        decoded_group_start < self.last_group_start()):
      raise ValueError('Trying to return group at %r which is before the'
                       ' last-returned group at %r' %
                       (decoded_group_start, self.last_group_start()))
    if (split_point and self.last_group_start() and
        self.last_group_start() == decoded_group_start):
      raise ValueError('Trying to return a group at a split point with '
                       'same position as the previous group: both at %r, '
                       'last group was %sat a split point.' %
                       (decoded_group_start,
                        ('' if self._last_group_was_at_a_split_point
                         else 'not ')))
    if not split_point:
      if self.last_group_start() is None:
        raise ValueError('The first group [at %r] must be at a split point' %
                         decoded_group_start)
      if self.last_group_start() != decoded_group_start:
        # This case is not a violation of general RangeTracker semantics, but it
        # is contrary to how GroupingShuffleReader in particular works. Hitting
        # it would mean it's behaving unexpectedly.
        raise ValueError('Trying to return a group not at a split point, but '
                         'with a different position than the previous group: '
                         'last group was %r at %r, current at a %s split'
                         ' point.' %
                         (self.last_group_start()
                          , decoded_group_start
                          , ('' if self._last_group_was_at_a_split_point
                             else 'non-')))

  def try_claim(self, decoded_group_start):
    with self._lock:
      self._validate_decoded_group_start(decoded_group_start, True)
      if (self.stop_position()
          and decoded_group_start >= self.stop_position()):
        return False

      self._decoded_last_group_start = decoded_group_start
      self._last_group_was_at_a_split_point = True
      return True

  def set_current_position(self, decoded_group_start):
    with self._lock:
      self._validate_decoded_group_start(decoded_group_start, False)
      self._decoded_last_group_start = decoded_group_start
      self._last_group_was_at_a_split_point = False

  def try_split(self, decoded_split_position):
    with self._lock:
      if self.last_group_start() is None:
        logging.info('Refusing to split %r at %r: unstarted'
                     , self, decoded_split_position)
        return

      if decoded_split_position <= self.last_group_start():
        logging.info('Refusing to split %r at %r: already past proposed split '
                     'position'
                     , self, decoded_split_position)
        return

      if ((self.stop_position()
           and decoded_split_position >= self.stop_position())
          or (self.start_position()
              and decoded_split_position <= self.start_position())):
        logging.error('Refusing to split %r at %r: proposed split position out '
                      'of range', self, decoded_split_position)
        return

      logging.debug('Agreeing to split %r at %r'
                    , self, decoded_split_position)
      self._decoded_stop_pos = decoded_split_position

      # Since GroupedShuffleRangeTracker cannot determine relative sizes of the
      # two splits, returning 0.5 as the fraction below so that the framework
      # assumes the splits to be of the same size.
      return self._decoded_stop_pos, 0.5

  def fraction_consumed(self):
    # GroupingShuffle sources have special support on the service and the
    # service will estimate progress from positions for us.
    raise RuntimeError('GroupedShuffleRangeTracker does not measure fraction'
                       ' consumed due to positions being opaque strings'
                       ' that are interpreted by the service')


class UnsplittableRangeTracker(iobase.RangeTracker):
  """A RangeTracker that always ignores split requests.

  This can be used to make a given ``RangeTracker`` object unsplittable by
  ignoring all calls to ``try_split()``. All other calls will be delegated to
  the given ``RangeTracker``.
  """

  def __init__(self, range_tracker):
    """Initializes UnsplittableRangeTracker.

    Args:
      range_tracker: a ``RangeTracker`` to which all method calls expect calls
      to ``try_split()`` will be delegated.
    """
    assert range_tracker
    self._range_tracker = range_tracker

  def start_position(self):
    return self._range_tracker.start_position()

  def stop_position(self):
    return self._range_tracker.stop_position()

  def position_at_fraction(self, fraction):
    return self._range_tracker.position_at_fraction(fraction)

  def try_claim(self, position):
    return self._range_tracker.try_claim(position)

  def try_split(self, position):
    return None

  def set_current_position(self, position):
    self._range_tracker.set_current_position(position)

  def fraction_consumed(self):
    return self._range_tracker.fraction_consumed()
