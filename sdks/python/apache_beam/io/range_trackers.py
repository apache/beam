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

"""iobase.RangeTracker implementations provided with Apache Beam.
"""
from __future__ import absolute_import
from __future__ import division

import codecs
import logging
import math
import threading
from builtins import zip

from past.builtins import long

from apache_beam.io import iobase

__all__ = ['OffsetRangeTracker', 'LexicographicKeyRangeTracker',
           'OrderedPositionRangeTracker', 'UnsplittableRangeTracker']


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
    assert isinstance(start, (int, long))
    if end != self.OFFSET_INFINITY:
      assert isinstance(end, (int, long))

    assert start <= end

    self._start_offset = start
    self._stop_offset = end

    self._last_record_start = -1
    self._offset_of_last_split_point = -1
    self._lock = threading.Lock()

    self._split_points_seen = 0
    self._split_points_unclaimed_callback = None

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

    if (split_point and self._offset_of_last_split_point != -1 and
        record_start == self._offset_of_last_split_point):
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
      self._split_points_seen += 1
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

  def split_points(self):
    with self._lock:
      split_points_consumed = (
          0 if self._split_points_seen == 0 else self._split_points_seen - 1)
      split_points_unclaimed = (
          self._split_points_unclaimed_callback(self.stop_position())
          if self._split_points_unclaimed_callback
          else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
      split_points_remaining = (
          iobase.RangeTracker.SPLIT_POINTS_UNKNOWN if
          split_points_unclaimed == iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
          else (split_points_unclaimed + 1))

      return (split_points_consumed, split_points_remaining)

  def set_split_points_unclaimed_callback(self, callback):
    self._split_points_unclaimed_callback = callback


class OrderedPositionRangeTracker(iobase.RangeTracker):
  """
  An abstract base class for range trackers whose positions are comparable.

  Subclasses only need to implement the mapping from position ranges
  to and from the closed interval [0, 1].
  """

  UNSTARTED = object()

  def __init__(self, start_position=None, stop_position=None):
    self._start_position = start_position
    self._stop_position = stop_position
    self._lock = threading.Lock()
    self._last_claim = self.UNSTARTED

  def start_position(self):
    return self._start_position

  def stop_position(self):
    with self._lock:
      return self._stop_position

  def try_claim(self, position):
    with self._lock:
      if self._last_claim is not self.UNSTARTED and position < self._last_claim:
        raise ValueError(
            "Positions must be claimed in order: "
            "claim '%s' attempted after claim '%s'" % (
                position, self._last_claim))
      elif self._start_position is not None and position < self._start_position:
        raise ValueError("Claim '%s' is before start '%s'" % (
            position, self._start_position))
      if self._stop_position is None or position < self._stop_position:
        self._last_claim = position
        return True
      else:
        return False

  def position_at_fraction(self, fraction):
    return self.fraction_to_position(
        fraction, self._start_position, self._stop_position)

  def try_split(self, position):
    with self._lock:
      if ((self._stop_position is not None and position >= self._stop_position)
          or (self._start_position is not None
              and position <= self._start_position)):
        raise ValueError("Split at '%s' not in range %s" % (
            position, [self._start_position, self._stop_position]))
      if self._last_claim is self.UNSTARTED or self._last_claim < position:
        fraction = self.position_to_fraction(
            position, start=self._start_position, end=self._stop_position)
        self._stop_position = position
        return position, fraction
      else:
        return None

  def fraction_consumed(self):
    if self._last_claim is self.UNSTARTED:
      return 0
    else:
      return self.position_to_fraction(
          self._last_claim, self._start_position, self._stop_position)

  def position_to_fraction(self, pos, start, end):
    """
    Converts a position `pos` betweeen `start` and `end` (inclusive) to a
    fraction between 0 and 1.
    """
    raise NotImplementedError

  def fraction_to_position(self, fraction, start, end):
    """
    Converts a fraction between 0 and 1 to a position between start and end.
    """
    raise NotImplementedError


class UnsplittableRangeTracker(iobase.RangeTracker):
  """A RangeTracker that always ignores split requests.

  This can be used to make a given
  :class:`~apache_beam.io.iobase.RangeTracker` object unsplittable by
  ignoring all calls to :meth:`.try_split()`. All other calls will be delegated
  to the given :class:`~apache_beam.io.iobase.RangeTracker`.
  """

  def __init__(self, range_tracker):
    """Initializes UnsplittableRangeTracker.

    Args:
      range_tracker (~apache_beam.io.iobase.RangeTracker): a
        :class:`~apache_beam.io.iobase.RangeTracker` to which all method
        calls expect calls to :meth:`.try_split()` will be delegated.
    """
    assert isinstance(range_tracker, iobase.RangeTracker)
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

  def split_points(self):
    # An unsplittable range only contains a single split point.
    return (0, 1)

  def set_split_points_unclaimed_callback(self, callback):
    self._range_tracker.set_split_points_unclaimed_callback(callback)


class LexicographicKeyRangeTracker(OrderedPositionRangeTracker):
  """
  A range tracker that tracks progress through a lexicographically
  ordered keyspace of strings.
  """

  @classmethod
  def fraction_to_position(cls, fraction, start=None, end=None):
    """
    Linearly interpolates a key that is lexicographically
    fraction of the way between start and end.
    """
    assert 0 <= fraction <= 1, fraction
    if start is None:
      start = b''
    if fraction == 1:
      return end
    elif fraction == 0:
      return start
    else:
      if not end:
        common_prefix_len = len(start) - len(start.lstrip(b'\xFF'))
      else:
        for ix, (s, e) in enumerate(zip(start, end)):
          if s != e:
            common_prefix_len = ix
            break
        else:
          common_prefix_len = min(len(start), len(end))
      # Convert the relative precision of fraction (~53 bits) to an absolute
      # precision needed to represent values between start and end distinctly.
      prec = common_prefix_len + int(-math.log(fraction, 256)) + 7
      istart = cls._bytestring_to_int(start, prec)
      iend = cls._bytestring_to_int(end, prec) if end else 1 << (prec * 8)
      ikey = istart + int((iend - istart) * fraction)
      # Could be equal due to rounding.
      # Adjust to ensure we never return the actual start and end
      # unless fraction is exatly 0 or 1.
      if ikey == istart:
        ikey += 1
      elif ikey == iend:
        ikey -= 1
      return cls._bytestring_from_int(ikey, prec).rstrip(b'\0')

  @classmethod
  def position_to_fraction(cls, key, start=None, end=None):
    """
    Returns the fraction of keys in the range [start, end) that
    are less than the given key.
    """
    if not key:
      return 0
    if start is None:
      start = b''
    prec = len(start) + 7
    if key.startswith(start):
      # Higher absolute precision needed for very small values of fixed
      # relative position.
      prec = max(prec, len(key) - len(key[len(start):].strip(b'\0')) + 7)
    istart = cls._bytestring_to_int(start, prec)
    ikey = cls._bytestring_to_int(key, prec)
    iend = cls._bytestring_to_int(end, prec) if end else 1 << (prec * 8)
    return float(ikey - istart) / (iend - istart)

  @staticmethod
  def _bytestring_to_int(s, prec):
    """
    Returns int(256**prec * f) where f is the fraction
    represented by interpreting '.' + s as a base-256
    floating point number.
    """
    if not s:
      return 0
    elif len(s) < prec:
      s += b'\0' * (prec - len(s))
    else:
      s = s[:prec]
    return int(codecs.encode(s, 'hex'), 16)

  @staticmethod
  def _bytestring_from_int(i, prec):
    """
    Inverse of _bytestring_to_int.
    """
    h = '%x' % i
    return codecs.decode('0' * (2 * prec - len(h)) + h, 'hex')
