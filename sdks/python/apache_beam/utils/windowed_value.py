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

"""Core windowing data structures."""

# This module is carefully crafted to have optimal performance when
# compiled while still being valid Python.  Care needs to be taken when
# editing this file as WindowedValues are created for every element for
# every step in a Beam pipeline.

# pytype: skip-file

import collections
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow


class PaneInfoTiming(object):
  """The timing of a PaneInfo."""

  EARLY = 0
  ON_TIME = 1
  LATE = 2
  UNKNOWN = 3

  @classmethod
  def to_string(cls, value):
    return {
        cls.EARLY: 'EARLY',
        cls.ON_TIME: 'ON_TIME',
        cls.LATE: 'LATE',
        cls.UNKNOWN: 'UNKNOWN',
    }[value]

  @classmethod
  def from_string(cls, value):
    return {
        'EARLY': cls.EARLY,
        'ON_TIME': cls.ON_TIME,
        'LATE': cls.LATE,
        'UNKNOWN': cls.UNKNOWN
    }[value]


class PaneInfo(object):
  """Describes the trigger firing information for a given WindowedValue.

  "Panes" represent individual firings on a single window. ``PaneInfo``s are
  passed downstream after trigger firings. They contain information about
  whether it's an early/on time/late firing, if it's the last or first firing
  from a window, and the index of the firing.
  """
  def __init__(self, is_first, is_last, timing, index, nonspeculative_index):
    self._is_first = is_first
    self._is_last = is_last
    self._timing = timing
    self._index = index
    self._nonspeculative_index = nonspeculative_index
    self._encoded_byte = self._get_encoded_byte()

  def _get_encoded_byte(self):
    byte = 0
    if self._is_first:
      byte |= 1
    if self._is_last:
      byte |= 2
    byte |= self._timing << 2
    return byte

  @staticmethod
  def from_encoded_byte(encoded_byte):
    assert encoded_byte in _BYTE_TO_PANE_INFO
    return _BYTE_TO_PANE_INFO[encoded_byte]

  # Because common PaneInfo objects are cached, it is important that the value
  # is immutable.  We therefore explicitly enforce this here with read-only
  # properties.

  @property
  def is_first(self):
    return self._is_first

  @property
  def is_last(self):
    return self._is_last

  @property
  def timing(self):
    return self._timing

  @property
  def index(self):
    # type: () -> int
    return self._index

  @property
  def nonspeculative_index(self):
    # type: () -> int
    return self._nonspeculative_index

  @property
  def encoded_byte(self):
    # type: () -> int
    return self._encoded_byte

  def __repr__(self):
    return (
        'PaneInfo(first: %r, last: %r, timing: %s, index: %d, '
        'nonspeculative_index: %d)') % (
            self.is_first,
            self.is_last,
            PaneInfoTiming.to_string(self.timing),
            self.index,
            self.nonspeculative_index)

  def __eq__(self, other):
    if self is other:
      return True

    if isinstance(other, PaneInfo):
      return (
          self.is_first == other.is_first and self.is_last == other.is_last and
          self.timing == other.timing and self.index == other.index and
          self.nonspeculative_index == other.nonspeculative_index)

    return NotImplemented

  def __hash__(self):
    return hash((
        self.is_first,
        self.is_last,
        self.timing,
        self.index,
        self.nonspeculative_index))

  def __reduce__(self):
    return PaneInfo, (self._is_first, self._is_last, self._timing, self._index,
                      self._nonspeculative_index)


def _construct_well_known_pane_infos():
  # type: () -> List[PaneInfo]
  pane_infos = []
  for timing in (PaneInfoTiming.EARLY,
                 PaneInfoTiming.ON_TIME,
                 PaneInfoTiming.LATE,
                 PaneInfoTiming.UNKNOWN):
    nonspeculative_index = -1 if timing == PaneInfoTiming.EARLY else 0
    pane_infos.append(PaneInfo(True, True, timing, 0, nonspeculative_index))
    pane_infos.append(PaneInfo(True, False, timing, 0, nonspeculative_index))
    pane_infos.append(PaneInfo(False, True, timing, -1, nonspeculative_index))
    pane_infos.append(PaneInfo(False, False, timing, -1, nonspeculative_index))
  result = [None] * (
      max(p.encoded_byte for p in pane_infos) + 1
  )  # type: List[PaneInfo]  # type: ignore[list-item]
  for pane_info in pane_infos:
    result[pane_info.encoded_byte] = pane_info
  return result


# Cache of well-known PaneInfo objects.
_BYTE_TO_PANE_INFO = _construct_well_known_pane_infos()

# Default PaneInfo descriptor for when a value is not the output of triggering.
PANE_INFO_UNKNOWN = _BYTE_TO_PANE_INFO[0xF]


class WindowedValue(object):
  """A windowed value having a value, a timestamp and set of windows.

  Attributes:
    value: The underlying value of a windowed value.
    timestamp: Timestamp associated with the value as seconds since Unix epoch.
    windows: A set (iterable) of window objects for the value. The window
      object are descendants of the BoundedWindow class.
    pane_info: A PaneInfo descriptor describing the triggering information for
      the pane that contained this value.  If None, will be set to
      PANE_INFO_UNKNOWN.
  """
  def __init__(
      self,
      value,
      timestamp,  # type: TimestampTypes
      windows,  # type: Tuple[BoundedWindow, ...]
      pane_info=PANE_INFO_UNKNOWN  # type: PaneInfo
  ):
    # type: (...) -> None
    # For performance reasons, only timestamp_micros is stored by default
    # (as a C int). The Timestamp object is created on demand below.
    self.value = value
    if isinstance(timestamp, int):
      self.timestamp_micros = timestamp * 1000000
      if TYPE_CHECKING:
        self.timestamp_object = None  # type: Optional[Timestamp]
    else:
      self.timestamp_object = (
          timestamp
          if isinstance(timestamp, Timestamp) else Timestamp.of(timestamp))
      self.timestamp_micros = self.timestamp_object.micros
    self.windows = windows
    self.pane_info = pane_info

  @property
  def timestamp(self):
    # type: () -> Timestamp
    if self.timestamp_object is None:
      self.timestamp_object = Timestamp(0, self.timestamp_micros)
    return self.timestamp_object

  def __repr__(self):
    return '(%s, %s, %s, %s)' % (
        repr(self.value),
        'MIN_TIMESTAMP' if self.timestamp == MIN_TIMESTAMP else 'MAX_TIMESTAMP'
        if self.timestamp == MAX_TIMESTAMP else float(self.timestamp),
        self.windows,
        self.pane_info)

  def __eq__(self, other):
    if isinstance(other, WindowedValue):
      return (
          type(self) == type(other) and
          self.timestamp_micros == other.timestamp_micros and
          self.value == other.value and self.windows == other.windows and
          self.pane_info == other.pane_info)
    return NotImplemented

  def __hash__(self):
    return ((hash(self.value) & 0xFFFFFFFFFFFFFFF) + 3 *
            (self.timestamp_micros & 0xFFFFFFFFFFFFFF) + 7 *
            (hash(tuple(self.windows)) & 0xFFFFFFFFFFFFF) + 11 *
            (hash(self.pane_info) & 0xFFFFFFFFFFFFF))

  def with_value(self, new_value):
    # type: (Any) -> WindowedValue

    """Creates a new WindowedValue with the same timestamps and windows as this.

    This is the fasted way to create a new WindowedValue.
    """
    return create(
        new_value, self.timestamp_micros, self.windows, self.pane_info)

  def __reduce__(self):
    return WindowedValue, (
        self.value, self.timestamp, self.windows, self.pane_info)


# TODO(robertwb): Move this to a static method.


def create(value, timestamp_micros, windows, pane_info=PANE_INFO_UNKNOWN):
  wv = WindowedValue.__new__(WindowedValue)
  wv.value = value
  wv.timestamp_micros = timestamp_micros
  wv.windows = windows
  wv.pane_info = pane_info
  return wv


class WindowedBatch(object):
  """A batch of N windowed values, each having a value, a timestamp and set of
  windows."""
  def with_values(self, new_values):
    # type: (Any) -> WindowedBatch

    """Creates a new WindowedBatch with the same timestamps and windows as this.

    This is the fasted way to create a new WindowedValue.
    """
    raise NotImplementedError

  def as_windowed_values(self, explode_fn: Callable) -> Iterable[WindowedValue]:
    raise NotImplementedError

  @staticmethod
  def from_windowed_values(
      windowed_values: Sequence[WindowedValue], *,
      produce_fn: Callable) -> Iterable['WindowedBatch']:
    return HomogeneousWindowedBatch.from_windowed_values(
        windowed_values, produce_fn=produce_fn)


class HomogeneousWindowedBatch(WindowedBatch):
  """A WindowedBatch with Homogeneous event-time information, represented
  internally as a WindowedValue.
  """
  def __init__(self, wv):
    self._wv = wv

  @staticmethod
  def of(values, timestamp, windows, pane_info):
    return HomogeneousWindowedBatch(
        WindowedValue(values, timestamp, windows, pane_info))

  @property
  def values(self):
    return self._wv.value

  @property
  def timestamp(self):
    return self._wv.timestamp

  @property
  def pane_info(self):
    return self._wv.pane_info

  @property
  def windows(self):
    return self._wv.windows

  @windows.setter
  def windows(self, value):
    self._wv.windows = value

  def with_values(self, new_values):
    # type: (Any) -> WindowedBatch
    return HomogeneousWindowedBatch(self._wv.with_value(new_values))

  def as_windowed_values(self, explode_fn: Callable) -> Iterable[WindowedValue]:
    for value in explode_fn(self._wv.value):
      yield self._wv.with_value(value)

  def as_empty_windowed_value(self):
    """Get a single WindowedValue with identical windowing information to this
    HomogeneousWindowedBatch, but with value=None. Useful for re-using APIs that
    pull windowing information from a WindowedValue."""
    return self._wv.with_value(None)

  def __eq__(self, other):
    if isinstance(other, HomogeneousWindowedBatch):
      return self._wv == other._wv
    return NotImplemented

  def __hash__(self):
    return hash(self._wv)

  @staticmethod
  def from_batch_and_windowed_value(
      *, batch, windowed_value: WindowedValue) -> 'WindowedBatch':
    return HomogeneousWindowedBatch(windowed_value.with_value(batch))

  @staticmethod
  def from_windowed_values(
      windowed_values: Sequence[WindowedValue], *,
      produce_fn: Callable) -> Iterable['WindowedBatch']:
    grouped = collections.defaultdict(lambda: [])
    for wv in windowed_values:
      grouped[wv.with_value(None)].append(wv.value)

    for key, values in grouped.items():
      yield HomogeneousWindowedBatch(key.with_value(produce_fn(values)))


try:
  WindowedValue.timestamp_object = None
except TypeError:
  # When we're compiled, we can't dynamically add attributes to
  # the cdef class, but in this case it's OK as it's already present
  # on each instance.
  pass


class _IntervalWindowBase(object):
  """Optimized form of IntervalWindow storing only microseconds for endpoints.
  """
  def __init__(self, start, end):
    # type: (TimestampTypes, TimestampTypes) -> None
    if start is not None:
      self._start_object = Timestamp.of(start)  # type: Optional[Timestamp]
      try:
        self._start_micros = self._start_object.micros
      except OverflowError:
        self._start_micros = (
            MIN_TIMESTAMP.micros
            if self._start_object.micros < 0 else MAX_TIMESTAMP.micros)
    else:
      # Micros must be populated elsewhere.
      self._start_object = None

    if end is not None:
      self._end_object = Timestamp.of(end)  # type: Optional[Timestamp]
      try:
        self._end_micros = self._end_object.micros
      except OverflowError:
        self._end_micros = (
            MIN_TIMESTAMP.micros
            if self._end_object.micros < 0 else MAX_TIMESTAMP.micros)
    else:
      # Micros must be populated elsewhere.
      self._end_object = None

  @property
  def start(self):
    # type: () -> Timestamp
    if self._start_object is None:
      self._start_object = Timestamp(0, self._start_micros)
    return self._start_object

  @property
  def end(self):
    # type: () -> Timestamp
    if self._end_object is None:
      self._end_object = Timestamp(0, self._end_micros)
    return self._end_object

  def __hash__(self):
    return hash((self._start_micros, self._end_micros))

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self._start_micros == other._start_micros and
        self._end_micros == other._end_micros)

  def __repr__(self):
    return '[%s, %s)' % (float(self.start), float(self.end))
