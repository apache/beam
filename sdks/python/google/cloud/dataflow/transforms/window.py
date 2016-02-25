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

"""Windowing concepts.

A WindowInto transform logically divides up or groups the elements of a
PCollection into finite windows according to a windowing function (derived from
WindowFn).

The output of WindowInto contains the same elements as input, but they have been
logically assigned to windows. The next GroupByKey(s) transforms, including one
within a composite transform, will group by the combination of keys and windows.

Windowing a PCollection allows chunks of it to be processed individually, before
the entire PCollection is available.  This is especially important for
PCollection(s) with unbounded size, since the full PCollection is never
available at once, since more data is continually arriving. For PCollection(s)
with a bounded size (aka. conventional batch mode), by default, all data is
implicitly in a single window (see GlobalWindows), unless WindowInto is
applied.

For example, a simple form of windowing divides up the data into fixed-width
time intervals, using FixedWindows. Floating-point seconds are used as the time
unit for built-in windowing strategies available here.

Custom windowing function classes can be created, by subclassing from
WindowFn.
"""

from __future__ import absolute_import


MIN_TIMESTAMP = float('-Inf')
MAX_TIMESTAMP = float('Inf')


class WindowFn(object):
  """An abstract windowing function defining a basic assign and merge."""

  class AssignContext(object):
    """Context passed to WindowFn.assign()."""

    def __init__(self, timestamp, element=None, existing_windows=None):
      self.timestamp = timestamp
      self.element = element
      self.existing_windows = existing_windows

  def assign(self, assign_context):
    """Associates a timestamp and set of windows to an element."""
    raise NotImplementedError

  class MergeContext(object):
    """Context passed to WindowFn.merge() to perform merging, if any."""

    def __init__(self, windows):
      self.windows = list(windows)

    def merge(self, to_be_merged, merge_result):
      raise NotImplementedError

  def merge(self, merge_context):
    """Returns a window that is the result of merging a set of windows."""
    raise NotImplementedError


class BoundedWindow(object):
  """A window for timestamps in range (-infinity, end).

  Attributes:
    end: End of window as floating-point seconds since Unix epoch.
  """

  def __init__(self, end):
    self.end = end

  def __cmp__(self, other):
    # Order first by endpoint, then arbitrarily.
    return cmp(self.end, other.end) or cmp(hash(self), hash(other))

  def __eq__(self, other):
    raise NotImplemented

  def __hash__(self):
    return hash(self.end)

  def __repr__(self):
    return '[?, %s)' % self.end


class IntervalWindow(BoundedWindow):
  """A window for timestamps in range [start, end).

  Attributes:
    start: Start of window as floating-point seconds since Unix epoch.
    end: End of window as floating-point seconds since Unix epoch.
  """

  def __init__(self, start, end):
    super(IntervalWindow, self).__init__(end)
    self.start = start

  def __hash__(self):
    return hash((self.start, self.end))

  def __eq__(self, other):
    return self.start == other.start and self.end == other.end

  def __repr__(self):
    return '[%s, %s)' % (self.start, self.end)

  def intersects(self, other):
    return other.start < self.end or self.start < other.end

  def union(self, other):
    return IntervalWindow(
        min(self.start, other.start), max(self.end, other.end))


class WindowedValue(object):
  """A windowed value having a value, a timestamp and set of windows.

  Attributes:
    value: The underlying value of a windowed value.
    timestamp: Timestamp associated with the value as floating-point seconds
      since Unix epoch.
    windows: A set (iterable) of window objects for the value. The window
      object are descendants of the BoundedWindow class.
  """

  def __init__(self, value, timestamp, windows):
    self.value = value
    self.timestamp = timestamp
    self.windows = windows

  def __repr__(self):
    return '(%s, %s, %s)' % (
        repr(self.value),
        'MIN_TIMESTAMP' if self.timestamp == MIN_TIMESTAMP else
        'MAX_TIMESTAMP' if self.timestamp == MAX_TIMESTAMP else
        self.timestamp,
        self.windows)

  def __hash__(self):
    return hash((self.value, self.timestamp, self.windows))

  def __eq__(self, other):
    return (type(self) == type(other)
            and self.value == other.value
            and self.timestamp == other.timestamp
            and self.windows == other.windows)


class TimestampedValue(object):
  """A timestamped value having a value and a timestamp.

  Attributes:
    value: The underlying value.
    timestamp: Timestamp associated with the value as floating-point seconds
      since Unix epoch.
  """

  def __init__(self, value, timestamp):
    self.value = value
    self.timestamp = timestamp


class GlobalWindow(BoundedWindow):
  """The default window into which all data is placed (via GlobalWindows)."""
  _instance = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super(GlobalWindow, cls).__new__(cls)
    return cls._instance

  def __init__(self):
    super(GlobalWindow, self).__init__(MAX_TIMESTAMP)

  def __repr__(self):
    return 'GlobalWindow'

  def __hash__(self):
    return hash(type(self))

  def __eq__(self, other):
    # Global windows are always and only equal to each other.
    return self is other or type(self) is type(other)


class GlobalWindows(WindowFn):
  """A windowing function that assigns everything to one global window."""

  @classmethod
  def WindowedValue(cls, value, timestamp=MIN_TIMESTAMP):  # pylint: disable=invalid-name
    return WindowedValue(value, timestamp, [GlobalWindow()])

  def assign(self, assign_context):
    return [GlobalWindow()]

  def merge(self, merge_context):
    pass  # No merging.

  def __hash__(self):
    return hash(type(self))

  def __eq__(self, other):
    # Global windowfn is always and only equal to each other.
    return self is other or type(self) is type(other)

  def __ne__(self, other):
    return not self == other


class FixedWindows(WindowFn):
  """A windowing function that assigns each element to one time interval.

  The attributes size and offset determine in what time interval a timestamp
  will be slotted. The time intervals have the following formula:
  [N * size + offset, (N + 1) * size + offset)

  Attributes:
    size: Size of the window as floating-point seconds.
    offset: Offset of this window as floating-point seconds since Unix epoch.
      Windows start at t=N * size + offset where t=0 is the epoch. The offset
      must be a value in range [0, size). If it is not it will be normalized to
      this range.
  """

  def __init__(self, size, offset=0):
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = size
    self.offset = offset % size

  def assign(self, context):
    timestamp = context.timestamp
    start = timestamp - (timestamp - self.offset) % self.size
    return [IntervalWindow(start, start + self.size)]

  def merge(self, merge_context):
    pass  # No merging.


class SlidingWindows(WindowFn):
  """A windowing function that assigns each element to a set of sliding windows.

  The attributes size and offset determine in what time interval a timestamp
  will be slotted. The time intervals have the following formula:
  [N * period + offset, N * period + offset + size)

  Attributes:
    size: Size of the window as floating-point seconds.
    period: Period of the windows as floating-point seconds.
    offset: Offset of this window as floating-point seconds since Unix epoch.
      Windows start at t=N * period + offset where t=0 is the epoch. The offset
      must be a value in range [0, period). If it is not it will be normalized
      to this range.
  """

  def __init__(self, size, period, offset=0):
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = size
    self.period = period
    self.offset = offset % size

  def assign(self, context):
    timestamp = context.timestamp
    start = timestamp - (timestamp - self.offset) % self.period
    return [IntervalWindow(s, s + self.size)
            for s in range(start, start - self.size, -self.period)]

  def merge(self, merge_context):
    pass  # No merging.


class Sessions(WindowFn):
  """A windowing function that groups elements into sessions.

  A session is defined as a series of consecutive events
  separated by a specified gap size.

  Attributes:
    gap_size: Size of the gap between windows as floating-point seconds.
  """

  def __init__(self, gap_size):
    if gap_size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.gap_size = gap_size

  def assign(self, context):
    timestamp = context.timestamp
    return [IntervalWindow(timestamp, timestamp + self.gap_size)]

  def merge(self, merge_context):
    to_merge = []
    for w in sorted(merge_context.windows, key=lambda w: w.start):
      if to_merge:
        if end > w.start:
          to_merge.append(w)
          if w.end > end:
            end = w.end
        else:
          if len(to_merge) > 1:
            merge_context.merge(to_merge,
                                IntervalWindow(to_merge[0].start, end))
          to_merge = [w]
          end = w.end
      else:
        to_merge = [w]
        end = w.end
    if len(to_merge) > 1:
      merge_context.merge(to_merge, IntervalWindow(to_merge[0].start, end))
