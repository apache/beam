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
time intervals, using FixedWindows.

Seconds are used as the time unit for the built-in windowing primitives here.
Integer or floating point seconds can be passed to these primitives.

Internally, seconds, with microsecond granularity, are stored as
timeutil.Timestamp and timeutil.Duration objects. This is done to avoid
precision errors that would occur with floating point representations.

Custom windowing function classes can be created, by subclassing from
WindowFn.
"""

from __future__ import absolute_import

from apache_beam import coders
from apache_beam.transforms import timeutil
from apache_beam.transforms.timeutil import Duration
from apache_beam.transforms.timeutil import MAX_TIMESTAMP
from apache_beam.transforms.timeutil import MIN_TIMESTAMP
from apache_beam.transforms.timeutil import Timestamp
from apache_beam.utils.windowed_value import WindowedValue


# TODO(ccy): revisit naming and semantics once Java Apache Beam finalizes their
# behavior.
class OutputTimeFn(object):
  """Determines how output timestamps of grouping operations are assigned."""

  OUTPUT_AT_EOW = 'OUTPUT_AT_EOW'
  OUTPUT_AT_EARLIEST = 'OUTPUT_AT_EARLIEST'
  OUTPUT_AT_LATEST = 'OUTPUT_AT_LATEST'
  OUTPUT_AT_EARLIEST_TRANSFORMED = 'OUTPUT_AT_EARLIEST_TRANSFORMED'

  @staticmethod
  def get_impl(output_time_fn, window_fn):
    if output_time_fn == OutputTimeFn.OUTPUT_AT_EOW:
      return timeutil.OutputAtEndOfWindowImpl()
    elif output_time_fn == OutputTimeFn.OUTPUT_AT_EARLIEST:
      return timeutil.OutputAtEarliestInputTimestampImpl()
    elif output_time_fn == OutputTimeFn.OUTPUT_AT_LATEST:
      return timeutil.OutputAtLatestInputTimestampImpl()
    elif output_time_fn == OutputTimeFn.OUTPUT_AT_EARLIEST_TRANSFORMED:
      return timeutil.OutputAtEarliestTransformedInputTimestampImpl(window_fn)
    else:
      raise ValueError('Invalid OutputTimeFn: %s.' % output_time_fn)


class WindowFn(object):
  """An abstract windowing function defining a basic assign and merge."""

  class AssignContext(object):
    """Context passed to WindowFn.assign()."""

    def __init__(self, timestamp, element=None, existing_windows=None):
      self.timestamp = Timestamp.of(timestamp)
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

  def get_window_coder(self):
    return coders.PickleCoder()

  def get_transformed_output_time(self, window, input_timestamp):  # pylint: disable=unused-argument
    """Given input time and output window, returns output time for window.

    If OutputTimeFn.OUTPUT_AT_EARLIEST_TRANSFORMED is used in the Windowing,
    the output timestamp for the given window will be the earliest of the
    timestamps returned by get_transformed_output_time() for elements of the
    window.

    Arguments:
      window: Output window of element.
      input_timestamp: Input timestamp of element as a timeutil.Timestamp
        object.

    Returns:
      Transformed timestamp.
    """
    # By default, just return the input timestamp.
    return input_timestamp


class BoundedWindow(object):
  """A window for timestamps in range (-infinity, end).

  Attributes:
    end: End of window.
  """

  def __init__(self, end):
    self.end = Timestamp.of(end)

  def __cmp__(self, other):
    # Order first by endpoint, then arbitrarily.
    return cmp(self.end, other.end) or cmp(hash(self), hash(other))

  def __eq__(self, other):
    raise NotImplementedError

  def __hash__(self):
    return hash(self.end)

  def __repr__(self):
    return '[?, %s)' % float(self.end)


class IntervalWindow(BoundedWindow):
  """A window for timestamps in range [start, end).

  Attributes:
    start: Start of window as seconds since Unix epoch.
    end: End of window as seconds since Unix epoch.
  """

  def __init__(self, start, end):
    super(IntervalWindow, self).__init__(end)
    self.start = Timestamp.of(start)

  def __hash__(self):
    return hash((self.start, self.end))

  def __eq__(self, other):
    return self.start == other.start and self.end == other.end

  def __repr__(self):
    return '[%s, %s)' % (float(self.start), float(self.end))

  def intersects(self, other):
    return other.start < self.end or self.start < other.end

  def union(self, other):
    return IntervalWindow(
        min(self.start, other.start), max(self.end, other.end))


class TimestampedValue(object):
  """A timestamped value having a value and a timestamp.

  Attributes:
    value: The underlying value.
    timestamp: Timestamp associated with the value as seconds since Unix epoch.
  """

  def __init__(self, value, timestamp):
    self.value = value
    self.timestamp = Timestamp.of(timestamp)


class GlobalWindow(BoundedWindow):
  """The default window into which all data is placed (via GlobalWindows)."""
  _instance = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super(GlobalWindow, cls).__new__(cls)
    return cls._instance

  def __init__(self):
    super(GlobalWindow, self).__init__(MAX_TIMESTAMP)
    self.start = MIN_TIMESTAMP

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
  def windowed_value(cls, value, timestamp=MIN_TIMESTAMP):
    return WindowedValue(value, timestamp, [GlobalWindow()])

  def assign(self, assign_context):
    return [GlobalWindow()]

  def merge(self, merge_context):
    pass  # No merging.

  def get_window_coder(self):
    return coders.SingletonCoder(GlobalWindow())

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
    size: Size of the window as seconds.
    offset: Offset of this window as seconds since Unix epoch. Windows start at
      t=N * size + offset where t=0 is the epoch. The offset must be a value
      in range [0, size). If it is not it will be normalized to this range.
  """

  def __init__(self, size, offset=0):
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = Duration.of(size)
    self.offset = Timestamp.of(offset) % self.size

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
    size: Size of the window as seconds.
    period: Period of the windows as seconds.
    offset: Offset of this window as seconds since Unix epoch. Windows start at
      t=N * period + offset where t=0 is the epoch. The offset must be a value
      in range [0, period). If it is not it will be normalized to this range.
  """

  def __init__(self, size, period, offset=0):
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = Duration.of(size)
    self.period = Duration.of(period)
    self.offset = Timestamp.of(offset) % size

  def assign(self, context):
    timestamp = context.timestamp
    start = timestamp - (timestamp - self.offset) % self.period
    return [IntervalWindow(Timestamp.of(s), Timestamp.of(s) + self.size)
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
    self.gap_size = Duration.of(gap_size)

  def assign(self, context):
    timestamp = context.timestamp
    return [IntervalWindow(timestamp, timestamp + self.gap_size)]

  def merge(self, merge_context):
    to_merge = []
    end = timeutil.MIN_TIMESTAMP
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
