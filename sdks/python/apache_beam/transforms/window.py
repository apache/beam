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

# pytype: skip-file

import abc
from functools import total_ordering
from typing import Any
from typing import Generic
from typing import Iterable
from typing import List
from typing import Optional
from typing import TypeVar

from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from apache_beam.coders import coders
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import standard_window_fns_pb2
from apache_beam.transforms import timeutil
from apache_beam.utils import proto_utils
from apache_beam.utils import urns
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import DurationTypes  # pylint: disable=unused-import
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import
from apache_beam.utils.windowed_value import WindowedValue

__all__ = [
    'TimestampCombiner',
    'WindowFn',
    'BoundedWindow',
    'IntervalWindow',
    'TimestampedValue',
    'GlobalWindow',
    'NonMergingWindowFn',
    'GlobalWindows',
    'FixedWindows',
    'SlidingWindows',
    'Sessions',
]


# TODO(ccy): revisit naming and semantics once Java Apache Beam finalizes their
# behavior.
class TimestampCombiner(object):
  """Determines how output timestamps of grouping operations are assigned."""

  OUTPUT_AT_EOW = beam_runner_api_pb2.OutputTime.END_OF_WINDOW
  OUTPUT_AT_EARLIEST = beam_runner_api_pb2.OutputTime.EARLIEST_IN_PANE
  OUTPUT_AT_LATEST = beam_runner_api_pb2.OutputTime.LATEST_IN_PANE
  # TODO(robertwb): Add this to the runner API or remove it.
  OUTPUT_AT_EARLIEST_TRANSFORMED = 'OUTPUT_AT_EARLIEST_TRANSFORMED'

  @staticmethod
  def get_impl(
      timestamp_combiner: beam_runner_api_pb2.OutputTime.Enum,
      window_fn: 'WindowFn') -> timeutil.TimestampCombinerImpl:
    if timestamp_combiner == TimestampCombiner.OUTPUT_AT_EOW:
      return timeutil.OutputAtEndOfWindowImpl()
    elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_EARLIEST:
      return timeutil.OutputAtEarliestInputTimestampImpl()
    elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_LATEST:
      return timeutil.OutputAtLatestInputTimestampImpl()
    elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_EARLIEST_TRANSFORMED:
      return timeutil.OutputAtEarliestTransformedInputTimestampImpl(window_fn)
    else:
      raise ValueError('Invalid TimestampCombiner: %s.' % timestamp_combiner)


class WindowFn(urns.RunnerApiFn, metaclass=abc.ABCMeta):
  """An abstract windowing function defining a basic assign and merge."""
  class AssignContext(object):
    """Context passed to WindowFn.assign()."""
    def __init__(
        self,
        timestamp: TimestampTypes,
        element: Optional[Any] = None,
        window: Optional['BoundedWindow'] = None) -> None:
      self.timestamp = Timestamp.of(timestamp)
      self.element = element
      self.window = window

  @abc.abstractmethod
  def assign(self,
             assign_context: 'AssignContext') -> Iterable['BoundedWindow']:
    # noqa: F821

    """Associates windows to an element.

    Arguments:
      assign_context: Instance of AssignContext.

    Returns:
      An iterable of BoundedWindow.
    """
    raise NotImplementedError

  class MergeContext(object):
    """Context passed to WindowFn.merge() to perform merging, if any."""
    def __init__(self, windows: Iterable['BoundedWindow']) -> None:
      self.windows = list(windows)

    def merge(
        self,
        to_be_merged: Iterable['BoundedWindow'],
        merge_result: 'BoundedWindow') -> None:
      raise NotImplementedError

  @abc.abstractmethod
  def merge(self, merge_context: 'WindowFn.MergeContext') -> None:
    """Returns a window that is the result of merging a set of windows."""
    raise NotImplementedError

  def is_merging(self) -> bool:
    """Returns whether this WindowFn merges windows."""
    return True

  @abc.abstractmethod
  def get_window_coder(self) -> coders.Coder:
    raise NotImplementedError

  def get_transformed_output_time(
      self, window: 'BoundedWindow', input_timestamp: Timestamp) -> Timestamp:  # pylint: disable=unused-argument
    """Given input time and output window, returns output time for window.

    If TimestampCombiner.OUTPUT_AT_EARLIEST_TRANSFORMED is used in the
    Windowing, the output timestamp for the given window will be the earliest
    of the timestamps returned by get_transformed_output_time() for elements
    of the window.

    Arguments:
      window: Output window of element.
      input_timestamp: Input timestamp of element as a timeutil.Timestamp
        object.

    Returns:
      Transformed timestamp.
    """
    # By default, just return the input timestamp.
    return input_timestamp

  urns.RunnerApiFn.register_pickle_urn(python_urns.PICKLED_WINDOWFN)


class BoundedWindow(object):
  """A window for timestamps in range (-infinity, end).

  Attributes:
    end: End of window.
  """
  def __init__(self, end: TimestampTypes) -> None:
    self._end = Timestamp.of(end)

  @property
  def start(self) -> Timestamp:
    raise NotImplementedError

  @property
  def end(self) -> Timestamp:
    return self._end

  def max_timestamp(self) -> Timestamp:
    return self.end.predecessor()

  def __eq__(self, other):
    raise NotImplementedError

  def __ne__(self, other):
    #  Order first by endpoint, then arbitrarily
    return self.end != other.end or hash(self) != hash(other)

  def __lt__(self, other):
    if self.end != other.end:
      return self.end < other.end
    return hash(self) < hash(other)

  def __le__(self, other):
    if self.end != other.end:
      return self.end <= other.end
    return hash(self) <= hash(other)

  def __gt__(self, other):
    if self.end != other.end:
      return self.end > other.end
    return hash(self) > hash(other)

  def __ge__(self, other):
    if self.end != other.end:
      return self.end >= other.end
    return hash(self) >= hash(other)

  def __hash__(self):
    raise NotImplementedError

  def __repr__(self):
    return '[?, %s)' % float(self.end)


@total_ordering
class IntervalWindow(windowed_value._IntervalWindowBase, BoundedWindow):
  """A window for timestamps in range [start, end).

  Attributes:
    start: Start of window as seconds since Unix epoch.
    end: End of window as seconds since Unix epoch.
  """
  def __lt__(self, other):
    if self.end != other.end:
      return self.end < other.end
    return hash(self) < hash(other)

  def intersects(self, other: 'IntervalWindow') -> bool:
    return other.start < self.end or self.start < other.end

  def union(self, other: 'IntervalWindow') -> 'IntervalWindow':
    return IntervalWindow(
        min(self.start, other.start), max(self.end, other.end))
  
  @staticmethod
  def try_from_global_window(value) -> 'IntervalWindow':
    gw = GlobalWindow()
    if gw == value:
      return IntervalWindow(gw.start, GlobalWindow._getTimestampFromProto())
    return value
  
  def try_to_global_window(self) -> BoundedWindow:
    gw = GlobalWindow()
    if self.start == gw.start and self.end == GlobalWindow._getTimestampFromProto():
      return gw
    return IntervalWindow(gw.start(), GlobalWindow._getTimestampFromProto())


V = TypeVar("V")


@total_ordering
class TimestampedValue(Generic[V]):
  """A timestamped value having a value and a timestamp.

  Attributes:
    value: The underlying value.
    timestamp: Timestamp associated with the value as seconds since Unix epoch.
  """
  def __init__(self, value: V, timestamp: TimestampTypes) -> None:
    self.value = value
    self.timestamp = Timestamp.of(timestamp)

  def __eq__(self, other):
    return (
        type(self) == type(other) and self.value == other.value and
        self.timestamp == other.timestamp)

  def __hash__(self):
    return hash((self.value, self.timestamp))

  def __lt__(self, other):
    if type(self) != type(other):
      return type(self).__name__ < type(other).__name__
    if self.value != other.value:
      return self.value < other.value
    return self.timestamp < other.timestamp


class GlobalWindow(BoundedWindow):
  """The default window into which all data is placed (via GlobalWindows)."""
  _instance: Optional['GlobalWindow'] = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super(GlobalWindow, cls).__new__(cls)
    return cls._instance

  def __init__(self) -> None:
    super().__init__(GlobalWindow._getTimestampFromProto())

  def __repr__(self):
    return 'GlobalWindow'

  def __hash__(self):
    return hash(type(self))

  def __eq__(self, other):
    return self is other or type(self) is type(other)

  @property
  def start(self) -> Timestamp:
    return MIN_TIMESTAMP

  @staticmethod
  def _getTimestampFromProto() -> Timestamp:
    ts_millis = int(
        common_urns.constants.GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS.constant)
    return Timestamp(micros=ts_millis * 1000)


class NonMergingWindowFn(WindowFn):
  def is_merging(self) -> bool:
    return False

  def merge(self, merge_context: WindowFn.MergeContext) -> None:
    pass  # No merging.


class GlobalWindows(NonMergingWindowFn):
  """A windowing function that assigns everything to one global window."""
  @classmethod
  def windowed_batch(
      cls,
      batch: Any,
      timestamp: Timestamp = MIN_TIMESTAMP,
      pane_info: windowed_value.PaneInfo = windowed_value.PANE_INFO_UNKNOWN
  ) -> windowed_value.WindowedBatch:
    return windowed_value.HomogeneousWindowedBatch.of(
        batch, timestamp, (GlobalWindow(), ), pane_info)

  @classmethod
  def windowed_value(
      cls,
      value: Any,
      timestamp: Timestamp = MIN_TIMESTAMP,
      pane_info: windowed_value.PaneInfo = windowed_value.PANE_INFO_UNKNOWN
  ) -> WindowedValue:
    return WindowedValue(value, timestamp, (GlobalWindow(), ), pane_info)

  @classmethod
  def windowed_value_at_end_of_window(cls, value):
    return cls.windowed_value(value, GlobalWindow().max_timestamp())

  def assign(self,
             assign_context: WindowFn.AssignContext) -> List[GlobalWindow]:
    return [GlobalWindow()]

  def get_window_coder(self) -> coders.GlobalWindowCoder:
    return coders.GlobalWindowCoder()

  def __hash__(self):
    return hash(type(self))

  def __eq__(self, other):
    # Global windowfn is always and only equal to each other.
    return self is other or type(self) is type(other)

  def to_runner_api_parameter(self, context):
    return common_urns.global_windows.urn, None

  @staticmethod
  @urns.RunnerApiFn.register_urn(common_urns.global_windows.urn, None)
  def from_runner_api_parameter(
      unused_fn_parameter, unused_context) -> 'GlobalWindows':
    return GlobalWindows()


class FixedWindows(NonMergingWindowFn):
  """A windowing function that assigns each element to one time interval.

  The attributes size and offset determine in what time interval a timestamp
  will be slotted. The time intervals have the following formula:
  [N * size + offset, (N + 1) * size + offset)

  Attributes:
    size: Size of the window as seconds.
    offset: Offset of this window as seconds. Windows start at
      t=N * size + offset where t=0 is the UNIX epoch. The offset must be a
      value in range [0, size). If it is not it will be normalized to this
      range.
  """
  def __init__(self, size: DurationTypes, offset: TimestampTypes = 0):
    """Initialize a ``FixedWindows`` function for a given size and offset.

    Args:
      size (int): Size of the window in seconds.
      offset(int): Offset of this window as seconds. Windows start at
        t=N * size + offset where t=0 is the UNIX epoch. The offset must be a
        value in range [0, size). If it is not it will be normalized to this
        range.
    """
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = Duration.of(size)
    self.offset = Timestamp.of(offset) % self.size

  def assign(self, context: WindowFn.AssignContext) -> List[IntervalWindow]:
    timestamp = context.timestamp
    start = timestamp - (timestamp - self.offset) % self.size
    return [IntervalWindow(start, start + self.size)]

  def get_window_coder(self) -> coders.IntervalWindowCoder:
    return coders.IntervalWindowCoder()

  def __eq__(self, other):
    if type(self) == type(other) == FixedWindows:
      return self.size == other.size and self.offset == other.offset

  def __hash__(self):
    return hash((self.size, self.offset))

  def to_runner_api_parameter(self, context):
    return (
        common_urns.fixed_windows.urn,
        standard_window_fns_pb2.FixedWindowsPayload(
            size=proto_utils.from_micros(
                duration_pb2.Duration, self.size.micros),
            offset=proto_utils.from_micros(
                timestamp_pb2.Timestamp, self.offset.micros)))

  @staticmethod
  @urns.RunnerApiFn.register_urn(
      common_urns.fixed_windows.urn,
      standard_window_fns_pb2.FixedWindowsPayload)
  def from_runner_api_parameter(fn_parameter, unused_context) -> 'FixedWindows':
    return FixedWindows(
        size=Duration(micros=fn_parameter.size.ToMicroseconds()),
        offset=Timestamp(micros=fn_parameter.offset.ToMicroseconds()))


class SlidingWindows(NonMergingWindowFn):
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
  def __init__(
      self,
      size: DurationTypes,
      period: DurationTypes,
      offset: TimestampTypes = 0,
  ):
    if size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.size = Duration.of(size)
    self.period = Duration.of(period)
    self.offset = Timestamp.of(offset) % period

  def assign(self, context: WindowFn.AssignContext) -> List[IntervalWindow]:
    timestamp = context.timestamp
    start = timestamp - ((timestamp - self.offset) % self.period)
    return [
        IntervalWindow(
            (interval_start := Timestamp(micros=s)),
            interval_start + self.size,
        ) for s in range(
            start.micros,
            timestamp.micros - self.size.micros,
            -self.period.micros)
    ]

  def get_window_coder(self) -> coders.IntervalWindowCoder:
    return coders.IntervalWindowCoder()

  def __eq__(self, other):
    if type(self) == type(other) == SlidingWindows:
      return (
          self.size == other.size and self.offset == other.offset and
          self.period == other.period)

  def __hash__(self):
    return hash((self.offset, self.period))

  def to_runner_api_parameter(self, context):
    return (
        common_urns.sliding_windows.urn,
        standard_window_fns_pb2.SlidingWindowsPayload(
            size=proto_utils.from_micros(
                duration_pb2.Duration, self.size.micros),
            offset=proto_utils.from_micros(
                timestamp_pb2.Timestamp, self.offset.micros),
            period=proto_utils.from_micros(
                duration_pb2.Duration, self.period.micros)))

  @staticmethod
  @urns.RunnerApiFn.register_urn(
      common_urns.sliding_windows.urn,
      standard_window_fns_pb2.SlidingWindowsPayload)
  def from_runner_api_parameter(
      fn_parameter, unused_context) -> 'SlidingWindows':
    return SlidingWindows(
        size=Duration(micros=fn_parameter.size.ToMicroseconds()),
        offset=Timestamp(micros=fn_parameter.offset.ToMicroseconds()),
        period=Duration(micros=fn_parameter.period.ToMicroseconds()))


class Sessions(WindowFn):
  """A windowing function that groups elements into sessions.

  A session is defined as a series of consecutive events
  separated by a specified gap size.

  Attributes:
    gap_size: Size of the gap between windows as floating-point seconds.
  """
  def __init__(self, gap_size: DurationTypes) -> None:
    if gap_size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.gap_size = Duration.of(gap_size)

  def assign(self, context: WindowFn.AssignContext) -> List[IntervalWindow]:
    timestamp = context.timestamp
    return [IntervalWindow(timestamp, timestamp + self.gap_size)]

  def get_window_coder(self) -> coders.IntervalWindowCoder:
    return coders.IntervalWindowCoder()

  def merge(self, merge_context: WindowFn.MergeContext) -> None:
    to_merge: List[BoundedWindow] = []
    end = MIN_TIMESTAMP
    for w in sorted(merge_context.windows, key=lambda w: w.start):
      if to_merge:
        if end > w.start:
          to_merge.append(w)
          if w.end > end:
            end = w.end
        else:
          if len(to_merge) > 1:
            merge_context.merge(
                to_merge, IntervalWindow(to_merge[0].start, end))
          to_merge = [w]
          end = w.end
      else:
        to_merge = [w]
        end = w.end
    if len(to_merge) > 1:
      merge_context.merge(to_merge, IntervalWindow(to_merge[0].start, end))

  def __eq__(self, other):
    if type(self) == type(other) == Sessions:
      return self.gap_size == other.gap_size

  def __hash__(self):
    return hash(self.gap_size)

  def to_runner_api_parameter(self, context):
    return (
        common_urns.session_windows.urn,
        standard_window_fns_pb2.SessionWindowsPayload(
            gap_size=proto_utils.from_micros(
                duration_pb2.Duration, self.gap_size.micros)))

  @staticmethod
  @urns.RunnerApiFn.register_urn(
      common_urns.session_windows.urn,
      standard_window_fns_pb2.SessionWindowsPayload)
  def from_runner_api_parameter(fn_parameter, unused_context) -> 'Sessions':
    return Sessions(
        gap_size=Duration(micros=fn_parameter.gap_size.ToMicroseconds()))
