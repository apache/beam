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

import enum
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.coders import PickleCoder
from apache_beam.coders import TimestampCoder
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import OrderedListStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints.typehints import TupleConstraint
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import DurationTypes  # pylint: disable=unused-import
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import

_LOGGER = logging.getLogger("ordered_window_elements")
"""An example putting elements into window in time order on a streaming setting.

The PTransform is a turn-key transform that can handle different input window
settings and element types.

Not only does it buffer elements, it can also prepend a window with
the last seen element if the window is empty or there is a gap between
the beginning of the window and the timestamp of its first element.
"""


class OrderedWindowElementsDoFn(beam.DoFn):
  """A Stateful DoFn that buffers and emits elements in time-ordered windows.

  This DoFn uses Beam's stateful processing capabilities to buffer elements
  and emit them in order within sliding windows. It handles out-of-order data,
  late data, and can fill starting gaps in windows by leveraging states and
  timers.

  Attributes:
    BUFFER_STATE: A `StateSpec` for storing incoming elements (timestamp, value)
      in a time-ordered buffer.
    WINDOW_TIMER: A `TimerSpec` set to the watermark time domain, used to
      trigger the emission of windowed elements.
    TIMER_STATE: A `ReadModifyWriteStateSpec` (BooleanCoder) to track whether
      the window timer has been initialized and set for the current key.
    LAST_VALUE: A `ReadModifyWriteStateSpec` (PickleCoder) to store the last
      emitted value for a key, used to fill the start of a window if there is a
      gap.
    BUFFER_MIN_TS_STATE: A `ReadModifyWriteStateSpec` (TimestampCoder) to
      keep track of the minimum timestamp currently present in the
      `buffer_state` for efficient clearing.
    ESTIMATED_WM_STATE: A `ReadModifyWriteStateSpec` (TimestampCoder) to
      store the highest observed timestamp for a key, used as an estimated
      watermark to detect and filter excessively late data.
  """
  BUFFER_STATE = OrderedListStateSpec('buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)
  TIMER_STATE = ReadModifyWriteStateSpec('timer_state', BooleanCoder())
  LAST_VALUE = ReadModifyWriteStateSpec('last_value', PickleCoder())
  BUFFER_MIN_TS_STATE = ReadModifyWriteStateSpec(
      'buffer_min_ts', TimestampCoder())
  ESTIMATED_WM_STATE = ReadModifyWriteStateSpec(
      'estimated_wm', TimestampCoder())

  def __init__(
      self,
      duration: DurationTypes,
      slide_interval: DurationTypes,
      offset: DurationTypes,
      allowed_lateness: DurationTypes,
      default_start_value,
      fill_start_if_missing: bool,
      stop_timestamp: Optional[TimestampTypes]):
    """Initializes the OrderedWindowElementsFn.

    Args:
      duration: The duration of each window.
      slide_interval: The interval at which windows slide.
      offset: The offset of the window boundaries. Windows start at `offset`
        past each `duration` interval.
      allowed_lateness: The duration for which late data is still processed
        after the window's end.
      default_start_value: The default value to prepend or emit if a window
        is empty and `fill_start_if_missing` is true.
      fill_start_if_missing: A boolean indicating whether to prepend the
        last seen value to a window that has missing values at its start.
      stop_timestamp: An optional `Timestamp` at which to stop processing
        and firing timers for this key.
    """
    self.duration = duration
    self.slide_interval = slide_interval
    self.offset = offset
    self.allowed_lateness = allowed_lateness
    self.default_start_value = default_start_value
    self.fill_start_if_missing = fill_start_if_missing
    self.stop_timestamp = stop_timestamp

  def start_bundle(self):
    _LOGGER.info("start bundle")

  def finish_bundle(self):
    _LOGGER.info("finish bundle")

  def _state_add(self, buffer_state, timestamp, value):
    """Add a timestamped-value into the buffer state."""
    buffer_state.add((timestamp, value))

  def _state_read_range(self, buffer_state, range_lo, range_hi):
    """Retrieves a specified range of elements from the buffer state."""
    return list(buffer_state.read_range(range_lo, range_hi))

  def _state_clear_range(self, buffer_state, range_lo, range_hi):
    """Clears a specified range of elements from the buffer state."""
    # TODO: Dataflow runner v2 gets stuck when MIN_TIMESTAMP is used
    # as the lower bound for clear_range. Investigate this further.
    buffer_state.clear_range(range_lo, range_hi)

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      last_value_state=beam.DoFn.StateParam(LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(BUFFER_MIN_TS_STATE),
      estimated_wm_state=beam.DoFn.StateParam(ESTIMATED_WM_STATE),
  ):
    """Processes incoming elements, buffering them and setting timers.

    This method receives elements, updates the estimated watermark, buffers
    the element in `buffer_state`, and sets an initial window timer if
    one hasn't been set yet for the current key. It also handles the
    `fill_start_if_missing` logic for the `last_value_state`.

    Args:
      element: A `(key, value)` tuple representing the input element.
      timestamp: The event-time timestamp of the element.
      buffer_state: The `State` instance for buffering elements.
      window_timer: The `Timer` instance for scheduling window firings.
      timer_state: The `ReadModifyWriteState` instance to check/set if the
        timer has been initialized.
      last_value_state: The `ReadModifyWriteState` instance for the last
        emitted value, used for filling gaps.
      buffer_min_ts_state: The `ReadModifyWriteState` instance for the
        minimum timestamp in the buffer.
      estimated_wm_state: The `ReadModifyWriteState` instance for the
        estimated watermark.

    Returns:
      An empty list, as elements are emitted by the `on_timer` method, not
      directly by `process`.
    """
    _, value = element
    _LOGGER.info(
        "[process] received element %s at timestamp %s", element, timestamp)

    estimated_wm = estimated_wm_state.read()
    if not estimated_wm or estimated_wm < timestamp:
      estimated_wm = timestamp
      estimated_wm_state.write(estimated_wm)
    else:
      # If the element is too late for the current watermark, drop it.
      if estimated_wm > timestamp + self.allowed_lateness:
        _LOGGER.info(
            "[process] data %s at %s is too late for watermark %s; dropping.",
            element,
            timestamp,
            estimated_wm)
        return []

    buffer_min_ts = buffer_min_ts_state.read()
    if not buffer_min_ts or timestamp < buffer_min_ts:
      buffer_min_ts_state.write(timestamp)

    self._state_add(buffer_state, timestamp, value)

    timer_started = timer_state.read()
    if not timer_started:
      offset_duration = Duration.of(self.offset)
      slide_duration = Duration.of(self.slide_interval)
      duration_duration = Duration.of(self.duration)

      # Align the timestamp with the windowing scheme.
      aligned_micros = (timestamp - offset_duration).micros

      # Calculate the start of the last window that could contain this timestamp
      last_window_start_aligned_micros = (
          (aligned_micros // slide_duration.micros) * slide_duration.micros)

      last_window_start = Timestamp(
          micros=last_window_start_aligned_micros) + offset_duration
      n = (duration_duration.micros - 1) // slide_duration.micros
      # Calculate the start of the first sliding window.
      first_slide_start_ts = last_window_start - Duration(
          micros=n * slide_duration.micros)

      # Set the initial timer to fire at the end of the first window plus
      # allowed lateness.
      first_window_end_ts = first_slide_start_ts + self.duration
      _LOGGER.info(
          "[process] setting initial timer to %s",
          first_window_end_ts + self.allowed_lateness)
      if (self.stop_timestamp is None or
          first_window_end_ts + self.allowed_lateness < self.stop_timestamp):
        window_timer.set(first_window_end_ts + self.allowed_lateness)

      timer_state.write(True)

    if self.fill_start_if_missing:
      last_value = last_value_state.read()
      if not last_value:
        last_value_state.write((MIN_TIMESTAMP, self.default_start_value))
    return []

  def _get_windowed_values_from_state(
      self, buffer_state, window_start_ts, window_end_ts, last_value_state):
    """Retrieves values for a window from the state, handling missing data.

    This helper method reads elements within a given window range from the
    buffer state. If `fill_start_if_missing` is enabled, it prepends
    the `last_value` if the window is initially empty or if there's a gap
    between the window start and the first element. It also updates the
    `last_value_state` with the last relevant element for the next window.

    Args:
      buffer_state: The state instance containing buffered elements.
      window_start_ts: The start timestamp of the window.
      window_end_ts: The end timestamp of the window.
      last_value_state: The `ReadModifyWriteState` instance storing the last
        emitted value.

    Returns:
      A list of `(timestamp, value)` tuples for the current window, potentially
      including a prepended last value if `fill_start_if_missing` is true.
    """
    windowed_values = self._state_read_range(
        buffer_state, window_start_ts, window_end_ts)
    _LOGGER.info(
        "[on_timer] windowed data in buffer (%d): %s",
        len(windowed_values),
        windowed_values)

    if self.fill_start_if_missing:
      if not windowed_values:
        # If the window is empty, use the last value.
        last_value = last_value_state.read()
        value_to_insert = (window_start_ts, last_value[1])
        windowed_values.append(value_to_insert)
      else:
        first_timestamp = windowed_values[0][0]
        last_value = last_value_state.read()
        if first_timestamp > window_start_ts and last_value:
          # Prepend the last value if there's a gap between the first element
          # in the window and the start of the window.
          value_to_insert = (window_start_ts, last_value[1])
          windowed_values = [value_to_insert] + windowed_values

      # Find the last element before the beginning of the next window to update
      # last_value_state.
      i = 0
      for v in windowed_values:
        if v[0] >= window_start_ts + self.slide_interval:
          break
        i += 1

      if i > 0:
        last_value = windowed_values[i - 1]
        last_value_state.write(last_value)
    return windowed_values

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      key=beam.DoFn.KeyParam,
      fire_ts=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      last_value_state=beam.DoFn.StateParam(LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(BUFFER_MIN_TS_STATE),
  ):
    """Handles timer firings to emit windowed elements.

    When the `WINDOW_TIMER` fires, this method extracts elements for the
    current window from the `buffer_state`, handles late-firing windows
    (if `allowed_lateness` > 0), and emits them as a windowed `PCollection`.
    It also clears processed elements from the buffer and sets the next timer.

    Args:
      key: The key for which the timer fired.
      fire_ts: The event-time timestamp at which the timer fired.
      buffer_state: The `State` instance containing buffered
        elements.
      window_timer: The `Timer` instance for scheduling subsequent timers.
      last_value_state: The `ReadModifyWriteState` instance for the last
        emitted value.
      buffer_min_ts_state: The `ReadModifyWriteState` instance for the
        minimum timestamp in the buffer.

    Yields:
      `TimestampedValue`: A tuple `((key, window_start_ts, window_end_ts),
      list_of_values)` where `list_of_values` are the elements windowed and
      ordered, timestamped at `window_end_ts - 1`.
    """
    _LOGGER.info("[on_timer] timer fired at %s", fire_ts)

    window_end_ts = fire_ts - self.allowed_lateness
    window_start_ts = window_end_ts - self.duration
    buffer_min_ts = buffer_min_ts_state.read()
    if not buffer_min_ts or buffer_min_ts > window_start_ts:
      buffer_min_ts = window_start_ts

    if self.allowed_lateness > 0:
      # Emit late windows that occurred prior to the current window.
      late_start_ts = window_start_ts
      while late_start_ts > buffer_min_ts:
        late_start_ts -= self.slide_interval

      while late_start_ts < window_start_ts:
        late_end_ts = late_start_ts + self.duration
        _LOGGER.info(
            "[on_timer] emitting late window: start=%s, end=%s",
            late_start_ts,
            late_end_ts)
        windowed_values = self._get_windowed_values_from_state(
            buffer_state, late_start_ts, late_end_ts, last_value_state)
        yield TimestampedValue(
            (key, ((late_start_ts, late_end_ts), windowed_values)),
            late_end_ts - 1)
        late_start_ts += self.slide_interval

    # Read and emit elements for the on-time window.
    _LOGGER.info(
        "[on_timer] emitting on-time window: start=%s, end=%s",
        window_start_ts,
        window_end_ts)
    windowed_values = self._get_windowed_values_from_state(
        buffer_state, window_start_ts, window_end_ts, last_value_state)
    yield TimestampedValue(
        (key, ((window_start_ts, window_end_ts), windowed_values)),
        window_end_ts - 1)

    # Post-emit actions for the current window:
    # - Compute the next window's start and end timestamps.
    # - Clean up states for expired windows.
    # - Set a new timer for the next window.
    next_window_end_ts = fire_ts - self.allowed_lateness + self.slide_interval
    next_window_start_ts = window_start_ts + self.slide_interval
    _LOGGER.info(
        "[on_timer] clearing timestamp range [%s, %s]",
        buffer_min_ts,
        next_window_start_ts)

    self._state_clear_range(buffer_state, buffer_min_ts, next_window_start_ts)
    buffer_min_ts_state.write(next_window_start_ts)

    _LOGGER.info(
        "[on_timer] setting follow-up timer to %s",
        next_window_end_ts + self.allowed_lateness)
    if (self.stop_timestamp is None or
        next_window_end_ts + self.allowed_lateness < self.stop_timestamp):
      window_timer.set(next_window_end_ts + self.allowed_lateness)


class OrderedWindowElementsDoFnWithBag(OrderedWindowElementsDoFn):
  """The implementation of stateful Dofn with BagState as buffer state"""

  BUFFER_STATE = BagStateSpec('buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)

  def _state_add(self, buffer_state, timestamp, value):
    """Add a timestamped-value into the buffer state."""
    buffer_state.add((timestamp, value))

  def _state_read_range(self, buffer_state, range_lo, range_hi):
    """Retrieves a specified range of elements from the buffer state."""
    all_elements = list(buffer_state.read())
    filtered_elements = [(ts, val) for ts, val in all_elements
                         if range_lo <= ts < range_hi]
    filtered_elements.sort(key=lambda x: x[0])
    return filtered_elements

  def _state_clear_range(self, buffer_state, range_lo, range_hi):
    """Clears a specified range of elements from the buffer state."""
    remaining_elements = self._state_read_range(
        buffer_state, range_hi, MAX_TIMESTAMP)
    buffer_state.clear()
    for e in remaining_elements:
      buffer_state.add(e)

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(OrderedWindowElementsDoFn.TIMER_STATE),
      last_value_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.BUFFER_MIN_TS_STATE),
      estimated_wm_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.ESTIMATED_WM_STATE),
  ):
    yield from super().process(
        element,
        timestamp,
        buffer_state,
        window_timer,
        timer_state,
        last_value_state,
        buffer_min_ts_state,
        estimated_wm_state)

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      key=beam.DoFn.KeyParam,
      fire_ts=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      last_value_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.BUFFER_MIN_TS_STATE),
  ):
    yield from super().on_timer(
        key=key,
        fire_ts=fire_ts,
        buffer_state=buffer_state,
        window_timer=window_timer,
        last_value_state=last_value_state,
        buffer_min_ts_state=buffer_min_ts_state)


class OrderedWindowElementsDoFnWithValue(OrderedWindowElementsDoFn):
  """The implementation of stateful Dofn with ValueState as buffer state"""

  BUFFER_STATE = ReadModifyWriteStateSpec('buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)

  def _state_add(self, buffer_state, timestamp, value):
    """Add a timestamped-value into the buffer state."""
    buffer = buffer_state.read() or []
    buffer.append((timestamp, value))
    buffer_state.write(buffer)

  def _state_read_range(self, buffer_state, range_lo, range_hi):
    """Retrieves a specified range of elements from the buffer state."""
    all_elements = buffer_state.read()
    filtered_elements = [(ts, val) for ts, val in all_elements
                         if range_lo <= ts < range_hi]
    filtered_elements.sort(key=lambda x: x[0])
    return filtered_elements

  def _state_clear_range(self, buffer_state, range_lo, range_hi):
    """Clears a specified range of elements from the buffer state."""
    remaining_elements = self._state_read_range(
        buffer_state, range_hi, MAX_TIMESTAMP)
    buffer_state.write(remaining_elements)

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(OrderedWindowElementsDoFn.TIMER_STATE),
      last_value_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.BUFFER_MIN_TS_STATE),
      estimated_wm_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.ESTIMATED_WM_STATE),
  ):
    yield from super().process(
        element,
        timestamp,
        buffer_state,
        window_timer,
        timer_state,
        last_value_state,
        buffer_min_ts_state,
        estimated_wm_state)

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      key=beam.DoFn.KeyParam,
      fire_ts=beam.DoFn.TimestampParam,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      last_value_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(
          OrderedWindowElementsDoFn.BUFFER_MIN_TS_STATE),
  ):
    yield from super().on_timer(
        key=key,
        fire_ts=fire_ts,
        buffer_state=buffer_state,
        window_timer=window_timer,
        last_value_state=last_value_state,
        buffer_min_ts_state=buffer_min_ts_state)


class BufferStateType(enum.Enum):
  ORDERED_LIST = 0
  BAG = 1
  VALUE = 2


class OrderedWindowElements(PTransform):
  """A PTransform that batches elements into ordered, sliding windows.

  This transform processes elements with timestamps, buffering them and
  emitting them in fixed or sliding windows. It supports late data handling
  and can fill missing initial values in a window.
  """
  def __init__(
      self,
      duration: DurationTypes,
      slide_interval: Optional[DurationTypes] = None,
      offset: DurationTypes = 0,
      allowed_lateness: DurationTypes = 0,
      default_start_value=None,
      fill_start_if_missing: bool = False,
      stop_timestamp: Optional[TimestampTypes] = None,
      buffer_state_type: BufferStateType = BufferStateType.ORDERED_LIST,
  ):
    """Initializes the OrderedWindowElements transform.

    Args:
      duration: The duration of each window.
      slide_interval: The interval at which windows slide. Defaults to
        `duration` if not provided (i.e., fixed windows).
      offset: The offset of the window boundaries.
      allowed_lateness: The maximum amount of time an element can be late and
        still be processed.
      default_start_value: The default value to use if `fill_start_if_missing`
        is true and a window is empty at its start.
      fill_start_if_missing: If true, the transform will attempt to fill the
        beginning of a window with the last known value if no elements are
        present at the window's start.
      stop_timestamp: An optional timestamp to stop processing and firing
        timers.
      buffer_state_type: An optional enum to control what backend state to use
        to store buffered elements. By default, it is using ordered list state.
    """
    self.duration = duration
    self.slide_interval = duration if slide_interval is None else slide_interval
    self.offset = offset
    self.allowed_lateness = allowed_lateness
    self.default_start_value = default_start_value
    self.fill_start_if_missing = fill_start_if_missing
    self.stop_timestamp = stop_timestamp
    self.buffer_state_type = buffer_state_type

  def expand(self, input):
    """Applies the OrderedWindowElements transform to the input PCollection.

    The input PCollection is first ensured to be in `GlobalWindows`. If it's
    unkeyed, a default key is added. The `OrderedWindowElementsFn` is then
    applied. If the input was originally unkeyed, the default key is removed.

    Args:
      input: The input `PCollection`. Can be keyed (e.g.,
        `PCollection[Tuple[K, V]]`) or unkeyed (e.g., `PCollection[V]`).

    Returns:
      A `PCollection` of `((key, window_start, window_end), list_of_values)`
      (if input was keyed) or `list_of_values` (if input was unkeyed), where
      `list_of_values` are the elements windowed and ordered.
    """
    windowing = input.windowing
    if not isinstance(windowing.windowfn, GlobalWindows):
      _LOGGER.warning(
          'Input PCollection is not in GlobalWindows. Overwriting windowing '
          'function with GlobalWindows.')
      input = input | "ToGlobalWindows" >> beam.WindowInto(GlobalWindows())

    if isinstance(input.element_type, TupleConstraint):
      keyed_input = input
    else:
      # Add a default key (0) if the input PCollection is unkeyed.
      keyed_input = input | beam.WithKeys(0)

    if self.buffer_state_type == BufferStateType.ORDERED_LIST:
      dofn = OrderedWindowElementsDoFn
    elif self.buffer_state_type == BufferStateType.BAG:
      dofn = OrderedWindowElementsDoFnWithBag
    elif self.buffer_state_type == BufferStateType.VALUE:
      dofn = OrderedWindowElementsDoFnWithValue
    else:
      raise ValueError("Unknown buffer_state_type: " + self.buffer_state_type)

    keyed_output = (
        keyed_input | 'Ordered Sliding Window' >> beam.ParDo(
            dofn(
                self.duration,
                self.slide_interval,
                self.offset,
                self.allowed_lateness,
                self.default_start_value,
                self.fill_start_if_missing,
                self.stop_timestamp)))

    if isinstance(input.element_type, TupleConstraint):
      ret = keyed_output
    else:
      # Remove the default key if the input PCollection was originally unkeyed.
      ret = keyed_output | beam.Values()

    return ret
