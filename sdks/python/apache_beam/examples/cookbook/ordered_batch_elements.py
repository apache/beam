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

import logging

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.coders import PickleCoder
from apache_beam.coders import TimestampCoder
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import OrderedListStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.utils.timestamp import DurationTypes  # pylint: disable=unused-import
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints.typehints import TupleConstraint

_LOGGER = logging.getLogger("ordered_batch_elements")
"""An example of using states and timers to batch elements in time order.

The PTransform is a turn-key transform that can handle different input window
settings and element types.

Not only does it buffer elements, it will also prepend a window with
the last seen element if the window is empty or there is a gap between
the beginning of the window and the timestamp of its first element.
This facilitates the subsequent forward-filling operation.
"""


class OrderedBatchElementsDoFn(beam.DoFn):
  """A DoFn that batches elements into ordered, fixed-size windows.

  This DoFn uses Beam's stateful processing capabilities to buffer elements
  and emit them in order within fixed windows. It handles out-of-order data,
  late data, and idle periods by leveraging `OrderedListState`, `TimerSpec`,
  and `ReadModifyWriteStateSpec`.

  Attributes:
    ORDERED_BUFFER_STATE: An `OrderedListStateSpec` for storing incoming
      elements (timestamp, value) in a time-ordered buffer.
    WINDOW_TIMER: A `TimerSpec` set to the watermark time domain, used to
      trigger the emission of batched elements at window boundaries.
    TIMER_STATE: A `ReadModifyWriteStateSpec` (BooleanCoder) to track whether
      the window timer has been initialized and set for the current key.
    IDLE_COUNT: A `ReadModifyWriteStateSpec` (VarIntCoder) to count
      consecutive empty windows, used to detect and potentially abort
      processing for idle keys.
    LAST_VALUE: A `ReadModifyWriteStateSpec` (PickleCoder) to store the last
      emitted value for a key, used to fill gaps in windows or provide a
      default for empty windows.
    BUFFER_MIN_TS_STATE: A `ReadModifyWriteStateSpec` (TimestampCoder) to
      keep track of the minimum timestamp currently present in the
      `ordered_buffer` for efficient clearing.
    ESTIMATED_WM_STATE: A `ReadModifyWriteStateSpec` (TimestampCoder) to
      store the highest observed timestamp for a key, used as an estimated
      watermark to detect and filter excessively late data.
  """
  ORDERED_BUFFER_STATE = OrderedListStateSpec('ordered_buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)
  TIMER_STATE = ReadModifyWriteStateSpec('timer_state', BooleanCoder())
  IDLE_COUNT = ReadModifyWriteStateSpec('idle_count', VarIntCoder())
  LAST_VALUE = ReadModifyWriteStateSpec('last_value', PickleCoder())
  BUFFER_MIN_TS_STATE = ReadModifyWriteStateSpec(
      'buffer_min_ts', TimestampCoder())
  ESTIMATED_WM_STATE = ReadModifyWriteStateSpec(
      'estimated_wm', TimestampCoder())

  def __init__(
      self,
      size,
      offset,
      default,
      allowed_lateness: DurationTypes = 0,
      max_idle_count=10):
    """Initializes the OrderedBatchElementsDoFn.

    Args:
      size: The duration of the fixed window (e.g., `Duration(seconds=60)`).
      offset: The offset of the fixed window from the Unix epoch
        (e.g., `Duration(seconds=0)`).
      default: The default value to prepend or emit if a window is empty.
      allowed_lateness: The duration for which late data is still processed
        after the window's end (e.g., `Duration(seconds=30)`).
      max_idle_count: The maximum number of consecutive empty windows for a
        key before processing for that key is considered idle and potentially
        aborted.
    """
    self.window_size = size
    self.slide_interval = size
    self.offset = offset
    self.default = default
    self.allowed_lateness = allowed_lateness
    self.max_idle_count = max_idle_count

  def start_bundle(self):
    """Logs the start of a bundle."""
    _LOGGER.info("start bundle")

  def finish_bundle(self):
    """Logs the finish of a bundle."""
    _LOGGER.info("finish bundle")

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      idle_count_state=beam.DoFn.StateParam(IDLE_COUNT),
      last_value_state=beam.DoFn.StateParam(LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(BUFFER_MIN_TS_STATE),
      estimated_wm_state=beam.DoFn.StateParam(ESTIMATED_WM_STATE),
  ):
    """Processes incoming elements, buffering them and setting timers.

    This method receives elements, updates the estimated watermark, buffers
    the element in `ordered_buffer`, and sets an initial window timer if
    one hasn't been set yet for the current key. It also resets the idle
    count.

    Args:
      element: A `(key, value)` tuple representing the input element.
      timestamp: The event-time timestamp of the element.
      ordered_buffer: The `OrderedListState` instance for buffering elements.
      window_timer: The `Timer` instance for scheduling window firings.
      timer_state: The `ReadModifyWriteState` instance to check/set if the
        timer has been initialized.
      idle_count_state: The `ReadModifyWriteState` instance for the idle
        counter.
      last_value_state: The `ReadModifyWriteState` instance for the last
        emitted value.
      buffer_min_ts_state: The `ReadModifyWriteState` instance for the
        minimum timestamp in the buffer.
      estimated_wm_state: The `ReadModifyWriteState` instance for the
        estimated watermark.

    Returns:
      An empty list, as elements are emitted by the `on_timer` method, not
      directly by `process`.
    """
    _, value = element
    _LOGGER.info("[process] receive %s at %s", element, timestamp)

    estimated_wm = estimated_wm_state.read()
    if not estimated_wm or estimated_wm < timestamp:
      estimated_wm = timestamp
      estimated_wm_state.write(estimated_wm)
    else:
      if estimated_wm > timestamp + self.allowed_lateness:
        _LOGGER.info(
            "[process] data %s at %s too late for watermark %s",
            element,
            timestamp,
            estimated_wm)
        return []

    buffer_min_ts = buffer_min_ts_state.read()
    if not buffer_min_ts or timestamp < buffer_min_ts:
      buffer_min_ts_state.write(timestamp)

    ordered_buffer.add((timestamp, value))

    timer_started = timer_state.read()
    if not timer_started:
      first_slide_start = int(
          (timestamp.micros / 1e6 - self.offset) //
          self.slide_interval) * self.slide_interval + self.offset
      first_slide_start_ts = Timestamp.of(first_slide_start)

      first_window_end_ts = first_slide_start_ts + self.window_size
      _LOGGER.info(
          "[process] set init timer to %s",
          first_window_end_ts + self.allowed_lateness)
      window_timer.set(first_window_end_ts + self.allowed_lateness)

      timer_state.write(True)

    idle_count_state.write(0)

    last_value = last_value_state.read()
    if not last_value:
      last_value_state.write((MIN_TIMESTAMP, self.default))
    return []

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      key=beam.DoFn.KeyParam,
      fire_ts=beam.DoFn.TimestampParam,
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      idle_count_state=beam.DoFn.StateParam(IDLE_COUNT),
      last_value_state=beam.DoFn.StateParam(LAST_VALUE),
      buffer_min_ts_state=beam.DoFn.StateParam(BUFFER_MIN_TS_STATE),
  ):
    """Handles timer firings to emit batched, ordered elements.

    When the `WINDOW_TIMER` fires, this method extracts elements for the
    current window from the `ordered_buffer`, handles late-firing windows
    (if `allowed_lateness` > 0), and emits them as a batched `PCollection`.
    It also manages the `idle_count`, clears processed elements from the
    buffer, and sets the next timer.

    Args:
      key: The key for which the timer fired.
      fire_ts: The event-time timestamp at which the timer fired.
      ordered_buffer: The `OrderedListState` instance containing buffered
        elements.
      window_timer: The `Timer` instance for scheduling subsequent timers.
      timer_state: The `ReadModifyWriteState` instance for the timer status.
      idle_count_state: The `ReadModifyWriteState` instance for the idle
        counter.
      last_value_state: The `ReadModifyWriteState` instance for the last
        emitted value.
      buffer_min_ts_state: The `ReadModifyWriteState` instance for the
        minimum timestamp in the buffer.

    Yields:
      `TimestampedValue`: A tuple `(key, list_of_values)` where `list_of_values`
      are the elements batched and ordered within the current fixed window,
      timestamped at `window_end_ts - 1`.
    """
    _LOGGER.info("[on_timer] timer fire at %s", fire_ts)

    window_end_ts = fire_ts - self.allowed_lateness
    window_start_ts = window_end_ts - self.window_size
    buffer_min_ts = buffer_min_ts_state.read()
    if not buffer_min_ts or buffer_min_ts > window_start_ts:
      buffer_min_ts = window_start_ts

    # For debugging purpose. In production we don't need to read all.
    all_values = list(ordered_buffer.read())
    _LOGGER.info(
        "[on_timer] all data in buffer (%d) %s", len(all_values), all_values)

    if self.allowed_lateness > 0:
      late_start_ts = window_start_ts
      while late_start_ts > buffer_min_ts:
        late_start_ts -= self.slide_interval

      while late_start_ts < window_start_ts:
        late_end_ts = late_start_ts + self.window_size
        _LOGGER.info(
            "[on_timer] late fire window start: %s, window end: %s",
            late_start_ts,
            late_end_ts)
        windowed_values = list(
            ordered_buffer.read_range(late_start_ts, late_end_ts))
        if len(windowed_values) == 0:
          last_value = last_value_state.read()
          windowed_values.append(last_value)
        else:
          first_timestamp = windowed_values[0][0]
          last_value = last_value_state.read()
          if first_timestamp > late_start_ts and last_value:
            # prepend the last value if there is a gap between the first element
            # in the window and the start of the window
            windowed_values = [
                last_value,
            ] + windowed_values

          last_value = windowed_values[-1]
          last_value_state.write(last_value)
        yield TimestampedValue((key, [v[1] for v in windowed_values]),
                               late_end_ts - 1)
        late_start_ts += self.slide_interval

    # read the elements in the window
    _LOGGER.info(
        "[on_timer] window start: %s, window end: %s",
        window_start_ts,
        window_end_ts)
    windowed_values = list(
        ordered_buffer.read_range(window_start_ts, window_end_ts))
    _LOGGER.info(
        "[on_timer] windowed data in buffer (%d) %s",
        len(windowed_values),
        windowed_values)

    next_window_end_ts = fire_ts - self.allowed_lateness + self.slide_interval
    next_window_start_ts = window_start_ts + self.slide_interval

    # Using MIN_TIMESTAMP as the lower bound of clear_range would cause dataflow
    # runner v2 becoming stuck.
    # ordered_buffer.clear_range(MIN_TIMESTAMP, next_window_start_ts)
    _LOGGER.info(
        "[on_timer] clear timestamp range [%d, %d]",
        buffer_min_ts,
        next_window_start_ts)
    ordered_buffer.clear_range(buffer_min_ts, next_window_start_ts)
    buffer_min_ts_state.write(next_window_start_ts)

    # handle the situation when idle lasting too long
    if len(windowed_values) == 0:
      idle_count = idle_count_state.read()
      idle_count += 1
      idle_count_state.write(idle_count)

      # abort if too idle
      if idle_count > self.max_idle_count:
        return

    if len(windowed_values) == 0:
      # the window is empty, use the last value
      last_value = last_value_state.read()
      windowed_values.append(last_value)
    else:
      first_timestamp = windowed_values[0][0]
      last_value = last_value_state.read()
      if first_timestamp > window_start_ts and last_value:
        # prepend the last value if there is a gap between the first element
        # in the window and the start of the window
        windowed_values = [
            last_value,
        ] + windowed_values

      last_value = windowed_values[-1]
      last_value_state.write(last_value)

    yield TimestampedValue((key, [v[1] for v in windowed_values]),
                           window_end_ts - 1)

    _LOGGER.info(
        "[on_timer] set follow-up timer to %s",
        next_window_end_ts + self.allowed_lateness)
    window_timer.set(next_window_end_ts + self.allowed_lateness)


class OrderedBatchElements(PTransform):
  """A PTransform that batches elements into ordered, fixed-size windows.

  This transform takes a PCollection (keyed or unkeyed) and applies a
  stateful `OrderedBatchElementsDoFn` to group elements by key into
  fixed-size, ordered batches. It respects event time, handles lateness,
  and ensures elements within each batch are ordered by timestamp.
  The output PCollection is re-windowed into `FixedWindows` matching the
  batching configuration.
  """
  def __init__(
      self,
      size: DurationTypes,
      offset: TimestampTypes = 0,
      default=None,
      allowed_lateness: DurationTypes = 0,
      max_idle_count=5):
    """Initializes the OrderedBatchElements PTransform.

    Args:
      size: The duration of the fixed window (e.g., `Duration(seconds=60)`).
      offset: The offset of the fixed window from the Unix epoch
        (e.g., `Duration(seconds=0)`).
      default: The default value to prepend or emit if a window is empty.
      allowed_lateness: The duration for which late data is still processed
        after the window's end (e.g., `Duration(seconds=30)`).
      max_idle_count: The maximum number of consecutive empty windows for a
        key before processing for that key is considered idle and potentially
        aborted.
    """
    self.size = size
    self.offset = offset
    self.default = default
    self.allowed_lateness = allowed_lateness
    self.max_idle_count = max_idle_count

  def expand(self, input):
    """Applies the OrderedBatchElements transform to the input PCollection.

    The input PCollection is first ensured to be in `GlobalWindows`. If it's
    unkeyed, a default key is added. The `OrderedBatchElementsDoFn` is then
    applied, followed by re-windowing the output into `FixedWindows`.
    If the input was originally unkeyed, the default key is removed.

    Args:
      input: The input `PCollection`. Can be keyed (e.g.,
        `PCollection[Tuple[K, V]]`) or unkeyed (e.g., `PCollection[V]`).

    Returns:
      A `PCollection` of `(key, list_of_values)` (if input was keyed) or
      `list_of_values` (if input was unkeyed), where `list_of_values` are
      the elements batched and ordered within each fixed window.
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
      # Add a dummy key if the input is unkeyed.
      keyed_input = input | 'AddDummyKey' >> beam.WithKeys(0)

    keyed_output = (
        keyed_input | 'RunOrderedBatchElementsDoFn' >> beam.ParDo(
            OrderedBatchElementsDoFn(
                self.size,
                self.offset,
                self.default,
                self.allowed_lateness,
                self.max_idle_count))
        | "ToFixedWindows" >> beam.WindowInto(
            FixedWindows(self.size, self.offset)))

    if isinstance(input.element_type, TupleConstraint):
      ret = keyed_output
    else:
      # Remove the dummy key if the input is unkeyed.
      ret = keyed_output | 'RemoveDummyKey' >> beam.Values()

    return ret
