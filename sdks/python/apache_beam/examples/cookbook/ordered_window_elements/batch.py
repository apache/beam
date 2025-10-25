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

import logging
from enum import Enum
from typing import Any
from typing import Callable
from typing import Optional

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.coders import PickleCoder
from apache_beam.pvalue import AsDict
from apache_beam.transforms.combiners import ToListCombineFn
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import OrderedListStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import DurationTypes  # pylint: disable=unused-import
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import


class FanOutToWindows(beam.DoFn):
  """
  Assigns each element to all the windows that contain it.

  This DoFn is used to expand a single element into multiple elements, each
  associated with a specific window.

  Args:
    duration: The duration of each window in seconds.
    slide_interval: The interval at which windows slide in seconds.
    offset: The offset for window alignment in seconds.
"""
  def __init__(self, duration, slide_interval, offset):
    self.duration = duration
    self.slide_interval = slide_interval
    self.offset = offset

  def process(self, element):
    """
    Processes an element and assigns it to relevant windows.

    Args:
      element: A tuple (timestamp, value) where timestamp is a Timestamp object
               and value is the actual element data.

    Yields:
      A tuple ((window_start, window_end), element) for each window the
      element belongs to.
    """
    timestamp = element[0]
    timestamp_secs = timestamp.micros / 1e6

    # Align the timestamp with the windowing scheme.
    aligned_timestamp = timestamp_secs - self.offset

    # Calculate the start of the last window that could contain this timestamp.
    last_window_start_aligned = ((aligned_timestamp // self.slide_interval) *
                                 self.slide_interval)
    last_window_start = last_window_start_aligned + self.offset

    # To find out the start of the first possible window that covers this
    # timestamp, we start with the last window and assume we slide backward n
    # times:
    #   first_possible_start = last_window_start - n * slide_interval
    #   first_possible_end = last_window_start - n * slide_interval + duration
    # The conditions hold:
    #   first_possible_end > timestamp.
    #   first_possible_end - slide_interval <= timestamp
    # Therefore,
    #   n < (last_window_start + duration - timestamp) / slide_interval
    #   n >= (last_window_start + duration - timestamp) / slide_interval - 1
    # The worst case is that the element is at the beginning of the slide:
    #   i.e. timestamp = last_window_start
    # And n is an integer satisfies
    #   duration / slide_interval - 1 <= n < duration / slide_interval
    # Case 1: if duration is divisible by slide_interval,
    #   then n = duration / slide_interval - 1
    # Case 2: if duration is not divisible by slide_interval,
    #   then n = duration // slide_interval
    # A unified solution is n = (duration - 1) // slide_interval
    n = (self.duration - 1) // self.slide_interval
    first_possible_start = last_window_start - n * self.slide_interval

    # We iterate from the first possible window start up to the last one.
    current_start = first_possible_start
    while current_start <= last_window_start:
      # An element is in a window [start, start + duration) if:
      # start <= timestamp < start + duration
      if current_start <= timestamp_secs < current_start + self.duration:
        yield (current_start, current_start + self.duration), element
      current_start += self.slide_interval


class FanOutToSlideBoundaries(beam.DoFn):
  """
  Assigns each element to a window representing its slide.

  This DoFn is used to group elements by the start of the slide they belong to.
  This is a preliminary step for generating context information for window gaps.

  Args:
    slide_interval: The interval at which windows slide in seconds.
    offset: The offset for window alignment in seconds.
  """
  def __init__(self, slide_interval, offset):
    self.slide_interval = slide_interval
    self.offset = offset

  def process(self, element):
    """
    Processes an element and assigns it to its corresponding slide boundary.

    Args:
      element: A tuple (timestamp, value) where timestamp is a Timestamp object
               and value is the actual element data.

    Yields:
      A tuple (slide_start, element) where slide_start is the beginning
      timestamp of the slide the element belongs to.
    """
    timestamp = element[0]
    timestamp_secs = timestamp.micros / 1e6

    # Align the timestamp with the windowing scheme.
    aligned_timestamp = timestamp_secs - self.offset

    # Calculate the start of the slide containing this timestamp.
    slide_start_aligned = ((aligned_timestamp // self.slide_interval) *
                           self.slide_interval)
    slide_start = slide_start_aligned + self.offset

    # slide_end = slide_start + self.slide_interval
    yield slide_start, element


class GenerateContextDoFn(beam.DoFn):
  """
  Generates context information for filling gaps in windows.

  This DoFn uses Beam's state and timer features to collect elements within
  slides and emit a "context" value for each slide. This context value is
  typically the element with the maximum timestamp within that slide, which
  can then be used to forward-fill empty windows or gaps at the start of
  windows.

  Args:
    duration: The duration of each window in seconds.
    slide_interval: The interval at which windows slide in seconds.
    offset: The offset for window alignment in seconds.
    default: The default value to use when no context is available.
  """
  ORDERED_BUFFER_STATE = OrderedListStateSpec('ordered_buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)
  TIMER_STATE = ReadModifyWriteStateSpec('timer_state', BooleanCoder())

  def __init__(self, duration, slide_interval, offset, default):
    self.duration = duration
    self.slide_interval = slide_interval
    self.offset = offset
    self.default = default

  def process(
      self,
      element=beam.DoFn.ElementParam,
      timestamp=beam.DoFn.TimestampParam,
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
  ):
    """
    Buffers elements and sets a timer to process them when the window closes.

    Args:
      element: The input element, expected to be (key, (slide_start, value)).
      timestamp: The timestamp of the element.
      window_timer: The timer for the current window.
      timer_state: State to track if the timer has been started.
      ordered_buffer: Ordered list state to buffer elements.
    """
    _, (slide_start, value) = element

    ordered_buffer.add((Timestamp.of(slide_start), value))

    timer_started = timer_state.read()
    if not timer_started:
      window_timer.set(GlobalWindow().end)
      timer_state.write(True)
    return []

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
  ):
    """
    Emits context results when the window timer fires.

    This method processes the buffered elements, identifies the maximum
    timestamp element for each slide, and yields context values to fill
    potential gaps in subsequent windows.

    Args:
      ordered_buffer: Ordered list state containing buffered elements.

    Yields:
      A tuple (timestamp, element) representing the context for a slide.
    """
    # Emit the context result once we collect all elements
    prev_max_timestamp_element = None
    prev_max_timestamp = MIN_TIMESTAMP
    prev_slide_start = None
    for slide_start, max_timestamp_event in ordered_buffer.read():
      event_ts = max_timestamp_event[0]
      if prev_slide_start != slide_start:
        # a new slide starts
        if prev_max_timestamp_element is not None:
          # Use the last available max timestamp element for slide between
          # the last seen slide and the current slide (which includes
          # empty slides in the middle).
          start = prev_slide_start
          while start < slide_start:
            yield (start + self.slide_interval, prev_max_timestamp_element)
            start += self.slide_interval
        else:
          yield (slide_start, (MIN_TIMESTAMP, self.default))

      prev_slide_start = slide_start

      if prev_max_timestamp < event_ts < slide_start + self.slide_interval:
        prev_max_timestamp = event_ts
        prev_max_timestamp_element = max_timestamp_event


class WindowGapStrategy(Enum):
  """
  Defines strategies for handling gaps in windows.

  Attributes:
    IGNORE: Do nothing for empty windows or gaps.
    DISCARD: Discard the window. Only applied to empty windows.
    FORWARD_FILL: Fill empty windows or gaps with the last known value.
  """
  IGNORE = 1
  DISCARD = 2
  FORWARD_FILL = 3


class WindowGapFillingDoFn(beam.DoFn):
  """
  On-demand filling the start gaps of a window or empty windows.

  This DoFn takes windowed data and a side input containing context information
  (e.g., the last element from a previous slide). It uses this context to
  fill gaps at the beginning of windows or to generate entire empty windows
  based on the configured gap filling strategies.

  Args:
    duration: The duration of each window in seconds.
    slide_interval: The interval at which windows slide in seconds.
    default: The default value to use for filling gaps.
    empty_window_strategy: The strategy for handling completely empty windows.
    window_start_gap_strategy: The strategy for handling gaps at the
                                start of non-empty windows.
  """
  def __init__(
      self,
      duration,
      slide_interval,
      default,
      empty_window_strategy,
      window_start_gap_strategy):
    self.duration = duration
    self.slide_interval = slide_interval
    self.default = default
    self.empty_window_strategy = empty_window_strategy
    self.window_start_gap_strategy = window_start_gap_strategy

  def process(self, element, context_side):
    """
    Processes a window of elements and fills gaps according to strategies.

    Args:
      element: A tuple (window, values) where window is (start_ts, end_ts)
               and values is a list of elements within that window.
      context_side: A side input (AsDict) containing context information
                    (slide_start -> max_timestamp_element) for previous slides.

    Yields:
      A tuple ((window_start, window_end), filled_values) where filled_values
      is the list of elements for the window, potentially with gaps filled.
    """
    window, values = element
    window_start_ts = Timestamp.of(window[0])

    # Part 1: Handle the current, non-empty window.
    # We get the value that should be used to fill gaps at the start of this
    # window. This value is the element with the max timestamp from the
    # *previous* slide, provided as a side input.
    context_for_current_window = context_side.get(
        window_start_ts, (window_start_ts, self.default))

    sorted_values = sorted(values, key=lambda x: x[0])
    first_element_ts = sorted_values[0][0]

    if self.window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
      # If the first element is not at the very beginning of the window,
      # prepend the context value to fill the gap.
      if first_element_ts > window_start_ts:
        _, fill_val = context_for_current_window
        sorted_values.insert(0, (window_start_ts, fill_val))

    yield (Timestamp.of(window[0]), Timestamp.of(window[1])), sorted_values

    if self.empty_window_strategy == WindowGapStrategy.DISCARD:
      # We won't emit empty windows prior to the current window under this
      # strategy
      return []

    # Part 2: Fill completely empty windows that preceded this one.
    # We iterate backwards from the current window's start time, slide by
    # slide, to find and fill any empty windows.
    prev_window_start_ts = window_start_ts - self.slide_interval
    while True:
      # Get the context for the preceding window.
      context_for_prev_window = context_side.get(
          prev_window_start_ts, (prev_window_start_ts, self.default))

      # A preceding window was empty if two conditions are met:
      # 1. Its context is the same as the current window's context. This implies
      #    that no new elements arrived in the slide(s) between them.
      # 2. The first element of the current window appeared *after* the end
      #    of the preceding window we are considering.
      is_empty = (
          context_for_prev_window == context_for_current_window and
          first_element_ts > prev_window_start_ts + self.duration)

      if is_empty:
        if self.empty_window_strategy == WindowGapStrategy.FORWARD_FILL:
          _, fill_val = context_for_prev_window
          fill_ts = prev_window_start_ts
          filled_window_values = [(fill_ts, fill_val)]
        else:
          assert (self.empty_window_strategy == WindowGapStrategy.IGNORE)
          filled_window_values = []

        yield (prev_window_start_ts,
               prev_window_start_ts + self.duration), filled_window_values
      else:
        # Stop when we find a non-empty window.
        break

      prev_window_start_ts -= self.slide_interval

    return []


def max_timestamp_element(elements):
  """
  Finds the element with the maximum timestamp from a list of elements.

  Args:
    elements: A list of elements, where each element is a tuple
              (timestamp, value).

  Returns:
    The element with the maximum timestamp, or None if the list is empty.
  """
  max_timestamp = MIN_TIMESTAMP
  ret = None
  for e in elements:
    if max_timestamp <= e[0]:
      max_timestamp = e[0]
      ret = e
  return ret


class OrderedWindowElements(PTransform):
  """
  A PTransform that orders elements within windows and fills gaps.

  This transform takes a PCollection of elements, assigns them to windows, and
  then processes these windows to ensure elements are ordered and to fill any
  gaps (empty windows or gaps at the start of windows) based on specified
  strategies.

  Args:
    duration: The duration of each window.
    slide_interval: The interval at which windows slide. Defaults to `duration`.
    offset: The offset for window alignment.
    default_start_value: The default value to use for filling gaps at the
                          start of windows.
    empty_window_strategy: The strategy for handling completely empty windows.
    window_start_gap_strategy: The strategy for handling gaps at the
                                start of non-empty windows.
    timestamp: An optional callable to extract a timestamp from an element.
                If not provided, elements are assumed to be (timestamp, value)
                tuples.
  """
  def __init__(
      self,
      duration: DurationTypes,
      slide_interval: Optional[DurationTypes] = None,
      offset: TimestampTypes = 0,
      default_start_value=None,
      empty_window_strategy: WindowGapStrategy = WindowGapStrategy.IGNORE,
      window_start_gap_strategy: WindowGapStrategy = WindowGapStrategy.IGNORE,
      timestamp: Optional[Callable[[Any], Timestamp]] = None):
    self.duration = duration
    self.slide_interval = duration if slide_interval is None else slide_interval
    self.offset = offset
    self.default_start_value = default_start_value
    self.empty_window_strategy = empty_window_strategy
    self.window_start_gap_strategy = window_start_gap_strategy
    self.timestamp_func = timestamp

    if self.window_start_gap_strategy == WindowGapStrategy.DISCARD:
      raise ValueError(
          "Using DISCARD on windows with start gap is not allowed "
          "due to potential data loss.")

  def key_with_timestamp(self, element) -> tuple[Timestamp, Any]:
    """
    Extracts the timestamp from an element and keys it with the element.

    Args:
      element: The input element.

    Returns:
      A tuple (timestamp, element).
    """
    return self.timestamp_func(element), element

  def expand(self, input):
    """
    Applies the PTransform to the input PCollection.

    Args:
      input: The input PCollection of elements.

    Returns:
      A PCollection of ((window_start, window_end), [ordered_elements])
      where ordered_elements are sorted by timestamp and gaps are filled
      according to the specified strategies.
    """
    if self.timestamp_func:
      input = input | beam.Map(self.key_with_timestamp)

    # PCollection[((window_start, window_end), [element...])]
    windowed_data = (
        input
        | "FanOutToWindows" >> beam.ParDo(
            FanOutToWindows(self.duration, self.slide_interval, self.offset))
        | beam.CombinePerKey(ToListCombineFn())
        | "LogWindowedData" >> beam.LogElements(
            prefix="windowed=", level=logging.WARNING))

    if (self.empty_window_strategy == WindowGapStrategy.DISCARD and
        self.window_start_gap_strategy == WindowGapStrategy.IGNORE):
      # A shortcut for doing nothing on empty window and window start gap.
      # PCollection[((window_start, window_end), [element...])]
      return (
          windowed_data | beam.MapTuple(
              lambda window, elements:
              ((Timestamp.of(window[0]), Timestamp.of(window[1])), sorted(
                  elements)))
          | "LogReturn" >> beam.LogElements(
              prefix="return=", level=logging.WARNING))

    # PCollection[(slide_start, max_timestamp_element)]
    fanout_data = (
        input | "FanOutToSlideBoundaries" >> beam.ParDo(
            FanOutToSlideBoundaries(self.slide_interval, self.offset))
        | beam.CombinePerKey(max_timestamp_element))

    # PCollection[(slide_start, element_to_fill_missing_start)]
    context = (
        fanout_data
        | beam.WithKeys(0)
        | "GenerateContextDoFn" >> beam.ParDo(
            GenerateContextDoFn(
                self.duration,
                self.slide_interval,
                self.offset,
                self.default_start_value),
        )
        | "LogContext" >> beam.LogElements(
            prefix="context=", level=logging.WARNING))

    # PCollection[((window_start, window_end), [element...])]
    return (
        windowed_data
        | beam.ParDo(
            WindowGapFillingDoFn(
                self.duration,
                self.slide_interval,
                self.default_start_value,
                self.empty_window_strategy,
                self.window_start_gap_strategy),
            context_side=AsDict(context))
        | "LogReturn" >> beam.LogElements(
            prefix="return=", level=logging.WARNING))
