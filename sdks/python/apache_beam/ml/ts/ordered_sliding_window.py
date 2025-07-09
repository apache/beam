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

# No backward compatibility guarantees.
# Everything in this module is experimental.

import logging

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.coders import PickleCoder
from apache_beam.coders import TimestampCoder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import OrderedListStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
_LOGGER.setLevel(logging.INFO)


class OrderedSlidingWindowFn(beam.DoFn):

  ORDERED_BUFFER_STATE = OrderedListStateSpec('ordered_buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)
  TIMER_STATE = ReadModifyWriteStateSpec('timer_state', BooleanCoder())
  EARLIEST_TS_STATE = ReadModifyWriteStateSpec('earliest_ts', TimestampCoder())

  def __init__(self, window_size, slide_interval):
    self.window_size = window_size
    self.slide_interval = slide_interval

  def start_bundle(self):
    _LOGGER.debug("start bundle")

  def finish_bundle(self):
    _LOGGER.debug("finish bundle")

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      earliest_ts_state=beam.DoFn.StateParam(EARLIEST_TS_STATE)):

    _, value = element
    ordered_buffer.add((timestamp, value))

    _LOGGER.debug("receive %s at %s", element, timestamp)
    timer_started = timer_state.read()

    earliest = earliest_ts_state.read()
    if not earliest or earliest > timestamp:
      earliest_ts_state.write(timestamp)

    if not timer_started:
      earliest_ts_state.write(timestamp)

      first_slide_start = int(
          timestamp.micros / 1e6 // self.slide_interval) * self.slide_interval
      first_slide_start_ts = Timestamp.of(first_slide_start)

      first_window_end_ts = first_slide_start_ts + self.window_size
      _LOGGER.debug("set timer to %s", first_window_end_ts)
      window_timer.set(first_window_end_ts)

      timer_state.write(True)

    return []

  @on_timer(WINDOW_TIMER)
  def on_timer(
      self,
      key=beam.DoFn.KeyParam,
      fire_ts=beam.DoFn.TimestampParam,
      ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
      window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
      timer_state=beam.DoFn.StateParam(TIMER_STATE),
      earliest_ts_state=beam.DoFn.StateParam(EARLIEST_TS_STATE)):
    _LOGGER.debug("timer fire at %s", fire_ts)
    window_end_ts = fire_ts
    window_start_ts = window_end_ts - self.window_size

    window_values = list(
        ordered_buffer.read_range(window_start_ts, window_end_ts))

    _LOGGER.debug(
        "window start: %s, window end: %s", window_start_ts, window_end_ts)
    _LOGGER.debug("windowed data in buffer %s", str(window_values))
    if window_values:
      yield (key, (window_start_ts, window_end_ts, window_values))

    next_window_end_ts = fire_ts + self.slide_interval
    next_window_start_ts = window_start_ts + self.slide_interval

    earliest_ts = earliest_ts_state.read()
    ordered_buffer.clear_range(earliest_ts, next_window_start_ts)

    remaining_data = list(
        ordered_buffer.read_range(next_window_start_ts, MAX_TIMESTAMP))

    if not remaining_data:
      timer_state.clear()
      earliest_ts_state.write(next_window_start_ts)
      return

    _LOGGER.debug("set timer to %s", next_window_end_ts)
    window_timer.set(next_window_end_ts)


class FillGapsFn(beam.DoFn):
  def __init__(self, expected_interval: float):
    """
    Args:
      expected_interval: The expected time delta between elements, in seconds.
    """
    self.expected_interval = expected_interval

  def process(self, element):
    key, (window_start_ts, window_end_ts, window_elements) = element

    received_data = {
        round(float(ts.micros / 1e6), 5): val
        for ts, val in window_elements
    }

    start_sec = float(window_start_ts.micros / 1e6)
    end_sec = float(window_end_ts.micros / 1e6)

    filled_values = []
    current_ts_sec = start_sec

    while current_ts_sec < end_sec:
      lookup_ts = round(current_ts_sec, 5)

      if lookup_ts in received_data:
        filled_values.append(float(received_data[lookup_ts]))
      else:
        filled_values.append('NaN')

      current_ts_sec += self.expected_interval

    yield (key, (window_start_ts, window_end_ts, filled_values))
