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

import apache_beam as beam
from apache_beam.coders import PickleCoder, BooleanCoder
from apache_beam.transforms.userstate import OrderedListStateSpec, TimerSpec, on_timer, ReadModifyWriteStateSpec
from apache_beam.utils.timestamp import Timestamp, MIN_TIMESTAMP, MAX_TIMESTAMP
from apache_beam.transforms.timeutil import TimeDomain
import typing
from collections import defaultdict
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

class OrderedSlidingWindowFn(beam.DoFn):

  ORDERED_BUFFER_STATE = OrderedListStateSpec('ordered_buffer', PickleCoder())
  WINDOW_TIMER = TimerSpec('window_timer', TimeDomain.WATERMARK)
  TIMER_STATE = ReadModifyWriteStateSpec('timer_state', BooleanCoder())
  EARLIEST_TS_STATE = ReadModifyWriteStateSpec('earliest_ts', PickleCoder())


  def __init__(self, window_size, slide_interval):
    self.window_size = window_size
    self.slide_interval = slide_interval

  def start_bundle(self):
    logging.info("start bundle")

  def finish_bundle(self):
    logging.info("finish bundle")

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
              window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
              timer_state=beam.DoFn.StateParam(TIMER_STATE),
              earliest_ts_state=beam.DoFn.StateParam(EARLIEST_TS_STATE)):

    key, value = element
    ordered_buffer.add((timestamp, value))

    logging.info(f"receive {element} at {timestamp}")
    timer_started = timer_state.read()
    if not timer_started:
      earliest_ts_state.write(timestamp)

      first_slide_start = int(
          timestamp.micros / 1e6 // self.slide_interval) * self.slide_interval
      first_slide_start_ts = Timestamp.of(first_slide_start)

      first_window_end_ts = first_slide_start_ts + self.window_size
      logging.info(f"set timer to {first_window_end_ts}")
      window_timer.set(first_window_end_ts)

      timer_state.write(True)

  @on_timer(WINDOW_TIMER)
  def on_timer(self,
               key=beam.DoFn.KeyParam,
               fire_ts=beam.DoFn.TimestampParam,
               ordered_buffer=beam.DoFn.StateParam(ORDERED_BUFFER_STATE),
               window_timer=beam.DoFn.TimerParam(WINDOW_TIMER),
               timer_state=beam.DoFn.StateParam(TIMER_STATE),
               earliest_ts_state=beam.DoFn.StateParam(EARLIEST_TS_STATE)):
    logging.info(f"timer fire at {fire_ts}")
    window_end_ts = fire_ts
    window_start_ts = window_end_ts - self.window_size

    window_values = list(ordered_buffer.read_range(window_start_ts, window_end_ts))
    logging.info(f"Window [{window_start_ts}, {window_end_ts}) contains {len(window_values)} elements.")


    logging.info(f"window start: {window_start_ts}, window end: {window_end_ts}")
    logging.info(f"windowed data in buffer {str(window_values)}")
    if window_values:
      yield (key, (window_start_ts, window_end_ts, window_values))

    next_window_end_ts = fire_ts + self.slide_interval
    next_window_start_ts = window_start_ts + self.slide_interval

    earliest_ts = earliest_ts_state.read()
    ordered_buffer.clear_range(earliest_ts, next_window_start_ts)

    remaining_data = list(ordered_buffer.read_range(next_window_start_ts, MAX_TIMESTAMP))

    if not remaining_data:
      timer_state.clear()
      earliest_ts_state.clear()
      return

    logging.info(f"set timer to {next_window_end_ts}")
    window_timer.set(next_window_end_ts)


class FillGapsFn(beam.DoFn):

    def __init__(self, expected_interval):
        self.expected_interval = expected_interval

    def process(self, element):
        key, (window_start_ts, window_end_ts, window_elements) = element


        # If there are no elements, or only one, there are no "middle" gaps to fill.
        if len(window_elements) <= 1:
            if window_elements:
                 # If one element, just yield it back as a list of one
                 yield (key, (window_start_ts, window_end_ts, [window_elements[0][1]]))
            return

        # Use a dictionary for efficient lookup, rounding for float precision.
        received_data = defaultdict(list)
        for ts, val in window_elements:
            lookup_ts = round(float(ts.micros / 1e6), 5)
            received_data[lookup_ts].append(val)

        # Find the boundaries of the data we actually received.
        min_ts = min(received_data.keys())
        max_ts = max(received_data.keys())

        filled_middle_values = []
        current_ts = min_ts

        # Iterate from the first observed timestamp to the last.
        while current_ts <= max_ts:
            lookup_ts = round(current_ts, 5)

            # Use the actual value if it exists, otherwise a placeholder.
            value = received_data.get(lookup_ts, ['NaN'])[0]
            filled_middle_values.append(float(value))

            # Move to the next expected timestamp.
            current_ts += self.expected_interval

        # Yield the original window boundaries but with the new, gap-filled list.
        yield (key, (window_start_ts, window_end_ts, filled_middle_values))


