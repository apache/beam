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

import math
import time
from typing import Any
from typing import Optional
from typing import Sequence

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


class ImpulseSeqGenRestrictionProvider(core.RestrictionProvider):
  def initial_restriction(self, element):
    start, end, interval = element
    if isinstance(start, Timestamp):
      start = start.micros / 1000000
    if isinstance(end, Timestamp):
      end = end.micros / 1000000

    assert start <= end
    assert interval > 0
    total_outputs = math.ceil((end - start) / interval)
    return OffsetRange(0, total_outputs)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, element, restriction):
    return _sequence_backlog_bytes(element, time.time(), restriction)

  # On drain, immediately stop emitting new elements
  def truncate(self, unused_element, unused_restriction):
    return None


def _sequence_backlog_bytes(element, now, offset_range):
  '''
  Calculates size of the output that the sequence should have emitted up to now.
  '''
  start, _, interval = element
  if isinstance(start, Timestamp):
    start = start.micros / 1000000
  assert interval > 0

  now_index = math.floor((now - start) / interval)
  if now_index < offset_range.start:
    return 0
  # We attempt to be precise as some runners scale based upon bytes and
  # output byte throughput.
  return 8 * (min(offset_range.stop, now_index) - offset_range.start)


class ImpulseSeqGenDoFn(beam.DoFn):
  '''
  ImpulseSeqGenDoFn fn receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, ImpulseSeqGenDoFn fn will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  ImpulseSeqGenDoFn can't guarantee that each element is output at exact time.
  ImpulseSeqGenDoFn guarantees that elements would not be output prior to
  given runtime timestamp.

  The output mode of the DoFn is based on the input `data`:

    - **None**: If `data` is None (by default), the output element will be the
      timestamp.
    - **Non-Timestamped Data**: If `data` is a sequence of arbitrary values
      (e.g., `[v1, v2, ...]`), the DoFn will assign a timestamp to each
      emitted element. The timestamps are calculated by starting at a given
      `start_time` and incrementing by a fixed `interval`.
    - **Pre-Timestamped Data**: If `data` is a sequence of tuples, where each
      tuple is `(apache_beam.utils.timestamp.Timestamp, value)`, the DoFn
      will use the provided timestamp for the emitted element.
  '''
  def __init__(self, data: Optional[Sequence[Any]] = None):
    self._data = data
    assert self._data is None or len(self._data) > 0
    self._len = len(self._data) if self._data is not None else 0
    self._is_timestamped_value = self._data is not None and self._len > 0 and \
        isinstance(self._data[0], tuple) and \
        isinstance(self._data[0][0], timestamp.Timestamp)

  def _get_output(self, index, current_output_timestamp):
    if self._data is None:
      return current_output_timestamp, Timestamp.of(current_output_timestamp)

    if self._is_timestamped_value:
      event_time, value = self._data[index % self._len]
      return TimestampedValue(value, event_time), event_time
    else:
      value = self._data[index % self._len]
      return TimestampedValue(value, current_output_timestamp), \
        Timestamp.of(current_output_timestamp)

  @beam.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(
          ImpulseSeqGenRestrictionProvider()),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    '''
    :param element: (start_timestamp, end_timestamp, interval)
    :param restriction_tracker:
    :return: yields elements at processing real-time intervals with value of
      target output timestamp for the element.
    '''
    start, _, interval = element

    if isinstance(start, Timestamp):
      start = start.micros / 1000000

    assert isinstance(restriction_tracker, sdf_utils.RestrictionTrackerView)

    current_output_index = restriction_tracker.current_restriction().start

    while True:
      current_output_timestamp = start + interval * current_output_index

      if current_output_timestamp > time.time():
        # we are too ahead of time, let's wait.
        restriction_tracker.defer_remainder(
            timestamp.Timestamp(current_output_timestamp))
        return

      if not restriction_tracker.try_claim(current_output_index):
        # nothing to claim, just stop
        return

      output, output_ts = self._get_output(current_output_index,
                                           current_output_timestamp)

      current_watermark = watermark_estimator.current_watermark()
      if current_watermark is None or output_ts > current_watermark:
        # ensure watermark is monotonic
        watermark_estimator.set_watermark(output_ts)

      yield output

      current_output_index += 1


class PeriodicSequence(PTransform):
  '''
  PeriodicSequence transform receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, PeriodicSequence transform will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  PeriodicSequence can't guarantee that each element is output at exact time.
  PeriodicSequence guarantees that elements would not be output prior to given
  runtime timestamp.
  The PCollection generated by PeriodicSequence is unbounded.
  '''
  def __init__(self):
    pass

  def expand(self, pcoll):
    return (
        pcoll
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))


class PeriodicImpulse(PTransform):
  '''
  PeriodicImpulse transform generates an infinite sequence of elements with
  given runtime interval.

  PeriodicImpulse transform behaves same as {@link PeriodicSequence} transform,
  but can be used as first transform in pipeline.
  The PCollection generated by PeriodicImpulse is unbounded.
  '''
  def __init__(
      self,
      start_timestamp=Timestamp.now(),
      stop_timestamp=MAX_TIMESTAMP,
      fire_interval=360.0,
      apply_windowing=False):
    '''
    :param start_timestamp: Timestamp for first element.
    :param stop_timestamp: Timestamp after which no elements will be output.
    :param fire_interval: Interval in seconds at which to output elements.
    :param apply_windowing: Whether each element should be assigned to
      individual window. If false, all elements will reside in global window.
    '''
    self.start_ts = start_timestamp
    self.stop_ts = stop_timestamp
    self.interval = fire_interval
    self.apply_windowing = apply_windowing

  def expand(self, pbegin):
    result = (
        pbegin
        | 'ImpulseElement' >> beam.Create(
            [(self.start_ts, self.stop_ts, self.interval)])
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))
    if self.apply_windowing:
      result = result | 'ApplyWindowing' >> beam.WindowInto(
          window.FixedWindows(self.interval))
    return result


class PeriodicStream(beam.PTransform):
  """A PTransform that generates a periodic stream of elements from a sequence.

  This transform creates a `PCollection` by emitting elements from a provided
  Python sequence at a specified time interval. It is designed for use in
  streaming pipelines to simulate a live, continuous source of data.

  The transform can be configured to:
  - Emit the sequence only once.
  - Repeat the sequence indefinitely or for a maximum duration.
  - Control the time interval between elements.

  To ensure that the stream does not emit a burst of elements immediately at
  pipeline startup, a fixed warmup period is added before the first element
  is generated.

  Args:
      data: The sequence of elements to emit into the PCollection. The elements
        can be raw values or pre-timestamped tuples in the format
        `(apache_beam.utils.timestamp.Timestamp, value)`.
      max_duration: The maximum total duration in seconds for the stream
        generation. If `None` (the default) and `repeat` is `True`, the
        stream is effectively infinite. If `repeat` is `False`, the stream's
        duration is the shorter of this value and the time required to emit
        the sequence once.
      interval: The delay in seconds between consecutive elements.
        Defaults to 0.1.
      repeat: If `True`, the input `data` sequence is emitted repeatedly.
        If `False` (the default), the sequence is emitted only once.
      warmup_time: The extra wait time for the impulse element
        (start, end, interval) to reach `ImpulseSeqGenDoFn`. It is used to
        avoid the events clustering at the beginning.
  """
  def __init__(
      self,
      data: Sequence[Any],
      max_duration: Optional[float] = None,
      interval: float = 0.1,
      repeat: bool = False,
      warmup_time: float = 2.0):
    self._data = data
    self._interval = interval
    self._repeat = repeat
    self._warmup_time = warmup_time

    # In `ImpulseSeqGenRestrictionProvider`, the total number of counts
    # (i.e. total_outputs) is computed by ceil((end - start) / interval),
    # where end is start + duration.
    # Due to precision error of arithmetic operations, even if duration is set
    # to len(self._data) * interval, (end - start) / interval could be a little
    # bit smaller or bigger than len(self._data).
    # In case of being bigger, total_outputs would be len(self._data) + 1,
    # as the ceil() operation is used.
    # Assuming that the precision error is no bigger than 1%, by subtracting
    # a small amount, we ensure that the result after ceil is stable even if
    # the precision error is present.
    self._duration = len(self._data) * interval - 0.01 * interval
    self._max_duration = max_duration if max_duration is not None else float(
        "inf")

  def expand(self, pbegin):
    start = timestamp.Timestamp.now() + self._warmup_time

    if not self._repeat:
      stop = start + min(self._duration, self._max_duration)
    else:
      stop = timestamp.MAX_TIMESTAMP if math.isinf(
          self._max_duration) else start + self._max_duration

    result = (
        pbegin
        | 'ImpulseElement' >> beam.Create([(start, stop, self._interval)])
        | 'GenStream' >> beam.ParDo(ImpulseSeqGenDoFn(self._data)))
    return result
