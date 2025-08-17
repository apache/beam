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
import math
import time
import warnings
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
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes


class ImpulseSeqGenRestrictionProvider(core.RestrictionProvider):
  def initial_restriction(self, element):
    start, end, interval = element
    if not isinstance(start, Timestamp):
      start = Timestamp.of(start)

    if not isinstance(end, Timestamp):
      end = Timestamp.of(end)

    interval_duration = Duration(interval)

    assert start <= end
    assert interval > 0
    total_duration = end - start
    total_outputs = math.ceil(total_duration.micros / interval_duration.micros)

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

  * first_timestamp = The timestamp of the first element to be generated
    (inclusive).
  * last_timestamp = The timestamp marking the end of the generation period
    (exclusive). No elements will be generated at or after this time.
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
      emitted element.
    - **Pre-Timestamped Data**: If `data` is a sequence of tuples, where each
      tuple is `(apache_beam.utils.timestamp.Timestamp, value)`, the DoFn
      will use the provided timestamp for the emitted element.

  See the parameter description of `PeriodicImpulse` for more information.
  '''
  def __init__(self, data: Optional[Sequence[Any]] = None):
    self._data = data
    assert self._data is None or len(self._data) > 0
    self._len = len(self._data) if self._data is not None else 0
    self._is_pre_timestamped = self._data is not None and self._len > 0 and \
        isinstance(self._data[0], tuple) and \
        isinstance(self._data[0][0], timestamp.Timestamp)

  def _get_output(self, index, current_output_timestamp):
    if self._data is None:
      return TimestampedValue(
          current_output_timestamp, current_output_timestamp)

    if self._is_pre_timestamped:
      event_time, value = self._data[index % self._len]
      return TimestampedValue(value, event_time)
    else:
      value = self._data[index % self._len]
      return TimestampedValue(value, current_output_timestamp)

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

      output = self._get_output(current_output_index, current_output_timestamp)

      current_watermark = watermark_estimator.current_watermark()
      if current_watermark is None or output.timestamp > current_watermark:
        # ensure watermark is monotonic
        watermark_estimator.set_watermark(output.timestamp)

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


class RebaseMode(enum.Enum):
  '''Controls how the start and stop timestamps are rebased to execution time.

  Attributes:
    REBASE_NONE: Timestamps are not changed.
    REBASE_ALL: Both start and stop timestamps are rebased, preserving the
      original duration.
    REBASE_START: Only the start timestamp is rebased; the stop timestamp
      is unchanged.
  '''
  REBASE_NONE = 0
  REBASE_ALL = 1
  REBASE_START = 2


class PeriodicImpulse(PTransform):
  '''
  PeriodicImpulse transform generates an infinite sequence of elements with
  given runtime interval.

  PeriodicImpulse transform behaves same as {@link PeriodicSequence} transform,
  but can be used as first transform in pipeline.
  The PCollection generated by PeriodicImpulse is unbounded.
  '''
  def _validate_and_adjust_duration(self):
    assert self.data

    # The total time we need to impulse all the data.
    data_duration = (len(self.data) - 1) * Duration(self.interval)

    is_pre_timestamped = isinstance(self.data[0], tuple) and \
      isinstance(self.data[0][0], timestamp.Timestamp)

    start_ts = Timestamp.of(self.start_ts)
    stop_ts = Timestamp.of(self.stop_ts)

    if stop_ts == MAX_TIMESTAMP:
      # When the stop timestamp is unbounded (MAX_TIMESTAMP), set it to the
      # data's actual end time plus an extra fire interval, because the
      # impulse duration's upper bound is exclusive.
      self.stop_ts = start_ts + data_duration + Duration(self.interval)
      stop_ts = self.stop_ts

    # The total time for the impulse signal which occurs in [start, end).
    impulse_duration = stop_ts - start_ts
    if data_duration + Duration(self.interval) < impulse_duration:
      # We don't have enough data for the impulse.
      # If we can fit at least one more data point in the impulse duration,
      # then we will be in the repeat mode.
      message = 'The number of elements in the provided pre-timestamped ' \
        'data sequence is not enough to span the full impulse duration. ' \
        f'Expected duration: {impulse_duration}, ' \
        f'actual data duration: {data_duration}.'

      if is_pre_timestamped:
        raise ValueError(
            f'{message} Please either provide more data or decrease '
            '`stop_timestamp`.')
      else:
        warnings.warn(
            f'{message} As a result, the data sequence will be repeated to '
            'generate elements for the entire duration.')

  def __init__(
      self,
      start_timestamp: TimestampTypes = Timestamp.now(),
      stop_timestamp: TimestampTypes = MAX_TIMESTAMP,
      fire_interval: float = 360.0,
      apply_windowing: bool = False,
      data: Optional[Sequence[Any]] = None,
      rebase: RebaseMode = RebaseMode.REBASE_NONE):
    '''
    :param start_timestamp: Timestamp for first element.
    :param stop_timestamp: Timestamp at or after which no elements will be
      output.
    :param fire_interval: Interval in seconds at which to output elements.
    :param apply_windowing: Whether each element should be assigned to
      individual window. If false, all elements will reside in global window.
    :param data: A sequence of elements to emit. The behavior depends on the
      content:

      - **None (default):** The transform emits the event timestamps as
        the element values, starting from start_timestamp and incrementing by
        `fire_interval` up to the `stop_timestamp` (exclusive)
      - **Sequence of raw values (e.g., `['a', 'b']`)**: The transform emits
        each value in the sequence, assigning it an event timestamp that is
        calculated in the same manner as the default scenario. The sequence
        is repeated if the impulse duration requires more elements than
        are in the sequence (a warning will be given in this case).
      - **Sequence of pre-timestamped tuples (e.g.,
        `[(t1, v1), (t2, v2)]`)**: The transform emits each value with its
        explicitly provided event time. The format must be
        `(apache_beam.utils.timestamp.Timestamp, value)`. The provided
        timestamps are used directly, overriding the calculated ones.
        Note that the elements in the sequence is NOT required to be ordered
        by event time; an element with a timestamp earlier than a preceding one
        will be treated as a potential late event.
        **Important**: In this mode, the number of elements in `data` must be
        sufficient to cover the duration defined by `start_timestamp`,
        `stop_timestamp`, and `fire_interval`; otherwise, a `ValueError` is
        raised.

    :param rebase: Controls how the start and stop timestamps are rebased to
      execution time. See `RebaseMode` for more details. Defaults to
      `REBASE_NONE`.
    '''
    self.start_ts = start_timestamp
    self.stop_ts = stop_timestamp
    self.interval = fire_interval
    self.apply_windowing = apply_windowing
    self.data = data
    self.rebase = rebase

    if self.data:
      self._validate_and_adjust_duration()

  def expand(self, pbegin):
    if self.rebase == RebaseMode.REBASE_ALL:
      duration = Timestamp.of(self.stop_ts) - Timestamp.of(self.start_ts)
      impulse_element = pbegin | beam.Impulse() | beam.Map(
          lambda _:
          [Timestamp.now(), Timestamp.now() + duration, self.interval])
    elif self.rebase == RebaseMode.REBASE_START:
      impulse_element = pbegin | beam.Impulse() | beam.Map(
          lambda _: [Timestamp.now(), self.stop_ts, self.interval])
    else:
      impulse_element = pbegin | 'ImpulseElement' >> beam.Create(
          [(self.start_ts, self.stop_ts, self.interval)])

    result = (
        impulse_element
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn(self.data)))

    if not self.data:
      # This step is actually an identity transform, because the Timestamped
      # values have already been generated in `ImpulseSeqGenDoFn`.
      # We keep this step here to prevent the current PeriodicImpulse from
      # breaking the compatibility.
      result = (result | 'MapToTimestamped' >> beam.Map(lambda tt: tt))

    if self.apply_windowing:
      result = result | 'ApplyWindowing' >> beam.WindowInto(
          window.FixedWindows(self.interval))
    return result
