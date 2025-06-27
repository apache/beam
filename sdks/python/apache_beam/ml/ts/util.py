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
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.transforms.periodicsequence import ImpulseSeqGenRestrictionProvider  # pylint:disable=line-too-long
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp


class ImpulseStreamGenDoFn(beam.DoFn):
  """
  Generates a periodic, unbounded stream of elements from a provided sequence.

  (Similar to ImpulseSeqGenDoFn in apache_beam.transforms.periodicsequence)

  This Splittable DoFn (SDF) is designed to simulate a continuous stream of
  data for testing or demonstration purposes. It takes a Python sequence (e.g.,
  a list) and emits its elements one by one in a loop, assigning a timestamp
  to each.

  The DoFn operates in two modes based on the structure of the input `data`:

    - **Non-Timestamped Data**: If `data` is a sequence of arbitrary values
      (e.g., `[v1, v2, ...]`), the DoFn will assign a new timestamp to each
      emitted element. The timestamps are calculated by starting at a given
      `start_time` and incrementing by a fixed `interval`.
    - **Pre-Timestamped Data**: If `data` is a sequence of tuples, where each
      tuple is `(apache_beam.utils.timestamp.Timestamp, value)`, the DoFn
      will use the provided timestamp for the emitted element.

  The rate of emission is controlled by wall-clock time. The DoFn will only
  emit elements whose timestamp (either calculated or provided) is in the past
  compared to the current system time. When it "catches up" to the present,
  it will pause and defer the remainder of the work.

  Args:
    data: The sequence of elements to emit into the PCollection. The elements
      can be raw values or pre-timestamped tuples in the format
      `(apache_beam.utils.timestamp.Timestamp, value)`.
  """
  def __init__(self, data: Sequence[Any]):
    self._data = data
    self._len = len(data)
    self._is_timestamped_value = len(data) > 0 and isinstance(
        data[0], tuple) and isinstance(data[0][0], timestamp.Timestamp)

  def _get_timestamped_value(self, index, current_output_timestamp):
    if self._is_timestamped_value:
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
    start, _, interval = element

    if isinstance(start, timestamp.Timestamp):
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

      output = self._get_timestamped_value(
          current_output_index, current_output_timestamp)

      current_watermark = watermark_estimator.current_watermark()
      if current_watermark is None or output.timestamp > current_watermark:
        # ensure watermark is monotonic
        watermark_estimator.set_watermark(output.timestamp)

      yield output

      current_output_index += 1


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
  """

  WARMUP_TIME = 2

  def __init__(
      self,
      data: Sequence[Any],
      max_duration: Optional[float] = None,
      interval: float = 0.1,
      repeat: bool = False):
    self._data = data
    self._interval = interval
    self._repeat = repeat

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
    # Give the runner some time to start up so the events will not cluster
    # at the beginning.
    start = timestamp.Timestamp.now() + PeriodicStream.WARMUP_TIME

    if not self._repeat:
      stop = start + min(self._duration, self._max_duration)
    else:
      stop = timestamp.MAX_TIMESTAMP if math.isinf(
          self._max_duration) else start + self._max_duration

    result = (
        pbegin
        | 'ImpulseElement' >> beam.Create([(start, stop, self._interval)])
        | 'GenStream' >> beam.ParDo(ImpulseStreamGenDoFn(self._data)))
    return result
