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

"""A native Python implementation of GenerateSequence transform.

This module provides a PTransform that generates a bounded or unbounded
sequence of integers, equivalent to Java SDK's GenerateSequence/CountingSource.

For the external (Flink-only) version that uses Java expansion service,
see apache_beam.io.external.generate_sequence.

Example usage::

    import apache_beam as beam
    from apache_beam.io.generate_sequence import GenerateSequence

    # Bounded mode
    with beam.Pipeline() as p:
        numbers = p | GenerateSequence(start=0, stop=100)

    # Unbounded mode (streaming)
    with beam.Pipeline() as p:
        numbers = p | GenerateSequence(start=0)

"""

import sys
import time

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.transforms import core
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

__all__ = ['GenerateSequence']


class GenerateSequence(beam.PTransform):
  """A PTransform that generates a bounded or unbounded sequence of integers.

  This transform produces integers from ``start`` (inclusive) to ``stop``
  (exclusive) in bounded mode, or from ``start`` up to Long.MAX_VALUE in
  unbounded mode. It is the native Python equivalent of Java SDK's
  GenerateSequence transform.

  Example usage::

      # Bounded mode: Generate integers [0, 100)
      p | GenerateSequence(start=0, stop=100)

      # Bounded mode: Generate integers [10, 20)
      p | GenerateSequence(start=10, stop=20)

      # Unbounded mode: Generate integers starting from 0
      p | GenerateSequence(start=0)

      # Unbounded mode with rate limiting: 10 elements per second
      p | GenerateSequence(start=0, elements_per_period=10, period=1.0)
  """
  def __init__(
      self,
      start,
      stop=None,
      elements_per_period=None,
      period=None):
    """Initializes GenerateSequence.

    Args:
      start: The first integer to generate (inclusive). Must be >= 0.
      stop: The upper bound (exclusive). If None, unbounded mode is used
          which generates integers up to sys.maxsize.
      elements_per_period: For unbounded mode, the number of elements to
          produce per period. Must be > 0 if specified.
      period: For unbounded mode, the duration of each period in seconds.
          Must be >= 0 if specified. If 0, elements are produced as fast
          as possible.

    Raises:
      ValueError: If start < 0, stop < start, elements_per_period <= 0,
          or period < 0.
    """
    super().__init__()
    if start < 0:
      raise ValueError('start must be >= 0, got %s' % start)
    if stop is not None and stop < start:
      raise ValueError('stop (%s) must be >= start (%s)' % (stop, start))
    if elements_per_period is not None and elements_per_period <= 0:
      raise ValueError(
          'elements_per_period must be > 0, got %s' % elements_per_period)
    if period is not None and period < 0:
      raise ValueError('period must be >= 0, got %s' % period)

    self._start = start
    self._stop = stop
    self._elements_per_period = elements_per_period
    self._period = period

  def expand(self, pbegin):
    if self._stop is not None:
      # Bounded mode: use BoundedSource
      return pbegin | iobase.Read(
          _BoundedCountingSource(self._start, self._stop))
    else:
      # Unbounded mode: use SDF-based approach
      return (
          pbegin
          | beam.Create([(
              self._start,
              self._elements_per_period,
              self._period)])
          | beam.ParDo(_UnboundedCountingDoFn()))

  def display_data(self):
    display = {
        'start': DisplayDataItem(self._start, label='Start'),
    }
    if self._stop is not None:
      display['stop'] = DisplayDataItem(self._stop, label='Stop')
    else:
      display['unbounded'] = DisplayDataItem(True, label='Unbounded')
    if self._elements_per_period is not None:
      display['elements_per_period'] = DisplayDataItem(
          self._elements_per_period, label='Elements Per Period')
    if self._period is not None:
      display['period'] = DisplayDataItem(self._period, label='Period (sec)')
    return display


class _BoundedCountingSource(iobase.BoundedSource):
  """A BoundedSource that produces integers in the range [start, stop).

  This is an internal class that implements the BoundedSource interface
  for generating a sequence of integers. Users should use the
  GenerateSequence PTransform instead of using this class directly.
  """
  def __init__(self, start, stop):
    """Initializes _BoundedCountingSource.

    Args:
      start: The first integer to generate (inclusive).
      stop: The upper bound (exclusive).
    """
    self._start = start
    self._stop = stop

  def estimate_size(self):
    """Estimates the size of this source in bytes.

    Each integer is assumed to be 8 bytes, matching Java's
    CountingSource.getBytesPerOffset().

    Returns:
      The estimated size in bytes.
    """
    return (self._stop - self._start) * 8

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    """Splits the source into bundles of approximately the desired size.

    Args:
      desired_bundle_size: The desired size of each bundle in bytes.
      start_position: Optional starting position for splitting.
      stop_position: Optional stopping position for splitting.

    Yields:
      SourceBundle objects representing the splits.
    """
    start = start_position if start_position is not None else self._start
    stop = stop_position if stop_position is not None else self._stop

    # Convert desired bytes to desired number of elements
    # (8 bytes per integer, same as Java's getBytesPerOffset())
    bundle_size_in_elements = max(1, desired_bundle_size // 8)

    bundle_start = start
    while bundle_start < stop:
      bundle_stop = min(bundle_start + bundle_size_in_elements, stop)
      yield iobase.SourceBundle(
          weight=(bundle_stop - bundle_start) * 8,
          source=_BoundedCountingSource(bundle_start, bundle_stop),
          start_position=bundle_start,
          stop_position=bundle_stop,
      )
      bundle_start = bundle_stop

  def get_range_tracker(self, start_position, stop_position):
    """Returns an OffsetRangeTracker for the given position range.

    Args:
      start_position: The starting position (inclusive). If None, uses
          this source's start.
      stop_position: The stopping position (exclusive). If None, uses
          this source's stop.

    Returns:
      An OffsetRangeTracker for the specified range.
    """
    start = start_position if start_position is not None else self._start
    stop = stop_position if stop_position is not None else self._stop
    return OffsetRangeTracker(start, stop)

  def read(self, range_tracker):
    """Reads integers from this source.

    This generator yields integers from the range tracker's start position
    up to (but not including) the stop position. It respects dynamic work
    rebalancing by calling try_claim() for each position.

    Args:
      range_tracker: The RangeTracker for the range to read.

    Yields:
      Integers in the specified range.
    """
    for i in range(range_tracker.start_position(),
                   range_tracker.stop_position()):
      if not range_tracker.try_claim(i):
        return
      yield i

  def default_output_coder(self):
    """Returns the default coder for the integers produced by this source."""
    return coders.VarIntCoder()

  def display_data(self):
    return {
        'start': DisplayDataItem(self._start, label='Start'),
        'stop': DisplayDataItem(self._stop, label='Stop'),
    }


class _UnboundedCountingRestrictionProvider(core.RestrictionProvider):
  """RestrictionProvider for unbounded counting source.

  This provider creates OffsetRange restrictions for the unbounded sequence,
  starting from the given start value up to sys.maxsize.
  """

  def initial_restriction(self, element):
    """Creates the initial restriction for the unbounded sequence.

    Args:
      element: A tuple of (start, elements_per_period, period).

    Returns:
      An OffsetRange from start to sys.maxsize.
    """
    start, _, _ = element
    return OffsetRange(start, sys.maxsize)

  def create_tracker(self, restriction):
    """Creates a tracker for the given restriction."""
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, element, restriction):
    """Estimates the size of work remaining in the restriction.

    For rate-limited sources, this is based on how much data should have
    been produced by now. For unlimited sources, returns a large value.

    Args:
      element: A tuple of (start, elements_per_period, period).
      restriction: The current OffsetRange restriction.

    Returns:
      Estimated size in bytes (8 bytes per element).
    """
    _, elements_per_period, period = element
    if period is None or period == 0 or elements_per_period is None:
      # No rate limiting - return remaining elements * 8 bytes
      # Cap at a reasonable size to avoid overflow
      remaining = min(restriction.stop - restriction.start, 1000000)
      return remaining * 8

    # With rate limiting, estimate based on expected output
    return _unbounded_sequence_backlog_bytes(
        element, time.time(), restriction)

  def truncate(self, element, restriction):
    """On drain, stop producing new elements."""
    return None


def _unbounded_sequence_backlog_bytes(element, now, offset_range):
  """Calculates backlog bytes for rate-limited unbounded sequence.

  Args:
    element: A tuple of (start, elements_per_period, period).
    now: Current time in seconds since epoch.
    offset_range: The current OffsetRange.

  Returns:
    Estimated backlog in bytes.
  """
  _, elements_per_period, period = element

  if period is None or period == 0 or elements_per_period is None:
    # No rate limiting
    return 8 * min(offset_range.stop - offset_range.start, 1000000)

  # Calculate how many elements should have been produced by now
  # This is a simplified model - in practice the reader tracks first_started
  elements_expected = elements_per_period * (now / period) if period > 0 else 0
  current_pos = offset_range.start

  if elements_expected < current_pos:
    return 0

  backlog = min(offset_range.stop, int(elements_expected)) - current_pos
  return 8 * max(0, backlog)


class _UnboundedCountingDoFn(beam.DoFn):
  """A DoFn that generates an unbounded sequence of integers.

  This DoFn uses the Splittable DoFn (SDF) framework to generate an
  unbounded stream of integers. It supports optional rate limiting
  through elements_per_period and period parameters.

  Each output element is a TimestampedValue with the integer value and
  a timestamp corresponding to when the element was produced.
  """

  @beam.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(
          _UnboundedCountingRestrictionProvider()),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    """Generates integers from the unbounded sequence.

    Args:
      element: A tuple of (start, elements_per_period, period).
      restriction_tracker: The restriction tracker for this element.
      watermark_estimator: The watermark estimator for this element.

    Yields:
      TimestampedValue containing the integer and its timestamp.
    """
    start, elements_per_period, period = element

    assert isinstance(restriction_tracker, sdf_utils.RestrictionTrackerView)

    current_value = restriction_tracker.current_restriction().start
    first_started = time.time()

    while True:
      # Check if we should wait due to rate limiting
      if period is not None and period > 0 and elements_per_period is not None:
        elements_produced = current_value - start
        expected_time = first_started + (
            elements_produced / elements_per_period) * period

        if expected_time > time.time():
          # We're ahead of schedule, defer remainder
          restriction_tracker.defer_remainder(
              Timestamp.of(expected_time))
          break

      if not restriction_tracker.try_claim(current_value):
        # Nothing more to claim
        break

      # Generate timestamp for this element (processing time)
      current_timestamp = Timestamp.now()

      # Update watermark
      current_watermark = watermark_estimator.current_watermark()
      if current_watermark is None or current_timestamp > current_watermark:
        watermark_estimator.set_watermark(current_timestamp)

      yield TimestampedValue(current_value, current_timestamp)

      current_value += 1

      # Safety check to prevent infinite loop in bounded testing scenarios
      if current_value >= sys.maxsize:
        break
