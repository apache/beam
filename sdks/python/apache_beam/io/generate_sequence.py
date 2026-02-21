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

This module provides a PTransform that generates a bounded sequence of
integers, equivalent to Java SDK's GenerateSequence/CountingSource.

For the external (Flink-only) version that uses Java expansion service,
see apache_beam.io.external.generate_sequence.

Example usage::

    import apache_beam as beam
    from apache_beam.io.generate_sequence import GenerateSequence

    with beam.Pipeline() as p:
        numbers = p | GenerateSequence(start=0, stop=100)

"""

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms.display import DisplayDataItem

__all__ = ['GenerateSequence']


class GenerateSequence(beam.PTransform):
  """A PTransform that generates a bounded sequence of integers.

  This transform produces integers from ``start`` (inclusive) to ``stop``
  (exclusive). It is the native Python equivalent of Java SDK's
  GenerateSequence transform.

  Example usage::

      # Generate integers [0, 100)
      p | GenerateSequence(start=0, stop=100)

      # Generate integers [10, 20)
      p | GenerateSequence(start=10, stop=20)
  """

  def __init__(self, start, stop=None):
    """Initializes GenerateSequence.

    Args:
      start: The first integer to generate (inclusive). Must be >= 0.
      stop: The upper bound (exclusive). If None, unbounded mode will be
          used (not yet implemented).

    Raises:
      ValueError: If start < 0 or stop < start.
    """
    super().__init__()
    if start < 0:
      raise ValueError('start must be >= 0, got %s' % start)
    if stop is not None and stop < start:
      raise ValueError(
          'stop (%s) must be >= start (%s)' % (stop, start))
    self._start = start
    self._stop = stop

  def expand(self, pbegin):
    if self._stop is not None:
      return pbegin | iobase.Read(_BoundedCountingSource(self._start,
                                                         self._stop))
    else:
      raise NotImplementedError(
          'Unbounded GenerateSequence is not yet implemented. '
          'Please specify a stop value for bounded mode.')

  def display_data(self):
    return {
        'start': DisplayDataItem(self._start, label='Start'),
        'stop': DisplayDataItem(self._stop, label='Stop'),
    }


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
