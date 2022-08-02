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

"""For internal use only; no backwards-compatibility guarantees.

Concat Source, which reads the union of several other sources.
"""
# pytype: skip-file

import bisect
import threading

from apache_beam.io import iobase


class ConcatSource(iobase.BoundedSource):
  """For internal use only; no backwards-compatibility guarantees.

  A ``BoundedSource`` that can group a set of ``BoundedSources``.

  Primarily for internal use, use the ``apache_beam.Flatten`` transform
  to create the union of several reads.
  """
  def __init__(self, sources):
    self._source_bundles = [
        source if isinstance(source, iobase.SourceBundle) else
        iobase.SourceBundle(None, source, None, None) for source in sources
    ]

  @property
  def sources(self):
    return [s.source for s in self._source_bundles]

  def estimate_size(self):
    return sum(s.source.estimate_size() for s in self._source_bundles)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    if start_position or stop_position:
      raise ValueError(
          'Multi-level initial splitting is not supported. Expected start and '
          'stop positions to be None. Received %r and %r respectively.' %
          (start_position, stop_position))

    for source in self._source_bundles:
      # We assume all sub-sources to produce bundles that specify weight using
      # the same unit. For example, all sub-sources may specify the size in
      # bytes as their weight.
      for bundle in source.source.split(desired_bundle_size,
                                        source.start_position,
                                        source.stop_position):
        yield bundle

  def get_range_tracker(self, start_position=None, stop_position=None):
    if start_position is None:
      start_position = (0, None)
    if stop_position is None:
      stop_position = (len(self._source_bundles), None)
    return ConcatRangeTracker(
        start_position, stop_position, self._source_bundles)

  def read(self, range_tracker):
    start_source, _ = range_tracker.start_position()
    stop_source, stop_pos = range_tracker.stop_position()
    if stop_pos is not None:
      stop_source += 1
    for source_ix in range(start_source, stop_source):
      if not range_tracker.try_claim((source_ix, None)):
        break
      for record in self._source_bundles[source_ix].source.read(
          range_tracker.sub_range_tracker(source_ix)):
        yield record

  def default_output_coder(self):
    if self._source_bundles:
      # Getting coder from the first sub-sources. This assumes all sub-sources
      # to produce the same coder.
      return self._source_bundles[0].source.default_output_coder()
    else:
      return super().default_output_coder()


class ConcatRangeTracker(iobase.RangeTracker):
  """For internal use only; no backwards-compatibility guarantees.

  Range tracker for ConcatSource"""
  def __init__(self, start, end, source_bundles):
    """Initializes ``ConcatRangeTracker``

    Args:
      start: start position, a tuple of (source_index, source_position)
      end: end position, a tuple of (source_index, source_position)
      source_bundles: the list of source bundles in the ConcatSource
    """
    super().__init__()
    self._start = start
    self._end = end
    self._source_bundles = source_bundles
    self._lock = threading.RLock()
    # Lazily-initialized list of RangeTrackers corresponding to each source.
    self._range_trackers = [None] * len(source_bundles)
    # The currently-being-iterated-over (and latest claimed) source.
    self._claimed_source_ix = self._start[0]
    # Now compute cumulative progress through the sources for converting
    # between global fractions and fractions within specific sources.
    # TODO(robertwb): Implement fraction-at-position to properly scale
    # partial start and end sources.
    # Note, however, that in practice splits are typically on source
    # boundaries anyways.
    last = end[0] if end[1] is None else end[0] + 1
    self._cumulative_weights = (
        [0] * start[0] +
        self._compute_cumulative_weights(source_bundles[start[0]:last]) + [1] *
        (len(source_bundles) - last - start[0]))

  @staticmethod
  def _compute_cumulative_weights(source_bundles):
    # Two adjacent sources must differ so that they can be uniquely
    # identified by a single global fraction.  Let min_diff be the
    # smallest allowable difference between sources.
    min_diff = 1e-5
    # For the computation below, we need weights for all sources.
    # Substitute average weights for those whose weights are
    # unspecified (or 1.0 for everything if none are known).
    known = [s.weight for s in source_bundles if s.weight is not None]
    avg = sum(known) / len(known) if known else 1.0
    weights = [s.weight or avg for s in source_bundles]

    # Now compute running totals of the percent done upon reaching
    # each source, with respect to the start and end positions.
    # E.g. if the weights were [100, 20, 3] we would produce
    # [0.0, 100/123, 120/123, 1.0]
    total = float(sum(weights))
    running_total = [0]
    for w in weights:
      running_total.append(max(min_diff, min(1, running_total[-1] + w / total)))
    running_total[-1] = 1  # In case of rounding error.
    # There are issues if, due to rouding error or greatly differing sizes,
    # two adjacent running total weights are equal. Normalize this things so
    # that this never happens.
    for k in range(1, len(running_total)):
      if running_total[k] == running_total[k - 1]:
        for j in range(k):
          running_total[j] *= (1 - min_diff)
    return running_total

  def start_position(self):
    return self._start

  def stop_position(self):
    return self._end

  def try_claim(self, pos):
    source_ix, source_pos = pos
    with self._lock:
      if source_ix > self._end[0]:
        return False
      elif source_ix == self._end[0] and self._end[1] is None:
        return False
      else:
        assert source_ix >= self._claimed_source_ix
        self._claimed_source_ix = source_ix
        if source_pos is None:
          return True
        else:
          return self.sub_range_tracker(source_ix).try_claim(source_pos)

  def try_split(self, pos):
    source_ix, source_pos = pos
    with self._lock:
      if source_ix < self._claimed_source_ix:
        # Already claimed.
        return None
      elif source_ix > self._end[0]:
        # After end.
        return None
      elif source_ix == self._end[0] and self._end[1] is None:
        # At/after end.
        return None
      else:
        if source_ix > self._claimed_source_ix:
          # Prefer to split on even boundary.
          split_pos = None
          ratio = self._cumulative_weights[source_ix]
        else:
          # Split the current subsource.
          split = self.sub_range_tracker(source_ix).try_split(source_pos)
          if not split:
            return None
          split_pos, frac = split
          ratio = self.local_to_global(source_ix, frac)

        self._end = source_ix, split_pos
        self._cumulative_weights = [
            min(w / ratio, 1) for w in self._cumulative_weights
        ]
        return (source_ix, split_pos), ratio

  def set_current_position(self, pos):
    raise NotImplementedError('Should only be called on sub-trackers')

  def position_at_fraction(self, fraction):
    source_ix, source_frac = self.global_to_local(fraction)
    last = self._end[0] if self._end[1] is None else self._end[0] + 1
    if source_ix == last:
      return (source_ix, None)
    else:
      return (
          source_ix,
          self.sub_range_tracker(source_ix).position_at_fraction(source_frac))

  def fraction_consumed(self):
    with self._lock:
      if self._claimed_source_ix == len(self._source_bundles):
        return 1.0
      else:
        return self.local_to_global(
            self._claimed_source_ix,
            self.sub_range_tracker(self._claimed_source_ix).fraction_consumed())

  def local_to_global(self, source_ix, source_frac):
    cw = self._cumulative_weights
    # The global fraction is the fraction to source_ix plus some portion of
    # the way towards the next source.
    return cw[source_ix] + source_frac * (cw[source_ix + 1] - cw[source_ix])

  def global_to_local(self, frac):
    if frac == 1:
      last = self._end[0] if self._end[1] is None else self._end[0] + 1
      return (last, None)
    else:
      cw = self._cumulative_weights
      # Find the last source that starts at or before frac.
      source_ix = bisect.bisect(cw, frac) - 1
      # Return this source, converting what's left of frac after starting
      # this source into a value in [0.0, 1.0) representing how far we are
      # towards the next source.
      return (
          source_ix,
          (frac - cw[source_ix]) / (cw[source_ix + 1] - cw[source_ix]))

  def sub_range_tracker(self, source_ix):
    assert self._start[0] <= source_ix <= self._end[0]
    if self._range_trackers[source_ix] is None:
      with self._lock:
        if self._range_trackers[source_ix] is None:
          source = self._source_bundles[source_ix]
          if source_ix == self._start[0] and self._start[1] is not None:
            start = self._start[1]
          else:
            start = source.start_position
          if source_ix == self._end[0] and self._end[1] is not None:
            stop = self._end[1]
          else:
            stop = source.stop_position
          self._range_trackers[source_ix] = source.source.get_range_tracker(
              start, stop)
    return self._range_trackers[source_ix]
