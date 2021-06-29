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

"""Unit tests for the sources framework."""
# pytype: skip-file

import logging
import unittest

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.io import source_test_utils
from apache_beam.io.concat_source import ConcatSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

__all__ = ['RangeSource']


class RangeSource(iobase.BoundedSource):

  __hash__ = None  # type: ignore[assignment]

  def __init__(self, start, end, split_freq=1):
    assert start <= end
    self._start = start
    self._end = end
    self._split_freq = split_freq

  def _normalize(self, start_position, end_position):
    return (
        self._start if start_position is None else start_position,
        self._end if end_position is None else end_position)

  def _round_up(self, index):
    """Rounds up to the nearest mulitple of split_freq."""
    return index - index % -self._split_freq

  def estimate_size(self):
    return self._end - self._start

  def split(self, desired_bundle_size, start_position=None, end_position=None):
    start, end = self._normalize(start_position, end_position)
    for sub_start in range(start, end, desired_bundle_size):
      sub_end = min(self._end, sub_start + desired_bundle_size)
      yield iobase.SourceBundle(
          sub_end - sub_start,
          RangeSource(sub_start, sub_end, self._split_freq),
          sub_start,
          sub_end)

  def get_range_tracker(self, start_position, end_position):
    start, end = self._normalize(start_position, end_position)
    return range_trackers.OffsetRangeTracker(start, end)

  def read(self, range_tracker):
    for k in range(self._round_up(range_tracker.start_position()),
                   self._round_up(range_tracker.stop_position())):
      if k % self._split_freq == 0:
        if not range_tracker.try_claim(k):
          return
      yield k

  # For testing
  def __eq__(self, other):
    return (
        type(self) == type(other) and self._start == other._start and
        self._end == other._end and self._split_freq == other._split_freq)


class ConcatSourceTest(unittest.TestCase):
  def test_range_source(self):
    source_test_utils.assert_split_at_fraction_exhaustive(RangeSource(0, 10, 3))

  def test_conact_source(self):
    source = ConcatSource([
        RangeSource(0, 4),
        RangeSource(4, 8),
        RangeSource(8, 12),
        RangeSource(12, 16),
    ])
    self.assertEqual(
        list(source.read(source.get_range_tracker())), list(range(16)))
    self.assertEqual(
        list(source.read(source.get_range_tracker((1, None), (2, 10)))),
        list(range(4, 10)))
    range_tracker = source.get_range_tracker(None, None)
    self.assertEqual(range_tracker.position_at_fraction(0), (0, 0))
    self.assertEqual(range_tracker.position_at_fraction(.5), (2, 8))
    self.assertEqual(range_tracker.position_at_fraction(.625), (2, 10))

    # Simulate a read.
    self.assertEqual(range_tracker.try_claim((0, None)), True)
    self.assertEqual(range_tracker.sub_range_tracker(0).try_claim(2), True)
    self.assertEqual(range_tracker.fraction_consumed(), 0.125)

    self.assertEqual(range_tracker.try_claim((1, None)), True)
    self.assertEqual(range_tracker.sub_range_tracker(1).try_claim(6), True)
    self.assertEqual(range_tracker.fraction_consumed(), 0.375)
    self.assertEqual(range_tracker.try_split((0, 1)), None)
    self.assertEqual(range_tracker.try_split((1, 5)), None)

    self.assertEqual(range_tracker.try_split((3, 14)), ((3, None), 0.75))
    self.assertEqual(range_tracker.try_claim((3, None)), False)
    self.assertEqual(range_tracker.sub_range_tracker(1).try_claim(7), True)
    self.assertEqual(range_tracker.try_claim((2, None)), True)
    self.assertEqual(range_tracker.sub_range_tracker(2).try_claim(9), True)

    self.assertEqual(range_tracker.try_split((2, 8)), None)
    self.assertEqual(range_tracker.try_split((2, 11)), ((2, 11), 11. / 12))
    self.assertEqual(range_tracker.sub_range_tracker(2).try_claim(10), True)
    self.assertEqual(range_tracker.sub_range_tracker(2).try_claim(11), False)

  def test_fraction_consumed_at_end(self):
    source = ConcatSource([
        RangeSource(0, 2),
        RangeSource(2, 4),
    ])
    range_tracker = source.get_range_tracker((2, None), None)
    self.assertEqual(range_tracker.fraction_consumed(), 1.0)

  def test_estimate_size(self):
    source = ConcatSource([
        RangeSource(0, 10),
        RangeSource(10, 100),
        RangeSource(100, 1000),
    ])
    self.assertEqual(source.estimate_size(), 1000)

  def test_position_at_fration(self):
    ranges = [(0, 4), (4, 16), (16, 24), (24, 32)]
    source = ConcatSource([
        iobase.SourceBundle((range[1] - range[0]) / 32.,
                            RangeSource(*range),
                            None,
                            None) for range in ranges
    ])

    range_tracker = source.get_range_tracker()
    self.assertEqual(range_tracker.position_at_fraction(0), (0, 0))
    self.assertEqual(range_tracker.position_at_fraction(.01), (0, 1))
    self.assertEqual(range_tracker.position_at_fraction(.1), (0, 4))
    self.assertEqual(range_tracker.position_at_fraction(.125), (1, 4))
    self.assertEqual(range_tracker.position_at_fraction(.2), (1, 7))
    self.assertEqual(range_tracker.position_at_fraction(.7), (2, 23))
    self.assertEqual(range_tracker.position_at_fraction(.75), (3, 24))
    self.assertEqual(range_tracker.position_at_fraction(.8), (3, 26))
    self.assertEqual(range_tracker.position_at_fraction(1), (4, None))

    range_tracker = source.get_range_tracker((1, None), (3, None))
    self.assertEqual(range_tracker.position_at_fraction(0), (1, 4))
    self.assertEqual(range_tracker.position_at_fraction(.01), (1, 5))
    self.assertEqual(range_tracker.position_at_fraction(.5), (1, 14))
    self.assertEqual(range_tracker.position_at_fraction(.599), (1, 16))
    self.assertEqual(range_tracker.position_at_fraction(.601), (2, 17))
    self.assertEqual(range_tracker.position_at_fraction(1), (3, None))

  def test_empty_source(self):
    read_all = source_test_utils.read_from_source

    empty = RangeSource(0, 0)
    self.assertEqual(read_all(ConcatSource([])), [])
    self.assertEqual(read_all(ConcatSource([empty])), [])
    self.assertEqual(read_all(ConcatSource([empty, empty])), [])

    range10 = RangeSource(0, 10)
    self.assertEqual(read_all(ConcatSource([range10]), (0, None), (0, 0)), [])
    self.assertEqual(read_all(ConcatSource([range10]), (0, 10), (1, None)), [])
    self.assertEqual(
        read_all(ConcatSource([range10, range10]), (0, 10), (1, 0)), [])

  def test_single_source(self):
    read_all = source_test_utils.read_from_source

    range10 = RangeSource(0, 10)
    self.assertEqual(read_all(ConcatSource([range10])), list(range(10)))
    self.assertEqual(
        read_all(ConcatSource([range10]), (0, 5)), list(range(5, 10)))
    self.assertEqual(
        read_all(ConcatSource([range10]), None, (0, 5)), list(range(5)))

  def test_source_with_empty_ranges(self):
    read_all = source_test_utils.read_from_source

    empty = RangeSource(0, 0)
    self.assertEqual(read_all(empty), [])

    range10 = RangeSource(0, 10)
    self.assertEqual(
        read_all(ConcatSource([empty, empty, range10])), list(range(10)))
    self.assertEqual(
        read_all(ConcatSource([empty, range10, empty])), list(range(10)))
    self.assertEqual(
        read_all(ConcatSource([range10, empty, range10, empty])),
        list(range(10)) + list(range(10)))

  def test_source_with_empty_ranges_exhastive(self):
    empty = RangeSource(0, 0)
    source = ConcatSource([
        empty,
        RangeSource(0, 10),
        empty,
        empty,
        RangeSource(10, 13),
        RangeSource(13, 17),
        empty,
    ])
    source_test_utils.assert_split_at_fraction_exhaustive(source)

  def test_run_concat_direct(self):
    source = ConcatSource([
        RangeSource(0, 10),
        RangeSource(10, 100),
        RangeSource(100, 1000),
    ])
    with TestPipeline() as pipeline:
      pcoll = pipeline | beam.io.Read(source)
      assert_that(pcoll, equal_to(list(range(1000))))

  def test_conact_source_exhaustive(self):
    source = ConcatSource([
        RangeSource(0, 10),
        RangeSource(100, 110),
        RangeSource(1000, 1010),
    ])
    source_test_utils.assert_split_at_fraction_exhaustive(source)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
