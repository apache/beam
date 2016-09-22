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

import logging
import os
import tempfile
import unittest

import apache_beam as beam

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.io import source_test_utils
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class LineSource(iobase.BoundedSource):
  """A simple source that reads lines from a given file."""

  TEST_BUNDLE_SIZE = 10

  def __init__(self, file_name):
    self._file_name = file_name

  def read(self, range_tracker):
    with open(self._file_name, 'rb') as f:
      start = range_tracker.start_position()
      f.seek(start)
      if start > 0:
        f.seek(-1, os.SEEK_CUR)
        start -= 1
        start += len(f.readline())
      current = start
      for line in f:
        if not range_tracker.try_claim(current):
          return
        yield line.rstrip('\n')
        current += len(line)

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    assert start_position is None
    assert stop_position is None
    with open(self._file_name, 'rb') as f:
      f.seek(0, os.SEEK_END)
      size = f.tell()

    bundle_start = 0
    while bundle_start < size:
      bundle_stop = min(bundle_start + LineSource.TEST_BUNDLE_SIZE, size)
      yield iobase.SourceBundle(1, self, bundle_start, bundle_stop)
      bundle_start = bundle_stop

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      with open(self._file_name, 'rb') as f:
        f.seek(0, os.SEEK_END)
        stop_position = f.tell()
    return range_trackers.OffsetRangeTracker(start_position, stop_position)

  def default_output_coder(self):
    return coders.BytesCoder()


class RangeSource(iobase.BoundedSource):

  def __init__(self, start, end, split_freq=1):
    assert start <= end
    self._start = start
    self._end = end
    self._split_freq = split_freq

  def _normalize(self, start_position, end_position):
    return (self._start if start_position is None else start_position,
            self._end if end_position is None else end_position)

  def _round_up(self, index):
    """Rounds up to the nearest mulitple of split_freq."""
    return index - index % -self._split_freq

  def estimate_size(self):
    return self._stop - self._start

  def split(self, desired_bundle_size, start_position=None, end_position=None):
    start, end = self._normalize(start_position, end_position)
    for sub_start in range(start, end, desired_bundle_size):
      sub_end = min(self._end, sub_start + desired_bundle_size)
      yield iobase.SourceBundle(
          sub_end - sub_start,
          RangeSource(sub_start, sub_end, self._split_freq),
          None, None)

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


class SourcesTest(unittest.TestCase):

  def _create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_read_from_source(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')

    source = LineSource(file_name)
    range_tracker = source.get_range_tracker(None, None)
    result = [line for line in source.read(range_tracker)]

    self.assertItemsEqual(['aaaa', 'bbbb', 'cccc', 'dddd'], result)

  def test_run_direct(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')
    pipeline = beam.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Read(LineSource(file_name))
    assert_that(pcoll, equal_to(['aaaa', 'bbbb', 'cccc', 'dddd']))

    pipeline.run()

  def test_range_source(self):
    source_test_utils.assertSplitAtFractionExhaustive(RangeSource(0, 10, 3))

  def test_conact_source(self):
    source = iobase.ConcatSource([RangeSource(0, 4),
                                  RangeSource(4, 8),
                                  RangeSource(8, 12),
                                  RangeSource(12, 16),
                                 ])
    self.assertEqual(list(source.read(source.get_range_tracker())),
                     range(16))
    self.assertEqual(list(source.read(source.get_range_tracker((1, None),
                                                               (2, 10)))),
                     range(4, 10))
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

  def test_run_concat_direct(self):
    source = iobase.ConcatSource([RangeSource(0, 10),
                                  RangeSource(10, 100),
                                  RangeSource(100, 1000),
                                 ])
    pipeline = beam.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Read(source)
    assert_that(pcoll, equal_to(range(1000)))

    pipeline.run()

  def test_conact_source_exhaustive(self):
    source = iobase.ConcatSource([RangeSource(0, 10),
                                  RangeSource(100, 110),
                                  RangeSource(1000, 1010),
                                 ])
    source_test_utils.assertSplitAtFractionExhaustive(source)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
