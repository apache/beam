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

import logging
import math
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io import range_trackers

# importing following private classes for testing
from apache_beam.io.filebasedsource import _ConcatSource as ConcatSource
from apache_beam.io.filebasedsource import _SingleFileSource as SingleFileSource

from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class LineSource(FileBasedSource):

  def read_records(self, file_name, range_tracker):
    f = self.open_file(file_name)
    try:
      start = range_tracker.start_position()
      f.seek(start)
      if start > 0:
        f.seek(-1, os.SEEK_CUR)
        start -= 1
        line = f.readline()
        start += len(line)
      current = start
      for line in f:
        if not range_tracker.try_claim(current):
          return
        yield line.rstrip('\n')
        current += len(line)
    finally:
      f.close()


def _write_data(num_lines, directory=None, prefix=tempfile.template):
  all_data = []
  with tempfile.NamedTemporaryFile(
      delete=False, dir=directory, prefix=prefix) as f:
    for i in range(num_lines):
      data = 'line' + str(i)
      all_data.append(data)
      f.write(data + '\n')

    return f.name, all_data


def _write_pattern(lines_per_file):
  temp_dir = tempfile.mkdtemp()

  all_data = []
  file_name = None
  start_index = 0
  for i in range(len(lines_per_file)):
    file_name, data = _write_data(lines_per_file[i],
                                  directory=temp_dir, prefix='mytemp')
    all_data.extend(data)
    start_index += lines_per_file[i]

  assert file_name
  return (
      file_name[:file_name.rfind(os.path.sep)] + os.path.sep + 'mytemp*',
      all_data)


class TestConcatSource(unittest.TestCase):

  class DummySource(iobase.BoundedSource):

    def __init__(self, values):
      self._values = values

    def split(self, desired_bundle_size, start_position=None,
              stop_position=None):
      # simply devides values into two bundles
      middle = len(self._values) / 2
      yield iobase.SourceBundle(0.5, TestConcatSource.DummySource(
          self._values[:middle]), None, None)
      yield iobase.SourceBundle(0.5, TestConcatSource.DummySource(
          self._values[middle:]), None, None)

    def get_range_tracker(self, start_position, stop_position):
      if start_position is None:
        start_position = 0
      if stop_position is None:
        stop_position = len(self._values)

      return range_trackers.OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
      for index, value in enumerate(self._values):
        if not range_tracker.try_claim(index):
          return

        yield value

    def estimate_size(self):
      return len(self._values)  # Assuming each value to be 1 byte.

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def test_read(self):
    sources = [TestConcatSource.DummySource(range(start, start + 10)) for start
               in [0, 10, 20]]
    concat = ConcatSource(sources)
    range_tracker = concat.get_range_tracker(None, None)
    read_data = [value for value in concat.read(range_tracker)]
    self.assertItemsEqual(range(30), read_data)

  def test_split(self):
    sources = [TestConcatSource.DummySource(range(start, start + 10)) for start
               in [0, 10, 20]]
    concat = ConcatSource(sources)
    splits = [split for split in concat.split()]
    self.assertEquals(6, len(splits))

    # Reading all splits
    read_data = []
    for split in splits:
      range_tracker_for_split = split.source.get_range_tracker(
          split.start_position,
          split.stop_position)
      read_data.extend([value for value in split.source.read(
          range_tracker_for_split)])
    self.assertItemsEqual(range(30), read_data)

  def test_estimate_size(self):
    sources = [TestConcatSource.DummySource(range(start, start + 10)) for start
               in [0, 10, 20]]
    concat = ConcatSource(sources)
    self.assertEquals(30, concat.estimate_size())


class TestFileBasedSource(unittest.TestCase):

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def test_fully_read_single_file(self):
    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10
    fbs = LineSource(file_name)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_fully_read_file_pattern(self):
    pattern, expected_data = _write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    fbs = LineSource(pattern)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_fully_read_file_pattern_with_empty_files(self):
    pattern, expected_data = _write_pattern([5, 0, 12, 0, 8, 0])
    assert len(expected_data) == 25
    fbs = LineSource(pattern)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_estimate_size_of_file(self):
    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10
    fbs = LineSource(file_name)
    self.assertEquals(10 * 6, fbs.estimate_size())

  def test_estimate_size_of_pattern(self):
    pattern, expected_data = _write_pattern([5, 3, 10, 8, 8, 4])
    assert len(expected_data) == 38
    fbs = LineSource(pattern)
    self.assertEquals(38 * 6, fbs.estimate_size())

    pattern, expected_data = _write_pattern([5, 3, 9])
    assert len(expected_data) == 17
    fbs = LineSource(pattern)
    self.assertEquals(17 * 6, fbs.estimate_size())

  def test_splits_into_subranges(self):
    pattern, expected_data = _write_pattern([5, 9, 6])
    assert len(expected_data) == 20
    fbs = LineSource(pattern)
    splits = [split for split in fbs.split(desired_bundle_size=15)]
    expected_num_splits = (
        math.ceil(float(6 * 5) / 15) +
        math.ceil(float(6 * 9) / 15) +
        math.ceil(float(6 * 6) / 15))
    assert len(splits) == expected_num_splits

  def test_read_splits_single_file(self):
    file_name, expected_data = _write_data(100)
    assert len(expected_data) == 100
    fbs = LineSource(file_name)
    splits = [split for split in fbs.split(desired_bundle_size=33)]

    # Reading all splits
    read_data = []
    for split in splits:
      source = split.source
      range_tracker = source.get_range_tracker(split.start_position,
                                               split.stop_position)
      data_from_split = [data for data in source.read(range_tracker)]
      read_data.extend(data_from_split)

    self.assertItemsEqual(expected_data, read_data)

  def test_read_splits_file_pattern(self):
    pattern, expected_data = _write_pattern([34, 66, 40, 24, 24, 12])
    assert len(expected_data) == 200
    fbs = LineSource(pattern)
    splits = [split for split in fbs.split(desired_bundle_size=50)]

    # Reading all splits
    read_data = []
    for split in splits:
      source = split.source
      range_tracker = source.get_range_tracker(split.start_position,
                                               split.stop_position)
      data_from_split = [data for data in source.read(range_tracker)]
      read_data.extend(data_from_split)

    self.assertItemsEqual(expected_data, read_data)

  def test_dataflow_file(self):
    file_name, expected_data = _write_data(100)
    assert len(expected_data) == 100
    pipeline = beam.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Read('Read', LineSource(file_name))
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_dataflow_pattern(self):
    pattern, expected_data = _write_pattern([34, 66, 40, 24, 24, 12])
    assert len(expected_data) == 200
    pipeline = beam.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Read('Read', LineSource(pattern))
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()


class TestSingleFileSource(unittest.TestCase):

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def test_source_creation_fails_for_non_number_offsets(self):
    start_not_a_number_error = 'start_offset must be a number*'
    stop_not_a_number_error = 'stop_offset must be a number*'

    fbs = LineSource('dymmy_pattern')

    with self.assertRaisesRegexp(ValueError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset='aaa', stop_offset='bbb')
    with self.assertRaisesRegexp(ValueError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset='aaa', stop_offset=100)
    with self.assertRaisesRegexp(ValueError, stop_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset='bbb')
    with self.assertRaisesRegexp(ValueError, stop_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=None)
    with self.assertRaisesRegexp(ValueError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=None, stop_offset=100)

  def test_source_creation_fails_if_start_lg_stop(self):
    start_larger_than_stop_error = (
        'start_offset must be smaller than stop_offset*')

    fbs = LineSource('dymmy_pattern')
    SingleFileSource(
        fbs, file_name='dummy_file', start_offset=99, stop_offset=100)
    with self.assertRaisesRegexp(ValueError, start_larger_than_stop_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=99)
    with self.assertRaisesRegexp(ValueError, start_larger_than_stop_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=100)

  def test_estimates_size(self):
    fbs = LineSource('dymmy_pattern')

    # Should simply return stop_offset - start_offset
    source = SingleFileSource(
        fbs, file_name='dummy_file', start_offset=0, stop_offset=100)
    self.assertEquals(100, source.estimate_size())

    source = SingleFileSource(fbs, file_name='dummy_file', start_offset=10,
                              stop_offset=100)
    self.assertEquals(90, source.estimate_size())

  def test_read_range_at_beginning(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(0, 20)
    read_data = [value for value in source.read(range_tracker)]
    self.assertItemsEqual(expected_data[:4], read_data)

  def test_read_range_at_end(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(40, 60)
    read_data = [value for value in source.read(range_tracker)]
    self.assertItemsEqual(expected_data[-3:], read_data)

  def test_read_range_at_middle(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(20, 40)
    read_data = [value for value in source.read(range_tracker)]
    self.assertItemsEqual(expected_data[4:7], read_data)

  def test_produces_splits_desiredsize_large_than_size(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10
    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    splits = [split for split in source.split(desired_bundle_size=100)]
    self.assertEquals(1, len(splits))
    self.assertEquals(60, splits[0].weight)
    self.assertEquals(0, splits[0].start_position)
    self.assertEquals(60, splits[0].stop_position)

    range_tracker = splits[0].source.get_range_tracker(None, None)
    read_data = [value for value in splits[0].source.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_produces_splits_desiredsize_smaller_than_size(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10
    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    splits = [split for split in source.split(desired_bundle_size=25)]
    self.assertEquals(3, len(splits))

    read_data = []
    for split in splits:
      source = split.source
      range_tracker = source.get_range_tracker(split.start_position,
                                               split.stop_position)
      data_from_split = [data for data in source.read(range_tracker)]
      read_data.extend(data_from_split)
    self.assertItemsEqual(expected_data, read_data)

  def test_produce_split_with_start_and_end_positions(self):
    fbs = LineSource('dymmy_pattern')

    file_name, expected_data = _write_data(10)
    assert len(expected_data) == 10
    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    splits = [split for split in
              source.split(desired_bundle_size=15, start_offset=10,
                           stop_offset=50)]
    self.assertEquals(3, len(splits))

    read_data = []
    for split in splits:
      source = split.source
      range_tracker = source.get_range_tracker(split.start_position,
                                               split.stop_position)
      data_from_split = [data for data in source.read(range_tracker)]
      read_data.extend(data_from_split)
    self.assertItemsEqual(expected_data[2:9], read_data)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
