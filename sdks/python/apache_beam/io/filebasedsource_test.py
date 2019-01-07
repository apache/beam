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

from __future__ import absolute_import
from __future__ import division

import bz2
import gzip
import io
import logging
import math
import os
import random
import sys
import tempfile
import unittest
from builtins import object
from builtins import range

import hamcrest as hc

import apache_beam as beam
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io import range_trackers
# importing following private classes for testing
from apache_beam.io.concat_source import ConcatSource
from apache_beam.io.filebasedsource import _SingleFileSource as SingleFileSource
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


class LineSource(FileBasedSource):

  def read_records(self, file_name, range_tracker):
    f = self.open_file(file_name)
    try:
      start = range_tracker.start_position()
      if start > 0:
        # Any line that starts after 'start' does not belong to the current
        # bundle. Seeking to (start - 1) and skipping a line moves the current
        # position to the starting position of the first line that belongs to
        # the current bundle.
        start -= 1
        f.seek(start)
        line = f.readline()
        start += len(line)
      current = start
      line = f.readline()
      while line:
        if not range_tracker.try_claim(current):
          return
        yield line.rstrip(b'\n')
        current += len(line)
        line = f.readline()
    finally:
      f.close()


class EOL(object):
  LF = 1
  CRLF = 2
  MIXED = 3
  LF_WITH_NOTHING_AT_LAST_LINE = 4


def write_data(
    num_lines, no_data=False, directory=None, prefix=tempfile.template,
    eol=EOL.LF):
  """Writes test data to a temporary file.

  Args:
    num_lines (int): The number of lines to write.
    no_data (bool): If :data:`True`, empty lines will be written, otherwise
      each line will contain a concatenation of b'line' and the line number.
    directory (str): The name of the directory to create the temporary file in.
    prefix (str): The prefix to use for the temporary file.
    eol (int): The line ending to use when writing.
      :class:`~apache_beam.io.filebasedsource_test.EOL` exposes attributes that
      can be used here to define the eol.

  Returns:
    Tuple[str, List[bytes]]: A tuple of the filename and a list of the written
      data.
  """
  all_data = []
  with tempfile.NamedTemporaryFile(
      delete=False, dir=directory, prefix=prefix) as f:
    sep_values = [b'\n', b'\r\n']
    for i in range(num_lines):
      data = b'' if no_data else b'line' + str(i).encode()
      all_data.append(data)

      if eol == EOL.LF:
        sep = sep_values[0]
      elif eol == EOL.CRLF:
        sep = sep_values[1]
      elif eol == EOL.MIXED:
        sep = sep_values[i % len(sep_values)]
      elif eol == EOL.LF_WITH_NOTHING_AT_LAST_LINE:
        sep = b'' if i == (num_lines - 1) else sep_values[0]
      else:
        raise ValueError('Received unknown value %s for eol.' % eol)

      f.write(data + sep)

    return f.name, all_data


def _write_prepared_data(data, directory=None,
                         prefix=tempfile.template, suffix=''):
  with tempfile.NamedTemporaryFile(
      delete=False, dir=directory, prefix=prefix, suffix=suffix) as f:
    f.write(data)
    return f.name


def write_prepared_pattern(data, suffixes=None):
  assert data, 'Data (%s) seems to be empty' % data
  if suffixes is None:
    suffixes = [''] * len(data)
  temp_dir = tempfile.mkdtemp()
  for i, d in enumerate(data):
    file_name = _write_prepared_data(d, temp_dir, prefix='mytemp',
                                     suffix=suffixes[i])
  return file_name[:file_name.rfind(os.path.sep)] + os.path.sep + 'mytemp*'


def write_pattern(lines_per_file, no_data=False):
  """Writes a pattern of temporary files.

  Args:
    lines_per_file (List[int]): The number of lines to write per file.
    no_data (bool): If :data:`True`, empty lines will be written, otherwise
      each line will contain a concatenation of b'line' and the line number.

  Returns:
    Tuple[str, List[bytes]]: A tuple of the filename pattern and a list of the
      written data.
  """
  temp_dir = tempfile.mkdtemp()

  all_data = []
  file_name = None
  start_index = 0
  for i in range(len(lines_per_file)):
    file_name, data = write_data(lines_per_file[i], no_data=no_data,
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
      middle = len(self._values) // 2
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

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

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
    self.assertCountEqual(list(range(30)), read_data)

  def test_split(self):
    sources = [TestConcatSource.DummySource(list(range(start, start + 10)))
               for start in [0, 10, 20]]
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
    self.assertCountEqual(list(range(30)), read_data)

  def test_estimate_size(self):
    sources = [TestConcatSource.DummySource(range(start, start + 10)) for start
               in [0, 10, 20]]
    concat = ConcatSource(sources)
    self.assertEquals(30, concat.estimate_size())


class TestFileBasedSource(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def test_string_or_value_provider_only(self):
    str_file_pattern = tempfile.NamedTemporaryFile(delete=False).name
    self.assertEqual(str_file_pattern,
                     FileBasedSource(str_file_pattern)._pattern.value)

    static_vp_file_pattern = StaticValueProvider(value_type=str,
                                                 value=str_file_pattern)
    self.assertEqual(static_vp_file_pattern,
                     FileBasedSource(static_vp_file_pattern)._pattern)

    runtime_vp_file_pattern = RuntimeValueProvider(
        option_name='arg',
        value_type=str,
        default_value=str_file_pattern)
    self.assertEqual(runtime_vp_file_pattern,
                     FileBasedSource(runtime_vp_file_pattern)._pattern)

    invalid_file_pattern = 123
    with self.assertRaises(TypeError):
      FileBasedSource(invalid_file_pattern)

  def test_validation_file_exists(self):
    file_name, _ = write_data(10)
    LineSource(file_name)

  def test_validation_directory_non_empty(self):
    temp_dir = tempfile.mkdtemp()
    file_name, _ = write_data(10, directory=temp_dir)
    LineSource(file_name)

  def test_validation_failing(self):
    no_files_found_error = 'No files found based on the file pattern*'
    with self.assertRaisesRegexp(IOError, no_files_found_error):
      LineSource('dummy_pattern')
    with self.assertRaisesRegexp(IOError, no_files_found_error):
      temp_dir = tempfile.mkdtemp()
      LineSource(os.path.join(temp_dir, '*'))

  def test_validation_file_missing_verification_disabled(self):
    LineSource('dummy_pattern', validate=False)

  def test_fully_read_single_file(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    fbs = LineSource(file_name)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertCountEqual(expected_data, read_data)

  def test_single_file_display_data(self):
    file_name, _ = write_data(10)
    fbs = LineSource(file_name)
    dd = DisplayData.create_from(fbs)
    expected_items = [
        DisplayDataItemMatcher('file_pattern', file_name),
        DisplayDataItemMatcher('compression', 'auto')]
    hc.assert_that(dd.items,
                   hc.contains_inanyorder(*expected_items))

  def test_fully_read_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    fbs = LineSource(pattern)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertCountEqual(expected_data, read_data)

  def test_fully_read_file_pattern_with_empty_files(self):
    pattern, expected_data = write_pattern([5, 0, 12, 0, 8, 0])
    assert len(expected_data) == 25
    fbs = LineSource(pattern)
    range_tracker = fbs.get_range_tracker(None, None)
    read_data = [record for record in fbs.read(range_tracker)]
    self.assertCountEqual(expected_data, read_data)

  def test_estimate_size_of_file(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    fbs = LineSource(file_name)
    self.assertEquals(10 * 6, fbs.estimate_size())

  def test_estimate_size_of_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 10, 8, 8, 4])
    assert len(expected_data) == 38
    fbs = LineSource(pattern)
    self.assertEquals(38 * 6, fbs.estimate_size())

    pattern, expected_data = write_pattern([5, 3, 9])
    assert len(expected_data) == 17
    fbs = LineSource(pattern)
    self.assertEquals(17 * 6, fbs.estimate_size())

  def test_estimate_size_with_sampling_same_size(self):
    num_files = 2 * FileBasedSource.MIN_NUMBER_OF_FILES_TO_STAT
    pattern, _ = write_pattern([10] * num_files)
    # Each line will be of length 6 since write_pattern() uses
    # ('line' + line number + '\n') as data.
    self.assertEqual(
        6 * 10 * num_files, FileBasedSource(pattern).estimate_size())

  def test_estimate_size_with_sampling_different_sizes(self):
    num_files = 2 * FileBasedSource.MIN_NUMBER_OF_FILES_TO_STAT

    # Each line will be of length 8 since write_pattern() uses
    # ('line' + line number + '\n') as data.
    base_size = 500
    variance = 5

    sizes = []
    for _ in range(num_files):
      sizes.append(int(random.uniform(base_size - variance,
                                      base_size + variance)))
    pattern, _ = write_pattern(sizes)
    tolerance = 0.05
    self.assertAlmostEqual(
        base_size * 8 * num_files,
        FileBasedSource(pattern).estimate_size(),
        delta=base_size * 8 * num_files * tolerance)

  def test_splits_into_subranges(self):
    pattern, expected_data = write_pattern([5, 9, 6])
    assert len(expected_data) == 20
    fbs = LineSource(pattern)
    splits = [split for split in fbs.split(desired_bundle_size=15)]
    expected_num_splits = (
        math.ceil(float(6 * 5) / 15) +
        math.ceil(float(6 * 9) / 15) +
        math.ceil(float(6 * 6) / 15))
    assert len(splits) == expected_num_splits

  def test_read_splits_single_file(self):
    file_name, expected_data = write_data(100)
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

    self.assertCountEqual(expected_data, read_data)

  def test_read_splits_file_pattern(self):
    pattern, expected_data = write_pattern([34, 66, 40, 24, 24, 12])
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

    self.assertCountEqual(expected_data, read_data)

  def _run_source_test(self, pattern, expected_data, splittable=True):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        pattern, splittable=splittable))
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_source_file(self):
    file_name, expected_data = write_data(100)
    assert len(expected_data) == 100
    self._run_source_test(file_name, expected_data)

  def test_source_pattern(self):
    pattern, expected_data = write_pattern([34, 66, 40, 24, 24, 12])
    assert len(expected_data) == 200
    self._run_source_test(pattern, expected_data)

  def test_unsplittable_does_not_split(self):
    pattern, expected_data = write_pattern([5, 9, 6])
    assert len(expected_data) == 20
    fbs = LineSource(pattern, splittable=False)
    splits = [split for split in fbs.split(desired_bundle_size=15)]
    self.assertEquals(3, len(splits))

  def test_source_file_unsplittable(self):
    file_name, expected_data = write_data(100)
    assert len(expected_data) == 100
    self._run_source_test(file_name, expected_data, False)

  def test_source_pattern_unsplittable(self):
    pattern, expected_data = write_pattern([34, 66, 40, 24, 24, 12])
    assert len(expected_data) == 200
    self._run_source_test(pattern, expected_data, False)

  def test_read_file_bzip2(self):
    _, lines = write_data(10)
    filename = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    with bz2.BZ2File(filename, 'wb') as f:
      f.write(b'\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        filename,
        splittable=False,
        compression_type=CompressionTypes.BZIP2))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_file_gzip(self):
    _, lines = write_data(10)
    filename = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    with gzip.GzipFile(filename, 'wb') as f:
      f.write(b'\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        filename,
        splittable=False,
        compression_type=CompressionTypes.GZIP))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_pattern_bzip2(self):
    _, lines = write_data(200)
    splits = [0, 34, 100, 140, 164, 188, 200]
    chunks = [lines[splits[i-1]:splits[i]] for i in range(1, len(splits))]
    compressed_chunks = []
    for c in chunks:
      compressobj = bz2.BZ2Compressor()
      compressed_chunks.append(
          compressobj.compress(b'\n'.join(c)) + compressobj.flush())
    file_pattern = write_prepared_pattern(compressed_chunks)
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        file_pattern,
        splittable=False,
        compression_type=CompressionTypes.BZIP2))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_pattern_gzip(self):
    _, lines = write_data(200)
    splits = [0, 34, 100, 140, 164, 188, 200]
    chunks = [lines[splits[i-1]:splits[i]] for i in range(1, len(splits))]
    compressed_chunks = []
    for c in chunks:
      out = io.BytesIO()
      with gzip.GzipFile(fileobj=out, mode="wb") as f:
        f.write(b'\n'.join(c))
      compressed_chunks.append(out.getvalue())
    file_pattern = write_prepared_pattern(compressed_chunks)
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        file_pattern,
        splittable=False,
        compression_type=CompressionTypes.GZIP))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_single_file_bzip2(self):
    _, lines = write_data(10)
    filename = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template, suffix='.bz2').name
    with bz2.BZ2File(filename, 'wb') as f:
      f.write(b'\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        filename,
        compression_type=CompressionTypes.AUTO))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_single_file_gzip(self):
    _, lines = write_data(10)
    filename = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template, suffix='.gz').name
    with gzip.GzipFile(filename, 'wb') as f:
      f.write(b'\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        filename,
        compression_type=CompressionTypes.AUTO))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_pattern(self):
    _, lines = write_data(200)
    splits = [0, 34, 100, 140, 164, 188, 200]
    chunks = [lines[splits[i - 1]:splits[i]] for i in range(1, len(splits))]
    compressed_chunks = []
    for c in chunks:
      out = io.BytesIO()
      with gzip.GzipFile(fileobj=out, mode="wb") as f:
        f.write(b'\n'.join(c))
      compressed_chunks.append(out.getvalue())
    file_pattern = write_prepared_pattern(
        compressed_chunks, suffixes=['.gz']*len(chunks))
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        file_pattern,
        compression_type=CompressionTypes.AUTO))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_pattern_compressed_and_uncompressed(self):
    _, lines = write_data(200)
    splits = [0, 34, 100, 140, 164, 188, 200]
    chunks = [lines[splits[i - 1]:splits[i]] for i in range(1, len(splits))]
    chunks_to_write = []
    for i, c in enumerate(chunks):
      if i%2 == 0:
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
          f.write(b'\n'.join(c))
        chunks_to_write.append(out.getvalue())
      else:
        chunks_to_write.append(b'\n'.join(c))
    file_pattern = write_prepared_pattern(chunks_to_write,
                                          suffixes=(['.gz', '']*3))
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> beam.io.Read(LineSource(
        file_pattern,
        compression_type=CompressionTypes.AUTO))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_splits_get_coder_from_fbs(self):
    class DummyCoder(object):
      val = 12345

    class FileBasedSourceWithCoder(LineSource):

      def default_output_coder(self):
        return DummyCoder()

    pattern, expected_data = write_pattern([34, 66, 40, 24, 24, 12])
    self.assertEqual(200, len(expected_data))
    fbs = FileBasedSourceWithCoder(pattern)
    splits = [split for split in fbs.split(desired_bundle_size=50)]
    self.assertTrue(len(splits))
    for split in splits:
      self.assertEqual(DummyCoder.val, split.source.default_output_coder().val)


class TestSingleFileSource(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def test_source_creation_fails_for_non_number_offsets(self):
    start_not_a_number_error = 'start_offset must be a number*'
    stop_not_a_number_error = 'stop_offset must be a number*'
    file_name = 'dummy_pattern'
    fbs = LineSource(file_name, validate=False)

    with self.assertRaisesRegexp(TypeError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset='aaa', stop_offset='bbb')
    with self.assertRaisesRegexp(TypeError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset='aaa', stop_offset=100)
    with self.assertRaisesRegexp(TypeError, stop_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset='bbb')
    with self.assertRaisesRegexp(TypeError, stop_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=None)
    with self.assertRaisesRegexp(TypeError, start_not_a_number_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=None, stop_offset=100)

  def test_source_creation_display_data(self):
    file_name = 'dummy_pattern'
    fbs = LineSource(file_name, validate=False)
    dd = DisplayData.create_from(fbs)
    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', file_name)]
    hc.assert_that(dd.items,
                   hc.contains_inanyorder(*expected_items))

  def test_source_creation_fails_if_start_lg_stop(self):
    start_larger_than_stop_error = (
        'start_offset must be smaller than stop_offset*')
    fbs = LineSource('dummy_pattern', validate=False)
    SingleFileSource(
        fbs, file_name='dummy_file', start_offset=99, stop_offset=100)
    with self.assertRaisesRegexp(ValueError, start_larger_than_stop_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=99)
    with self.assertRaisesRegexp(ValueError, start_larger_than_stop_error):
      SingleFileSource(
          fbs, file_name='dummy_file', start_offset=100, stop_offset=100)

  def test_estimates_size(self):
    fbs = LineSource('dummy_pattern', validate=False)

    # Should simply return stop_offset - start_offset
    source = SingleFileSource(
        fbs, file_name='dummy_file', start_offset=0, stop_offset=100)
    self.assertEquals(100, source.estimate_size())

    source = SingleFileSource(fbs, file_name='dummy_file', start_offset=10,
                              stop_offset=100)
    self.assertEquals(90, source.estimate_size())

  def test_read_range_at_beginning(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(0, 20)
    read_data = [value for value in source.read(range_tracker)]
    self.assertCountEqual(expected_data[:4], read_data)

  def test_read_range_at_end(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(40, 60)
    read_data = [value for value in source.read(range_tracker)]
    self.assertCountEqual(expected_data[-3:], read_data)

  def test_read_range_at_middle(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10

    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    range_tracker = source.get_range_tracker(20, 40)
    read_data = [value for value in source.read(range_tracker)]
    self.assertCountEqual(expected_data[4:7], read_data)

  def test_produces_splits_desiredsize_large_than_size(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = SingleFileSource(fbs, file_name, 0, 10 * 6)
    splits = [split for split in source.split(desired_bundle_size=100)]
    self.assertEquals(1, len(splits))
    self.assertEquals(60, splits[0].weight)
    self.assertEquals(0, splits[0].start_position)
    self.assertEquals(60, splits[0].stop_position)

    range_tracker = splits[0].source.get_range_tracker(None, None)
    read_data = [value for value in splits[0].source.read(range_tracker)]
    self.assertCountEqual(expected_data, read_data)

  def test_produces_splits_desiredsize_smaller_than_size(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
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
    self.assertCountEqual(expected_data, read_data)

  def test_produce_split_with_start_and_end_positions(self):
    fbs = LineSource('dummy_pattern', validate=False)

    file_name, expected_data = write_data(10)
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
    self.assertCountEqual(expected_data[2:9], read_data)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
