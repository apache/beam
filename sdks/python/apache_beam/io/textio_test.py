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

"""Tests for textio module."""

import bz2
import glob
import gzip
import logging
import os
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io import iobase, ReadAllFromText
import apache_beam.io.source_test_utils as source_test_utils

# Importing following private classes for testing.
from apache_beam.io.textio import _TextSink as TextSink
from apache_beam.io.textio import _TextSource as TextSource

from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText

from apache_beam import coders
from apache_beam.io.filebasedsource_test import EOL
from apache_beam.io.filebasedsource_test import write_data
from apache_beam.io.filebasedsource_test import write_pattern
from apache_beam.io.filesystem import CompressionTypes

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from apache_beam.transforms.core import Create


# TODO: Refactor code so all io tests are using same library
# TestCaseWithTempDirCleanup class.
class _TestCaseWithTempDirCleanUp(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result

  def _create_temp_file(self, name='', suffix=''):
    if not name:
      name = tempfile.template
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=self._new_tempdir(), suffix=suffix).name
    return file_name


class TextSourceTest(_TestCaseWithTempDirCleanUp):

  # Number of records that will be written by most tests.
  DEFAULT_NUM_RECORDS = 100

  def _run_read_test(self, file_or_pattern, expected_data,
                     buffer_size=DEFAULT_NUM_RECORDS,
                     compression=CompressionTypes.UNCOMPRESSED):
    # Since each record usually takes more than 1 byte, default buffer size is
    # smaller than the total size of the file. This is done to
    # increase test coverage for cases that hit the buffer boundary.
    source = TextSource(file_or_pattern, 0, compression,
                        True, coders.StrUtf8Coder(), buffer_size)
    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_read_single_file(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_single_file_smaller_than_default_buffer(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    self._run_read_test(file_name, expected_data,
                        buffer_size=TextSource.DEFAULT_READ_BUFFER_SIZE)

  def test_read_single_file_larger_than_default_buffer(self):
    file_name, expected_data = write_data(TextSource.DEFAULT_READ_BUFFER_SIZE)
    self._run_read_test(file_name, expected_data,
                        buffer_size=TextSource.DEFAULT_READ_BUFFER_SIZE)

  def test_read_file_pattern(self):
    pattern, expected_data = write_pattern(
        [TextSourceTest.DEFAULT_NUM_RECORDS * 5,
         TextSourceTest.DEFAULT_NUM_RECORDS * 3,
         TextSourceTest.DEFAULT_NUM_RECORDS * 12,
         TextSourceTest.DEFAULT_NUM_RECORDS * 8,
         TextSourceTest.DEFAULT_NUM_RECORDS * 8,
         TextSourceTest.DEFAULT_NUM_RECORDS * 4])
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS * 40
    self._run_read_test(pattern, expected_data)

  def test_read_single_file_windows_eol(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS,
                                          eol=EOL.CRLF)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_single_file_mixed_eol(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS,
                                          eol=EOL.MIXED)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_single_file_last_line_no_eol(self):
    file_name, expected_data = write_data(
        TextSourceTest.DEFAULT_NUM_RECORDS,
        eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_single_file_single_line_no_eol(self):
    file_name, expected_data = write_data(
        1, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    assert len(expected_data) == 1
    self._run_read_test(file_name, expected_data)

  def test_read_empty_single_file(self):
    file_name, written_data = write_data(
        1, no_data=True, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    assert len(written_data) == 1
    # written data has a single entry with an empty string. Reading the source
    # should not produce anything since we only wrote a single empty string
    # without an end of line character.
    self._run_read_test(file_name, [])

  def test_read_single_file_last_line_no_eol_gzip(self):
    file_name, expected_data = write_data(
        TextSourceTest.DEFAULT_NUM_RECORDS,
        eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    gzip_file_name = file_name + '.gz'
    with open(file_name) as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)

    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(gzip_file_name, expected_data,
                        compression=CompressionTypes.GZIP)

  def test_read_single_file_single_line_no_eol_gzip(self):
    file_name, expected_data = write_data(
        1, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    gzip_file_name = file_name + '.gz'
    with open(file_name) as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)

    assert len(expected_data) == 1
    self._run_read_test(gzip_file_name, expected_data,
                        compression=CompressionTypes.GZIP)

  def test_read_empty_single_file_no_eol_gzip(self):
    file_name, written_data = write_data(
        1, no_data=True, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    gzip_file_name = file_name + '.gz'
    with open(file_name) as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)

    assert len(written_data) == 1
    # written data has a single entry with an empty string. Reading the source
    # should not produce anything since we only wrote a single empty string
    # without an end of line character.
    self._run_read_test(gzip_file_name, [], compression=CompressionTypes.GZIP)

  def test_read_single_file_with_empty_lines(self):
    file_name, expected_data = write_data(
        TextSourceTest.DEFAULT_NUM_RECORDS, no_data=True, eol=EOL.LF)

    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    assert not expected_data[0]

    self._run_read_test(file_name, expected_data)

  def test_read_single_file_without_striping_eol_lf(self):
    file_name, written_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS,
                                         eol=EOL.LF)
    assert len(written_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    source = TextSource(file_name, 0,
                        CompressionTypes.UNCOMPRESSED,
                        False, coders.StrUtf8Coder())

    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    self.assertItemsEqual([line + '\n' for line in written_data], read_data)

  def test_read_single_file_without_striping_eol_crlf(self):
    file_name, written_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS,
                                         eol=EOL.CRLF)
    assert len(written_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED,
                        False, coders.StrUtf8Coder())

    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    self.assertItemsEqual([line + '\r\n' for line in written_data], read_data)

  def test_read_file_pattern_with_empty_files(self):
    pattern, expected_data = write_pattern(
        [5 * TextSourceTest.DEFAULT_NUM_RECORDS,
         3 * TextSourceTest.DEFAULT_NUM_RECORDS,
         12 * TextSourceTest.DEFAULT_NUM_RECORDS,
         8 * TextSourceTest.DEFAULT_NUM_RECORDS,
         8 * TextSourceTest.DEFAULT_NUM_RECORDS,
         4 * TextSourceTest.DEFAULT_NUM_RECORDS],
        no_data=True)
    assert len(expected_data) == 40 * TextSourceTest.DEFAULT_NUM_RECORDS
    assert not expected_data[0]
    self._run_read_test(pattern, expected_data)

  def test_read_after_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=33)]

    reference_source_info = (source, None, None)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_progress(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    fraction_consumed_report = []
    split_points_report = []
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)
    for _ in splits[0].source.read(range_tracker):
      fraction_consumed_report.append(range_tracker.fraction_consumed())
      split_points_report.append(range_tracker.split_points())

    self.assertEqual(
        [float(i) / 10 for i in range(0, 10)], fraction_consumed_report)
    expected_split_points_report = [
        ((i - 1), iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
        for i in range(1, 10)]

    # At last split point, the remaining split points callback returns 1 since
    # the expected position of next record becomes equal to the stop position.
    expected_split_points_report.append((9, 1))

    self.assertEqual(
        expected_split_points_report, split_points_report)

  def test_read_reentrant_without_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_read_reentrant_after_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assert_reentrant_reads_succeed(
        (splits[0].source, splits[0].start_position, splits[0].stop_position))

  def test_dynamic_work_rebalancing(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)

  def test_dynamic_work_rebalancing_windows_eol(self):
    file_name, expected_data = write_data(15, eol=EOL.CRLF)
    assert len(expected_data) == 15
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_dynamic_work_rebalancing_mixed_eol(self):
    file_name, expected_data = write_data(5, eol=EOL.MIXED)
    assert len(expected_data) == 5
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_read_from_text_single_file(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_all_single_file(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> Create(
        [file_name]) |'ReadAll' >> ReadAllFromText()
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_all_many_single_files(self):
    file_name1, expected_data1 = write_data(5)
    assert len(expected_data1) == 5
    file_name2, expected_data2 = write_data(10)
    assert len(expected_data2) == 10
    file_name3, expected_data3 = write_data(15)
    assert len(expected_data3) == 15
    expected_data = []
    expected_data.extend(expected_data1)
    expected_data.extend(expected_data2)
    expected_data.extend(expected_data3)
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> Create(
        [file_name1, file_name2, file_name3]) |'ReadAll' >> ReadAllFromText()
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_all_unavailable_files_ignored(self):
    file_name1, expected_data1 = write_data(5)
    assert len(expected_data1) == 5
    file_name2, expected_data2 = write_data(10)
    assert len(expected_data2) == 10
    file_name3, expected_data3 = write_data(15)
    assert len(expected_data3) == 15
    file_name4 = "/unavailable_file"
    expected_data = []
    expected_data.extend(expected_data1)
    expected_data.extend(expected_data2)
    expected_data.extend(expected_data3)
    pipeline = TestPipeline()
    pcoll = (pipeline
             | 'Create' >> Create(
                 [file_name1, file_name2, file_name3, file_name4])
             |'ReadAll' >> ReadAllFromText())
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_from_text_single_file_with_coder(self):
    class DummyCoder(coders.Coder):
      def encode(self, x):
        raise ValueError

      def decode(self, x):
        return x * 2

    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name, coder=DummyCoder())
    assert_that(pcoll, equal_to([record * 2 for record in expected_data]))
    pipeline.run()

  def test_read_from_text_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(pattern)
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_all_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    pipeline = TestPipeline()
    pcoll = (pipeline
             | 'Create' >> Create([pattern])
             |'ReadAll' >> ReadAllFromText())
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_all_many_file_patterns(self):
    pattern1, expected_data1 = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data1) == 40
    pattern2, expected_data2 = write_pattern([3, 7, 9])
    assert len(expected_data2) == 19
    pattern3, expected_data3 = write_pattern([11, 20, 5, 5])
    assert len(expected_data3) == 41
    expected_data = []
    expected_data.extend(expected_data1)
    expected_data.extend(expected_data2)
    expected_data.extend(expected_data3)
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> Create(
        [pattern1, pattern2, pattern3]) |'ReadAll' >> ReadAllFromText()
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_auto_bzip2(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file(suffix='.bz2')
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_gzip(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file(suffix='.gz')

    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_bzip2(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        compression_type=CompressionTypes.BZIP2)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_corrupted_bzip2_fails(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    with open(file_name, 'wb') as f:
      f.write('corrupt')

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        compression_type=CompressionTypes.BZIP2)
    assert_that(pcoll, equal_to(lines))
    with self.assertRaises(Exception):
      pipeline.run()

  def test_read_bzip2_concat(self):
    bzip2_file_name1 = self._create_temp_file()
    lines = ['a', 'b', 'c']
    with bz2.BZ2File(bzip2_file_name1, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    bzip2_file_name2 = self._create_temp_file()
    lines = ['p', 'q', 'r']
    with bz2.BZ2File(bzip2_file_name2, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    bzip2_file_name3 = self._create_temp_file()
    lines = ['x', 'y', 'z']
    with bz2.BZ2File(bzip2_file_name3, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    final_bzip2_file = self._create_temp_file()
    with open(bzip2_file_name1, 'rb') as src, open(
        final_bzip2_file, 'wb') as dst:
      dst.writelines(src.readlines())

    with open(bzip2_file_name2, 'rb') as src, open(
        final_bzip2_file, 'ab') as dst:
      dst.writelines(src.readlines())

    with open(bzip2_file_name3, 'rb') as src, open(
        final_bzip2_file, 'ab') as dst:
      dst.writelines(src.readlines())

    pipeline = TestPipeline()
    lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(
        final_bzip2_file,
        compression_type=beam.io.filesystem.CompressionTypes.BZIP2)

    expected = ['a', 'b', 'c', 'p', 'q', 'r', 'x', 'y', 'z']
    assert_that(lines, equal_to(expected))
    pipeline.run()

  def test_read_gzip(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_corrupted_gzip_fails(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    with open(file_name, 'wb') as f:
      f.write('corrupt')

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to(lines))

    with self.assertRaises(Exception):
      pipeline.run()

  def test_read_gzip_concat(self):
    gzip_file_name1 = self._create_temp_file()
    lines = ['a', 'b', 'c']
    with gzip.open(gzip_file_name1, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    gzip_file_name2 = self._create_temp_file()
    lines = ['p', 'q', 'r']
    with gzip.open(gzip_file_name2, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    gzip_file_name3 = self._create_temp_file()
    lines = ['x', 'y', 'z']
    with gzip.open(gzip_file_name3, 'wb') as dst:
      data = '\n'.join(lines) + '\n'
      dst.write(data)

    final_gzip_file = self._create_temp_file()
    with open(gzip_file_name1, 'rb') as src, open(final_gzip_file, 'wb') as dst:
      dst.writelines(src.readlines())

    with open(gzip_file_name2, 'rb') as src, open(final_gzip_file, 'ab') as dst:
      dst.writelines(src.readlines())

    with open(gzip_file_name3, 'rb') as src, open(final_gzip_file, 'ab') as dst:
      dst.writelines(src.readlines())

    pipeline = TestPipeline()
    lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(
        final_gzip_file,
        compression_type=beam.io.filesystem.CompressionTypes.GZIP)

    expected = ['a', 'b', 'c', 'p', 'q', 'r', 'x', 'y', 'z']
    assert_that(lines, equal_to(expected))

  def test_read_all_gzip(self):
    _, lines = write_data(100)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))
    pipeline = TestPipeline()
    pcoll = (pipeline
             | Create([file_name])
             | 'ReadAll' >> ReadAllFromText(
                 compression_type=CompressionTypes.GZIP))
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_gzip_large(self):
    _, lines = write_data(10000)
    file_name = self._create_temp_file()

    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_gzip_large_after_splitting(self):
    _, lines = write_data(10000)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    source = TextSource(file_name, 0, CompressionTypes.GZIP, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=1000)]

    if len(splits) > 1:
      raise ValueError('FileBasedSource generated more than one initial split '
                       'for a compressed file.')

    reference_source_info = (source, None, None)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_read_gzip_empty_file(self):
    file_name = self._create_temp_file()
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to([]))
    pipeline.run()

  def _remove_lines(self, lines, sublist_lengths, num_to_remove):
    """Utility function to remove num_to_remove lines from each sublist.

    Args:
      lines: list of items.
      sublist_lengths: list of integers representing length of sublist
        corresponding to each source file.
      num_to_remove: number of lines to remove from each sublist.
    Returns:
      remaining lines.
    """
    curr = 0
    result = []
    for offset in sublist_lengths:
      end = curr + offset
      start = min(curr + num_to_remove, end)
      result += lines[start:end]
      curr += offset
    return result

  def _read_skip_header_lines(self, file_or_pattern, skip_header_lines):
    """Simple wrapper function for instantiating TextSource."""
    source = TextSource(
        file_or_pattern,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        skip_header_lines=skip_header_lines)

    range_tracker = source.get_range_tracker(None, None)
    return [record for record in source.read(range_tracker)]

  def test_read_skip_header_single(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    skip_header_lines = 1
    expected_data = self._remove_lines(expected_data,
                                       [TextSourceTest.DEFAULT_NUM_RECORDS],
                                       skip_header_lines)
    read_data = self._read_skip_header_lines(file_name, skip_header_lines)
    self.assertEqual(len(expected_data), len(read_data))
    self.assertItemsEqual(expected_data, read_data)

  def test_read_skip_header_pattern(self):
    line_counts = [
        TextSourceTest.DEFAULT_NUM_RECORDS * 5,
        TextSourceTest.DEFAULT_NUM_RECORDS * 3,
        TextSourceTest.DEFAULT_NUM_RECORDS * 12,
        TextSourceTest.DEFAULT_NUM_RECORDS * 8,
        TextSourceTest.DEFAULT_NUM_RECORDS * 8,
        TextSourceTest.DEFAULT_NUM_RECORDS * 4
    ]
    skip_header_lines = 2
    pattern, data = write_pattern(line_counts)

    expected_data = self._remove_lines(data, line_counts, skip_header_lines)
    read_data = self._read_skip_header_lines(pattern, skip_header_lines)
    self.assertEqual(len(expected_data), len(read_data))
    self.assertItemsEqual(expected_data, read_data)

  def test_read_skip_header_pattern_insufficient_lines(self):
    line_counts = [
        5, 3, # Fewer lines in file than we want to skip
        12, 8, 8, 4
    ]
    skip_header_lines = 4
    pattern, data = write_pattern(line_counts)

    data = self._remove_lines(data, line_counts, skip_header_lines)
    read_data = self._read_skip_header_lines(pattern, skip_header_lines)
    self.assertEqual(len(data), len(read_data))
    self.assertItemsEqual(data, read_data)

  def test_read_gzip_with_skip_lines(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name, 0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder(), skip_header_lines=2)
    assert_that(pcoll, equal_to(lines[2:]))
    pipeline.run()

  def test_read_after_splitting_skip_header(self):
    file_name, expected_data = write_data(100)
    assert len(expected_data) == 100
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder(), skip_header_lines=2)
    splits = [split for split in source.split(desired_bundle_size=33)]

    reference_source_info = (source, None, None)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    self.assertGreater(len(sources_info), 1)
    reference_lines = source_test_utils.read_from_source(*reference_source_info)
    split_lines = []
    for source_info in sources_info:
      split_lines.extend(source_test_utils.read_from_source(*source_info))

    self.assertEqual(expected_data[2:], reference_lines)
    self.assertEqual(reference_lines, split_lines)


class TextSinkTest(_TestCaseWithTempDirCleanUp):

  def setUp(self):
    super(TextSinkTest, self).setUp()
    self.lines = ['Line %d' % d for d in range(100)]
    self.path = self._create_temp_file()

  def _write_lines(self, sink, lines):
    f = sink.open(self.path)
    for line in lines:
      sink.write_record(f, line)
    sink.close(f)

  def test_write_text_file(self):
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_text_file_empty(self):
    sink = TextSink(self.path)
    self._write_lines(sink, [])

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), [])

  def test_write_bzip2_file(self):
    sink = TextSink(
        self.path, compression_type=CompressionTypes.BZIP2)
    self._write_lines(sink, self.lines)

    with bz2.BZ2File(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_bzip2_file_auto(self):
    self.path = self._create_temp_file(suffix='.bz2')
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with bz2.BZ2File(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file(self):
    sink = TextSink(
        self.path, compression_type=CompressionTypes.GZIP)
    self._write_lines(sink, self.lines)

    with gzip.GzipFile(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file_auto(self):
    self.path = self._create_temp_file(suffix='.gz')
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with gzip.GzipFile(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file_empty(self):
    sink = TextSink(
        self.path, compression_type=CompressionTypes.GZIP)
    self._write_lines(sink, [])

    with gzip.GzipFile(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), [])

  def test_write_text_file_with_header(self):
    header = 'header1\nheader2'
    sink = TextSink(self.path, header=header)
    self._write_lines(sink, self.lines)

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), header.splitlines() + self.lines)

  def test_write_text_file_empty_with_header(self):
    header = 'header1\nheader2'
    sink = TextSink(self.path, header=header)
    self._write_lines(sink, [])

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), header.splitlines())

  def test_write_dataflow(self):
    pipeline = TestPipeline()
    pcoll = pipeline | beam.core.Create(self.lines)
    pcoll | 'Write' >> WriteToText(self.path)  # pylint: disable=expression-not-assigned
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)

  def test_write_dataflow_auto_compression(self):
    pipeline = TestPipeline()
    pcoll = pipeline | beam.core.Create(self.lines)
    pcoll | 'Write' >> WriteToText(self.path, file_name_suffix='.gz')  # pylint: disable=expression-not-assigned
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)

  def test_write_dataflow_auto_compression_unsharded(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> beam.core.Create(self.lines)
    pcoll | 'Write' >> WriteToText(  # pylint: disable=expression-not-assigned
        self.path + '.gz',
        shard_name_template='')

    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)

  def test_write_dataflow_header(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> beam.core.Create(self.lines)
    header_text = 'foo'
    pcoll | 'Write' >> WriteToText(  # pylint: disable=expression-not-assigned
        self.path + '.gz',
        shard_name_template='',
        header=header_text)
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, [header_text] + self.lines)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
