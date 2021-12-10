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
# pytype: skip-file

import bz2
import glob
import gzip
import logging
import os
import shutil
import tempfile
import unittest
import zlib

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import ReadAllFromText
from apache_beam.io import iobase
from apache_beam.io import source_test_utils
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.textio import _TextSink as TextSink
from apache_beam.io.textio import _TextSource as TextSource
# Importing following private classes for testing.
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import ReadFromTextWithFilename
from apache_beam.io.textio import WriteToText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import TempDir
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import Create


class DummyCoder(coders.Coder):
  def encode(self, x):
    raise ValueError

  def decode(self, x):
    return (x * 2).decode('utf-8')


class EOL(object):
  LF = 1
  CRLF = 2
  MIXED = 3
  LF_WITH_NOTHING_AT_LAST_LINE = 4
  CUSTOM_DELIMITER = 5


def write_data(
    num_lines,
    no_data=False,
    directory=None,
    prefix=tempfile.template,
    eol=EOL.LF,
    custom_delimiter=None,
    line_value=b'line'):
  """Writes test data to a temporary file.

  Args:
    num_lines (int): The number of lines to write.
    no_data (bool): If :data:`True`, empty lines will be written, otherwise
      each line will contain a concatenation of b'line' and the line number.
    directory (str): The name of the directory to create the temporary file in.
    prefix (str): The prefix to use for the temporary file.
    eol (int): The line ending to use when writing.
      :class:`~apache_beam.io.textio_test.EOL` exposes attributes that can be
      used here to define the eol.
    custom_delimiter (bytes): The custom delimiter.
    line_value (bytes): Default value for test data, default b'line'

  Returns:
    Tuple[str, List[str]]: A tuple of the filename and a list of the
      utf-8 decoded written data.
  """
  all_data = []
  with tempfile.NamedTemporaryFile(delete=False, dir=directory,
                                   prefix=prefix) as f:
    sep_values = [b'\n', b'\r\n']
    for i in range(num_lines):
      data = b'' if no_data else line_value + str(i).encode()
      all_data.append(data)

      if eol == EOL.LF:
        sep = sep_values[0]
      elif eol == EOL.CRLF:
        sep = sep_values[1]
      elif eol == EOL.MIXED:
        sep = sep_values[i % len(sep_values)]
      elif eol == EOL.LF_WITH_NOTHING_AT_LAST_LINE:
        sep = b'' if i == (num_lines - 1) else sep_values[0]
      elif eol == EOL.CUSTOM_DELIMITER:
        if custom_delimiter is None or len(custom_delimiter) == 0:
          raise ValueError('delimiter can not be null or empty')
        else:
          sep = custom_delimiter
      else:
        raise ValueError('Received unknown value %s for eol.' % eol)

      f.write(data + sep)

    return f.name, [line.decode('utf-8') for line in all_data]


def write_pattern(lines_per_file, no_data=False, return_filenames=False):
  """Writes a pattern of temporary files.

  Args:
    lines_per_file (List[int]): The number of lines to write per file.
    no_data (bool): If :data:`True`, empty lines will be written, otherwise
      each line will contain a concatenation of b'line' and the line number.
    return_filenames (bool): If True, returned list will contain
      (filename, data) pairs.

  Returns:
    Tuple[str, List[Union[str, (str, str)]]]: A tuple of the filename pattern
      and a list of the utf-8 decoded written data or (filename, data) pairs.
  """
  temp_dir = tempfile.mkdtemp()

  all_data = []
  file_name = None
  start_index = 0
  for i in range(len(lines_per_file)):
    file_name, data = write_data(lines_per_file[i], no_data=no_data,
                                 directory=temp_dir, prefix='mytemp')
    if return_filenames:
      all_data.extend(zip([file_name] * len(data), data))
    else:
      all_data.extend(data)
    start_index += lines_per_file[i]

  assert file_name
  return (
      file_name[:file_name.rfind(os.path.sep)] + os.path.sep + 'mytemp*',
      all_data)


class TextSourceTest(unittest.TestCase):

  # Number of records that will be written by most tests.
  DEFAULT_NUM_RECORDS = 100

  def _run_read_test(
      self,
      file_or_pattern,
      expected_data,
      buffer_size=DEFAULT_NUM_RECORDS,
      compression=CompressionTypes.UNCOMPRESSED,
      delimiter=None,
      escapechar=None):
    # Since each record usually takes more than 1 byte, default buffer size is
    # smaller than the total size of the file. This is done to
    # increase test coverage for cases that hit the buffer boundary.
    kwargs = {}
    if delimiter:
      kwargs['delimiter'] = delimiter
    if escapechar:
      kwargs['escapechar'] = escapechar
    source = TextSource(
        file_or_pattern,
        0,
        compression,
        True,
        coders.StrUtf8Coder(),
        buffer_size,
        **kwargs)
    range_tracker = source.get_range_tracker(None, None)
    read_data = list(source.read(range_tracker))
    self.assertCountEqual(expected_data, read_data)

  def test_read_single_file(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_single_file_smaller_than_default_buffer(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    self._run_read_test(
        file_name,
        expected_data,
        buffer_size=TextSource.DEFAULT_READ_BUFFER_SIZE)

  def test_read_single_file_larger_than_default_buffer(self):
    file_name, expected_data = write_data(TextSource.DEFAULT_READ_BUFFER_SIZE)
    self._run_read_test(
        file_name,
        expected_data,
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
    with open(file_name, 'rb') as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)

    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(
        gzip_file_name, expected_data, compression=CompressionTypes.GZIP)

  def test_read_single_file_single_line_no_eol_gzip(self):
    file_name, expected_data = write_data(
        1, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    gzip_file_name = file_name + '.gz'
    with open(file_name, 'rb') as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)

    assert len(expected_data) == 1
    self._run_read_test(
        gzip_file_name, expected_data, compression=CompressionTypes.GZIP)

  def test_read_empty_single_file_no_eol_gzip(self):
    file_name, written_data = write_data(
        1, no_data=True, eol=EOL.LF_WITH_NOTHING_AT_LAST_LINE)

    gzip_file_name = file_name + '.gz'
    with open(file_name, 'rb') as src, gzip.open(gzip_file_name, 'wb') as dst:
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
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        False,
        coders.StrUtf8Coder())

    range_tracker = source.get_range_tracker(None, None)
    read_data = list(source.read(range_tracker))
    self.assertCountEqual([line + '\n' for line in written_data], read_data)

  def test_read_single_file_without_striping_eol_crlf(self):
    file_name, written_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS,
                                         eol=EOL.CRLF)
    assert len(written_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        False,
        coders.StrUtf8Coder())

    range_tracker = source.get_range_tracker(None, None)
    read_data = list(source.read(range_tracker))
    self.assertCountEqual([line + '\r\n' for line in written_data], read_data)

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
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=33))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_header_processing(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10

    def header_matcher(line):
      return line in expected_data[:5]

    header_lines = []

    def store_header(lines):
      for line in lines:
        header_lines.append(line)

    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        header_processor_fns=(header_matcher, store_header))
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)
    read_data = list(source.read_records(file_name, range_tracker))

    self.assertCountEqual(expected_data[:5], header_lines)
    self.assertCountEqual(expected_data[5:], read_data)

  def test_progress(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    fraction_consumed_report = []
    split_points_report = []
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)
    for _ in splits[0].source.read(range_tracker):
      fraction_consumed_report.append(range_tracker.fraction_consumed())
      split_points_report.append(range_tracker.split_points())

    self.assertEqual([float(i) / 10 for i in range(0, 10)],
                     fraction_consumed_report)
    expected_split_points_report = [((i - 1),
                                     iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
                                    for i in range(1, 10)]

    # At last split point, the remaining split points callback returns 1 since
    # the expected position of next record becomes equal to the stop position.
    expected_split_points_report.append((9, 1))

    self.assertEqual(expected_split_points_report, split_points_report)

  def test_read_reentrant_without_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_read_reentrant_after_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    source_test_utils.assert_reentrant_reads_succeed(
        (splits[0].source, splits[0].start_position, splits[0].stop_position))

  def test_dynamic_work_rebalancing(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)

  def test_dynamic_work_rebalancing_windows_eol(self):
    file_name, expected_data = write_data(15, eol=EOL.CRLF)
    assert len(expected_data) == 15
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source,
        splits[0].start_position,
        splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_dynamic_work_rebalancing_mixed_eol(self):
    file_name, expected_data = write_data(5, eol=EOL.MIXED)
    assert len(expected_data) == 5
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder())
    splits = list(source.split(desired_bundle_size=100000))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source,
        splits[0].start_position,
        splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_read_from_text_single_file(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromText(file_name)
      assert_that(pcoll, equal_to(expected_data))

  def test_read_from_text_with_file_name_single_file(self):
    file_name, data = write_data(5)
    expected_data = [(file_name, el) for el in data]
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromTextWithFilename(file_name)
      assert_that(pcoll, equal_to(expected_data))

  def test_read_all_single_file(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create(
          [file_name]) | 'ReadAll' >> ReadAllFromText()
      assert_that(pcoll, equal_to(expected_data))

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
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create([
          file_name1, file_name2, file_name3
      ]) | 'ReadAll' >> ReadAllFromText()
      assert_that(pcoll, equal_to(expected_data))

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
    with TestPipeline() as pipeline:
      pcoll = (
          pipeline
          | 'Create' >> Create([file_name1, file_name2, file_name3, file_name4])
          | 'ReadAll' >> ReadAllFromText())
      assert_that(pcoll, equal_to(expected_data))

  def test_read_from_text_single_file_with_coder(self):

    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromText(file_name, coder=DummyCoder())
      assert_that(pcoll, equal_to([record * 2 for record in expected_data]))

  def test_read_from_text_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromText(pattern)
      assert_that(pcoll, equal_to(expected_data))

  def test_read_from_text_with_file_name_file_pattern(self):
    pattern, expected_data = write_pattern(
        lines_per_file=[5, 5], return_filenames=True)
    assert len(expected_data) == 10
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromTextWithFilename(pattern)
      assert_that(pcoll, equal_to(expected_data))

  def test_read_all_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    with TestPipeline() as pipeline:
      pcoll = (
          pipeline
          | 'Create' >> Create([pattern])
          | 'ReadAll' >> ReadAllFromText())
      assert_that(pcoll, equal_to(expected_data))

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
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create(
          [pattern1, pattern2, pattern3]) | 'ReadAll' >> ReadAllFromText()
      assert_that(pcoll, equal_to(expected_data))

  def test_read_all_with_filename(self):
    pattern, expected_data = write_pattern([5, 3], return_filenames=True)
    assert len(expected_data) == 8

    with TestPipeline() as pipeline:
      pcoll = (
          pipeline
          | 'Create' >> Create([pattern])
          | 'ReadAll' >> ReadAllFromText(with_filename=True))
      assert_that(pcoll, equal_to(expected_data))

  def test_read_auto_bzip2(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file(suffix='.bz2')
      with bz2.BZ2File(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(file_name)
        assert_that(pcoll, equal_to(lines))

  def test_read_auto_deflate(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file(suffix='.deflate')
      with open(file_name, 'wb') as f:
        f.write(zlib.compress('\n'.join(lines).encode('utf-8')))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(file_name)
        assert_that(pcoll, equal_to(lines))

  def test_read_auto_gzip(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file(suffix='.gz')

      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(file_name)
        assert_that(pcoll, equal_to(lines))

  def test_read_bzip2(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with bz2.BZ2File(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name, compression_type=CompressionTypes.BZIP2)
        assert_that(pcoll, equal_to(lines))

  def test_read_corrupted_bzip2_fails(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with bz2.BZ2File(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with open(file_name, 'wb') as f:
        f.write(b'corrupt')

      with self.assertRaises(Exception):
        with TestPipeline() as pipeline:
          pcoll = pipeline | 'Read' >> ReadFromText(
              file_name, compression_type=CompressionTypes.BZIP2)
          assert_that(pcoll, equal_to(lines))

  def test_read_bzip2_concat(self):
    with TempDir() as tempdir:
      bzip2_file_name1 = tempdir.create_temp_file()
      lines = ['a', 'b', 'c']
      with bz2.BZ2File(bzip2_file_name1, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      bzip2_file_name2 = tempdir.create_temp_file()
      lines = ['p', 'q', 'r']
      with bz2.BZ2File(bzip2_file_name2, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      bzip2_file_name3 = tempdir.create_temp_file()
      lines = ['x', 'y', 'z']
      with bz2.BZ2File(bzip2_file_name3, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      final_bzip2_file = tempdir.create_temp_file()
      with open(bzip2_file_name1, 'rb') as src, open(
          final_bzip2_file, 'wb') as dst:
        dst.writelines(src.readlines())

      with open(bzip2_file_name2, 'rb') as src, open(
          final_bzip2_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with open(bzip2_file_name3, 'rb') as src, open(
          final_bzip2_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with TestPipeline() as pipeline:
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(
            final_bzip2_file,
            compression_type=beam.io.filesystem.CompressionTypes.BZIP2)

        expected = ['a', 'b', 'c', 'p', 'q', 'r', 'x', 'y', 'z']
        assert_that(lines, equal_to(expected))

  def test_read_deflate(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with open(file_name, 'wb') as f:
        f.write(zlib.compress('\n'.join(lines).encode('utf-8')))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name, 0, CompressionTypes.DEFLATE, True, coders.StrUtf8Coder())
        assert_that(pcoll, equal_to(lines))

  def test_read_corrupted_deflate_fails(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with open(file_name, 'wb') as f:
        f.write(zlib.compress('\n'.join(lines).encode('utf-8')))

      with open(file_name, 'wb') as f:
        f.write(b'corrupt')

      with self.assertRaises(Exception):
        with TestPipeline() as pipeline:
          pcoll = pipeline | 'Read' >> ReadFromText(
              file_name,
              0,
              CompressionTypes.DEFLATE,
              True,
              coders.StrUtf8Coder())
          assert_that(pcoll, equal_to(lines))

  def test_read_deflate_concat(self):
    with TempDir() as tempdir:
      deflate_file_name1 = tempdir.create_temp_file()
      lines = ['a', 'b', 'c']
      with open(deflate_file_name1, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(zlib.compress(data.encode('utf-8')))

      deflate_file_name2 = tempdir.create_temp_file()
      lines = ['p', 'q', 'r']
      with open(deflate_file_name2, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(zlib.compress(data.encode('utf-8')))

      deflate_file_name3 = tempdir.create_temp_file()
      lines = ['x', 'y', 'z']
      with open(deflate_file_name3, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(zlib.compress(data.encode('utf-8')))

      final_deflate_file = tempdir.create_temp_file()
      with open(deflate_file_name1, 'rb') as src, \
              open(final_deflate_file, 'wb') as dst:
        dst.writelines(src.readlines())

      with open(deflate_file_name2, 'rb') as src, \
              open(final_deflate_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with open(deflate_file_name3, 'rb') as src, \
              open(final_deflate_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with TestPipeline() as pipeline:
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(
            final_deflate_file,
            compression_type=beam.io.filesystem.CompressionTypes.DEFLATE)

        expected = ['a', 'b', 'c', 'p', 'q', 'r', 'x', 'y', 'z']
        assert_that(lines, equal_to(expected))

  def test_read_gzip(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name, 0, CompressionTypes.GZIP, True, coders.StrUtf8Coder())
        assert_that(pcoll, equal_to(lines))

  def test_read_corrupted_gzip_fails(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with open(file_name, 'wb') as f:
        f.write(b'corrupt')

      with self.assertRaises(Exception):
        with TestPipeline() as pipeline:
          pcoll = pipeline | 'Read' >> ReadFromText(
              file_name, 0, CompressionTypes.GZIP, True, coders.StrUtf8Coder())
          assert_that(pcoll, equal_to(lines))

  def test_read_gzip_concat(self):
    with TempDir() as tempdir:
      gzip_file_name1 = tempdir.create_temp_file()
      lines = ['a', 'b', 'c']
      with gzip.open(gzip_file_name1, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      gzip_file_name2 = tempdir.create_temp_file()
      lines = ['p', 'q', 'r']
      with gzip.open(gzip_file_name2, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      gzip_file_name3 = tempdir.create_temp_file()
      lines = ['x', 'y', 'z']
      with gzip.open(gzip_file_name3, 'wb') as dst:
        data = '\n'.join(lines) + '\n'
        dst.write(data.encode('utf-8'))

      final_gzip_file = tempdir.create_temp_file()
      with open(gzip_file_name1, 'rb') as src, \
           open(final_gzip_file, 'wb') as dst:
        dst.writelines(src.readlines())

      with open(gzip_file_name2, 'rb') as src, \
           open(final_gzip_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with open(gzip_file_name3, 'rb') as src, \
           open(final_gzip_file, 'ab') as dst:
        dst.writelines(src.readlines())

      with TestPipeline() as pipeline:
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(
            final_gzip_file,
            compression_type=beam.io.filesystem.CompressionTypes.GZIP)

        expected = ['a', 'b', 'c', 'p', 'q', 'r', 'x', 'y', 'z']
        assert_that(lines, equal_to(expected))

  def test_read_all_gzip(self):
    _, lines = write_data(100)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))
      with TestPipeline() as pipeline:
        pcoll = (
            pipeline
            | Create([file_name])
            | 'ReadAll' >>
            ReadAllFromText(compression_type=CompressionTypes.GZIP))
        assert_that(pcoll, equal_to(lines))

  def test_read_gzip_large(self):
    _, lines = write_data(10000)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()

      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name, 0, CompressionTypes.GZIP, True, coders.StrUtf8Coder())
        assert_that(pcoll, equal_to(lines))

  def test_read_gzip_large_after_splitting(self):
    _, lines = write_data(10000)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      source = TextSource(
          file_name, 0, CompressionTypes.GZIP, True, coders.StrUtf8Coder())
      splits = list(source.split(desired_bundle_size=1000))

      if len(splits) > 1:
        raise ValueError(
            'FileBasedSource generated more than one initial '
            'split for a compressed file.')

      reference_source_info = (source, None, None)
      sources_info = ([
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ])
      source_test_utils.assert_sources_equal_reference_source(
          reference_source_info, sources_info)

  def test_read_gzip_empty_file(self):
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name, 0, CompressionTypes.GZIP, True, coders.StrUtf8Coder())
        assert_that(pcoll, equal_to([]))

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
    return list(source.read(range_tracker))

  def test_read_skip_header_single(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    skip_header_lines = 1
    expected_data = self._remove_lines(
        expected_data, [TextSourceTest.DEFAULT_NUM_RECORDS], skip_header_lines)
    read_data = self._read_skip_header_lines(file_name, skip_header_lines)
    self.assertEqual(len(expected_data), len(read_data))
    self.assertCountEqual(expected_data, read_data)

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
    self.assertCountEqual(expected_data, read_data)

  def test_read_skip_header_pattern_insufficient_lines(self):
    line_counts = [
        5,
        3,  # Fewer lines in file than we want to skip
        12,
        8,
        8,
        4
    ]
    skip_header_lines = 4
    pattern, data = write_pattern(line_counts)

    data = self._remove_lines(data, line_counts, skip_header_lines)
    read_data = self._read_skip_header_lines(pattern, skip_header_lines)
    self.assertEqual(len(data), len(read_data))
    self.assertCountEqual(data, read_data)

  def test_read_gzip_with_skip_lines(self):
    _, lines = write_data(15)
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file()
      with gzip.GzipFile(file_name, 'wb') as f:
        f.write('\n'.join(lines).encode('utf-8'))

      with TestPipeline() as pipeline:
        pcoll = pipeline | 'Read' >> ReadFromText(
            file_name,
            0,
            CompressionTypes.GZIP,
            True,
            coders.StrUtf8Coder(),
            skip_header_lines=2)
        assert_that(pcoll, equal_to(lines[2:]))

  def test_read_after_splitting_skip_header(self):
    file_name, expected_data = write_data(100)
    assert len(expected_data) == 100
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        skip_header_lines=2)
    splits = list(source.split(desired_bundle_size=33))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    self.assertGreater(len(sources_info), 1)
    reference_lines = source_test_utils.read_from_source(*reference_source_info)
    split_lines = []
    for source_info in sources_info:
      split_lines.extend(source_test_utils.read_from_source(*source_info))

    self.assertEqual(expected_data[2:], reference_lines)
    self.assertEqual(reference_lines, split_lines)

  def test_custom_delimiter_read_from_text(self):
    file_name, expected_data = write_data(
      5, eol=EOL.CUSTOM_DELIMITER, custom_delimiter=b'@#')
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Read' >> ReadFromText(file_name, delimiter=b'@#')
      assert_that(pcoll, equal_to(expected_data))

  def test_custom_delimiter_read_all_single_file(self):
    file_name, expected_data = write_data(
      5, eol=EOL.CUSTOM_DELIMITER, custom_delimiter=b'@#')
    assert len(expected_data) == 5
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create(
          [file_name]) | 'ReadAll' >> ReadAllFromText(delimiter=b'@#')
      assert_that(pcoll, equal_to(expected_data))

  def test_invalid_delimiters_are_rejected(self):
    file_name, _ = write_data(1)
    for delimiter in (b'', '', '\r\n', 'a', 1):
      with self.assertRaises(
          ValueError, msg='Delimiter must be a non-empty bytes sequence.'):
        _ = TextSource(
            file_pattern=file_name,
            min_bundle_size=0,
            buffer_size=6,
            compression_type=CompressionTypes.UNCOMPRESSED,
            strip_trailing_newlines=True,
            coder=coders.StrUtf8Coder(),
            delimiter=delimiter,
        )

  def test_non_self_overlapping_delimiter_is_accepted(self):
    file_name, _ = write_data(1)
    for delimiter in (b'\n', b'\r\n', b'*', b'abc', b'cabdab', b'abcabd'):
      _ = TextSource(
          file_pattern=file_name,
          min_bundle_size=0,
          buffer_size=6,
          compression_type=CompressionTypes.UNCOMPRESSED,
          strip_trailing_newlines=True,
          coder=coders.StrUtf8Coder(),
          delimiter=delimiter,
      )

  def test_self_overlapping_delimiter_is_rejected(self):
    file_name, _ = write_data(1)
    for delimiter in (b'||', b'***', b'aba', b'abcab'):
      with self.assertRaises(ValueError,
                             msg='Delimiter must not self-overlap.'):
        _ = TextSource(
            file_pattern=file_name,
            min_bundle_size=0,
            buffer_size=6,
            compression_type=CompressionTypes.UNCOMPRESSED,
            strip_trailing_newlines=True,
            coder=coders.StrUtf8Coder(),
            delimiter=delimiter,
        )

  def test_read_with_customer_delimiter(self):
    delimiters = [
        b'\n',
        b'\r\n',
        b'*|',
        b'*',
        b'*=-',
    ]

    for delimiter in delimiters:
      file_name, expected_data = write_data(
        10,
        eol=EOL.CUSTOM_DELIMITER,
        custom_delimiter=delimiter)

      assert len(expected_data) == 10
      source = TextSource(
          file_pattern=file_name,
          min_bundle_size=0,
          compression_type=CompressionTypes.UNCOMPRESSED,
          strip_trailing_newlines=True,
          coder=coders.StrUtf8Coder(),
          delimiter=delimiter)
      range_tracker = source.get_range_tracker(None, None)
      read_data = list(source.read(range_tracker))

      self.assertEqual(read_data, expected_data)

  def test_read_with_custom_delimiter_around_split_point(self):
    for delimiter in (b'\n', b'\r\n', b'@#', b'abc'):
      file_name, expected_data = write_data(
        20,
        eol=EOL.CUSTOM_DELIMITER,
        custom_delimiter=delimiter)
      assert len(expected_data) == 20
      for desired_bundle_size in (4, 5, 6, 7):
        source = TextSource(
            file_name,
            0,
            CompressionTypes.UNCOMPRESSED,
            True,
            coders.StrUtf8Coder(),
            delimiter=delimiter)
        splits = list(source.split(desired_bundle_size=desired_bundle_size))

        reference_source_info = (source, None, None)
        sources_info = ([
            (split.source, split.start_position, split.stop_position)
            for split in splits
        ])
        source_test_utils.assert_sources_equal_reference_source(
            reference_source_info, sources_info)

  def test_read_with_customer_delimiter_truncated(self):
    """
    Corner case: delimiter truncated at the end of the file
    Use delimiter with length = 3, buffer_size = 6
    and line_value with length = 4
    to split the delimiter
    """
    delimiter = b'@$*'

    file_name, expected_data = write_data(
      10,
      eol=EOL.CUSTOM_DELIMITER,
      line_value=b'a' * 4,
      custom_delimiter=delimiter)

    assert len(expected_data) == 10
    source = TextSource(
        file_pattern=file_name,
        min_bundle_size=0,
        buffer_size=6,
        compression_type=CompressionTypes.UNCOMPRESSED,
        strip_trailing_newlines=True,
        coder=coders.StrUtf8Coder(),
        delimiter=delimiter,
    )
    range_tracker = source.get_range_tracker(None, None)
    read_data = list(source.read(range_tracker))

    self.assertEqual(read_data, expected_data)

  def test_read_with_customer_delimiter_over_buffer_size(self):
    """
    Corner case: delimiter is on border of size of buffer
    """
    file_name, expected_data = write_data(3, eol=EOL.CRLF, line_value=b'\rline')
    assert len(expected_data) == 3
    self._run_read_test(
        file_name, expected_data, buffer_size=7, delimiter=b'\r\n')

  def test_read_with_customer_delimiter_truncated_and_not_equal(self):
    """
    Corner case: delimiter truncated at the end of the file
    and only part of delimiter equal end of buffer

    Use delimiter with length = 3, buffer_size = 6
    and line_value with length = 4
    to split the delimiter
    """

    write_delimiter = b'@$'
    read_delimiter = b'@$*'

    file_name, expected_data = write_data(
      10,
      eol=EOL.CUSTOM_DELIMITER,
      line_value=b'a' * 4,
      custom_delimiter=write_delimiter)

    # In this case check, that the line won't be splitted
    write_delimiter_encode = write_delimiter.decode('utf-8')
    expected_data_str = [
        write_delimiter_encode.join(expected_data) + write_delimiter_encode
    ]

    source = TextSource(
        file_pattern=file_name,
        min_bundle_size=0,
        buffer_size=6,
        compression_type=CompressionTypes.UNCOMPRESSED,
        strip_trailing_newlines=True,
        coder=coders.StrUtf8Coder(),
        delimiter=read_delimiter,
    )
    range_tracker = source.get_range_tracker(None, None)

    read_data = list(source.read(range_tracker))

    self.assertEqual(read_data, expected_data_str)

  def test_read_crlf_split_by_buffer(self):
    file_name, expected_data = write_data(3, eol=EOL.CRLF)
    assert len(expected_data) == 3
    self._run_read_test(file_name, expected_data, buffer_size=6)

  def test_read_escaped_lf(self):
    file_name, expected_data = write_data(
      self.DEFAULT_NUM_RECORDS, eol=EOL.LF, line_value=b'li\\\nne')
    assert len(expected_data) == self.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data, escapechar=b'\\')

  def test_read_escaped_crlf(self):
    file_name, expected_data = write_data(
      TextSource.DEFAULT_READ_BUFFER_SIZE,
      eol=EOL.CRLF,
      line_value=b'li\\\r\\\nne')
    assert len(expected_data) == TextSource.DEFAULT_READ_BUFFER_SIZE
    self._run_read_test(file_name, expected_data, escapechar=b'\\')

  def test_read_escaped_cr_before_not_escaped_lf(self):
    file_name, expected_data_temp = write_data(
      self.DEFAULT_NUM_RECORDS, eol=EOL.CRLF, line_value=b'li\\\r\nne')
    expected_data = []
    for line in expected_data_temp:
      expected_data += line.split("\n")
    assert len(expected_data) == self.DEFAULT_NUM_RECORDS * 2
    self._run_read_test(file_name, expected_data, escapechar=b'\\')

  def test_read_escaped_custom_delimiter_crlf(self):
    file_name, expected_data = write_data(
      self.DEFAULT_NUM_RECORDS, eol=EOL.CRLF, line_value=b'li\\\r\nne')
    assert len(expected_data) == self.DEFAULT_NUM_RECORDS
    self._run_read_test(
        file_name, expected_data, delimiter=b'\r\n', escapechar=b'\\')

  def test_read_escaped_custom_delimiter(self):
    file_name, expected_data = write_data(
      TextSource.DEFAULT_READ_BUFFER_SIZE,
      eol=EOL.CUSTOM_DELIMITER,
      custom_delimiter=b'*|',
      line_value=b'li\\*|ne')
    assert len(expected_data) == TextSource.DEFAULT_READ_BUFFER_SIZE
    self._run_read_test(
        file_name, expected_data, delimiter=b'*|', escapechar=b'\\')

  def test_read_escaped_lf_at_buffer_edge(self):
    file_name, expected_data = write_data(3, eol=EOL.LF, line_value=b'line\\\n')
    assert len(expected_data) == 3
    self._run_read_test(
        file_name, expected_data, buffer_size=5, escapechar=b'\\')

  def test_read_escaped_crlf_split_by_buffer(self):
    file_name, expected_data = write_data(
      3, eol=EOL.CRLF, line_value=b'line\\\r\n')
    assert len(expected_data) == 3
    self._run_read_test(
        file_name,
        expected_data,
        buffer_size=6,
        delimiter=b'\r\n',
        escapechar=b'\\')

  def test_read_escaped_lf_after_splitting(self):
    file_name, expected_data = write_data(3, line_value=b'line\\\n')
    assert len(expected_data) == 3
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        escapechar=b'\\')
    splits = list(source.split(desired_bundle_size=6))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_read_escaped_lf_after_splitting_many(self):
    file_name, expected_data = write_data(
      3, line_value=b'\\\\\\\\\\\n')  # 5 escapes
    assert len(expected_data) == 3
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        escapechar=b'\\')
    splits = list(source.split(desired_bundle_size=6))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_read_escaped_escapechar_after_splitting(self):
    file_name, expected_data = write_data(3, line_value=b'line\\\\*|')
    assert len(expected_data) == 3
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        delimiter=b'*|',
        escapechar=b'\\')
    splits = list(source.split(desired_bundle_size=8))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)

  def test_read_escaped_escapechar_after_splitting_many(self):
    file_name, expected_data = write_data(
      3, line_value=b'\\\\\\\\\\\\*|')  # 6 escapes
    assert len(expected_data) == 3
    source = TextSource(
        file_name,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        delimiter=b'*|',
        escapechar=b'\\')
    splits = list(source.split(desired_bundle_size=8))

    reference_source_info = (source, None, None)
    sources_info = ([(split.source, split.start_position, split.stop_position)
                     for split in splits])
    source_test_utils.assert_sources_equal_reference_source(
        reference_source_info, sources_info)


class TextSinkTest(unittest.TestCase):
  def setUp(self):
    super().setUp()
    self.lines = [b'Line %d' % d for d in range(100)]
    self.tempdir = tempfile.mkdtemp()
    self.path = self._create_temp_file()

  def tearDown(self):
    if os.path.exists(self.tempdir):
      shutil.rmtree(self.tempdir)

  def _create_temp_file(self, name='', suffix=''):
    if not name:
      name = tempfile.template
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name, dir=self.tempdir, suffix=suffix).name
    return file_name

  def _write_lines(self, sink, lines):
    f = sink.open(self.path)
    for line in lines:
      sink.write_record(f, line)
    sink.close(f)

  def test_write_text_file(self):
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with open(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_text_file_empty(self):
    sink = TextSink(self.path)
    self._write_lines(sink, [])

    with open(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), [])

  def test_write_bzip2_file(self):
    sink = TextSink(self.path, compression_type=CompressionTypes.BZIP2)
    self._write_lines(sink, self.lines)

    with bz2.BZ2File(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_bzip2_file_auto(self):
    self.path = self._create_temp_file(suffix='.bz2')
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with bz2.BZ2File(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file(self):
    sink = TextSink(self.path, compression_type=CompressionTypes.GZIP)
    self._write_lines(sink, self.lines)

    with gzip.GzipFile(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file_auto(self):
    self.path = self._create_temp_file(suffix='.gz')
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with gzip.GzipFile(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_gzip_file_empty(self):
    sink = TextSink(self.path, compression_type=CompressionTypes.GZIP)
    self._write_lines(sink, [])

    with gzip.GzipFile(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), [])

  def test_write_deflate_file(self):
    sink = TextSink(self.path, compression_type=CompressionTypes.DEFLATE)
    self._write_lines(sink, self.lines)

    with open(self.path, 'rb') as f:
      self.assertEqual(zlib.decompress(f.read()).splitlines(), self.lines)

  def test_write_deflate_file_auto(self):
    self.path = self._create_temp_file(suffix='.deflate')
    sink = TextSink(self.path)
    self._write_lines(sink, self.lines)

    with open(self.path, 'rb') as f:
      self.assertEqual(zlib.decompress(f.read()).splitlines(), self.lines)

  def test_write_deflate_file_empty(self):
    sink = TextSink(self.path, compression_type=CompressionTypes.DEFLATE)
    self._write_lines(sink, [])

    with open(self.path, 'rb') as f:
      self.assertEqual(zlib.decompress(f.read()).splitlines(), [])

  def test_write_text_file_with_header(self):
    header = b'header1\nheader2'
    sink = TextSink(self.path, header=header)
    self._write_lines(sink, self.lines)

    with open(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), header.splitlines() + self.lines)

  def test_write_text_file_with_footer(self):
    footer = b'footer1\nfooter2'
    sink = TextSink(self.path, footer=footer)
    self._write_lines(sink, self.lines)

    with open(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), self.lines + footer.splitlines())

  def test_write_text_file_empty_with_header(self):
    header = b'header1\nheader2'
    sink = TextSink(self.path, header=header)
    self._write_lines(sink, [])

    with open(self.path, 'rb') as f:
      self.assertEqual(f.read().splitlines(), header.splitlines())

  def test_write_pipeline(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | beam.core.Create(self.lines)
      pcoll | 'Write' >> WriteToText(self.path)  # pylint: disable=expression-not-assigned

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(sorted(read_result), sorted(self.lines))

  def test_write_pipeline_non_globalwindow_input(self):
    with TestPipeline() as p:
      _ = (
          p
          | beam.core.Create(self.lines)
          | beam.WindowInto(beam.transforms.window.FixedWindows(1))
          | 'Write' >> WriteToText(self.path))

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(sorted(read_result), sorted(self.lines))

  def test_write_pipeline_auto_compression(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | beam.core.Create(self.lines)
      pcoll | 'Write' >> WriteToText(self.path, file_name_suffix='.gz')  # pylint: disable=expression-not-assigned

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(sorted(read_result), sorted(self.lines))

  def test_write_pipeline_auto_compression_unsharded(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> beam.core.Create(self.lines)
      pcoll | 'Write' >> WriteToText(  # pylint: disable=expression-not-assigned
          self.path + '.gz',
          shard_name_template='')

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(sorted(read_result), sorted(self.lines))

  def test_write_pipeline_header(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> beam.core.Create(self.lines)
      header_text = 'foo'
      pcoll | 'Write' >> WriteToText(  # pylint: disable=expression-not-assigned
          self.path + '.gz',
          shard_name_template='',
          header=header_text)

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())
    # header_text is automatically encoded in WriteToText
    self.assertEqual(read_result[0], header_text.encode('utf-8'))
    self.assertEqual(sorted(read_result[1:]), sorted(self.lines))

  def test_write_pipeline_footer(self):
    with TestPipeline() as pipeline:
      footer_text = 'footer'
      pcoll = pipeline | beam.core.Create(self.lines)
      pcoll | 'Write' >> WriteToText(   # pylint: disable=expression-not-assigned
        self.path,
        footer=footer_text)

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'rb') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(sorted(read_result[:-1]), sorted(self.lines))
    self.assertEqual(read_result[-1], footer_text.encode('utf-8'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
