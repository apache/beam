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
import tempfile
import unittest

import hamcrest as hc

import apache_beam as beam
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
from apache_beam.io.fileio import CompressionTypes

from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class TextSourceTest(unittest.TestCase):

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
    source_test_utils.assertSourcesEqualReferenceSource(
        reference_source_info, sources_info)

  def test_progress(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    fraction_consumed_report = []
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)
    for _ in splits[0].source.read(range_tracker):
      fraction_consumed_report.append(range_tracker.fraction_consumed())

    self.assertEqual(
        [float(i) / 10 for i in range(0, 10)], fraction_consumed_report)

  def test_read_reentrant_without_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    source_test_utils.assertReentrantReadsSucceed((source, None, None))

  def test_read_reentrant_after_splitting(self):
    file_name, expected_data = write_data(10)
    assert len(expected_data) == 10
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assertReentrantReadsSucceed(
        (splits[0].source, splits[0].start_position, splits[0].stop_position))

  def test_dynamic_work_rebalancing(self):
    file_name, expected_data = write_data(15)
    assert len(expected_data) == 15
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assertSplitAtFractionExhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)

  def test_dynamic_work_rebalancing_windows_eol(self):
    file_name, expected_data = write_data(15, eol=EOL.CRLF)
    assert len(expected_data) == 15
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assertSplitAtFractionExhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_dynamic_work_rebalancing_mixed_eol(self):
    file_name, expected_data = write_data(15, eol=EOL.MIXED)
    assert len(expected_data) == 15
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder())
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assertSplitAtFractionExhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position,
        perform_multi_threaded_test=False)

  def test_read_display_data(self):
    read = ReadFromText('prefix', validate=False)
    dd = DisplayData.create_from(read)
    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', 'prefix'),
        DisplayDataItemMatcher('strip_newline', True)]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_dataflow_single_file(self):
    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_dataflow_single_file_with_coder(self):
    class DummyCoder(coders.Coder):
      def encode(self, x):
        raise ValueError

      def decode(self, x):
        return x * 2

    file_name, expected_data = write_data(5)
    assert len(expected_data) == 5
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(file_name, coder=DummyCoder())
    assert_that(pcoll, equal_to([record * 2 for record in expected_data]))
    pipeline.run()

  def test_dataflow_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(pattern)
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_auto_bzip2(self):
    _, lines = write_data(15)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template, suffix='.bz2').name
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_gzip(self):
    _, lines = write_data(15)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template, suffix='.gz').name
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_bzip2(self):
    _, lines = write_data(15)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        compression_type=CompressionTypes.BZIP2)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_gzip(self):
    _, lines = write_data(15)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_gzip_large(self):
    _, lines = write_data(10000)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder())
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_gzip_large_after_splitting(self):
    _, lines = write_data(10000)
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
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
    source_test_utils.assertSourcesEqualReferenceSource(
        reference_source_info, sources_info)

  def test_read_gzip_empty_file(self):
    filename = tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template).name
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | 'Read' >> ReadFromText(
        filename,
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
    for offset in offsets:
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


class TextSinkTest(unittest.TestCase):

  def setUp(self):
    self.lines = ['Line %d' % d for d in range(100)]
    self.path = tempfile.NamedTemporaryFile().name

  def tearDown(self):
    if os.path.exists(self.path):
      os.remove(self.path)

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
    self.path = tempfile.NamedTemporaryFile(suffix='.bz2').name
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
    self.path = tempfile.NamedTemporaryFile(suffix='.gz').name
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

  def test_write_text_file_empty_with_heather(self):
    header = 'header1\nheader2'
    sink = TextSink(self.path, header=header)
    self._write_lines(sink, [])

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), header.splitlines())

  def test_write_display_data(self):
    write = WriteToText('prefix')
    dd = DisplayData.create_from(write)
    expected_items = [
        DisplayDataItemMatcher(
            'append_newline', True),
        DisplayDataItemMatcher(
            'compression', 'auto'),
        DisplayDataItemMatcher(
            'shards', 0),
        DisplayDataItemMatcher(
            'file_pattern',
            '{}{}'.format('prefix',
                          '-%(shard_num)05d-of-%(num_shards)05d'))]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_write_dataflow(self):
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | beam.core.Create('Create', self.lines)
    pcoll | 'Write' >> WriteToText(self.path)  # pylint: disable=expression-not-assigned
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)

  def test_write_dataflow_auto_compression(self):
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | beam.core.Create('Create', self.lines)
    pcoll | 'Write' >> WriteToText(self.path, file_name_suffix='.gz')  # pylint: disable=expression-not-assigned
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)

  def test_write_dataflow_auto_compression_unsharded(self):
    pipeline = beam.Pipeline('DirectRunner')
    pcoll = pipeline | beam.core.Create('Create', self.lines)
    pcoll | 'Write' >> WriteToText(self.path + '.gz', shard_name_template='')  # pylint: disable=expression-not-assigned
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result, self.lines)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
