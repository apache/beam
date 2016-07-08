# -*- coding: utf-8 -*-
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

"""Unit tests for local and GCS sources and sinks."""

import glob
import gzip
import logging
import os
import tempfile
import unittest
import zlib

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import fileio
from apache_beam.io import iobase


class TestTextFileSource(unittest.TestCase):

  def create_temp_file(self, text):
    temp = tempfile.NamedTemporaryFile(delete=False)
    with temp.file as tmp:
      tmp.write(text)
    return temp.name

  def read_with_offsets(self, input_lines, output_lines,
                        start_offset=None, end_offset=None):
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(input_lines)),
        start_offset=start_offset, end_offset=end_offset)
    read_lines = []
    with source.reader() as reader:
      for line in reader:
        read_lines.append(line)
    self.assertEqual(read_lines, output_lines)

  def progress_with_offsets(self, input_lines,
                            start_offset=None, end_offset=None):
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(input_lines)),
        start_offset=start_offset, end_offset=end_offset)
    progress_record = []
    with source.reader() as reader:
      self.assertEqual(reader.get_progress().position.byte_offset, -1)
      for line in reader:
        self.assertIsNotNone(line)
        progress_record.append(reader.get_progress().position.byte_offset)

    previous = 0
    for current in progress_record:
      self.assertGreater(current, previous)
      previous = current

  def test_read_entire_file(self):
    lines = ['First', 'Second', 'Third']
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(lines)))
    read_lines = []
    with source.reader() as reader:
      for line in reader:
        read_lines.append(line)
    self.assertEqual(read_lines, lines)

  def test_progress_entire_file(self):
    lines = ['First', 'Second', 'Third']
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(lines)))
    progress_record = []
    with source.reader() as reader:
      self.assertEqual(-1, reader.get_progress().position.byte_offset)
      for line in reader:
        self.assertIsNotNone(line)
        progress_record.append(reader.get_progress().position.byte_offset)
      self.assertEqual(13, reader.get_progress().position.byte_offset)

    self.assertEqual(len(progress_record), 3)
    self.assertEqual(progress_record, [0, 6, 13])

  def try_splitting_reader_at(self, reader, split_request, expected_response):
    actual_response = reader.request_dynamic_split(split_request)

    if expected_response is None:
      self.assertIsNone(actual_response)
    else:
      self.assertIsNotNone(actual_response.stop_position)
      self.assertIsInstance(actual_response.stop_position,
                            iobase.ReaderPosition)
      self.assertIsNotNone(actual_response.stop_position.byte_offset)
      self.assertEqual(expected_response.stop_position.byte_offset,
                       actual_response.stop_position.byte_offset)

      return actual_response

  def test_update_stop_position_for_percent_complete(self):
    lines = ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee']
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(lines)))
    with source.reader() as reader:
      # Reading two lines
      reader_iter = iter(reader)
      next(reader_iter)
      next(reader_iter)
      next(reader_iter)

      # Splitting at end of the range should be unsuccessful
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(percent_complete=0)),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(percent_complete=1)),
          None)

      # Splitting at positions on or before start offset of the last record
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(percent_complete=
                                                           0.2)),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(percent_complete=
                                                           0.4)),
          None)

      # Splitting at a position after the start offset of the last record should
      # be successful
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(percent_complete=
                                                           0.6)),
          iobase.DynamicSplitResultWithPosition(iobase.ReaderPosition(
              byte_offset=15)))

  def test_update_stop_position_percent_complete_for_position(self):
    lines = ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee']
    source = fileio.TextFileSource(
        file_path=self.create_temp_file('\n'.join(lines)))
    with source.reader() as reader:
      # Reading two lines
      reader_iter = iter(reader)
      next(reader_iter)
      next(reader_iter)
      next(reader_iter)

      # Splitting at end of the range should be unsuccessful
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(byte_offset=0))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(byte_offset=25))),
          None)

      # Splitting at positions on or before start offset of the last record
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(byte_offset=5))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(byte_offset=10))),
          None)

      # Splitting at a position after the start offset of the last record should
      # be successful
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(byte_offset=15))),
          iobase.DynamicSplitResultWithPosition(iobase.ReaderPosition(
              byte_offset=15)))

  def run_update_stop_position_exhaustive(self, lines, newline):
    """An exhaustive test for dynamic splitting.

    For the given set of data items, try to perform a split at all possible
    combinations of following.

    * start position
    * original stop position
    * updated stop position
    * number of items read

    Args:
      lines: set of data items to be used to create the file
      newline: separater to be used when writing give set of lines to a text
        file.
    """

    file_path = self.create_temp_file(newline.join(lines))

    total_records = len(lines)
    total_bytes = 0

    for line in lines:
      total_bytes += len(line)
    total_bytes += len(newline) * (total_records - 1)

    for start in xrange(0, total_bytes - 1):
      for end in xrange(start + 1, total_bytes):
        for stop in xrange(start, end):
          for records_to_read in range(0, total_records):
            self.run_update_stop_position(start, end, stop, records_to_read,
                                          file_path)

  def test_update_stop_position_exhaustive(self):
    self.run_update_stop_position_exhaustive(
        ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee'], '\n')

  def test_update_stop_position_exhaustive_with_empty_lines(self):
    self.run_update_stop_position_exhaustive(
        ['', 'aaaa', '', 'bbbb', 'cccc', '', 'dddd', 'eeee', ''], '\n')

  def test_update_stop_position_exhaustive_windows_newline(self):
    self.run_update_stop_position_exhaustive(
        ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee'], '\r\n')

  def test_update_stop_position_exhaustive_multi_byte(self):
    self.run_update_stop_position_exhaustive(
        [u'අඅඅඅ'.encode('utf-8'), u'බබබබ'.encode('utf-8'),
         u'කකකක'.encode('utf-8')], '\n')

  def run_update_stop_position(self, start_offset, end_offset, stop_offset,
                               records_to_read,
                               file_path):
    source = fileio.TextFileSource(file_path, start_offset, end_offset)

    records_of_first_split = ''

    with source.reader() as reader:
      reader_iter = iter(reader)
      i = 0

      try:
        while i < records_to_read:
          records_of_first_split += next(reader_iter)
          i += 1
      except StopIteration:
        # Invalid case, given source does not contain this many records.
        return

      last_record_start_after_reading = reader.range_tracker.last_record_start

      if stop_offset <= last_record_start_after_reading:
        expected_split_response = None
      elif stop_offset == start_offset or stop_offset == end_offset:
        expected_split_response = None
      elif records_to_read == 0:
        expected_split_response = None  # unstarted
      else:
        expected_split_response = iobase.DynamicSplitResultWithPosition(
            stop_position=iobase.ReaderPosition(byte_offset=stop_offset))

      split_response = self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(progress=iobase.ReaderProgress(
              iobase.ReaderPosition(byte_offset=stop_offset))),
          expected_split_response)

      # Reading remaining records from the updated reader.
      for line in reader:
        records_of_first_split += line

    if split_response is not None:
      # Total contents received by reading the two splits should be equal to the
      # result obtained by reading the original source.
      records_of_original = ''
      records_of_second_split = ''

      with source.reader() as original_reader:
        for line in original_reader:
          records_of_original += line

      new_source = fileio.TextFileSource(
          file_path,
          split_response.stop_position.byte_offset,
          end_offset)
      with new_source.reader() as reader:
        for line in reader:
          records_of_second_split += line

      self.assertEqual(records_of_original,
                       records_of_first_split + records_of_second_split)

  def test_various_offset_combination_with_local_file_for_read(self):
    lines = ['01234', '6789012', '456789012']
    self.read_with_offsets(lines, lines[1:], start_offset=5)
    self.read_with_offsets(lines, lines[1:], start_offset=6)
    self.read_with_offsets(lines, lines[2:], start_offset=7)
    self.read_with_offsets(lines, lines[1:2], start_offset=5, end_offset=13)
    self.read_with_offsets(lines, lines[1:2], start_offset=5, end_offset=14)
    self.read_with_offsets(lines, lines[1:], start_offset=5, end_offset=16)
    self.read_with_offsets(lines, lines[2:], start_offset=14, end_offset=20)
    self.read_with_offsets(lines, lines[2:], start_offset=14)
    self.read_with_offsets(lines, [], start_offset=20, end_offset=20)

  def test_various_offset_combination_with_local_file_for_progress(self):
    lines = ['01234', '6789012', '456789012']
    self.progress_with_offsets(lines, start_offset=5)
    self.progress_with_offsets(lines, start_offset=6)
    self.progress_with_offsets(lines, start_offset=7)
    self.progress_with_offsets(lines, start_offset=5, end_offset=13)
    self.progress_with_offsets(lines, start_offset=5, end_offset=14)
    self.progress_with_offsets(lines, start_offset=5, end_offset=16)
    self.progress_with_offsets(lines, start_offset=14, end_offset=20)
    self.progress_with_offsets(lines, start_offset=14)
    self.progress_with_offsets(lines, start_offset=20, end_offset=20)


class NativeTestTextFileSink(unittest.TestCase):

  def create_temp_file(self):
    temp = tempfile.NamedTemporaryFile(delete=False)
    return temp.name

  def test_write_entire_file(self):
    lines = ['First', 'Second', 'Third']
    file_path = self.create_temp_file()
    sink = fileio.NativeTextFileSink(file_path)
    with sink.writer() as writer:
      for line in lines:
        writer.Write(line)
    with open(file_path, 'r') as f:
      self.assertEqual(f.read().splitlines(), lines)


class TestTextFileSink(unittest.TestCase):

  def setUp(self):
    self.lines = ['Line %d' % d for d in range(100)]
    self.path = tempfile.NamedTemporaryFile().name

  def _write_lines(self, sink, lines):
    f = sink.open(self.path)
    for line in lines:
      sink.write_record(f, line)
    sink.close(f)

  def test_write_text_file(self):
    sink = fileio.TextFileSink(self.path)
    self._write_lines(sink, self.lines)

    with open(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_deflate_file(self):
    sink = fileio.TextFileSink(self.path,
                               compression_type=fileio.CompressionTypes.DEFLATE)
    self._write_lines(sink, self.lines)

    with open(self.path, 'r') as f:
      content = f.read()
      self.assertEqual(
          zlib.decompress(content, -zlib.MAX_WBITS).splitlines(), self.lines)

  def test_write_gzip_file(self):
    sink = fileio.TextFileSink(self.path,
                               compression_type=fileio.CompressionTypes.GZIP)
    self._write_lines(sink, self.lines)

    with gzip.GzipFile(self.path, 'r') as f:
      self.assertEqual(f.read().splitlines(), self.lines)

  def test_write_zlib_file(self):
    sink = fileio.TextFileSink(self.path,
                               compression_type=fileio.CompressionTypes.ZLIB)
    self._write_lines(sink, self.lines)

    with open(self.path, 'r') as f:
      content = f.read()
      # Below decompress option should work for both zlib/gzip header
      # auto detection.
      self.assertEqual(
          zlib.decompress(content, zlib.MAX_WBITS | 32).splitlines(),
          self.lines)


class MyFileSink(fileio.FileSink):

  def open(self, temp_path):
    # TODO(robertwb): Fix main session pickling.
    # file_handle = super(MyFileSink, self).open(temp_path)
    file_handle = fileio.FileSink.open(self, temp_path)
    file_handle.write('[start]')
    return file_handle

  def write_encoded_record(self, file_handle, encoded_value):
    file_handle.write('[')
    file_handle.write(encoded_value)
    file_handle.write(']')

  def close(self, file_handle):
    file_handle.write('[end]')
    # TODO(robertwb): Fix main session pickling.
    # file_handle = super(MyFileSink, self).close(file_handle)
    file_handle = fileio.FileSink.close(self, file_handle)


class TestFileSink(unittest.TestCase):

  def test_file_sink_writing(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(temp_path,
                      file_name_suffix='.foo',
                      coder=coders.ToStringCoder())

    # Manually invoke the generic Sink API.
    init_token = sink.initialize_write()

    writer1 = sink.open_writer(init_token, '1')
    writer1.write('a')
    writer1.write('b')
    res1 = writer1.close()

    writer2 = sink.open_writer(init_token, '2')
    writer2.write('x')
    writer2.write('y')
    writer2.write('z')
    res2 = writer2.close()

    res = list(sink.finalize_write(init_token, [res1, res2]))
    # Retry the finalize operation (as if the first attempt was lost).
    res = list(sink.finalize_write(init_token, [res1, res2]))

    # Check the results.
    shard1 = temp_path + '-00000-of-00002.foo'
    shard2 = temp_path + '-00001-of-00002.foo'
    self.assertEqual(res, [shard1, shard2])
    self.assertEqual(open(shard1).read(), '[start][a][b][end]')
    self.assertEqual(open(shard2).read(), '[start][x][y][z][end]')

    # Check that any temp files are deleted.
    self.assertItemsEqual([shard1, shard2], glob.glob(temp_path + '*'))

  def test_empty_write(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(temp_path,
                      file_name_suffix='.foo',
                      coder=coders.ToStringCoder())
    p = beam.Pipeline('DirectPipelineRunner')
    p | beam.Create([]) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned
    p.run()
    self.assertEqual(open(temp_path + '-00000-of-00001.foo').read(),
                     '[start][end]')

  def test_fixed_shard_write(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(temp_path,
                      file_name_suffix='.foo',
                      num_shards=3,
                      shard_name_template='_NN_SSS_',
                      coder=coders.ToStringCoder())
    p = beam.Pipeline('DirectPipelineRunner')
    p | beam.Create(['a', 'b']) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned

    p.run()

    concat = ''.join(open(temp_path + '_03_%03d_.foo' % shard_num).read()
                     for shard_num in range(3))
    self.assertTrue('][a][' in concat, concat)
    self.assertTrue('][b][' in concat, concat)

  def test_file_sink_multi_shards(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(temp_path,
                      file_name_suffix='.foo',
                      coder=coders.ToStringCoder())

    # Manually invoke the generic Sink API.
    init_token = sink.initialize_write()

    num_shards = 1000
    writer_results = []
    for i in range(num_shards):
      uuid = 'uuid-%05d' % i
      writer = sink.open_writer(init_token, uuid)
      writer.write('a')
      writer.write('b')
      writer.write(uuid)
      writer_results.append(writer.close())

    res_first = list(sink.finalize_write(init_token, writer_results))
    # Retry the finalize operation (as if the first attempt was lost).
    res_second = list(sink.finalize_write(init_token, writer_results))

    self.assertItemsEqual(res_first, res_second)

    res = sorted(res_second)
    for i in range(num_shards):
      shard_name = '%s-%05d-of-%05d.foo' % (temp_path, i, num_shards)
      uuid = 'uuid-%05d' % i
      self.assertEqual(res[i], shard_name)
      self.assertEqual(
          open(shard_name).read(), ('[start][a][b][%s][end]' % uuid))

    # Check that any temp files are deleted.
    self.assertItemsEqual(res, glob.glob(temp_path + '*'))

  def test_file_sink_io_error(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(temp_path,
                      file_name_suffix='.foo',
                      coder=coders.ToStringCoder())

    # Manually invoke the generic Sink API.
    init_token = sink.initialize_write()

    writer1 = sink.open_writer(init_token, '1')
    writer1.write('a')
    writer1.write('b')
    res1 = writer1.close()

    writer2 = sink.open_writer(init_token, '2')
    writer2.write('x')
    writer2.write('y')
    writer2.write('z')
    res2 = writer2.close()

    os.remove(res2)
    with self.assertRaises(IOError):
      list(sink.finalize_write(init_token, [res1, res2]))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
