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
import logging
import os
import shutil
import tempfile
import unittest

import hamcrest as hc
import mock

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import fileio
from apache_beam.test_pipeline import TestPipeline
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


class TestChannelFactory(unittest.TestCase):

  @mock.patch('apache_beam.io.fileio.gcsio')
  def test_size_of_files_in_glob_complete(self, *unused_args):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    fileio.gcsio.GcsIO = lambda: gcsio_mock
    file_names = ['gs://bucket/file1', 'gs://bucket/file2']
    gcsio_mock.size_of_files_in_glob.return_value = {
        'gs://bucket/file1': 1,
        'gs://bucket/file2': 2
    }
    expected_results = {
        'gs://bucket/file1': 1,
        'gs://bucket/file2': 2
    }
    self.assertEqual(
        fileio.ChannelFactory.size_of_files_in_glob(
            'gs://bucket/*', file_names),
        expected_results)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with('gs://bucket/*')

  @mock.patch('apache_beam.io.fileio.gcsio')
  def test_size_of_files_in_glob_incomplete(self, *unused_args):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    fileio.gcsio.GcsIO = lambda: gcsio_mock
    file_names = ['gs://bucket/file1', 'gs://bucket/file2']
    gcsio_mock.size_of_files_in_glob.return_value = {
        'gs://bucket/file1': 1
    }
    gcsio_mock.size.return_value = 2
    expected_results = {
        'gs://bucket/file1': 1,
        'gs://bucket/file2': 2
    }
    self.assertEqual(
        fileio.ChannelFactory.size_of_files_in_glob(
            'gs://bucket/*', file_names),
        expected_results)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with('gs://bucket/*')
    gcsio_mock.size.assert_called_once_with('gs://bucket/file2')


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


class TestCompressedFile(_TestCaseWithTempDirCleanUp):

  def test_seekable(self):
    readable = fileio._CompressedFile(open(self._create_temp_file(), 'r'))
    self.assertFalse(readable.seekable)

    writeable = fileio._CompressedFile(open(self._create_temp_file(), 'w'))
    self.assertFalse(writeable.seekable)

  def test_tell(self):
    lines = ['line%d\n' % i for i in range(10)]
    tmpfile = self._create_temp_file()
    writeable = fileio._CompressedFile(open(tmpfile, 'w'))
    current_offset = 0
    for line in lines:
      writeable.write(line)
      current_offset += len(line)
      self.assertEqual(current_offset, writeable.tell())

    writeable.close()
    readable = fileio._CompressedFile(open(tmpfile))
    current_offset = 0
    while True:
      line = readable.readline()
      current_offset += len(line)
      self.assertEqual(current_offset, readable.tell())
      if not line:
        break


class MyFileSink(fileio.FileSink):

  def open(self, temp_path):
    # TODO: Fix main session pickling.
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
    # TODO: Fix main session pickling.
    # file_handle = super(MyFileSink, self).close(file_handle)
    file_handle = fileio.FileSink.close(self, file_handle)


class TestFileSink(_TestCaseWithTempDirCleanUp):

  def test_file_sink_writing(self):
    temp_path = os.path.join(self._new_tempdir(), 'filesink')
    sink = MyFileSink(
        temp_path, file_name_suffix='.foo', coder=coders.ToStringCoder())

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

    _ = list(sink.finalize_write(init_token, [res1, res2]))
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

  def test_file_sink_display_data(self):
    temp_path = os.path.join(self._new_tempdir(), 'display')
    sink = MyFileSink(
        temp_path, file_name_suffix='.foo', coder=coders.ToStringCoder())
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher(
            'compression', 'auto'),
        DisplayDataItemMatcher(
            'file_pattern',
            '{}{}'.format(temp_path,
                          '-%(shard_num)05d-of-%(num_shards)05d.foo'))]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_empty_write(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileSink(
        temp_path, file_name_suffix='.foo', coder=coders.ToStringCoder())
    p = TestPipeline()
    p | beam.Create([]) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned
    p.run()
    self.assertEqual(
        open(temp_path + '-00000-of-00001.foo').read(), '[start][end]')

  def test_fixed_shard_write(self):
    temp_path = os.path.join(self._new_tempdir(), 'empty')
    sink = MyFileSink(
        temp_path,
        file_name_suffix='.foo',
        num_shards=3,
        shard_name_template='_NN_SSS_',
        coder=coders.ToStringCoder())
    p = TestPipeline()
    p | beam.Create(['a', 'b']) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned

    p.run()

    concat = ''.join(
        open(temp_path + '_03_%03d_.foo' % shard_num).read()
        for shard_num in range(3))
    self.assertTrue('][a][' in concat, concat)
    self.assertTrue('][b][' in concat, concat)

  def test_file_sink_multi_shards(self):
    temp_path = os.path.join(self._new_tempdir(), 'multishard')
    sink = MyFileSink(
        temp_path, file_name_suffix='.foo', coder=coders.ToStringCoder())

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
    temp_path = os.path.join(self._new_tempdir(), 'ioerror')
    sink = MyFileSink(
        temp_path, file_name_suffix='.foo', coder=coders.ToStringCoder())

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
    with self.assertRaises(Exception):
      list(sink.finalize_write(init_token, [res1, res2]))

  @mock.patch('apache_beam.io.fileio.ChannelFactory.rename')
  @mock.patch('apache_beam.io.fileio.gcsio')
  def test_rename_batch(self, *unused_args):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    fileio.gcsio.GcsIO = lambda: gcsio_mock
    fileio.ChannelFactory.rename = mock.MagicMock()
    to_rename = [
        ('gs://bucket/from1', 'gs://bucket/to1'),
        ('gs://bucket/from2', 'gs://bucket/to2'),
        ('/local/from1', '/local/to1'),
        ('gs://bucket/from3', 'gs://bucket/to3'),
        ('/local/from2', '/local/to2'),
    ]
    gcsio_mock.copy_batch.side_effect = [[
        ('gs://bucket/from1', 'gs://bucket/to1', None),
        ('gs://bucket/from2', 'gs://bucket/to2', None),
        ('gs://bucket/from3', 'gs://bucket/to3', None),
    ]]
    gcsio_mock.delete_batch.side_effect = [[
        ('gs://bucket/from1', None),
        ('gs://bucket/from2', None),
        ('gs://bucket/from3', None),
    ]]

    # Issue batch rename.
    fileio.ChannelFactory.rename_batch(to_rename)

    # Verify mocks.
    expected_local_rename_calls = [
        mock.call('/local/from1', '/local/to1'),
        mock.call('/local/from2', '/local/to2'),
    ]
    self.assertEqual(fileio.ChannelFactory.rename.call_args_list,
                     expected_local_rename_calls)
    gcsio_mock.copy_batch.assert_called_once_with([
        ('gs://bucket/from1', 'gs://bucket/to1'),
        ('gs://bucket/from2', 'gs://bucket/to2'),
        ('gs://bucket/from3', 'gs://bucket/to3'),
    ])
    gcsio_mock.delete_batch.assert_called_once_with([
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
