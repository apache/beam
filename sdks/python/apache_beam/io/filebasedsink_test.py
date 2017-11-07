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

"""Unit tests for file sinks."""

import glob
import logging
import os
import shutil
import tempfile
import unittest

import hamcrest as hc
import mock

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.io import filebasedsink
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


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


class MyFileBasedSink(filebasedsink.FileBasedSink):

  def open(self, temp_path):
    # TODO: Fix main session pickling.
    # file_handle = super(MyFileBasedSink, self).open(temp_path)
    file_handle = filebasedsink.FileBasedSink.open(self, temp_path)
    file_handle.write('[start]')
    return file_handle

  def write_encoded_record(self, file_handle, encoded_value):
    file_handle.write('[')
    file_handle.write(encoded_value)
    file_handle.write(']')

  def close(self, file_handle):
    file_handle.write('[end]')
    # TODO: Fix main session pickling.
    # file_handle = super(MyFileBasedSink, self).close(file_handle)
    file_handle = filebasedsink.FileBasedSink.close(self, file_handle)


class TestFileBasedSink(_TestCaseWithTempDirCleanUp):

  def test_file_sink_writing(self):
    temp_path = os.path.join(self._new_tempdir(), 'FileBasedSink')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToStringCoder())

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
    shard1 = temp_path + '-00000-of-00002.output'
    shard2 = temp_path + '-00001-of-00002.output'
    self.assertEqual(res, [shard1, shard2])
    self.assertEqual(open(shard1).read(), '[start][a][b][end]')
    self.assertEqual(open(shard2).read(), '[start][x][y][z][end]')

    # Check that any temp files are deleted.
    self.assertItemsEqual([shard1, shard2], glob.glob(temp_path + '*'))

  def test_file_sink_display_data(self):
    temp_path = os.path.join(self._new_tempdir(), 'display')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToStringCoder())
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher(
            'compression', 'auto'),
        DisplayDataItemMatcher(
            'file_pattern',
            '{}{}'.format(
                temp_path,
                '-%(shard_num)05d-of-%(num_shards)05d.output'))]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_empty_write(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToStringCoder()
    )
    with TestPipeline() as p:
      p | beam.Create([]) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned
    self.assertEqual(
        open(temp_path + '-00000-of-00001.output').read(), '[start][end]')

  def test_static_value_provider_empty_write(self):
    temp_path = StaticValueProvider(value_type=str,
                                    value=tempfile.NamedTemporaryFile().name)
    sink = MyFileBasedSink(
        temp_path,
        file_name_suffix=StaticValueProvider(value_type=str, value='.output'),
        coder=coders.ToStringCoder()
    )
    with TestPipeline() as p:
      p | beam.Create([]) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned
    self.assertEqual(
        open(temp_path.get() + '-00000-of-00001.output').read(), '[start][end]')

  def test_fixed_shard_write(self):
    temp_path = os.path.join(self._new_tempdir(), 'empty')
    sink = MyFileBasedSink(
        temp_path,
        file_name_suffix='.output',
        num_shards=3,
        shard_name_template='_NN_SSS_',
        coder=coders.ToStringCoder())
    with TestPipeline() as p:
      p | beam.Create(['a', 'b']) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned

    concat = ''.join(
        open(temp_path + '_03_%03d_.output' % shard_num).read()
        for shard_num in range(3))
    self.assertTrue('][a][' in concat, concat)
    self.assertTrue('][b][' in concat, concat)

  # Not using 'test' in name so that 'nose' doesn't pick this as a test.
  def run_temp_dir_check(self, no_dir_path, dir_path, no_dir_root_path,
                         dir_root_path, prefix, separator):
    def _get_temp_dir(file_path_prefix):
      sink = MyFileBasedSink(
          file_path_prefix, file_name_suffix='.output',
          coder=coders.ToStringCoder())
      return sink.initialize_write()

    temp_dir = _get_temp_dir(no_dir_path)
    self.assertTrue(temp_dir.startswith(prefix))
    last_sep = temp_dir.rfind(separator)
    self.assertTrue(temp_dir[last_sep + 1:].startswith('beam-temp'))

    temp_dir = _get_temp_dir(dir_path)
    self.assertTrue(temp_dir.startswith(prefix))
    last_sep = temp_dir.rfind(separator)
    self.assertTrue(temp_dir[last_sep + 1:].startswith('beam-temp'))

    with self.assertRaises(ValueError):
      _get_temp_dir(no_dir_root_path)

    with self.assertRaises(ValueError):
      _get_temp_dir(dir_root_path)

  def test_temp_dir_gcs(self):
    try:
      self.run_temp_dir_check(
          'gs://aaa/bbb', 'gs://aaa/bbb/', 'gs://aaa', 'gs://aaa/', 'gs://',
          '/')
    except ValueError:
      logging.debug('Ignoring test since GCP module is not installed')

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_temp_dir_local(self, filesystem_os_mock):
    # Here we test a unix-like mock file-system
    # (not really testing Unix or Windows since we mock the function of 'os'
    # module).

    def _fake_unix_split(path):
      sep = path.rfind('/')
      if sep < 0:
        raise ValueError('Path must contain a separator')
      return (path[:sep], path[sep + 1:])

    def _fake_unix_join(base, path):
      return base + '/' + path

    filesystem_os_mock.path.abspath = lambda a: a
    filesystem_os_mock.path.split.side_effect = _fake_unix_split
    filesystem_os_mock.path.join.side_effect = _fake_unix_join
    self.run_temp_dir_check(
        '/aaa/bbb', '/aaa/bbb/', '/', '/', '/', '/')

  def test_file_sink_multi_shards(self):
    temp_path = os.path.join(self._new_tempdir(), 'multishard')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToStringCoder())

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
      shard_name = '%s-%05d-of-%05d.output' % (temp_path, i, num_shards)
      uuid = 'uuid-%05d' % i
      self.assertEqual(res[i], shard_name)
      self.assertEqual(
          open(shard_name).read(), ('[start][a][b][%s][end]' % uuid))

    # Check that any temp files are deleted.
    self.assertItemsEqual(res, glob.glob(temp_path + '*'))

  def test_file_sink_io_error(self):
    temp_path = os.path.join(self._new_tempdir(), 'ioerror')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToStringCoder())

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
