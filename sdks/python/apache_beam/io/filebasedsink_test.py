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

# pytype: skip-file

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
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

_LOGGER = logging.getLogger(__name__)


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

  def _create_temp_file(self, name='', suffix='', dir=None, content=None):
    if not name:
      name = tempfile.template
    if not dir:
      dir = self._new_tempdir()
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name, dir=dir, suffix=suffix).name

    if content:
      with open(file_name, 'w') as f:
        f.write(content)
    return file_name


class MyFileBasedSink(filebasedsink.FileBasedSink):
  def open(self, temp_path):
    # TODO: Fix main session pickling.
    # file_handle = super().open(temp_path)
    file_handle = filebasedsink.FileBasedSink.open(self, temp_path)
    file_handle.write(b'[start]')
    return file_handle

  def write_encoded_record(self, file_handle, encoded_value):
    file_handle.write(b'[')
    file_handle.write(encoded_value)
    file_handle.write(b']')

  def close(self, file_handle):
    file_handle.write(b'[end]')
    # TODO: Fix main session pickling.
    # file_handle = super().close(file_handle)
    file_handle = filebasedsink.FileBasedSink.close(self, file_handle)


class TestFileBasedSink(_TestCaseWithTempDirCleanUp):
  def _common_init(self, sink):
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

    return init_token, [res1, res2]

  def test_file_sink_writing(self):
    temp_path = os.path.join(self._new_tempdir(), 'FileBasedSink')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())

    init_token, writer_results = self._common_init(sink)

    pre_finalize_results = sink.pre_finalize(init_token, writer_results)
    finalize_res1 = list(
        sink.finalize_write(init_token, writer_results, pre_finalize_results))
    # Retry the finalize operation (as if the first attempt was lost).
    finalize_res2 = list(
        sink.finalize_write(init_token, writer_results, pre_finalize_results))

    # Check the results.
    shard1 = temp_path + '-00000-of-00002.output'
    shard2 = temp_path + '-00001-of-00002.output'
    self.assertEqual(finalize_res1, [shard1, shard2])
    self.assertEqual(finalize_res2, [])
    self.assertEqual(open(shard1).read(), '[start][a][b][end]')
    self.assertEqual(open(shard2).read(), '[start][x][y][z][end]')

    # Check that any temp files are deleted.
    self.assertCountEqual([shard1, shard2], glob.glob(temp_path + '*'))

  def test_file_sink_display_data(self):
    temp_path = os.path.join(self._new_tempdir(), 'display')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher(
            'file_pattern',
            '{}{}'.format(
                temp_path, '-%(shard_num)05d-of-%(num_shards)05d.output'))
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_empty_write(self):
    temp_path = tempfile.NamedTemporaryFile().name
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    with TestPipeline() as p:
      p | beam.Create([]) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned
    self.assertEqual(
        open(temp_path + '-00000-of-00001.output').read(), '[start][end]')

  def test_static_value_provider_empty_write(self):
    temp_path = StaticValueProvider(
        value_type=str, value=tempfile.NamedTemporaryFile().name)
    sink = MyFileBasedSink(
        temp_path,
        file_name_suffix=StaticValueProvider(value_type=str, value='.output'),
        coder=coders.ToBytesCoder())
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
        coder=coders.ToBytesCoder())
    with TestPipeline() as p:
      p | beam.Create(['a', 'b']) | beam.io.Write(sink)  # pylint: disable=expression-not-assigned

    concat = ''.join(
        open(temp_path + '_03_%03d_.output' % shard_num).read()
        for shard_num in range(3))
    self.assertTrue('][a][' in concat, concat)
    self.assertTrue('][b][' in concat, concat)

  # Not using 'test' in name so that 'nose' doesn't pick this as a test.
  def run_temp_dir_check(
      self,
      no_dir_path,
      dir_path,
      no_dir_root_path,
      dir_root_path,
      prefix,
      separator):
    def _get_temp_dir(file_path_prefix):
      sink = MyFileBasedSink(
          file_path_prefix,
          file_name_suffix='.output',
          coder=coders.ToBytesCoder())
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

  def test_temp_dir_uniqueness(self):
    temp_path = os.path.join(self._new_tempdir(), 'unique')
    sink = MyFileBasedSink(temp_path, coder=coders.ToBytesCoder())
    init_list = [''] * 1000
    temp_dir_list = [sink._create_temp_dir(temp_path) for _ in init_list]
    temp_dir_set = set(temp_dir_list)
    self.assertEqual(len(temp_dir_list), len(temp_dir_set))

  def test_temp_dir_gcs(self):
    try:
      self.run_temp_dir_check(
          'gs://aaa/bbb',
          'gs://aaa/bbb/',
          'gs://aaa',
          'gs://aaa/',
          'gs://',
          '/')
    except ValueError:
      _LOGGER.debug('Ignoring test since GCP module is not installed')

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
    self.run_temp_dir_check('/aaa/bbb', '/aaa/bbb/', '/', '/', '/', '/')

  def test_file_sink_multi_shards(self):
    temp_path = os.path.join(self._new_tempdir(), 'multishard')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())

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

    pre_finalize_results = sink.pre_finalize(init_token, writer_results)
    res = sorted(
        sink.finalize_write(init_token, writer_results, pre_finalize_results))

    for i in range(num_shards):
      shard_name = '%s-%05d-of-%05d.output' % (temp_path, i, num_shards)
      uuid = 'uuid-%05d' % i
      self.assertEqual(res[i], shard_name)
      self.assertEqual(
          open(shard_name).read(), ('[start][a][b][%s][end]' % uuid))

    # Check that any temp files are deleted.
    self.assertCountEqual(res, glob.glob(temp_path + '*'))

  @mock.patch.object(filebasedsink.FileSystems, 'rename')
  def test_file_sink_rename_error(self, rename_mock):
    temp_path = os.path.join(self._new_tempdir(), 'rename_error')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    init_token, writer_results = self._common_init(sink)
    pre_finalize_results = sink.pre_finalize(init_token, writer_results)

    error_str = 'mock rename error description'
    rename_mock.side_effect = BeamIOError(
        'mock rename error', {('src', 'dst'): error_str})
    with self.assertRaisesRegex(Exception, error_str):
      list(
          sink.finalize_write(init_token, writer_results, pre_finalize_results))

  def test_file_sink_src_missing(self):
    temp_path = os.path.join(self._new_tempdir(), 'src_missing')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    init_token, writer_results = self._common_init(sink)
    pre_finalize_results = sink.pre_finalize(init_token, writer_results)

    os.remove(writer_results[0])
    with self.assertRaisesRegex(Exception, r'not exist'):
      list(
          sink.finalize_write(init_token, writer_results, pre_finalize_results))

  def test_file_sink_dst_matches_src(self):
    temp_path = os.path.join(self._new_tempdir(), 'dst_matches_src')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    init_token, [res1, res2] = self._common_init(sink)

    pre_finalize_results = sink.pre_finalize(init_token, [res1, res2])
    list(sink.finalize_write(init_token, [res1, res2], pre_finalize_results))

    self.assertFalse(os.path.exists(res1))
    self.assertFalse(os.path.exists(res2))
    shard1 = temp_path + '-00000-of-00002.output'
    shard2 = temp_path + '-00001-of-00002.output'
    self.assertEqual(open(shard1).read(), '[start][a][b][end]')
    self.assertEqual(open(shard2).read(), '[start][x][y][z][end]')

    os.makedirs(os.path.dirname(res1))
    shutil.copyfile(shard1, res1)
    shutil.copyfile(shard2, res2)
    list(sink.finalize_write(init_token, [res1, res2], pre_finalize_results))

  def test_pre_finalize(self):
    temp_path = os.path.join(self._new_tempdir(), 'pre_finalize')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    init_token, [res1, res2] = self._common_init(sink)

    # no-op
    sink.pre_finalize(init_token, [res1, res2])

    # Create finalized outputs from a previous run, which pre_finalize should
    # delete.
    shard1 = temp_path + '-00000-of-00002.output'
    shard2 = temp_path + '-00001-of-00002.output'
    with open(shard1, 'w') as f:
      f.write('foo')
    with open(shard2, 'w') as f:
      f.write('foo')
    self.assertTrue(os.path.exists(res1))
    self.assertTrue(os.path.exists(res2))
    self.assertTrue(os.path.exists(shard1))
    self.assertTrue(os.path.exists(shard2))

    sink.pre_finalize(init_token, [res1, res2])
    self.assertTrue(os.path.exists(res1))
    self.assertTrue(os.path.exists(res2))
    self.assertFalse(os.path.exists(shard1))
    self.assertFalse(os.path.exists(shard2))

  @mock.patch.object(filebasedsink.FileSystems, 'delete')
  def test_pre_finalize_error(self, delete_mock):
    temp_path = os.path.join(self._new_tempdir(), 'pre_finalize')
    sink = MyFileBasedSink(
        temp_path, file_name_suffix='.output', coder=coders.ToBytesCoder())
    init_token, [res1, res2] = self._common_init(sink)

    # no-op
    sink.pre_finalize(init_token, [res1, res2])

    # Create finalized outputs from a previous run, which pre_finalize should
    # delete.
    shard1 = temp_path + '-00000-of-00002.output'
    shard2 = temp_path + '-00001-of-00002.output'
    with open(shard1, 'w') as f:
      f.write('foo')
    with open(shard2, 'w') as f:
      f.write('foo')

    error_str = 'mock rename error description'
    delete_mock.side_effect = BeamIOError(
        'mock rename error', {shard2: error_str})
    with self.assertRaisesRegex(Exception, error_str):
      sink.pre_finalize(init_token, [res1, res2])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
