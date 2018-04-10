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

"""Unit tests for LocalFileSystem."""

import filecmp
import logging
import os
import shutil
import tempfile
import unittest

import mock

from apache_beam.io import localfilesystem
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.pipeline_options import PipelineOptions


def _gen_fake_join(separator):
  """Returns a callable that joins paths with the given separator."""

  def _join(first_path, *paths):
    return separator.join((first_path.rstrip(separator),) + paths)

  return _join


def _gen_fake_split(separator):
  """Returns a callable that splits a with the given separator."""

  def _split(path):
    sep_index = path.rfind(separator)
    if sep_index >= 0:
      return (path[:sep_index], path[sep_index + 1:])
    else:
      return (path, '')

  return _split


class LocalFileSystemTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    pipeline_options = PipelineOptions()
    self.fs = localfilesystem.LocalFileSystem(pipeline_options)

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_scheme(self):
    self.assertIsNone(self.fs.scheme())
    self.assertIsNone(localfilesystem.LocalFileSystem.scheme())

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_unix_path_join(self, *unused_mocks):
    # Test joining of Unix paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('/')
    self.assertEqual('/tmp/path/to/file',
                     self.fs.join('/tmp/path', 'to', 'file'))
    self.assertEqual('/tmp/path/to/file',
                     self.fs.join('/tmp/path', 'to/file'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_windows_path_join(self, *unused_mocks):
    # Test joining of Windows paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('\\')
    self.assertEqual(r'C:\tmp\path\to\file',
                     self.fs.join(r'C:\tmp\path', 'to', 'file'))
    self.assertEqual(r'C:\tmp\path\to\file',
                     self.fs.join(r'C:\tmp\path', r'to\file'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_unix_path_split(self, os_mock):
    os_mock.path.abspath.side_effect = lambda a: a
    os_mock.path.split.side_effect = _gen_fake_split('/')
    self.assertEqual(('/tmp/path/to', 'file'),
                     self.fs.split('/tmp/path/to/file'))
    # Actual os.path.split will split following to '/' and 'tmp' when run in
    # Unix.
    self.assertEqual(('', 'tmp'),
                     self.fs.split('/tmp'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_windows_path_split(self, os_mock):
    os_mock.path.abspath = lambda a: a
    os_mock.path.split.side_effect = _gen_fake_split('\\')
    self.assertEqual((r'C:\tmp\path\to', 'file'),
                     self.fs.split(r'C:\tmp\path\to\file'))
    # Actual os.path.split will split following to 'C:\' and 'tmp' when run in
    # Windows.
    self.assertEqual((r'C:', 'tmp'),
                     self.fs.split(r'C:\tmp'))

  def test_mkdirs(self):
    path = os.path.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(path)
    self.assertTrue(os.path.isdir(path))

  def test_mkdirs_failed(self):
    path = os.path.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(path)

    # Check IOError if existing directory is created
    with self.assertRaises(IOError):
      self.fs.mkdirs(path)

    with self.assertRaises(IOError):
      self.fs.mkdirs(os.path.join(self.tmpdir, 't1'))

  def test_match_file(self):
    path = os.path.join(self.tmpdir, 'f1')
    open(path, 'a').close()

    # Match files in the temp directory
    result = self.fs.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [path])

  def test_match_file_empty(self):
    path = os.path.join(self.tmpdir, 'f2')  # Does not exist

    # Match files in the temp directory
    result = self.fs.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [])

  def test_match_file_exception(self):
    # Match files with None so that it throws an exception
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Match operation failed') as error:
      self.fs.match([None])
    self.assertEqual(error.exception.exception_details.keys(), [None])

  def test_match_glob(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    open(path1, 'a').close()
    open(path2, 'a').close()

    # Match both the files in the directory
    path = os.path.join(self.tmpdir, '*')
    result = self.fs.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertItemsEqual(files, [path1, path2])

  def test_match_directory(self):
    result = self.fs.match([self.tmpdir])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [self.tmpdir])

  def test_copy(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')

    self.fs.copy([path1], [path2])
    self.assertTrue(filecmp.cmp(path1, path2))

  def test_copy_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Copy operation failed') as error:
      self.fs.copy([path1], [path2])
    self.assertEqual(error.exception.exception_details.keys(), [(path1, path2)])

  def test_copy_directory(self):
    path_t1 = os.path.join(self.tmpdir, 't1')
    path_t2 = os.path.join(self.tmpdir, 't2')
    self.fs.mkdirs(path_t1)
    self.fs.mkdirs(path_t2)

    path1 = os.path.join(path_t1, 'f1')
    path2 = os.path.join(path_t2, 'f1')
    with open(path1, 'a') as f:
      f.write('Hello')

    self.fs.copy([path_t1], [path_t2])
    self.assertTrue(filecmp.cmp(path1, path2))

  def test_rename(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')

    self.fs.rename([path1], [path2])
    self.assertTrue(self.fs.exists(path2))
    self.assertFalse(self.fs.exists(path1))

  def test_rename_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Rename operation failed') as error:
      self.fs.rename([path1], [path2])
    self.assertEqual(error.exception.exception_details.keys(), [(path1, path2)])

  def test_rename_directory(self):
    path_t1 = os.path.join(self.tmpdir, 't1')
    path_t2 = os.path.join(self.tmpdir, 't2')
    self.fs.mkdirs(path_t1)

    path1 = os.path.join(path_t1, 'f1')
    path2 = os.path.join(path_t2, 'f1')
    with open(path1, 'a') as f:
      f.write('Hello')

    self.fs.rename([path_t1], [path_t2])
    self.assertTrue(self.fs.exists(path_t2))
    self.assertFalse(self.fs.exists(path_t1))
    self.assertTrue(self.fs.exists(path2))
    self.assertFalse(self.fs.exists(path1))

  def test_exists(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')
    self.assertTrue(self.fs.exists(path1))
    self.assertFalse(self.fs.exists(path2))

  def test_checksum(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')
    with open(path2, 'a') as f:
      f.write('foo')
    self.assertEquals(self.fs.checksum(path1), str(5))
    self.assertEquals(self.fs.checksum(path2), str(3))

  def test_delete(self):
    path1 = os.path.join(self.tmpdir, 'f1')

    with open(path1, 'a') as f:
      f.write('Hello')

    self.assertTrue(self.fs.exists(path1))
    self.fs.delete([path1])
    self.assertFalse(self.fs.exists(path1))

  def test_delete_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Delete operation failed') as error:
      self.fs.delete([path1])
    self.assertEqual(error.exception.exception_details.keys(), [path1])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
