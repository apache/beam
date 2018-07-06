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

from __future__ import absolute_import

import filecmp
import logging
import os
import shutil
import tempfile
import unittest

import mock

from apache_beam.io import localfilesystem
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems


def _gen_fake_join(separator):
  """Returns a callable that joins paths with the given separator."""

  def _join(first_path, *paths):
    return separator.join((first_path.rstrip(separator),) + paths)

  return _join


class FileSystemsTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_get_scheme(self):
    self.assertIsNone(FileSystems.get_scheme('/abc/cdf'))
    self.assertIsNone(FileSystems.get_scheme('c:\\abc\cdf'))  # pylint: disable=anomalous-backslash-in-string
    self.assertEqual(FileSystems.get_scheme('gs://abc/cdf'), 'gs')

  def test_get_filesystem(self):
    self.assertTrue(isinstance(FileSystems.get_filesystem('/tmp'),
                               localfilesystem.LocalFileSystem))
    self.assertTrue(isinstance(FileSystems.get_filesystem('c:\\abc\def'),  # pylint: disable=anomalous-backslash-in-string
                               localfilesystem.LocalFileSystem))
    with self.assertRaises(ValueError):
      FileSystems.get_filesystem('error://abc/def')

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_unix_path_join(self, *unused_mocks):
    # Test joining of Unix paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('/')
    self.assertEqual('/tmp/path/to/file',
                     FileSystems.join('/tmp/path', 'to', 'file'))
    self.assertEqual('/tmp/path/to/file',
                     FileSystems.join('/tmp/path', 'to/file'))
    self.assertEqual('/tmp/path/to/file',
                     FileSystems.join('/', 'tmp/path', 'to/file'))
    self.assertEqual('/tmp/path/to/file',
                     FileSystems.join('/tmp/', 'path', 'to/file'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_windows_path_join(self, *unused_mocks):
    # Test joining of Windows paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('\\')
    self.assertEqual(r'C:\tmp\path\to\file',
                     FileSystems.join(r'C:\tmp\path', 'to', 'file'))
    self.assertEqual(r'C:\tmp\path\to\file',
                     FileSystems.join(r'C:\tmp\path', r'to\file'))
    self.assertEqual(r'C:\tmp\path\to\file',
                     FileSystems.join(r'C:\tmp\path\\', 'to', 'file'))

  def test_mkdirs(self):
    path = os.path.join(self.tmpdir, 't1/t2')
    FileSystems.mkdirs(path)
    self.assertTrue(os.path.isdir(path))

  def test_mkdirs_failed(self):
    path = os.path.join(self.tmpdir, 't1/t2')
    FileSystems.mkdirs(path)

    # Check IOError if existing directory is created
    with self.assertRaises(IOError):
      FileSystems.mkdirs(path)

    with self.assertRaises(IOError):
      FileSystems.mkdirs(os.path.join(self.tmpdir, 't1'))

  def test_match_file(self):
    path = os.path.join(self.tmpdir, 'f1')
    open(path, 'a').close()

    # Match files in the temp directory
    result = FileSystems.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [path])

  def test_match_file_empty(self):
    path = os.path.join(self.tmpdir, 'f2')  # Does not exist

    # Match files in the temp directory
    result = FileSystems.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [])

  def test_match_file_exception(self):
    # Match files with None so that it throws an exception
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Unable to get the Filesystem') as error:
      FileSystems.match([None])
    self.assertEqual(list(error.exception.exception_details), [None])

  def test_match_directory(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    open(path1, 'a').close()
    open(path2, 'a').close()

    # Match both the files in the directory
    path = os.path.join(self.tmpdir, '*')
    result = FileSystems.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [path1, path2])

  def test_match_directory(self):
    result = FileSystems.match([self.tmpdir])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [self.tmpdir])

  def test_copy(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')

    FileSystems.copy([path1], [path2])
    self.assertTrue(filecmp.cmp(path1, path2))

  def test_copy_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Copy operation failed') as error:
      FileSystems.copy([path1], [path2])
    self.assertEqual(list(error.exception.exception_details.keys()),
                     [(path1, path2)])

  def test_copy_directory(self):
    path_t1 = os.path.join(self.tmpdir, 't1')
    path_t2 = os.path.join(self.tmpdir, 't2')
    FileSystems.mkdirs(path_t1)
    FileSystems.mkdirs(path_t2)

    path1 = os.path.join(path_t1, 'f1')
    path2 = os.path.join(path_t2, 'f1')
    with open(path1, 'a') as f:
      f.write('Hello')

    FileSystems.copy([path_t1], [path_t2])
    self.assertTrue(filecmp.cmp(path1, path2))

  def test_rename(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')

    FileSystems.rename([path1], [path2])
    self.assertTrue(FileSystems.exists(path2))
    self.assertFalse(FileSystems.exists(path1))

  def test_rename_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Rename operation failed') as error:
      FileSystems.rename([path1], [path2])
    self.assertEqual(list(error.exception.exception_details.keys()),
                     [(path1, path2)])

  def test_rename_directory(self):
    path_t1 = os.path.join(self.tmpdir, 't1')
    path_t2 = os.path.join(self.tmpdir, 't2')
    FileSystems.mkdirs(path_t1)

    path1 = os.path.join(path_t1, 'f1')
    path2 = os.path.join(path_t2, 'f1')
    with open(path1, 'a') as f:
      f.write('Hello')

    FileSystems.rename([path_t1], [path_t2])
    self.assertTrue(FileSystems.exists(path_t2))
    self.assertFalse(FileSystems.exists(path_t1))
    self.assertTrue(FileSystems.exists(path2))
    self.assertFalse(FileSystems.exists(path1))

  def test_exists(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    with open(path1, 'a') as f:
      f.write('Hello')
    self.assertTrue(FileSystems.exists(path1))
    self.assertFalse(FileSystems.exists(path2))

  def test_delete(self):
    path1 = os.path.join(self.tmpdir, 'f1')

    with open(path1, 'a') as f:
      f.write('Hello')

    self.assertTrue(FileSystems.exists(path1))
    FileSystems.delete([path1])
    self.assertFalse(FileSystems.exists(path1))

  def test_delete_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Delete operation failed') as error:
      FileSystems.delete([path1])
    self.assertEqual(list(error.exception.exception_details.keys()), [path1])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
