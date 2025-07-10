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
import contextlib
import filecmp
import logging
import os
import shutil
import tempfile
import unittest

import mock
from parameterized import param
from parameterized import parameterized

from apache_beam.io import localfilesystem
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.pipeline_options import PipelineOptions

# pytype: skip-file


def _gen_fake_join(separator):
  """Returns a callable that joins paths with the given separator."""
  def _join(first_path, *paths):
    return separator.join((first_path.rstrip(separator), ) + paths)

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

  @contextlib.contextmanager
  def tmpdir_as_cwd(self):
    """Context manager that sets the current working directory to a temp dir."""
    old_cwd = os.getcwd()
    os.chdir(self.tmpdir)
    try:
      yield
    finally:
      os.chdir(old_cwd)

  def test_scheme(self):
    self.assertIsNone(self.fs.scheme())
    self.assertIsNone(localfilesystem.LocalFileSystem.scheme())

  def test_create_cwd_file(self):
    with self.tmpdir_as_cwd():
      with self.fs.create("blah.txt") as f:
        f.write(b"blah")
      with open("blah.txt", "rb") as f:
        assert f.read() == b"blah"

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_unix_path_join(self, *unused_mocks):
    # Test joining of Unix paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('/')
    self.assertEqual(
        '/tmp/path/to/file', self.fs.join('/tmp/path', 'to', 'file'))
    self.assertEqual('/tmp/path/to/file', self.fs.join('/tmp/path', 'to/file'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_windows_path_join(self, *unused_mocks):
    # Test joining of Windows paths.
    localfilesystem.os.path.join.side_effect = _gen_fake_join('\\')
    self.assertEqual(
        r'C:\tmp\path\to\file', self.fs.join(r'C:\tmp\path', 'to', 'file'))
    self.assertEqual(
        r'C:\tmp\path\to\file', self.fs.join(r'C:\tmp\path', r'to\file'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_unix_path_split(self, os_mock):
    os_mock.path.abspath.side_effect = lambda a: a
    os_mock.path.split.side_effect = _gen_fake_split('/')
    self.assertEqual(('/tmp/path/to', 'file'),
                     self.fs.split('/tmp/path/to/file'))
    # Actual os.path.split will split following to '/' and 'tmp' when run in
    # Unix.
    self.assertEqual(('', 'tmp'), self.fs.split('/tmp'))

  @mock.patch('apache_beam.io.localfilesystem.os')
  def test_windows_path_split(self, os_mock):
    os_mock.path.abspath = lambda a: a
    os_mock.path.split.side_effect = _gen_fake_split('\\')
    self.assertEqual((r'C:\tmp\path\to', 'file'),
                     self.fs.split(r'C:\tmp\path\to\file'))
    # Actual os.path.split will split following to 'C:\' and 'tmp' when run in
    # Windows.
    self.assertEqual((r'C:', 'tmp'), self.fs.split(r'C:\tmp'))

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
    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed') as error:
      self.fs.match([None])
    self.assertEqual(list(error.exception.exception_details.keys()), [None])

  @parameterized.expand([
      param('*', files=['a', 'b', os.path.join('c', 'x')], expected=['a', 'b']),
      param(
          '**',
          files=['a', os.path.join('b', 'x'), os.path.join('c', 'x')],
          expected=['a', os.path.join('b', 'x'), os.path.join('c', 'x')]),
      param(
          os.path.join('*', '*'),
          files=[
              'a',
              os.path.join('b', 'x'),
              os.path.join('c', 'x'),
              os.path.join('d', 'x', 'y')
          ],
          expected=[os.path.join('b', 'x'), os.path.join('c', 'x')]),
      param(
          os.path.join('**', '*'),
          files=[
              'a',
              os.path.join('b', 'x'),
              os.path.join('c', 'x'),
              os.path.join('d', 'x', 'y')
          ],
          expected=[
              os.path.join('b', 'x'),
              os.path.join('c', 'x'),
              os.path.join('d', 'x', 'y')
          ]),
  ])
  def test_match_glob(self, pattern, files, expected):
    for filename in files:
      full_path = os.path.join(self.tmpdir, filename)
      dirname = os.path.dirname(full_path)
      if not dirname == full_path:
        # Make sure we don't go outside the tmpdir
        assert os.path.commonprefix([self.tmpdir, full_path]) == self.tmpdir
        try:
          self.fs.mkdirs(dirname)
        except IOError:
          # Directory exists
          pass

      open(full_path, 'a').close()  # create empty file

    # Match both the files in the directory
    full_pattern = os.path.join(self.tmpdir, pattern)
    result = self.fs.match([full_pattern])[0]
    files = [os.path.relpath(f.path, self.tmpdir) for f in result.metadata_list]
    self.assertCountEqual(files, expected)

  def test_match_directory(self):
    result = self.fs.match([self.tmpdir])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [self.tmpdir])

  def test_match_directory_contents(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    open(path1, 'a').close()
    open(path2, 'a').close()

    result = self.fs.match([os.path.join(self.tmpdir, '*')])[0]
    files = [f.path for f in result.metadata_list]
    self.assertCountEqual(files, [path1, path2])

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
    with self.assertRaisesRegex(BeamIOError,
                                r'^Copy operation failed') as error:
      self.fs.copy([path1], [path2])
    self.assertEqual(
        list(error.exception.exception_details.keys()), [(path1, path2)])

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
    with self.assertRaisesRegex(BeamIOError,
                                r'^Rename operation failed') as error:
      self.fs.rename([path1], [path2])
    self.assertEqual(
        list(error.exception.exception_details.keys()), [(path1, path2)])

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
    # tests that localfilesystem checksum returns file size
    checksum1 = self.fs.checksum(path1)
    checksum2 = self.fs.checksum(path2)
    self.assertEqual(checksum1, str(5))
    self.assertEqual(checksum2, str(3))
    # tests that fs.checksum and str(fs.size) are consistent
    self.assertEqual(checksum1, str(self.fs.size(path1)))
    self.assertEqual(checksum2, str(self.fs.size(path2)))

  def make_tree(self, path, value, expected_leaf_count=None):
    """Create a file+directory structure from a simple dict-based DSL

    :param path: root path to create directories+files under
    :param value: a specification of what ``path`` should contain: ``None`` to
     make it an empty directory, a string literal to make it a file with those
      contents, and a ``dict`` to make it a non-empty directory and recurse
    :param expected_leaf_count: only be set at the top of a recursive call
     stack; after the whole tree has been created, verify the presence and
     number of all files+directories, as a sanity check
    """
    if value is None:
      # empty directory
      os.makedirs(path)
    elif isinstance(value, str):
      # file with string-literal contents
      dir = os.path.dirname(path)
      if not os.path.exists(dir):
        os.makedirs(dir)
      with open(path, 'a') as f:
        f.write(value)
    elif isinstance(value, dict):
      # recurse to create a subdirectory tree
      for basename, v in value.items():
        self.make_tree(os.path.join(path, basename), v)
    else:
      raise Exception('Unexpected value in tempdir tree: %s' % value)

    if expected_leaf_count is not None:
      self.assertEqual(self.check_tree(path, value), expected_leaf_count)

  def check_tree(self, path, value, expected_leaf_count=None):
    """Verify a directory+file structure according to the rules described in
    ``make_tree``

    :param path: path to check under
    :param value: DSL-representation of expected files+directories under
    ``path``
    :return: number of leaf files/directories that were verified
    """
    actual_leaf_count = None
    if value is None:
      # empty directory
      self.assertTrue(os.path.exists(path), msg=path)
      self.assertEqual(os.listdir(path), [])
      actual_leaf_count = 1
    elif isinstance(value, str):
      # file with string-literal contents
      with open(path, 'r') as f:
        self.assertEqual(f.read(), value, msg=path)

      actual_leaf_count = 1
    elif isinstance(value, dict):
      # recurse to check subdirectory tree
      actual_leaf_count = sum([
          self.check_tree(os.path.join(path, basename), v)
          for basename, v in value.items()
      ])
    else:
      raise Exception('Unexpected value in tempdir tree: %s' % value)

    if expected_leaf_count is not None:
      self.assertEqual(actual_leaf_count, expected_leaf_count)

    return actual_leaf_count

  _test_tree = {
      'path1': '111',
      'path2': {
          '2': '222', 'emptydir': None
      },
      'aaa': {
          'b1': 'b1', 'b2': None, 'bbb': {
              'ccc': {
                  'ddd': 'DDD'
              }
          }, 'c': None
      }
  }

  def test_delete_globs(self):
    dir = os.path.join(self.tmpdir, 'dir')
    self.make_tree(dir, self._test_tree, expected_leaf_count=7)

    self.fs.delete([os.path.join(dir, 'path*'), os.path.join(dir, 'aaa', 'b*')])

    # One empty nested directory is left
    self.check_tree(dir, {'aaa': {'c': None}}, expected_leaf_count=1)

  def test_recursive_delete(self):
    dir = os.path.join(self.tmpdir, 'dir')
    self.make_tree(dir, self._test_tree, expected_leaf_count=7)

    self.fs.delete([dir])

    self.check_tree(self.tmpdir, {'': None}, expected_leaf_count=1)

  def test_delete_glob_errors(self):
    dir = os.path.join(self.tmpdir, 'dir')
    self.make_tree(dir, self._test_tree, expected_leaf_count=7)

    with self.assertRaisesRegex(BeamIOError,
                                r'^Delete operation failed') as error:
      self.fs.delete([
          os.path.join(dir, 'path*'),
          os.path.join(dir, 'aaa', 'b*'),
          os.path.join(dir, 'aaa', 'd*')  # doesn't match anything, will raise
      ])

    self.check_tree(dir, {'aaa': {'c': None}}, expected_leaf_count=1)

    self.assertEqual(
        list(error.exception.exception_details.keys()),
        [os.path.join(dir, 'aaa', 'd*')])

    with self.assertRaisesRegex(BeamIOError,
                                r'^Delete operation failed') as error:
      self.fs.delete([
          os.path.join(dir, 'path*')  # doesn't match anything, will raise
      ])

    self.check_tree(dir, {'aaa': {'c': None}}, expected_leaf_count=1)

    self.assertEqual(
        list(error.exception.exception_details.keys()),
        [os.path.join(dir, 'path*')])

  def test_delete(self):
    path1 = os.path.join(self.tmpdir, 'f1')

    with open(path1, 'a') as f:
      f.write('Hello')

    self.assertTrue(self.fs.exists(path1))
    self.fs.delete([path1])
    self.assertFalse(self.fs.exists(path1))

  def test_delete_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    with self.assertRaisesRegex(BeamIOError,
                                r'^Delete operation failed') as error:
      self.fs.delete([path1])
    self.assertEqual(list(error.exception.exception_details.keys()), [path1])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
