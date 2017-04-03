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

import unittest

import filecmp
import os
import shutil
import tempfile
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.localfilesystem import LocalFileSystem


class LocalFileSystemTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.fs = LocalFileSystem()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

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
    with self.assertRaises(BeamIOError) as error:
      self.fs.match([None])
    self.assertTrue(
        error.exception.message.startswith('Match operation failed'))
    self.assertEqual(error.exception.exception_details.keys(), [None])

  def test_match_directory(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    path2 = os.path.join(self.tmpdir, 'f2')
    open(path1, 'a').close()
    open(path2, 'a').close()

    # Match both the files in the directory
    path = os.path.join(self.tmpdir, '*')
    result = self.fs.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [path1, path2])

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
    with self.assertRaises(BeamIOError) as error:
      self.fs.copy([path1], [path2])
    self.assertTrue(
        error.exception.message.startswith('Copy operation failed'))
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
    with self.assertRaises(BeamIOError) as error:
      self.fs.rename([path1], [path2])
    self.assertTrue(
        error.exception.message.startswith('Rename operation failed'))
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

  def test_delete(self):
    path1 = os.path.join(self.tmpdir, 'f1')

    with open(path1, 'a') as f:
      f.write('Hello')

    self.assertTrue(self.fs.exists(path1))
    self.fs.delete([path1])
    self.assertFalse(self.fs.exists(path1))

  def test_delete_error(self):
    path1 = os.path.join(self.tmpdir, 'f1')
    with self.assertRaises(BeamIOError) as error:
      self.fs.delete([path1])
    self.assertTrue(
        error.exception.message.startswith('Delete operation failed'))
    self.assertEqual(error.exception.exception_details.keys(), [path1])
