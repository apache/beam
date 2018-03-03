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

"""Unittest for testing utilities,"""

import logging
import os
import tempfile
import unittest

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing import test_utils as utils


class TestUtilsTest(unittest.TestCase):

  def setUp(self):
    utils.patch_retry(self, utils)
    self.tmpdir = tempfile.mkdtemp()

  def test_delete_files_succeeds(self):
    path = os.path.join(self.tmpdir, 'f1')

    with open(path, 'a') as f:
      f.write('test')

    assert FileSystems.exists(path)
    utils.delete_files([path])
    assert not FileSystems.exists(path)

  def test_delete_files_fails_with_io_error(self):
    path = os.path.join(self.tmpdir, 'f2')

    with self.assertRaises(BeamIOError) as error:
      utils.delete_files([path])
    self.assertTrue(
        error.exception.args[0].startswith('Delete operation failed'))
    self.assertEqual(error.exception.exception_details.keys(), [path])

  def test_delete_files_fails_with_invalid_arg(self):
    with self.assertRaises(RuntimeError):
      utils.delete_files([])

  def test_temp_dir_removes_files(self):
    dir_path = ''
    file_path = ''
    with utils.TempDir() as tempdir:
      dir_path = tempdir.get_path()
      file_path = tempdir.create_temp_file()
      self.assertTrue(os.path.exists(dir_path))
      self.assertTrue(os.path.exists(file_path))

    self.assertFalse(os.path.exists(dir_path))
    self.assertFalse(os.path.exists(file_path))

  def test_temp_file_field_correct(self):
    with utils.TempDir() as tempdir:
      filename = tempdir.create_temp_file(
          suffix='.txt',
          lines=['line1\n', 'line2\n', 'line3\n'])
      self.assertTrue(filename.endswith('.txt'))

      with open(filename, 'rb') as f:
        self.assertEqual(f.readline(), 'line1\n')
        self.assertEqual(f.readline(), 'line2\n')
        self.assertEqual(f.readline(), 'line3\n')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
