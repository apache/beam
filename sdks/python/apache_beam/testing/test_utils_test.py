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
import mock

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

  @mock.patch('time.sleep', return_value=None)
  def test_wait_for_subscriptions_created_fails(self, patched_time_sleep):
    sub1 = mock.MagicMock()
    sub1.exists.return_value = True
    sub2 = mock.MagicMock()
    sub2.exists.return_value = False
    with self.assertRaises(RuntimeError) as error:
      utils.wait_for_subscriptions_created([sub1, sub2], timeout=0.1)
    self.assertTrue(sub1.exists.called)
    self.assertTrue(sub2.exists.called)
    self.assertTrue(error.exception.args[0].startswith('Timeout after'))

  @mock.patch('time.sleep', return_value=None)
  def test_wait_for_topics_created_fails(self, patched_time_sleep):
    topic1 = mock.MagicMock()
    topic1.exists.return_value = True
    topic2 = mock.MagicMock()
    topic2.exists.return_value = False
    with self.assertRaises(RuntimeError) as error:
      utils.wait_for_subscriptions_created([topic1, topic2], timeout=0.1)
    self.assertTrue(topic1.exists.called)
    self.assertTrue(topic2.exists.called)
    self.assertTrue(error.exception.args[0].startswith('Timeout after'))

  @mock.patch('time.sleep', return_value=None)
  def test_wait_for_subscriptions_created_succeeds(self, patched_time_sleep):
    sub1 = mock.MagicMock()
    sub1.exists.return_value = True
    self.assertTrue(
        utils.wait_for_subscriptions_created([sub1], timeout=0.1))

  @mock.patch('time.sleep', return_value=None)
  def test_wait_for_topics_created_succeeds(self, patched_time_sleep):
    topic1 = mock.MagicMock()
    topic1.exists.return_value = True
    self.assertTrue(
        utils.wait_for_subscriptions_created([topic1], timeout=0.1))
    self.assertTrue(topic1.exists.called)

  def test_cleanup_subscriptions(self):
    mock_sub = mock.MagicMock()
    mock_sub.exist.return_value = True
    utils.cleanup_subscription([mock_sub])
    self.assertTrue(mock_sub.delete.called)

  def test_cleanup_topics(self):
    mock_topics = mock.MagicMock()
    mock_topics.exist.return_value = True
    utils.cleanup_subscription([mock_topics])
    self.assertTrue(mock_topics.delete.called)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
