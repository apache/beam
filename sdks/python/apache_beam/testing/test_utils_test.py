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

# pytype: skip-file

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
    self.assertEqual(list(error.exception.exception_details.keys()), [path])

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
          suffix='.txt', lines=[b'line1\n', b'line2\n', b'line3\n'])
      self.assertTrue(filename.endswith('.txt'))

      with open(filename, 'rb') as f:
        self.assertEqual(f.readline(), b'line1\n')
        self.assertEqual(f.readline(), b'line2\n')
        self.assertEqual(f.readline(), b'line3\n')

  def test_cleanup_subscriptions(self):
    sub_client = mock.Mock()
    sub = mock.Mock()
    sub.name = 'test_sub'
    utils.cleanup_subscriptions(sub_client, [sub])
    sub_client.delete_subscription.assert_called_with(subscription=sub.name)

  def test_cleanup_topics(self):
    pub_client = mock.Mock()
    topic = mock.Mock()
    topic.name = 'test_topic'
    utils.cleanup_topics(pub_client, [topic])
    pub_client.delete_topic.assert_called_with(topic=topic.name)


class LCGeneratorTest(unittest.TestCase):
  Generator = utils.LCGenerator

  def test_generator_seed_results(self):
    generator = self.Generator()
    generator.seed(0)
    self.assertEqual(generator.next_int(), 4232237)

    generator.seed(1)
    self.assertEqual(generator.next_int(), -1151252339)
    self.assertEqual(generator.next_uint(), 3745583449)
    self.assertAlmostEqual(generator.random_sample(), 0.375548, delta=1e-6)
    self.assertEqual(generator.randbytes(10), b'\xa6\x8fW\xcb\xb1\xa88]dP')

  def test_generator_seed_jdk_results(self):
    generator = self.Generator()
    generator.seed_jdk(0)
    self.assertEqual(generator.next_int(), -1155484576)

    generator.seed_jdk(1)
    # the first next_int after seed_jdk(1) is close to seed_jdk(0)
    self.assertEqual(generator.next_int(), -1155869325)
    self.assertEqual(generator.next_uint(), 431529176)
    self.assertAlmostEqual(generator.random_sample(), 0.410081, delta=1e-6)
    self.assertEqual(generator.randbytes(10), b'\x92\xf9Mh\xfc\xcc,5\xf0\xb8')


try:
  # pylint: disable=wrong-import-position
  from .fast_test_utils import LCGenerator as FastLCGenerator

  class FastLCGeneratorTest(LCGeneratorTest):
    Generator = FastLCGenerator

except ImportError:
  pass

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
