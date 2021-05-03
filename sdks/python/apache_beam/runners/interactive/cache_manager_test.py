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

# pytype: skip-file

import os
import shutil
import tempfile
import time
import unittest

from apache_beam import coders
from apache_beam.io import filesystems
from apache_beam.runners.interactive import cache_manager as cache


class FileBasedCacheManagerTest(object):
  """Unit test for FileBasedCacheManager.

  Note that this set of tests focuses only the methods that interacts with
  the LOCAL file system. The idea is that once FileBasedCacheManager works well
  with the local file system, it should work with any file system with
  `apache_beam.io.filesystem` interface. Those tests that involve interactions
  with Beam pipeline (i.e. source(), sink(), ReadCache, and WriteCache) will be
  tested with InteractiveRunner as a part of integration tests instead.
  """

  cache_format = None  # type: str

  def setUp(self):
    self.test_dir = tempfile.mkdtemp()
    self.cache_manager = cache.FileBasedCacheManager(
        self.test_dir, cache_format=self.cache_format)

  def tearDown(self):
    # The test_dir might have already been removed by cache_manager.cleanup().
    if os.path.exists(self.test_dir):
      shutil.rmtree(self.test_dir)

  def mock_write_cache(self, pcoll_list, prefix, cache_label):
    """Cache the PCollection where cache.WriteCache would write to."""
    cache_path = filesystems.FileSystems.join(
        self.cache_manager._cache_dir, prefix)
    if not filesystems.FileSystems.exists(cache_path):
      filesystems.FileSystems.mkdirs(cache_path)

    # Pause for 0.1 sec, because the Jenkins test runs so fast that the file
    # writes happen at the same timestamp.
    time.sleep(0.1)

    cache_file = cache_label + '-1-of-2'
    labels = [prefix, cache_label]

    # Usually, the pcoder will be inferred from `pcoll.element_type`
    pcoder = coders.registry.get_coder(object)
    # Save a pcoder for reading.
    self.cache_manager.save_pcoder(pcoder, *labels)
    # Save a pcoder for the fake write to the file.
    self.cache_manager.save_pcoder(pcoder, prefix, cache_file)
    self.cache_manager.write(pcoll_list, prefix, cache_file)

  def test_exists(self):
    """Test that CacheManager can correctly tell if the cache exists or not."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    self.assertFalse(self.cache_manager.exists(prefix, cache_label))
    self.mock_write_cache(cache_version_one, prefix, cache_label)
    self.assertTrue(self.cache_manager.exists(prefix, cache_label))
    self.cache_manager.cleanup()
    self.assertFalse(self.cache_manager.exists(prefix, cache_label))
    self.mock_write_cache(cache_version_one, prefix, cache_label)
    self.assertTrue(self.cache_manager.exists(prefix, cache_label))

  def test_size(self):
    """Test getting the size of some cache label."""

    # The Beam API for writing doesn't return the number of bytes that was
    # written to disk. So this test is only possible when the coder encodes the
    # bytes that will be written directly to disk, which only the WriteToText
    # transform does (with respect to the WriteToTFRecord transform).
    if self.cache_manager.cache_format != 'text':
      return

    prefix = 'full'
    cache_label = 'some-cache-label'

    # Test that if nothing is written the size is 0.
    self.assertEqual(self.cache_manager.size(prefix, cache_label), 0)

    value = 'a'
    self.mock_write_cache([value], prefix, cache_label)
    coder = self.cache_manager.load_pcoder(prefix, cache_label)
    encoded = coder.encode(value)

    # Add one to the size on disk because of the extra new-line character when
    # writing to file.
    self.assertEqual(
        self.cache_manager.size(prefix, cache_label), len(encoded) + 1)

  def test_clear(self):
    """Test that CacheManager can correctly tell if the cache exists or not."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    self.assertFalse(self.cache_manager.exists(prefix, cache_label))
    self.mock_write_cache(cache_version_one, prefix, cache_label)
    self.assertTrue(self.cache_manager.exists(prefix, cache_label))
    self.assertTrue(self.cache_manager.clear(prefix, cache_label))
    self.assertFalse(self.cache_manager.exists(prefix, cache_label))

  def test_read_basic(self):
    """Test the condition where the cache is read once after written once."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    self.mock_write_cache(cache_version_one, prefix, cache_label)
    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)
    self.assertListEqual(pcoll_list, cache_version_one)
    self.assertEqual(version, 0)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

  def test_read_version_update(self):
    """Tests if the version is properly updated after the files are updated."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']
    cache_version_two = ['cache', 'version', 'two']

    self.mock_write_cache(cache_version_one, prefix, cache_label)
    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)

    self.mock_write_cache(cache_version_two, prefix, cache_label)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)
    self.assertListEqual(pcoll_list, cache_version_two)
    self.assertEqual(version, 1)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

  def test_read_before_write(self):
    """Test the behavior when read() is called before WriteCache completes."""
    prefix = 'full'
    cache_label = 'some-cache-label'

    self.assertFalse(self.cache_manager.exists(prefix, cache_label))

    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)
    self.assertListEqual(pcoll_list, [])
    self.assertEqual(version, -1)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

  def test_read_over_cleanup(self):
    """Test the behavior of read() over cache cleanup."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']
    cache_version_two = ['cache', 'version', 'two']

    # The initial write and read.
    self.mock_write_cache(cache_version_one, prefix, cache_label)
    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)

    # Cache cleanup.
    self.cache_manager.cleanup()
    # Check that even if cache is evicted, the latest version stays the same.
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)
    self.assertListEqual(pcoll_list, [])
    self.assertEqual(version, -1)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    # PCollection brought back to cache.
    self.mock_write_cache(cache_version_two, prefix, cache_label)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    reader, version = self.cache_manager.read(prefix, cache_label)
    pcoll_list = list(reader)
    self.assertListEqual(pcoll_list, cache_version_two)
    # Check that version continues from the previous value instead of starting
    # from 0 again.
    self.assertEqual(version, 1)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))


class TextFileBasedCacheManagerTest(
    FileBasedCacheManagerTest,
    unittest.TestCase,
):

  cache_format = 'text'


class TFRecordBasedCacheManagerTest(
    FileBasedCacheManagerTest,
    unittest.TestCase,
):

  cache_format = 'tfrecord'


if __name__ == '__main__':
  unittest.main()
