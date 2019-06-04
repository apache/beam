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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import tempfile
import time
import unittest

from apache_beam.io import filesystems
from apache_beam.runners.interactive import cache_manager as cache


class FileBasedCacheManagerTest(unittest.TestCase):
  """Unit test for FileBasedCacheManager.

  Note that this set of tests focuses only the the methods that interacts with
  the LOCAL file system. The idea is that once FileBasedCacheManager works well
  with the local file system, it should work with any file system with
  `apache_beam.io.filesystem` interface. Those tests that involve interactions
  with Beam pipeline (i.e. source(), sink(), ReadCache, and WriteCache) will be
  tested with InteractiveRunner as a part of integration tests instead.
  """

  def setUp(self):
    self.test_dir = tempfile.mkdtemp()
    self.cache_manager = cache.FileBasedCacheManager(self.test_dir)

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
    with open(self.cache_manager._path(prefix, cache_file), 'wb') as f:
      for line in pcoll_list:
        f.write(cache.SafeFastPrimitivesCoder().encode(line))
        f.write(b'\n')

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

  def test_read_basic(self):
    """Test the condition where the cache is read once after written once."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    self.mock_write_cache(cache_version_one, prefix, cache_label)
    pcoll_list, version = self.cache_manager.read(prefix, cache_label)
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
    pcoll_list, version = self.cache_manager.read(prefix, cache_label)

    self.mock_write_cache(cache_version_two, prefix, cache_label)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    pcoll_list, version = self.cache_manager.read(prefix, cache_label)
    self.assertListEqual(pcoll_list, cache_version_two)
    self.assertEqual(version, 1)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

  def test_read_before_write(self):
    """Test the behavior when read() is called before WriteCache completes."""
    prefix = 'full'
    cache_label = 'some-cache-label'

    self.assertFalse(self.cache_manager.exists(prefix, cache_label))

    pcoll_list, version = self.cache_manager.read(prefix, cache_label)
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
    pcoll_list, version = self.cache_manager.read(prefix, cache_label)

    # Cache cleanup.
    self.cache_manager.cleanup()
    # Check that even if cache is evicted, the latest version stays the same.
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    pcoll_list, version = self.cache_manager.read(prefix, cache_label)
    self.assertListEqual(pcoll_list, [])
    self.assertEqual(version, -1)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    # PCollection brought back to cache.
    self.mock_write_cache(cache_version_two, prefix, cache_label)
    self.assertFalse(
        self.cache_manager.is_latest_version(version, prefix, cache_label))

    pcoll_list, version = self.cache_manager.read(prefix, cache_label)
    self.assertListEqual(pcoll_list, cache_version_two)
    # Check that version continues from the previous value instead of starting
    # from 0 again.
    self.assertEqual(version, 1)
    self.assertTrue(
        self.cache_manager.is_latest_version(version, prefix, cache_label))


if __name__ == '__main__':
  unittest.main()
