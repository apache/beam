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
import sys
import tempfile
import time
import unittest

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive import cache_manager as cache


class CacheManagerTest(object):
  """Unit test for CacheManager.

  Note that this set of tests focuses only the methods that interacts with
  the LOCAL file system. The idea is that once CacheManager works well
  with the local file system, it should work with any file system with
  `apache_beam.io.filesystem` interface. Those tests that involve interactions
  with Beam pipeline (i.e. source(), sink(), ReadCache, and WriteCache) will be
  tested with InteractiveRunner as a part of integration tests instead.
  """

  cache_format = None

  @classmethod
  def setUpClass(cls):
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    self._test_dir = tempfile.mkdtemp()
    options = PipelineOptions(temp_location=self._test_dir)
    self.cache_manager = cache.CacheManager(options)

  def tearDown(self):
    # The test_dir might have already been removed by cache_manager.cleanup().
    if os.path.exists(self._test_dir):
      shutil.rmtree(self._test_dir)

  def mock_write_cache(self, pcoll_list, cache_name):
    """Cache the PCollection where cache.WriteCache would write to."""
    cache = self.cache_manager.get_or_create_cache(
        cache_name=cache_name, cache_class=self.cache_format)
    cache.write(pcoll_list)

  def test_exists(self):
    """Test that CacheManager can correctly tell if the cache exists or not."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    cache_name = self.cache_manager.generate_label(cache_label, prefix)

    self.assertFalse(cache_name in self.cache_manager)
    self.mock_write_cache(cache_version_one, cache_name)
    self.assertTrue(cache_name in self.cache_manager)
    self.cache_manager.cleanup()
    self.assertFalse(cache_name in self.cache_manager)
    self.mock_write_cache(cache_version_one, cache_name)
    self.assertTrue(cache_name in self.cache_manager)

  def test_read_basic(self):
    """Test the condition where the cache is read once after written once."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']

    cache_name = self.cache_manager.generate_label(cache_label, prefix)

    self.mock_write_cache(cache_version_one, cache_name)
    pcoll_list = list(self.cache_manager[cache_name].read())
    self.assertCountEqual(pcoll_list, cache_version_one)

  def test_read_version_update(self):
    """Tests if the version is properly updated after the files are updated."""
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']
    cache_version_two = ['cache', 'version', 'two']

    cache_name = self.cache_manager.generate_label(cache_label, prefix)

    self.mock_write_cache(cache_version_one, cache_name)
    cache1 = self.cache_manager[cache_name]
    cache1_data = list(cache1.read())
    cache1_timestamp = cache1.timestamp
    self.assertCountEqual(cache1_data, cache_version_one)

    # NOTE: cache.timestamp does not have perfect temporal resolution.
    # If this is important, The implementation may need to change.
    time.sleep(0.01)

    self.mock_write_cache(cache_version_two, cache_name)
    cache2 = self.cache_manager[cache_name]
    cache2_data = list(cache2.read())
    self.assertCountEqual(cache2_data, cache_version_one + cache_version_two)
    cache2_timestamp = cache2.timestamp
    self.assertGreater(cache2_timestamp, cache1_timestamp)

    cache3 = self.cache_manager[cache_name]
    self.assertEqual(cache3.timestamp, cache2_timestamp)

  def test_read_before_write(self):
    prefix = 'full'
    cache_label = 'some-cache-label'

    cache_name = self.cache_manager.generate_label(cache_label, prefix)

    self.assertFalse(cache_name in self.cache_manager)
    with self.assertRaises(KeyError):
      _ = list(self.cache_manager[cache_name].read())

  def test_read_over_cleanup(self):
    prefix = 'full'
    cache_label = 'some-cache-label'
    cache_version_one = ['cache', 'version', 'one']
    cache_version_two = ['cache', 'version', 'two']

    cache_name = self.cache_manager.generate_label(cache_label, prefix)

    self.mock_write_cache(cache_version_one, cache_name)
    pcoll_list = list(self.cache_manager[cache_name].read())
    self.assertCountEqual(pcoll_list, cache_version_one)

    self.cache_manager.cleanup()

    with self.assertRaises(KeyError):
      _ = list(self.cache_manager[cache_name].read())

    # PCollection brought back to cache.
    self.mock_write_cache(cache_version_two, cache_name)
    pcoll_list = list(self.cache_manager[cache_name].read())
    self.assertCountEqual(pcoll_list, cache_version_two)


class TextFileBasedCacheManagerTest(
    CacheManagerTest,
    unittest.TestCase,
):

  cache_format = 'TextBasedCache'


class TFRecordBasedCacheManagerTest(
    CacheManagerTest,
    unittest.TestCase,
):

  cache_format = 'TFRecordBasedCache'


if __name__ == '__main__':
  unittest.main()
