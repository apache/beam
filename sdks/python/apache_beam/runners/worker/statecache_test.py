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

"""Tests for state caching."""
from __future__ import absolute_import

import logging
import unittest

from apache_beam.runners.worker.statecache import StateCache


class StateCacheTest(unittest.TestCase):

  def test_empty_cache_get(self):
    cache = StateCache(5)
    self.assertEqual(cache.get("key", 'cache_token'), None)
    with self.assertRaises(Exception):
      self.assertEqual(cache.get("key", None), None)

  def test_put_get(self):
    cache = StateCache(5)
    cache.put("key", "cache_token", "value")
    self.assertEqual(len(cache), 1)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key", "cache_token2"), None)
    with self.assertRaises(Exception):
      self.assertEqual(cache.get("key", None), None)

  def test_overwrite(self):
    cache = StateCache(2)
    cache.put("key", "cache_token", "value")
    cache.put("key", "cache_token2", "value2")
    self.assertEqual(len(cache), 1)
    self.assertEqual(cache.get("key", "cache_token"), None)
    self.assertEqual(cache.get("key", "cache_token2"), "value2")

  def test_extend(self):
    cache = StateCache(3)
    cache.put("key", "cache_token", ['val'])
    # test extend for existing key
    cache.extend("key", "cache_token", ['yet', 'another', 'val'])
    self.assertEqual(len(cache), 1)
    self.assertEqual(cache.get("key", "cache_token"),
                     ['val', 'yet', 'another', 'val'])
    # test extend without existing key
    cache.extend("key2", "cache_token", ['another', 'val'])
    self.assertEqual(len(cache), 2)
    self.assertEqual(cache.get("key2", "cache_token"), ['another', 'val'])
    # test eviction in case the cache token changes
    cache.extend("key2", "new_token", ['new_value'])
    self.assertEqual(cache.get("key2", "new_token"), None)
    self.assertEqual(len(cache), 1)

  def test_clear(self):
    cache = StateCache(5)
    cache.clear("new-key", "cache_token")
    cache.put("key", "cache_token", ["value"])
    self.assertEqual(len(cache), 2)
    self.assertEqual(cache.get("new-key", "new_token"), None)
    self.assertEqual(cache.get("key", "cache_token"), ['value'])
    # test clear without existing key/token
    cache.clear("non-existing", "token")
    self.assertEqual(len(cache), 3)
    self.assertEqual(cache.get("non-existing", "token"), [])
    # test eviction in case the cache token changes
    cache.clear("new-key", "wrong_token")
    self.assertEqual(len(cache), 2)
    self.assertEqual(cache.get("new-key", "cache_token"), None)
    self.assertEqual(cache.get("new-key", "wrong_token"), None)

  def test_max_size(self):
    cache = StateCache(2)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token", "value")
    self.assertEqual(len(cache), 2)
    cache.put("key2", "cache_token", "value")
    self.assertEqual(len(cache), 2)
    cache.put("key", "cache_token", "value")
    self.assertEqual(len(cache), 2)

  def test_evict_all(self):
    cache = StateCache(5)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token", "value2")
    self.assertEqual(len(cache), 2)
    cache.evict_all()
    self.assertEqual(len(cache), 0)
    self.assertEqual(cache.get("key", "cache_token"), None)
    self.assertEqual(cache.get("key2", "cache_token"), None)

  def test_lru(self):
    cache = StateCache(5)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token2", "value2")
    cache.put("key3", "cache_token", "value0")
    cache.put("key3", "cache_token", "value3")
    cache.put("key4", "cache_token4", "value4")
    cache.put("key5", "cache_token", "value0")
    cache.put("key5", "cache_token", ["value5"])
    self.assertEqual(len(cache), 5)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key2", "cache_token2"), "value2")
    self.assertEqual(cache.get("key3", "cache_token"), "value3")
    self.assertEqual(cache.get("key4", "cache_token4"), "value4")
    self.assertEqual(cache.get("key5", "cache_token"), ["value5"])
    # insert another key to trigger cache eviction
    cache.put("key6", "cache_token2", "value7")
    self.assertEqual(len(cache), 5)
    # least recently used key should be gone ("key")
    self.assertEqual(cache.get("key", "cache_token"), None)
    # trigger a read on "key2"
    cache.get("key2", "cache_token")
    # insert another key to trigger cache eviction
    cache.put("key7", "cache_token", "value7")
    self.assertEqual(len(cache), 5)
    # least recently used key should be gone ("key3")
    self.assertEqual(cache.get("key3", "cache_token"), None)
    # trigger a put on "key2"
    cache.put("key2", "cache_token", "put")
    self.assertEqual(len(cache), 5)
    # insert another key to trigger cache eviction
    cache.put("key8", "cache_token", "value8")
    self.assertEqual(len(cache), 5)
    # least recently used key should be gone ("key4")
    self.assertEqual(cache.get("key4", "cache_token"), None)
    # make "key5" used by appending to it
    cache.extend("key5", "cache_token", ["another"])
    # least recently used key should be gone ("key6")
    self.assertEqual(cache.get("key6", "cache_token"), None)

  def test_is_cached_enabled(self):
    cache = StateCache(1)
    self.assertEqual(cache.is_cache_enabled(), True)
    cache = StateCache(0)
    self.assertEqual(cache.is_cache_enabled(), False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
