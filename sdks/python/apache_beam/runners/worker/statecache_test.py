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
# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

from apache_beam.metrics.monitoring_infos import LATEST_INT64_TYPE
from apache_beam.metrics.monitoring_infos import SUM_INT64_TYPE
from apache_beam.runners.worker.statecache import StateCache


class StateCacheTest(unittest.TestCase):

  def test_empty_cache_get(self):
    cache = self.get_cache(5)
    self.assertEqual(cache.get("key", 'cache_token'), None)
    with self.assertRaises(Exception):
      # Invalid cache token provided
      self.assertEqual(cache.get("key", None), None)
    self.verify_metrics(cache, {'get': 1, 'put': 0, 'extend': 0,
                                'miss': 1, 'hit': 0, 'clear': 0,
                                'evict': 0,
                                'size': 0, 'capacity': 5})

  def test_put_get(self):
    cache = self.get_cache(5)
    cache.put("key", "cache_token", "value")
    self.assertEqual(cache.size(), 1)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key", "cache_token2"), None)
    with self.assertRaises(Exception):
      self.assertEqual(cache.get("key", None), None)
    self.verify_metrics(cache, {'get': 2, 'put': 1, 'extend': 0,
                                'miss': 1, 'hit': 1, 'clear': 0,
                                'evict': 0,
                                'size': 1, 'capacity': 5})

  def test_overwrite(self):
    cache = self.get_cache(2)
    cache.put("key", "cache_token", "value")
    cache.put("key", "cache_token2", "value2")
    self.assertEqual(cache.size(), 1)
    self.assertEqual(cache.get("key", "cache_token"), None)
    self.assertEqual(cache.get("key", "cache_token2"), "value2")
    self.verify_metrics(cache, {'get': 2, 'put': 2, 'extend': 0,
                                'miss': 1, 'hit': 1, 'clear': 0,
                                'evict': 0,
                                'size': 1, 'capacity': 2})

  def test_extend(self):
    cache = self.get_cache(3)
    cache.put("key", "cache_token", ['val'])
    # test extend for existing key
    cache.extend("key", "cache_token", ['yet', 'another', 'val'])
    self.assertEqual(cache.size(), 1)
    self.assertEqual(cache.get("key", "cache_token"),
                     ['val', 'yet', 'another', 'val'])
    # test extend without existing key
    cache.extend("key2", "cache_token", ['another', 'val'])
    self.assertEqual(cache.size(), 2)
    self.assertEqual(cache.get("key2", "cache_token"), ['another', 'val'])
    # test eviction in case the cache token changes
    cache.extend("key2", "new_token", ['new_value'])
    self.assertEqual(cache.get("key2", "new_token"), None)
    self.assertEqual(cache.size(), 1)
    self.verify_metrics(cache, {'get': 3, 'put': 1, 'extend': 3,
                                'miss': 1, 'hit': 2, 'clear': 0,
                                'evict': 1,
                                'size': 1, 'capacity': 3})

  def test_clear(self):
    cache = self.get_cache(5)
    cache.clear("new-key", "cache_token")
    cache.put("key", "cache_token", ["value"])
    self.assertEqual(cache.size(), 2)
    self.assertEqual(cache.get("new-key", "new_token"), None)
    self.assertEqual(cache.get("key", "cache_token"), ['value'])
    # test clear without existing key/token
    cache.clear("non-existing", "token")
    self.assertEqual(cache.size(), 3)
    self.assertEqual(cache.get("non-existing", "token"), [])
    # test eviction in case the cache token changes
    cache.clear("new-key", "wrong_token")
    self.assertEqual(cache.size(), 2)
    self.assertEqual(cache.get("new-key", "cache_token"), None)
    self.assertEqual(cache.get("new-key", "wrong_token"), None)
    self.verify_metrics(cache, {'get': 5, 'put': 1, 'extend': 0,
                                'miss': 3, 'hit': 2, 'clear': 3,
                                'evict': 1,
                                'size': 2, 'capacity': 5})

  def test_max_size(self):
    cache = self.get_cache(2)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token", "value")
    self.assertEqual(cache.size(), 2)
    cache.put("key2", "cache_token", "value")
    self.assertEqual(cache.size(), 2)
    cache.put("key", "cache_token", "value")
    self.assertEqual(cache.size(), 2)
    self.verify_metrics(cache, {'get': 0, 'put': 4, 'extend': 0,
                                'miss': 0, 'hit': 0, 'clear': 0,
                                'evict': 0,
                                'size': 2, 'capacity': 2})

  def test_evict_all(self):
    cache = self.get_cache(5)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token", "value2")
    self.assertEqual(cache.size(), 2)
    cache.evict_all()
    self.assertEqual(cache.size(), 0)
    self.assertEqual(cache.get("key", "cache_token"), None)
    self.assertEqual(cache.get("key2", "cache_token"), None)
    self.verify_metrics(cache, {'get': 2, 'put': 2, 'extend': 0,
                                'miss': 2, 'hit': 0, 'clear': 0,
                                'evict': 0,
                                'size': 0, 'capacity': 5})

  def test_lru(self):
    cache = self.get_cache(5)
    cache.put("key", "cache_token", "value")
    cache.put("key2", "cache_token2", "value2")
    cache.put("key3", "cache_token", "value0")
    cache.put("key3", "cache_token", "value3")
    cache.put("key4", "cache_token4", "value4")
    cache.put("key5", "cache_token", "value0")
    cache.put("key5", "cache_token", ["value5"])
    self.assertEqual(cache.size(), 5)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key2", "cache_token2"), "value2")
    self.assertEqual(cache.get("key3", "cache_token"), "value3")
    self.assertEqual(cache.get("key4", "cache_token4"), "value4")
    self.assertEqual(cache.get("key5", "cache_token"), ["value5"])
    # insert another key to trigger cache eviction
    cache.put("key6", "cache_token2", "value7")
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key")
    self.assertEqual(cache.get("key", "cache_token"), None)
    # trigger a read on "key2"
    cache.get("key2", "cache_token")
    # insert another key to trigger cache eviction
    cache.put("key7", "cache_token", "value7")
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key3")
    self.assertEqual(cache.get("key3", "cache_token"), None)
    # trigger a put on "key2"
    cache.put("key2", "cache_token", "put")
    self.assertEqual(cache.size(), 5)
    # insert another key to trigger cache eviction
    cache.put("key8", "cache_token", "value8")
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key4")
    self.assertEqual(cache.get("key4", "cache_token"), None)
    # make "key5" used by appending to it
    cache.extend("key5", "cache_token", ["another"])
    # least recently used key should be gone ("key6")
    self.assertEqual(cache.get("key6", "cache_token"), None)
    self.verify_metrics(cache, {'get': 10, 'put': 11, 'extend': 1,
                                'miss': 5, 'hit': 5, 'clear': 0,
                                'evict': 0,
                                'size': 5, 'capacity': 5})

  def test_is_cached_enabled(self):
    cache = self.get_cache(1)
    self.assertEqual(cache.is_cache_enabled(), True)
    self.verify_metrics(cache, {})
    cache = self.get_cache(0)
    self.assertEqual(cache.is_cache_enabled(), False)
    self.verify_metrics(cache, {})

  def verify_metrics(self, cache, expected_metrics):
    infos = cache.get_monitoring_infos()
    # Reconstruct metrics dictionary from monitoring infos
    metrics = {
        info.urn.rsplit(':', 1)[1]: info.metric.counter_data.int64_value
        for info in infos
        if "_total" not in info.urn and info.type == LATEST_INT64_TYPE
    }
    self.assertDictEqual(metrics, expected_metrics)
    # Metrics and total metrics should be identical for a single bundle.
    # The following two gauges are not part of the total metrics:
    try:
      del metrics['capacity']
      del metrics['size']
    except KeyError:
      pass
    total_metrics = {
        info.urn.rsplit(':', 1)[1].rsplit("_total")[0]:
        info.metric.counter_data.int64_value
        for info in infos
        if "_total" in info.urn and info.type == SUM_INT64_TYPE
    }
    self.assertDictEqual(metrics, total_metrics)

  @staticmethod
  def get_cache(size):
    cache = StateCache(size)
    cache.initialize_metrics()
    return cache


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
