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

import logging
import threading
import unittest
import weakref

from apache_beam.runners.worker.statecache import CacheAware
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.runners.worker.statecache import WeightedValue


class StateCacheTest(unittest.TestCase):
  def test_weakref(self):
    test_value = WeightedValue('test', 10 << 20)

    class WeightedValueRef():
      def __init__(self):
        self.ref = weakref.ref(test_value)

    cache = StateCache(5 << 20)
    wait_event = threading.Event()
    o = WeightedValueRef()
    cache.put('deep ref', 'a', o)
    # Ensure that the contents of the internal weak ref isn't sized
    self.assertIsNotNone(cache.get('deep ref', 'a'))
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 100.00%, lookups 1, evictions 0')
    cache.invalidate_all()

    # Ensure that putting in a weakref doesn't fail regardless of whether
    # it is alive or not
    o_ref = weakref.ref(o, lambda value: wait_event.set())
    cache.put('not deleted ref', 'a', o_ref)
    del o
    wait_event.wait()
    cache.put('deleted', 'a', o_ref)

  def test_weakref_proxy(self):
    test_value = WeightedValue('test', 10 << 20)

    class WeightedValueRef():
      def __init__(self):
        self.ref = weakref.ref(test_value)

    cache = StateCache(5 << 20)
    wait_event = threading.Event()
    o = WeightedValueRef()
    cache.put('deep ref', 'a', o)
    # Ensure that the contents of the internal weak ref isn't sized
    self.assertIsNotNone(cache.get('deep ref', 'a'))
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 100.00%, lookups 1, evictions 0')
    cache.invalidate_all()

    # Ensure that putting in a weakref doesn't fail regardless of whether
    # it is alive or not
    o_ref = weakref.proxy(o, lambda value: wait_event.set())
    cache.put('not deleted', 'a', o_ref)
    del o
    wait_event.wait()
    cache.put('deleted', 'a', o_ref)

  def test_size_of_fails(self):
    class BadSizeOf(object):
      def __sizeof__(self):
        raise RuntimeError("TestRuntimeError")

    cache = StateCache(5 << 20)
    with self.assertLogs('apache_beam.runners.worker.statecache',
                         level='WARNING') as context:
      cache.put('key', 'a', BadSizeOf())
      self.assertEqual(1, len(context.output))
      self.assertTrue('Failed to size' in context.output[0])
      # Test that we don't spam the logs
      cache.put('key', 'a', BadSizeOf())
      self.assertEqual(1, len(context.output))

  def test_empty_cache_get(self):
    cache = StateCache(5 << 20)
    self.assertEqual(cache.get("key", 'cache_token'), None)
    with self.assertRaises(Exception):
      # Invalid cache token provided
      self.assertEqual(cache.get("key", None), None)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 0.00%, lookups 1, evictions 0')

  def test_put_get(self):
    cache = StateCache(5 << 20)
    cache.put("key", "cache_token", WeightedValue("value", 1 << 20))
    self.assertEqual(cache.size(), 1)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key", "cache_token2"), None)
    with self.assertRaises(Exception):
      self.assertEqual(cache.get("key", None), None)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 1/5 MB, hit 50.00%, lookups 2, evictions 0')

  def test_clear(self):
    cache = StateCache(5 << 20)
    cache.clear("new-key", "cache_token")
    cache.put("key", "cache_token", WeightedValue(["value"], 1 << 20))
    self.assertEqual(cache.size(), 2)
    self.assertEqual(cache.get("new-key", "new_token"), None)
    self.assertEqual(cache.get("key", "cache_token"), ['value'])
    # test clear without existing key/token
    cache.clear("non-existing", "token")
    self.assertEqual(cache.size(), 3)
    self.assertEqual(cache.get("non-existing", "token"), [])
    self.assertEqual(
        cache.describe_stats(),
        'used/max 1/5 MB, hit 66.67%, lookups 3, evictions 0')

  def test_default_sized_put(self):
    cache = StateCache(5 << 20)
    cache.put("key", "cache_token", bytearray(1 << 20))
    cache.put("key2", "cache_token", bytearray(1 << 20))
    cache.put("key3", "cache_token", bytearray(1 << 20))
    self.assertEqual(cache.get("key3", "cache_token"), bytearray(1 << 20))
    cache.put("key4", "cache_token", bytearray(1 << 20))
    cache.put("key5", "cache_token", bytearray(1 << 20))
    # note that each byte array instance takes slightly over 1 MB which is why
    # these 5 byte arrays can't all be stored in the cache causing a single
    # eviction
    self.assertEqual(
        cache.describe_stats(),
        'used/max 4/5 MB, hit 100.00%, lookups 1, evictions 1')

  def test_max_size(self):
    cache = StateCache(2 << 20)
    cache.put("key", "cache_token", WeightedValue("value", 1 << 20))
    cache.put("key2", "cache_token", WeightedValue("value2", 1 << 20))
    self.assertEqual(cache.size(), 2)
    cache.put("key3", "cache_token", WeightedValue("value3", 1 << 20))
    self.assertEqual(cache.size(), 2)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 2/2 MB, hit 100.00%, lookups 0, evictions 1')

  def test_invalidate_all(self):
    cache = StateCache(5 << 20)
    cache.put("key", "cache_token", WeightedValue("value", 1 << 20))
    cache.put("key2", "cache_token", WeightedValue("value2", 1 << 20))
    self.assertEqual(cache.size(), 2)
    cache.invalidate_all()
    self.assertEqual(cache.size(), 0)
    self.assertEqual(cache.get("key", "cache_token"), None)
    self.assertEqual(cache.get("key2", "cache_token"), None)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 0.00%, lookups 2, evictions 0')

  def test_lru(self):
    cache = StateCache(5 << 20)
    cache.put("key", "cache_token", WeightedValue("value", 1 << 20))
    cache.put("key2", "cache_token2", WeightedValue("value2", 1 << 20))
    cache.put("key3", "cache_token", WeightedValue("value0", 1 << 20))
    cache.put("key3", "cache_token", WeightedValue("value3", 1 << 20))
    cache.put("key4", "cache_token4", WeightedValue("value4", 1 << 20))
    cache.put("key5", "cache_token", WeightedValue("value0", 1 << 20))
    cache.put("key5", "cache_token", WeightedValue(["value5"], 1 << 20))
    self.assertEqual(cache.size(), 5)
    self.assertEqual(cache.get("key", "cache_token"), "value")
    self.assertEqual(cache.get("key2", "cache_token2"), "value2")
    self.assertEqual(cache.get("key3", "cache_token"), "value3")
    self.assertEqual(cache.get("key4", "cache_token4"), "value4")
    self.assertEqual(cache.get("key5", "cache_token"), ["value5"])
    # insert another key to trigger cache eviction
    cache.put("key6", "cache_token2", WeightedValue("value6", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key")
    self.assertEqual(cache.get("key", "cache_token"), None)
    # trigger a read on "key2"
    cache.get("key2", "cache_token2")
    # insert another key to trigger cache eviction
    cache.put("key7", "cache_token", WeightedValue("value7", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key3")
    self.assertEqual(cache.get("key3", "cache_token"), None)
    # trigger a put on "key2"
    cache.put("key2", "cache_token", WeightedValue("put", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # insert another key to trigger cache eviction
    cache.put("key8", "cache_token", WeightedValue("value8", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key4")
    self.assertEqual(cache.get("key4", "cache_token"), None)
    # make "key5" used by writing to it
    cache.put("key5", "cache_token", WeightedValue("val", 1 << 20))
    # least recently used key should be gone ("key6")
    self.assertEqual(cache.get("key6", "cache_token"), None)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 5/5 MB, hit 60.00%, lookups 10, evictions 5')

  def test_is_cached_enabled(self):
    cache = StateCache(1 << 20)
    self.assertEqual(cache.is_cache_enabled(), True)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/1 MB, hit 100.00%, lookups 0, evictions 0')
    cache = StateCache(0)
    self.assertEqual(cache.is_cache_enabled(), False)
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/0 MB, hit 100.00%, lookups 0, evictions 0')

  def test_get_referents_for_cache(self):
    class GetReferentsForCache(CacheAware):
      def __init__(self):
        self.measure_me = bytearray(1 << 20)
        self.ignore_me = bytearray(2 << 20)

      def get_referents_for_cache(self):
        return [self.measure_me]

    cache = StateCache(5 << 20)
    cache.put("key", "cache_token", GetReferentsForCache())
    self.assertEqual(
        cache.describe_stats(),
        'used/max 1/5 MB, hit 100.00%, lookups 0, evictions 0')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
