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
import re
import threading
import time
import unittest
import weakref

from hamcrest import assert_that
from hamcrest import contains_string

from apache_beam.runners.worker.statecache import CacheAware
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.runners.worker.statecache import WeightedValue
from apache_beam.runners.worker.statecache import _LoadingValue


class StateCacheTest(unittest.TestCase):
  def test_weakref(self):
    test_value = WeightedValue('test', 10 << 20)

    class WeightedValueRef():
      def __init__(self):
        self.ref = weakref.ref(test_value)

    cache = StateCache(5 << 20)
    wait_event = threading.Event()
    o = WeightedValueRef()
    cache.put('deep ref', o)
    # Ensure that the contents of the internal weak ref isn't sized
    self.assertIsNotNone(cache.peek('deep ref'))
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 100.00%, lookups 1, avg load time 0 ns, loads 0, '
        'evictions 0')
    cache.invalidate_all()

    # Ensure that putting in a weakref doesn't fail regardless of whether
    # it is alive or not
    o_ref = weakref.ref(o, lambda value: wait_event.set())
    cache.put('not deleted ref', o_ref)
    del o
    wait_event.wait()
    cache.put('deleted', o_ref)

  def test_weakref_proxy(self):
    test_value = WeightedValue('test', 10 << 20)

    class WeightedValueRef():
      def __init__(self):
        self.ref = weakref.ref(test_value)

    cache = StateCache(5 << 20)
    wait_event = threading.Event()
    o = WeightedValueRef()
    cache.put('deep ref', o)
    # Ensure that the contents of the internal weak ref isn't sized
    self.assertIsNotNone(cache.peek('deep ref'))
    self.assertEqual(
        cache.describe_stats(),
        'used/max 0/5 MB, hit 100.00%, lookups 1, avg load time 0 ns, loads 0, '
        'evictions 0')
    cache.invalidate_all()

    # Ensure that putting in a weakref doesn't fail regardless of whether
    # it is alive or not
    o_ref = weakref.proxy(o, lambda value: wait_event.set())
    cache.put('not deleted', o_ref)
    del o
    wait_event.wait()
    cache.put('deleted', o_ref)

  def test_size_of_fails(self):
    class BadSizeOf(object):
      def __sizeof__(self):
        raise RuntimeError("TestRuntimeError")

    cache = StateCache(5 << 20)
    with self.assertLogs('apache_beam.runners.worker.statecache',
                         level='WARNING') as context:
      cache.put('key', BadSizeOf())
      self.assertEqual(1, len(context.output))
      self.assertTrue('Failed to size' in context.output[0])
      # Test that we don't spam the logs
      cache.put('key', BadSizeOf())
      self.assertEqual(1, len(context.output))

  def test_empty_cache_peek(self):
    cache = StateCache(5 << 20)
    self.assertEqual(cache.peek("key"), None)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 0/5 MB, hit 0.00%, lookups 1, '
            'avg load time 0 ns, loads 0, evictions 0'))

  def test_put_peek(self):
    cache = StateCache(5 << 20)
    cache.put("key", WeightedValue("value", 1 << 20))
    self.assertEqual(cache.size(), 1)
    self.assertEqual(cache.peek("key"), "value")
    self.assertEqual(cache.peek("key2"), None)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 1/5 MB, hit 50.00%, lookups 2, '
            'avg load time 0 ns, loads 0, evictions 0'))

  def test_default_sized_put(self):
    cache = StateCache(5 << 20)
    cache.put("key", bytearray(1 << 20))
    cache.put("key2", bytearray(1 << 20))
    cache.put("key3", bytearray(1 << 20))
    self.assertEqual(cache.peek("key3"), bytearray(1 << 20))
    cache.put("key4", bytearray(1 << 20))
    cache.put("key5", bytearray(1 << 20))
    # note that each byte array instance takes slightly over 1 MB which is why
    # these 5 byte arrays can't all be stored in the cache causing a single
    # eviction
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 4/5 MB, hit 100.00%, lookups 1, '
            'avg load time 0 ns, loads 0, evictions 1'))

  def test_max_size(self):
    cache = StateCache(2 << 20)
    cache.put("key", WeightedValue("value", 1 << 20))
    cache.put("key2", WeightedValue("value2", 1 << 20))
    self.assertEqual(cache.size(), 2)
    cache.put("key3", WeightedValue("value3", 1 << 20))
    self.assertEqual(cache.size(), 2)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 2/2 MB, hit 100.00%, lookups 0, '
            'avg load time 0 ns, loads 0, evictions 1'))

  def test_invalidate_all(self):
    cache = StateCache(5 << 20)
    cache.put("key", WeightedValue("value", 1 << 20))
    cache.put("key2", WeightedValue("value2", 1 << 20))
    self.assertEqual(cache.size(), 2)
    cache.invalidate_all()
    self.assertEqual(cache.size(), 0)
    self.assertEqual(cache.peek("key"), None)
    self.assertEqual(cache.peek("key2"), None)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 0/5 MB, hit 0.00%, lookups 2, '
            'avg load time 0 ns, loads 0, evictions 0'))

  def test_lru(self):
    cache = StateCache(5 << 20)
    cache.put("key", WeightedValue("value", 1 << 20))
    cache.put("key2", WeightedValue("value2", 1 << 20))
    cache.put("key3", WeightedValue("value0", 1 << 20))
    cache.put("key3", WeightedValue("value3", 1 << 20))
    cache.put("key4", WeightedValue("value4", 1 << 20))
    cache.put("key5", WeightedValue("value0", 1 << 20))
    cache.put("key5", WeightedValue(["value5"], 1 << 20))
    self.assertEqual(cache.size(), 5)
    self.assertEqual(cache.peek("key"), "value")
    self.assertEqual(cache.peek("key2"), "value2")
    self.assertEqual(cache.peek("key3"), "value3")
    self.assertEqual(cache.peek("key4"), "value4")
    self.assertEqual(cache.peek("key5"), ["value5"])
    # insert another key to trigger cache eviction
    cache.put("key6", WeightedValue("value6", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key")
    self.assertEqual(cache.peek("key"), None)
    # trigger a read on "key2"
    cache.peek("key2")
    # insert another key to trigger cache eviction
    cache.put("key7", WeightedValue("value7", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key3")
    self.assertEqual(cache.peek("key3"), None)
    # insert another key to trigger cache eviction
    cache.put("key8", WeightedValue("put", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # insert another key to trigger cache eviction
    cache.put("key9", WeightedValue("value8", 1 << 20))
    self.assertEqual(cache.size(), 5)
    # least recently used key should be gone ("key4")
    self.assertEqual(cache.peek("key4"), None)
    # make "key5" used by writing to it
    cache.put("key5", WeightedValue("val", 1 << 20))
    # least recently used key should be gone ("key6")
    self.assertEqual(cache.peek("key6"), None)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 5/5 MB, hit 60.00%, lookups 10, '
            'avg load time 0 ns, loads 0, evictions 5'))

  def test_get(self):
    def check_key(key):
      self.assertEqual(key, "key")
      time.sleep(0.5)
      return "value"

    def raise_exception(key):
      time.sleep(0.5)
      raise Exception("TestException")

    cache = StateCache(5 << 20)
    self.assertEqual("value", cache.get("key", check_key))
    with cache._lock:
      self.assertFalse(isinstance(cache._cache["key"], _LoadingValue))
    self.assertEqual("value", cache.peek("key"))
    cache.invalidate_all()

    with self.assertRaisesRegex(Exception, "TestException"):
      cache.get("key", raise_exception)
    # The cache should not have the value after the failing load causing
    # check_key to load the value.
    self.assertEqual("value", cache.get("key", check_key))
    with cache._lock:
      self.assertFalse(isinstance(cache._cache["key"], _LoadingValue))
    self.assertEqual("value", cache.peek("key"))

    assert_that(cache.describe_stats(), contains_string(", loads 3,"))
    load_time_ns = re.search(
        ", avg load time (.+) ns,", cache.describe_stats()).group(1)
    # Load time should be larger then the sleep time and less than 2x sleep time
    self.assertGreater(int(load_time_ns), 0.5 * 1_000_000_000)
    self.assertLess(int(load_time_ns), 1_000_000_000)

  def test_concurrent_get_waits(self):
    event = threading.Semaphore(0)
    threads_running = threading.Barrier(3)

    def wait_for_event(key):
      with cache._lock:
        self.assertTrue(isinstance(cache._cache["key"], _LoadingValue))
      event.release()
      return "value"

    cache = StateCache(5 << 20)

    def load_key(output):
      threads_running.wait()
      output["value"] = cache.get("key", wait_for_event)
      output["time"] = time.time_ns()

    t1_output = {}
    t1 = threading.Thread(target=load_key, args=(t1_output, ))
    t1.start()

    t2_output = {}
    t2 = threading.Thread(target=load_key, args=(t2_output, ))
    t2.start()

    # Wait for both threads to start
    threads_running.wait()
    # Record the time and wait for the load to start
    current_time_ns = time.time_ns()
    event.acquire()
    t1.join()
    t2.join()

    # Ensure that only one thread did the loading and not both by checking that
    # the semaphore was only released once
    self.assertFalse(event.acquire(blocking=False))

    # Ensure that the load time is greater than the set time ensuring that
    # both loads had to wait for the event
    self.assertLessEqual(current_time_ns, t1_output["time"])
    self.assertLessEqual(current_time_ns, t2_output["time"])
    self.assertEqual("value", t1_output["value"])
    self.assertEqual("value", t2_output["value"])
    self.assertEqual("value", cache.peek("key"))

  def test_concurrent_get_superseded_by_put(self):
    load_happening = threading.Event()
    finish_loading = threading.Event()

    def wait_for_event(key):
      load_happening.set()
      finish_loading.wait()
      return "value"

    cache = StateCache(5 << 20)

    def load_key(output):
      output["value"] = cache.get("key", wait_for_event)

    t1_output = {}
    t1 = threading.Thread(target=load_key, args=(t1_output, ))
    t1.start()

    # Wait for the load to start, update the key, and then let the load finish
    load_happening.wait()
    cache.put("key", "value2")
    finish_loading.set()
    t1.join()

    # Ensure that the original value is loaded and returned and not the
    # updated value
    self.assertEqual("value", t1_output["value"])
    # Ensure that the updated value supersedes the loaded value.
    self.assertEqual("value2", cache.peek("key"))

  def test_is_cached_enabled(self):
    cache = StateCache(1 << 20)
    self.assertEqual(cache.is_cache_enabled(), True)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 0/1 MB, hit 100.00%, lookups 0, '
            'avg load time 0 ns, loads 0, evictions 0'))
    cache = StateCache(0)
    self.assertEqual(cache.is_cache_enabled(), False)
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 0/0 MB, hit 100.00%, lookups 0, '
            'avg load time 0 ns, loads 0, evictions 0'))

  def test_get_referents_for_cache(self):
    class GetReferentsForCache(CacheAware):
      def __init__(self):
        self.measure_me = bytearray(1 << 20)
        self.ignore_me = bytearray(2 << 20)

      def get_referents_for_cache(self):
        return [self.measure_me]

    cache = StateCache(5 << 20)
    cache.put("key", GetReferentsForCache())
    self.assertEqual(
        cache.describe_stats(),
        (
            'used/max 1/5 MB, hit 100.00%, lookups 0, '
            'avg load time 0 ns, loads 0, evictions 0'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
