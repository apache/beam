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

"""A module for caching state reads/writes in Beam applications."""
from __future__ import absolute_import

import collections
import logging
from threading import Lock


class StateCache(object):
  """ Cache for Beam state access, scoped by state key and cache_token.

  For a given state_key, caches a (cache_token, value) tuple and allows to
    a) read from the cache (get),
           if the currently stored cache_token matches the provided
    a) write to the cache (put),
           storing the new value alongside with a cache token
    c) append to the cache (extend),
           if the currently stored cache_token matches the provided

  The operations on the cache are thread-safe for use by multiple workers.

  :arg max_entries The maximum number of entries to store in the cache.
  TODO Memory-based caching: https://issues.apache.org/jira/browse/BEAM-8297
  """

  def __init__(self, max_entries):
    logging.info('Creating state cache with size %s', max_entries)
    self._cache = self.LRUCache(max_entries, (None, None))
    self._lock = Lock()

  def get(self, state_key, cache_token):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, value = self._cache.get(state_key)
    return value if token == cache_token else None

  def put(self, state_key, cache_token, value):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      return self._cache.put(state_key, (cache_token, value))

  def extend(self, state_key, cache_token, elements):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, value = self._cache.get(state_key)
      if token in [cache_token, None]:
        if value is None:
          value = []
        value.extend(elements)
        self._cache.put(state_key, (cache_token, value))
      else:
        # Discard cached state if tokens do not match
        self._cache.evict(state_key)

  def clear(self, state_key, cache_token):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, _ = self._cache.get(state_key)
      if token in [cache_token, None]:
        self._cache.put(state_key, (cache_token, []))
      else:
        # Discard cached state if tokens do not match
        self._cache.evict(state_key)

  def evict(self, state_key):
    assert self.is_cache_enabled()
    with self._lock:
      self._cache.evict(state_key)

  def evict_all(self):
    with self._lock:
      self._cache.evict_all()

  def is_cache_enabled(self):
    return self._cache._max_entries > 0

  def __len__(self):
    return len(self._cache)

  class LRUCache(object):

    def __init__(self, max_entries, default_entry):
      self._max_entries = max_entries
      self._default_entry = default_entry
      self._cache = collections.OrderedDict()

    def get(self, key):
      value = self._cache.pop(key, self._default_entry)
      if value != self._default_entry:
        self._cache[key] = value
      return value

    def put(self, key, value):
      self._cache[key] = value
      while len(self._cache) > self._max_entries:
        self._cache.popitem(last=False)

    def evict(self, key):
      self._cache.pop(key, self._default_entry)

    def evict_all(self):
      self._cache.clear()

    def __len__(self):
      return len(self._cache)
