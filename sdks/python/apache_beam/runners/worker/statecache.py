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
# pytype: skip-file
# mypy: disallow-untyped-defs

import collections
import gc
import logging
import sys
import threading
import time
import types
import weakref
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import objsize

_LOGGER = logging.getLogger(__name__)
_DEFAULT_WEIGHT = 8
_TYPES_TO_NOT_MEASURE = (
    # Do not measure shared types
    type,
    types.ModuleType,
    types.FrameType,
    types.BuiltinFunctionType,
    # Do not measure lambdas as they typically share lots of state
    types.FunctionType,
    types.LambdaType,
    # Do not measure weak references as they will be deleted and not counted
    *weakref.ProxyTypes,
    weakref.ReferenceType)


class WeightedValue(object):
  """Value type that stores corresponding weight.

  :arg value The value to be stored.
  :arg weight The associated weight of the value. If unspecified, the objects
  size will be used.
  """
  def __init__(self, value, weight):
    # type: (Any, int) -> None
    self._value = value
    if weight <= 0:
      raise ValueError(
          'Expected weight to be > 0 for %s but received %d' % (value, weight))
    self._weight = weight

  def weight(self):
    # type: () -> int
    return self._weight

  def value(self):
    # type: () -> Any
    return self._value


class CacheAware(object):
  def __init__(self):
    # type: () -> None
    pass

  def get_referents_for_cache(self):
    # type: () -> List[Any]

    """Returns the list of objects accounted during cache measurement."""
    raise NotImplementedError()


def _safe_isinstance(obj, type):
  # type: (Any, Union[type, Tuple[type, ...]]) -> bool

  """
  Return whether an object is an instance of a class or of a subclass thereof.
  See `isinstance()` for more information.

  Returns false on `isinstance()` failure. For example applying `isinstance()`
  on `weakref.proxy` objects attempts to dereference the proxy objects, which
  may yield an exception. See https://github.com/apache/beam/issues/23389 for
  additional details.
  """
  try:
    return isinstance(obj, type)
  except Exception:
    return False


def _size_func(obj):
  # type: (Any) -> int

  """
  Returns the size of the object or a default size if an error occurred during
  sizing.
  """
  try:
    return sys.getsizeof(obj)
  except Exception as e:
    current_time = time.time()
    # Limit outputting this log so we don't spam the logs on these
    # occurrences.
    if _size_func.last_log_time + 300 < current_time:  # type: ignore
      _LOGGER.warning(
          'Failed to size %s of type %s. Note that this may '
          'impact cache sizing such that the cache is over '
          'utilized which may lead to out of memory errors.',
          obj,
          type(obj),
          exc_info=e)
      _size_func.last_log_time = current_time  # type: ignore
    # Use an arbitrary default size that would account for some of the object
    # overhead.
    return _DEFAULT_WEIGHT


_size_func.last_log_time = 0  # type: ignore


def _get_referents_func(*objs):
  # type: (List[Any]) -> List[Any]

  """Returns the list of objects accounted during cache measurement.

  Users can inherit CacheAware to override which referents should be
  used when measuring the deep size of the object. The default is to
  use gc.get_referents(*objs).
  """
  rval = []
  for obj in objs:
    if _safe_isinstance(obj, CacheAware):
      rval.extend(obj.get_referents_for_cache())  # type: ignore
    else:
      rval.extend(gc.get_referents(obj))
  return rval


def _filter_func(o):
  # type: (Any) -> bool

  """
  Filter out specific types from being measured.

  Note that we do want to measure the cost of weak references as they will only
  stay in scope as long as other code references them and will effectively be
  garbage collected as soon as there isn't a strong reference anymore.

  Note that we cannot use the default filter function due to isinstance raising
  an error on weakref.proxy types. See
  https://github.com/liran-funaro/objsize/issues/6 for additional details.
  """
  return not _safe_isinstance(o, _TYPES_TO_NOT_MEASURE)


def get_deep_size(*objs):
  # type: (Any) -> int

  """Calculates the deep size of all the arguments in bytes."""
  return objsize.get_deep_size(
      objs,
      get_size_func=_size_func,
      get_referents_func=_get_referents_func,
      filter_func=_filter_func)


class StateCache(object):
  """Cache for Beam state access, scoped by state key and cache_token.
     Assumes a bag state implementation.

  For a given state_key and cache_token, caches a value and allows to
    a) read from the cache (get),
           if the currently stored cache_token matches the provided
    b) write to the cache (put),
           storing the new value alongside with a cache token
    c) empty a cached element (clear),
           if the currently stored cache_token matches the provided
    d) invalidate a cached element (invalidate)
    e) invalidate all cached elements (invalidate_all)

  The operations on the cache are thread-safe for use by multiple workers.

  :arg max_weight The maximum weight of entries to store in the cache in bytes.
  """
  def __init__(self, max_weight):
    # type: (int) -> None
    _LOGGER.info('Creating state cache with size %s', max_weight)
    self._max_weight = max_weight
    self._current_weight = 0
    self._cache = collections.OrderedDict(
    )  # type: collections.OrderedDict[Tuple[bytes, Optional[bytes]], WeightedValue]
    self._hit_count = 0
    self._miss_count = 0
    self._evict_count = 0
    self._lock = threading.RLock()

  def get(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> Any
    assert cache_token and self.is_cache_enabled()
    key = (state_key, cache_token)
    with self._lock:
      value = self._cache.get(key, None)
      if value is None:
        self._miss_count += 1
        return None
      self._cache.move_to_end(key)
      self._hit_count += 1
      return value.value()

  def put(self, state_key, cache_token, value):
    # type: (bytes, Optional[bytes], Any) -> None
    assert cache_token and self.is_cache_enabled()
    if not _safe_isinstance(value, WeightedValue):
      weight = get_deep_size(value)
      if weight <= 0:
        _LOGGER.warning(
            'Expected object size to be >= 0 for %s but received %d.',
            value,
            weight)
        weight = _DEFAULT_WEIGHT
      value = WeightedValue(value, weight)
    key = (state_key, cache_token)
    with self._lock:
      old_value = self._cache.pop(key, None)
      if old_value is not None:
        self._current_weight -= old_value.weight()
      self._cache[(state_key, cache_token)] = value
      self._current_weight += value.weight()
      while self._current_weight > self._max_weight:
        (_, weighted_value) = self._cache.popitem(last=False)
        self._current_weight -= weighted_value.weight()
        self._evict_count += 1

  def clear(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> None
    self.put(state_key, cache_token, [])

  def invalidate(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> None
    assert self.is_cache_enabled()
    with self._lock:
      weighted_value = self._cache.pop((state_key, cache_token), None)
      if weighted_value is not None:
        self._current_weight -= weighted_value.weight()

  def invalidate_all(self):
    # type: () -> None
    with self._lock:
      self._cache.clear()
      self._current_weight = 0

  def describe_stats(self):
    # type: () -> str
    with self._lock:
      request_count = self._hit_count + self._miss_count
      if request_count > 0:
        hit_ratio = 100.0 * self._hit_count / request_count
      else:
        hit_ratio = 100.0
      return 'used/max %d/%d MB, hit %.2f%%, lookups %d, evictions %d' % (
          self._current_weight >> 20,
          self._max_weight >> 20,
          hit_ratio,
          request_count,
          self._evict_count)

  def is_cache_enabled(self):
    # type: () -> bool
    return self._max_weight > 0

  def size(self):
    # type: () -> int
    with self._lock:
      return len(self._cache)
