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
import logging
import threading
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Generic
from typing import Hashable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar

from apache_beam.metrics import monitoring_infos

if TYPE_CHECKING:
  from apache_beam.portability.api import metrics_pb2

_LOGGER = logging.getLogger(__name__)

CallableT = TypeVar('CallableT', bound='Callable')
KT = TypeVar('KT')
VT = TypeVar('VT')


class Metrics(object):
  """Metrics container for state cache metrics."""

  # A set of all registered metrics
  ALL_METRICS = set()  # type: Set[Hashable]
  PREFIX = "beam:metric:statecache:"

  def __init__(self):
    # type: () -> None
    self._context = threading.local()

  def initialize(self):
    # type: () -> None

    """Needs to be called once per thread to initialize the local metrics cache.
    """
    if hasattr(self._context, 'metrics'):
      return  # Already initialized
    self._context.metrics = collections.defaultdict(int)

  def count(self, name):
    # type: (str) -> None
    self._context.metrics[name] += 1

  def hit_miss(self, total_name, hit_miss_name):
    # type: (str, str) -> None
    self._context.metrics[total_name] += 1
    self._context.metrics[hit_miss_name] += 1

  def get_monitoring_infos(self, cache_size, cache_capacity):
    # type: (int, int) -> List[metrics_pb2.MonitoringInfo]

    """Returns the metrics scoped to the current bundle."""
    metrics = self._context.metrics
    if len(metrics) == 0:
      # No metrics collected, do not report
      return []
    # Add all missing metrics which were not reported
    for key in Metrics.ALL_METRICS:
      if key not in metrics:
        metrics[key] = 0
    # Gauges which reflect the state since last queried
    gauges = [
        monitoring_infos.int64_gauge(self.PREFIX + name, val) for name,
        val in metrics.items()
    ]
    gauges.append(
        monitoring_infos.int64_gauge(self.PREFIX + 'size', cache_size))
    gauges.append(
        monitoring_infos.int64_gauge(self.PREFIX + 'capacity', cache_capacity))
    # Counters for the summary across all metrics
    counters = [
        monitoring_infos.int64_counter(self.PREFIX + name + '_total', val)
        for name,
        val in metrics.items()
    ]
    # Reinitialize metrics for this thread/bundle
    metrics.clear()
    return gauges + counters

  @staticmethod
  def counter_hit_miss(total_name, hit_name, miss_name):
    # type: (str, str, str) -> Callable[[CallableT], CallableT]

    """Decorator for counting function calls and whether
       the return value equals None (=miss) or not (=hit)."""
    Metrics.ALL_METRICS.update([total_name, hit_name, miss_name])

    def decorator(function):
      # type: (CallableT) -> CallableT
      def reporter(self, *args, **kwargs):
        # type: (StateCache, Any, Any) -> Any
        value = function(self, *args, **kwargs)
        if value is None:
          self._metrics.hit_miss(total_name, miss_name)
        else:
          self._metrics.hit_miss(total_name, hit_name)
        return value

      return reporter  # type: ignore[return-value]

    return decorator

  @staticmethod
  def counter(metric_name):
    # type: (str) -> Callable[[CallableT], CallableT]

    """Decorator for counting function calls."""
    Metrics.ALL_METRICS.add(metric_name)

    def decorator(function):
      # type: (CallableT) -> CallableT
      def reporter(self, *args, **kwargs):
        # type: (StateCache, Any, Any) -> Any
        self._metrics.count(metric_name)
        return function(self, *args, **kwargs)

      return reporter  # type: ignore[return-value]

    return decorator


class StateCache(object):
  """ Cache for Beam state access, scoped by state key and cache_token.
      Assumes a bag state implementation.

  For a given state_key, caches a (cache_token, value) tuple and allows to
    a) read from the cache (get),
           if the currently stored cache_token matches the provided
    a) write to the cache (put),
           storing the new value alongside with a cache token
    c) append to the currently cache item (extend),
           if the currently stored cache_token matches the provided
    c) empty a cached element (clear),
           if the currently stored cache_token matches the provided
    d) evict a cached element (evict)

  The operations on the cache are thread-safe for use by multiple workers.

  :arg max_entries The maximum number of entries to store in the cache.
  TODO Memory-based caching: https://issues.apache.org/jira/browse/BEAM-8297
  """
  def __init__(self, max_entries):
    # type: (int) -> None
    _LOGGER.info('Creating state cache with size %s', max_entries)
    self._missing = None
    self._cache = self.LRUCache[Tuple[bytes, Optional[bytes]],
                                Any](max_entries, self._missing)
    self._lock = threading.RLock()
    self._metrics = Metrics()

  @Metrics.counter_hit_miss("get", "hit", "miss")
  def get(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> Any
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      return self._cache.get((state_key, cache_token))

  @Metrics.counter("put")
  def put(self, state_key, cache_token, value):
    # type: (bytes, Optional[bytes], Any) -> None
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      return self._cache.put((state_key, cache_token), value)

  @Metrics.counter("clear")
  def clear(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> None
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      self._cache.put((state_key, cache_token), [])

  @Metrics.counter("evict")
  def evict(self, state_key, cache_token):
    # type: (bytes, Optional[bytes]) -> None
    assert self.is_cache_enabled()
    with self._lock:
      self._cache.evict((state_key, cache_token))

  def evict_all(self):
    # type: () -> None
    with self._lock:
      self._cache.evict_all()

  def initialize_metrics(self):
    # type: () -> None
    self._metrics.initialize()

  def is_cache_enabled(self):
    # type: () -> bool
    return self._cache._max_entries > 0

  def size(self):
    # type: () -> int
    return len(self._cache)

  def get_monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]

    """Retrieves the monitoring infos and resets the counters."""
    with self._lock:
      size = len(self._cache)
    capacity = self._cache._max_entries
    return self._metrics.get_monitoring_infos(size, capacity)

  class LRUCache(Generic[KT, VT]):
    def __init__(self, max_entries, default_entry):
      # type: (int, VT) -> None
      self._max_entries = max_entries
      self._default_entry = default_entry
      self._cache = collections.OrderedDict(
      )  # type: collections.OrderedDict[KT, VT]

    def get(self, key):
      # type: (KT) -> VT
      value = self._cache.pop(key, self._default_entry)
      if value != self._default_entry:
        self._cache[key] = value
      return value

    def put(self, key, value):
      # type: (KT, VT) -> None
      self._cache[key] = value
      while len(self._cache) > self._max_entries:
        self._cache.popitem(last=False)

    def evict(self, key):
      # type: (KT) -> None
      self._cache.pop(key, self._default_entry)

    def evict_all(self):
      # type: () -> None
      self._cache.clear()

    def __len__(self):
      # type: () -> int
      return len(self._cache)
