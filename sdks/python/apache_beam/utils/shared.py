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

"""Shared class.

Shared class for managing a single instance of an object shared by multiple
threads within the same process. Shared is a serializable object that can be
shared by all threads of each worker process, which will be initialized as
necessary by calls to acquire.

Example usage:

To share a very large list across all threads of each worker in a DoFn::

  class GetNthStringFn(beam.DoFn):
    def __init__(self, shared_handle):
      self._shared_handle = shared_handle

    def process(self, element):
      def initialize_list():
        # Build the giant initial list.
        return [str(i) for i in range(1000000)]

      giant_list = self._shared_handle.acquire(initialize_list)
      yield giant_list[element]

  p = beam.Pipeline()
  shared_handle = shared.Shared()
  (p | beam.Create([2, 4, 6, 8])
     | beam.ParDo(GetNthStringFn(shared_handle)))


Real-world uses will typically involve using a side-input to a DoFn to
initialize the shared resource in a way that can't be done with just its
constructor::

  class RainbowTableLookupFn(beam.DoFn):
    def __init__(self, shared_handle):
      self._shared_handle = shared_handle

    def process(self, element, table_elements):
      def construct_table():
        # Construct the rainbow table from the table elements.
        # The table contains lines in the form "string::hash"
        result = dict()
        for key, value in table_elements:
          result[value] = key
        return result

      rainbow_table = self._shared_handle.acquire(construct_table)
      unhashed_str = rainbow_table.get(element)
      if unhashed_str is not None:
        yield unhashed_str

  p = beam.Pipeline()
  shared_handle = shared.Shared()
  reverse_hash_table = p | "ReverseHashTable" >> beam.Create([
                  ('a', '0cc175b9c0f1b6a831c399e269772661'),
                  ('b', '92eb5ffee6ae2fec3ad71c777531578f'),
                  ('c', '4a8a08f09d37b73795649038408b5f33'),
                  ('d', '8277e0910d750195b448797616e091ad')])
  unhashed = (p
              | 'Hashes' >> beam.Create([
                  '0cc175b9c0f1b6a831c399e269772661',
                  '8277e0910d750195b448797616e091ad'])
              | 'Unhash' >> beam.ParDo(
                   RainbowTableLookupFn(shared_handle), reverse_hash_table))

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading
import uuid
import weakref
from typing import Any
from typing import Callable
from typing import Text


class _SharedControlBlock(object):
  """Wrapper class for holding objects in the SharedMap.

  We need this so we can call constructors for distinct Shared elements in the
  SharedMap concurrently.
  """
  def __init__(self):
    self._lock = threading.Lock()
    self._ref = None
    self._tag = None

  def acquire(self, constructor_fn: Callable[[], Any], tag: Any = None) -> Any:
    """Acquire a reference to the object this shared control block manages.

    Args:
      constructor_fn: function that initialises / constructs the object if not
        present in the cache. This function should take no arguments. It should
        return an initialised object, or None if the object could not be
        initialised / constructed.
      tag: an optional indentifier to store with the cached object. If
        subsequent calls to acquire use different tags, the object will be
        reloaded rather than returned from cache.

    Returns:
      An initialised object, either from a previous initialisation, or
      newly-constructed.
    """
    with self._lock:
      # self._ref is None if this is a new control block.
      # self._ref() is None if the weak reference was GCed.
      # self._tag != tag if user specifies a new identifier
      if self._ref is None or self._ref() is None or self._tag != tag:
        result = constructor_fn()
        if result is None:
          return None
        self._ref = weakref.ref(result)
      else:
        result = self._ref()
    return result


class _SharedMap(object):
  """Map for storing objects pointed to by Shared.

  The behaviour of SharedMap is as follows: when acquire is called, if the
  Shared object has already been initialised, we return the already-initialised
  copy. If not, we call the constructor_fn to construct it, and store it in
  the cache.

  One big caveat is this: we want to support cases where there is some delay
  between reacquistion of Shared objects, i.e. there may be a short period of
  time in which there are no references to the object before it is reacquired.

  This happens in various Beam runners (e.g. Dataflow runner): if we use a
  single thread for doing predictions with a large model, when the thread
  finishes its workitem, it will release the reference to the model. Since
  there's only a single thread, the model will have zero references to it
  and will be garbage collected. Shortly after this, the process receives a new
  workitem, creates a new thread, and attempts to reacquire the model. If we
  don't keep the model alive in between, the new thread will have to
  reinitialise the model from scratch.

  As such, we need to do some extra work to manage cached objects' lifetime.
  Ideally we would want to release the shared objects once the stage is
  complete, but we don't have information about that. As such, we work around
  this limitation as follows: when an object is first initialised, we create and
  maintain an explicit reference to it. This means that it will always have one
  reference to it from within _SharedMap.

  When acquire is called for a *different* object, we delete explicit references
  to *all other objects*. This means that if there are no external references to
  these objects, they will be garbage collected.

  This has the following implications:
  *  A shared object won't be GC'ed if there isn't another acquire called for
     a different shared object. This is okay for our use-cases. This means
     that the shared object will be kept alive for all stages fused with the
     stage that works with the shared object. However, all these stages would
     be allocated the same memory anyway, even if the shared object
     were released after the stage that uses it was done with it.
  *  Each stage can only use exactly one Shared token, otherwise only one
     Shared token, *NOT NECESSARILY THE LATEST*, will be "kept-alive" (using
     multiple shared tokens per-stage won't affect correctness, but will have
     no performance benefit either)
  *  If there are two different stages using separate Shared tokens, but which
     get fused together, only one Shared token will be "kept-alive". This
     effectively means that the Shared tokens do nothing: since S2 displaces S1,
     and after S2 executes a new thread is created starting with S1 again, which
     displaces S2.

  Related issues:
    BEAM-562 - DoFn reuse
  """
  def __init__(self):
    # Lock that protects cache_map
    self._lock = threading.Lock()

    # Dictionary of references to shared control blocks
    self._cache_map = dict()

    # Tuple of (key, obj), where obj is an object we explicitly hold a reference
    # to keep it alive
    self._keepalive = (None, None)

  def make_key(self) -> Text:
    return str(uuid.uuid1())

  def acquire(
      self,
      key: Text,
      constructor_fn: Callable[[], Any],
      tag: Any = None) -> Any:
    """Acquire a reference to a Shared object.

    Args:
      key: the key to the shared object
      constructor_fn: function that initialises / constructs the object if not
        present in the cache. This function should take no arguments. It should
        return an initialised object, or None if the object could not be
        initialised / constructed.
      tag: an optional indentifier to store with the cached object. If
        subsequent calls to acquire use different tags, the object will be
        reloaded rather than returned from cache.

    Returns:
      A reference to the initialised object, either from the cache, or
      newly-constructed.
    """
    with self._lock:
      control_block = self._cache_map.get(key)
      if control_block is None:
        control_block = _SharedControlBlock()
        self._cache_map[key] = control_block

    result = control_block.acquire(constructor_fn, tag)

    # Because we release the lock in between, if we acquire multiple Shareds
    # in a short time, there's no guarantee as to which one will be kept alive.
    with self._lock:
      self._keepalive = (key, result)

    return result


# Instance of the shared map to be used with Shared objects.
_shared_map = _SharedMap()


class Shared(object):
  """Handle for managing shared per-process objects.

  Each instance of a Shared object represents a distinct handle to a distinct
  object. Example usage is described in the file comment of shared.py.

  This object has the following limitations:
  *  A shared object won't be GC'ed if there isn't another acquire called for
  a different shared object.
  *  Each stage can only use exactly one Shared token, otherwise only one
  Shared token, *NOT NECESSARILY THE LATEST*, will be "kept-alive".
  *  If there are two different stages using separate Shared tokens, but which
  get fused together, only one Shared token will be "kept-alive".

  (See documentation of _SharedMap for details.)
  """

  # TODO(altay): Consider allowing users to also pass in a key (GUID)
  # for more easily sharing of identifiable expensive objects. User would be
  # responsible for handling collisions.
  def __init__(self):
    self._key = _shared_map.make_key()

  def acquire(self, constructor_fn: Callable[[], Any], tag: Any = None) -> Any:
    """Acquire a reference to the object associated with this Shared handle.

    Args:
      constructor_fn: function that initialises / constructs the object if not
        present in the cache. This function should take no arguments. It should
        return an initialised object, or None if the object could not be
        initialised / constructed.
      tag: an optional indentifier to store with the cached object. If
        subsequent calls to acquire use different tags, the object will be
        reloaded rather than returned from cache.

    Returns:
      A reference to an initialised object, either from the cache, or
      newly-constructed.
    """
    return _shared_map.acquire(self._key, constructor_fn, tag)
