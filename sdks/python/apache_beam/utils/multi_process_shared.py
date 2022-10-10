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

"""Implements a shared object that spans processes.

This object will be instanciated once per VM and methods will be invoked
on it via rpc.
"""
# pytype: skip-file

import logging
import multiprocessing.managers
import os
import tempfile
import threading
import uuid
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Optional
from typing import TypeVar

import fasteners

T = TypeVar('T')


class _SingletonProxy:
  """Proxies the shared object so we can release it with better errors and no
  risk of dangling references in the multiprocessing manager infrastructure.
  """
  def __init__(self, entry):
    # Guard names so as to not conflict with names of underlying object.
    self._SingletonProxy_entry = entry
    self._SingletonProxy_valid = True

  def _SingletonProxy_release(self):
    assert self._SingletonProxy_valid
    self._SingletonProxy_valid = False

  def __getattr__(self, name):
    if not self._SingletonProxy_valid:
      raise RuntimeError('Entry was released.')
    return getattr(self._SingletonProxy_entry.obj, name)

  def __dir__(self):
    # Needed for multiprocessing.managers's proxying.
    return self._SingletonProxy_entry.obj.__dir__()


class _SingletonEntry:
  """Represents a single, refcounted entry in this process."""
  def __init__(self, constructor, initialize_eagerly=True):
    self.constructor = constructor
    self.refcount = 0
    self.lock = threading.Lock()
    if initialize_eagerly:
      self.obj = constructor()
      self.initialied = True
    else:
      self.initialied = False

  def acquire(self):
    with self.lock:
      if not self.initialied:
        self.obj = self.constructor()
        self.initialied = True
      self.refcount += 1
      return _SingletonProxy(self)

  def release(self, proxy):
    proxy._SingletonProxy_release()
    with self.lock:
      self.refcount -= 1
      if self.refcount == 0:
        del self.obj
        self.initialied = False


class _SingletonManager:
  entries: Dict[Any, Any] = {}

  def register_singleton(self, constructor, tag, initialize_eagerly=True):
    assert tag not in self.entries, tag
    self.entries[tag] = _SingletonEntry(constructor, initialize_eagerly)

  def has_singleton(self, tag):
    return tag in self.entries

  def acquire_singleton(self, tag):
    return self.entries[tag].acquire()

  def release_singleton(self, tag, obj):
    return self.entries[tag].release(obj)


_process_level_singleton_manager = _SingletonManager()

_process_local_lock = threading.Lock()


class _SingletonRegistrar(multiprocessing.managers.BaseManager):
  pass


_SingletonRegistrar.register(
    'acquire_singleton',
    callable=_process_level_singleton_manager.acquire_singleton)
_SingletonRegistrar.register(
    'release_singleton',
    callable=_process_level_singleton_manager.release_singleton)


class MultiProcessShared(Generic[T]):
  """MultiProcessShared is used to share a single object across processes.

  For example, one could have the class::

    class MyExpensiveObject(object):
      def __init__(self, args):
        [expensive initialization and memory allocation]

      def method(self, arg):
        ...

  One could share a single instance of this class by wrapping it as::

    shared_ptr = MultiProcessShared(lambda: MyExpensiveObject(...))
    my_expensive_object = shared_ptr.acquire()

  which could then be invoked as::

    my_expensive_object.method(arg)

  This can then be released with::

    shared_ptr.release(my_expensive_object)

  but care should be taken to avoid releasing the object too soon or
  expensive re-initialization may be required, defeating the point of
  using a shared object.


  Args:
    constructor: function that initialises / constructs the object if not
      present in the cache. This function should take no arguments. It should
      return an initialised object, or raise an exception if the object could
      not be initialised / constructed.
    tag: an optional indentifier to store with the cached object. If multiple
      MultiProcessShared instances are created with the same tag, they will all
      share the same proxied object.
    path: a temporary path in which to create the inter-process lock
    always_proxy: whether to direct all calls through the proxy, rather than
      call the object directly for the process that created it
  """
  def __init__(
      self,
      constructor: Callable[[], T],
      tag: Optional[Any] = None,
      *,
      path: str = tempfile.gettempdir(),
      always_proxy: Optional[bool] = None):
    self._constructor = constructor
    self._tag = tag or uuid.uuid4().hex
    self._path = path
    self._always_proxy = False if always_proxy is None else always_proxy
    self._proxy = None
    self._manager = None
    self._rpc_address = None
    self._cross_process_lock = fasteners.InterProcessLock(
        os.path.join(self._path, self._tag) + '.lock')

  def _get_manager(self):
    if self._manager is None:
      address_file = os.path.join(self._path, self._tag) + ".address"
      while self._manager is None:
        with _process_local_lock:
          with self._cross_process_lock:
            if not os.path.exists(address_file):
              self._create_server(address_file)

            if _process_level_singleton_manager.has_singleton(
                self._tag) and not self._always_proxy:
              self._manager = _process_level_singleton_manager
            else:
              with open(address_file) as fin:
                address = fin.read()
              logging.info('Connecting to remote proxy at %s', address)
              host, port = address.split(':')
              manager = _SingletonRegistrar(address=(host, int(port)))
              try:
                manager.connect()
                self._manager = manager
              except ConnectionError:
                # The server is no longer good, assume it died.
                os.unlink(address_file)

    return self._manager

  def acquire(self):
    # TODO: Allow passing/parameterizing the callable here, in case they are
    # not available at MultiProcessShared construction time (e.g. from side
    # inputs)
    # Caveat: They must always agree, as they will be ignored if the object
    # is already constructed.
    return self._get_manager().acquire_singleton(self._tag)

  def release(self, obj):
    self._manager.release_singleton(self._tag, obj)

  def _create_server(self, address_file):
    self._serving_manager = _SingletonRegistrar(address=('localhost', 0))
    # Initialize eagerly to avoid acting as the server if there are issues.
    # Note, however, that _create_server itself is called lazily.
    _process_level_singleton_manager.register_singleton(
        self._constructor, self._tag, initialize_eagerly=True)
    self._server = self._serving_manager.get_server()
    logging.info(
        'Starting proxy server at %s for shared %s',
        self._server.address,
        self._tag)
    with open(address_file + '.tmp', 'w') as fout:
      fout.write('%s:%d' % self._server.address)
    os.rename(address_file + '.tmp', address_file)
    t = threading.Thread(target=self._server.serve_forever, daemon=True)
    t.start()
    logging.info('Done starting server')
