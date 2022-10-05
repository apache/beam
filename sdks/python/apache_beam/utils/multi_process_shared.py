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
import os
import multiprocessing.managers
import pickle
import tempfile
import threading
import uuid

from typing import Any
from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar

import fasteners

T = TypeVar('T')

_singletons = {}


def _create_singleton(constructor, tag):
  _singletons[tag] = constructor()


def _get_singleton(tag):
  return _singletons[tag]


class _SingletonManager(multiprocessing.managers.BaseManager):
  pass


_SingletonManager.register('get_singleton', callable=_get_singleton)

_process_local_lock = threading.Lock()


class MultiProcessShared(Generic[T]):
  """MultiProcessShared is used to share a single object across processes.

  For example, one could have the class::

    class MyExpensiveObject(object):
      def __init__(self, args):
        [expensive initialization and memory allocation]

      def method(self, arg):
        ...

  One could share a single instance of this class by wrapping it as::

    my_expensive_object = MultiProcessShared(lambda: MyExpensiveObject(...))

  which could then be invoked as::

    my_expensive_object.method(arg)


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

  def __getattr__(self, name):
    return getattr(self._get_proxy(), name)

  def _get_proxy(self):
    if self._proxy is None:
      address_file = os.path.join(self._path, self._tag) + ".address"
      while self._proxy is None:
        with _process_local_lock:
          with self._cross_process_lock:
            if not os.path.exists(address_file):
              self._create_server(address_file)

            if _singletons.get(self._tag) and not self._always_proxy:
              self._proxy = _singletons[self._tag]
            else:
              with open(address_file) as fin:
                address = fin.read()
                logging.info('Connecting to remote proxy at %s', address)
                host, port = address.split(':')
                self._manager = _SingletonManager(address=(host, int(port)))
                try:
                  self._manager.connect()
                  self._proxy = self._manager.get_singleton(self._tag)
                  break
                except ConnectionError:
                  # The server is no longer good, assume it died.
                  os.unlink(address_file)

    return self._proxy

  def _create_server(self, address_file):
    self._serving_manager = _SingletonManager(address=('localhost', 0))
    _create_singleton(self._constructor, self._tag)
    self._server = self._serving_manager.get_server()
    logging.info('Starting proxy server at %s', self._server.address)
    with open(address_file + '.tmp', 'w') as fout:
      fout.write('%s:%d' % self._server.address)
    os.rename(address_file + '.tmp', address_file)
    t = threading.Thread(target=self._server.serve_forever, daemon=True)
    t.start()
