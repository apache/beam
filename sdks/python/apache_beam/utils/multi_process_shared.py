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
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Optional
from typing import TypeVar

import fasteners

# In some python versions, there is a bug where AutoProxy doesn't handle
# the kwarg 'manager_owned'. We implement our own backup here to make sure
# we avoid this problem. More info here:
# https://stackoverflow.com/questions/46779860/multiprocessing-managers-and-custom-classes
autoproxy = multiprocessing.managers.AutoProxy  # type: ignore[attr-defined]


def patched_autoproxy(
    token,
    serializer,
    manager=None,
    authkey=None,
    exposed=None,
    incref=True,
    manager_owned=True):
  return autoproxy(token, serializer, manager, authkey, exposed, incref)


multiprocessing.managers.AutoProxy = patched_autoproxy  # type: ignore[attr-defined]

T = TypeVar('T')
AUTH_KEY = b'mps'


class _SingletonProxy:
  """Proxies the shared object so we can release it with better errors and no
  risk of dangling references in the multiprocessing manager infrastructure.
  """
  def __init__(self, entry):
    # Guard names so as to not conflict with names of underlying object.
    self._SingletonProxy_entry = entry
    self._SingletonProxy_valid = True

  # Used to make the shared object callable (see _AutoProxyWrapper below)
  def singletonProxy_call__(self, *args, **kwargs):
    if not self._SingletonProxy_valid:
      raise RuntimeError('Entry was released.')
    return self._SingletonProxy_entry.obj.__call__(*args, **kwargs)

  def singletonProxy_release(self):
    assert self._SingletonProxy_valid
    self._SingletonProxy_valid = False

  def __getattr__(self, name):
    if not self._SingletonProxy_valid:
      raise RuntimeError('Entry was released.')
    try:
      return getattr(self._SingletonProxy_entry.obj, name)
    except AttributeError as e:
      # Swallow AttributeError exceptions so that they are ignored when
      # calculating public functions. These can occur if __getattr__ is
      # overriden, for example to only support some platforms. This will mean
      # that these functions will be silently unavailable to the
      # MultiProcessShared object, leading to worse errors when someone tries
      # to use them, but it will keep them from breaking the whole object for
      # functions which are unusable anyways.
      logging.info(
          'Attribute %s is unavailable as a public function because '
          'its __getattr__ function raised the following exception '
          '%s',
          name,
          e)
      return None

  def __dir__(self):
    # Needed for multiprocessing.managers's proxying.
    dir = self._SingletonProxy_entry.obj.__dir__()
    dir.append('singletonProxy_call__')
    dir.append('singletonProxy_release')
    return dir


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
    proxy.singletonProxy_release()
    with self.lock:
      self.refcount -= 1
      if self.refcount == 0:
        del self.obj
        self.initialied = False

  def unsafe_hard_delete(self):
    with self.lock:
      if self.initialied:
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

  def unsafe_hard_delete_singleton(self, tag):
    return self.entries[tag].unsafe_hard_delete()


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
_SingletonRegistrar.register(
    'unsafe_hard_delete_singleton',
    callable=_process_level_singleton_manager.unsafe_hard_delete_singleton)


# By default, objects registered with BaseManager.register will have only
# public methods available (excluding __call__). If you know the functions
# you would like to expose, you can do so at register time with the `exposed`
# attribute. Since we don't, we will add a wrapper around the returned AutoProxy
# object to handle __call__ function calls and turn them into
# singletonProxy_call__ calls (which is a wrapper around the underlying
# object's __call__ function)
class _AutoProxyWrapper:
  def __init__(self, proxyObject: multiprocessing.managers.BaseProxy):
    self._proxyObject = proxyObject

  def __call__(self, *args, **kwargs):
    return self._proxyObject.singletonProxy_call__(*args, **kwargs)

  def __getattr__(self, name):
    return getattr(self._proxyObject, name)

  def get_auto_proxy_object(self):
    return self._proxyObject


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
    tag: an indentifier to store with the cached object. If multiple
      MultiProcessShared instances are created with the same tag, they will all
      share the same proxied object.
    path: a temporary path in which to create the inter-process lock
    always_proxy: whether to direct all calls through the proxy, rather than
      call the object directly for the process that created it
  """
  def __init__(
      self,
      constructor: Callable[[], T],
      tag: Any,
      *,
      path: str = tempfile.gettempdir(),
      always_proxy: Optional[bool] = None):
    self._constructor = constructor
    self._tag = tag
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
              # We need to be able to authenticate with both the manager and
              # the process.
              manager = _SingletonRegistrar(
                  address=(host, int(port)), authkey=AUTH_KEY)
              multiprocessing.current_process().authkey = AUTH_KEY
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
    singleton = self._get_manager().acquire_singleton(self._tag)
    return _AutoProxyWrapper(singleton)

  def release(self, obj):
    self._manager.release_singleton(self._tag, obj.get_auto_proxy_object())

  def unsafe_hard_delete(self):
    """Force deletes the underlying object
    
      This function should be used with great care since any other references
      to this object will now be invalid and may lead to strange errors. Only
      call unsafe_hard_delete if either (a) you are sure no other references
      to this object exist, or (b) you are ok with all existing references to
      this object throwing strange errors when derefrenced.
    """
    self._get_manager().unsafe_hard_delete_singleton(self._tag)

  def _create_server(self, address_file):
    # We need to be able to authenticate with both the manager and the process.
    self._serving_manager = _SingletonRegistrar(
        address=('localhost', 0), authkey=AUTH_KEY)
    multiprocessing.current_process().authkey = AUTH_KEY
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
