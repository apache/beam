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

# pytype: skip-file

import queue
import threading
import weakref
from concurrent.futures import _base


class _WorkItem(object):
  def __init__(self, future, fn, args, kwargs):
    self._future = future
    self._fn = fn
    self._fn_args = args
    self._fn_kwargs = kwargs

  def run(self):
    if self._future.set_running_or_notify_cancel():
      # If the future wasn't cancelled, then attempt to execute it.
      try:
        self._future.set_result(self._fn(*self._fn_args, **self._fn_kwargs))
      except BaseException as exc:
        self._future.set_exception(exc)


class _Worker(threading.Thread):
  def __init__(self, idle_worker_queue, work_item):
    super().__init__()
    self._idle_worker_queue = idle_worker_queue
    self._work_item = work_item
    self._wake_semaphore = threading.Semaphore(0)
    self._lock = threading.Lock()
    self._shutdown = False

  def run(self):
    while True:
      self._work_item.run()
      self._work_item = None

      self._idle_worker_queue.put(self)
      self._wake_semaphore.acquire()
      if self._work_item is None:
        return

  def assign_work(self, work_item):
    """Assigns the work item and wakes up the thread.

    This method must only be called while the worker is idle.
    """
    self._work_item = work_item
    self._wake_semaphore.release()

  def shutdown(self):
    """Wakes up this thread with a 'None' work item signalling to shutdown."""
    self._wake_semaphore.release()


class UnboundedThreadPoolExecutor(_base.Executor):
  def __init__(self):
    self._idle_worker_queue = queue.Queue()
    self._max_idle_threads = 16
    self._workers = weakref.WeakSet()
    self._shutdown = False
    self._lock = threading.Lock()  # Guards access to _workers and _shutdown

  def submit(self, fn, *args, **kwargs):
    """Attempts to submit the work item.

    A runtime error is raised if the pool has been shutdown.
    """
    future = _base.Future()
    work_item = _WorkItem(future, fn, args, kwargs)
    with self._lock:
      if self._shutdown:
        raise RuntimeError(
            'Cannot schedule new tasks after thread pool has been shutdown.')
      try:
        self._idle_worker_queue.get(block=False).assign_work(work_item)

        # If we have more idle threads then the max allowed, shutdown a thread.
        if self._idle_worker_queue.qsize() > self._max_idle_threads:
          try:
            self._idle_worker_queue.get(block=False).shutdown()
          except queue.Empty:
            pass
      except queue.Empty:
        worker = _Worker(self._idle_worker_queue, work_item)
        worker.daemon = True
        worker.start()
        self._workers.add(worker)
    return future

  def shutdown(self, wait=True):
    with self._lock:
      if self._shutdown:
        return

      self._shutdown = True

      for worker in self._workers:
        worker.shutdown()

      if wait:
        for worker in self._workers:
          worker.join()


class _SharedUnboundedThreadPoolExecutor(UnboundedThreadPoolExecutor):
  def shutdown(self, wait=True):
    # Prevent shutting down the shared thread pool
    pass


_SHARED_UNBOUNDED_THREAD_POOL_EXECUTOR = _SharedUnboundedThreadPoolExecutor()


def shared_unbounded_instance():
  return _SHARED_UNBOUNDED_THREAD_POOL_EXECUTOR
