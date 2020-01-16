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

from __future__ import absolute_import

import sys
import threading
import weakref
from concurrent.futures import _base

try:  # Python3
  import queue
except Exception:  # Python2
  import Queue as queue  # type: ignore[no-redef]


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
        # Even though Python 2 futures library has #set_exection(),
        # the way it generates the traceback doesn't align with
        # the way in which Python 3 does it so we provide alternative
        # implementations that match our test expectations.
        if sys.version_info.major >= 3:
          self._future.set_exception(exc)
        else:
          e, tb = sys.exc_info()[1:]
          self._future.set_exception_info(e, tb)


class _Worker(threading.Thread):
  def __init__(self, idle_worker_queue, permitted_thread_age_in_seconds,
               work_item):
    super(_Worker, self).__init__()
    self._idle_worker_queue = idle_worker_queue
    self._permitted_thread_age_in_seconds = permitted_thread_age_in_seconds
    self._work_item = work_item
    self._wake_event = threading.Event()
    self._lock = threading.Lock()
    self._shutdown = False

  def run(self):
    while True:
      self._work_item.run()
      self._work_item = None

      # If we are explicitly awake then don't add ourselves back to the
      # idle queue. This occurs in case 3 described below.
      if not self._wake_event.is_set():
        self._idle_worker_queue.put(self)

      self._wake_event.wait(self._permitted_thread_age_in_seconds)
      with self._lock:
        # When we are awoken, we may be in one of three states:
        #  1) _work_item is set and _shutdown is False.
        #     This represents the case when we have accepted work.
        #  2) _work_item is unset and _shutdown is True.
        #     This represents the case where either we timed out before
        #     accepting work or explicitly were shutdown without accepting
        #     any work.
        #  3) _work_item is set and _shutdown is True.
        #     This represents a race where we accepted work and also
        #     were shutdown before the worker thread started processing
        #     that work. In this case we guarantee to process the work
        #     but we don't clear the event ensuring that the next loop
        #     around through to the wait() won't block and we will exit
        #     since _work_item will be unset.

        # We only exit when _work_item is unset to prevent dropping of
        # submitted work.
        if self._work_item is None:
          self._shutdown = True
          return
        if not self._shutdown:
          self._wake_event.clear()

  def accepted_work(self, work_item):
    """Returns True if the work was accepted.

    This method must only be called while the worker is idle.
    """
    with self._lock:
      if self._shutdown:
        return False

      self._work_item = work_item
      self._wake_event.set()
      return True

  def shutdown(self):
    """Marks this thread as shutdown possibly waking it up if it is idle."""
    with self._lock:
      if self._shutdown:
        return
      self._shutdown = True
      self._wake_event.set()


class UnboundedThreadPoolExecutor(_base.Executor):
  def __init__(self, permitted_thread_age_in_seconds=30):
    self._permitted_thread_age_in_seconds = permitted_thread_age_in_seconds
    self._idle_worker_queue = queue.Queue()
    self._workers = weakref.WeakSet()
    self._shutdown = False
    self._lock = threading.Lock() # Guards access to _workers and _shutdown

  def submit(self, fn, *args, **kwargs):
    """Attempts to submit the work item.

    A runtime error is raised if the pool has been shutdown.
    """
    future = _base.Future()
    work_item = _WorkItem(future, fn, args, kwargs)
    try:
      # Keep trying to get an idle worker from the queue until we find one
      # that accepts the work.
      while not self._idle_worker_queue.get(
          block=False).accepted_work(work_item):
        pass
      return future
    except queue.Empty:
      with self._lock:
        if self._shutdown:
          raise RuntimeError('Cannot schedule new tasks after thread pool '
                             'has been shutdown.')

        worker = _Worker(
            self._idle_worker_queue, self._permitted_thread_age_in_seconds,
            work_item)
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
