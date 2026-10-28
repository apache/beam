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

import gc
import logging
import multiprocessing
import multiprocessing.connection
import os
import tempfile
import threading
import time
import unittest
from typing import Any
from unittest.mock import patch

import pytest

from apache_beam.utils import multi_process_shared


@pytest.fixture(autouse=True)
def isolate_multi_process_shared_tests(tmp_path, monkeypatch):
  """Isolates MultiProcessShared tests by using a unique temp directory per test.

  This prevents tests running in parallel (e.g. with pytest-xdist) from
  interfering with each other by writing to the same shared default temp directory.
  """
  orig_init = multi_process_shared.MultiProcessShared.__init__

  def mock_init(self, constructor, tag, *args, **kwargs):
    if 'path' not in kwargs:
      kwargs['path'] = str(tmp_path)
    return orig_init(self, constructor, tag, *args, **kwargs)

  monkeypatch.setattr(
      multi_process_shared.MultiProcessShared, '__init__', mock_init)


class CallableCounter(object):
  def __init__(self, start=0):
    self.running = start
    self.lock = threading.Lock()

  def __call__(self):
    return self.running

  def increment(self, value=1):
    with self.lock:
      self.running += value
      return self.running

  def error(self, msg):
    raise RuntimeError(msg)


class Counter(object):
  def __init__(self, start=0):
    self.running = start
    self.lock = threading.Lock()

  def get(self):
    return self.running

  def increment(self, value=1):
    with self.lock:
      self.running += value
      return self.running

  def error(self, msg):
    raise RuntimeError(msg)


class CounterWithBadAttr(object):
  def __init__(self, start=0):
    self.running = start
    self.lock = threading.Lock()

  def get(self):
    return self.running

  def increment(self, value=1):
    with self.lock:
      self.running += value
      return self.running

  def error(self, msg):
    raise RuntimeError(msg)

  def __getattribute__(self, __name: str) -> Any:
    if __name == 'error':
      raise AttributeError('error is not actually supported on this platform')
    else:
      # Default behaviour
      return object.__getattribute__(self, __name)


class PidCounter(object):
  def __init__(self, start=0):
    self.running = start
    self.lock = threading.Lock()

  def get_pid(self):
    return os.getpid()

  def get(self):
    return self.running

  def increment(self, value=1):
    with self.lock:
      self.running += value
      return self.running


class SlowFinalizingObject(object):
  def __init__(self, entered_path, finalized_path):
    self._entered_path = entered_path
    self._finalized_path = finalized_path

  def block_call(self):
    with open(self._entered_path, 'w') as f:
      f.write('entered')
    time.sleep(30)
    return 'done'

  def __del__(self):
    with open(self._finalized_path, 'w') as f:
      f.write('finalized')


class MultiProcessSharedTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.shared = multi_process_shared.MultiProcessShared(
        Counter, tag='basic', always_proxy=True).acquire()
    cls.sharedCallable = multi_process_shared.MultiProcessShared(
        CallableCounter, tag='callable', always_proxy=True).acquire()

  def test_call(self):
    self.assertEqual(self.shared.get(), 0)
    self.assertEqual(self.shared.increment(), 1)
    self.assertEqual(self.shared.increment(10), 11)
    self.assertEqual(self.shared.increment(value=10), 21)
    self.assertEqual(self.shared.get(), 21)

  def test_call_illegal_attr(self):
    shared_handle = multi_process_shared.MultiProcessShared(
        CounterWithBadAttr, tag='test_call_illegal_attr', always_proxy=True)
    shared = shared_handle.acquire()

    self.assertEqual(shared.get(), 0)
    self.assertEqual(shared.increment(), 1)
    self.assertEqual(shared.get(), 1)

  def test_call_callable(self):
    self.assertEqual(self.sharedCallable(), 0)
    self.assertEqual(self.sharedCallable.increment(), 1)
    self.assertEqual(self.sharedCallable.increment(10), 11)
    self.assertEqual(self.sharedCallable.increment(value=10), 21)
    self.assertEqual(self.sharedCallable(), 21)

  def test_error(self):
    with self.assertRaisesRegex(Exception, 'something bad'):
      self.shared.error('something bad')

  def test_no_method(self):
    with self.assertRaisesRegex(Exception, 'no_such_method'):
      self.shared.no_such_method()

  def test_connect(self):
    first = multi_process_shared.MultiProcessShared(
        Counter, tag='counter').acquire()
    second = multi_process_shared.MultiProcessShared(
        Counter, tag='counter').acquire()
    self.assertEqual(first.get(), 0)
    self.assertEqual(first.increment(), 1)

    self.assertEqual(second.get(), 1)
    self.assertEqual(second.increment(), 2)

    self.assertEqual(first.get(), 2)
    self.assertEqual(first.increment(), 3)

  def test_release(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_release')
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_release')

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    counter1again = shared1.acquire()
    self.assertEqual(counter1again.increment(), 3)

    shared1.release(counter1)
    shared2.release(counter2)

    with self.assertRaisesRegex(Exception, 'released'):
      counter1.get()
    with self.assertRaisesRegex(Exception, 'released'):
      counter2.get()

    self.assertEqual(counter1again.get(), 3)

    shared1.release(counter1again)

    counter1New = shared1.acquire()
    self.assertEqual(counter1New.get(), 0)

    with self.assertRaisesRegex(Exception, 'released'):
      counter1.get()

  def test_unsafe_hard_delete(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete', always_proxy=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete', always_proxy=True)

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    try:
      multi_process_shared.MultiProcessShared(
          Counter, tag='test_unsafe_hard_delete').unsafe_hard_delete()
    except Exception:
      pass

    with self.assertRaises(Exception):
      counter1.get()
    with self.assertRaises(Exception):
      counter2.get()

    shared3 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete', always_proxy=True)

    counter3 = shared3.acquire()

    self.assertEqual(counter3.increment(), 1)

  def test_unsafe_hard_delete_autoproxywrapper(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter,
        tag='test_unsafe_hard_delete_autoproxywrapper',
        always_proxy=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter,
        tag='test_unsafe_hard_delete_autoproxywrapper',
        always_proxy=True)

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    try:
      counter2.singletonProxy_unsafe_hard_delete()
    except Exception:
      pass

    with self.assertRaises(Exception):
      counter1.get()
    with self.assertRaises(Exception):
      counter2.get()

    counter3 = multi_process_shared.MultiProcessShared(
        Counter,
        tag='test_unsafe_hard_delete_autoproxywrapper',
        always_proxy=True).acquire()
    self.assertEqual(counter3.increment(), 1)

  def test_unsafe_hard_delete_no_op(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete_no_op', always_proxy=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete_no_op', always_proxy=True)

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    try:
      multi_process_shared.MultiProcessShared(
          Counter, tag='no_tag_to_delete').unsafe_hard_delete()
    except Exception:
      pass

    self.assertEqual(counter1.increment(), 3)
    self.assertEqual(counter2.increment(), 4)

  def test_release_always_proxy(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_release_always_proxy', always_proxy=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_release_always_proxy', always_proxy=True)

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    counter1again = shared1.acquire()
    self.assertEqual(counter1again.increment(), 3)

    shared1.release(counter1)
    shared2.release(counter2)

    with self.assertRaisesRegex(Exception, 'released'):
      counter1.get()
    with self.assertRaisesRegex(Exception, 'released'):
      counter2.get()

    self.assertEqual(counter1again.get(), 3)

    shared1.release(counter1again)

    counter1New = shared1.acquire()
    self.assertEqual(counter1New.get(), 0)

    with self.assertRaisesRegex(Exception, 'released'):
      counter1.get()


class MultiProcessSharedSpawnProcessTest(unittest.TestCase):
  def tearDown(self):
    for p in multiprocessing.active_children():
      if p.is_alive():
        try:
          p.kill()
          p.join(timeout=1.0)
        except Exception:
          pass

  def test_call(self):
    shared = multi_process_shared.MultiProcessShared(
        Counter, tag='main', always_proxy=True, spawn_process=True).acquire()
    self.assertEqual(shared.get(), 0)
    self.assertEqual(shared.increment(), 1)
    self.assertEqual(shared.increment(10), 11)
    self.assertEqual(shared.increment(value=10), 21)
    self.assertEqual(shared.get(), 21)

  def test_unsafe_hard_delete_autoproxywrapper(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_delete', always_proxy=True, spawn_process=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_delete', always_proxy=True, spawn_process=True)
    counter3 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_keep', always_proxy=True,
        spawn_process=True).acquire()

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    try:
      counter2.singletonProxy_unsafe_hard_delete()
    except Exception:
      pass

    with self.assertRaises(Exception):
      counter1.get()
    with self.assertRaises(Exception):
      counter2.get()

    counter4 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_delete', always_proxy=True,
        spawn_process=True).acquire()

    self.assertEqual(counter3.increment(), 1)
    self.assertEqual(counter4.increment(), 1)

  def test_mix_usage(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='mix1', always_proxy=True, spawn_process=False).acquire()
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='mix2', always_proxy=True, spawn_process=True).acquire()

    self.assertEqual(shared1.get(), 0)
    self.assertEqual(shared1.increment(), 1)
    self.assertEqual(shared2.get(), 0)
    self.assertEqual(shared2.increment(), 1)

  def test_process_exits_on_unsafe_hard_delete(self):
    shared = multi_process_shared.MultiProcessShared(
        Counter, tag='test_process_exit', always_proxy=True, spawn_process=True)
    obj = shared.acquire()

    self.assertEqual(obj.increment(), 1)

    children = multiprocessing.active_children()
    server_process = None
    for p in children:
      if p.pid != os.getpid() and p.is_alive():
        server_process = p
        break

    self.assertIsNotNone(
        server_process, "Could not find spawned server process")
    try:
      obj.singletonProxy_unsafe_hard_delete()
    except Exception:
      pass
    server_process.join(timeout=5)

    self.assertFalse(
        server_process.is_alive(),
        f"Server process {server_process.pid} is still alive after hard delete")
    self.assertIsNotNone(
        server_process.exitcode, "Process has no exit code (did not exit)")

    with self.assertRaises(Exception):
      obj.get()

  def test_process_exits_on_unsafe_hard_delete_with_manager(self):
    shared = multi_process_shared.MultiProcessShared(
        Counter, tag='test_process_exit', always_proxy=True, spawn_process=True)
    obj = shared.acquire()

    self.assertEqual(obj.increment(), 1)

    children = multiprocessing.active_children()
    server_process = None
    for p in children:
      if p.pid != os.getpid() and p.is_alive():
        server_process = p
        break

    self.assertIsNotNone(
        server_process, "Could not find spawned server process")
    try:
      shared.unsafe_hard_delete()
    except Exception:
      pass
    server_process.join(timeout=5)

    self.assertFalse(
        server_process.is_alive(),
        f"Server process {server_process.pid} is still alive after hard delete")
    self.assertIsNotNone(
        server_process.exitcode, "Process has no exit code (did not exit)")

    with self.assertRaises(Exception):
      obj.get()

  def test_zombie_reaping_on_acquire(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='test_zombie_reap', always_proxy=True, spawn_process=True)
    obj = shared1.acquire()

    children = multiprocessing.active_children()
    server_pid = next(
        p.pid for p in children if p.is_alive() and p.pid != os.getpid())

    try:
      obj.singletonProxy_unsafe_hard_delete()
    except Exception:
      pass

    try:
      os.kill(server_pid, 0)
      is_zombie = True
    except OSError:
      is_zombie = False
    self.assertTrue(
        is_zombie,
        f"Server process {server_pid} was reaped too early before acquire()")

    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='unrelated_tag', always_proxy=True, spawn_process=True)
    _ = shared2.acquire()

    # If reaping worked, our old server_pid should NOT be in this list.
    current_children_pids = [p.pid for p in multiprocessing.active_children()]

    self.assertNotIn(
        server_pid,
        current_children_pids,
        f"Old server process {server_pid} was not reaped by acquire() sweep")
    try:
      shared2.unsafe_hard_delete()
    except Exception:
      pass

  def test_transient_connection_error_recovery(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='transient_test', always_proxy=True, spawn_process=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='transient_test', always_proxy=True, spawn_process=True)

    counter1 = shared1.acquire()

    orig_connect = multi_process_shared._SingletonRegistrar.connect
    connect_calls = [0]

    def side_effect_connect(self_mgr, *args, **kwargs):
      connect_calls[0] += 1
      if connect_calls[0] == 1:
        raise ConnectionError("Simulated transient connection failure")
      return orig_connect(self_mgr, *args, **kwargs)

    with patch.object(multi_process_shared._SingletonRegistrar,
                      'connect',
                      autospec=True,
                      side_effect=side_effect_connect):
      counter2 = shared2.acquire()

    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

  def test_spawn_process_runs_in_dedicated_pid(self):
    parent_pid = os.getpid()
    proxies = [
        multi_process_shared.MultiProcessShared(
            PidCounter,
            tag=f'dedicated_pid_{i}',
            always_proxy=True,
            spawn_process=True).acquire() for i in range(3)
    ]
    pids = [proxy.get_pid() for proxy in proxies]
    self.assertEqual(len(set(pids)), 3)
    for pid in pids:
      self.assertNotEqual(pid, parent_pid)

    same_tag_proxy = multi_process_shared.MultiProcessShared(
        PidCounter,
        tag='dedicated_pid_0',
        always_proxy=True,
        spawn_process=True).acquire()
    self.assertEqual(same_tag_proxy.get_pid(), pids[0])
    self.assertEqual(proxies[0].increment(5), 5)
    self.assertEqual(same_tag_proxy.get(), 5)

  def test_no_cyclic_gc_self_deadlock_issue_40673(self):
    """Regression test for https://github.com/apache/beam/issues/40673.

    When always_proxy=True and spawn_process=False, cyclic GC collecting an
    unreachable BaseProxy on the in-process Server.accepter thread deadlocks
    the server against itself. With spawn_process=True, the server runs in a
    dedicated child process with no BaseProxy references, preventing deadlock.
    """
    orig_accept = multiprocessing.connection.Listener.accept

    def gc_on_accepter_accept(listener_self):
      if 'accepter' in threading.current_thread().name:
        gc.collect()
      return orig_accept(listener_self)

    stop_bg_gc = threading.Event()

    def background_gc():
      while not stop_bg_gc.is_set():
        gc.collect()
        time.sleep(0.005)

    errors = []

    def workload():
      try:
        for i in range(15):
          handle = multi_process_shared.MultiProcessShared(
              Counter,
              tag='issue_40673_tag',
              always_proxy=True,
              spawn_process=True)
          proxy = handle.acquire()
          self.assertEqual(proxy.increment(), i + 1)
          cycle = []
          cycle.append(cycle)
          cycle.append(proxy)
          cycle.append(handle)
          del proxy
          del handle
          del cycle
          if i % 3 == 0:
            gc.collect()
      except BaseException as e:  # pylint: disable=broad-exception-caught
        errors.append(e)

    with patch.object(multiprocessing.connection.Listener,
                      'accept',
                      gc_on_accepter_accept):
      gc_thread = threading.Thread(target=background_gc, daemon=True)
      worker_thread = threading.Thread(target=workload, daemon=True)
      gc_thread.start()
      worker_thread.start()
      worker_thread.join(timeout=30)
      stop_bg_gc.set()
      gc_thread.join(timeout=5)

    self.assertFalse(
        worker_thread.is_alive(),
        'Deadlock detected while acquiring/collecting cyclic BaseProxy '
        'instances with spawn_process=True')
    self.assertEqual(errors, [])

    final_proxy = multi_process_shared.MultiProcessShared(
        Counter, tag='issue_40673_tag', always_proxy=True,
        spawn_process=True).acquire()
    self.assertEqual(final_proxy.get(), 15)

  def test_spawn_process_supports_lambda_and_closure_constructors(self):
    captured_offset = 42

    def local_factory(extra):
      return PidCounter(start=captured_offset + extra)

    proxy = multi_process_shared.MultiProcessShared(
        lambda: local_factory(8),
        tag='closure_constructor',
        always_proxy=True,
        spawn_process=True).acquire()
    self.assertNotEqual(proxy.get_pid(), os.getpid())
    self.assertEqual(proxy.get(), 50)
    self.assertEqual(proxy.increment(5), 55)

  def test_unsafe_hard_delete_missing_tag_is_noop(self):
    handle = multi_process_shared.MultiProcessShared(
        Counter, tag='nonexistent_tag', always_proxy=True, spawn_process=True)
    handle.unsafe_hard_delete()
    self.assertEqual(multiprocessing.active_children(), [])

  def test_unsafe_hard_delete_runs_del_during_inflight_blocked_call(self):
    with tempfile.TemporaryDirectory() as tmp_dir:
      entered_path = os.path.join(tmp_dir, 'entered')
      finalized_path = os.path.join(tmp_dir, 'finalized')
      handle = multi_process_shared.MultiProcessShared(
          lambda: SlowFinalizingObject(entered_path, finalized_path),
          tag='inflight_hard_delete',
          always_proxy=True,
          spawn_process=True)
      proxy = handle.acquire()
      server_proc = next(
          p for p in multiprocessing.active_children() if p.is_alive())

      def call_ignoring_disconnect():
        try:
          proxy.block_call()
        except (EOFError, OSError):
          pass

      call_thread = threading.Thread(
          target=call_ignoring_disconnect, daemon=True)
      call_thread.start()

      deadline = time.time() + 5
      while not os.path.exists(entered_path) and time.time() < deadline:
        time.sleep(0.02)
      self.assertTrue(os.path.exists(entered_path))

      handle.unsafe_hard_delete()
      server_proc.join(timeout=5)
      self.assertFalse(server_proc.is_alive())
      self.assertTrue(os.path.exists(finalized_path))
      with open(finalized_path) as f:
        self.assertEqual(f.read(), 'finalized')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
