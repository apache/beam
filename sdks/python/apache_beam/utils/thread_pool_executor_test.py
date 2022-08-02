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

"""Unit tests for UnboundedThreadPoolExecutor."""

# pytype: skip-file

import itertools
import threading
import time
import traceback
import unittest

from apache_beam.utils import thread_pool_executor
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor


class UnboundedThreadPoolExecutorTest(unittest.TestCase):
  def setUp(self):
    self._lock = threading.Lock()
    self._worker_idents = []

  def append_and_sleep(self, sleep_time):
    with self._lock:
      self._worker_idents.append(threading.current_thread().ident)
    time.sleep(sleep_time)

  def raise_error(self, message):
    raise ValueError(message)

  def test_shutdown_with_no_workers(self):
    with UnboundedThreadPoolExecutor():
      pass

  def test_shutdown_with_fast_workers(self):
    futures = []
    with UnboundedThreadPoolExecutor() as executor:
      for _ in range(0, 5):
        futures.append(executor.submit(self.append_and_sleep, 0.01))

    for future in futures:
      future.result(timeout=10)

    with self._lock:
      self.assertEqual(5, len(self._worker_idents))

  def test_shutdown_with_slow_workers(self):
    futures = []
    with UnboundedThreadPoolExecutor() as executor:
      for _ in range(0, 5):
        futures.append(executor.submit(self.append_and_sleep, 1))

    for future in futures:
      future.result(timeout=10)

    with self._lock:
      self.assertEqual(5, len(self._worker_idents))

  def test_worker_reuse(self):
    futures = []
    with UnboundedThreadPoolExecutor() as executor:
      for _ in range(0, 5):
        futures.append(executor.submit(self.append_and_sleep, 0.01))
      time.sleep(3)
      for _ in range(0, 5):
        futures.append(executor.submit(self.append_and_sleep, 0.01))

    for future in futures:
      future.result(timeout=10)

    with self._lock:
      self.assertEqual(10, len(self._worker_idents))
      self.assertTrue(len(set(self._worker_idents)) < 10)

  def test_exception_propagation(self):
    with UnboundedThreadPoolExecutor() as executor:
      future = executor.submit(self.raise_error, 'footest')

    try:
      future.result()
    except Exception:
      message = traceback.format_exc()
    else:
      raise AssertionError('expected exception not raised')

    self.assertIn('footest', message)
    self.assertIn('raise_error', message)

  def test_map(self):
    with UnboundedThreadPoolExecutor() as executor:
      executor.map(self.append_and_sleep, itertools.repeat(0.01, 5))

    with self._lock:
      self.assertEqual(5, len(self._worker_idents))

  def test_shared_shutdown_does_nothing(self):
    thread_pool_executor.shared_unbounded_instance().shutdown()

    futures = []
    with thread_pool_executor.shared_unbounded_instance() as executor:
      for _ in range(0, 5):
        futures.append(executor.submit(self.append_and_sleep, 0.01))

    for future in futures:
      future.result(timeout=10)

    with self._lock:
      self.assertEqual(5, len(self._worker_idents))


if __name__ == '__main__':
  unittest.main()
