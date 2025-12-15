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

import logging
import threading
import tempfile
import os
import multiprocessing
import unittest
from typing import Any

from apache_beam.utils import multi_process_shared


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


class SimpleClass:
  def make_proxy(
      self, tag: str = 'proxy_on_proxy', spawn_process: bool = False):
    return multi_process_shared.MultiProcessShared(
        Counter, tag=tag, always_proxy=True,
        spawn_process=spawn_process).acquire()


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

    multi_process_shared.MultiProcessShared(
        Counter, tag='test_unsafe_hard_delete').unsafe_hard_delete()

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

    counter2.unsafe_hard_delete()

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

    multi_process_shared.MultiProcessShared(
        Counter, tag='no_tag_to_delete').unsafe_hard_delete()

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

  def test_proxy_on_proxy(self):
    shared1 = multi_process_shared.MultiProcessShared(
        SimpleClass, tag='proxy_on_proxy_main', always_proxy=True)
    instance = shared1.acquire()
    proxy_instance = instance.make_proxy()
    self.assertEqual(proxy_instance.increment(), 1)


class MultiProcessSharedSpawnProcessTest(unittest.TestCase):
  def setUp(self):
    tempdir = tempfile.gettempdir()
    for tag in ['basic',
                'proxy_on_proxy',
                'proxy_on_proxy_main',
                'main',
                'to_delete',
                'mix1',
                'mix2']:
      for ext in ['', '.address', '.address.error']:
        try:
          os.remove(os.path.join(tempdir, tag + ext))
        except OSError:
          pass

  def tearDown(self):
    for p in multiprocessing.active_children():
      p.terminate()
      p.join()

  def test_call(self):
    shared = multi_process_shared.MultiProcessShared(
        Counter, tag='basic', always_proxy=True, spawn_process=True).acquire()
    self.assertEqual(shared.get(), 0)
    self.assertEqual(shared.increment(), 1)
    self.assertEqual(shared.increment(10), 11)
    self.assertEqual(shared.increment(value=10), 21)
    self.assertEqual(shared.get(), 21)

  def test_proxy_on_proxy(self):
    shared1 = multi_process_shared.MultiProcessShared(
        SimpleClass, tag='main', always_proxy=True)
    instance = shared1.acquire()
    proxy_instance = instance.make_proxy(spawn_process=True)
    self.assertEqual(proxy_instance.increment(), 1)
    proxy_instance.unsafe_hard_delete()

    proxy_instance2 = instance.make_proxy(tag='proxy_2', spawn_process=True)
    self.assertEqual(proxy_instance2.increment(), 1)

  def test_unsafe_hard_delete_autoproxywrapper(self):
    shared1 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_delete', always_proxy=True, spawn_process=True)
    shared2 = multi_process_shared.MultiProcessShared(
        Counter, tag='to_delete', always_proxy=True, spawn_process=True)
    counter3 = multi_process_shared.MultiProcessShared(
        Counter, tag='basic', always_proxy=True, spawn_process=True).acquire()

    counter1 = shared1.acquire()
    counter2 = shared2.acquire()
    self.assertEqual(counter1.increment(), 1)
    self.assertEqual(counter2.increment(), 2)

    counter2.unsafe_hard_delete()

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
