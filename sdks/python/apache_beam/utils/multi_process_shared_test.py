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
import unittest

from apache_beam.utils import multi_process_shared


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


class MultiProcessSharedTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.shared = multi_process_shared.MultiProcessShared(
        Counter, always_proxy=True).acquire()

  def test_call(self):
    self.assertEqual(self.shared.get(), 0)
    self.assertEqual(self.shared.increment(), 1)
    self.assertEqual(self.shared.increment(10), 11)
    self.assertEqual(self.shared.increment(value=10), 21)
    self.assertEqual(self.shared.get(), 21)

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
