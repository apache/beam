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

"""Unit tests for byte-limited queue."""

import queue
import sys
import threading
import time
import unittest

from apache_beam.utils.byte_limited_queue import ByteLimitedQueue


class FakeItem(object):
  def __init__(self, size):
    self._size = size

  def weight(self):
    return self._size


class ByteLimitedQueueTest(unittest.TestCase):
  def test_unbounded(self):
    bq = ByteLimitedQueue(lambda x: x.weight())
    for i in range(200):
      bq.put(FakeItem(i))
    # Add 1 since weight of zero is set to 1
    self.assertEqual(bq.byte_size(), sum(range(200)) + 1)
    self.assertEqual(bq.qsize(), 200)

  def test_put_and_get(self):
    bq = ByteLimitedQueue(lambda x: x.weight(), maxweight=200)
    bq.put(FakeItem(50))
    bq.put(FakeItem(140))
    self.assertEqual(bq.byte_size(), 190)
    self.assertEqual(bq.qsize(), 2)
    # Putting another would exceed 200.
    with self.assertRaises(queue.Full):
      bq.put(FakeItem(20), block=False)
    bq.put(FakeItem(10), block=False)
    self.assertEqual(bq.byte_size(), 200)
    self.assertEqual(bq.qsize(), 3)

    self.assertEqual(bq.get().weight(), 50)
    self.assertEqual(bq.byte_size(), 150)
    self.assertEqual(bq.qsize(), 2)
    bq.put(FakeItem(20), block=False)

  def test_dual_limit(self):
    # Queue limits: at most 2 items, OR at most 100 weight.
    bq = ByteLimitedQueue(lambda x: x.weight(), maxsize=3, maxweight=100)
    bq.put(FakeItem(30))
    bq.put(FakeItem(40))
    bq.put(FakeItem(20))
    self.assertEqual(bq.byte_size(), 90)
    self.assertEqual(bq.qsize(), 3)
    # Full on element count (size=2).
    with self.assertRaises(queue.Full):
      bq.put(FakeItem(10), block=False)
    self.assertEqual(bq.get().weight(), 30)
    self.assertEqual(bq.get().weight(), 40)
    bq.put(FakeItem(10))
    # Full on byte count
    with self.assertRaises(queue.Full):
      bq.put(FakeItem(90), block=False)
    self.assertEqual(bq.get().weight(), 20)
    bq.put(FakeItem(90), block=False)

  @unittest.skipIf(sys.version_info < (3, 13), 'Queue.ShutDown added in 3.13.')
  def test_multithreading(self):
    bq = ByteLimitedQueue(lambda x: x.weight(), maxsize=0, maxweight=100)
    received = []

    def producer():
      for i in range(101):
        bq.put(FakeItem(i))

    def consumer():
      while True:
        try:
          received.append(bq.get().weight())
        except queue.ShutDown:
          break

    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=producer)
    t3 = threading.Thread(target=consumer)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    bq.shutdown()

    t3.join()

    self.assertEqual(len(received), 202)
    self.assertEqual(sum(received), 2 * sum(range(101)))

  def test_multithreading_timeout(self):
    bq = ByteLimitedQueue(lambda x: x.weight(), maxsize=0, maxweight=10)
    bq.put(FakeItem(10))

    # The queue is completely full. A timeout put should raise queue.Full.
    with self.assertRaises(queue.Full):
      bq.put(FakeItem(5), timeout=0.01)

    def delayed_consumer():
      time.sleep(0.05)
      bq.get()

    # Start a thread that will free up space after 50ms.
    t = threading.Thread(target=delayed_consumer)
    t.start()

    # The put should succeed once the consumer runs, use a high timeout to
    # flakiness.
    bq.put(FakeItem(5), timeout=60)
    t.join()

  def test_negative_timeout(self):
    bq = ByteLimitedQueue(lambda x: x.weight())
    # Putting an item with a negative timeout should raise ValueError.
    with self.assertRaises(ValueError):
      bq.put(FakeItem(5), timeout=-1)

  def test_single_element_override(self):
    bq = ByteLimitedQueue(lambda x: x.weight(), maxweight=10)
    # An item of size 50 exceeds maxweight 10, but should be admitted
    # immediately without blocking since the queue is currently empty!
    bq.put(FakeItem(50), block=False)
    self.assertEqual(bq.qsize(), 1)
    self.assertEqual(bq.byte_size(), 50)

  def test_inconsistent_weighing_fn(self):
    # Return a different weight for the same item.
    weights = [10, 5]
    bq = ByteLimitedQueue(lambda x: weights.pop(0), maxweight=100)

    bq.put(1)
    self.assertEqual(bq.byte_size(), 10)

    # Upon popping, the weighing function (if called) would have returned 5,
    # but the stored weight prevents corruption and cleanly reduces the size to
    # 0.
    bq.get()
    self.assertEqual(bq.byte_size(), 0)


if __name__ == '__main__':
  unittest.main()
