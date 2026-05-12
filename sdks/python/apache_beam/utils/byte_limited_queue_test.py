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
import threading
import time
import unittest

from apache_beam.utils.byte_limited_queue import ByteLimitedQueue


class ByteLimitedQueueTest(unittest.TestCase):
  def test_unbounded(self):
    bq = ByteLimitedQueue()
    for i in range(201):
      bq.put(str(i), i)
    self.assertEqual(bq.byte_size(), sum(range(201)))
    self.assertEqual(bq.qsize(), 201)

  def test_put_and_get(self):
    bq = ByteLimitedQueue(maxbytes=200)
    bq.put('50', 50)
    bq.put('140', 140)
    self.assertEqual(bq.byte_size(), 190)
    self.assertEqual(bq.qsize(), 2)
    # Putting another would exceed 200.
    with self.assertRaises(queue.Full):
      bq.put('20', 20, block=False)
    bq.put('10', 10, block=False)
    self.assertEqual(bq.byte_size(), 200)
    self.assertEqual(bq.qsize(), 3)

    self.assertEqual(bq.get(), '50')
    self.assertEqual(bq.byte_size(), 150)
    self.assertEqual(bq.qsize(), 2)
    bq.put('20', 20, block=False)

  def test_dual_limit(self):
    # Queue limits: at most 3 items, OR at most 100 item bytes.
    bq = ByteLimitedQueue(maxsize=3, maxbytes=100)
    bq.put('30', 30)
    bq.put('40', 40)
    bq.put('20', 20)
    self.assertEqual(bq.byte_size(), 90)
    self.assertEqual(bq.qsize(), 3)
    # Full on element count (size=3).
    with self.assertRaises(queue.Full):
      bq.put('10', 10, block=False)
    self.assertEqual(bq.get(), '30')
    self.assertEqual(bq.get(), '40')
    bq.put('10', 10)
    # Full on byte count
    with self.assertRaises(queue.Full):
      bq.put('90', 90, block=False)
    self.assertEqual(bq.get(), '20')
    bq.put('90', 90, block=False)

  def test_multithreading(self):
    bq = ByteLimitedQueue(maxsize=0, maxbytes=100)
    received = []

    def producer():
      for i in range(101):
        bq.put(str(i), i)

    poison_pill = 'POISON'

    def consumer():
      while True:
        item = bq.get()
        if item == poison_pill:
          break
        received.append(int(item))

    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=producer)
    t3 = threading.Thread(target=consumer)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    bq.put(poison_pill, 0)

    t3.join()

    self.assertEqual(len(received), 202)
    self.assertEqual(sum(received), 2 * sum(range(101)))

  def test_put_timeout(self):
    bq = ByteLimitedQueue(maxsize=0, maxbytes=10)
    bq.put('10', 10)

    # The queue is completely full. A timeout put should raise queue.Full.
    with self.assertRaises(queue.Full):
      bq.put('5', 5, timeout=0.01)

    def delayed_consumer():
      time.sleep(0.05)
      bq.get()

    # Start a thread that will free up space after 50ms.
    t = threading.Thread(target=delayed_consumer)
    t.start()

    # The put should succeed once the consumer runs, use a high timeout to
    # flakiness.
    bq.put('item', 5, timeout=60)
    t.join()

  def test_get_timeout(self):
    bq = ByteLimitedQueue(maxsize=0, maxbytes=100)
    with self.assertRaises(queue.Empty):
      bq.get(block=False)
    with self.assertRaises(queue.Empty):
      bq.get(timeout=0.0)
    with self.assertRaises(queue.Empty):
      bq.get(timeout=.01)

    bq.put('1', 1)
    self.assertEqual('1', bq.get(timeout=0))

    bq.put('2', 2)
    self.assertEqual('2', bq.get(timeout=0.1))

    def delayed_producer():
      time.sleep(0.05)
      bq.put('3', 3)

    # Start a thread that will produce soon
    t = threading.Thread(target=delayed_producer)
    t.start()

    # The get should succeed once the produer runs, use a high timeout to
    # flakiness.
    self.assertEqual('3', bq.get(timeout=60))
    t.join()

  def test_negative_timeout(self):
    bq = ByteLimitedQueue()
    # Putting an item with a negative timeout should raise ValueError.
    with self.assertRaises(ValueError):
      bq.put('5', 5, timeout=-1)
    with self.assertRaises(ValueError):
      bq.get(timeout=-1)

  def test_single_element_override(self):
    bq = ByteLimitedQueue(maxbytes=10)
    # An item of size 50 exceeds maxbytes 10, but should be admitted
    # immediately without blocking since the queue is currently empty!
    bq.put('50', 50, block=False)
    self.assertEqual(bq.qsize(), 1)
    self.assertEqual(bq.byte_size(), 50)

  def test_fairness(self):
    bq = ByteLimitedQueue(maxbytes=10)
    # Put an initial item so that the queue is not empty,
    # causing the subsequent large item to block.
    bq.put('first', 2)
    self.assertEqual(bq.blocked_byte_size(), 0)

    def producer(item, size):
      bq.put(item, size)

    # Add an item in a background thread that should block due to exceeding
    # the limit.
    t1 = threading.Thread(target=producer, args=('too_large', 9))
    t1.start()

    # Wait until the background write is queued.
    while bq.blocked_byte_size() < 1:
      time.sleep(0.005)
    self.assertEqual(bq.blocked_byte_size(), 9)

    # Add smaller items afterwards.
    t2 = threading.Thread(target=producer, args=('small1', 1))
    t2.start()

    while bq.blocked_byte_size() < 10:
      time.sleep(0.005)
    self.assertEqual(bq.blocked_byte_size(), 10)

    t3 = threading.Thread(target=producer, args=('small2', 1))
    t3.start()

    while bq.blocked_byte_size() < 11:
      time.sleep(0.005)
    self.assertEqual(bq.blocked_byte_size(), 11)

    # Verify all items are received in order.
    self.assertEqual(bq.get(), 'first')
    t1.join()
    t2.join()
    self.assertEqual(bq.get(), 'too_large')
    t3.join()
    self.assertEqual(bq.get(), 'small1')
    self.assertEqual(bq.get(), 'small2')

  def test_blocked_waiter_timeout_multiple(self):
    bq = ByteLimitedQueue(maxbytes=10)
    bq.put('initial', 5)

    status = []
    lock = threading.Lock()

    def producer(name, size, timeout_val):
      try:
        bq.put(name, size, timeout=timeout_val)
        with lock:
          status.append((name, 'success'))
      except queue.Full:
        with lock:
          status.append((name, 'timeout'))

    threads = []
    threads.append(threading.Thread(target=producer, args=('t1', 8, 0.2)))
    threads.append(threading.Thread(target=producer, args=('t2', 8, 60.0)))
    threads.append(threading.Thread(target=producer, args=('t3', 3, 0.1)))
    threads.append(threading.Thread(target=producer, args=('t4', 3, 60.0)))
    threads.append(threading.Thread(target=producer, args=('t5', 3, 0.1)))
    for t in threads:
      t.start()

    # Wait for the short-timeout threads.
    threads[4].join()
    threads[2].join()
    threads[0].join()

    # Now waiting writers should just be t1 and t3
    self.assertEqual(bq.blocked_byte_size(), 11)

    self.assertEqual(bq.get(), 'initial')
    threads[1].join()
    self.assertGreater(bq.blocked_byte_size(), 0)

    elem = bq.get()
    self.assertTrue(elem == 't2' or elem == 't4')
    threads[3].join()
    self.assertEqual(bq.blocked_byte_size(), 0)
    elem = bq.get()
    self.assertTrue(elem == 't2' or elem == 't4')

    with lock:
      self.assertIn(('t1', 'timeout'), status)
      self.assertIn(('t2', 'success'), status)
      self.assertIn(('t3', 'timeout'), status)
      self.assertIn(('t4', 'success'), status)
      self.assertIn(('t5', 'timeout'), status)


if __name__ == '__main__':
  unittest.main()
