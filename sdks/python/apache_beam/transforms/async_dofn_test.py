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

import logging
import random
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import apache_beam as beam
import apache_beam.transforms.async_dofn as async_lib


class BasicDofn(beam.DoFn):
  def __init__(self, sleep_time=0):
    self.processed = 0
    self.sleep_time = sleep_time
    self.lock = Lock()

  def process(self, element):
    logging.info('start processing element %s', element)
    time.sleep(self.sleep_time)
    self.lock.acquire()
    self.processed += 1
    self.lock.release()
    logging.info('finish processing element %s', element)
    yield element

  def getProcessed(self):
    self.lock.acquire()
    result = self.processed
    self.lock.release()
    return result


# Used for testing multiple elements produced by a single input element.
class MultiElementDoFn(beam.DoFn):
  def finish_bundle(self):
    yield ('key', 'bundle end')

  def process(self, element):
    yield element
    yield element


class FakeBagState:
  def __init__(self, items):
    self.items = items
    # Normally SE would have a lock on the BT row protecting this from multiple
    # updates. Here without SE we must lock ourselvs.
    self.lock = Lock()

  def add(self, item):
    with self.lock:
      self.items.append(item)

  def clear(self):
    with self.lock:
      self.items = []

  def read(self):
    with self.lock:
      return self.items.copy()


class FakeTimer:
  def __init__(self, time):
    self.time = time

  def set(self, time):
    self.time = time


class AsyncTest(unittest.TestCase):
  def setUp(self):
    super().setUp()
    async_lib.AsyncWrapper.reset_state()

  def wait_for_empty(self, async_dofn, timeout=10):
    count = 0
    increment = 1
    while not async_dofn.is_empty():
      time.sleep(1)
      count += increment
      if count > timeout:
        raise TimeoutError('Timed out waiting for async dofn to be empty')
    # Usually we are waiting for an item to be ready to output not just no
    # longer in the queue. These are not the same atomic operation. Sleep a bit
    # to make sure the item is also ready to output.
    time.sleep(1)

  def check_output(self, result, expected_output):
    output = []
    for x in result:
      output.append(x)
    # Ordering is not guaranteed so sort both lists before comparing.
    output.sort()
    expected_output.sort()
    self.assertEqual(output, expected_output)

  def check_items_in_buffer(self, async_dofn, expected_count):
    self.assertEqual(
        async_lib.AsyncWrapper._items_in_buffer[async_dofn._uuid],
        expected_count,
    )

  def test_basic(self):
    # Setup an async dofn and send a message in to process.
    dofn = BasicDofn()
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    result = async_dofn.process(
        msg, to_process=fake_bag_state, timer=fake_timer)

    # nothing should be produced because this is async.
    self.assertEqual(result, [])
    # there should be an element to process in the bag state.
    self.assertEqual(fake_bag_state.items, [msg])
    # The timer should be set.
    self.assertNotEqual(fake_timer.time, 0)

    # Give time for the async element to process.
    self.wait_for_empty(async_dofn)
    # Fire the (fake) timer and check that the input message has produced output
    # and the message has been removed from bag state.
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg])
    self.assertTrue(dofn.getProcessed() == 1)
    self.assertEqual(fake_bag_state.items, [])

  def test_multi_key(self):
    # Send in two messages with different keys..
    dofn = BasicDofn()
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state_key1 = FakeBagState([])
    fake_bag_state_key2 = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg1 = ('key1', 1)
    msg2 = ('key2', 2)
    async_dofn.process(msg1, to_process=fake_bag_state_key1, timer=fake_timer)
    async_dofn.process(msg2, to_process=fake_bag_state_key2, timer=fake_timer)
    self.wait_for_empty(async_dofn)

    # The result should depend on which key the timer fires for. We are firing
    # 'key2' first so expect its message to be produced and msg1 to remain in
    # state.
    result = async_dofn.commit_finished_items(fake_bag_state_key2, fake_timer)
    self.check_output(result, [msg2])
    self.assertEqual(fake_bag_state_key1.items, [msg1])
    self.assertEqual(fake_bag_state_key2.items, [])

    # Now fire 'key1' and expect its message to be produced.
    result = async_dofn.commit_finished_items(fake_bag_state_key1, fake_timer)
    self.check_output(result, [msg1])
    self.assertEqual(fake_bag_state_key1.items, [])
    self.assertEqual(fake_bag_state_key2.items, [])

  def test_long_item(self):
    # Test that everything still works with a long running time for the dofn.
    dofn = BasicDofn(sleep_time=5)
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)

    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [])
    self.assertTrue(dofn.getProcessed() == 0)
    self.assertEqual(fake_bag_state.items, [msg])

    self.wait_for_empty(async_dofn, 20)

    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg])
    self.assertTrue(dofn.getProcessed() == 1)
    self.assertEqual(fake_bag_state.items, [])

  def test_lost_item(self):
    # Setup an element in the bag stat thats not in processing state.
    # The async dofn should reschedule this element.
    dofn = BasicDofn()
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    fake_bag_state = FakeBagState([msg])

    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [])
    # The element should be rescheduled and still processed.
    self.wait_for_empty(async_dofn)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg])

  def test_cancelled_item(self):
    # Test that an item gets removed for processing and does not get output when
    # it is not present in the bag state. Either this item moved or a commit
    # failed making the local state and bag stat inconsistent.
    dofn = BasicDofn()
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    msg = ('key1', 1)
    msg2 = ('key1', 2)
    fake_timer = FakeTimer(0)
    fake_bag_state = FakeBagState([])
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    async_dofn.process(msg2, to_process=fake_bag_state, timer=fake_timer)

    self.wait_for_empty(async_dofn)
    fake_bag_state.clear()
    fake_bag_state.add(msg2)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg2])
    self.assertEqual(fake_bag_state.items, [])

  def test_multi_element_dofn(self):
    # Test that async works when a dofn produces multiple elements in process
    # and finish_bundle.
    dofn = MultiElementDoFn()
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)

    self.wait_for_empty(async_dofn)

    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg, msg, ('key', 'bundle end')])
    self.assertEqual(fake_bag_state.items, [])

  def test_duplicates(self):
    # Test that async will produce a single output when a given input is sent
    # multiple times.
    dofn = BasicDofn(5)
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    # SE will only deliver the message again if the commit failed and so bag
    # state should still be empty.
    fake_bag_state.clear()
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    self.assertEqual(fake_bag_state.items, [msg])

    self.wait_for_empty(async_dofn)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg])
    self.assertEqual(fake_bag_state.items, [])

  def test_slow_duplicates(self):
    # Test that async will produce a single output when a given input is sent
    # multiple times.
    dofn = BasicDofn(5)
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    fake_bag_state = FakeBagState([])
    fake_timer = FakeTimer(0)
    msg = ('key1', 1)
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    time.sleep(10)
    # SE will only deliver the message again if the commit failed and so bag
    # state should still be empty.
    fake_bag_state.clear()
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [])
    self.assertEqual(fake_bag_state.items, [])

    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    self.assertEqual(fake_bag_state.items, [msg])
    self.wait_for_empty(async_dofn)

    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, [msg])
    self.assertEqual(fake_bag_state.items, [])

  def test_buffer_count(self):
    # Test that the buffer count is correctly incremented when adding items.
    dofn = BasicDofn(5)
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    msg = ('key1', 1)
    fake_timer = FakeTimer(0)
    fake_bag_state = FakeBagState([])
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    self.check_items_in_buffer(async_dofn, 1)
    self.wait_for_empty(async_dofn)
    self.check_items_in_buffer(async_dofn, 0)

    # Commiting already finished items should not change the count.
    async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_items_in_buffer(async_dofn, 0)

  def test_buffer_stops_accepting_items(self):
    # Test that the buffer stops accepting items when it is full.
    dofn = BasicDofn(5)
    async_dofn = async_lib.AsyncWrapper(
        dofn, parallelism=1, max_items_to_buffer=5)
    async_dofn.setup()
    fake_timer = FakeTimer(0)
    fake_bag_state = FakeBagState([])

    # Send more elements than the buffer can hold.
    pool = ThreadPoolExecutor(max_workers=10)
    expected_output = []
    for i in range(0, 10):

      def add_item(i):
        item = ('key', i)
        expected_output.append(item)
        pool.submit(
            lambda: async_dofn.process(
                item, to_process=fake_bag_state, timer=fake_timer))

      add_item(i)

    # Assert that the buffer stops accepting items at its limit.
    time.sleep(1)
    self.assertEqual(async_dofn._max_items_to_buffer, 5)
    self.check_items_in_buffer(async_dofn, 5)

    # After 55 seconds all items should be finished (including those which were
    # waiting on the buffer).
    self.wait_for_empty(async_dofn, 100)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_output(result, expected_output)
    self.check_items_in_buffer(async_dofn, 0)

  def test_buffer_with_cancellation(self):
    dofn = BasicDofn(3)
    async_dofn = async_lib.AsyncWrapper(dofn)
    async_dofn.setup()
    msg = ('key1', 1)
    msg2 = ('key1', 2)
    fake_timer = FakeTimer(0)
    fake_bag_state = FakeBagState([])
    async_dofn.process(msg, to_process=fake_bag_state, timer=fake_timer)
    async_dofn.process(msg2, to_process=fake_bag_state, timer=fake_timer)
    self.check_items_in_buffer(async_dofn, 2)
    self.assertEqual(fake_bag_state.items, [msg, msg2])

    # Remove one item from the bag state to simulate the user worker being out
    # of sync with SE state. The item should be cancelled and also removed from
    # the buffer.
    fake_bag_state.clear()
    fake_bag_state.add(msg2)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    # Note buffer may still be 2 as msg1 can start processing before we get to
    # cancel it but also not be finished yet.
    self.check_output(result, [])
    self.assertEqual(fake_bag_state.items, [msg2])

    self.wait_for_empty(async_dofn)
    result = async_dofn.commit_finished_items(fake_bag_state, fake_timer)
    self.check_items_in_buffer(async_dofn, 0)
    self.check_output(result, [msg2])
    self.assertEqual(fake_bag_state.items, [])

  def test_load_correctness(self):
    # Test AsyncDofn over heavy load.
    dofn = BasicDofn(1)
    max_sleep = 10
    async_dofn = async_lib.AsyncWrapper(dofn, max_wait_time=max_sleep)
    async_dofn.setup()
    bag_states = {}
    timers = {}
    expected_outputs = {}
    pool = ThreadPoolExecutor(max_workers=10)
    futures = []

    for i in range(0, 10):
      bag_states['key' + str(i)] = FakeBagState([])
      timers['key' + str(i)] = FakeTimer(0)
      expected_outputs['key' + str(i)] = []

    for i in range(0, 100):

      def add_item(i):
        item = ('key' + str(random.randint(0, 9)), i)
        expected_outputs[item[0]].append(item)
        futures.append(
            pool.submit(
                lambda: async_dofn.process(
                    item, to_process=bag_states[item[0]], timer=timers[item[0]])
            ))

      add_item(i)
      time.sleep(random.random())

    # Run for a while. Should be enough to start all items but not finish them
    # all.
    time.sleep(random.randint(30, 50))
    # Commit some stuff
    pre_crash_results = []
    for i in range(0, 10):
      pre_crash_results.append(
          async_dofn.commit_finished_items(
              bag_states['key' + str(i)], timers['key' + str(i)]))

    # Wait for all items to at least make it into the buffer.
    done = False
    while not done:
      time.sleep(10)
      done = True
      for future in futures:
        if not future.done():
          done = False
          break

    # Wait for all items to finish.
    self.wait_for_empty(async_dofn)

    for i in range(0, 10):
      result = async_dofn.commit_finished_items(
          bag_states['key' + str(i)], timers['key' + str(i)])
      logging.info('pre_crash_results %s', pre_crash_results[i])
      logging.info('result %s', result)
      self.check_output(
          pre_crash_results[i] + result, expected_outputs['key' + str(i)])
      self.assertEqual(bag_states['key' + str(i)].items, [])


if __name__ == '__main__':
  unittest.main()
