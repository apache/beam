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

from __future__ import absolute_import

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from math import floor
from threading import RLock
from time import sleep
from time import time
from types import GeneratorType

import apache_beam as beam
from apache_beam import TimeDomain
from apache_beam.coders import coders
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.utils.shared import Shared
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


# A wrapper around a dofn that processes that dofn in an asynchronous manner.
class AsyncWrapper(beam.DoFn):
  """Class that wraps a dofn and converts it from one which process elements
  synchronously to one which processes them asynchronously.

  For synchronous dofns the default settings mean that many (100s) of elements
  will be processed in parallel and that processing an element will block all
  other work on that key.  In addition runners are optimized for latencies less
  than a few seconds and longer operations can result in high retry rates. Async
  should be considered when the default parallelism is not correct and/or items
  are expected to take longer than a few seconds to process.
  """

  TIMER = TimerSpec('timer', TimeDomain.REAL_TIME)
  TIMER_SET = ReadModifyWriteStateSpec('timer_set', coders.BooleanCoder())
  TO_PROCESS = BagStateSpec(
      'to_process',
      coders.TupleCoder([coders.StrUtf8Coder(), coders.StrUtf8Coder()]),
  )
  _timer_frequency = 20
  # The below items are one per dofn (not instance) so are maps of UUID to
  # value.
  _processing_elements = {}
  _items_in_buffer = {}
  _pool = {}
  # Must be reentrant lock so that callbacks can either be called by a pool
  # thread OR the thread submitting the callback which must already hold this
  # lock.
  _lock = RLock()
  _verbose_logging = False

  def __init__(
      self,
      sync_fn,
      parallelism=1,
      callback_frequency=5,
      max_items_to_buffer=None,
      max_wait_time=120,
  ):
    """Wraps the sync_fn to create an asynchronous version.

    Args:
      sync_fn: The dofn to wrap.  Must take (K, V) as input.
      parallelism: The maximum number of elements to process in parallel per
        worker for this dofn.  By default this is set to 1 as the most common
        case for async dofns are heavy CPU or GPU dofns where the dofn requires
        the a signficant fraction of the CPU/GPU.
      callback_frequency: The frequency with which the runner will check for
        elements to commit.  A short callback frequency will mean items are
        commited shortly after processing but cause additional work for the
        worker.  A large callback frequency will result in slower commits but
        less busy work.  The default of 5s will result in a maximum added
        latency of 5s while requiring relatively few resources.  If your
        messages take significantly longer than 5s to process it is recommended
        to raise this.
      max_items_to_buffer: We should ideally buffer enough to always be busy but
        not so much that the worker ooms.  By default will be 2x the parallelism
        which should be good for most pipelines.
      max_wait_time: The maximum amount of time an item should wait to be added
        to the buffer.  Used for testing to ensure timeouts are met.
    """
    self._sync_fn = sync_fn
    self._uuid = uuid.uuid4().hex
    self._parallelism = parallelism
    self._max_wait_time = max_wait_time
    self._timer_frequency = 20
    if max_items_to_buffer is None:
      self._max_items_to_buffer = max(parallelism * 2, 10)
    else:
      self._max_items_to_buffer = max_items_to_buffer

    AsyncWrapper._processing_elements[self._uuid] = {}
    AsyncWrapper._items_in_buffer[self._uuid] = 0
    self.max_wait_time = max_wait_time
    self.timer_frequency_ = callback_frequency
    self.parallelism_ = parallelism
    self._next_time_to_fire = Timestamp.now() + Duration(seconds=5)
    self._shared_handle = Shared()

  @staticmethod
  def initialize_pool(parallelism):
    return lambda: ThreadPoolExecutor(max_workers=parallelism)

  @staticmethod
  def reset_state():
    for pool in AsyncWrapper._pool.values():
      pool.acquire(AsyncWrapper.initialize_pool(1)).shutdown(
          wait=True, cancel_futures=True)
    with AsyncWrapper._lock:
      AsyncWrapper._pool = {}
      AsyncWrapper._processing_elements = {}
      AsyncWrapper._items_in_buffer = {}

  def setup(self):
    """Forwards to the wrapped dofn's setup method."""
    self._sync_fn.setup()
    with AsyncWrapper._lock:
      if not self._uuid in AsyncWrapper._pool:
        AsyncWrapper._pool[self._uuid] = Shared()
        AsyncWrapper._processing_elements[self._uuid] = {}
        AsyncWrapper._items_in_buffer[self._uuid] = 0

  def teardown(self):
    """Forwards to the wrapped dofn's teardown method."""
    self._sync_fn.teardown()

  def sync_fn_process(self, element, *args, **kwargs):
    """Makes the call to the wrapped dofn's start_bundle, process

    methods.  It will then combine the results into a single generator.

    Args:
      element: The element to process.
      *args: Any additional arguments to pass to the wrapped dofn's process
        method.  Will be the same args that the async wrapper is called with.
      **kwargs: Any additional keyword arguments to pass to the wrapped dofn's
        process method.  Will be the same kwargs that the async wrapper is
        called with.

    Returns:
      A generator of elements produced by the input element.
    """
    self._sync_fn.start_bundle()
    process_result = self._sync_fn.process(element, *args, **kwargs)
    bundle_result = self._sync_fn.finish_bundle()

    # both process and finish bundle may or may not return generators. We want
    # to combine whatever results have been returned into a single generator. If
    # they are single elements then wrap them in lists so that we can combine
    # them.
    if not process_result:
      process_result = []
    elif not isinstance(process_result, GeneratorType):
      process_result = [process_result]
    if not bundle_result:
      bundle_result = []
    elif not isinstance(bundle_result, GeneratorType):
      bundle_result = [bundle_result]

    to_return = []
    for x in process_result:
      to_return.append(x)
    for x in bundle_result:
      to_return.append(x)

    return to_return

  def decrement_items_in_buffer(self, future):
    with AsyncWrapper._lock:
      AsyncWrapper._items_in_buffer[self._uuid] -= 1

  def schedule_if_room(self, element, ignore_buffer=False, *args, **kwargs):
    """Schedules an item to be processed asynchronously if there is room.

    Args:
      element: The element to process.
      ignore_buffer: If true will ignore the buffer limit and schedule the item
        regardless of the buffer size.  Used when an item needs to skip to the
        front such as retries.
      *args: arguments that the wrapped dofn requires.
      **kwargs: keyword arguments that the wrapped dofn requires.

    Returns:
      True if the item was scheduled False otherwise.
    """
    with AsyncWrapper._lock:
      if element in AsyncWrapper._processing_elements[self._uuid]:
        logging.info('item %s already in processing elements', element)
        return True
      if self.accepting_items() or ignore_buffer:
        result = AsyncWrapper._pool[self._uuid].acquire(
            AsyncWrapper.initialize_pool(self._parallelism)).submit(
                lambda: self.sync_fn_process(element, *args, **kwargs),
            )
        result.add_done_callback(self.decrement_items_in_buffer)
        AsyncWrapper._processing_elements[self._uuid][element] = result
        AsyncWrapper._items_in_buffer[self._uuid] += 1
        return True
      else:
        return False

  # Add an item to the processing pool. Add the future returned by that item to
  # processing_elements_.
  def schedule_item(self, element, ignore_buffer=False, *args, **kwargs):
    """Schedules an item to be processed asynchronously.

    If the queue is full will block until room opens up.

    After calling AsyncWrapper will hold a future pointing to the
    result of this processing

    Args:
      element: The element to process.
      ignore_buffer: If true will ignore the buffer limit and schedule the item
        regardless of the buffer size.  Used when an item needs to skip to the
        front such as retries.
      *args: arguments that the wrapped dofn requires.
      **kwargs: keyword arguments that the wrapped dofn requires.
    """
    done = False
    sleep_time = 1
    total_sleep = 0
    while not done:
      done = self.schedule_if_room(element, ignore_buffer, *args, **kwargs)
      if not done:
        sleep_time = min(self.max_wait_time, sleep_time * 2)
        if self._verbose_logging or total_sleep > 10:
          logging.info(
              'buffer is full for item %s, %s waiting %s seconds.  Have waited'
              ' for %s seconds.',
              element,
              AsyncWrapper._items_in_buffer[self._uuid],
              sleep_time,
              total_sleep,
          )
        total_sleep += sleep_time
        sleep(sleep_time)

  def next_time_to_fire(self):
    return (
        floor((time() + self._timer_frequency) / self._timer_frequency) *
        self._timer_frequency)

  def accepting_items(self):
    with AsyncWrapper._lock:
      return (
          AsyncWrapper._items_in_buffer[self._uuid] < self._max_items_to_buffer)

  def is_empty(self):
    with AsyncWrapper._lock:
      return AsyncWrapper._items_in_buffer[self._uuid] == 0

  # Add the incoming element to a pool of elements to process asynchronously.
  def process(
      self,
      element,
      timer=beam.DoFn.TimerParam(TIMER),
      to_process=beam.DoFn.StateParam(TO_PROCESS),
      *args,
      **kwargs):
    """Add the elements to the list of items to be processed asynchronously.

    Performs additional bookkeeping to maintain exactly once and set timers to
    commit item after it has finished processing.

    Args:
      element: The element to process.
      timer: Callback timer that will commit elements.
      to_process: State that keeps track of queued items for exactly once.
      *args: arguments that the wrapped dofn requires.
      **kwargs: keyword arguments that the wrapped dofn requires.

    Returns:
      An empty list. The elements will be output asynchronously.
    """

    self.schedule_item(element)

    to_process.add(element)

    # Set a timer to fire on the next round increment of timer_frequency_. Note
    # we do this so that each messages timer doesn't get overwritten by the
    # next.
    time_to_fire = self.next_time_to_fire()
    timer.set(time_to_fire)

    # Don't output any elements.  This will be done in commit_finished_items.
    return []

  # Synchronises local state (processing_elements_) with SE state (to_process).
  # Then outputs all finished elements. Finally, sets a timer to fire on the
  # next round increment of timer_frequency_.
  def commit_finished_items(
      self,
      to_process=beam.DoFn.StateParam(TO_PROCESS),
      timer=beam.DoFn.TimerParam(TIMER),
  ):
    """Commits finished items and synchronizes local state with runner state.

    Note timer firings are per key while local state contains messages for all
    keys.  Only messages for the given key will be output/cleaned up.

    Args:
      to_process: State that keeps track of queued messagees for this key.
      timer: Timer that initiated this commit and can be reset if not all items
        have finished..

    Returns:
      A list of elements that have finished processing for this key.
    """
    # For all elements that are in processing state:
    # If the element is done processing, delete it from all state and yield the
    # output.
    # If the element is not yet done, print it. If the element is not in
    # local state, schedule it for processing.
    items_finished = 0
    items_not_yet_finished = 0
    items_rescheduled = 0
    items_cancelled = 0
    items_in_processing_state = 0
    items_in_se_state = 0

    to_process_local = list(to_process.read())

    # For all elements that in local state but not processing state delete them
    # from local state and cancel their futures.
    to_remove = []
    key = None
    if to_process_local:
      key = str(to_process_local[0][0])
    else:
      logging.error(
          'no elements in state during timer callback. Timer should not have'
          ' been set.')
    if self._verbose_logging:
      logging.info('procesing timer for key: %s', key)
    # processing state is per key so we expect this state to only contain a
    # given key.  Skip items in processing_elements which are for a different
    # key.
    with AsyncWrapper._lock:
      for x in AsyncWrapper._processing_elements[self._uuid]:
        if x[0] == key and x not in to_process_local:
          items_cancelled += 1
          AsyncWrapper._processing_elements[self._uuid][x].cancel()
          to_remove.append(x)
          logging.info(
              'cancelling item %s which is no longer in processing state', x)
      for x in to_remove:
        AsyncWrapper._processing_elements[self._uuid].pop(x)

      # For all elements which have finished processing output their result.
      to_return = []
      finished_items = []
      for x in to_process_local:
        items_in_se_state += 1
        if x in AsyncWrapper._processing_elements[self._uuid]:
          if AsyncWrapper._processing_elements[self._uuid][x].done():
            to_return.append(
                AsyncWrapper._processing_elements[self._uuid][x].result())
            finished_items.append(x)
            AsyncWrapper._processing_elements[self._uuid].pop(x)
            items_finished += 1
          else:
            items_not_yet_finished += 1
        else:
          logging.info(
              'item %s found in processing state but not local state,'
              ' scheduling now',
              x)
          self.schedule_item(x, ignore_buffer=True)
          items_rescheduled += 1

    # Update processing state to remove elements we've finished
    to_process.clear()
    for x in to_process_local:
      if x not in finished_items:
        items_in_processing_state += 1
        to_process.add(x)

    logging.info('items finished %d', items_finished)
    logging.info('items not yet finished %d', items_not_yet_finished)
    logging.info('items rescheduled %d', items_rescheduled)
    logging.info('items cancelled %d', items_cancelled)
    logging.info('items in processing state %d', items_in_processing_state)
    logging.info(
        'items in buffer %d', AsyncWrapper._items_in_buffer[self._uuid])

    # If there are items not yet finished then set a timer to fire in the
    # future.
    self._next_time_to_fire = Timestamp.now() + Duration(seconds=5)
    if items_not_yet_finished > 0:
      time_to_fire = self.next_time_to_fire()
      timer.set(time_to_fire)

    # Each result is a list. We want to combine them into a single
    # list of all elements we wish to output.
    merged_return = []
    for x in to_return:
      merged_return.extend(x)
    return merged_return

  @on_timer(TIMER)
  def timer_callback(
      self,
      to_process=beam.DoFn.StateParam(TO_PROCESS),
      timer=beam.DoFn.TimerParam(TIMER),
  ):
    """Helper method to commit finished items in response to timer firing.

    Args:
      to_process: State that keeps track of queued items for exactly once.
      timer: Timer that initiated this commit and can be reset if not all items
        have finished.

    Returns:
      A generator of elements that have finished processing for this key.
    """
    return self.commit_finished_items(to_process, timer)
