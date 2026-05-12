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

"""A thread-safe queue that limits capacity by total byte size."""

import collections
import queue
import threading
import time
import types


class ByteLimitedQueue(object):
  """A fair queue that limits by both element count and total byte size.

  A single element is allowed to exceed the maxbytes to avoid deadlock.
  """
  __class_getitem__ = classmethod(types.GenericAlias)

  def __init__(
      self,
      maxsize=0,  # type: int
      maxbytes=0,  # type: int
  ):
    # type: (...) -> None

    """Initializes a ByteLimitedQueue.

    Args:
      maxsize: The maximum number of items allowed in the queue. If 0 or
        negative, there is no limit on the number of elements.
      maxbytes: The maximum accumulated bytes allowed in the queue. If 0 or
        negative, there is no limit on the total bytes of the elements.
    """
    self.max_elements = maxsize
    self.max_bytes = maxbytes

    self._byte_size = 0
    self._blocked_bytes = 0
    self._mutex = threading.Lock()
    self._not_empty = threading.Condition(self._mutex)

    self._waiting_writers = collections.deque()
    self._condition_pool = []
    self._queue = collections.deque()

  def put(self, item, item_bytes, *, block=True, timeout=None):
    """Put an item into the queue.

    If the queue is full, block until a free slot is available, unless `block`
    is false or a timeout occurs.

    Args:
      item: The item to put into the queue.
      item_bytes: The size of the item.
      block: If True, block until space is available. If False, raise queue.Full
        immediately if the queue is full.
      timeout: If block is True, wait for at most `timeout` seconds. If None,
        block indefinitely.

    Raises:
      ValueError: If timeout or item_bytes is negative.
      queue.Full: If the queue is full and block is False or the timeout occurs.
    """
    if timeout is not None and timeout < 0:
      raise ValueError("'timeout' must be a non-negative number")
    if item_bytes < 0:
      raise ValueError("'item_bytes' must be a non-negative number")

    with self._mutex:
      if not self._waiting_writers and self._can_fit(item_bytes):
        self._queue.append((item, item_bytes))
        self._byte_size += item_bytes
        self._not_empty.notify()
        return

      if not block:
        raise queue.Full

      # Reuse or create a condition
      my_cond = (
          self._condition_pool.pop()
          if self._condition_pool else threading.Condition(self._mutex))

      endtime = time.monotonic() + timeout if timeout is not None else None

      try:
        self._blocked_bytes += item_bytes
        self._waiting_writers.append(my_cond)
        while True:
          if timeout is None:
            my_cond.wait()
          else:
            remaining = endtime - time.monotonic()
            if remaining <= 0.0:
              raise queue.Full
            my_cond.wait(remaining)

          if self._waiting_writers[0] is my_cond and self._can_fit(item_bytes):
            break

        self._queue.append((item, item_bytes))
        self._byte_size += item_bytes
        self._not_empty.notify()
      finally:
        self._blocked_bytes -= item_bytes
        if self._waiting_writers:
          was_first = (self._waiting_writers[0] is my_cond)
          if was_first:
            self._waiting_writers.popleft()
          else:
            self._waiting_writers.remove(my_cond)
          self._condition_pool.append(my_cond)
          if was_first and self._waiting_writers:
            self._waiting_writers[0].notify()

  def get(self, *, block=True, timeout=None):
    """Remove and return an item from the queue.

    If the queue is empty, block until an item is available, unless `block`
    is false or a timeout occurs.

    Args:
      block: If True, block until an item is available. If False, raise
        queue.Empty immediately if the queue is empty.
      timeout: If block is True, wait for at most `timeout` seconds. If None,
        block indefinitely.

    Returns:
      The item removed from the queue.

    Raises:
      ValueError: If timeout is negative.
      queue.Empty: If the queue is empty and block is False or the timeout
        occurs.
    """
    if timeout is not None and timeout < 0:
      raise ValueError("'timeout' must be a non-negative number")

    with self._mutex:
      if not block:
        if not self._queue:
          raise queue.Empty
      elif timeout is None:
        while not self._queue:
          self._not_empty.wait()
      else:
        endtime = time.monotonic() + timeout
        while not self._queue:
          remaining = endtime - time.monotonic()
          if remaining <= 0.0:
            raise queue.Empty
          self._not_empty.wait(remaining)

      item, item_bytes = self._queue.popleft()
      self._byte_size -= item_bytes

      if self._waiting_writers:
        self._waiting_writers[0].notify()

      return item

  def get_nowait(self):
    """Remove and return an item from the queue without blocking."""
    return self.get(block=False)

  def byte_size(self):
    """Return the total byte size of elements in the queue."""
    with self._mutex:
      return self._byte_size

  def blocked_byte_size(self):
    """Return the total byte size of elements in the queue that are blocked."""
    with self._mutex:
      return self._blocked_bytes

  def qsize(self):
    """Return the total number of elements in the queue."""
    with self._mutex:
      return len(self._queue)

  def _can_fit(self, item_bytes):
    # Always let in a single element, regardless of size.
    if not self._queue:
      return True
    if self.max_elements > 0 and len(self._queue) >= self.max_elements:
      return False
    if self.max_bytes > 0 and self._byte_size + item_bytes > self.max_bytes:
      return False
    return True
