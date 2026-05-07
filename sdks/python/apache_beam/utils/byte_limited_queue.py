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

import queue
import time
from typing import Any
from typing import Callable


class ByteLimitedQueue(queue.Queue):
  """A queue.Queue that limits by both element count and total weight.

  A single element is allowed to exceed the maxweight to avoid deadlock.
  """
  def __init__(
      self,
      weighing_fn,  # type: Callable[[Any], int]
      maxsize=0,  # type: int
      maxweight=0,  # type: int
  ):
    # type: (...) -> None

    """Initializes a ByteLimitedQueue.

    Args:
      weighing_fn: A Callable that accepts an item and returns its integer
        weight.
      maxsize: The maximum number of items allowed in the queue. If 0 or
        negative, there is no limit on the number of elements.
      maxweight: The maximum accumulated weight allowed in the queue.
    """
    super().__init__(maxsize=0)
    self.max_elements = maxsize
    self.max_weight = maxweight
    self.weighing_fn = weighing_fn
    self._byte_size = 0

  def _is_full(self, item_size):
    if self._qsize() == 0:
      return False
    if self.max_elements > 0 and self._qsize() >= self.max_elements:
      return True
    if self.max_weight > 0 and self._byte_size + item_size > self.max_weight:
      return True
    return False

  def put(self, item, block=True, timeout=None):
    item_size = max(1, self.weighing_fn(item))
    with self.not_full:
      if not block:
        if self._is_full(item_size):
          raise queue.Full
      elif timeout is None:
        while self._is_full(item_size):
          self.not_full.wait()
      elif timeout < 0:
        raise ValueError("'timeout' must be a non-negative number")
      else:
        endtime = time.time() + timeout
        while self._is_full(item_size):
          remaining = endtime - time.time()
          if remaining <= 0.0:
            raise queue.Full
          self.not_full.wait(remaining)

      self._put((item, item_size))
      self._byte_size += item_size
      self.unfinished_tasks += 1
      self.not_empty.notify()

  def _get(self):
    item, item_weight = super()._get()
    self._byte_size -= item_weight
    return item

  def byte_size(self):
    """Return the total byte weight of elements in the queue."""
    with self.mutex:
      return self._byte_size
