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

"""Module to condition how Interactive Beam stops capturing data.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import threading

from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.runners.interactive import interactive_environment as ie


class Limiter:
  """Limits an aspect of the caching layer."""
  def is_triggered(self):
    # type: () -> bool

    """Returns True if the limiter has triggered, and caching should stop."""
    raise NotImplementedError


class ElementLimiter(Limiter):
  """A `Limiter` that limits reading from cache based on some property of an
  element.
  """
  def update(self, e):
    # type: (Any) -> None

    """Update the internal state based on some property of an element.

    This is executed on every element that is read from cache.
    """
    raise NotImplementedError


class SizeLimiter(Limiter):
  """Limits the cache size to a specified byte limit."""
  def __init__(
      self,
      size_limit  # type: int
  ):
    self._size_limit = size_limit

  def is_triggered(self):
    total_capture_size = 0
    ie.current_env().track_user_pipelines()
    for user_pipeline in ie.current_env().tracked_user_pipelines:
      cache_manager = ie.current_env().get_cache_manager(user_pipeline)
      if hasattr(cache_manager, 'capture_size'):
        total_capture_size += cache_manager.capture_size
    return total_capture_size >= self._size_limit


class DurationLimiter(Limiter):
  """Limits the duration of the capture."""
  def __init__(
      self,
      duration_limit  # type: datetime.timedelta
  ):
    self._duration_limit = duration_limit
    self._timer = threading.Timer(duration_limit.total_seconds(), self._trigger)
    self._timer.daemon = True
    self._triggered = False
    self._timer.start()

  def _trigger(self):
    self._triggered = True

  def is_triggered(self):
    return self._triggered


class CountLimiter(ElementLimiter):
  """Limits by counting the number of elements seen."""
  def __init__(self, max_count):
    self._max_count = max_count
    self._count = 0

  def update(self, e):
    # A TestStreamFileRecord can contain many elements at once. If e is a file
    # record, then count the number of elements in the bundle.
    if isinstance(e, TestStreamFileRecord):
      if not e.recorded_event.element_event:
        return
      self._count += len(e.recorded_event.element_event.elements)

    # Otherwise, count everything else but the header of the file since it is
    # not an element.
    elif not isinstance(e, TestStreamFileHeader):
      self._count += 1

  def is_triggered(self):
    return self._count >= self._max_count


class ProcessingTimeLimiter(ElementLimiter):
  """Limits by how long the ProcessingTime passed in the element stream.

  This measures the duration from the first element in the stream. Each
  subsequent element has a delta "advance_duration" that moves the internal
  clock forward. This triggers when the duration from the internal clock and
  the start exceeds the given duration.
  """
  def __init__(self, max_duration_secs):
    """Initialize the ProcessingTimeLimiter."""
    self._max_duration_us = max_duration_secs * 1e6
    self._start_us = 0
    self._cur_time_us = 0

  def update(self, e):
    # Only look at TestStreamFileRecords which hold the processing time.
    if not isinstance(e, TestStreamFileRecord):
      return

    if not e.recorded_event.processing_time_event:
      return

    if self._start_us == 0:
      self._start_us = e.recorded_event.processing_time_event.advance_duration
    self._cur_time_us += e.recorded_event.processing_time_event.advance_duration

  def is_triggered(self):
    return self._cur_time_us - self._start_us >= self._max_duration_us
