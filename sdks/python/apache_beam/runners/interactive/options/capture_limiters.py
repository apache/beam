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
  """A `Limiter` that limits based on some property of an element."""
  def update(self, e):
    # type: (Any) -> None
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
  def __init__(self, max_count):
    self._max_count = max_count
    self._count = 0

  def update(self, e):
    if isinstance(e, TestStreamFileRecord):
      if not e.recorded_event.element_event:
        return
      self._count += len(e.recorded_event.element_event.elements)
    elif not isinstance(e, TestStreamFileHeader):
      self._count += 1
    # print(e, self._count)

  def is_triggered(self):
    return self._count >= self._max_count


class ProcessingTimeLimiter(ElementLimiter):
  def __init__(self, duration, start=None):
    self._duration = duration * 1e6
    self._start = start * 1e6 if start else None
    self._cur_time = 0

  def update(self, e):
    if isinstance(e, TestStreamFileHeader):
      return

    if not isinstance(e, TestStreamFileRecord):
      return

    if not e.recorded_event.processing_time_event:
      return

    if self._start == None:
      self._start = e.recorded_event.processing_time_event.advance_duration
    self._cur_time += e.recorded_event.processing_time_event.advance_duration
    print('self._cur_time', self._cur_time)
    print('self._start', self._start)

  def is_triggered(self):
    start = self._start if self._start else 0
    print(1e-6 * (self._cur_time - start), self._duration * 1e-6)
    return self._cur_time - start >= self._duration


class PipelineTerminatedLimiter(ElementLimiter):
  def __init__(self, pipeline):
    self._pipeline = pipeline

  def update(self, _):
    pass

  def is_triggered(self):
    if ie.current_env().is_terminated(self._pipeline):
      print('pipeline is terminated')
    return ie.current_env().is_terminated(self._pipeline)
