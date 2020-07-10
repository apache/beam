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

from apache_beam.runners.interactive import interactive_environment as ie


class Limiter:
  """Limits an aspect of the caching layer."""
  def is_triggered(self):
    # type: () -> bool

    """Returns True if the limiter has triggered, and caching should stop."""
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
