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

"""Clock implementations for real time processing and testing."""

from __future__ import absolute_import

import time


class Clock(object):

  @property
  def now(self):
    """Returns the number of milliseconds since epoch."""
    return int(time.time() * 1000)


class MockClock(Clock):
  """Mock clock implementation for testing."""

  def __init__(self, now_in_ms):
    self._now_in_ms = now_in_ms

  @property
  def now(self):
    return self._now_in_ms

  @now.setter
  def now(self, value_in_ms):
    assert value_in_ms >= self._now_in_ms
    self._now_in_ms = value_in_ms

  def advance(self, duration_in_ms):
    assert duration_in_ms >= 0
    self._now_in_ms += duration_in_ms
