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

"""Clock implementations for real time processing and testing.

For internal use only. No backwards compatibility guarantees.
"""
# pytype: skip-file

from __future__ import absolute_import

import time
from builtins import object

from apache_beam.utils.timestamp import Timestamp


class Clock(object):
  def time(self):
    """Returns the number of seconds since epoch."""
    raise NotImplementedError()

  def advance_time(self, advance_by):
    """Advances the clock by a number of seconds."""
    raise NotImplementedError()


class RealClock(object):
  def time(self):
    return time.time()


class TestClock(object):
  """Clock used for Testing"""
  def __init__(self, current_time=None):
    self._current_time = current_time if current_time else Timestamp()

  def time(self):
    return float(self._current_time)

  def advance_time(self, advance_by):
    self._current_time += advance_by
