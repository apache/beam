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
from __future__ import absolute_import

import time


class Clock(object):
  def current_time(self):
    raise NotImplementedError()

  def advance_time(self):
    raise NotImplementedError()


class RealClock(object):
  def current_time(self):
    return time.time()


class TestClock(object):
  """Clock used for Testing"""
  def __init__(self, current=0):
    self._current = current

  def current_time(self):
    return self._current

  def advance_time(self, advance_by):
    self._current += advance_by
