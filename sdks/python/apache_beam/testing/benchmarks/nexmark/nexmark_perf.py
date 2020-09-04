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

"""
performance summary for a run of nexmark query
"""


class NexmarkPerf(object):
  def __init__(
      self,
      runtime_sec=None,
      event_count=None,
      event_per_sec=None,
      result_count=None):
    self.runtime_sec = runtime_sec if runtime_sec else -1.0
    self.event_count = event_count if event_count else -1
    self.event_per_sec = event_per_sec if event_per_sec else -1.0
    self.result_count = result_count if result_count else -1

  def has_progress(self, previous_perf):
    # type: (NexmarkPerf) -> bool

    """
    Args:
      previous_perf: a NexmarkPerf object to be compared to self

    Returns:
      True if there are activity between self and other NexmarkPerf values
    """
    if self.runtime_sec != previous_perf.runtime_sec or\
       self.event_count != previous_perf.event_count or\
       self.result_count != previous_perf.result_count:
      return True
    return False
