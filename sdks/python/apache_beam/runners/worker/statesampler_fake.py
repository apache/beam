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

# This module is experimental. No backwards-compatibility guarantees.

from apache_beam.utils.counters import CounterName


class StateSampler(object):

  def __init__(self, *args, **kwargs):
    self._current_state = _FakeScopedState(self, 'unknown', 'unknown')

  def scoped_state(self, step_name, state_name=None, io_target=None):
    return _FakeScopedState(self, step_name, state_name)

  def current_state(self):
    return self._current_state

  def start(self):
    pass

  def stop(self):
    pass

  def stop_if_still_running(self):
    self.stop()

  def commit_counters(self):
    pass


class _FakeScopedState(object):

  def __init__(self, sampler, step_name, state_name):
    self.name = CounterName(state_name + '-msecs',
                            step_name=step_name)
    self.sampler = sampler

  def __enter__(self):
    self.old_state = self.sampler.current_state()
    self.sampler._current_state = self

  def __exit__(self, *unused_args):
    self.sampler._current_state = self.old_state

  def sampled_seconds(self):
    return 0
