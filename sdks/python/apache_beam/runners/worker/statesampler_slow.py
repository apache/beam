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


class StateSampler(object):

  def __init__(self, sampling_period_ms):
    self._state_stack = [ScopedState(None, self, None)]
    self.state_transition_count = 0
    self.time_since_transition = 0
    self.started = False
    self.finished = False

  def current_state(self):
    """Returns the current execution state.

    This operation is not thread safe, and should only be called from the
    execution thread."""
    return self._state_stack[-1]

  def _scoped_state(self,
                    counter_name,
                    output_counter,
                    metrics_container=None):
    return ScopedState(self, counter_name, output_counter, metrics_container)

  def _enter_state(self, state):
    self.state_transition_count += 1
    self._state_stack.append(state)

  def _exit_state(self):
    self.state_transition_count += 1
    self._state_stack.pop()

  def start(self):
    # Sampling not yet supported. Only state tracking at the moment.
    pass

  def stop(self):
    self.finished = True


class ScopedState(object):

  def __init__(self, sampler, name, counter=None, metrics_container=None):
    self.state_sampler = sampler
    self.name = name
    self.counter = counter
    self.nsecs = 0
    self.metrics_container = metrics_container

  def sampled_seconds(self):
    return 1e-9 * self.nsecs

  def __repr__(self):
    return "ScopedState[%s, %s]" % (self.name, self.nsecs)

  def __enter__(self):
    self.state_sampler._enter_state(self)

  def __exit__(self, exc_type, exc_value, traceback):
    self.state_sampler._exit_state()
