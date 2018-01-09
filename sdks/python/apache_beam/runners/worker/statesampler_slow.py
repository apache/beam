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
import threading
from collections import namedtuple

from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterName


class ExecutionStateSamplers(threading.local):
  """ Per-thread state sampler. """
  def __init__(self):
    super(ExecutionStateSamplers, self).__init__()
    self._current_sampler = None

  def current_sampler(self):
    return self._current_sampler

  def set_sampler(self, sampler):
    self._current_sampler = sampler


EXECUTION_STATE_SAMPLERS = ExecutionStateSamplers()


StateSamplerInfo = namedtuple(
    'StateSamplerInfo',
    ['state_name', 'transition_count', 'time_since_transition'])


# Default period for sampling current state of pipeline execution.
DEFAULT_SAMPLING_PERIOD_MS = 200


class StateSampler(object):

  def __init__(self, prefix, counter_factory,
               sampling_period_ms=DEFAULT_SAMPLING_PERIOD_MS):
    self._prefix = prefix
    self._state_stack = [ScopedState(None, self, None)]
    self.states_by_name = {}
    self.transition_count = 0
    self._counter_factory = counter_factory

  def current_state(self):
    """Returns the current execution state."""
    return self._state_stack[-1]

  def scoped_state(self, step_name, state_name, io_target=None):
    counter_name = CounterName(state_name + '-msecs',
                               stage_name=self._prefix,
                               step_name=step_name,
                               io_target=io_target)
    if counter_name in self.states_by_name:
      return self.states_by_name[counter_name]
    else:
      output_counter = self._counter_factory.get_counter(counter_name,
                                                         Counter.SUM)
      self.states_by_name[counter_name] = ScopedState(
          counter_name, self, output_counter)
      return self.states_by_name[counter_name]

  def _enter_state(self, state):
    self.transition_count += 1
    self._state_stack.append(state)

  def _exit_state(self):
    self.transition_count += 1
    self._state_stack.pop()

  def start(self):
    # Sampling not yet supported. Only state tracking at the moment.
    pass

  def stop(self):
    pass

  def stop_if_still_running(self):
    self.stop()

  def get_info(self):
    """Returns StateSamplerInfo with transition statistics."""
    return StateSamplerInfo(
        self.current_state().name, self.transition_count, 0)

  def commit_counters(self):
    for state in self.states_by_name.values():
      state_msecs = state.msecs
      state.counter.update(state_msecs - state.counter.value())


class ScopedState(object):

  def __init__(self, state_name, state_sampler, counter):
    self.name = state_name
    self.state_sampler = state_sampler
    self.counter = counter
    self.msecs = 0

  def enter(self):
    self.state_sampler._enter_state(self)

  def exit(self):
    self.state_sampler._exit_state()

  def sampled_seconds(self):
    return 1e-3 * self.msecs

  def __enter__(self):
    return self.enter()

  def __exit__(self, exc_type, exc_value, traceback):
    return self.exit()
