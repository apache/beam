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

from __future__ import absolute_import

import threading
from collections import namedtuple

from apache_beam.runners import common
from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName

try:
  from apache_beam.runners.worker import statesampler_fast as statesampler_impl
  FAST_SAMPLER = True
except ImportError:
  from apache_beam.runners.worker import statesampler_slow as statesampler_impl
  FAST_SAMPLER = False


_STATE_SAMPLERS = threading.local()


def set_current_tracker(tracker):
  _STATE_SAMPLERS.tracker = tracker


def get_current_tracker():
  try:
    return _STATE_SAMPLERS.tracker
  except AttributeError:
    return None


def for_test():
  set_current_tracker(StateSampler('test', CounterFactory()))
  return get_current_tracker()


StateSamplerInfo = namedtuple(
    'StateSamplerInfo',
    ['state_name',
     'transition_count',
     'time_since_transition',
     'tracked_thread'])


# Default period for sampling current state of pipeline execution.
DEFAULT_SAMPLING_PERIOD_MS = 200


class StateSampler(statesampler_impl.StateSampler):

  def __init__(self, prefix, counter_factory,
               sampling_period_ms=DEFAULT_SAMPLING_PERIOD_MS):
    self.states_by_name = {}
    self._prefix = prefix
    self._counter_factory = counter_factory
    self._states_by_name = {}
    self.sampling_period_ms = sampling_period_ms
    self.tracked_thread = None
    self.finished = False
    self.started = False
    super(StateSampler, self).__init__(sampling_period_ms)

  @property
  def stage_name(self):
    return self._prefix

  def stop(self):
    set_current_tracker(None)
    super(StateSampler, self).stop()

  def stop_if_still_running(self):
    if self.started and not self.finished:
      self.stop()

  def start(self):
    self.tracked_thread = threading.current_thread()
    set_current_tracker(self)
    super(StateSampler, self).start()
    self.started = True

  def get_info(self):
    """Returns StateSamplerInfo with transition statistics."""
    return StateSamplerInfo(
        self.current_state().name,
        self.state_transition_count,
        self.time_since_transition,
        self.tracked_thread)

  def scoped_state(self,
                   name_context,
                   state_name,
                   io_target=None,
                   metrics_container=None):
    """Returns a ScopedState object associated to a Step and a State.

    Args:
      name_context: common.NameContext. It is the step name information.
      state_name: str. It is the state name (e.g. process / start / finish).
      io_target:
      metrics_container: MetricsContainer. The step's metrics container.

    Returns:
      A ScopedState that keeps the execution context and is able to switch it
      for the execution thread.
    """
    if not isinstance(name_context, common.NameContext):
      name_context = common.NameContext(name_context)

    counter_name = CounterName(state_name + '-msecs',
                               stage_name=self._prefix,
                               step_name=name_context.metrics_name(),
                               io_target=io_target)
    if counter_name in self._states_by_name:
      return self._states_by_name[counter_name]
    else:
      output_counter = self._counter_factory.get_counter(counter_name,
                                                         Counter.SUM)
      self._states_by_name[counter_name] = super(
          StateSampler, self)._scoped_state(counter_name,
                                            name_context,
                                            output_counter,
                                            metrics_container)
      return self._states_by_name[counter_name]

  def commit_counters(self):
    """Updates output counters with latest state statistics."""
    for state in self._states_by_name.values():
      state_msecs = int(1e-6 * state.nsecs)
      state.counter.update(state_msecs - state.counter.value())
