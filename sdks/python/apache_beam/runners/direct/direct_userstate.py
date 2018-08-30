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

"""Support for user state in the BundleBasedDirectRunner."""
from __future__ import absolute_import

import unittest

import mock

import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import RuntimeState
from apache_beam.transforms.userstate import RuntimeTimer
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import UserStateContext
from apache_beam.transforms.userstate import UserStateUtils
from apache_beam.transforms.trigger import _CombiningValueStateTag
from apache_beam.transforms.trigger import _ListStateTag


class DirectUserStateContext(UserStateContext):
  """UserStateContext for the BundleBasedDirectRunner.

  The DirectUserStateContext buffers up updates that are to be committed by the
  TransformEvaluator after running a DoFn.
  """

  def __init__(self, state, dofn):
    # The underlying apache_beam.transforms.trigger.UnmergedState object for
    # the given key.
    self.state = state

    self.all_state_specs, self.all_timer_specs = (
        UserStateUtils.get_dofn_specs(dofn))
    self.state_tags = {}
    for state_spec in self.all_state_specs:
      state_key = 'user/%s' % state_spec.name
      if isinstance(state_spec, BagStateSpec):
        state_tag = _ListStateTag(state_key)
      elif isinstance(state_spec, CombiningValueStateSpec):
        state_tag = _CombiningValueStateTag(state_key, state_spec.combine_fn)
      else:
        raise ValueError('Invalid state spec: %s' % state_spec)
      self.state_tags[state_spec] = state_tag

    self.cached_states = {}
    self.cached_timers = {}

  def get_timer(self, timer_spec, window):
    assert timer_spec in self.all_timer_specs
    if (window, timer_spec) not in self.cached_timers:
      self.cached_timers[(window, timer_spec)] = RuntimeTimer(timer_spec)
    return self.cached_timers[(window, timer_spec)]

  def get_state(self, state_spec, window):
    assert state_spec in self.all_state_specs
    if (window, state_spec) not in self.cached_states:
      state_tag = self.state_tags[state_spec]
      value_accessor = lambda: self._get_underlying_state(state_spec, window)
      self.cached_states[(window, state_spec)] = (
          RuntimeState.for_spec(state_spec, state_tag, value_accessor))
    return self.cached_states[(window, state_spec)]

  def _get_underlying_state(self, state_spec, window):
    state_tag = self.state_tags[state_spec]
    return self.state.get_state(window, state_tag)

  def commit(self):
    # Commit state modifications.
    for (window, state_spec), runtime_state in self.cached_states.items():
      state_tag = self.state_tags[state_spec]
      if isinstance(state_spec, BagStateSpec):
        if runtime_state._cleared:
          self.state.clear_state(window, state_tag)
        for new_value in runtime_state._new_values:
          self.state.add_state(window, state_tag, new_value)
      elif isinstance(state_spec, CombiningValueStateSpec):
        if runtime_state._modified:
          self.state.clear_state(window, state_tag)
          self.state.add_state(window, state_tag,
                               runtime_state._current_accumulator)
      else:
        raise ValueError('Invalid state spec: %s' % state_spec)

    # Commit new timers.
    for (window, timer_spec), runtime_timer in self.cached_timers.items():
      timer_name = 'user/%s' % timer_spec.name
      if runtime_timer._cleared:
        self.state.clear_timer(window, timer_name, timer_spec.time_domain)
      if runtime_timer._new_timestamp is not None:
        # TODO(ccy): add corresponding watermark holds after the DirectRunner
        # allows for keyed watermark holds.
        self.state.set_timer(window, timer_name, timer_spec.time_domain,
                             runtime_timer._new_timestamp)
