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

from apache_beam.transforms import userstate
from apache_beam.transforms.trigger import _ListStateTag


class DirectUserStateContext(userstate.UserStateContext):
  """userstate.UserStateContext for the BundleBasedDirectRunner.

  The DirectUserStateContext buffers up updates that are to be committed
  by the TransformEvaluator after running a DoFn.
  """

  def __init__(self, step_context, dofn, key_coder):
    self.step_context = step_context
    self.dofn = dofn
    self.key_coder = key_coder

    self.all_state_specs, self.all_timer_specs = (
        userstate.get_dofn_specs(dofn))
    self.state_tags = {}
    for state_spec in self.all_state_specs:
      state_key = 'user/%s' % state_spec.name
      if isinstance(state_spec, userstate.BagStateSpec):
        state_tag = _ListStateTag(state_key)
      elif isinstance(state_spec, userstate.CombiningValueStateSpec):
        state_tag = _ListStateTag(state_key)
      else:
        raise ValueError('Invalid state spec: %s' % state_spec)
      self.state_tags[state_spec] = state_tag

    self.cached_states = {}
    self.cached_timers = {}

  def get_timer(self, timer_spec, key, window):
    assert timer_spec in self.all_timer_specs
    encoded_key = self.key_coder.encode(key)
    cache_key = (encoded_key, window, timer_spec)
    if cache_key not in self.cached_timers:
      self.cached_timers[cache_key] = userstate.RuntimeTimer(timer_spec)
    return self.cached_timers[cache_key]

  def get_state(self, state_spec, key, window):
    assert state_spec in self.all_state_specs
    encoded_key = self.key_coder.encode(key)
    cache_key = (encoded_key, window, state_spec)
    if cache_key not in self.cached_states:
      state_tag = self.state_tags[state_spec]
      value_accessor = (
          lambda: self._get_underlying_state(state_spec, key, window))
      self.cached_states[cache_key] = userstate.RuntimeState.for_spec(
          state_spec, state_tag, value_accessor)
    return self.cached_states[cache_key]

  def _get_underlying_state(self, state_spec, key, window):
    state_tag = self.state_tags[state_spec]
    encoded_key = self.key_coder.encode(key)
    return (self.step_context.get_keyed_state(encoded_key)
            .get_state(window, state_tag))

  def commit(self):
    # Commit state modifications.
    for cache_key, runtime_state in self.cached_states.items():
      encoded_key, window, state_spec = cache_key
      state = self.step_context.get_keyed_state(encoded_key)
      state_tag = self.state_tags[state_spec]
      if isinstance(state_spec, userstate.BagStateSpec):
        if runtime_state._cleared:
          state.clear_state(window, state_tag)
        for new_value in runtime_state._new_values:
          state.add_state(window, state_tag, new_value)
      elif isinstance(state_spec, userstate.CombiningValueStateSpec):
        if runtime_state._modified:
          state.clear_state(window, state_tag)
          state.add_state(
              window, state_tag,
              state_spec.coder.encode(runtime_state._current_accumulator))
      else:
        raise ValueError('Invalid state spec: %s' % state_spec)

    # Commit new timers.
    for cache_key, runtime_timer in self.cached_timers.items():
      encoded_key, window, timer_spec = cache_key
      state = self.step_context.get_keyed_state(encoded_key)
      timer_name = 'user/%s' % timer_spec.name
      if runtime_timer._cleared:
        state.clear_timer(window, timer_name, timer_spec.time_domain)
      if runtime_timer._new_timestamp is not None:
        # TODO(ccy): add corresponding watermark holds after the DirectRunner
        # allows for keyed watermark holds.
        state.set_timer(window, timer_name, timer_spec.time_domain,
                        runtime_timer._new_timestamp)
