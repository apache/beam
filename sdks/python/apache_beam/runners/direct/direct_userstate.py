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
# pytype: skip-file

from __future__ import absolute_import

import itertools

from apache_beam.transforms import userstate
from apache_beam.transforms.trigger import _ListStateTag
from apache_beam.transforms.trigger import _ReadModifyWriteStateTag
from apache_beam.transforms.trigger import _SetStateTag


class DirectRuntimeState(userstate.RuntimeState):
  def __init__(self, state_spec, state_tag, current_value_accessor):
    self._state_spec = state_spec
    self._state_tag = state_tag
    self._current_value_accessor = current_value_accessor

  @staticmethod
  def for_spec(state_spec, state_tag, current_value_accessor):
    if isinstance(state_spec, userstate.ReadModifyWriteStateSpec):
      return ReadModifyWriteRuntimeState(
          state_spec, state_tag, current_value_accessor)
    elif isinstance(state_spec, userstate.BagStateSpec):
      return BagRuntimeState(state_spec, state_tag, current_value_accessor)
    elif isinstance(state_spec, userstate.CombiningValueStateSpec):
      return CombiningValueRuntimeState(
          state_spec, state_tag, current_value_accessor)
    elif isinstance(state_spec, userstate.SetStateSpec):
      return SetRuntimeState(state_spec, state_tag, current_value_accessor)
    else:
      raise ValueError('Invalid state spec: %s' % state_spec)

  def _encode(self, value):
    return self._state_spec.coder.encode(value)

  def _decode(self, value):
    return self._state_spec.coder.decode(value)


# Sentinel designating an unread value.
UNREAD_VALUE = object()


class ReadModifyWriteRuntimeState(DirectRuntimeState,
                                  userstate.ReadModifyWriteRuntimeState):
  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(ReadModifyWriteRuntimeState,
          self).__init__(state_spec, state_tag, current_value_accessor)
    self._value = UNREAD_VALUE
    self._cleared = False
    self._modified = False

  def read(self):
    if self._cleared:
      return None
    if self._value is UNREAD_VALUE:
      self._value = self._current_value_accessor()
    if not self._value:
      return None
    return self._decode(self._value[0])

  def write(self, value):
    self._cleared = False
    self._modified = True
    self._value = [self._encode(value)]

  def clear(self):
    self._cleared = True
    self._modified = False
    self._value = []

  def is_cleared(self):
    return self._cleared

  def is_modified(self):
    return self._modified


class BagRuntimeState(DirectRuntimeState, userstate.BagRuntimeState):
  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(BagRuntimeState,
          self).__init__(state_spec, state_tag, current_value_accessor)
    self._cached_value = UNREAD_VALUE
    self._cleared = False
    self._new_values = []

  def read(self):
    if self._cached_value is UNREAD_VALUE:
      self._cached_value = self._current_value_accessor()
    if not self._cleared:
      encoded_values = itertools.chain(self._cached_value, self._new_values)
    else:
      encoded_values = self._new_values
    return (self._decode(v) for v in encoded_values)

  def add(self, value):
    self._new_values.append(self._encode(value))

  def clear(self):
    self._cleared = True
    self._cached_value = []
    self._new_values = []


class SetRuntimeState(DirectRuntimeState, userstate.SetRuntimeState):
  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(SetRuntimeState,
          self).__init__(state_spec, state_tag, current_value_accessor)
    self._current_accumulator = UNREAD_VALUE
    self._modified = False

  def _read_initial_value(self):
    if self._current_accumulator is UNREAD_VALUE:
      self._current_accumulator = {
          self._decode(a)
          for a in self._current_value_accessor()
      }

  def read(self):
    self._read_initial_value()
    return self._current_accumulator

  def add(self, value):
    self._read_initial_value()
    self._modified = True
    self._current_accumulator.add(value)

  def clear(self):
    self._current_accumulator = set()
    self._modified = True

  def is_modified(self):
    return self._modified


class CombiningValueRuntimeState(DirectRuntimeState,
                                 userstate.CombiningValueRuntimeState):
  """Combining value state interface object passed to user code."""
  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(CombiningValueRuntimeState,
          self).__init__(state_spec, state_tag, current_value_accessor)
    self._current_accumulator = UNREAD_VALUE
    self._modified = False
    self._combine_fn = state_spec.combine_fn

  def _read_initial_value(self):
    if self._current_accumulator is UNREAD_VALUE:
      existing_accumulators = list(
          self._decode(a) for a in self._current_value_accessor())
      if existing_accumulators:
        self._current_accumulator = self._combine_fn.merge_accumulators(
            existing_accumulators)
      else:
        self._current_accumulator = self._combine_fn.create_accumulator()

  def read(self):
    self._read_initial_value()
    return self._combine_fn.extract_output(self._current_accumulator)

  def add(self, value):
    self._read_initial_value()
    self._modified = True
    self._current_accumulator = self._combine_fn.add_input(
        self._current_accumulator, value)

  def clear(self):
    self._modified = True
    self._current_accumulator = self._combine_fn.create_accumulator()


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
      if isinstance(state_spec, userstate.ReadModifyWriteStateSpec):
        state_tag = _ReadModifyWriteStateTag(state_key)
      elif isinstance(state_spec, userstate.BagStateSpec):
        state_tag = _ListStateTag(state_key)
      elif isinstance(state_spec, userstate.CombiningValueStateSpec):
        state_tag = _ListStateTag(state_key)
      elif isinstance(state_spec, userstate.SetStateSpec):
        state_tag = _SetStateTag(state_key)
      else:
        raise ValueError('Invalid state spec: %s' % state_spec)
      self.state_tags[state_spec] = state_tag

    self.cached_states = {}
    self.cached_timers = {}

  def get_timer(self, timer_spec, key, window, timestamp, pane):
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
      self.cached_states[cache_key] = DirectRuntimeState.for_spec(
          state_spec, state_tag, value_accessor)
    return self.cached_states[cache_key]

  def _get_underlying_state(self, state_spec, key, window):
    state_tag = self.state_tags[state_spec]
    encoded_key = self.key_coder.encode(key)
    return (
        self.step_context.get_keyed_state(encoded_key).get_state(
            window, state_tag))

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
              window,
              state_tag,
              state_spec.coder.encode(runtime_state._current_accumulator))
      elif isinstance(state_spec, userstate.SetStateSpec):
        if runtime_state.is_modified():
          state.clear_state(window, state_tag)
          for new_value in runtime_state._current_accumulator:
            state.add_state(
                window, state_tag, state_spec.coder.encode(new_value))
      elif isinstance(state_spec, userstate.ReadModifyWriteStateSpec):
        if runtime_state.is_cleared():
          state.clear_state(window, state_tag)
        if runtime_state.is_modified():
          state.clear_state(window, state_tag)
          state.add_state(window, state_tag, runtime_state._value)
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
        state.set_timer(
            window,
            timer_name,
            timer_spec.time_domain,
            runtime_timer._new_timestamp)
