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

"""User-facing interfaces for the Beam State and Timer APIs.

Experimental; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import itertools
import types
from builtins import object

from apache_beam.coders import Coder
from apache_beam.coders import coders
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms.timeutil import TimeDomain


class StateSpec(object):
  """Specification for a user DoFn state cell."""

  def __init__(self):
    raise NotImplementedError

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)

  def to_runner_api(self, context):
    raise NotImplementedError


class BagStateSpec(StateSpec):
  """Specification for a user DoFn bag state cell."""

  def __init__(self, name, coder):
    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    self.name = name
    self.coder = coder

  def to_runner_api(self, context):
    return beam_runner_api_pb2.StateSpec(
        bag_spec=beam_runner_api_pb2.BagStateSpec(
            element_coder_id=context.coders.get_id(self.coder)))


class CombiningValueStateSpec(StateSpec):
  """Specification for a user DoFn combining value state cell."""

  def __init__(self, name, coder, combine_fn):
    # Avoid circular import.
    from apache_beam.transforms.core import CombineFn

    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    self.name = name
    # The coder here should be for the accumulator type of the given CombineFn.
    self.coder = coder
    self.combine_fn = CombineFn.maybe_from_callable(combine_fn)

  def to_runner_api(self, context):
    return beam_runner_api_pb2.StateSpec(
        combining_spec=beam_runner_api_pb2.CombiningStateSpec(
            combine_fn=self.combine_fn.to_runner_api(context),
            accumulator_coder_id=context.coders.get_id(self.coder)))


class TimerSpec(object):
  """Specification for a user stateful DoFn timer."""

  def __init__(self, name, time_domain):
    self.name = name
    if time_domain not in (TimeDomain.WATERMARK, TimeDomain.REAL_TIME):
      raise ValueError('Unsupported TimeDomain: %r.' % (time_domain,))
    self.time_domain = time_domain
    self._attached_callback = None

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)

  def to_runner_api(self, context):
    return beam_runner_api_pb2.TimerSpec(
        time_domain=TimeDomain.to_runner_api(self.time_domain),
        timer_coder_id=context.coders.get_id(
            coders._TimerCoder(coders.SingletonCoder(None))))


def on_timer(timer_spec):
  """Decorator for timer firing DoFn method.

  This decorator allows a user to specify an on_timer processing method
  in a stateful DoFn.  Sample usage::

    class MyDoFn(DoFn):
      TIMER_SPEC = TimerSpec('timer', TimeDomain.WATERMARK)

      @on_timer(TIMER_SPEC)
      def my_timer_expiry_callback(self):
        logging.info('Timer expired!')
  """

  if not isinstance(timer_spec, TimerSpec):
    raise ValueError('@on_timer decorator expected TimerSpec.')

  def _inner(method):
    if not callable(method):
      raise ValueError('@on_timer decorator expected callable.')
    if timer_spec._attached_callback:
      raise ValueError(
          'Multiple on_timer callbacks registered for %r.' % timer_spec)
    timer_spec._attached_callback = method
    return method

  return _inner


def get_dofn_specs(dofn):
  """Gets the state and timer specs for a DoFn, if any."""

  # Avoid circular import.
  from apache_beam.runners.common import MethodWrapper
  from apache_beam.transforms.core import _DoFnParam
  from apache_beam.transforms.core import _StateDoFnParam
  from apache_beam.transforms.core import _TimerDoFnParam

  all_state_specs = set()
  all_timer_specs = set()

  # Validate params to process(), start_bundle(), finish_bundle() and to
  # any on_timer callbacks.
  for method_name in dir(dofn):
    if not isinstance(getattr(dofn, method_name, None), types.MethodType):
      continue
    method = MethodWrapper(dofn, method_name)
    param_ids = [d.param_id for d in method.defaults
                 if isinstance(d, _DoFnParam)]
    if len(param_ids) != len(set(param_ids)):
      raise ValueError(
          'DoFn %r has duplicate %s method parameters: %s.' % (
              dofn, method_name, param_ids))
    for d in method.defaults:
      if isinstance(d, _StateDoFnParam):
        all_state_specs.add(d.state_spec)
      elif isinstance(d, _TimerDoFnParam):
        all_timer_specs.add(d.timer_spec)

  return all_state_specs, all_timer_specs


def is_stateful_dofn(dofn):
  """Determines whether a given DoFn is a stateful DoFn."""

  # A Stateful DoFn is a DoFn that uses user state or timers.
  all_state_specs, all_timer_specs = get_dofn_specs(dofn)
  return bool(all_state_specs or all_timer_specs)


def validate_stateful_dofn(dofn):
  """Validates the proper specification of a stateful DoFn."""

  # Get state and timer specs.
  all_state_specs, all_timer_specs = get_dofn_specs(dofn)

  # Reject DoFns that have multiple state or timer specs with the same name.
  if len(all_state_specs) != len(set(s.name for s in all_state_specs)):
    raise ValueError(
        'DoFn %r has multiple StateSpecs with the same name: %s.' % (
            dofn, all_state_specs))
  if len(all_timer_specs) != len(set(s.name for s in all_timer_specs)):
    raise ValueError(
        'DoFn %r has multiple TimerSpecs with the same name: %s.' % (
            dofn, all_timer_specs))

  # Reject DoFns that use timer specs without corresponding timer callbacks.
  for timer_spec in all_timer_specs:
    if not timer_spec._attached_callback:
      raise ValueError(
          ('DoFn %r has a TimerSpec without an associated on_timer '
           'callback: %s.') % (dofn, timer_spec))
    method_name = timer_spec._attached_callback.__name__
    if (timer_spec._attached_callback !=
        getattr(dofn, method_name, None).__func__):
      raise ValueError(
          ('The on_timer callback for %s is not the specified .%s method '
           'for DoFn %r (perhaps it was overwritten?).') % (
               timer_spec, method_name, dofn))


class RuntimeTimer(object):
  """Timer interface object passed to user code."""

  def __init__(self, timer_spec):
    self._cleared = False
    self._new_timestamp = None

  def clear(self):
    self._cleared = True
    self._new_timestamp = None

  def set(self, timestamp):
    self._new_timestamp = timestamp


class RuntimeState(object):
  """State interface object passed to user code."""

  def __init__(self, state_spec, state_tag, current_value_accessor):
    self._state_spec = state_spec
    self._state_tag = state_tag
    self._current_value_accessor = current_value_accessor

  @staticmethod
  def for_spec(state_spec, state_tag, current_value_accessor):
    if isinstance(state_spec, BagStateSpec):
      return BagRuntimeState(state_spec, state_tag, current_value_accessor)
    elif isinstance(state_spec, CombiningValueStateSpec):
      return CombiningValueRuntimeState(state_spec, state_tag,
                                        current_value_accessor)
    else:
      raise ValueError('Invalid state spec: %s' % state_spec)

  def _encode(self, value):
    return self._state_spec.coder.encode(value)

  def _decode(self, value):
    return self._state_spec.coder.decode(value)

  def prefetch(self):
    # The default implementation here does nothing.
    pass


# Sentinel designating an unread value.
UNREAD_VALUE = object()


class BagRuntimeState(RuntimeState):
  """Bag state interface object passed to user code."""

  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(BagRuntimeState, self).__init__(
        state_spec, state_tag, current_value_accessor)
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


class CombiningValueRuntimeState(RuntimeState):
  """Combining value state interface object passed to user code."""

  def __init__(self, state_spec, state_tag, current_value_accessor):
    super(CombiningValueRuntimeState, self).__init__(
        state_spec, state_tag, current_value_accessor)
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


class UserStateContext(object):
  """Wrapper allowing user state and timers to be accessed by a DoFnInvoker."""

  def get_timer(self, timer_spec, key, window):
    raise NotImplementedError()

  def get_state(self, state_spec, key, window):
    raise NotImplementedError()

  def commit(self):
    raise NotImplementedError()
