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

# pytype: skip-file

from __future__ import absolute_import

import types
from builtins import object
from collections import namedtuple
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar

from apache_beam.coders import Coder
from apache_beam.coders import coders
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms.timeutil import TimeDomain

if TYPE_CHECKING:
  from apache_beam.runners.pipeline_context import PipelineContext
  from apache_beam.transforms.core import CombineFn

CallableT = TypeVar('CallableT', bound=Callable)


class StateSpec(object):
  """Specification for a user DoFn state cell."""
  def __init__(self, name: str, coder: Coder) -> None:
    if not isinstance(name, str):
      raise TypeError("name is not a string")
    if not isinstance(coder, Coder):
      raise TypeError("coder is not of type Coder")
    self.name = name
    self.coder = coder

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)

  def to_runner_api(self, context):
    raise NotImplementedError


class ReadModifyWriteStateSpec(StateSpec):
  """Specification for a user DoFn value state cell."""
  def to_runner_api(self, context: PipelineContext) -> beam_runner_api_pb2.StateSpec:
    return beam_runner_api_pb2.StateSpec(
        read_modify_write_spec=beam_runner_api_pb2.ReadModifyWriteStateSpec(
            coder_id=context.coders.get_id(self.coder)))


class BagStateSpec(StateSpec):
  """Specification for a user DoFn bag state cell."""
  def to_runner_api(self, context: PipelineContext) -> beam_runner_api_pb2.StateSpec:
    return beam_runner_api_pb2.StateSpec(
        bag_spec=beam_runner_api_pb2.BagStateSpec(
            element_coder_id=context.coders.get_id(self.coder)))


class SetStateSpec(StateSpec):
  """Specification for a user DoFn Set State cell"""
  def to_runner_api(self, context):
    return beam_runner_api_pb2.StateSpec(
        set_spec=beam_runner_api_pb2.SetStateSpec(
            element_coder_id=context.coders.get_id(self.coder)))


class CombiningValueStateSpec(StateSpec):
  """Specification for a user DoFn combining value state cell."""
  def __init__(self, name: str, coder: Optional[Coder] = None, combine_fn: Any = None) -> None:

    """Initialize the specification for CombiningValue state.

    CombiningValueStateSpec(name, combine_fn) -> Coder-inferred combining value
      state spec.
    CombiningValueStateSpec(name, coder, combine_fn) -> Combining value state
      spec with coder and combine_fn specified.

    Args:
      name (str): The name by which the state is identified.
      coder (Coder): Coder specifying how to encode the values to be combined.
        May be inferred.
      combine_fn (``CombineFn`` or ``callable``): Function specifying how to
        combine the values passed to state.
    """
    # Avoid circular import.
    from apache_beam.transforms.core import CombineFn
    # We want the coder to be optional, but unfortunately it comes
    # before the non-optional combine_fn parameter, which we can't
    # change for backwards compatibility reasons.
    #
    # Instead, allow it to be omitted (by either passing two arguments
    # or combine_fn by keyword.)
    if combine_fn is None:
      if coder is None:
        raise ValueError('combine_fn must be provided')
      else:
        coder, combine_fn = None, coder
    self.combine_fn = CombineFn.maybe_from_callable(combine_fn)
    # The coder here should be for the accumulator type of the given CombineFn.
    if coder is None:
      coder = self.combine_fn.get_accumulator_coder()

    super(CombiningValueStateSpec, self).__init__(name, coder)

  def to_runner_api(self, context: PipelineContext) -> beam_runner_api_pb2.StateSpec:
    return beam_runner_api_pb2.StateSpec(
        combining_spec=beam_runner_api_pb2.CombiningStateSpec(
            combine_fn=self.combine_fn.to_runner_api(context),
            accumulator_coder_id=context.coders.get_id(self.coder)))


# TODO(BEAM-9562): Update Timer to have of() and clear() APIs.
Timer = namedtuple(
    'Timer',
    'user_key '
    'dynamic_timer_tag '
    'windows '
    'clear_bit '
    'fire_timestamp '
    'hold_timestamp '
    'paneinfo')


# TODO(BEAM-9562): Plumb through actual key_coder and window_coder.
class TimerSpec(object):
  """Specification for a user stateful DoFn timer."""
  prefix = "ts-"

  def __init__(self, name, time_domain):
    self.name = self.prefix + name
    if time_domain not in (TimeDomain.WATERMARK, TimeDomain.REAL_TIME):
      raise ValueError('Unsupported TimeDomain: %r.' % (time_domain, ))
    self.time_domain = time_domain
    self._attached_callback = None

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)

  def to_runner_api(self, context: PipelineContext, key_coder: Coder, window_coder: Coder) -> beam_runner_api_pb2.TimerFamilySpec:
    return beam_runner_api_pb2.TimerFamilySpec(
        time_domain=TimeDomain.to_runner_api(self.time_domain),
        timer_family_coder_id=context.coders.get_id(
            coders._TimerCoder(key_coder, window_coder)))


# TODO(BEAM-9602): Provide support for dynamic timer.
class TimerFamilySpec(object):
  prefix = "tfs-"

  def __init__(self, name, time_domain):
    self.name = self.prefix + name
    if time_domain not in (TimeDomain.WATERMARK, TimeDomain.REAL_TIME):
      raise ValueError('Unsupported TimeDomain: %r.' % (time_domain, ))
    self.time_domain = time_domain

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)

  def to_runner_api(self, context, key_coder, window_coder):
    return beam_runner_api_pb2.TimerFamilySpec(
        time_domain=TimeDomain.to_runner_api(self.time_domain),
        timer_family_coder_id=context.coders.get_id(
            coders._TimerCoder(key_coder, window_coder)))


def on_timer(timer_spec: TimerSpec) -> Callable[[CallableT], CallableT]:

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


def get_dofn_specs(dofn) -> Tuple[Set[StateSpec], Set[TimerSpec]]:

  """Gets the state and timer specs for a DoFn, if any.

  Args:
    dofn (apache_beam.transforms.core.DoFn): The DoFn instance to introspect for
      timer and state specs.
  """

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
    param_ids = [
        d.param_id for d in method.defaults if isinstance(d, _DoFnParam)
    ]
    if len(param_ids) != len(set(param_ids)):
      raise ValueError(
          'DoFn %r has duplicate %s method parameters: %s.' %
          (dofn, method_name, param_ids))
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
        'DoFn %r has multiple StateSpecs with the same name: %s.' %
        (dofn, all_state_specs))
  if len(all_timer_specs) != len(set(s.name for s in all_timer_specs)):
    raise ValueError(
        'DoFn %r has multiple TimerSpecs with the same name: %s.' %
        (dofn, all_timer_specs))

  # Reject DoFns that use timer specs without corresponding timer callbacks.
  for timer_spec in all_timer_specs:
    if not timer_spec._attached_callback:
      raise ValueError((
          'DoFn %r has a TimerSpec without an associated on_timer '
          'callback: %s.') % (dofn, timer_spec))
    method_name = timer_spec._attached_callback.__name__
    if (timer_spec._attached_callback != getattr(dofn, method_name,
                                                 None).__func__):
      raise ValueError((
          'The on_timer callback for %s is not the specified .%s method '
          'for DoFn %r (perhaps it was overwritten?).') %
                       (timer_spec, method_name, dofn))


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
  def prefetch(self):
    # The default implementation here does nothing.
    pass


class ReadModifyWriteRuntimeState(RuntimeState):
  def read(self) -> Any:
    raise NotImplementedError(type(self))

  def write(self, value: Any) -> None:
    raise NotImplementedError(type(self))

  def clear(self) -> None:
    raise NotImplementedError(type(self))

  def commit(self) -> None:
    raise NotImplementedError(type(self))


class AccumulatingRuntimeState(RuntimeState):
  def read(self) -> Iterable[Any]:
    raise NotImplementedError(type(self))

  def add(self, value: Any) -> None:
    raise NotImplementedError(type(self))

  def clear(self) -> None:
    raise NotImplementedError(type(self))

  def commit(self) -> None:
    raise NotImplementedError(type(self))


class BagRuntimeState(AccumulatingRuntimeState):
  """Bag state interface object passed to user code."""


class SetRuntimeState(AccumulatingRuntimeState):
  """Set state interface object passed to user code."""


class CombiningValueRuntimeState(AccumulatingRuntimeState):
  """Combining value state interface object passed to user code."""


class UserStateContext(object):
  """Wrapper allowing user state and timers to be accessed by a DoFnInvoker."""
  def get_timer(self, timer_spec, key, window, timestamp, pane):
    raise NotImplementedError(type(self))

  def get_state(self, state_spec, key, window):
    raise NotImplementedError(type(self))

  def commit(self):
    raise NotImplementedError(type(self))
