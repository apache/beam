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

import types

from apache_beam.coders import Coder
from apache_beam.transforms.timeutil import TimeDomain


class StateSpec(object):
  """Specification for a user DoFn state cell."""

  def __init__(self):
    raise NotImplementedError

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)


class BagStateSpec(StateSpec):
  """Specification for a user DoFn bag state cell."""

  def __init__(self, name, coder):
    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    self.name = name
    self.coder = coder


class CombiningValueStateSpec(StateSpec):
  """Specification for a user DoFn combining value state cell."""

  def __init__(self, name, coder, combiner):
    # Avoid circular import.
    from apache_beam.transforms.core import CombineFn

    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    assert isinstance(combiner, CombineFn)
    self.name = name
    self.coder = coder
    self.combiner = combiner


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


class UserStateUtils(object):

  @staticmethod
  def validate_stateful_dofn(dofn):
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
