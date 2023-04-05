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

"""Deprecated and experimental annotations.

Experimental: Signifies that a public API (public class, method or field) is
subject to incompatible changes, or even removal, in a future release. Note that
the presence of this annotation implies nothing about the quality or performance
of the API in question, only the fact that the API or behavior may change in any
way.

Deprecated: Signifies that users are discouraged from using a public API
typically because a better alternative exists, and the current form might be
removed in a future version.

Usage:
For internal use only; no backwards-compatibility guarantees.

Annotations come in two flavors: deprecated and experimental

The 'deprecated' annotation requires a 'since" parameter to specify
what version deprecated it.
Both 'deprecated' and 'experimental' annotations can specify the
current recommended version to use by means of a 'current' parameter.

The following example illustrates how to annotate coexisting versions of the
same function 'multiply'.::

  def multiply(arg1, arg2):
    print(arg1, '*', arg2, '=', end=' ')
    return arg1*arg2

# This annotation marks 'old_multiply' as deprecated since 'v.1' and suggests
# using 'multiply' instead.::

  @deprecated(since='v.1', current='multiply')
  def old_multiply(arg1, arg2):
    result = 0
    for i in xrange(arg1):
        result += arg2
    print(arg1, '*', arg2, '(the old way)=', end=' ')
    return result

# This annotation marks 'exp_multiply' as experimental and suggests
# using 'multiply' instead.::

  @experimental(since='v.1', current='multiply')
  def exp_multiply(arg1, arg2):
    print(arg1, '*', arg2, '(the experimental way)=', end=' ')
    return (arg1*arg2)*(arg1/arg2)*(arg2/arg1)

# If a custom message is needed, on both annotations types the
# arg custom_message can be used.::

  @experimental(since='v.1', current='multiply'
                custom_message='Experimental since %since%
                                Please use %current% insted.')
  def exp_multiply(arg1, arg2):
    print(arg1, '*', arg2, '(the experimental way)=', end=' ')
    return (arg1*arg2)*(arg1/arg2)*(arg2/arg1)

# Set a warning filter to control how often warnings are produced.::

  warnings.simplefilter("always")
  print(multiply(5, 6))
  print(old_multiply(5,6))
  print(exp_multiply(5,6))
"""

# pytype: skip-file

import inspect
import warnings
from functools import partial
from functools import wraps


class BeamDeprecationWarning(DeprecationWarning):
  """Beam-specific deprecation warnings."""


# Don't ignore BeamDeprecationWarnings.
warnings.simplefilter('once', BeamDeprecationWarning)


class _WarningMessage:
  """Utility class for assembling the warning message."""
  def __init__(self, label, since, current, extra_message, custom_message):
    """Initialize message, leave only name as placeholder."""
    if custom_message is None:
      message = '%name% is ' + label
      if label == 'deprecated':
        message += ' since %s' % since
      message += '. Use %s instead.' % current if current else '.'
      if extra_message:
        message += ' ' + extra_message
    else:
      if label == 'deprecated' and '%since%' not in custom_message:
        raise TypeError(
            "Replacement string %since% not found on \
        custom message")
      emptyArg = lambda x: '' if x is None else x
      message = custom_message\
      .replace('%since%', emptyArg(since))\
      .replace('%current%', emptyArg(current))\
      .replace('%extra%', emptyArg(extra_message))
    self.label = label
    self.message = message

  def emit_warning(self, fnc_name):
    if self.label == 'deprecated':
      warning_type = BeamDeprecationWarning
    else:
      warning_type = FutureWarning
    warnings.warn(
        self.message.replace('%name%', fnc_name), warning_type, stacklevel=3)


def annotate(label, since, current, extra_message, custom_message=None):
  """Decorates an API with a deprecated or experimental annotation.

  Args:
    label: the kind of annotation ('deprecated' or 'experimental').
    since: the version that causes the annotation.
    current: the suggested replacement function.
    extra_message: an optional additional message.
    custom_message: if the default message does not suffice, the message
      can be changed using this argument. A string
      whit replacement tokens.
      A replecement string is were the previus args will
      be located on the custom message.
      The following replacement strings can be used:
      %name% -> API.__name__
      %since% -> since (Mandatory for the decapreted annotation)
      %current% -> current
      %extra% -> extra_message

  Returns:
    The decorator for the API.
  """
  warning_message = _WarningMessage(
      label=label,
      since=since,
      current=current,
      extra_message=extra_message,
      custom_message=custom_message)

  def _annotate(fnc):
    if inspect.isclass(fnc):
      # Wrapping class into function causes documentation rendering issue.
      # Patch class's __new__ method instead.
      old_new = fnc.__new__

      def wrapped_new(cls, *args, **kwargs):
        # Emit a warning when class instance is created.
        warning_message.emit_warning(fnc.__name__)
        if old_new is object.__new__:
          # object.__new__ takes no extra argument for python>=3
          return old_new(cls)
        return old_new(cls, *args, **kwargs)

      fnc.__new__ = staticmethod(wrapped_new)
      return fnc
    else:

      @wraps(fnc)
      def inner(*args, **kwargs):
        # Emit a warning when the function is called.
        warning_message.emit_warning(fnc.__name__)
        return fnc(*args, **kwargs)

      return inner

  return _annotate


# Use partial application to customize each annotation.
# 'current' will be optional in both deprecated and experimental
# while 'since' will be mandatory for deprecated.
deprecated = partial(
    annotate, label='deprecated', current=None, extra_message=None)
experimental = partial(
    annotate,
    label='experimental',
    current=None,
    since=None,
    extra_message=None)
