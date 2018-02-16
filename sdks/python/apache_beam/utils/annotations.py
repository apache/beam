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

For internal use only; no backwards-compatibility guarantees.

Annotations come in two flavors: deprecated and experimental

The 'deprecated' annotation requires a 'since" parameter to specify
what version deprecated it.
Both 'deprecated' and 'experimental' annotations can specify the
current recommended version to use by means of a 'current' parameter.

The following example illustrates how to annotate coexisting versions of the
same function 'multiply'.::

  def multiply(arg1, arg2):
    print arg1, '*', arg2, '=',
    return arg1*arg2

# This annotation marks 'old_multiply' as deprecated since 'v.1' and suggests
# using 'multiply' instead.::

  @deprecated(since='v.1', current='multiply')
  def old_multiply(arg1, arg2):
    result = 0
    for i in xrange(arg1):
        result += arg2
    print arg1, '*', arg2, '(the old way)=',
    return result

# This annotation marks 'exp_multiply' as experimental and suggests
# using 'multiply' instead.::

  @experimental(since='v.1', current='multiply')
  def exp_multiply(arg1, arg2):
    print arg1, '*', arg2, '(the experimental way)=',
    return (arg1*arg2)*(arg1/arg2)*(arg2/arg1)

# Set a warning filter to control how often warnings are produced.::

  warnings.simplefilter("always")
  print multiply(5, 6)
  print old_multiply(5,6)
  print exp_multiply(5,6)
"""

import warnings
from functools import partial
from functools import wraps

# Produce only the first occurrence of matching warnings regardless of
# location per line of execution. Since the number of lines of execution
# depends on the concrete runner, the number of warnings produced will
# vary depending on the runner.
warnings.simplefilter("once")


def annotate(label, since, current, extra_message):
  """Decorates a function with a deprecated or experimental annotation.

  Args:
    label: the kind of annotation ('deprecated' or 'experimental').
    since: the version that causes the annotation.
    current: the suggested replacement function.
    extra_message: an optional additional message.

  Returns:
    The decorator for the function.
  """
  def _annotate(fnc):
    @wraps(fnc)
    def inner(*args, **kwargs):
      if label == 'deprecated':
        warning_type = DeprecationWarning
      else:
        warning_type = FutureWarning
      message = '%s is %s' % (fnc.__name__, label)
      if label == 'deprecated':
        message += ' since %s' % since
      message += '. Use %s instead.' % current if current else '.'
      if extra_message:
        message += '. ' + extra_message
      warnings.warn(message, warning_type, stacklevel=2)
      return fnc(*args, **kwargs)
    return inner
  return _annotate


# Use partial application to customize each annotation.
# 'current' will be optional in both deprecated and experimental
# while 'since' will be mandatory for deprecated.
deprecated = partial(annotate, label='deprecated',
                     current=None, extra_message=None)
experimental = partial(annotate, label='experimental',
                       current=None, since=None, extra_message=None)
