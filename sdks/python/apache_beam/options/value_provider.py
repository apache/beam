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

"""A ValueProvider class to implement templates with both statically
and dynamically provided values.
"""

from __future__ import absolute_import

from builtins import object
from functools import wraps

from apache_beam import error

__all__ = [
    'ValueProvider',
    'StaticValueProvider',
    'RuntimeValueProvider',
    'check_accessible',
    ]


class ValueProvider(object):
  def is_accessible(self):
    raise NotImplementedError(
        'ValueProvider.is_accessible implemented in derived classes'
    )

  def get(self):
    raise NotImplementedError(
        'ValueProvider.get implemented in derived classes'
    )


class StaticValueProvider(ValueProvider):
  def __init__(self, value_type, value):
    self.value_type = value_type
    self.value = value_type(value)

  def is_accessible(self):
    return True

  def get(self):
    return self.value

  def __str__(self):
    return str(self.value)

  def __eq__(self, other):
    if self.value == other:
      return True
    if isinstance(other, StaticValueProvider):
      if (self.value_type == other.value_type and
          self.value == other.value):
        return True
    return False

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((type(self), self.value_type, self.value))


class RuntimeValueProvider(ValueProvider):
  runtime_options = None
  experiments = set()

  def __init__(self, option_name, value_type, default_value):
    self.option_name = option_name
    self.default_value = default_value
    self.value_type = value_type

  def is_accessible(self):
    return RuntimeValueProvider.runtime_options is not None

  @classmethod
  def get_value(cls, option_name, value_type, default_value):
    if not RuntimeValueProvider.runtime_options:
      return default_value

    candidate = RuntimeValueProvider.runtime_options.get(option_name)
    if candidate:
      return value_type(candidate)
    else:
      return default_value

  def get(self):
    if RuntimeValueProvider.runtime_options is None:
      raise error.RuntimeValueProviderError(
          '%s.get() not called from a runtime context' % self)

    return RuntimeValueProvider.get_value(self.option_name,
                                          self.value_type,
                                          self.default_value)

  @classmethod
  def set_runtime_options(cls, pipeline_options):
    RuntimeValueProvider.runtime_options = pipeline_options
    RuntimeValueProvider.experiments = RuntimeValueProvider.get_value(
        'experiments', set, set())

  def __str__(self):
    return '%s(option: %s, type: %s, default_value: %s)' % (
        self.__class__.__name__,
        self.option_name,
        self.value_type.__name__,
        repr(self.default_value)
    )


def check_accessible(value_provider_list):
  """Check accessibility of a list of ValueProvider objects."""
  assert isinstance(value_provider_list, list)

  def _check_accessible(fnc):
    @wraps(fnc)
    def _f(self, *args, **kwargs):
      for obj in [getattr(self, vp) for vp in value_provider_list]:
        if not obj.is_accessible():
          raise error.RuntimeValueProviderError('%s not accessible' % obj)
      return fnc(self, *args, **kwargs)
    return _f
  return _check_accessible
