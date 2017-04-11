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

from functools import wraps


class RuntimeValueProviderError(RuntimeError):
  def __init__(self, msg):
    """Class representing the errors thrown during runtime by the valueprovider
    Args:
      msg: Message string for the exception thrown
    """
    super(RuntimeValueProviderError, self).__init__(msg)


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


class RuntimeValueProvider(ValueProvider):
  runtime_options_map = {}

  def __init__(self, option_name, value_type, default_value, options_id):
    assert options_id is not None
    self.option_name = option_name
    self.default_value = default_value
    self.value_type = value_type
    self.options_id = options_id

  def is_accessible(self):
    return RuntimeValueProvider.runtime_options_map.get(
        self.options_id) is not None

  def get(self):
    runtime_options = (
        RuntimeValueProvider.runtime_options_map.get(self.options_id))
    if runtime_options is None:
      raise RuntimeValueProviderError(
          '%s.get() not called from a runtime context' % self)

    candidate = runtime_options.get(self.option_name)
    if candidate:
      value = self.value_type(candidate)
    else:
      value = self.default_value
    return value

  @classmethod
  def set_runtime_options(cls, options_id, pipeline_options):
    assert options_id not in RuntimeValueProvider.runtime_options_map
    RuntimeValueProvider.runtime_options_map[options_id] = pipeline_options

  @classmethod
  def unset_runtime_options(cls, options_id):
    assert options_id in RuntimeValueProvider.runtime_options_map
    del RuntimeValueProvider.runtime_options_map[options_id]

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
          raise RuntimeValueProviderError('%s not accessible' % obj)
      return fnc(self, *args, **kwargs)
    return _f
  return _check_accessible
