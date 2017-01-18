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
  pipeline_options_dict = None

  def __init__(self, pipeline_options_subclass, option_name,
               value_type, default_value):
    self.pipeline_options_subclass = pipeline_options_subclass
    self.option_name = option_name
    self.default_value = default_value
    self.value_type = value_type

  def is_accessible(self):
    return RuntimeValueProvider.pipeline_options_dict is not None

  def get(self):
    pipeline_options_dict = RuntimeValueProvider.pipeline_options_dict
    if pipeline_options_dict is None:
      raise RuntimeError('%s.get() not called from a runtime context' %self)
    pipeline_option = (
        self.pipeline_options_subclass.from_dictionary(pipeline_options_dict)
        ._visible_options
        .__dict__
        .get(self.option_name)
    )
    value = (
        pipeline_option.get()
        if isinstance(pipeline_option, StaticValueProvider)
        else self.default_value
    )
    return value

  @classmethod
  def set_runtime_options(cls, pipeline_options):
    assert RuntimeValueProvider.pipeline_options_dict is None
    RuntimeValueProvider.pipeline_options_dict = pipeline_options

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
          raise RuntimeError('%s not accessible' % obj)
      return fnc(self, *args, **kwargs)
    return _f
  return _check_accessible
