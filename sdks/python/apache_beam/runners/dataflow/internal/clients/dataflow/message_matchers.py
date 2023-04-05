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
# pytype: skip-file

from hamcrest.core.base_matcher import BaseMatcher

IGNORED = object()


class MetricStructuredNameMatcher(BaseMatcher):
  """Matches a MetricStructuredName."""
  def __init__(self, name=IGNORED, origin=IGNORED, context=IGNORED):
    """Creates a MetricsStructuredNameMatcher.

    Any property not passed in to the constructor will be ignored when matching.

    Args:
      name: A string with the metric name.
      origin: A string with the metric namespace.
      context: A key:value dictionary that will be matched to the
        structured name.
    """
    if context != IGNORED and not isinstance(context, dict):
      raise ValueError('context must be a Python dictionary.')

    self.name = name
    self.origin = origin
    self.context = context

  def _matches(self, item):
    if self.name != IGNORED and item.name != self.name:
      return False
    if self.origin != IGNORED and item.origin != self.origin:
      return False
    if self.context != IGNORED:
      for key, name in self.context.items():
        if key not in item.context:
          return False
        if name != IGNORED and item.context[key] != name:
          return False
    return True

  def describe_to(self, description):
    descriptors = []
    if self.name != IGNORED:
      descriptors.append('name is {}'.format(self.name))
    if self.origin != IGNORED:
      descriptors.append('origin is {}'.format(self.origin))
    if self.context != IGNORED:
      descriptors.append('context is ({})'.format(str(self.context)))

    item_description = ' and '.join(descriptors)
    description.append(item_description)


class MetricUpdateMatcher(BaseMatcher):
  """Matches a metrics update protocol buffer."""
  def __init__(
      self, cumulative=IGNORED, name=IGNORED, scalar=IGNORED, kind=IGNORED):
    """Creates a MetricUpdateMatcher.

    Any property not passed in to the constructor will be ignored when matching.

    Args:
      cumulative: A boolean.
      name: A MetricStructuredNameMatcher object that matches the name.
      scalar: An integer with the metric update.
      kind: A string defining the kind of counter.
    """
    if name != IGNORED and not isinstance(name, MetricStructuredNameMatcher):
      raise ValueError('name must be a MetricStructuredNameMatcher.')

    self.cumulative = cumulative
    self.name = name
    self.scalar = scalar
    self.kind = kind

  def _matches(self, item):
    if self.cumulative != IGNORED and item.cumulative != self.cumulative:
      return False
    if self.name != IGNORED and not self.name._matches(item.name):
      return False
    if self.kind != IGNORED and item.kind != self.kind:
      return False
    if self.scalar != IGNORED:
      value_property = [
          p for p in item.scalar.object_value.properties if p.key == 'value'
      ]
      int_value = value_property[0].value.integer_value
      if self.scalar != int_value:
        return False
    return True

  def describe_to(self, description):
    descriptors = []
    if self.cumulative != IGNORED:
      descriptors.append('cumulative is {}'.format(self.cumulative))
    if self.name != IGNORED:
      descriptors.append('name is {}'.format(self.name))
    if self.scalar != IGNORED:
      descriptors.append('scalar is ({})'.format(str(self.scalar)))

    item_description = ' and '.join(descriptors)
    description.append(item_description)
