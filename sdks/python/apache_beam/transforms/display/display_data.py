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

"""
DisplayData, its classes, interfaces and methods.
"""

from __future__ import absolute_import

import calendar
from datetime import datetime, timedelta
import inspect
import json

__all__ = ['HasDisplayData', 'DisplayDataItem', 'DisplayData']


class HasDisplayData(object):
  """ Basic interface for elements that contain display data.

  It contains only the display_data method.
  """
  def __init__(self, *args, **kwargs):
    super(HasDisplayData, self).__init__(*args, **kwargs)

  def display_data(self):
    return {}

  def _namespace(self):
    return '{}.{}'.format(self.__module__, self.__class__.__name__)


class DisplayData(object):
  def __init__(self, namespace='__main__'):
    self.namespace = namespace
    self.items = []

  def populate_items(self, display_data_dict):
    for key, element in display_data_dict.items():
      if isinstance(element, HasDisplayData):
        subcomponent_display_data = DisplayData(element._namespace())
        subcomponent_display_data.populate_items(element.display_data())
        self.items += subcomponent_display_data.items
        continue

      if isinstance(element, dict):
        self.items.append(
            DisplayDataItem(self.namespace,
                            key,
                            DisplayDataItem._get_value_type(element['value']),
                            element['value'],
                            shortValue=element.get('shortValue'),
                            url=element.get('url'),
                            label=element.get('label')))
        continue

      # If it's not a HasDisplayData element,
      # nor a dictionary, then it's a simple value
      self.items.append(
          DisplayDataItem(self.namespace,
                          key,
                          DisplayDataItem._get_value_type(element),
                          element))

  def output(self):
    return [item.get_dict() for item in self.items]

  @classmethod
  def create_from(cls, has_display_data):
    if not isinstance(has_display_data, HasDisplayData):
      raise ValueError('Element of class {}.{} does not subclass HasDisplayData'
                       .format(has_display_data.__module__,
                               has_display_data.__class__.__name__))
    display_data = cls(has_display_data._namespace())
    display_data.populate_items(has_display_data.display_data())
    return display_data


class DisplayDataItem(object):
  typeDict = {str:'STRING',
              int:'INTEGER',
              float:'FLOAT',
              timedelta:'DURATION',
              datetime:'TIMESTAMP'}

  def __init__(self, namespace, key, type_, value,
               shortValue=None, url=None, label=None):
    if key is None:
      raise ValueError('Key must not be None')
    if value is None:
      raise ValueError('Value must not be None')
    if type_ is None:
      raise ValueError('Value {} is of an unsupported type.'.format(value))

    self.namespace = namespace
    self.key = key
    self.type = type_
    self.value = value
    self.shortValue = shortValue
    self.url = url
    self.label = label

  def get_dict(self):
    res = {'key': self.key,
           'namespace': self.namespace,
           'type': self.type}

    if self.url is not None:
      res['url'] = self.url
    # TODO: What to do about shortValue? No special processing?
    if self.shortValue is not None:
      res['shortValue'] = self.shortValue
    if self.label is not None:
      res['label'] = self.label
    res['value'] = self._format_value(self.value, self.type)
    return res

  def __repr__(self):
    return 'DisplayDataItem({})'.format(json.dumps(self.get_dict()))

  @classmethod
  def _format_value(cls, value, type_):
    res = value
    if type_ == 'JAVA_CLASS':
      res = '{}.{}'.format(value.__module__, value.__name__)
    if type_ == 'DURATION':
      res = value.total_seconds()*1000
    if type_ == 'TIMESTAMP':
      res = calendar.timegm(value.timetuple())*1000 + value.microsecond//1000
    return res

  @classmethod
  def _get_value_type(cls, value):
    type_ = cls.typeDict.get(type(value))
    if type_ is None:
      type_ = 'JAVA_CLASS' if inspect.isclass(value) else None
    return type_
