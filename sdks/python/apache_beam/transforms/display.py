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

  It contains only the display_data method and a namespace method.
  """
  def display_data(self):
    return {}

  def _namespace(self):
    return '{}.{}'.format(self.__module__, self.__class__.__name__)


class DisplayData(object):
  def __init__(self, namespace, display_data_dict):
    self.namespace = namespace
    self.items = []
    self.populate_items(display_data_dict)

  def populate_items(self, display_data_dict):
    for key, element in display_data_dict.items():
      if isinstance(element, HasDisplayData):
        subcomponent_display_data = DisplayData(element._namespace(),
                                                element.display_data())
        self.items += subcomponent_display_data.items
        continue

      if isinstance(element, DisplayDataItem):
        element.key = key
        element.namespace = self.namespace
        self.items.append(element)
        continue

      # If it's not a HasDisplayData element,
      # nor a dictionary, then it's a simple value
      self.items.append(
          DisplayDataItem(element,
                          namespace=self.namespace,
                          key=key))

  def output(self):
    return [item.get_dict() for item in self.items]

  @classmethod
  def create_from(cls, has_display_data):
    if not isinstance(has_display_data, HasDisplayData):
      raise ValueError('Element of class {}.{} does not subclass HasDisplayData'
                       .format(has_display_data.__module__,
                               has_display_data.__class__.__name__))
    return cls(has_display_data._namespace(), has_display_data.display_data())


class DisplayDataItem(object):
  typeDict = {str:'STRING',
              int:'INTEGER',
              float:'FLOAT',
              timedelta:'DURATION',
              datetime:'TIMESTAMP'}

  def __init__(self, value, url=None, label=None,
               namespace=None, key=None, shortValue=None):
    self.namespace = namespace
    self.key = key
    self.type = self._get_value_type(value)
    self.shortValue = (shortValue if shortValue is not None else
                       self._get_short_value(value, self.type))
    self.value = value
    self.url = url
    self.label = label

  def is_valid(self):
    if self.key is None:
      raise ValueError('Key must not be None')
    if self.namespace is None:
      raise ValueError('Namespace must not be None')
    if self.value is None:
      raise ValueError('Value must not be None')
    if self.type is None:
      raise ValueError('Value {} is of an unsupported type.'.format(self.value))

  def get_dict(self):
    self.is_valid()

    res = {'key': self.key,
           'namespace': self.namespace,
           'type': self.type}

    if self.url is not None:
      res['url'] = self.url
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
  def _get_short_value(cls, value, type_):
    if type_ == 'JAVA_CLASS':
      return value.__name__
    return None

  @classmethod
  def _get_value_type(cls, value):
    type_ = cls.typeDict.get(type(value))
    if type_ is None:
      type_ = 'JAVA_CLASS' if inspect.isclass(value) else None
    return type_
