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
:class:`DisplayData`, its classes, interfaces and methods.

The classes in this module allow users and transform developers to define
static display data to be displayed when a pipeline runs.
:class:`~apache_beam.transforms.ptransform.PTransform` s,
:class:`~apache_beam.transforms.core.DoFn` s
and other pipeline components are subclasses of the :class:`HasDisplayData`
mixin. To add static display data to a component, you can override the
:meth:`HasDisplayData.display_data()` method.

Available classes:

* :class:`HasDisplayData` - Components that inherit from this class can have
  static display data shown in the UI.
* :class:`DisplayDataItem` - This class represents static display data
  elements.
* :class:`DisplayData` - Internal class that is used to create display data
  and communicate it to the API.
"""

from __future__ import absolute_import

import calendar
import inspect
import json
from datetime import datetime
from datetime import timedelta

import six

__all__ = ['HasDisplayData', 'DisplayDataItem', 'DisplayData']


class HasDisplayData(object):
  """ Basic mixin for elements that contain display data.

  It implements only the display_data method and a _namespace method.
  """

  def display_data(self):
    """ Returns the display data associated to a pipeline component.

    It should be reimplemented in pipeline components that wish to have
    static display data.

    Returns:
      Dict[str, Any]: A dictionary containing ``key:value`` pairs.
      The value might be an integer, float or string value; a
      :class:`DisplayDataItem` for values that have more data
      (e.g. short value, label, url); or a :class:`HasDisplayData` instance
      that has more display data that should be picked up. For example::

        {
          'key1': 'string_value',
          'key2': 1234,
          'key3': 3.14159265,
          'key4': DisplayDataItem('apache.org', url='http://apache.org'),
          'key5': subComponent
        }
    """
    return {}

  def _namespace(self):
    return '{}.{}'.format(self.__module__, self.__class__.__name__)


class DisplayData(object):
  """ Static display data associated with a pipeline component.
  """

  def __init__(self, namespace, display_data_dict):
    self.namespace = namespace
    self.items = []
    self._populate_items(display_data_dict)

  def _populate_items(self, display_data_dict):
    """ Populates the list of display data items.
    """
    for key, element in display_data_dict.items():
      if isinstance(element, HasDisplayData):
        subcomponent_display_data = DisplayData(element._namespace(),
                                                element.display_data())
        self.items += subcomponent_display_data.items
        continue

      if isinstance(element, DisplayDataItem):
        if element.should_drop():
          continue
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

  @classmethod
  def create_from_options(cls, pipeline_options):
    """ Creates :class:`DisplayData` from a
    :class:`~apache_beam.options.pipeline_options.PipelineOptions` instance.

    When creating :class:`DisplayData`, this method will convert the value of
    any item of a non-supported type to its string representation.
    The normal :meth:`.create_from()` method rejects those items.

    Returns:
      DisplayData: A :class:`DisplayData` instance with populated items.

    Raises:
      ~exceptions.ValueError: If the **has_display_data** argument is
        not an instance of :class:`HasDisplayData`.
    """
    from apache_beam.options.pipeline_options import PipelineOptions
    if not isinstance(pipeline_options, PipelineOptions):
      raise ValueError(
          'Element of class {}.{} does not subclass PipelineOptions'
          .format(pipeline_options.__module__,
                  pipeline_options.__class__.__name__))

    items = {k: (v if DisplayDataItem._get_value_type(v) is not None
                 else str(v))
             for k, v in pipeline_options.display_data().items()}
    return cls(pipeline_options._namespace(), items)

  @classmethod
  def create_from(cls, has_display_data):
    """ Creates :class:`DisplayData` from a :class:`HasDisplayData` instance.

    Returns:
      DisplayData: A :class:`DisplayData` instance with populated items.

    Raises:
      ~exceptions.ValueError: If the **has_display_data** argument is
        not an instance of :class:`HasDisplayData`.
    """
    if not isinstance(has_display_data, HasDisplayData):
      raise ValueError('Element of class {}.{} does not subclass HasDisplayData'
                       .format(has_display_data.__module__,
                               has_display_data.__class__.__name__))
    return cls(has_display_data._namespace(), has_display_data.display_data())


class DisplayDataItem(object):
  """ A DisplayDataItem represents a unit of static display data.

  Each item is identified by a key and the namespace of the component the
  display item belongs to.
  """
  typeDict = {str:'STRING',
              six.text_type:'STRING',
              int:'INTEGER',
              float:'FLOAT',
              bool: 'BOOLEAN',
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
    self._drop_if_none = False
    self._drop_if_default = False

  def drop_if_none(self):
    """ The item should be dropped if its value is None.

    Returns:
      Returns self.
    """
    self._drop_if_none = True
    return self

  def drop_if_default(self, default):
    """ The item should be dropped if its value is equal to its default.

    Returns:
      Returns self.
    """
    self._default = default
    self._drop_if_default = True
    return self

  def should_drop(self):
    """ Return True if the item should be dropped, or False if it should not
    be dropped. This depends on the drop_if_none, and drop_if_default calls.

    Returns:
      True or False; depending on whether the item should be dropped or kept.
    """
    if self._drop_if_none and self.value is None:
      return True
    if self._drop_if_default and self.value == self._default:
      return True
    return False

  def is_valid(self):
    """ Checks that all the necessary fields of the :class:`DisplayDataItem`
    are filled in. It checks that neither key, namespace, value or type are
    :data:`None`.

    Raises:
      ~exceptions.ValueError: If the item does not have a key, namespace,
        value or type.
    """
    if self.key is None:
      raise ValueError('Invalid DisplayDataItem. Key must not be None')
    if self.namespace is None:
      raise ValueError('Invalid DisplayDataItem. Namespace must not be None')
    if self.value is None:
      raise ValueError('Invalid DisplayDataItem. Value must not be None')
    if self.type is None:
      raise ValueError(
          'Invalid DisplayDataItem. Value {} is of an unsupported type.'
          .format(self.value))

  def _get_dict(self):
    res = {'key': self.key,
           'namespace': self.namespace,
           'type': self.type if self.type != 'CLASS' else 'STRING'}
    # TODO: Python Class types should not be special-cased once
    # the Fn API is in.
    if self.url is not None:
      res['url'] = self.url
    if self.shortValue is not None:
      res['shortValue'] = self.shortValue
    if self.label is not None:
      res['label'] = self.label
    res['value'] = self._format_value(self.value, self.type)
    return res

  def get_dict(self):
    """ Returns the internal-API dictionary representing the
    :class:`DisplayDataItem`.

    Returns:
      Dict[str, Any]: A dictionary. The internal-API dictionary representing
      the :class:`DisplayDataItem`.

    Raises:
      ~exceptions.ValueError: if the item is not valid.
    """
    self.is_valid()
    return self._get_dict()

  def __repr__(self):
    return 'DisplayDataItem({})'.format(json.dumps(self._get_dict()))

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self._get_dict() == other._get_dict()
    return False

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(tuple(sorted(self._get_dict().items())))

  @classmethod
  def _format_value(cls, value, type_):
    """ Returns the API representation of a value given its type.

    Args:
      value: The value of the item that needs to be shortened.
      type_(string): The type of the value.

    Returns:
      A formatted value in the form of a float, int, or string.
    """
    res = value
    if type_ == 'CLASS':
      res = '{}.{}'.format(value.__module__, value.__name__)
    elif type_ == 'DURATION':
      res = value.total_seconds()*1000
    elif type_ == 'TIMESTAMP':
      res = calendar.timegm(value.timetuple())*1000 + value.microsecond//1000
    return res

  @classmethod
  def _get_short_value(cls, value, type_):
    """ Calculates the short value for an item.

    Args:
      value: The value of the item that needs to be shortened.
      type_(string): The type of the value.

    Returns:
      The unqualified name of a class if type_ is 'CLASS'. None otherwise.
    """
    if type_ == 'CLASS':
      return value.__name__
    return None

  @classmethod
  def _get_value_type(cls, value):
    """ Infers the type of a given value.

    Args:
      value: The value whose type needs to be inferred. For 'DURATION' and
        'TIMESTAMP', the corresponding Python type is datetime.timedelta and
        datetime.datetime respectively. For Python classes, the API type is
        just 'STRING' at the moment.

    Returns:
      One of 'STRING', 'INTEGER', 'FLOAT', 'CLASS', 'DURATION', or
      'TIMESTAMP', depending on the type of the value.
    """
    #TODO: Fix Args: documentation once the Python classes handling has changed
    type_ = cls.typeDict.get(type(value))
    if type_ is None:
      type_ = 'CLASS' if inspect.isclass(value) else None
    if type_ is None and value is None:
      type_ = 'STRING'
    return type_
