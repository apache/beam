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

"""JSON conversion utility functions."""

# pytype: skip-file

from apache_beam.options.value_provider import ValueProvider

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py import extra_types
except ImportError:
  extra_types = None
# pylint: enable=wrong-import-order, wrong-import-position

_MAXINT64 = (1 << 63) - 1
_MININT64 = -(1 << 63)


def get_typed_value_descriptor(obj):
  """For internal use only; no backwards-compatibility guarantees.

  Converts a basic type into a @type/value dictionary.

  Args:
    obj: A bytes, unicode, bool, int, or float to be converted.

  Returns:
    A dictionary containing the keys ``@type`` and ``value`` with the value for
    the ``@type`` of appropriate type.

  Raises:
    TypeError: if the Python object has a type that is not
      supported.
  """
  if isinstance(obj, (bytes, str)):
    type_name = 'Text'
  elif isinstance(obj, bool):
    type_name = 'Boolean'
  elif isinstance(obj, int):
    type_name = 'Integer'
  elif isinstance(obj, float):
    type_name = 'Float'
  else:
    raise TypeError('Cannot get a type descriptor for %s.' % repr(obj))
  return {'@type': 'http://schema.org/%s' % type_name, 'value': obj}


def to_json_value(obj, with_type=False):
  """For internal use only; no backwards-compatibility guarantees.

  Converts Python objects into extra_types.JsonValue objects.

  Args:
    obj: Python object to be converted. Can be :data:`None`.
    with_type: If true then the basic types (``bytes``, ``unicode``, ``int``,
      ``float``, ``bool``) will be wrapped in ``@type:value`` dictionaries.
      Otherwise the straight value is encoded into a ``JsonValue``.

  Returns:
    A ``JsonValue`` object using ``JsonValue``, ``JsonArray`` and ``JsonObject``
    types for the corresponding values, lists, or dictionaries.

  Raises:
    TypeError: if the Python object contains a type that is not
      supported.

  The types supported are ``str``, ``bool``, ``list``, ``tuple``, ``dict``, and
  ``None``. The Dataflow API requires JsonValue(s) in many places, and it is
  quite convenient to be able to specify these hierarchical objects using
  Python syntax.
  """
  if obj is None:
    return extra_types.JsonValue(is_null=True)
  elif isinstance(obj, (list, tuple)):
    return extra_types.JsonValue(
        array_value=extra_types.JsonArray(
            entries=[to_json_value(o, with_type=with_type) for o in obj]))
  elif isinstance(obj, dict):
    json_object = extra_types.JsonObject()
    for k, v in obj.items():
      json_object.properties.append(
          extra_types.JsonObject.Property(
              key=k, value=to_json_value(v, with_type=with_type)))
    return extra_types.JsonValue(object_value=json_object)
  elif with_type:
    return to_json_value(get_typed_value_descriptor(obj), with_type=False)
  elif isinstance(obj, str):
    return extra_types.JsonValue(string_value=obj)
  elif isinstance(obj, bytes):
    return extra_types.JsonValue(string_value=obj.decode('utf8'))
  elif isinstance(obj, bool):
    return extra_types.JsonValue(boolean_value=obj)
  elif isinstance(obj, int):
    if _MININT64 <= obj <= _MAXINT64:
      return extra_types.JsonValue(integer_value=obj)
    else:
      raise TypeError('Can not encode {} as a 64-bit integer'.format(obj))
  elif isinstance(obj, float):
    return extra_types.JsonValue(double_value=obj)
  elif isinstance(obj, ValueProvider):
    if obj.is_accessible():
      return to_json_value(obj.get())
    return extra_types.JsonValue(is_null=True)
  else:
    raise TypeError('Cannot convert %s to a JSON value.' % repr(obj))


def from_json_value(v):
  """For internal use only; no backwards-compatibility guarantees.

  Converts ``extra_types.JsonValue`` objects into Python objects.

  Args:
    v: ``JsonValue`` object to be converted.

  Returns:
    A Python object structured as values, lists, and dictionaries corresponding
    to ``JsonValue``, ``JsonArray`` and ``JsonObject`` types.

  Raises:
    TypeError: if the ``JsonValue`` object contains a type that is
      not supported.

  The types supported are ``str``, ``bool``, ``list``, ``dict``, and ``None``.
  The Dataflow API returns JsonValue(s) in many places and it is quite
  convenient to be able to convert these hierarchical objects to much simpler
  Python objects.
  """
  if isinstance(v, extra_types.JsonValue):
    if v.string_value is not None:
      return v.string_value
    elif v.boolean_value is not None:
      return v.boolean_value
    elif v.integer_value is not None:
      return v.integer_value
    elif v.double_value is not None:
      return v.double_value
    elif v.array_value is not None:
      return from_json_value(v.array_value)
    elif v.object_value is not None:
      return from_json_value(v.object_value)
    elif v.is_null:
      return None
  elif isinstance(v, extra_types.JsonArray):
    return [from_json_value(e) for e in v.entries]
  elif isinstance(v, extra_types.JsonObject):
    return {p.key: from_json_value(p.value) for p in v.properties}
  raise TypeError('Cannot convert %s from a JSON value.' % repr(v))
