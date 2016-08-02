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

from apitools.base.py import extra_types


def _get_typed_value_descriptor(obj):
  """Converts a basic type into a @type/value dictionary.

  Args:
    obj: A basestring, bool, int, or float to be converted.

  Returns:
    A dictionary containing the keys '@type' and 'value' with the value for
    the @type of appropriate type.

  Raises:
    TypeError: if the Python object has a type that is not supported.
  """
  if isinstance(obj, basestring):
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
  """Converts Python objects into extra_types.JsonValue objects.

  Args:
    obj: Python object to be converted. Can be 'None'.
    with_type: If true then the basic types (string, int, float, bool) will
      be wrapped in @type/value dictionaries. Otherwise the straight value is
      encoded into a JsonValue.

  Returns:
    A JsonValue object using JsonValue, JsonArray and JsonObject types for the
    corresponding values, lists, or dictionaries.

  Raises:
    TypeError: if the Python object contains a type that is not supported.

  The types supported are str, bool, list, tuple, dict, and None. The Dataflow
  API requires JsonValue(s) in many places, and it is quite convenient to be
  able to specify these hierarchical objects using Python syntax.
  """
  if obj is None:
    return extra_types.JsonValue(is_null=True)
  elif isinstance(obj, (list, tuple)):
    return extra_types.JsonValue(
        array_value=extra_types.JsonArray(
            entries=[to_json_value(o, with_type=with_type) for o in obj]))
  elif isinstance(obj, dict):
    json_object = extra_types.JsonObject()
    for k, v in obj.iteritems():
      json_object.properties.append(
          extra_types.JsonObject.Property(
              key=k, value=to_json_value(v, with_type=with_type)))
    return extra_types.JsonValue(object_value=json_object)
  elif with_type:
    return to_json_value(_get_typed_value_descriptor(obj), with_type=False)
  elif isinstance(obj, basestring):
    return extra_types.JsonValue(string_value=obj)
  elif isinstance(obj, bool):
    return extra_types.JsonValue(boolean_value=obj)
  elif isinstance(obj, int):
    return extra_types.JsonValue(integer_value=obj)
  elif isinstance(obj, float):
    return extra_types.JsonValue(double_value=obj)
  else:
    raise TypeError('Cannot convert %s to a JSON value.' % repr(obj))


def from_json_value(v):
  """Converts extra_types.JsonValue objects into Python objects.

  Args:
    v: JsonValue object to be converted.

  Returns:
    A Python object structured as values, lists, and dictionaries corresponding
    to JsonValue, JsonArray and JsonObject types.

  Raises:
    TypeError: if the JsonValue object contains a type that is not supported.

  The types supported are str, bool, list, dict, and None. The Dataflow API
  returns JsonValue(s) in many places and it is quite convenient to be able to
  convert these hierarchical objects to much simpler Python objects.
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
