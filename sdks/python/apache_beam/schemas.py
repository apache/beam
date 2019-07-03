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

import numpy as np
import six
from collections import OrderedDict
from apache_beam.portability.api import schema_pb2


class Schema(object):
  """Initialize a new Beam Schema.

  Either type_mapping or names *and* types must be defined.
  Args:
    type_mapping: Either a dictionary or a list of (name, type) tuples.
    names: A list of field names.
    types: A list of types for each field.
  """

  def __init__(self, type_mapping=None, names=None, types=None):
    if type_mapping is None:
      if names is None and types is None:
        raise TypeError("Must define either type_mapping or names/types")
      type_mapping = zip(names, types)
    self.type_mapping_ = OrderedDict(type_mapping)
    validate_type_mapping(self.type_mapping_)

  def to_schema_api(self):
    fields = [
        schema_pb2.Field(name=name, type=fieldtype_to_schema_api(type_))
        for name, type_ in six.iteritems(self.type_mapping_)
    ]
    return schema_pb2.Schema(fields=fields)

  @staticmethod
  def from_schema_api(proto):
    fields = proto.fields
    names = (field.name for field in fields)

    def fieldtype_from_schema_api(fieldtype_proto):
      type_info = fieldtype_proto.WhichOneof("type_info")
      if type_info == "atomic_type":
        if fieldtype_proto.atomic_type == schema_pb2.AtomicType.BYTE:
          return np.byte
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.INT16:
          return np.int16
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.INT32:
          return np.int32
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.INT64:
          return np.int64
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.FLOAT:
          return np.float32
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.DOUBLE:
          return np.float64
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.STRING:
          return np.unicode
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.BOOLEAN:
          return np.boolean
        elif fieldtype_proto.atomic_type == schema_pb2.AtomicType.BYTES:
          return np.bytes_
        else:
          raise TypeError("Unsupported atomic type: {0}".format(
              fieldtype_proto.atomic_type))
      elif type_info == "array_type":
        return ListType(
            fieldtype_from_schema_api(fieldtype_proto.array_type.element_type))
      elif type_info == "map_type":
        return MapType(
            fieldtype_from_schema_api(fieldtype_proto.map_type.key_type),
            fieldtype_from_schema_api(fieldtype_proto.map_type.value_type)
        )
      elif type_info == "row_type":
        return RowType(
            Schema.from_schema_api(fieldtype_proto.row_type.schema))
      elif type_info == "logical_type":
        pass  # TODO

    types = (fieldtype_from_schema_api(field.type) for field in fields)
    return Schema(names=names, types=types)

  def __eq__(self, other):
    return len(other.type_mapping_) == len(self.type_mapping_) and \
        all(this_tuple == other_tuple
                                   for this_tuple, other_tuple in zip(
                                       six.iteritems(self.type_mapping_),
                                       six.iteritems(other.type_mapping_)))


# throw an exception if the given type mapping is invalid

ALLOWED_PRIMITIVES = (
    np.byte,
    np.int16,
    np.int32,
    np.int64,
    np.float32,
    np.float64,
    np.unicode,
    np.bytes_,
)


class ListType(object):

  def __init__(self, element_type):
    self.element_type = element_type

  def __eq__(self, other):
    return self.element_type == other.element_type

  def to_schema_api(self):
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(
            element_type=fieldtype_to_schema_api(self.element_type)))


class MapType(object):

  def __init__(self, key_type, value_type):
    self.key_type = key_type
    self.value_type = value_type

  def __eq__(self, other):
    return self.key_type == other.key_type and self.value_type == other.value_type

  def to_schema_api(self):
    return schema_pb2.FieldType(
        map_type=schema_pb2.MapType(
            key_type=fieldtype_to_schema_api(self.key_type),
            value_type=fieldtype_to_schema_api(self.value_type)))

class RowType(object):

  def __init__(self, schema):
    self.schema = schema

  def __eq__(self, other):
    return self.schema == other.schema

  def to_schema_api(self):
    return schema_pb2.FieldType(
        row_type=schema_pb2.RowType(
            schema=fieldtype_to_schema_api(self.schema)))


class LogicalType(object):

  def __init__(self, representation, urn, args):
    self.representation = representation
    self.urn = urn
    self.args = args


def validate_type_mapping(type_mapping, root_name=None):
  for name, type_ in six.iteritems(type_mapping):
    if root_name is not None:
      name = "{0}.{1}".format(root_name, name)
    validate_type(type_, name=name)


def validate_type(type_, name=None):
  if isinstance(type_, dict):
    validate_type_mapping(type_, name)
  elif isinstance(type_, ListType):
    validate_type(type_.element_type, name + "[element]")
  elif isinstance(type_, MapType):
    validate_type(type_.key_type, name + "[key]")
    validate_type(type_.value_type, name + "[value]")
  elif isinstance(type_, type):
    if not type_ in ALLOWED_PRIMITIVES:
      raise TypeError("Disallowed primitive type: {0}".format(type_))
  else:
    raise TypeError(
        "Encountered unknown type for field with name \"{0}\": {1}".format(
            name, type_))


def fieldtype_to_schema_api(type_):
  if isinstance(type_, type):

    def primitive_to_schema_api(type_):
      if type_ == np.byte:
        return schema_pb2.AtomicType.BYTE
      elif type_ == np.int16:
        return schema_pb2.AtomicType.INT16
      elif type_ == np.int32:
        return schema_pb2.AtomicType.INT32
      elif type_ == np.int64:
        return schema_pb2.AtomicType.INT64
      elif type_ == np.float32:
        return schema_pb2.AtomicType.FLOAT
      elif type_ == np.float64:
        return schema_pb2.AtomicType.DOUBLE
      elif type_ == np.unicode:
        return schema_pb2.AtomicType.STRING
      elif type_ == np.bool:
        return schema_pb2.AtomicType.BOOLEAN
      elif type_ == np.bytes_:
        return schema_pb2.AtomicType.BYTES
      else:
        raise TypeError(
            "Encountered unexpected primitive type: {0}".format(type_))

    return schema_pb2.FieldType(atomic_type=primitive_to_schema_api(type_))
  else:
    return type_.to_schema_api()
