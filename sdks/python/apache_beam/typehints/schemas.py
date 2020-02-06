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

""" Support for mapping python types to proto Schemas and back again.

Python              Schema
np.int8     <-----> BYTE
np.int16    <-----> INT16
np.int32    <-----> INT32
np.int64    <-----> INT64
int         ---/
np.float32  <-----> FLOAT
np.float64  <-----> DOUBLE
float       ---/
bool        <-----> BOOLEAN

The mappings for STRING and BYTES are different between python 2 and python 3,
because of the changes to str:
py3:
str/unicode <-----> STRING
bytes       <-----> BYTES
ByteString  ---/

py2:
str will be rejected since it is ambiguous.
unicode     <-----> STRING
ByteString  <-----> BYTES
"""

# pytype: skip-file

from __future__ import absolute_import

import sys
from typing import ByteString
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from uuid import uuid4

import numpy as np
from past.builtins import unicode

from apache_beam.portability.api import schema_pb2
from apache_beam.typehints.native_type_compatibility import _get_args
from apache_beam.typehints.native_type_compatibility import _match_is_exactly_mapping
from apache_beam.typehints.native_type_compatibility import _match_is_named_tuple
from apache_beam.typehints.native_type_compatibility import _match_is_optional
from apache_beam.typehints.native_type_compatibility import _safe_issubclass
from apache_beam.typehints.native_type_compatibility import extract_optional_type


# Registry of typings for a schema by UUID
class SchemaTypeRegistry(object):
  def __init__(self):
    self.by_id = {}
    self.by_typing = {}

  def add(self, typing, schema):
    self.by_id[schema.id] = (typing, schema)

  def get_typing_by_id(self, unique_id):
    result = self.by_id.get(unique_id, None)
    return result[0] if result is not None else None

  def get_schema_by_id(self, unique_id):
    result = self.by_id.get(unique_id, None)
    return result[1] if result is not None else None


SCHEMA_REGISTRY = SchemaTypeRegistry()

# Bi-directional mappings
_PRIMITIVES = (
    (np.int8, schema_pb2.BYTE),
    (np.int16, schema_pb2.INT16),
    (np.int32, schema_pb2.INT32),
    (np.int64, schema_pb2.INT64),
    (np.float32, schema_pb2.FLOAT),
    (np.float64, schema_pb2.DOUBLE),
    (unicode, schema_pb2.STRING),
    (bool, schema_pb2.BOOLEAN),
    (bytes if sys.version_info.major >= 3 else ByteString, schema_pb2.BYTES),
)

PRIMITIVE_TO_ATOMIC_TYPE = dict((typ, atomic) for typ, atomic in _PRIMITIVES)
ATOMIC_TYPE_TO_PRIMITIVE = dict((atomic, typ) for typ, atomic in _PRIMITIVES)

# One-way mappings
PRIMITIVE_TO_ATOMIC_TYPE.update({
    # In python 2, this is a no-op because we define it as the bi-directional
    # mapping above. This just ensures the one-way mapping is defined in python
    # 3.
    ByteString: schema_pb2.BYTES,
    # Allow users to specify a native int, and use INT64 as the cross-language
    # representation. Technically ints have unlimited precision, but RowCoder
    # should throw an error if it sees one with a bit width > 64 when encoding.
    int: schema_pb2.INT64,
    float: schema_pb2.DOUBLE,
})


def typing_to_runner_api(type_):
  if _match_is_named_tuple(type_):
    schema = None
    if hasattr(type_, 'id'):
      schema = SCHEMA_REGISTRY.get_schema_by_id(type_.id)
    if schema is None:
      fields = [
          schema_pb2.Field(
              name=name, type=typing_to_runner_api(type_._field_types[name]))
          for name in type_._fields
      ]
      type_id = str(uuid4())
      schema = schema_pb2.Schema(fields=fields, id=type_id)
      SCHEMA_REGISTRY.add(type_, schema)

    return schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=schema))

  # All concrete types (other than NamedTuple sub-classes) should map to
  # a supported primitive type.
  elif type_ in PRIMITIVE_TO_ATOMIC_TYPE:
    return schema_pb2.FieldType(atomic_type=PRIMITIVE_TO_ATOMIC_TYPE[type_])

  elif sys.version_info.major == 2 and type_ == str:
    raise ValueError(
        "type 'str' is not supported in python 2. Please use 'unicode' or "
        "'typing.ByteString' instead to unambiguously indicate if this is a "
        "UTF-8 string or a byte array.")

  elif _match_is_exactly_mapping(type_):
    key_type, value_type = map(typing_to_runner_api, _get_args(type_))
    return schema_pb2.FieldType(
        map_type=schema_pb2.MapType(key_type=key_type, value_type=value_type))

  elif _match_is_optional(type_):
    # It's possible that a user passes us Optional[Optional[T]], but in python
    # typing this is indistinguishable from Optional[T] - both resolve to
    # Union[T, None] - so there's no need to check for that case here.
    result = typing_to_runner_api(extract_optional_type(type_))
    result.nullable = True
    return result

  elif _safe_issubclass(type_, Sequence):
    element_type = typing_to_runner_api(_get_args(type_)[0])
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(element_type=element_type))

  raise ValueError("Unsupported type: %s" % type_)


def typing_from_runner_api(fieldtype_proto):
  if fieldtype_proto.nullable:
    # In order to determine the inner type, create a copy of fieldtype_proto
    # with nullable=False and pass back to typing_from_runner_api
    base_type = schema_pb2.FieldType()
    base_type.CopyFrom(fieldtype_proto)
    base_type.nullable = False
    return Optional[typing_from_runner_api(base_type)]

  type_info = fieldtype_proto.WhichOneof("type_info")
  if type_info == "atomic_type":
    try:
      return ATOMIC_TYPE_TO_PRIMITIVE[fieldtype_proto.atomic_type]
    except KeyError:
      raise ValueError(
          "Unsupported atomic type: {0}".format(fieldtype_proto.atomic_type))
  elif type_info == "array_type":
    return Sequence[typing_from_runner_api(
        fieldtype_proto.array_type.element_type)]
  elif type_info == "map_type":
    return Mapping[typing_from_runner_api(fieldtype_proto.map_type.key_type),
                   typing_from_runner_api(fieldtype_proto.map_type.value_type)]
  elif type_info == "row_type":
    schema = fieldtype_proto.row_type.schema
    user_type = SCHEMA_REGISTRY.get_typing_by_id(schema.id)
    if user_type is None:
      from apache_beam import coders
      type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
      user_type = NamedTuple(
          type_name,
          [(field.name, typing_from_runner_api(field.type))
           for field in schema.fields])
      user_type.id = schema.id
      SCHEMA_REGISTRY.add(user_type, schema)
      coders.registry.register_coder(user_type, coders.RowCoder)
    return user_type

  elif type_info == "logical_type":
    pass  # TODO


def named_tuple_from_schema(schema):
  return typing_from_runner_api(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=schema)))


def named_tuple_to_schema(named_tuple):
  return typing_to_runner_api(named_tuple).row_type.schema
