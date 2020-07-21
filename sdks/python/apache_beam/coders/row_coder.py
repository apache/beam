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

from __future__ import absolute_import

import itertools
from array import array

from apache_beam.coders import typecoders
from apache_beam.coders.coder_impl import StreamCoderImpl
from apache_beam.coders.coders import BooleanCoder
from apache_beam.coders.coders import BytesCoder
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import FastCoder
from apache_beam.coders.coders import FloatCoder
from apache_beam.coders.coders import IterableCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import TupleCoder
from apache_beam.coders.coders import VarIntCoder
from apache_beam.portability import common_urns
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import row_type
from apache_beam.typehints.schemas import named_fields_to_schema
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.utils import proto_utils

__all__ = ["RowCoder"]


class RowCoder(FastCoder):
  """ Coder for `typing.NamedTuple` instances.

  Implements the beam:coder:row:v1 standard coder spec.
  """
  def __init__(self, schema):
    """Initializes a :class:`RowCoder`.

    Args:
      schema (apache_beam.portability.api.schema_pb2.Schema): The protobuf
        representation of the schema of the data that the RowCoder will be used
        to encode/decode.
    """
    self.schema = schema
    self.components = [
        RowCoder.coder_from_type(field.type) for field in self.schema.fields
    ]

  def _create_impl(self):
    return RowCoderImpl(self.schema, self.components)

  def is_deterministic(self):
    return all(c.is_deterministic() for c in self.components)

  def to_type_hint(self):
    return named_tuple_from_schema(self.schema)

  def __hash__(self):
    return hash(self.schema.SerializeToString())

  def __eq__(self, other):
    return type(self) == type(other) and self.schema == other.schema

  def to_runner_api_parameter(self, unused_context):
    return (common_urns.coders.ROW.urn, self.schema, [])

  @staticmethod
  @Coder.register_urn(common_urns.coders.ROW.urn, schema_pb2.Schema)
  def from_runner_api_parameter(schema, components, unused_context):
    return RowCoder(schema)

  @staticmethod
  def from_type_hint(type_hint, registry):
    if isinstance(type_hint, row_type.RowTypeConstraint):
      schema = named_fields_to_schema(type_hint._fields)
    else:
      schema = named_tuple_to_schema(type_hint)
    return RowCoder(schema)

  @staticmethod
  def from_payload(payload):
    # type: (bytes) -> RowCoder
    return RowCoder(proto_utils.parse_Bytes(payload, schema_pb2.Schema))

  @staticmethod
  def coder_from_type(field_type):
    type_info = field_type.WhichOneof("type_info")
    if type_info == "atomic_type":
      if field_type.atomic_type in (schema_pb2.INT32, schema_pb2.INT64):
        return VarIntCoder()
      elif field_type.atomic_type == schema_pb2.DOUBLE:
        return FloatCoder()
      elif field_type.atomic_type == schema_pb2.STRING:
        return StrUtf8Coder()
      elif field_type.atomic_type == schema_pb2.BOOLEAN:
        return BooleanCoder()
      elif field_type.atomic_type == schema_pb2.BYTES:
        return BytesCoder()
    elif type_info == "array_type":
      return IterableCoder(
          RowCoder.coder_from_type(field_type.array_type.element_type))
    elif type_info == "row_type":
      return RowCoder(field_type.row_type.schema)

    # The Java SDK supports several more types, but the coders are not yet
    # standard, and are not implemented in Python.
    raise ValueError(
        "Encountered a type that is not currently supported by RowCoder: %s" %
        field_type)

  def __reduce__(self):
    # when pickling, use bytes representation of the schema. schema_pb2.Schema
    # objects cannot be pickled.
    return (RowCoder.from_payload, (self.schema.SerializeToString(), ))


typecoders.registry.register_coder(row_type.RowTypeConstraint, RowCoder)


class RowCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  SIZE_CODER = VarIntCoder().get_impl()
  NULL_MARKER_CODER = BytesCoder().get_impl()

  def __init__(self, schema, components):
    self.schema = schema
    self.constructor = named_tuple_from_schema(schema)
    self.components = list(c.get_impl() for c in components)
    self.has_nullable_fields = any(
        field.type.nullable for field in self.schema.fields)

  def encode_to_stream(self, value, out, nested):
    nvals = len(self.schema.fields)
    self.SIZE_CODER.encode_to_stream(nvals, out, True)
    attrs = [getattr(value, f.name) for f in self.schema.fields]

    words = array('B')
    if self.has_nullable_fields:
      nulls = list(attr is None for attr in attrs)
      if any(nulls):
        words = array('B', itertools.repeat(0, (nvals + 7) // 8))
        for i, is_null in enumerate(nulls):
          words[i // 8] |= is_null << (i % 8)

    self.NULL_MARKER_CODER.encode_to_stream(words.tostring(), out, True)

    for c, field, attr in zip(self.components, self.schema.fields, attrs):
      if attr is None:
        if not field.type.nullable:
          raise ValueError(
              "Attempted to encode null for non-nullable field \"{}\".".format(
                  field.name))
        continue
      c.encode_to_stream(attr, out, True)

  def decode_from_stream(self, in_stream, nested):
    nvals = self.SIZE_CODER.decode_from_stream(in_stream, True)
    words = array('B')
    words.fromstring(self.NULL_MARKER_CODER.decode_from_stream(in_stream, True))

    if words:
      nulls = ((words[i // 8] >> (i % 8)) & 0x01 for i in range(nvals))
    else:
      nulls = itertools.repeat(False, nvals)

    # If this coder's schema has more attributes than the encoded value, then
    # the schema must have changed. Populate the unencoded fields with nulls.
    if len(self.components) > nvals:
      nulls = itertools.chain(
          nulls, itertools.repeat(True, len(self.components) - nvals))

    # Note that if this coder's schema has *fewer* attributes than the encoded
    # value, we just need to ignore the additional values, which will occur
    # here because we only decode as many values as we have coders for.
    return self.constructor(
        *(
            None if is_null else c.decode_from_stream(in_stream, True) for c,
            is_null in zip(self.components, nulls)))

  def _make_value_coder(self, nulls=itertools.repeat(False)):
    components = [
        component for component,
        is_null in zip(self.components, nulls) if not is_null
    ] if self.has_nullable_fields else self.components
    return TupleCoder(components).get_impl()
