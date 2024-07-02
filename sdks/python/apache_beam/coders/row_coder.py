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

from apache_beam.coders import typecoders
from apache_beam.coders.coder_impl import LogicalTypeCoderImpl
from apache_beam.coders.coder_impl import RowCoderImpl
from apache_beam.coders.coders import BigEndianShortCoder
from apache_beam.coders.coders import BooleanCoder
from apache_beam.coders.coders import BytesCoder
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import DecimalCoder
from apache_beam.coders.coders import FastCoder
from apache_beam.coders.coders import FloatCoder
from apache_beam.coders.coders import IterableCoder
from apache_beam.coders.coders import MapCoder
from apache_beam.coders.coders import NullableCoder
from apache_beam.coders.coders import SinglePrecisionFloatCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import TimestampCoder
from apache_beam.coders.coders import VarIntCoder
from apache_beam.portability import common_urns
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import row_type
from apache_beam.typehints.schemas import PYTHON_ANY_URN
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import schema_from_element_type
from apache_beam.utils import proto_utils

__all__ = ["RowCoder"]


class RowCoder(FastCoder):
  """ Coder for `typing.NamedTuple` instances.

  Implements the beam:coder:row:v1 standard coder spec.
  """
  def __init__(self, schema, force_deterministic=False):
    """Initializes a :class:`RowCoder`.

    Args:
      schema (apache_beam.portability.api.schema_pb2.Schema): The protobuf
        representation of the schema of the data that the RowCoder will be used
        to encode/decode.
    """
    self.schema = schema

    # Eagerly generate type hint to escalate any issues with the Schema proto
    self._type_hint = named_tuple_from_schema(self.schema)

    # Use non-null coders because null values are represented separately
    self.components = [
        _nonnull_coder_from_type(field.type) for field in self.schema.fields
    ]
    if force_deterministic:
      self.components = [
          c.as_deterministic_coder(force_deterministic) for c in self.components
      ]
    self.forced_deterministic = bool(force_deterministic)

  def _create_impl(self):
    return RowCoderImpl(self.schema, self.components)

  def is_deterministic(self):
    return all(c.is_deterministic() for c in self.components)

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      return RowCoder(self.schema, error_message or step_label)

  def to_type_hint(self):
    return self._type_hint

  def __hash__(self):
    return hash(self.schema.SerializeToString())

  def __eq__(self, other):
    return (
        type(self) == type(other) and self.schema == other.schema and
        self.forced_deterministic == other.forced_deterministic)

  def to_runner_api_parameter(self, unused_context):
    return (common_urns.coders.ROW.urn, self.schema, [])

  @staticmethod
  @Coder.register_urn(common_urns.coders.ROW.urn, schema_pb2.Schema)
  def from_runner_api_parameter(schema, components, unused_context):
    return RowCoder(schema)

  @classmethod
  def from_type_hint(cls, type_hint, registry):
    # TODO(https://github.com/apache/beam/issues/21541): Remove once all
    # runners are portable.
    if isinstance(type_hint, str):
      import importlib
      main_module = importlib.import_module('__main__')
      type_hint = getattr(main_module, type_hint, type_hint)
    schema = schema_from_element_type(type_hint)
    return cls(schema)

  @staticmethod
  def from_payload(payload: bytes) -> 'RowCoder':
    return RowCoder(proto_utils.parse_Bytes(payload, schema_pb2.Schema))

  def __reduce__(self):
    # when pickling, use bytes representation of the schema. schema_pb2.Schema
    # objects cannot be pickled.
    return (RowCoder.from_payload, (self.schema.SerializeToString(), ))


typecoders.registry.register_coder(row_type.RowTypeConstraint, RowCoder)
typecoders.registry.register_coder(
    row_type.GeneratedClassRowTypeConstraint, RowCoder)


def _coder_from_type(field_type):
  coder = _nonnull_coder_from_type(field_type)
  if field_type.nullable:
    return NullableCoder(coder)
  else:
    return coder


def _nonnull_coder_from_type(field_type):
  type_info = field_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    if field_type.atomic_type in (schema_pb2.INT32, schema_pb2.INT64):
      return VarIntCoder()
    if field_type.atomic_type == schema_pb2.INT16:
      return BigEndianShortCoder()
    elif field_type.atomic_type == schema_pb2.FLOAT:
      return SinglePrecisionFloatCoder()
    elif field_type.atomic_type == schema_pb2.DOUBLE:
      return FloatCoder()
    elif field_type.atomic_type == schema_pb2.STRING:
      return StrUtf8Coder()
    elif field_type.atomic_type == schema_pb2.BOOLEAN:
      return BooleanCoder()
    elif field_type.atomic_type == schema_pb2.BYTES:
      return BytesCoder()
  elif type_info == "array_type":
    return IterableCoder(_coder_from_type(field_type.array_type.element_type))
  elif type_info == "map_type":
    return MapCoder(
        _coder_from_type(field_type.map_type.key_type),
        _coder_from_type(field_type.map_type.value_type))
  elif type_info == "logical_type":
    if field_type.logical_type.urn == PYTHON_ANY_URN:
      # Special case for the Any logical type. Just use the default coder for an
      # unknown Python object.
      return typecoders.registry.get_coder(object)
    elif field_type.logical_type.urn == common_urns.millis_instant.urn:
      # Special case for millis instant logical type used to handle Java sdk's
      # millis Instant. It explicitly uses TimestampCoder which deals with fix
      # length 8-bytes big-endian-long instead of VarInt coder.
      return TimestampCoder()
    elif field_type.logical_type.urn == 'beam:logical_type:decimal:v1':
      return DecimalCoder()

    logical_type = LogicalType.from_runner_api(field_type.logical_type)
    return LogicalTypeCoder(
        logical_type, _coder_from_type(field_type.logical_type.representation))
  elif type_info == "row_type":
    return RowCoder(field_type.row_type.schema)

  # The Java SDK supports several more types, but the coders are not yet
  # standard, and are not implemented in Python.
  raise ValueError(
      "Encountered a type that is not currently supported by RowCoder: %s" %
      field_type)


class LogicalTypeCoder(FastCoder):
  def __init__(self, logical_type, representation_coder):
    self.logical_type = logical_type
    self.representation_coder = representation_coder

  def _create_impl(self):
    return LogicalTypeCoderImpl(self.logical_type, self.representation_coder)

  def is_deterministic(self):
    return self.representation_coder.is_deterministic()

  def to_type_hint(self):
    return self.logical_type.language_type()
