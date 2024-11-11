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

Imposes a mapping between common Python types and Beam portable schemas
(https://s.apache.org/beam-schemas)::

  Python              Schema
  np.int8     <-----> BYTE
  np.int16    <-----> INT16
  np.int32    <-----> INT32
  np.int64    <-----> INT64
  int         ------> INT64
  np.float32  <-----> FLOAT
  np.float64  <-----> DOUBLE
  float       ------> DOUBLE
  bool        <-----> BOOLEAN
  str         <-----> STRING
  bytes       <-----> BYTES
  ByteString  ------> BYTES
  Timestamp   <-----> LogicalType(urn="beam:logical_type:micros_instant:v1")
  Decimal     <-----> LogicalType(urn="beam:logical_type:fixed_decimal:v1")
  Mapping     <-----> MapType
  Sequence    <-----> ArrayType
  NamedTuple  <-----> RowType
  beam.Row    ------> RowType

One direction mapping of Python types from Beam portable schemas:

  bytes
    <------ LogicalType(urn="beam:logical_type:fixed_bytes:v1")
    <------ LogicalType(urn="beam:logical_type:var_bytes:v1")
  str
    <------ LogicalType(urn="beam:logical_type:fixed_char:v1")
    <------ LogicalType(urn="beam:logical_type:var_char:v1")
  Timestamp
    <------ LogicalType(urn="beam:logical_type:millis_instant:v1")

Note that some of these mappings are provided as conveniences,
but they are lossy and will not survive a roundtrip from python to Beam schemas
and back. For example, the Python type :code:`int` will map to :code:`INT64` in
Beam schemas but converting that back to a Python type will yield
:code:`np.int64`.

:code:`nullable=True` on a Beam :code:`FieldType` is represented in Python by
wrapping the type in :code:`Optional`.

This module is intended for internal use only. Nothing defined here provides
any backwards-compatibility guarantee.
"""

# pytype: skip-file

import decimal
import logging
from typing import Any
from typing import ByteString
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import numpy as np
from google.protobuf import text_format

from apache_beam.portability import common_urns
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import _get_args
from apache_beam.typehints.native_type_compatibility import _match_is_exactly_mapping
from apache_beam.typehints.native_type_compatibility import _match_is_optional
from apache_beam.typehints.native_type_compatibility import _safe_issubclass
from apache_beam.typehints.native_type_compatibility import convert_to_typing_type
from apache_beam.typehints.native_type_compatibility import extract_optional_type
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple
from apache_beam.typehints.schema_registry import SCHEMA_REGISTRY
from apache_beam.typehints.schema_registry import SchemaTypeRegistry
from apache_beam.utils import proto_utils
from apache_beam.utils.python_callable import PythonCallableWithSource
from apache_beam.utils.timestamp import Timestamp

PYTHON_ANY_URN = "beam:logical:pythonsdk_any:v1"

# Bi-directional mappings
_PRIMITIVES = (
    (np.int8, schema_pb2.BYTE),
    (np.int16, schema_pb2.INT16),
    (np.int32, schema_pb2.INT32),
    (np.int64, schema_pb2.INT64),
    (np.float32, schema_pb2.FLOAT),
    (np.float64, schema_pb2.DOUBLE),
    (str, schema_pb2.STRING),
    (bool, schema_pb2.BOOLEAN),
    (bytes, schema_pb2.BYTES),
)

PRIMITIVE_TO_ATOMIC_TYPE = dict((typ, atomic) for typ, atomic in _PRIMITIVES)
ATOMIC_TYPE_TO_PRIMITIVE = dict((atomic, typ) for typ, atomic in _PRIMITIVES)

# One-way mappings
PRIMITIVE_TO_ATOMIC_TYPE.update({
    ByteString: schema_pb2.BYTES,
    # Allow users to specify a native int, and use INT64 as the cross-language
    # representation. Technically ints have unlimited precision, but RowCoder
    # should throw an error if it sees one with a bit width > 64 when encoding.
    int: schema_pb2.INT64,
    float: schema_pb2.DOUBLE,
})

_LOGGER = logging.getLogger(__name__)

# Serialized schema_pb2.Schema w/o id to id.
_SCHEMA_ID_CACHE = {}


def named_fields_to_schema(
    names_and_types: Union[Dict[str, type], Sequence[Tuple[str, type]]],
    schema_id: Optional[str] = None,
    schema_options: Optional[Sequence[Tuple[str, Any]]] = None,
    field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]] = None,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY,
):
  schema_options = schema_options or []
  field_options = field_options or {}

  if isinstance(names_and_types, dict):
    names_and_types = names_and_types.items()

  schema = schema_pb2.Schema(
      fields=[
          schema_pb2.Field(
              name=name,
              type=typing_to_runner_api(type),
              options=[
                  option_to_runner_api(option_tuple)
                  for option_tuple in field_options.get(name, [])
              ],
          ) for (name, type) in names_and_types
      ],
      options=[
          option_to_runner_api(option_tuple) for option_tuple in schema_options
      ])

  if schema_id is None:
    key = schema.SerializeToString()
    if key not in _SCHEMA_ID_CACHE:
      _SCHEMA_ID_CACHE[key] = schema_registry.generate_new_id()
    schema_id = _SCHEMA_ID_CACHE[key]

  schema.id = schema_id
  return schema


def named_fields_from_schema(
    schema):  # (schema_pb2.Schema) -> typing.List[typing.Tuple[str, type]]
  return [(field.name, typing_from_runner_api(field.type))
          for field in schema.fields]


def typing_to_runner_api(
    type_: type,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY
) -> schema_pb2.FieldType:
  return SchemaTranslation(
      schema_registry=schema_registry).typing_to_runner_api(type_)


def typing_from_runner_api(
    fieldtype_proto: schema_pb2.FieldType,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY) -> type:
  return SchemaTranslation(
      schema_registry=schema_registry).typing_from_runner_api(fieldtype_proto)


def value_to_runner_api(
    type_proto: schema_pb2.FieldType,
    value,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY
) -> schema_pb2.FieldValue:
  return SchemaTranslation(schema_registry=schema_registry).value_to_runner_api(
      type_proto, value)


def value_from_runner_api(
    type_proto: schema_pb2.FieldType,
    value_proto: schema_pb2.FieldValue,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY
) -> schema_pb2.FieldValue:
  return SchemaTranslation(
      schema_registry=schema_registry).value_from_runner_api(
          type_proto, value_proto)


def option_to_runner_api(
    option: Tuple[str, Any],
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY) -> schema_pb2.Option:
  return SchemaTranslation(
      schema_registry=schema_registry).option_to_runner_api(option)


def option_from_runner_api(
    option_proto: schema_pb2.Option,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY) -> Tuple[str, Any]:
  return SchemaTranslation(
      schema_registry=schema_registry).option_from_runner_api(option_proto)


def schema_field(
    name: str,
    field_type: Union[schema_pb2.FieldType, type],
    description: Optional[str] = None) -> schema_pb2.Field:
  return schema_pb2.Field(
      name=name,
      type=field_type if isinstance(field_type, schema_pb2.FieldType) else
      typing_to_runner_api(field_type),
      description=description)


class SchemaTranslation(object):
  def __init__(self, schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY):
    self.schema_registry = schema_registry

  def typing_to_runner_api(self, type_: type) -> schema_pb2.FieldType:
    if isinstance(type_, schema_pb2.Schema):
      return schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=type_))

    if hasattr(type_, '_beam_schema_proto') and type_._beam_schema_proto.obj:
      return schema_pb2.FieldType(
          row_type=schema_pb2.RowType(schema=type_._beam_schema_proto.obj))

    if isinstance(type_, row_type.RowTypeConstraint):
      if type_.schema_id is None:
        schema_id = SCHEMA_REGISTRY.generate_new_id()
        type_.set_schema_id(schema_id)
        schema = None
      else:
        schema_id = type_.schema_id
        schema = self.schema_registry.get_schema_by_id(schema_id)

      if schema is None:
        # Either user_type was not annotated with a schema id, or there was
        # no schema in the registry with the id. The latter should only happen
        # in tests.
        # Either way, we need to generate a new schema proto.
        schema = schema_pb2.Schema(
            fields=[
                schema_pb2.Field(
                    name=field_name,
                    type=self.typing_to_runner_api(field_type),
                    options=[
                        self.option_to_runner_api(option_tuple)
                        for option_tuple in type_.field_options(field_name)
                    ],
                    description=type_._field_descriptions.get(field_name, None),
                ) for (field_name, field_type) in type_._fields
            ],
            id=schema_id,
            options=[
                self.option_to_runner_api(option_tuple)
                for option_tuple in type_.schema_options
            ],
        )
        self.schema_registry.add(type_.user_type, schema)
      return schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=schema))
    else:
      # See if this is coercible to a RowTypeConstraint (e.g. a NamedTuple or
      # dataclass)
      row_type_constraint = row_type.RowTypeConstraint.from_user_type(type_)
      if row_type_constraint is not None:
        return self.typing_to_runner_api(row_type_constraint)

    if isinstance(type_, typehints.TypeConstraint):
      type_ = convert_to_typing_type(type_)

    # All concrete types (other than NamedTuple sub-classes) should map to
    # a supported primitive type.
    if type_ in PRIMITIVE_TO_ATOMIC_TYPE:
      return schema_pb2.FieldType(atomic_type=PRIMITIVE_TO_ATOMIC_TYPE[type_])

    elif _match_is_exactly_mapping(type_):
      key_type, value_type = map(self.typing_to_runner_api, _get_args(type_))
      return schema_pb2.FieldType(
          map_type=schema_pb2.MapType(key_type=key_type, value_type=value_type))

    elif _match_is_optional(type_):
      # It's possible that a user passes us Optional[Optional[T]], but in python
      # typing this is indistinguishable from Optional[T] - both resolve to
      # Union[T, None] - so there's no need to check for that case here.
      result = self.typing_to_runner_api(extract_optional_type(type_))
      result.nullable = True
      return result

    elif type_ == range:
      return schema_pb2.FieldType(
          array_type=schema_pb2.ArrayType(
              element_type=schema_pb2.FieldType(
                  atomic_type=PRIMITIVE_TO_ATOMIC_TYPE[int])))

    elif _safe_issubclass(type_, Sequence) and not _safe_issubclass(type_, str):
      element_type = self.typing_to_runner_api(_get_args(type_)[0])
      return schema_pb2.FieldType(
          array_type=schema_pb2.ArrayType(element_type=element_type))

    elif _safe_issubclass(type_, Mapping):
      key_type, value_type = map(self.typing_to_runner_api, _get_args(type_))
      return schema_pb2.FieldType(
          map_type=schema_pb2.MapType(key_type=key_type, value_type=value_type))

    elif _safe_issubclass(type_, Iterable) and not _safe_issubclass(type_, str):
      element_type = self.typing_to_runner_api(_get_args(type_)[0])
      return schema_pb2.FieldType(
          array_type=schema_pb2.ArrayType(element_type=element_type))

    try:
      logical_type = LogicalType.from_typing(type_)
    except ValueError:
      # Unknown type, just treat it like Any
      return schema_pb2.FieldType(
          logical_type=schema_pb2.LogicalType(urn=PYTHON_ANY_URN),
          nullable=True)
    else:
      argument_type = None
      argument = None
      if logical_type.argument_type() is not None:
        argument_type = self.typing_to_runner_api(logical_type.argument_type())
        try:
          argument = self.value_to_runner_api(
              argument_type, logical_type.argument())
        except ValueError:
          # TODO(https://github.com/apache/beam/issues/23373): Complete support
          # for logical types that require arguments beyond atomic type.
          # For now, skip arguments.
          argument = None
      return schema_pb2.FieldType(
          logical_type=schema_pb2.LogicalType(
              urn=logical_type.urn(),
              representation=self.typing_to_runner_api(
                  logical_type.representation_type()),
              argument_type=argument_type,
              argument=argument))

  def atomic_value_from_runner_api(
      self,
      atomic_type: schema_pb2.AtomicType,
      atomic_value: schema_pb2.AtomicTypeValue):
    if atomic_type == schema_pb2.BYTE:
      value = np.int8(atomic_value.byte)
    elif atomic_type == schema_pb2.INT16:
      value = np.int16(atomic_value.int16)
    elif atomic_type == schema_pb2.INT32:
      value = np.int32(atomic_value.int32)
    elif atomic_type == schema_pb2.INT64:
      value = np.int64(atomic_value.int64)
    elif atomic_type == schema_pb2.FLOAT:
      value = np.float32(atomic_value.float)
    elif atomic_type == schema_pb2.DOUBLE:
      value = np.float64(atomic_value.double)
    elif atomic_type == schema_pb2.STRING:
      value = atomic_value.string
    elif atomic_type == schema_pb2.BOOLEAN:
      value = atomic_value.boolean
    elif atomic_type == schema_pb2.BYTES:
      value = atomic_value.bytes
    else:
      raise ValueError(
          f"Unrecognized atomic_type ({atomic_type}) "
          f"when decoding value {atomic_value!r}")

    return value

  def atomic_value_to_runner_api(
      self, atomic_type: schema_pb2.AtomicType,
      value) -> schema_pb2.AtomicTypeValue:
    if atomic_type == schema_pb2.BYTE:
      atomic_value = schema_pb2.AtomicTypeValue(byte=value)
    elif atomic_type == schema_pb2.INT16:
      atomic_value = schema_pb2.AtomicTypeValue(int16=value)
    elif atomic_type == schema_pb2.INT32:
      atomic_value = schema_pb2.AtomicTypeValue(int32=value)
    elif atomic_type == schema_pb2.INT64:
      atomic_value = schema_pb2.AtomicTypeValue(int64=value)
    elif atomic_type == schema_pb2.FLOAT:
      atomic_value = schema_pb2.AtomicTypeValue(float=value)
    elif atomic_type == schema_pb2.DOUBLE:
      atomic_value = schema_pb2.AtomicTypeValue(double=value)
    elif atomic_type == schema_pb2.STRING:
      atomic_value = schema_pb2.AtomicTypeValue(string=value)
    elif atomic_type == schema_pb2.BOOLEAN:
      atomic_value = schema_pb2.AtomicTypeValue(boolean=value)
    elif atomic_type == schema_pb2.BYTES:
      atomic_value = schema_pb2.AtomicTypeValue(bytes=value)
    else:
      raise ValueError(
          "Unrecognized atomic_type {atomic_type} when encoding value {value}")

    return atomic_value

  def value_from_runner_api(
      self,
      type_proto: schema_pb2.FieldType,
      value_proto: schema_pb2.FieldValue):
    if type_proto.WhichOneof("type_info") != "atomic_type":
      # TODO: Allow other value types
      raise ValueError(
          "Encounterd option with unsupported type. Only "
          f"atomic_type options are supported: {type_proto}")

    value = self.atomic_value_from_runner_api(
        type_proto.atomic_type, value_proto.atomic_value)
    return value

  def value_to_runner_api(self, typing_proto: schema_pb2.FieldType, value):
    if typing_proto.WhichOneof("type_info") != "atomic_type":
      # TODO: Allow other value types
      raise ValueError(
          "Only atomic_type option values are currently supported in Python. "
          f"Got {value!r}, which maps to fieldtype {typing_proto!r}.")

    atomic_value = self.atomic_value_to_runner_api(
        typing_proto.atomic_type, value)
    value_proto = schema_pb2.FieldValue(atomic_value=atomic_value)
    return value_proto

  def option_from_runner_api(
      self, option_proto: schema_pb2.Option) -> Tuple[str, Any]:
    if not option_proto.HasField('type'):
      return option_proto.name, None

    value = self.value_from_runner_api(option_proto.type, option_proto.value)
    return option_proto.name, value

  def option_to_runner_api(self, option: Tuple[str, Any]) -> schema_pb2.Option:
    name, value = option

    if value is None:
      # a value of None indicates the option is just a flag.
      # Don't set type, value
      return schema_pb2.Option(name=name)

    type_proto = self.typing_to_runner_api(type(value))
    value_proto = self.value_to_runner_api(type_proto, value)
    return schema_pb2.Option(name=name, type=type_proto, value=value_proto)

  def typing_from_runner_api(
      self, fieldtype_proto: schema_pb2.FieldType) -> type:
    if fieldtype_proto.nullable:
      # In order to determine the inner type, create a copy of fieldtype_proto
      # with nullable=False and pass back to typing_from_runner_api
      base_type = schema_pb2.FieldType()
      base_type.CopyFrom(fieldtype_proto)
      base_type.nullable = False
      base = self.typing_from_runner_api(base_type)
      if base == Any:
        return base
      else:
        return Optional[base]

    type_info = fieldtype_proto.WhichOneof("type_info")
    if type_info == "atomic_type":
      try:
        return ATOMIC_TYPE_TO_PRIMITIVE[fieldtype_proto.atomic_type]
      except KeyError:
        raise ValueError(
            "Unsupported atomic type: {0}".format(fieldtype_proto.atomic_type))
    elif type_info == "array_type":
      return Sequence[self.typing_from_runner_api(
          fieldtype_proto.array_type.element_type)]
    elif type_info == "map_type":
      return Mapping[
          self.typing_from_runner_api(fieldtype_proto.map_type.key_type),
          self.typing_from_runner_api(fieldtype_proto.map_type.value_type)]
    elif type_info == "row_type":
      schema = fieldtype_proto.row_type.schema
      schema_options = [
          self.option_from_runner_api(option_proto)
          for option_proto in schema.options
      ]
      field_options = {
          field.name: [
              self.option_from_runner_api(option_proto)
              for option_proto in field.options
          ]
          for field in schema.fields if field.options
      }
      # First look for user type in the registry
      user_type = self.schema_registry.get_typing_by_id(schema.id)

      if user_type is None:
        # If not in SDK options (the coder likely came from another SDK),
        # generate a NamedTuple type to use.

        fields = named_fields_from_schema(schema)
        result = row_type.RowTypeConstraint.from_fields(
            fields=fields,
            schema_id=schema.id,
            schema_options=schema_options,
            field_options=field_options,
            schema_registry=self.schema_registry,
        )
        return result
      else:
        return row_type.RowTypeConstraint.from_user_type(
            user_type,
            schema_options=schema_options,
            field_options=field_options)

    elif type_info == "logical_type":
      if fieldtype_proto.logical_type.urn == PYTHON_ANY_URN:
        return Any
      else:
        return LogicalType.from_runner_api(
            fieldtype_proto.logical_type).language_type()

    else:
      raise ValueError(f"Unrecognized type_info: {type_info!r}")

  def named_tuple_from_schema(self, schema: schema_pb2.Schema) -> type:
    from apache_beam import coders

    type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))

    subfields = []
    descriptions = {}
    for field in schema.fields:
      try:
        field_py_type = self.typing_from_runner_api(field.type)
        if isinstance(field_py_type, row_type.RowTypeConstraint):
          field_py_type = field_py_type.user_type
      except ValueError as e:
        raise ValueError(
            "Failed to decode schema due to an issue with Field proto:\n\n"
            f"{text_format.MessageToString(field)}") from e

      descriptions[field.name] = field.description
      subfields.append((field.name, field_py_type))

    user_type = NamedTuple(type_name, subfields)

    # Define a reduce function, otherwise these types can't be pickled
    # (See BEAM-9574)
    setattr(
        user_type,
        '__reduce__',
        _named_tuple_reduce_method(schema.SerializeToString()))
    setattr(user_type, "_field_descriptions", descriptions)
    setattr(user_type, row_type._BEAM_SCHEMA_ID, schema.id)

    self.schema_registry.add(user_type, schema)
    coders.registry.register_coder(user_type, coders.RowCoder)

    return user_type


def _named_tuple_reduce_method(serialized_schema):
  def __reduce__(self):
    return _hydrate_namedtuple_instance, (serialized_schema, tuple(self))

  return __reduce__


def _hydrate_namedtuple_instance(encoded_schema, values):
  return named_tuple_from_schema(
      proto_utils.parse_Bytes(encoded_schema, schema_pb2.Schema))(*values)


def named_tuple_from_schema(
    schema, schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY) -> type:
  return SchemaTranslation(
      schema_registry=schema_registry).named_tuple_from_schema(schema)


def named_tuple_to_schema(
    named_tuple,
    schema_registry: SchemaTypeRegistry = SCHEMA_REGISTRY) -> schema_pb2.Schema:
  return typing_to_runner_api(named_tuple, schema_registry).row_type.schema


def schema_from_element_type(element_type: type) -> schema_pb2.Schema:
  """Get a schema for the given PCollection element_type.

  Returns schema as a list of (name, python_type) tuples"""
  if isinstance(element_type, row_type.RowTypeConstraint):
    return named_fields_to_schema(element_type._fields)
  elif match_is_named_tuple(element_type):
    return named_tuple_to_schema(element_type)
  else:
    raise TypeError(
        f"Could not determine schema for type hint {element_type!r}. Did you "
        "mean to create a schema-aware PCollection? See "
        "https://s.apache.org/beam-python-schemas")


def named_fields_from_element_type(
    element_type: type) -> List[Tuple[str, type]]:
  return named_fields_from_schema(schema_from_element_type(element_type))


def union_schema_type(element_types):
  """Returns a schema whose fields are the union of each corresponding field.

  element_types must be a set of schema-aware types whose fields have the
  same naming and ordering.
  """
  union_fields_and_types = []
  for field in zip(*[named_fields_from_element_type(t) for t in element_types]):
    names, types = zip(*field)
    name_set = set(names)
    if len(name_set) != 1:
      raise TypeError(
          f"Could not determine schema for type hints {element_types!r}: "
          f"Inconsistent names: {name_set}")
    union_fields_and_types.append(
        (next(iter(name_set)), typehints.Union[types]))
  return named_tuple_from_schema(named_fields_to_schema(union_fields_and_types))


class _Ephemeral:
  """Helper class for wrapping unpicklable objects."""
  def __init__(self, obj):
    self.obj = obj

  def __reduce__(self):
    return _Ephemeral, (None, )


# Registry of typings for a schema by UUID
class LogicalTypeRegistry(object):
  def __init__(self):
    self.by_urn = {}
    self.by_logical_type = {}
    self.by_language_type = {}

  def add(self, urn, logical_type):
    self.by_urn[urn] = logical_type
    self.by_logical_type[logical_type] = urn
    self.by_language_type[logical_type.language_type()] = logical_type

  def get_logical_type_by_urn(self, urn):
    return self.by_urn.get(urn, None)

  def get_urn_by_logial_type(self, logical_type):
    return self.by_logical_type.get(logical_type, None)

  def get_logical_type_by_language_type(self, representation_type):
    return self.by_language_type.get(representation_type, None)

  def copy(self):
    copy = LogicalTypeRegistry()
    copy.by_urn.update(self.by_urn)
    copy.by_logical_type.update(self.by_logical_type)
    copy.by_language_type.update(self.by_language_type)
    return copy


LanguageT = TypeVar('LanguageT')
RepresentationT = TypeVar('RepresentationT')
ArgT = TypeVar('ArgT')


class LogicalType(Generic[LanguageT, RepresentationT, ArgT]):
  _known_logical_types = LogicalTypeRegistry()

  @classmethod
  def urn(cls):
    # type: () -> str

    """Return the URN used to identify this logical type"""
    raise NotImplementedError()

  @classmethod
  def language_type(cls):
    # type: () -> type

    """Return the language type this LogicalType encodes.

    The returned type should match LanguageT"""
    raise NotImplementedError()

  @classmethod
  def representation_type(cls):
    # type: () -> type

    """Return the type of the representation this LogicalType uses to encode the
    language type.

    The returned type should match RepresentationT"""
    raise NotImplementedError()

  @classmethod
  def argument_type(cls):
    # type: () -> type

    """Return the type of the argument used for variations of this LogicalType.

    The returned type should match ArgT"""
    raise NotImplementedError(cls)

  def argument(self):
    # type: () -> ArgT

    """Return the argument for this instance of the LogicalType."""
    raise NotImplementedError()

  def to_representation_type(self, value):
    # type: (LanguageT) -> RepresentationT

    """Convert an instance of LanguageT to RepresentationT."""
    raise NotImplementedError()

  def to_language_type(self, value):
    # type: (RepresentationT) -> LanguageT

    """Convert an instance of RepresentationT to LanguageT."""
    raise NotImplementedError()

  @classmethod
  def register_logical_type(cls, logical_type_cls):
    """Register an implementation of LogicalType."""
    cls._known_logical_types.add(logical_type_cls.urn(), logical_type_cls)
    return logical_type_cls

  @classmethod
  def from_typing(cls, typ):
    # type: (type) -> LogicalType

    """Construct an instance of a registered LogicalType implementation given a
    typing.

    Raises ValueError if no registered LogicalType implementation can encode the
    given typing."""

    logical_type = cls._known_logical_types.get_logical_type_by_language_type(
        typ)
    if logical_type is None:
      raise ValueError("No logical type registered for typing '%s'" % typ)

    return logical_type._from_typing(typ)

  @classmethod
  def _from_typing(cls, typ):
    # type: (type) -> LogicalType

    """Construct an instance of this LogicalType implementation given a typing.
    """
    raise NotImplementedError()

  @classmethod
  def from_runner_api(cls, logical_type_proto):
    # type: (schema_pb2.LogicalType) -> LogicalType

    """Construct an instance of a registered LogicalType implementation given a
    proto LogicalType.

    Raises ValueError if no LogicalType registered for the given URN.
    """
    logical_type = cls._known_logical_types.get_logical_type_by_urn(
        logical_type_proto.urn)
    if logical_type is None:
      raise ValueError(
          "No logical type registered for URN '%s'" % logical_type_proto.urn)
    if not logical_type_proto.HasField(
        "argument_type") or not logical_type_proto.HasField("argument"):
      # logical type_proto without argument
      return logical_type()
    else:
      try:
        argument = value_from_runner_api(
            logical_type_proto.argument_type, logical_type_proto.argument)
      except ValueError:
        # TODO(https://github.com/apache/beam/issues/23373): Complete support
        # for logical types that require arguments beyond atomic type.
        # For now, skip arguments.
        _LOGGER.warning(
            'Logical type %s with argument is currently unsupported. '
            'Argument values are omitted',
            logical_type_proto.urn)
        return logical_type()
      return logical_type(argument)


class NoArgumentLogicalType(LogicalType[LanguageT, RepresentationT, None]):
  @classmethod
  def argument_type(cls):
    # type: () -> type
    return None

  def argument(self):
    # type: () -> ArgT
    return None

  @classmethod
  def _from_typing(cls, typ):
    # type: (type) -> LogicalType

    # Since there's no argument, there can be no additional information encoded
    # in the typing. Just construct an instance.
    return cls()


class PassThroughLogicalType(LogicalType[LanguageT, LanguageT, ArgT]):
  """A base class for LogicalTypes that use the same type as the underlying
  representation type.
  """
  def to_language_type(self, value):
    return value

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return cls.language_type()

  def to_representation_type(self, value):
    return value

  @classmethod
  def _from_typing(cls, typ):
    # type: (type) -> LogicalType
    # TODO(https://github.com/apache/beam/issues/23373): enable argument
    return cls()


MicrosInstantRepresentation = NamedTuple(
    'MicrosInstantRepresentation', [('seconds', np.int64),
                                    ('micros', np.int64)])


@LogicalType.register_logical_type
class MillisInstant(NoArgumentLogicalType[Timestamp, np.int64]):
  """Millisecond-precision instant logical type handles values consistent with
  that encoded by ``InstantCoder`` in the Java SDK.

  This class handles :class:`apache_beam.utils.timestamp.Timestamp` language
  type as :class:`MicrosInstant`, but it only provides millisecond precision,
  because it is aimed to handle data encoded by Java sdk's InstantCoder which
  has same precision level.

  Timestamp is handled by `MicrosInstant` by default. In some scenario, such as
  read from cross-language transform with rows containing InstantCoder encoded
  timestamps, one may need to override the mapping of Timetamp to MillisInstant.
  To do this, re-register this class with
  :func:`~LogicalType.register_logical_type`.
  """
  @classmethod
  def representation_type(cls):
    # type: () -> type
    return np.int64

  @classmethod
  def urn(cls):
    return common_urns.millis_instant.urn

  @classmethod
  def language_type(cls):
    return Timestamp

  def to_language_type(self, value):
    # type: (np.int64) -> Timestamp

    # value shifted as in apache_beams.coders.coder_impl.TimestampCoderImpl
    if value < 0:
      millis = int(value) + (1 << 63)
    else:
      millis = int(value) - (1 << 63)

    return Timestamp(micros=millis * 1000)


# Make sure MicrosInstant is registered after MillisInstant so that it
# overwrites the mapping of Timestamp language type representation choice and
# thus does not lose microsecond precision inside python sdk.
@LogicalType.register_logical_type
class MicrosInstant(NoArgumentLogicalType[Timestamp,
                                          MicrosInstantRepresentation]):
  """Microsecond-precision instant logical type that handles ``Timestamp``."""
  @classmethod
  def urn(cls):
    return common_urns.micros_instant.urn

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return MicrosInstantRepresentation

  @classmethod
  def language_type(cls):
    return Timestamp

  def to_representation_type(self, value):
    # type: (Timestamp) -> MicrosInstantRepresentation
    return MicrosInstantRepresentation(
        value.micros // 1000000, value.micros % 1000000)

  def to_language_type(self, value):
    # type: (MicrosInstantRepresentation) -> Timestamp
    return Timestamp(seconds=int(value.seconds), micros=int(value.micros))


@LogicalType.register_logical_type
class PythonCallable(NoArgumentLogicalType[PythonCallableWithSource, str]):
  """A logical type for PythonCallableSource objects."""
  @classmethod
  def urn(cls):
    return common_urns.python_callable.urn

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return str

  @classmethod
  def language_type(cls):
    return PythonCallableWithSource

  def to_representation_type(self, value):
    # type: (PythonCallableWithSource) -> str
    return value.get_source()

  def to_language_type(self, value):
    # type: (str) -> PythonCallableWithSource
    return PythonCallableWithSource(value)


FixedPrecisionDecimalArgumentRepresentation = NamedTuple(
    'FixedPrecisionDecimalArgumentRepresentation', [('precision', np.int32),
                                                    ('scale', np.int32)])


class DecimalLogicalType(NoArgumentLogicalType[decimal.Decimal, bytes]):
  """A logical type for decimal objects handling values consistent with that
  encoded by ``BigDecimalCoder`` in the Java SDK.
  """
  @classmethod
  def urn(cls):
    return common_urns.decimal.urn

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return bytes

  @classmethod
  def language_type(cls):
    return decimal.Decimal

  def to_representation_type(self, value):
    # type: (decimal.Decimal) -> bytes
    return str(value).encode()

  def to_language_type(self, value):
    # type: (bytes) -> decimal.Decimal
    return decimal.Decimal(value.decode())


@LogicalType.register_logical_type
class FixedPrecisionDecimalLogicalType(
    LogicalType[decimal.Decimal,
                DecimalLogicalType,
                FixedPrecisionDecimalArgumentRepresentation]):
  """A wrapper of DecimalLogicalType that contains the precision value.
  """
  def __init__(self, precision=-1, scale=0):
    self.precision = precision
    self.scale = scale

  @classmethod
  def urn(cls):
    # TODO(https://github.com/apache/beam/issues/23373) promote this URN to
    # schema.proto once logical types with argument are fully supported and the
    # implementation of this logical type can thus be considered standardized.
    return "beam:logical_type:fixed_decimal:v1"

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return DecimalLogicalType

  @classmethod
  def language_type(cls):
    return decimal.Decimal

  def to_representation_type(self, value):
    # type: (decimal.Decimal) -> bytes

    return DecimalLogicalType().to_representation_type(value)

  def to_language_type(self, value):
    # type: (bytes) -> decimal.Decimal

    return DecimalLogicalType().to_language_type(value)

  @classmethod
  def argument_type(cls):
    return FixedPrecisionDecimalArgumentRepresentation

  def argument(self):
    return FixedPrecisionDecimalArgumentRepresentation(
        precision=self.precision, scale=self.scale)

  @classmethod
  def _from_typing(cls, typ):
    return cls()


# TODO(yathu,BEAM-10722): Investigate and resolve conflicts in logical type
# registration when more than one logical types sharing the same language type
LogicalType.register_logical_type(DecimalLogicalType)


@LogicalType.register_logical_type
class FixedBytes(PassThroughLogicalType[bytes, np.int32]):
  """A logical type for fixed-length bytes."""
  @classmethod
  def urn(cls):
    return common_urns.fixed_bytes.urn

  def __init__(self, length: np.int32):
    self.length = length

  @classmethod
  def language_type(cls) -> type:
    return bytes

  def to_language_type(self, value: bytes):
    length = len(value)
    if length > self.length:
      raise ValueError(
          "value length {} > allowed length {}".format(length, self.length))
    elif length < self.length:
      # padding at the end
      value = value + b'\0' * (self.length - length)

    return value

  @classmethod
  def argument_type(cls):
    return np.int32

  def argument(self):
    return self.length


@LogicalType.register_logical_type
class VariableBytes(PassThroughLogicalType[bytes, np.int32]):
  """A logical type for variable-length bytes with specified maximum length."""
  @classmethod
  def urn(cls):
    return common_urns.var_bytes.urn

  def __init__(self, max_length: np.int32 = np.iinfo(np.int32).max):
    self.max_length = max_length

  @classmethod
  def language_type(cls) -> type:
    return bytes

  def to_language_type(self, value: bytes):
    length = len(value)
    if length > self.max_length:
      raise ValueError(
          "value length {} > allowed length {}".format(length, self.max_length))

    return value

  @classmethod
  def argument_type(cls):
    return np.int32

  def argument(self):
    return self.max_length


@LogicalType.register_logical_type
class FixedString(PassThroughLogicalType[str, np.int32]):
  """A logical type for fixed-length string."""
  @classmethod
  def urn(cls):
    return common_urns.fixed_char.urn

  def __init__(self, length: np.int32):
    self.length = length

  @classmethod
  def language_type(cls) -> type:
    return str

  def to_language_type(self, value: str):
    length = len(value)
    if length > self.length:
      raise ValueError(
          "value length {} > allowed length {}".format(length, self.length))
    elif length < self.length:
      # padding at the end
      value = value + ' ' * (self.length - length)

    return value

  @classmethod
  def argument_type(cls):
    return np.int32

  def argument(self):
    return self.length


@LogicalType.register_logical_type
class VariableString(PassThroughLogicalType[str, np.int32]):
  """A logical type for variable-length string with specified maximum length."""
  @classmethod
  def urn(cls):
    return common_urns.var_char.urn

  def __init__(self, max_length: np.int32 = np.iinfo(np.int32).max):
    self.max_length = max_length

  @classmethod
  def language_type(cls) -> type:
    return str

  def to_language_type(self, value: str):
    length = len(value)
    if length > self.max_length:
      raise ValueError(
          "value length {} > allowed length {}".format(length, self.max_length))

    return value

  @classmethod
  def argument_type(cls):
    return np.int32

  def argument(self):
    return self.max_length
