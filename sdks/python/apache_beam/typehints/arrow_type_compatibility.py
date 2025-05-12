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

"""Utilities for converting between Beam and Arrow schemas.

For internal use only, no backward compatibility guarantees.
"""

from functools import partial
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

import pyarrow as pa

from apache_beam.portability.api import schema_pb2
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import typing_from_runner_api
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils import proto_utils

__all__ = []

# Get major, minor version
PYARROW_VERSION = tuple(map(int, pa.__version__.split('.')[0:2]))

BEAM_SCHEMA_ID_KEY = b'beam:schema_id'
# We distinguish between schema and field options, because they have to be
# combined into arrow Field-level metadata for nested structs.
BEAM_SCHEMA_OPTION_KEY_PREFIX = b'beam:schema_option:'
BEAM_FIELD_OPTION_KEY_PREFIX = b'beam:field_option:'


def _hydrate_beam_option(encoded_option: bytes) -> schema_pb2.Option:
  return proto_utils.parse_Bytes(encoded_option, schema_pb2.Option)


def beam_schema_from_arrow_schema(arrow_schema: pa.Schema) -> schema_pb2.Schema:
  if arrow_schema.metadata:
    schema_id = arrow_schema.metadata.get(BEAM_SCHEMA_ID_KEY, None)
    schema_options = [
        _hydrate_beam_option(value)
        for key, value in arrow_schema.metadata.items()
        if key.startswith(BEAM_SCHEMA_OPTION_KEY_PREFIX)
    ]
  else:
    schema_id = None
    schema_options = []

  return schema_pb2.Schema(
      fields=[
          _beam_field_from_arrow_field(arrow_schema.field(i))
          for i in range(len(arrow_schema.types))
      ],
      options=schema_options,
      id=schema_id)


def _beam_field_from_arrow_field(arrow_field: pa.Field) -> schema_pb2.Field:
  beam_fieldtype = _beam_fieldtype_from_arrow_field(arrow_field)

  if arrow_field.metadata:
    field_options = [
        _hydrate_beam_option(value)
        for key, value in arrow_field.metadata.items()
        if key.startswith(BEAM_FIELD_OPTION_KEY_PREFIX)
    ]
    if isinstance(arrow_field.type, pa.StructType):
      beam_fieldtype.row_type.schema.options.extend([
          _hydrate_beam_option(value)
          for key, value in arrow_field.metadata.items()
          if key.startswith(BEAM_SCHEMA_OPTION_KEY_PREFIX)
      ])
      if BEAM_SCHEMA_ID_KEY in arrow_field.metadata:
        beam_fieldtype.row_type.schema.id = arrow_field.metadata[
            BEAM_SCHEMA_ID_KEY]

  else:
    field_options = None

  return schema_pb2.Field(
      name=arrow_field.name,
      type=beam_fieldtype,
      options=field_options,
  )


def _beam_fieldtype_from_arrow_field(
    arrow_field: pa.Field) -> schema_pb2.FieldType:
  beam_fieldtype = _beam_fieldtype_from_arrow_type(arrow_field.type)
  beam_fieldtype.nullable = arrow_field.nullable

  return beam_fieldtype


def _beam_fieldtype_from_arrow_type(
    arrow_type: pa.DataType) -> schema_pb2.FieldType:
  if arrow_type in PYARROW_TO_ATOMIC_TYPE:
    return schema_pb2.FieldType(atomic_type=PYARROW_TO_ATOMIC_TYPE[arrow_type])
  elif isinstance(arrow_type, pa.ListType):
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(
            element_type=_beam_fieldtype_from_arrow_field(
                arrow_type.value_field)))
  elif isinstance(arrow_type, pa.MapType):
    return schema_pb2.FieldType(map_type=_arrow_map_to_beam_map(arrow_type))
  elif isinstance(arrow_type, pa.StructType):
    return schema_pb2.FieldType(
        row_type=schema_pb2.RowType(
            schema=schema_pb2.Schema(
                fields=[
                    _beam_field_from_arrow_field(arrow_type[i])
                    for i in range(len(arrow_type))
                ],
            )))

  else:
    raise ValueError(f"Unrecognized arrow type: {arrow_type!r}")


def _option_as_arrow_metadata(beam_option: schema_pb2.Option, *,
                              prefix: bytes) -> Tuple[bytes, bytes]:
  return (
      prefix + beam_option.name.encode('UTF-8'),
      beam_option.SerializeToString())


_field_option_as_arrow_metadata = partial(
    _option_as_arrow_metadata, prefix=BEAM_FIELD_OPTION_KEY_PREFIX)
_schema_option_as_arrow_metadata = partial(
    _option_as_arrow_metadata, prefix=BEAM_SCHEMA_OPTION_KEY_PREFIX)


def arrow_schema_from_beam_schema(beam_schema: schema_pb2.Schema) -> pa.Schema:
  return pa.schema(
      [_arrow_field_from_beam_field(field) for field in beam_schema.fields],
      {
          BEAM_SCHEMA_ID_KEY: beam_schema.id,
          **dict(
              _schema_option_as_arrow_metadata(option) for option in beam_schema.options)  # pylint: disable=line-too-long
      },
  )


def _arrow_field_from_beam_field(beam_field: schema_pb2.Field) -> pa.Field:
  return _arrow_field_from_beam_fieldtype(
      beam_field.type, name=beam_field.name, field_options=beam_field.options)


_ARROW_PRIMITIVE_MAPPING = [
    # TODO(https://github.com/apache/beam/issues/23816): Support unsigned ints
    # and float16
    (schema_pb2.BYTE, pa.int8()),
    (schema_pb2.INT16, pa.int16()),
    (schema_pb2.INT32, pa.int32()),
    (schema_pb2.INT64, pa.int64()),
    (schema_pb2.FLOAT, pa.float32()),
    (schema_pb2.DOUBLE, pa.float64()),
    (schema_pb2.BOOLEAN, pa.bool_()),
    (schema_pb2.STRING, pa.string()),
    (schema_pb2.BYTES, pa.binary()),
]
ATOMIC_TYPE_TO_PYARROW = {
    beam: arrow
    for beam, arrow in _ARROW_PRIMITIVE_MAPPING
}
PYARROW_TO_ATOMIC_TYPE = {
    arrow: beam
    for beam, arrow in _ARROW_PRIMITIVE_MAPPING
}


def _arrow_field_from_beam_fieldtype(
    beam_fieldtype: schema_pb2.FieldType,
    name=b'',
    field_options: Sequence[schema_pb2.Option] = None) -> pa.DataType:
  arrow_type = _arrow_type_from_beam_fieldtype(beam_fieldtype)
  if field_options is not None:
    metadata = dict(
        _field_option_as_arrow_metadata(field_option)
        for field_option in field_options)
  else:
    metadata = {}

  type_info = beam_fieldtype.WhichOneof("type_info")
  if type_info == "row_type":
    schema = beam_fieldtype.row_type.schema
    metadata.update(
        dict(
            _schema_option_as_arrow_metadata(schema_option)
            for schema_option in schema.options))
    if schema.id:
      metadata[BEAM_SCHEMA_ID_KEY] = schema.id

  return pa.field(
      name=name,
      type=arrow_type,
      nullable=beam_fieldtype.nullable,
      metadata=metadata,
  )


if PYARROW_VERSION < (6, 0):
  # In pyarrow < 6.0.0 we cannot construct a MapType object from Field
  # instances, pa.map_ will only accept DataType instances. This makes it
  # impossible to propagate nullability.
  #
  # Note this was changed in:
  # https://github.com/apache/arrow/commit/64bef2ad8d9cd2fea122cfa079f8ca3fea8cdf5d
  #
  # Here we define a custom arrow map conversion function to handle these cases
  # and error as appropriate.

  def _make_arrow_map(beam_map_type: schema_pb2.MapType):
    if beam_map_type.key_type.nullable:
      raise TypeError('Arrow map key field cannot be nullable')
    elif beam_map_type.value_type.nullable:
      raise TypeError(
          "pyarrow<6 does not support creating maps with nullable "
          "values. Please use pyarrow>=6.0.0")

    return pa.map_(
        _arrow_type_from_beam_fieldtype(beam_map_type.key_type),
        _arrow_type_from_beam_fieldtype(beam_map_type.value_type))

  def _arrow_map_to_beam_map(arrow_map_type):
    return schema_pb2.MapType(
        key_type=_beam_fieldtype_from_arrow_type(arrow_map_type.key_type),
        value_type=_beam_fieldtype_from_arrow_type(arrow_map_type.item_type))

else:

  def _make_arrow_map(beam_map_type: schema_pb2.MapType):
    return pa.map_(
        _arrow_field_from_beam_fieldtype(beam_map_type.key_type),
        _arrow_field_from_beam_fieldtype(beam_map_type.value_type))

  def _arrow_map_to_beam_map(arrow_map_type):
    return schema_pb2.MapType(
        key_type=_beam_fieldtype_from_arrow_field(arrow_map_type.key_field),
        value_type=_beam_fieldtype_from_arrow_field(arrow_map_type.item_field))


def _arrow_type_from_beam_fieldtype(
    beam_fieldtype: schema_pb2.FieldType,
) -> Tuple[pa.DataType, Optional[Dict[bytes, bytes]]]:
  # Note this function is not concerned with beam_fieldtype.nullable, as
  # nullability is a property of the Field in Arrow.
  type_info = beam_fieldtype.WhichOneof("type_info")
  if type_info == 'atomic_type':
    try:
      output_arrow_type = ATOMIC_TYPE_TO_PYARROW[beam_fieldtype.atomic_type]
    except KeyError:
      raise ValueError(
          "Unsupported atomic type: {0}".format(beam_fieldtype.atomic_type))
  elif type_info == "array_type":
    output_arrow_type = pa.list_(
        _arrow_field_from_beam_fieldtype(
            beam_fieldtype.array_type.element_type))
  elif type_info == "map_type":
    output_arrow_type = _make_arrow_map(beam_fieldtype.map_type)
  elif type_info == "row_type":
    schema = beam_fieldtype.row_type.schema
    # Note schema id and options are handled at the arrow field level, they are
    # added at field-level metadata.
    output_arrow_type = pa.struct(
        [_arrow_field_from_beam_field(field) for field in schema.fields])
  elif type_info == "logical_type":
    # TODO(https://github.com/apache/beam/issues/23817): Add support for logical
    # types.
    raise NotImplementedError(
        "Beam logical types are not currently supported "
        "in arrow_type_compatibility.")
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")

  return output_arrow_type


class PyarrowBatchConverter(BatchConverter):
  def __init__(self, element_type: RowTypeConstraint):
    super().__init__(pa.Table, element_type)
    self._beam_schema = typing_to_runner_api(element_type).row_type.schema
    arrow_schema = arrow_schema_from_beam_schema(self._beam_schema)

    self._arrow_schema = arrow_schema

  @staticmethod
  def from_typehints(element_type,
                     batch_type) -> Optional['PyarrowBatchConverter']:
    assert batch_type == pa.Table

    if not isinstance(element_type, RowTypeConstraint):
      element_type = RowTypeConstraint.from_user_type(element_type)
      if element_type is None:
        raise TypeError(
            f"Element type {element_type} must be compatible with Beam Schemas "
            "(https://beam.apache.org/documentation/programming-guide/#schemas)"
            " for batch type pa.Table.")

    return PyarrowBatchConverter(element_type)

  def produce_batch(self, elements):
    arrays = [
        pa.array([getattr(el, name) for el in elements],
                 type=self._arrow_schema.field(name).type)
        for name, _ in self._element_type._fields
    ]
    return pa.Table.from_arrays(arrays, schema=self._arrow_schema)

  def explode_batch(self, batch: pa.Table):
    """Convert an instance of B to Generator[E]."""
    for row_values in zip(*batch.columns):
      yield self._element_type.user_type(
          **{
              name: val.as_py()
              for name, val in zip(self._arrow_schema.names, row_values)
          })

  def combine_batches(self, batches: List[pa.Table]):
    return pa.concat_tables(batches)

  def get_length(self, batch: pa.Table):
    return batch.num_rows

  def estimate_byte_size(self, batch: pa.Table):
    return batch.nbytes

  @staticmethod
  def _from_serialized_schema(serialized_schema):
    beam_schema = proto_utils.parse_Bytes(serialized_schema, schema_pb2.Schema)
    element_type = typing_from_runner_api(
        schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))
    return PyarrowBatchConverter(element_type)

  def __reduce__(self):
    return self._from_serialized_schema, (
        self._beam_schema.SerializeToString(), )


class PyarrowArrayBatchConverter(BatchConverter):
  def __init__(self, element_type: type):
    super().__init__(pa.Array, element_type)
    self._element_type = element_type
    beam_fieldtype = typing_to_runner_api(element_type)
    self._arrow_type = _arrow_type_from_beam_fieldtype(beam_fieldtype)

  @staticmethod
  def from_typehints(element_type,
                     batch_type) -> Optional['PyarrowArrayBatchConverter']:
    assert batch_type == pa.Array

    return PyarrowArrayBatchConverter(element_type)

  def produce_batch(self, elements):
    return pa.array(list(elements), type=self._arrow_type)

  def explode_batch(self, batch: pa.Array):
    """Convert an instance of B to Generator[E]."""
    for val in batch:
      yield val.as_py()

  def combine_batches(self, batches: List[pa.Array]):
    return pa.concat_arrays(batches)

  def get_length(self, batch: pa.Array):
    return batch.num_rows

  def estimate_byte_size(self, batch: pa.Array):
    return batch.nbytes


@BatchConverter.register(name="pyarrow")
def create_pyarrow_batch_converter(
    element_type: type, batch_type: type) -> BatchConverter:
  if batch_type == pa.Table:
    return PyarrowBatchConverter.from_typehints(
        element_type=element_type, batch_type=batch_type)
  elif batch_type == pa.Array:
    return PyarrowArrayBatchConverter.from_typehints(
        element_type=element_type, batch_type=batch_type)

  raise TypeError("batch type must be pa.Table or pa.Array")
