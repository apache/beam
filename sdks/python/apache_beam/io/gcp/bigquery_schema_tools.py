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

"""Tools used tool work with Schema types in the context of BigQuery.
Classes, constants and functions in this file are experimental and have no
backwards compatibility guarantees.
NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

import datetime
from typing import Optional
from typing import Sequence

import numpy as np

import apache_beam as beam
import apache_beam.io.gcp.bigquery_tools
import apache_beam.typehints.schemas
import apache_beam.utils.proto_utils
import apache_beam.utils.timestamp
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.portability.api import schema_pb2
from apache_beam.transforms import DoFn

# BigQuery types as listed in
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# with aliases (RECORD, BOOLEAN, FLOAT, INTEGER) as defined in
# https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableFieldSchema.html#setType-java.lang.String-
BIG_QUERY_TO_PYTHON_TYPES = {
    "STRING": str,
    "INTEGER": np.int64,
    "FLOAT64": np.float64,
    "FLOAT": np.float64,
    "BOOLEAN": bool,
    "BYTES": bytes,
    "TIMESTAMP": apache_beam.utils.timestamp.Timestamp,
    "GEOGRAPHY": str,
    #TODO(https://github.com/apache/beam/issues/20810):
    # Finish mappings for all BQ types
}


def generate_user_type_from_bq_schema(
    the_table_schema,
    selected_fields: 'bigquery.TableSchema' = None,
    type_overrides=None) -> type:
  """Convert a schema of type TableSchema into a pcollection element.

  Args:
    the_table_schema: A BQ schema of type TableSchema
    selected_fields: if not None, the subset of fields to consider
    type_overrides: Optional mapping of BigQuery type names (uppercase)
      to Python types. These override the default mappings in
      BIG_QUERY_TO_PYTHON_TYPES. For example:
      ``{'DATE': datetime.date, 'JSON': dict}``

  Returns:
    type: type that can be used to work with pCollections.
  """
  effective_types = {**BIG_QUERY_TO_PYTHON_TYPES, **(type_overrides or {})}
  the_schema = beam.io.gcp.bigquery_tools.get_dict_table_schema(
      the_table_schema)
  if the_schema == {}:
    raise ValueError("Encountered an empty schema")
  field_names_and_types = []
  for field in the_schema['fields']:
    if selected_fields is not None and field['name'] not in selected_fields:
      continue
    if field['type'] in effective_types:
      typ = bq_field_to_type(field['type'], field['mode'], type_overrides)
    else:
      raise ValueError(
          f"Encountered "
          f"an unsupported type: {field['type']!r}")
    field_names_and_types.append((field['name'], typ))
  sample_schema = beam.typehints.schemas.named_fields_to_schema(
      field_names_and_types)
  usertype = beam.typehints.schemas.named_tuple_from_schema(sample_schema)
  return usertype


def bq_field_to_type(field, mode, type_overrides=None):
  """Convert a BigQuery field type and mode to a Python type hint.

  Args:
    field: The BigQuery type name (e.g., 'STRING', 'DATE').
    mode: The field mode ('NULLABLE', 'REPEATED', 'REQUIRED').
    type_overrides: Optional mapping of BigQuery type names (uppercase)
      to Python types. These override the default mappings.

  Returns:
    The corresponding Python type hint.
  """
  effective_types = {**BIG_QUERY_TO_PYTHON_TYPES, **(type_overrides or {})}
  if mode == 'NULLABLE' or mode is None or mode == '':
    return Optional[effective_types[field]]
  elif mode == 'REPEATED':
    return Sequence[effective_types[field]]
  elif mode == 'REQUIRED':
    return effective_types[field]
  else:
    raise ValueError(f"Encountered an unsupported mode: {mode!r}")


_ATOMIC_TYPE_TO_BQ_TYPE = {
    schema_pb2.BOOLEAN: 'BOOL',
    schema_pb2.BYTES: 'BYTES',
    schema_pb2.STRING: 'STRING',
    schema_pb2.BYTE: 'INT64',
    schema_pb2.INT16: 'INT64',
    schema_pb2.INT32: 'INT64',
    schema_pb2.INT64: 'INT64',
    schema_pb2.FLOAT: 'FLOAT64',
    schema_pb2.DOUBLE: 'FLOAT64',
}


def _logical_type_to_bq_type_map():
  # Built lazily to avoid a module-level import cycle between
  # bigquery_schema_tools and typehints.schemas.
  schemas = apache_beam.typehints.schemas
  return {
      schemas.MillisInstant.urn(): 'TIMESTAMP',
      schemas.MicrosInstant.urn(): 'TIMESTAMP',
      schemas.Date.urn(): 'DATE',
      schemas.DecimalLogicalType.urn(): 'NUMERIC',
      schemas.FixedPrecisionDecimalLogicalType.urn(): 'NUMERIC',
  }


def _bq_type_mode_and_fields(field_type: schema_pb2.FieldType):
  """Maps a Beam schema_pb2.FieldType to a
  ``(bq_type, bq_mode, nested_bq_fields)`` tuple, where ``nested_bq_fields``
  is a list of BigQuery field dicts for STRUCT types, and ``None`` otherwise.
  """
  type_info = field_type.WhichOneof('type_info')
  mode = 'NULLABLE' if field_type.nullable else 'REQUIRED'

  if type_info in ('array_type', 'iterable_type'):
    element_type = (
        field_type.array_type.element_type
        if type_info == 'array_type' else field_type.iterable_type.element_type)
    if element_type.WhichOneof('type_info') in ('array_type', 'iterable_type'):
      raise ValueError(
          'BigQuery does not support nested (repeated-of-repeated) fields; '
          'please provide an explicit schema instead of relying on '
          'auto-inference.')
    bq_type, _, nested_fields = _bq_type_mode_and_fields(element_type)
    return bq_type, 'REPEATED', nested_fields
  elif type_info == 'row_type':
    nested_fields = [
        beam_field_to_bq_field(f) for f in field_type.row_type.schema.fields
    ]
    return 'STRUCT', mode, nested_fields
  elif type_info == 'logical_type':
    urn = field_type.logical_type.urn
    logical_type_to_bq_type = _logical_type_to_bq_type_map()
    if urn in logical_type_to_bq_type:
      return logical_type_to_bq_type[urn], mode, None
    # Fall back to the logical type's representation type, which covers
    # e.g. fixed/variable-length strings and bytes.
    representation_type = field_type.logical_type.representation
    if representation_type.WhichOneof('type_info') == 'atomic_type':
      atomic = representation_type.atomic_type
      if atomic in _ATOMIC_TYPE_TO_BQ_TYPE:
        return _ATOMIC_TYPE_TO_BQ_TYPE[atomic], mode, None
    raise ValueError(
        f'Cannot automatically infer a BigQuery type for the logical type '
        f'with urn {urn!r}. Please provide an explicit schema.')
  elif type_info == 'atomic_type':
    atomic = field_type.atomic_type
    if atomic not in _ATOMIC_TYPE_TO_BQ_TYPE:
      raise ValueError(
          f'Cannot automatically infer a BigQuery type for the atomic type '
          f'{atomic!r}. Please provide an explicit schema.')
    return _ATOMIC_TYPE_TO_BQ_TYPE[atomic], mode, None
  elif type_info == 'map_type':
    raise ValueError(
        'BigQuery schema auto-inference does not support MapType fields; '
        'please provide an explicit schema.')
  else:
    raise ValueError(
        f'Cannot automatically infer a BigQuery type for field type '
        f'{field_type!r}. Please provide an explicit schema.')


def beam_field_to_bq_field(field: schema_pb2.Field) -> dict:
  """Convert a single Beam schema field (schema_pb2.Field) into a BigQuery
  TableFieldSchema, in dictionary form."""
  bq_type, mode, nested_fields = _bq_type_mode_and_fields(field.type)
  bq_field = {'name': field.name, 'type': bq_type, 'mode': mode}
  if nested_fields is not None:
    bq_field['fields'] = nested_fields
  return bq_field


def beam_schema_to_bq_table_schema(schema: schema_pb2.Schema) -> dict:
  """Convert a Beam schema (schema_pb2.Schema) into a BigQuery TableSchema,
  in dictionary form.

  This is the reverse of `generate_user_type_from_bq_schema`, and is used to
  auto-infer the destination table's schema from a schema'd PCollection --
  for example a PCollection of NamedTuples, dataclasses, or Beam Rows, such
  as the ones produced by
  ``ReadFromBigQuery(..., output_type='BEAM_ROW')``.

  Args:
    schema: a `schema_pb2.Schema` instance, as returned by
      `apache_beam.typehints.schemas.schema_from_element_type`.

  Returns:
    Dict[str, Any]: A BigQuery TableSchema in dictionary form, e.g.
    ``{'fields': [{'name': 'a', 'type': 'INT64', 'mode': 'NULLABLE'}, ...]}``

  Raises:
    ValueError: if a field's type has no known BigQuery equivalent.
  """
  return {'fields': [beam_field_to_bq_field(f) for f in schema.fields]}


def convert_to_usertype(
    table_schema, selected_fields=None, type_overrides=None):
  """Convert a BigQuery table schema to a user type.

  Args:
    table_schema: A BQ schema of type TableSchema
    selected_fields: if not None, the subset of fields to consider
    type_overrides: Optional mapping of BigQuery type names (uppercase)
      to Python types.

  Returns:
    A ParDo transform that converts dictionaries to the user type.
  """
  usertype = generate_user_type_from_bq_schema(
      table_schema, selected_fields, type_overrides)
  return beam.ParDo(BeamSchemaConversionDoFn(usertype))


class BeamSchemaConversionDoFn(DoFn):
  def __init__(self, pcoll_val_ctor):
    self._pcoll_val_ctor = pcoll_val_ctor

  def process(self, dict_of_tuples):
    for k, v in dict_of_tuples.items():
      if isinstance(v, datetime.datetime):
        dict_of_tuples[k] = beam.utils.timestamp.Timestamp.from_utc_datetime(v)
    yield self._pcoll_val_ctor(**dict_of_tuples)

  def infer_output_type(self, input_type):
    return self._pcoll_val_ctor

  @classmethod
  def _from_serialized_schema(cls, schema_str):
    return cls(
        apache_beam.typehints.schemas.named_tuple_from_schema(
            apache_beam.utils.proto_utils.parse_Bytes(
                schema_str, schema_pb2.Schema)))

  def __reduce__(self):
    # when pickling, use bytes representation of the schema.
    return (
        self._from_serialized_schema,
        (
            beam.typehints.schemas.named_tuple_to_schema(
                self._pcoll_val_ctor).SerializeToString(), ))
