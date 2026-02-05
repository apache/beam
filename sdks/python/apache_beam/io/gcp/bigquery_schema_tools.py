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
