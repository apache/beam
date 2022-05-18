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

from typing import Optional
from typing import Sequence

import numpy as np

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery

# BigQuery types as listed in
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# with aliases (RECORD, BOOLEAN, FLOAT, INTEGER) as defined in
# https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableFieldSchema.html#setType-java.lang.String-
BIG_QUERY_TO_PYTHON_TYPES = {
    "STRING": str,
    "INTEGER": np.int64,
    "FLOAT64": np.float64,
    "BOOLEAN": bool,
    "BYTES": bytes,
    "TIMESTAMP": beam.utils.timestamp.Timestamp,
    #TODO svetaksundhar@: Finish mappings for all BQ types
}


def produce_pcoll_with_schema(the_table_schema):
  #type: (bigquery.TableSchema) -> type

  """Convert a schema of type TableSchema into a pcollection element.
      Args:
        the_table_schema: A BQ schema of type TableSchema
      Returns:
        type: type that can be used to work with pCollections.
  """

  the_schema = beam.io.gcp.bigquery_tools.get_dict_table_schema(
      the_table_schema)
  if the_schema == {}:
    raise ValueError("The schema is empty")
  dict_of_tuples = []
  for i in range(len(the_schema['fields'])):
    if the_schema['fields'][i]['type'] in BIG_QUERY_TO_PYTHON_TYPES:
      typ = bq_field_to_type(
          the_schema['fields'][i]['type'], the_schema['fields'][i]['mode'])
    else:
      raise ValueError(the_schema['fields'][i]['type'])
    # TODO svetaksundhar@: Map remaining BQ types
    dict_of_tuples.append((the_schema['fields'][i]['name'], typ))
  sample_schema = beam.typehints.schemas.named_fields_to_schema(dict_of_tuples)
  usertype = beam.typehints.schemas.named_tuple_from_schema(sample_schema)
  return usertype


def produce_pcoll_using_bqio(project_id, dataset_id, table_id):
  the_table_schema = beam.io.gcp.bigquery.bigquery_tools.BigQueryWrapper(
  ).get_table(project_id, dataset_id, table_id)
  beam.io.gcp.bigquery_schema_tools.produce_pcoll_with_schema(the_table_schema)


def bq_field_to_type(field, mode):
  if mode == 'NULLABLE':
    return Optional[BIG_QUERY_TO_PYTHON_TYPES[field]]
  elif mode == 'REPEATED':
    return Sequence[BIG_QUERY_TO_PYTHON_TYPES[field]]
  elif mode == 'None' or mode == '':
    return BIG_QUERY_TO_PYTHON_TYPES[field]
  else:
    return ValueError("Not a supported mode")


class BeamSchemaUnbatchDoFn(beam.DoFn):
  def __init__(self, pcoll_val_ctor):
    self._pcoll_val_ctor = pcoll_val_ctor

  def infer_output_type(self, input_type):
    return self._pcoll_val_ctor

  @classmethod
  def _from_serialized_schema(cls, dict_of_tuples):
    return cls(
        beam.typehints.schemas.named_tuple_from_schema(
            beam.dataframe.schemas.proto_utils.parse_Bytes(
                dict_of_tuples, beam.dataframe.schemas.schema_pb2.Schema)))

  def __reduce__(self):
    # when pickling, use bytes representation of the schema.
    return (
        self._from_serialized_schema,
        (
            beam.typehints.schemas.named_tuple_to_schema(
                self._pcoll_val_ctor).SerializeToString(), ))
