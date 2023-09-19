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

"""This module contains the Python implementations for the builtin IOs.

They are referenced from standard_io.py.

Note that in the case that they overlap with other (likely Java)
implementations of the same transforms, the configs must be kept in sync.
"""

import os
from typing import Any
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Union

import yaml

import apache_beam as beam
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import schemas
from apache_beam.yaml import yaml_provider


def read_from_bigquery(
    query=None, table=None, row_restriction=None, fields=None):
  if query is None:
    assert table is not None
  else:
    assert table is None and row_restriction is None and fields is None
  return ReadFromBigQuery(
      query=query,
      table=table,
      row_restriction=row_restriction,
      selected_fields=fields,
      method='DIRECT_READ',
      output_type='BEAM_ROW')


def write_to_bigquery(
    table,
    *,
    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=BigQueryDisposition.WRITE_APPEND,
    error_handling=None):
  class WriteToBigQueryHandlingErrors(beam.PTransform):
    def default_label(self):
      return 'WriteToBigQuery'

    def expand(self, pcoll):
      write_result = pcoll | WriteToBigQuery(
          table,
          method=WriteToBigQuery.Method.STORAGE_WRITE_API
          if error_handling else None,
          create_disposition=create_disposition,
          write_disposition=write_disposition,
          temp_file_format='AVRO')
      if error_handling and 'output' in error_handling:
        # TODO: Support error rates.
        return {
            'post_write': write_result.failed_rows_with_errors
            | beam.FlatMap(lambda x: None),
            error_handling['output']: write_result.failed_rows_with_errors
        }
      else:
        if write_result._method == WriteToBigQuery.Method.FILE_LOADS:
          # Never returns errors, just fails.
          return {
              'post_write': write_result.destination_load_jobid_pairs
              | beam.FlatMap(lambda x: None)
          }
        else:

          # This should likely be pushed into the BQ read itself to avoid
          # the possibility of silently ignoring errors.
          def raise_exception(failed_row_with_error):
            raise RuntimeError(failed_row_with_error.error_message)

          _ = write_result.failed_rows_with_errors | beam.Map(raise_exception)
          return {
              'post_write': write_result.failed_rows_with_errors
              | beam.FlatMap(lambda x: None)
          }

  return WriteToBigQueryHandlingErrors()


def _create_parser(format, schema):
  if format == 'raw':
    if schema:
      raise ValueError('raw format does not take a schema')
    return (
        schema_pb2.Schema(fields=[schemas.schema_field('payload', bytes)]),
        lambda payload: beam.Row(payload=payload))
  else:
    raise ValueError(f'Unknown format: {format}')


@beam.ptransform_fn
def read_from_pubsub(
    root,
    *,
    topic: Optional[str]=None,
    subscription: Optional[str]=None,
    format: str,
    schema: Optional[Any]=None,
    attributes: Optional[Union[str, Iterable[str]]]=None,
    timestamp_attribute: Optional[str]=None,
    error_handling: Optional[Mapping[str, str]]=None):
  if topic and subscription:
    raise TypeError('Only one of topic and subscription may be specified.')
  elif not topic and not subscription:
    raise TypeError('One of topic or subscription may be specified.')
  payload_schema, parser = _create_parser(format, schema)
  if not attributes:
    extra_fields = []
    mapper = lambda msg: parser(msg)
  else:
    if isinstance(attributes, str):
      extra_fields = [schemas.schema_field(attributes, Mapping[str, str])]
    else:
      extra_fields = [schemas.schema_field(attr, str) for attr in attributes]

    def mapper(msg):
      values = parser(msg.data).as_dict()
      if isinstance(attributes, str):
        values[attributes] = msg.attributes
      else:
        # Should missing attributes be optional or parse errors?
        for attr in attributes:
          values[attr] = msg.attributes[attr]
      return beam.Row(**values)

  if error_handling:
    raise ValueError('waiting for https://github.com/apache/beam/pull/28462')
  output = (
      root
      | beam.io.ReadFromPubSub(
          topic=topic,
          subscription=subscription,
          with_attributes=bool(attributes),
          timestamp_attribute=timestamp_attribute)
      | 'ParseMessage' >> beam.Map(mapper))
  output.element_type = schemas.named_tuple_from_schema(
      schema_pb2.Schema(fields=list(payload_schema.fields) + extra_fields))
  return output


def _create_formatter(format, schema, beam_schema):
  if format == 'raw':
    if schema:
      raise ValueError('raw format does not take a schema')
    field_names = [field.name for field in beam_schema.fields]
    if len(field_names) != 1:
      raise ValueError(f'Expecting exactly one field, found {field_names}')
    return lambda row: getattr(row, field_names[0])
  else:
    raise ValueError(f'Unknown format: {format}')


@beam.ptransform_fn
def write_to_pubsub(
    pcoll,
    *,
    topic: str,
    format: str,
    schema: Optional[Any]=None,
    attributes: Optional[Union[str, Iterable[str]]]=None,
    timestamp_attribute: Optional[str]=None,
    error_handling: Optional[Mapping[str, str]]=None):

  input_schema = schemas.schema_from_element_type(pcoll.element_type)

  if not attributes:
    extra_fields = []
    attributes_extractor = lambda row: {}
  elif isinstance(attributes, str):
    extra_fields = [attributes]
    attributes_extractor = lambda row: getattr(row, attributes)
  else:
    extra_fields = attributes
    attributes_extractor = lambda row: {
        attr: getattr(row, attr)
        for attr in attributes
    }

  schema_names = set(f.name for f in input_schema.fields)
  missing_attribute_names = set(extra_fields) - schema_names
  if missing_attribute_names:
    raise ValueError(f'Attributes {missing_attribute_names} not found in schema fields {schema_names}')

  payload_schema = schema_pb2.Schema(
      fields=[
          field for field in input_schema.fields
          if field.name not in extra_fields
      ])
  formatter = _create_formatter(format, schema, payload_schema)
  _ = (
      pcoll | beam.Map(
          lambda row: beam.io.gcp.pubsub.PubsubMessage(
              formatter(row), attributes_extractor(row)))
      | beam.io.WriteToPubSub(
          topic, with_attributes=True, timestamp_attribute=timestamp_attribute))
  return {}


def io_providers():
  with open(os.path.join(os.path.dirname(__file__), 'standard_io.yaml')) as fin:
    return yaml_provider.parse_providers(yaml.load(fin, Loader=yaml.SafeLoader))
