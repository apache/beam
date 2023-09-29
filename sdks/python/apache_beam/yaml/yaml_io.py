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

import json
import os
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import yaml

import apache_beam as beam
import apache_beam.io as beam_io
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import schemas
from apache_beam.yaml import yaml_mapping
from apache_beam.yaml import yaml_provider


def read_from_text(path: str):
  # TODO(yaml): Consider passing the filename and offset, possibly even
  # by default.
  return beam_io.ReadFromText(path) | beam.Map(lambda s: beam.Row(line=s))


@beam.ptransform_fn
def write_to_text(pcoll, path: str):
  try:
    field_names = [
        name for name,
        _ in schemas.named_fields_from_element_type(pcoll.element_type)
    ]
  except Exception as exn:
    raise ValueError(
        "WriteToText requires an input schema with exactly one field.") from exn
  if len(field_names) != 1:
    raise ValueError(
        "WriteToText requires an input schema with exactly one field, got %s" %
        field_names)
  sole_field_name, = field_names
  return pcoll | beam.Map(
      lambda x: str(getattr(x, sole_field_name))) | beam.io.WriteToText(path)


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


def _create_parser(
    format,
    schema: Any) -> Tuple[schema_pb2.Schema, Callable[[bytes], beam.Row]]:
  if format == 'raw':
    if schema:
      raise ValueError('raw format does not take a schema')
    return (
        schema_pb2.Schema(fields=[schemas.schema_field('payload', bytes)]),
        lambda payload: beam.Row(payload=payload))
  elif format == 'json':
    beam_schema = json_schema_to_beam_schema(schema)
    return beam_schema, json_parser(beam_schema)
  else:
    raise ValueError(f'Unknown format: {format}')


def _create_formatter(
    format, schema: Any,
    beam_schema: schema_pb2.Schema) -> Callable[[beam.Row], bytes]:
  if format == 'raw':
    if schema:
      raise ValueError('raw format does not take a schema')
    field_names = [field.name for field in beam_schema.fields]
    if len(field_names) != 1:
      raise ValueError(f'Expecting exactly one field, found {field_names}')
    return lambda row: getattr(row, field_names[0])
  elif format == 'json':
    return json_formater(beam_schema)
  else:
    raise ValueError(f'Unknown format: {format}')


# Move all these to json_utils?
def json_schema_to_beam_schema(
    json_schema: Dict[str, Any]) -> schema_pb2.Schema:
  def maybe_nullable(beam_type, nullable):
    if nullable:
      beam_type.nullable = True
    return beam_type

  json_type = json_schema.get('type', None)
  if json_type != 'object':
    raise ValueError('Expected object type, got {json_type}.')
  if 'properties' not in json_schema:
    # Technically this is a valid (vacuous) schema, but as it's not generally
    # meaningful, throw an informative error instead.
    # (We could add a flag to allow this degenerate case.)
    raise ValueError('Missing properties for {json_schema}.')
  required = set(json_schema.get('required', []))
  return schema_pb2.Schema(
      fields=[
          schemas.schema_field(
              name,
              maybe_nullable(json_type_to_beam_type(t), name not in required))
          for (name, t) in json_schema['properties'].items()
      ])


JSON_ATOMIC_TYPES_TO_BEAM = {
    'boolean': schema_pb2.BOOLEAN,
    'integer': schema_pb2.INT64,
    'number': schema_pb2.DOUBLE,
    'string': schema_pb2.STRING,
}


def json_type_to_beam_type(json_type: Dict[str, Any]) -> schema_pb2.FieldType:
  if not isinstance(json_type, dict) or 'type' not in json_type:
    raise ValueError(f'Malformed type {json_type}.')
  type_name = json_type['type']
  if type_name in JSON_ATOMIC_TYPES_TO_BEAM:
    return schema_pb2.FieldType(
        atomic_type=JSON_ATOMIC_TYPES_TO_BEAM[type_name])
  elif type_name == 'array':
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(
            element_type=json_type_to_beam_type(json_type['items'])))
  elif type_name == 'object':
    if 'properties' in json_type:
      return schema_pb2.FieldType(
          row_type=schema_pb2.RowType(
              schema=json_schema_to_beam_schema(json_type)))
    elif 'additionalProperties' in json_type:
      return schema_pb2.FieldType(
          map_type=schema_pb2.MapType(
              key_type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
              value_type=json_type_to_beam_type(
                  json_type['additionalProperties'])))
    else:
      raise ValueError(
          f'Object type must have either properties or additionalProperties, '
          f'got {json_type}.')
  else:
    raise ValueError(f'Unable to convert {json_type} to a Beam schema.')


def json_to_row(beam_type: schema_pb2.FieldType) -> Callable[[Any], Any]:
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    return lambda value: value
  elif type_info == "array_type":
    element_converter = json_to_row(beam_type.array_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "iterable_type":
    element_converter = json_to_row(beam_type.iterable_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type != schema_pb2.STRING:
      raise TypeError(
          f'Only strings allowd as map keys when converting from JSON, '
          f'found {beam_type}')
    value_converter = json_to_row(beam_type.map_type.value_type)
    return lambda value: {k: value_converter(v) for (k, v) in value.items()}
  elif type_info == "row_type":
    converters = {
        field.name: json_to_row(field.type)
        for field in beam_type.row_type.schema.fields
    }
    return lambda value: beam.Row(
        **
        {name: convert(value[name])
         for (name, convert) in converters.items()})
  elif type_info == "logical_type":
    return lambda value: value
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def json_parser(beam_schema: schema_pb2.Schema) -> Callable[[bytes], beam.Row]:
  to_row = json_to_row(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))
  return lambda s: to_row(json.loads(s))


def row_to_json(beam_type):
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    return lambda value: value
  elif type_info == "array_type":
    element_converter = row_to_json(beam_type.array_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "iterable_type":
    element_converter = row_to_json(beam_type.iterable_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type != schema_pb2.STRING:
      raise TypeError(
          f'Only strings allowd as map keys when converting to JSON, '
          f'found {beam_type}')
    value_converter = row_to_json(beam_type.map_type.value_type)
    return lambda value: {k: value_converter(v) for (k, v) in value.items()}
  elif type_info == "row_type":
    converters = {
        field.name: row_to_json(field.type)
        for field in beam_type.row_type.schema.fields
    }
    return lambda row: {
        name: convert(getattr(row, name))
        for (name, convert) in converters.items()
    }
  elif type_info == "logical_type":
    return lambda value: value
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def json_formater(
    beam_schema: schema_pb2.Schema) -> Callable[[beam.Row], bytes]:
  convert = row_to_json(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))
  return lambda row: json.dumps(convert(row), sort_keys=True).encode('utf-8')


@beam.ptransform_fn
@yaml_mapping.maybe_with_exception_handling_transform_fn
def read_from_pubsub(
    root,
    *,
    topic: Optional[str] = None,
    subscription: Optional[str] = None,
    format: str,
    schema: Optional[Any] = None,
    attributes: Optional[Iterable[str]] = None,
    attributes_map: Optional[str] = None,
    id_attribute: Optional[str] = None,
    timestamp_attribute: Optional[str] = None):
  """Reads messages from Cloud Pub/Sub.

  Args:
    topic: Cloud Pub/Sub topic in the form
      "projects/<project>/topics/<topic>". If provided, subscription must be
      None.
    subscription: Existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>". If not
      specified, a temporary subscription will be created from the specified
      topic. If provided, topic must be None.
    format: The expected format of the message payload.  Currently suported
      formats are

        - raw: Produces records with a single `payload` field whose contents
            are the raw bytes of the pubsub message.

    schema: Schema specification for the given format.
    attributes: List of attribute keys whose values will be flattened into the
      output message as additional fields.  For example, if the format is `raw`
      and attributes is `["a", "b"]` then this read will produce elements of
      the form `Row(payload=..., a=..., b=...)`.
    attribute_map: Name of a field in which to store the full set of attributes
      associated with this message.  For example, if the format is `raw` and
      `attribute_map` is set to `"attrs"` then this read will produce elements
      of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type
      of string to string.
      If both `attributes` and `attribute_map` are set, the overlapping
      attribute values will be present in both the flattened structure and the
      attribute map.
    id_attribute: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier. When specified, the value of this attribute (which
      can be any string that uniquely identifies the record) will be used for
      deduplication of messages. If not provided, we cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
    timestamp_attribute: Message value to use as element timestamp. If None,
      uses message publishing time as the timestamp.

      Timestamp values should be in one of two formats:

      - A numerical value representing the number of milliseconds since the
        Unix epoch.
      - A string in RFC 3339 format, UTC timezone. Example:
        ``2015-10-29T23:41:41.123Z``. The sub-second component of the
        timestamp is optional, and digits beyond the first three (i.e., time
        units smaller than milliseconds) may be ignored.
  """
  if topic and subscription:
    raise TypeError('Only one of topic and subscription may be specified.')
  elif not topic and not subscription:
    raise TypeError('One of topic or subscription may be specified.')
  payload_schema, parser = _create_parser(format, schema)
  extra_fields: List[schema_pb2.Field] = []
  if not attributes and not attributes_map:
    mapper = lambda msg: parser(msg)
  else:
    if isinstance(attributes, str):
      attributes = [attributes]
    if attributes:
      extra_fields.extend(
          [schemas.schema_field(attr, str) for attr in attributes])
    if attributes_map:
      extra_fields.append(
          schemas.schema_field(attributes_map, Mapping[str, str]))

    def mapper(msg):
      values = parser(msg.data).as_dict()
      if attributes:
        # Should missing attributes be optional or parse errors?
        for attr in attributes:
          values[attr] = msg.attributes[attr]
      if attributes_map:
        values[attributes_map] = msg.attributes
      return beam.Row(**values)

  output = (
      root
      | beam.io.ReadFromPubSub(
          topic=topic,
          subscription=subscription,
          with_attributes=bool(attributes or attributes_map),
          id_label=id_attribute,
          timestamp_attribute=timestamp_attribute)
      | 'ParseMessage' >> beam.Map(mapper))
  output.element_type = schemas.named_tuple_from_schema(
      schema_pb2.Schema(fields=list(payload_schema.fields) + extra_fields))
  return output


@beam.ptransform_fn
@yaml_mapping.maybe_with_exception_handling_transform_fn
def write_to_pubsub(
    pcoll,
    *,
    topic: str,
    format: str,
    schema: Optional[Any] = None,
    attributes: Optional[Iterable[str]] = None,
    attributes_map: Optional[str] = None,
    id_attribute: Optional[str] = None,
    timestamp_attribute: Optional[str] = None):
  """Writes messages from Cloud Pub/Sub.

  Args:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    format: How to format the message payload.  Currently suported
      formats are

        - raw: Expects a message with a single field (excluding
            attribute-related fields )whose contents are used as the raw bytes
            of the pubsub message.

    schema: Schema specification for the given format.
    attributes: List of attribute keys whose values will be pulled out as
      PubSub message attributes.  For example, if the format is `raw`
      and attributes is `["a", "b"]` then elements of the form
      `Row(any_field=..., a=..., b=...)` will result in PubSub messages whose
      payload has the contents of any_field and whose attribute will be
      populated with the values of `a` and `b`.
    attribute_map: Name of a string-to-string map field in which to pull a set
      of attributes associated with this message.  For example, if the format
      is `raw` and `attribute_map` is set to `"attrs"` then elements of the form
      `Row(any_field=..., attrs=...)` will result in PubSub messages whose
      payload has the contents of any_field and whose attribute will be
      populated with the values from attrs.
      If both `attributes` and `attribute_map` are set, the union of attributes
      from these two sources will be used to populate the PubSub message
      attributes.
    id_attribute: If set, will set an attribute for each Cloud Pub/Sub message
      with the given name and a unique value. This attribute can then be used
      in a ReadFromPubSub PTransform to deduplicate messages.
    timestamp_attribute: If set, will set an attribute for each Cloud Pub/Sub
      message with the given name and the message's publish time as the value.
  """
  input_schema = schemas.schema_from_element_type(pcoll.element_type)

  extra_fields: List[str] = []
  if isinstance(attributes, str):
    attributes = [attributes]
  if attributes:
    extra_fields.extend(attributes)
  if attributes_map:
    extra_fields.append(attributes_map)

  def attributes_extractor(row):
    if attributes_map:
      attribute_values = dict(getattr(row, attributes_map))
    else:
      attribute_values = {}
    if attributes:
      attribute_values.update({attr: getattr(row, attr) for attr in attributes})
    return attribute_values

  schema_names = set(f.name for f in input_schema.fields)
  missing_attribute_names = set(extra_fields) - schema_names
  if missing_attribute_names:
    raise ValueError(
        f'Attribute fields {missing_attribute_names} '
        f'not found in schema fields {schema_names}')

  payload_schema = schema_pb2.Schema(
      fields=[
          field for field in input_schema.fields
          if field.name not in extra_fields
      ])
  formatter = _create_formatter(format, schema, payload_schema)
  return (
      pcoll | beam.Map(
          lambda row: beam.io.gcp.pubsub.PubsubMessage(
              formatter(row), attributes_extractor(row)))
      | beam.io.WriteToPubSub(
          topic,
          with_attributes=True,
          id_label=id_attribute,
          timestamp_attribute=timestamp_attribute))


def io_providers():
  with open(os.path.join(os.path.dirname(__file__), 'standard_io.yaml')) as fin:
    return yaml_provider.parse_providers(yaml.load(fin, Loader=yaml.SafeLoader))
