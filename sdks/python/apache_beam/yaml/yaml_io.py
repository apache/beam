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

import io
import os
from typing import Any
from typing import Callable
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import fastavro
import yaml

import apache_beam as beam
import apache_beam.io as beam_io
from apache_beam.io import ReadFromAvro
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import ReadFromCsv
from apache_beam.io import ReadFromJson
from apache_beam.io import ReadFromParquet
from apache_beam.io import WriteToAvro
from apache_beam.io import WriteToBigQuery
from apache_beam.io import WriteToCsv
from apache_beam.io import WriteToJson
from apache_beam.io import WriteToParquet
from apache_beam.io import avroio
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import schemas
from apache_beam.yaml import json_utils
from apache_beam.yaml import yaml_errors
from apache_beam.yaml import yaml_provider


def read_from_text(
    path: str,
    delimiter: Optional[str] = None,
):
  # TODO(yaml): Consider passing the filename and offset, possibly even
  # by default.

  """Reads lines from text files into Beam rows.

  The resulting collection consists of Beam rows, each with a single string
  field named "line."

  For example, each Beam row will look like: ::

      line: "a single line of text from source"

  Parses a text file as newline-delimited elements, by default assuming
  ``UTF-8`` encoding. Supports newline delimiters ``\n`` and ``\r\n``
  or specified delimiter.

  Args:
    delimiter (str): Delimiter to split records.
    path (str): The file path to read from as a local file path or a
        GCS ``gs://`` path. The path can contain glob
      characters such as ``*`` and ``?``.
  """
  return beam_io.ReadFromText(
      file_pattern=path,
      delimiter=delimiter) | beam.Map(lambda s: beam.Row(line=s))


@beam.ptransform_fn
def write_to_text(pcoll, path: str):
  """Writes Beam rows to a (set of) text files(s).

  The input must be a beam Row whose schema has exactly one field.

  For example: ::

      key: "text to write"

  where `key` can be any name. The idea is that the previous transform should be
  outputting a collection of Beam rows that have only one field which is the
  text to be written.

  See MapToFields for guidance on how to drop and map rows to this format.

  Args:
      path (str): The file path to write to as a local file path or a
        GCS ``gs://`` path. The files written will
        begin with this prefix, followed by a shard identifier.
  """
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


def read_from_csv(
    path: str,
    comment: Optional[str] = None,
    delimiter: Optional[str] = None,
    **kwargs):
  """Reads comma-separated values (csv) files into Beam rows.

  For more information about possible arguments, see
  <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>

  Args:
      delimiter (str): Character or regex pattern to treat as the delimiter,
        default ',' (comma).
      comment (str): Character indicating that the remainder of line should
        not be parsed. If found at the beginning of a line, the line will be
        ignored altogether. This parameter must be a single character.
      path (str): The file path to read from as a local file path or a
        GCS ``gs://`` path. The path can contain glob
        characters such as ``*`` and ``?``.
  """
  return ReadFromCsv(path=path, comment=comment, sep=delimiter, **kwargs)


def write_to_csv(path: str, delimiter: Optional[str] = ",", **kwargs):
  """Writes Beam rows to a (set of) comma-separated values (csv) files.

  For more information about possible arguments, see
  <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html>

  Args:
      delimiter (str): Character or regex pattern to treat as the delimiter.
      path (str): The file path to write to as a local file path or a
        GCS ``gs://`` path. The files written will
        begin with this prefix, followed by a shard identifier.
  """
  return WriteToCsv(path=path, sep=delimiter, **kwargs)


def read_from_json(path: str, **kwargs):
  """Reads json values from files into Beam rows.

    For more information about possible arguments, see
    <https://pandas.pydata.org/docs/reference/api/pandas.read_json.html>

    Args:
        path (str): The file path to read from as a local file path or a
          GCS ``gs://`` path. The path can contain glob
          characters such as ``*`` and ``?``.
      """
  return ReadFromJson(path=path, **kwargs)


def write_to_json(path: str, **kwargs):
  """Writes Beam rows as json values to files.

      For more information about possible arguments, see
      <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html>

      Args:
          path (str): The file path to write to as a local file path or a
            GCS ``gs://`` path. The files written will
            begin with this prefix, followed by a shard identifier.
        """
  return WriteToJson(path=path, **kwargs)


def read_from_parquet(path: str, **kwargs):
  """Read a set of Parquet files defined by a given file pattern.

        For more information about possible arguments, see
        <https://beam.apache.org/releases/pydoc/BEAM_VERSION/apache_beam.io.parquetio.html#apache_beam.io.parquetio.ReadFromParquet>

        Args:
            path (str): The file path to read from as a local file path or a
              GCS ``gs://`` path.
          """
  return ReadFromParquet(file_pattern=path, as_rows=True, **kwargs)


def write_to_parquet(path: str, **kwargs):
  """Writes parquet files from a collection of Beam rows.

      For more information about possible arguments, see
        <https://beam.apache.org/releases/pydoc/BEAM_VERSION/apache_beam.io.parquetio.html#apache_beam.io.parquetio.WriteToParquet>

      Args:
        path (str): The file path to write to as a local file path or a
          GCS ``gs://`` path. The files written will begin
          with this prefix, followed by a shard identifier, and
          end in a common extension.
      """
  return WriteToParquet(file_path_prefix=path, **kwargs)


def read_from_avro(path: str, **kwargs):
  """Reads records from avro files into Beam rows.

    Records that are of simple types will be
    mapped to beam Rows with a single `record` field containing the records
    value. Records that are of Avro type ``RECORD`` will be mapped to Beam rows
    that comply with the schema contained in the Avro file that contains those
    records.

    For more information about possible arguments, see
        <https://beam.apache.org/releases/pydoc/BEAM_VERSION/apache_beam.io.avroio.html#apache_beam.io.avroio.ReadFromAvro>

    Args:
      path (str): The file path to read from as a local file path or a
        GCS ``gs://`` path.
    """
  return ReadFromAvro(file_pattern=path, as_rows=True, **kwargs)


def write_to_avro(path: str, **kwargs):
  """Writes avro records into files from a collection of Beam rows.

      The avro schema will be automatically generated and used to write the
      output records.

      For more information about possible arguments, see
          <https://beam.apache.org/releases/pydoc/BEAM_VERSION/apache_beam.io.avroio.html#apache_beam.io.avroio.WriteToAvro>

      Args:
        path (str): The file path to write to as a local file path or a
          GCS ``gs://`` path. The files written will begin
          with this prefix, followed by a shard identifier, and
          end in a common extension.
      """
  return WriteToAvro(file_path_prefix=path, **kwargs)


def read_from_bigquery(
    *,
    table: Optional[str] = None,
    query: Optional[str] = None,
    row_restriction: Optional[str] = None,
    fields: Optional[Iterable[str]] = None):
  """Reads data from BigQuery.

  Exactly one of table or query must be set.
  If query is set, neither row_restriction nor fields should be set.

  Args:
    table (str): The table to read from, specified as `DATASET.TABLE`
      or `PROJECT:DATASET.TABLE`.
    query (str): A query to be used instead of the table argument.
    row_restriction (str): Optional SQL text filtering statement, similar to a
      WHERE clause in a query. Aggregates are not supported. Restricted to a
      maximum length for 1 MB.
    selected_fields (List[str]): Optional List of names of the fields in the
      table that should be read. If empty, all fields will be read. If the
      specified field is a nested field, all the sub-fields in the field will be
      selected. The output field order is unrelated to the order of fields
      given here.
  """
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
    table: str,
    *,
    create_disposition: Optional[str] = BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition: Optional[str] = BigQueryDisposition.WRITE_APPEND,
    error_handling=None):
  f"""Writes data to a BigQuery table.

  Args:
    table (str): The table to read from, specified as `DATASET.TABLE`
      or `PROJECT:DATASET.TABLE`.
      create_disposition (BigQueryDisposition): A string describing what
        happens if the table does not exist. Possible values are:

        * :attr:`{BigQueryDisposition.CREATE_IF_NEEDED}`: create if does not
          exist.
        * :attr:`{BigQueryDisposition.CREATE_NEVER}`: fail the write if does not
          exist.

        Defaults to `{BigQueryDisposition.CREATE_IF_NEEDED}`.

      write_disposition (BigQueryDisposition): A string describing what happens
        if the table has already some data. Possible values are:

        * :attr:`{BigQueryDisposition.WRITE_TRUNCATE}`: delete existing rows.
        * :attr:`{BigQueryDisposition.WRITE_APPEND}`: add to existing rows.
        * :attr:`{BigQueryDisposition.WRITE_EMPTY}`: fail the write if table not
          empty.

        For streaming pipelines WriteTruncate can not be used.

        Defaults to `{BigQueryDisposition.WRITE_APPEND}`.

      error_handling: If specified, should be a mapping giving an output into
        which to emit records that failed to bet written to BigQuery, as
        described at https://beam.apache.org/documentation/sdks/yaml-errors/
        Otherwise permanently failing records will cause pipeline failure.
  """
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

  format = format.upper()

  def _validate_schema():
    if not schema:
      raise ValueError(
          f'{format} format requires valid {format} schema to be passed to '
          f'schema parameter.')

  if format == 'RAW':
    if schema:
      raise ValueError('RAW format does not take a schema')
    return (
        schema_pb2.Schema(fields=[schemas.schema_field('payload', bytes)]),
        lambda payload: beam.Row(payload=payload))
  elif format == 'JSON':
    _validate_schema()
    beam_schema = json_utils.json_schema_to_beam_schema(schema)
    return beam_schema, json_utils.json_parser(beam_schema, schema)
  elif format == 'AVRO':
    _validate_schema()
    beam_schema = avroio.avro_schema_to_beam_schema(schema)
    covert_to_row = avroio.avro_dict_to_beam_row(schema, beam_schema)
    # pylint: disable=line-too-long
    return (
        beam_schema,
        lambda record: covert_to_row(
            fastavro.schemaless_reader(io.BytesIO(record), schema)))  # type: ignore[call-arg]
  else:
    raise ValueError(f'Unknown format: {format}')


def _create_formatter(
    format, schema: Any,
    beam_schema: schema_pb2.Schema) -> Callable[[beam.Row], bytes]:

  if format.islower():
    format = format.upper()

  if format == 'RAW':
    if schema:
      raise ValueError('RAW format does not take a schema')
    field_names = [field.name for field in beam_schema.fields]
    if len(field_names) != 1:
      raise ValueError(f'Expecting exactly one field, found {field_names}')

    def convert_to_bytes(row):
      output = getattr(row, field_names[0])
      if isinstance(output, bytes):
        return output
      elif isinstance(output, str):
        return output.encode('utf-8')
      else:
        raise ValueError(
            f"Cannot encode payload for WriteToPubSub. "
            f"Expected valid string or bytes object, "
            f"got {repr(output)} of type {type(output)}.")

    return convert_to_bytes
  elif format == 'JSON':
    return json_utils.json_formater(beam_schema)
  elif format == 'AVRO':
    avro_schema = schema or avroio.beam_schema_to_avro_schema(beam_schema)
    from_row = avroio.beam_row_to_avro_dict(avro_schema, beam_schema)

    def formatter(row):
      buffer = io.BytesIO()
      fastavro.schemaless_writer(buffer, avro_schema, from_row(row))
      buffer.seek(0)
      return buffer.read()

    return formatter
  else:
    raise ValueError(f'Unknown format: {format}')


@beam.ptransform_fn
@yaml_errors.maybe_with_exception_handling_transform_fn
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

        - RAW: Produces records with a single `payload` field whose contents
            are the raw bytes of the pubsub message.
        - AVRO: Parses records with a given Avro schema.
        - JSON: Parses records with a given JSON schema.

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
@yaml_errors.maybe_with_exception_handling_transform_fn
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
  """Writes messages to Cloud Pub/Sub.

  Args:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    format: How to format the message payload.  Currently suported
      formats are

        - RAW: Expects a message with a single field (excluding
            attribute-related fields) whose contents are used as the raw bytes
            of the pubsub message.
        - AVRO: Encodes records with a given Avro schema, which may be inferred
            from the input PCollection schema.
        - JSON: Formats records with a given JSON schema, which may be inferred
            from the input PCollection schema.

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
