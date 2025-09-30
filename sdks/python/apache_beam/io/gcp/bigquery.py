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

"""BigQuery sources and sinks.

This module implements reading from and writing to BigQuery tables. It relies
on several classes exposed by the BigQuery API: TableSchema, TableFieldSchema,
TableRow, and TableCell. The default mode is to return table rows read from a
BigQuery source as dictionaries. Similarly a Write transform to a BigQuerySink
accepts PCollections of dictionaries. This is done for more convenient
programming.  If desired, the native TableRow objects can be used throughout to
represent rows (use an instance of TableRowJsonCoder as a coder argument when
creating the sources or sinks respectively).

Also, for programming convenience, instances of TableReference and TableSchema
have a string representation that can be used for the corresponding arguments:

  - TableReference can be a PROJECT:DATASET.TABLE or DATASET.TABLE string.
  - TableSchema can be a NAME:TYPE{,NAME:TYPE}* string
    (e.g. 'month:STRING,event_count:INTEGER').

The syntax supported is described here:
https://cloud.google.com/bigquery/bq-command-line-tool-quickstart

BigQuery sources can be used as main inputs or side inputs. A main input
(common case) is expected to be massive and will be split into manageable chunks
and processed in parallel. Side inputs are expected to be small and will be read
completely every time a ParDo DoFn gets executed. In the example below the
lambda function implementing the DoFn for the Map transform will get on each
call *one* row of the main table and *all* rows of the side table. The runner
may use some caching techniques to share the side inputs between calls in order
to avoid excessive reading:::

  main_table = pipeline | 'VeryBig' >> beam.io.ReadFromBigQuery(...)
  side_table = pipeline | 'NotBig' >> beam.io.ReadFromBigQuery(...)
  results = (
      main_table
      | 'ProcessData' >> beam.Map(
          lambda element, side_input: ..., AsList(side_table)))

There is no difference in how main and side inputs are read. What makes the
side_table a 'side input' is the AsList wrapper used when passing the table
as a parameter to the Map transform. AsList signals to the execution framework
that its input should be made available whole.

The main and side inputs are implemented differently. Reading a BigQuery table
as main input entails exporting the table to a set of GCS files (in AVRO or in
JSON format) and then processing those files.

Users may provide a query to read from rather than reading all of a BigQuery
table. If specified, the result obtained by executing the specified query will
be used as the data of the input transform.::

  query_results = pipeline | beam.io.gcp.bigquery.ReadFromBigQuery(
      query='SELECT year, mean_temp FROM samples.weather_stations')

When creating a BigQuery input transform, users should provide either a query
or a table. Pipeline construction will fail with a validation error if neither
or both are specified.

When reading via `ReadFromBigQuery` using `EXPORT`,
bytes are returned decoded as bytes.
This is due to the fact that ReadFromBigQuery uses Avro exports by default.
When reading from BigQuery using `apache_beam.io.BigQuerySource`, bytes are
returned as base64-encoded bytes. To get base64-encoded bytes using
`ReadFromBigQuery`, you can use the flag `use_json_exports` to export
data as JSON, and receive base64-encoded bytes.

ReadAllFromBigQuery
-------------------
Beam 2.27.0 introduces a new transform called `ReadAllFromBigQuery` which
allows you to define table and query reads from BigQuery at pipeline
runtime.:::

  read_requests = p | beam.Create([
      ReadFromBigQueryRequest(query='SELECT * FROM mydataset.mytable'),
      ReadFromBigQueryRequest(table='myproject.mydataset.mytable')])
  results = read_requests | ReadAllFromBigQuery()

A good application for this transform is in streaming pipelines to
refresh a side input coming from BigQuery. This would work like so:::

  side_input = (
      p
      | 'PeriodicImpulse' >> PeriodicImpulse(
          first_timestamp, last_timestamp, interval, True)
      | 'MapToReadRequest' >> beam.Map(
          lambda x: ReadFromBigQueryRequest(table='dataset.table'))
      | beam.io.ReadAllFromBigQuery())
  main_input = (
      p
      | 'MpImpulse' >> beam.Create(sample_main_input_elements)
      |
      'MapMpToTimestamped' >> beam.Map(lambda src: TimestampedValue(src, src))
      | 'WindowMpInto' >> beam.WindowInto(
          window.FixedWindows(main_input_windowing_interval)))
  result = (
      main_input
      | 'ApplyCrossJoin' >> beam.FlatMap(
          cross_join, rights=beam.pvalue.AsIter(side_input)))

**Note**: This transform is supported on Portable and Dataflow v2 runners.

**Note**: This transform does not currently clean up temporary datasets
created for its execution. (BEAM-11359)

Writing Data to BigQuery
========================

The `WriteToBigQuery` transform is the recommended way of writing data to
BigQuery. It supports a large set of parameters to customize how you'd like to
write to BigQuery.

Table References
----------------

This transform allows you to provide static `project`, `dataset` and `table`
parameters which point to a specific BigQuery table to be created. The `table`
parameter can also be a dynamic parameter (i.e. a callable), which receives an
element to be written to BigQuery, and returns the table that that element
should be sent to.

You may also provide a tuple of PCollectionView elements to be passed as side
inputs to your callable. For example, suppose that one wishes to send
events of different types to different tables, and the table names are
computed at pipeline runtime, one may do something like the following::

    with Pipeline() as p:
      elements = (p | 'Create elements' >> beam.Create([
        {'type': 'error', 'timestamp': '12:34:56', 'message': 'bad'},
        {'type': 'user_log', 'timestamp': '12:34:59', 'query': 'flu symptom'},
      ]))

      table_names = (p | 'Create table_names' >> beam.Create([
        ('error', 'my_project:dataset1.error_table_for_today'),
        ('user_log', 'my_project:dataset1.query_table_for_today'),
      ]))

      table_names_dict = beam.pvalue.AsDict(table_names)

      elements | beam.io.gcp.bigquery.WriteToBigQuery(
        table=lambda row, table_dict: table_dict[row['type']],
        table_side_inputs=(table_names_dict,))

In the example above, the `table_dict` argument passed to the function in
`table_dict` is the side input coming from `table_names_dict`, which is passed
as part of the `table_side_inputs` argument.

Schemas
---------

This transform also allows you to provide a static or dynamic `schema`
parameter (i.e. a callable).

If providing a callable, this should take in a table reference (as returned by
the `table` parameter), and return the corresponding schema for that table.
This allows to provide different schemas for different tables::

    def compute_table_name(row):
      ...

    errors_schema = {'fields': [
      {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
      {'name': 'message', 'type': 'STRING', 'mode': 'NULLABLE'}]}
    queries_schema = {'fields': [
      {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
      {'name': 'query', 'type': 'STRING', 'mode': 'NULLABLE'}]}

    with Pipeline() as p:
      elements = (p | beam.Create([
        {'type': 'error', 'timestamp': '12:34:56', 'message': 'bad'},
        {'type': 'user_log', 'timestamp': '12:34:59', 'query': 'flu symptom'},
      ]))

      elements | beam.io.gcp.bigquery.WriteToBigQuery(
        table=compute_table_name,
        schema=lambda table: (errors_schema
                              if 'errors' in table
                              else queries_schema))

It may be the case that schemas are computed at pipeline runtime. In cases
like these, one can also provide a `schema_side_inputs` parameter, which is
a tuple of PCollectionViews to be passed to the schema callable (much like
the `table_side_inputs` parameter).

Additional Parameters for BigQuery Tables
-----------------------------------------

This sink is able to create tables in BigQuery if they don't already exist. It
also relies on creating temporary tables when performing file loads.

The WriteToBigQuery transform creates tables using the BigQuery API by
inserting a load job (see the API reference [1]), or by inserting a new table
(see the API reference for that [2][3]).

When creating a new BigQuery table, there are a number of extra parameters
that one may need to specify. For example, clustering, partitioning, data
encoding, etc. It is possible to provide these additional parameters by
passing a Python dictionary as `additional_bq_parameters` to the transform.
As an example, to create a table that has specific partitioning, and
clustering properties, one would do the following::

    additional_bq_parameters = {
      'timePartitioning': {'type': 'DAY'},
      'clustering': {'fields': ['country']}}
    with Pipeline() as p:
      elements = (p | beam.Create([
        {'country': 'mexico', 'timestamp': '12:34:56', 'query': 'acapulco'},
        {'country': 'canada', 'timestamp': '12:34:59', 'query': 'influenza'},
      ]))

      elements | beam.io.gcp.bigquery.WriteToBigQuery(
        table='project_name1:dataset_2.query_events_table',
        additional_bq_parameters=additional_bq_parameters)

Much like the schema case, the parameter with `additional_bq_parameters` can
also take a callable that receives a table reference.


[1] https://cloud.google.com/bigquery/docs/reference/rest/v2/Job\
        #jobconfigurationload
[2] https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
[3] https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource

Chaining of operations after WriteToBigQuery
--------------------------------------------
WriteToBigQuery returns an object with several PCollections that consist of
metadata about the write operations. These are useful to inspect the write
operation and follow with the results::

  schema = {'fields': [
      {'name': 'column', 'type': 'STRING', 'mode': 'NULLABLE'}]}

  error_schema = {'fields': [
      {'name': 'destination', 'type': 'STRING', 'mode': 'NULLABLE'},
      {'name': 'row', 'type': 'STRING', 'mode': 'NULLABLE'},
      {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'}]}

  with Pipeline() as p:
    result = (p
      | 'Create Columns' >> beam.Create([
              {'column': 'value'},
              {'bad_column': 'bad_value'}
            ])
      | 'Write Data' >> WriteToBigQuery(
              method=WriteToBigQuery.Method.STREAMING_INSERTS,
              table=my_table,
              schema=schema,
              insert_retry_strategy=RetryStrategy.RETRY_NEVER
            ))

    _ = (result.failed_rows_with_errors
      | 'Get Errors' >> beam.Map(lambda e: {
              "destination": e[0],
              "row": json.dumps(e[1]),
              "error_message": e[2][0]['message']
            })
      | 'Write Errors' >> WriteToBigQuery(
              method=WriteToBigQuery.Method.STREAMING_INSERTS,
              table=error_log_table,
              schema=error_schema,
            ))

Often, the simplest use case is to chain an operation after writing data to
BigQuery.To do this, one can chain the operation after one of the output
PCollections. A generic way in which this operation (independent of write
method) could look like::

  def chain_after(result):
    try:
      # This works for FILE_LOADS, where we run load and possibly copy jobs.
      return (result.destination_load_jobid_pairs,
          result.destination_copy_jobid_pairs) | beam.Flatten()
    except AttributeError:
      # Works for STREAMING_INSERTS, where we return the rows BigQuery rejected
      return result.failed_rows

  result = (pcoll | WriteToBigQuery(...))

  _ = (chain_after(result)
       | beam.Reshuffle() # Force a 'commit' of the intermediate date
       | MyOperationAfterWriteToBQ())

Attributes can be accessed using dot notation or bracket notation:

result.failed_rows                  <--> result['FailedRows']
result.failed_rows_with_errors      <--> result['FailedRowsWithErrors']
result.destination_load_jobid_pairs <--> result['destination_load_jobid_pairs']
result.destination_file_pairs       <--> result['destination_file_pairs']
result.destination_copy_jobid_pairs <--> result['destination_copy_jobid_pairs']

Writing with Storage Write API using Cross Language
---------------------------------------------------
This sink is able to write with BigQuery's Storage Write API. To do so, specify
the method `WriteToBigQuery.Method.STORAGE_WRITE_API`. This will use the
StorageWriteToBigQuery() transform to discover and use the Java implementation.
Using this transform directly will require the use of beam.Row() elements.

Similar to streaming inserts, it returns two dead-letter queue PCollections:
one containing just the failed rows and the other containing failed rows and
errors. They can be accessed with `failed_rows` and `failed_rows_with_errors`,
respectively. See the examples above for how to do this.


*** Short introduction to BigQuery concepts ***
Tables have rows (TableRow) and each row has cells (TableCell).
A table has a schema (TableSchema), which in turn describes the schema of each
cell (TableFieldSchema). The terms field and cell are used interchangeably.

TableSchema: Describes the schema (types and order) for values in each row.
  Has one attribute, 'field', which is list of TableFieldSchema objects.

TableFieldSchema: Describes the schema (type, name) for one field.
  Has several attributes, including 'name' and 'type'. Common values for
  the type attribute are: 'STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'NUMERIC',
  'GEOGRAPHY'.
  All possible values are described at:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types

TableRow: Holds all values in a table row. Has one attribute, 'f', which is a
  list of TableCell instances.

TableCell: Holds the value for one cell (or field).  Has one attribute,
  'v', which is a JsonValue instance. This class is defined in
  apitools.base.py.extra_types.py module.

As of Beam 2.7.0, the NUMERIC data type is supported. This data type supports
high-precision decimal numbers (precision of 38 digits, scale of 9 digits).
The GEOGRAPHY data type works with Well-Known Text (See
https://en.wikipedia.org/wiki/Well-known_text) format for reading and writing
to BigQuery.
BigQuery IO requires values of BYTES datatype to be encoded using base64
encoding when writing to BigQuery.

**Updates to the I/O connector code**

For any significant updates to this I/O connector, please consider involving
corresponding code reviewers mentioned in
https://github.com/apache/beam/blob/master/sdks/python/OWNERS
"""

# pytype: skip-file

import collections
import io
import itertools
import json
import logging
import random
import secrets
import time
import uuid
import warnings
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import fastavro
from objsize import get_deep_size

import apache_beam as beam
from apache_beam import coders
from apache_beam import pvalue
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io import range_trackers
from apache_beam.io.avroio import _create_avro_source as create_avro_source
from apache_beam.io.filesystems import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp import bigquery_schema_tools
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery_io_metadata import create_bigquery_io_metadata
from apache_beam.io.gcp.bigquery_read_internal import _BigQueryReadSplit
from apache_beam.io.gcp.bigquery_read_internal import _JsonToDictCoder
from apache_beam.io.gcp.bigquery_read_internal import _PassThroughThenCleanup
from apache_beam.io.gcp.bigquery_read_internal import _PassThroughThenCleanupTempDatasets
from apache_beam.io.gcp.bigquery_read_internal import bigquery_export_destination_uri
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.iobase import BoundedSource
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.iobase import SDFBoundedSourceReader
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.textio import _TextSource as TextSource
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import Lineage
from apache_beam.options import value_provider as vp
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible
from apache_beam.pvalue import PCollection
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.sideinputs import SIDE_INPUT_PREFIX
from apache_beam.transforms.sideinputs import get_sideinput_index
from apache_beam.transforms.util import ReshufflePerKey
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import schema_from_element_type
from apache_beam.utils import retry
from apache_beam.utils.annotations import deprecated

try:
  from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
  from apache_beam.io.gcp.internal.clients.bigquery import TableReference
  from apache_beam.io.gcp.internal.clients.bigquery import JobReference
except ImportError:
  DatasetReference = None
  TableReference = None
  JobReference = None

_LOGGER = logging.getLogger(__name__)

try:
  import google.cloud.bigquery_storage_v1 as bq_storage
except ImportError:
  _LOGGER.info(
      'No module named google.cloud.bigquery_storage_v1. '
      'As a result, the ReadFromBigQuery transform *CANNOT* be '
      'used with `method=DIRECT_READ`.')

__all__ = [
    'TableRowJsonCoder',
    'BigQueryDisposition',
    'BigQuerySource',
    'BigQuerySink',
    'BigQueryQueryPriority',
    'WriteToBigQuery',
    'WriteResult',
    'ReadFromBigQuery',
    'ReadFromBigQueryRequest',
    'ReadAllFromBigQuery',
    'SCHEMA_AUTODETECT',
]
"""
Template for BigQuery jobs created by BigQueryIO. This template is:
`"beam_bq_job_{job_type}_{job_id}_{step_id}_{random}"`, where:

- `job_type` represents the BigQuery job type (e.g. extract / copy / load /
    query).
- `job_id` is the Beam job name.
- `step_id` is a UUID representing the Dataflow step that created the
    BQ job.
- `random` is a random string.

NOTE: This job name template does not have backwards compatibility guarantees.
"""
BQ_JOB_NAME_TEMPLATE = "beam_bq_job_{job_type}_{job_id}_{step_id}{random}"
"""
The maximum number of times that a bundle of rows that errors out should be
sent for insertion into BigQuery.

The default is 10,000 with exponential backoffs, so a bundle of rows may be
tried for a very long time. You may reduce this property to reduce the number
of retries.
"""
MAX_INSERT_RETRIES = 10000
"""
The maximum byte size for a BigQuery legacy streaming insert payload.

Note: The actual limit is 10MB, but we set it to 9MB to make room for request
overhead: https://cloud.google.com/bigquery/quotas#streaming_inserts
"""
MAX_INSERT_PAYLOAD_SIZE = 9 << 20


@deprecated(since='2.11.0', current="bigquery_tools.parse_table_reference")
def _parse_table_reference(table, dataset=None, project=None):
  return bigquery_tools.parse_table_reference(table, dataset, project)


@deprecated(
    since='2.11.0', current="bigquery_tools.parse_table_schema_from_json")
def parse_table_schema_from_json(schema_string):
  return bigquery_tools.parse_table_schema_from_json(schema_string)


@deprecated(since='2.11.0', current="bigquery_tools.default_encoder")
def default_encoder(obj):
  return bigquery_tools.default_encoder(obj)


@deprecated(since='2.11.0', current="bigquery_tools.RowAsDictJsonCoder")
def RowAsDictJsonCoder(*args, **kwargs):
  return bigquery_tools.RowAsDictJsonCoder(*args, **kwargs)


@deprecated(since='2.11.0', current="bigquery_tools.BigQueryWrapper")
def BigQueryWrapper(*args, **kwargs):
  return bigquery_tools.BigQueryWrapper(*args, **kwargs)


class TableRowJsonCoder(coders.Coder):
  """A coder for a TableRow instance to/from a JSON string.

  Note that the encoding operation (used when writing to sinks) requires the
  table schema in order to obtain the ordered list of field names. Reading from
  sources on the other hand does not need the table schema.
  """
  def __init__(self, table_schema=None):
    # The table schema is needed for encoding TableRows as JSON (writing to
    # sinks) because the ordered list of field names is used in the JSON
    # representation.
    self.table_schema = table_schema
    # Precompute field names since we need them for row encoding.
    if self.table_schema:
      self.field_names = tuple(fs.name for fs in self.table_schema.fields)
      self.field_types = tuple(fs.type for fs in self.table_schema.fields)

  def encode(self, table_row):
    if self.table_schema is None:
      raise AttributeError(
          'The TableRowJsonCoder requires a table schema for '
          'encoding operations. Please specify a table_schema argument.')
    try:
      return json.dumps(
          collections.OrderedDict(
              zip(
                  self.field_names,
                  [from_json_value(f.v) for f in table_row.f])),
          allow_nan=False,
          default=bigquery_tools.default_encoder)
    except ValueError as e:
      raise ValueError('%s. %s' % (e, bigquery_tools.JSON_COMPLIANCE_ERROR))

  def decode(self, encoded_table_row):
    od = json.loads(
        encoded_table_row, object_pairs_hook=collections.OrderedDict)
    return bigquery.TableRow(
        f=[bigquery.TableCell(v=to_json_value(e)) for e in od.values()])


class BigQueryDisposition(object):
  """Class holding standard strings used for create and write dispositions."""

  CREATE_NEVER = 'CREATE_NEVER'
  CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
  WRITE_TRUNCATE = 'WRITE_TRUNCATE'
  WRITE_APPEND = 'WRITE_APPEND'
  WRITE_EMPTY = 'WRITE_EMPTY'

  @staticmethod
  def validate_create(disposition):
    values = (
        BigQueryDisposition.CREATE_NEVER, BigQueryDisposition.CREATE_IF_NEEDED)
    if disposition not in values:
      raise ValueError(
          'Invalid create disposition %s. Expecting %s' % (disposition, values))
    return disposition

  @staticmethod
  def validate_write(disposition):
    values = (
        BigQueryDisposition.WRITE_TRUNCATE,
        BigQueryDisposition.WRITE_APPEND,
        BigQueryDisposition.WRITE_EMPTY)
    if disposition not in values:
      raise ValueError(
          'Invalid write disposition %s. Expecting %s' % (disposition, values))
    return disposition


class BigQueryQueryPriority(object):
  """Class holding standard strings used for query priority."""

  INTERACTIVE = 'INTERACTIVE'
  BATCH = 'BATCH'


# -----------------------------------------------------------------------------
# BigQuerySource, BigQuerySink.


@deprecated(since='2.25.0', current="ReadFromBigQuery")
def BigQuerySource(
    table=None,
    dataset=None,
    project=None,
    query=None,
    validate=False,
    coder=None,
    use_standard_sql=False,
    flatten_results=True,
    kms_key=None,
    use_dataflow_native_source=False):
  if use_dataflow_native_source:
    warnings.warn(
        "Native sources no longer implemented; "
        "falling back to standard Beam source.")
  return ReadFromBigQuery(
      table=table,
      dataset=dataset,
      project=project,
      query=query,
      validate=validate,
      coder=coder,
      use_standard_sql=use_standard_sql,
      flatten_results=flatten_results,
      use_json_exports=True,
      kms_key=kms_key)


@deprecated(since='2.25.0', current="ReadFromBigQuery")
def _BigQuerySource(*args, **kwargs):
  """A source based on a BigQuery table."""
  warnings.warn(
      "Native sources no longer implemented; "
      "falling back to standard Beam source.")
  return ReadFromBigQuery(*args, **kwargs)


# TODO(https://github.com/apache/beam/issues/21622): remove the serialization
# restriction in transform implementation once InteractiveRunner can work
# without runner api roundtrips.
@dataclass
class _BigQueryExportResult:
  coder: beam.coders.Coder
  paths: List[str]


class _CustomBigQuerySource(BoundedSource):
  def __init__(
      self,
      method,
      gcs_location=None,
      table=None,
      dataset=None,
      project=None,
      query=None,
      validate=False,
      pipeline_options=None,
      coder=None,
      use_standard_sql=False,
      flatten_results=True,
      kms_key=None,
      bigquery_job_labels=None,
      use_json_exports=False,
      job_name=None,
      step_name=None,
      unique_id=None,
      temp_dataset=None,
      query_priority=BigQueryQueryPriority.BATCH):
    if table is not None and query is not None:
      raise ValueError(
          'Both a BigQuery table and a query were specified.'
          ' Please specify only one of these.')
    elif table is None and query is None:
      raise ValueError('A BigQuery table or a query must be specified')
    elif table is not None:
      self.table_reference = bigquery_tools.parse_table_reference(
          table, dataset, project)
      self.query = None
      self.use_legacy_sql = True
    else:
      if isinstance(query, str):
        query = StaticValueProvider(str, query)
      self.query = query
      # TODO(BEAM-1082): Change the internal flag to be standard_sql
      self.use_legacy_sql = not use_standard_sql
      self.table_reference = None

    self.method = method
    self.gcs_location = gcs_location
    self.project = project
    self.validate = validate
    self.flatten_results = flatten_results
    self.coder = coder or _JsonToDictCoder
    self.kms_key = kms_key
    self.export_result = None
    self.options = pipeline_options
    self.bq_io_metadata = None  # Populate in setup, as it may make an RPC
    self.bigquery_job_labels = bigquery_job_labels or {}
    self.use_json_exports = use_json_exports
    self.temp_dataset = temp_dataset
    self.query_priority = query_priority
    self._job_name = job_name or 'BQ_EXPORT_JOB'
    self._step_name = step_name
    self._source_uuid = unique_id

  def _get_bq_metadata(self):
    if not self.bq_io_metadata:
      self.bq_io_metadata = create_bigquery_io_metadata(self._step_name)
    return self.bq_io_metadata

  def display_data(self):
    export_format = 'JSON' if self.use_json_exports else 'AVRO'
    return {
        'method': str(self.method),
        'table': str(self.table_reference),
        'query': str(self.query),
        'project': str(self.project),
        'use_legacy_sql': self.use_legacy_sql,
        'bigquery_job_labels': json.dumps(self.bigquery_job_labels),
        'export_file_format': export_format,
        'launchesBigQueryJobs': DisplayDataItem(
            True, label="This Dataflow job launches bigquery jobs."),
    }

  def estimate_size(self):
    bq = bigquery_tools.BigQueryWrapper.from_pipeline_options(self.options)
    if self.table_reference is not None:
      table_ref = self.table_reference
      if (isinstance(self.table_reference, vp.ValueProvider) and
          self.table_reference.is_accessible()):
        table_ref = bigquery_tools.parse_table_reference(
            self.table_reference.get(), project=self._get_project())
      elif isinstance(self.table_reference, vp.ValueProvider):
        # Size estimation is best effort. We return None as we have
        # no access to the table that we're querying.
        return None
      if not table_ref.projectId:
        table_ref.projectId = self._get_project()
      table = bq.get_table(
          table_ref.projectId, table_ref.datasetId, table_ref.tableId)
      return int(table.numBytes)
    elif self.query is not None and self.query.is_accessible():
      project = self._get_project()
      query_job_name = bigquery_tools.generate_bq_job_name(
          self._job_name,
          self._source_uuid,
          bigquery_tools.BigQueryJobTypes.QUERY,
          '%s_%s' % (int(time.time()), random.randint(0, 1000)))
      job = bq._start_query_job(
          project,
          self.query.get(),
          self.use_legacy_sql,
          self.flatten_results,
          job_id=query_job_name,
          priority=self.query_priority,
          dry_run=True,
          kms_key=self.kms_key,
          job_labels=self._get_bq_metadata().add_additional_bq_job_labels(
              self.bigquery_job_labels))

      if job.statistics.totalBytesProcessed is None:
        # Some queries may not have access to `totalBytesProcessed` as a
        # result of row-level security.
        # > BigQuery hides sensitive statistics on all queries against
        # > tables with row-level security.
        # See cloud.google.com/bigquery/docs/managing-row-level-security
        # and cloud.google.com/bigquery/docs/best-practices-row-level-security
        return None

      return int(job.statistics.totalBytesProcessed)
    else:
      # Size estimation is best effort. We return None as we have
      # no access to the query that we're running.
      return None

  def _get_project(self):
    """Returns the project that queries and exports will be billed to."""

    project = self.options.view_as(GoogleCloudOptions).project
    if isinstance(project, vp.ValueProvider):
      project = project.get()
    if self.temp_dataset:
      return self.temp_dataset.projectId
    if not project:
      project = self.project
    return project

  def _create_source(self, path, coder):
    if not self.use_json_exports:
      return create_avro_source(path, validate=self.validate)
    else:
      return TextSource(
          path,
          min_bundle_size=0,
          compression_type=CompressionTypes.UNCOMPRESSED,
          strip_trailing_newlines=True,
          coder=coder,
          validate=self.validate)

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    if self.export_result is None:
      bq = bigquery_tools.BigQueryWrapper(
          temp_dataset_id=(
              self.temp_dataset.datasetId if self.temp_dataset else None),
          client=bigquery_tools.BigQueryWrapper._bigquery_client(self.options))

      if self.query is not None:
        self._setup_temporary_dataset(bq)
        self.table_reference = self._execute_query(bq)

      if isinstance(self.table_reference, vp.ValueProvider):
        self.table_reference = bigquery_tools.parse_table_reference(
            self.table_reference.get(), project=self._get_project())
      elif not self.table_reference.projectId:
        self.table_reference.projectId = self._get_project()
      Lineage.sources().add(
          'bigquery',
          self.table_reference.projectId,
          self.table_reference.datasetId,
          self.table_reference.tableId)

      schema, metadata_list = self._export_files(bq)
      self.export_result = _BigQueryExportResult(
          coder=self.coder(schema),
          paths=[metadata.path for metadata in metadata_list])

      if self.query is not None:
        bq.clean_up_temporary_dataset(self._get_project())

    for path in self.export_result.paths:
      source = self._create_source(path, self.export_result.coder)
      yield SourceBundle(
          weight=1.0, source=source, start_position=None, stop_position=None)

  def get_range_tracker(self, start_position, stop_position):
    class CustomBigQuerySourceRangeTracker(RangeTracker):
      """A RangeTracker that always returns positions as None."""
      def start_position(self):
        return None

      def stop_position(self):
        return None

    return CustomBigQuerySourceRangeTracker()

  def read(self, range_tracker):
    raise NotImplementedError('BigQuery source must be split before being read')

  @check_accessible(['query'])
  def _setup_temporary_dataset(self, bq):
    if self.temp_dataset:
      # Temp dataset was provided by the user so we can just return.
      return
    location = bq.get_query_location(
        self._get_project(), self.query.get(), self.use_legacy_sql)
    bq.create_temporary_dataset(
        self._get_project(), location, kms_key=self.kms_key)

  @check_accessible(['query'])
  def _execute_query(self, bq):
    query_job_name = bigquery_tools.generate_bq_job_name(
        self._job_name,
        self._source_uuid,
        bigquery_tools.BigQueryJobTypes.QUERY,
        '%s_%s' % (int(time.time()), random.randint(0, 1000)))
    job = bq._start_query_job(
        self._get_project(),
        self.query.get(),
        self.use_legacy_sql,
        self.flatten_results,
        job_id=query_job_name,
        priority=self.query_priority,
        kms_key=self.kms_key,
        job_labels=self._get_bq_metadata().add_additional_bq_job_labels(
            self.bigquery_job_labels))
    job_ref = job.jobReference
    bq.wait_for_bq_job(job_ref, max_retries=0)
    return bq._get_temp_table(self._get_project())

  def _export_files(self, bq):
    """Runs a BigQuery export job.

    Returns:
      bigquery.TableSchema instance, a list of FileMetadata instances
    """
    job_labels = self._get_bq_metadata().add_additional_bq_job_labels(
        self.bigquery_job_labels)
    export_job_name = bigquery_tools.generate_bq_job_name(
        self._job_name,
        self._source_uuid,
        bigquery_tools.BigQueryJobTypes.EXPORT,
        '%s_%s' % (int(time.time()), random.randint(0, 1000)))
    temp_location = self.options.view_as(GoogleCloudOptions).temp_location
    gcs_location = bigquery_export_destination_uri(
        self.gcs_location, temp_location, self._source_uuid)
    try:
      if self.use_json_exports:
        job_ref = bq.perform_extract_job([gcs_location],
                                         export_job_name,
                                         self.table_reference,
                                         bigquery_tools.FileFormat.JSON,
                                         project=self._get_project(),
                                         job_labels=job_labels,
                                         include_header=False)
      else:
        job_ref = bq.perform_extract_job([gcs_location],
                                         export_job_name,
                                         self.table_reference,
                                         bigquery_tools.FileFormat.AVRO,
                                         project=self._get_project(),
                                         include_header=False,
                                         job_labels=job_labels,
                                         use_avro_logical_types=True)
      bq.wait_for_bq_job(job_ref)
    except Exception as exn:  # pylint: disable=broad-except
      # The error messages thrown in this case are generic and misleading,
      # so leave this breadcrumb in case it's the root cause.
      logging.warning(
          "Error exporting table: %s. "
          "Note that external tables cannot be exported: "
          "https://cloud.google.com/bigquery/docs/external-tables"
          "#external_table_limitations",
          exn)
      raise
    metadata_list = FileSystems.match([gcs_location])[0].metadata_list

    if isinstance(self.table_reference, vp.ValueProvider):
      table_ref = bigquery_tools.parse_table_reference(
          self.table_reference.get(), project=self.project)
    else:
      table_ref = self.table_reference
    table = bq.get_table(
        table_ref.projectId, table_ref.datasetId, table_ref.tableId)

    return table.schema, metadata_list


class _CustomBigQueryStorageSource(BoundedSource):
  """A base class for BoundedSource implementations which read from BigQuery
  using the BigQuery Storage API.
  Args:
    table (str, TableReference): The ID of the table. If **dataset** argument is
      :data:`None` then the table argument must contain the entire table
      reference specified as: ``'PROJECT:DATASET.TABLE'`` or must specify a
      TableReference.
    dataset (str): Optional ID of the dataset containing this table or
      :data:`None` if the table argument specifies a TableReference.
    project (str): Optional ID of the project containing this table or
      :data:`None` if the table argument specifies a TableReference.
    selected_fields (List[str]): Optional List of names of the fields in the
      table that should be read. If empty, all fields will be read. If the
      specified field is a nested field, all the sub-fields in the field will be
      selected. The output field order is unrelated to the order of fields in
      selected_fields.
    row_restriction (str): Optional SQL text filtering statement, similar to a
      WHERE clause in a query. Aggregates are not supported. Restricted to a
      maximum length for 1 MB.
    use_native_datetime (bool): If :data:`True`, BigQuery DATETIME fields will
      be returned as native Python datetime objects. If :data:`False`,
      DATETIME fields will be returned as formatted strings (for example:
      2021-01-01T12:59:59). The default is :data:`False`.
  """

  # The maximum number of streams which will be requested when creating a read
  # session, regardless of the desired bundle size.
  MAX_SPLIT_COUNT = 10000
  # The minimum number of streams which will be requested when creating a read
  # session, regardless of the desired bundle size. Note that the server may
  # still choose to return fewer than ten streams based on the layout of the
  # table.
  MIN_SPLIT_COUNT = 10

  def __init__(
      self,
      method: str,
      query_priority: [BigQueryQueryPriority] = BigQueryQueryPriority.BATCH,
      table: Optional[Union[str, TableReference]] = None,
      dataset: Optional[str] = None,
      project: Optional[str] = None,
      query: Optional[str] = None,
      selected_fields: Optional[List[str]] = None,
      row_restriction: Optional[str] = None,
      pipeline_options: Optional[GoogleCloudOptions] = None,
      unique_id: Optional[uuid.UUID] = None,
      bigquery_job_labels: Optional[Dict] = None,
      bigquery_dataset_labels: Optional[Dict] = None,
      job_name: Optional[str] = None,
      step_name: Optional[str] = None,
      use_standard_sql: Optional[bool] = False,
      flatten_results: Optional[bool] = True,
      kms_key: Optional[str] = None,
      temp_dataset: Optional[DatasetReference] = None,
      temp_table: Optional[TableReference] = None,
      use_native_datetime: Optional[bool] = False,
      timeout: Optional[float] = None):

    if table is not None and query is not None:
      raise ValueError(
          'Both a BigQuery table and a query were specified.'
          ' Please specify only one of these.')
    elif table is None and query is None:
      raise ValueError('A BigQuery table or a query must be specified')
    elif table is not None:
      self.table_reference = bigquery_tools.parse_table_reference(
          table, dataset, project)
      self.query = None
      self.use_legacy_sql = True
    else:
      if isinstance(query, str):
        query = StaticValueProvider(str, query)
      self.query = query
      # TODO(BEAM-1082): Change the internal flag to be standard_sql
      self.use_legacy_sql = not use_standard_sql
      self.table_reference = None

    self.method = method
    self.project = project
    self.selected_fields = selected_fields
    self.row_restriction = row_restriction
    self.pipeline_options = pipeline_options
    self.split_result = None
    self.bigquery_job_labels = bigquery_job_labels or {}
    self.bigquery_dataset_labels = bigquery_dataset_labels or {}
    self.bq_io_metadata = None  # Populate in setup, as it may make an RPC
    self.flatten_results = flatten_results
    self.kms_key = kms_key
    self.temp_table = temp_table
    self.query_priority = query_priority
    self.use_native_datetime = use_native_datetime
    self.timeout = timeout
    self._job_name = job_name or 'BQ_DIRECT_READ_JOB'
    self._step_name = step_name
    self._source_uuid = unique_id

  def _get_parent_project(self):
    """Returns the project that will be billed."""
    if self.temp_table:
      return self.temp_table.projectId

    project = self.pipeline_options.view_as(GoogleCloudOptions).project
    if isinstance(project, vp.ValueProvider):
      project = project.get()
    if not project:
      project = self.project
    return project

  def _get_table_size(self, bq, table_reference):
    project = (
        table_reference.projectId
        if table_reference.projectId else self._get_parent_project())
    table = bq.get_table(
        project, table_reference.datasetId, table_reference.tableId)
    return table.numBytes

  def _get_bq_metadata(self):
    if not self.bq_io_metadata:
      self.bq_io_metadata = create_bigquery_io_metadata(self._step_name)
    return self.bq_io_metadata

  @check_accessible(['query'])
  def _setup_temporary_dataset(self, bq):
    if self.temp_table:
      # Temp dataset was provided by the user so we can just return.
      return
    location = bq.get_query_location(
        self._get_parent_project(), self.query.get(), self.use_legacy_sql)
    _LOGGER.warning("### Labels: %s", str(self.bigquery_dataset_labels))
    bq.create_temporary_dataset(
        self._get_parent_project(),
        location,
        self.bigquery_dataset_labels,
        kms_key=self.kms_key)

  @check_accessible(['query'])
  def _execute_query(self, bq):
    query_job_name = bigquery_tools.generate_bq_job_name(
        self._job_name,
        self._source_uuid,
        bigquery_tools.BigQueryJobTypes.QUERY,
        '%s_%s' % (int(time.time()), random.randint(0, 1000)))
    job = bq._start_query_job(
        self._get_parent_project(),
        self.query.get(),
        self.use_legacy_sql,
        self.flatten_results,
        job_id=query_job_name,
        priority=self.query_priority,
        kms_key=self.kms_key,
        job_labels=self._get_bq_metadata().add_additional_bq_job_labels(
            self.bigquery_job_labels))
    job_ref = job.jobReference
    bq.wait_for_bq_job(job_ref, max_retries=0)
    table_reference = bq._get_temp_table(self._get_parent_project())
    return table_reference

  def display_data(self):
    return {
        'method': self.method,
        'output_format': 'ARROW' if self.use_native_datetime else 'AVRO',
        'project': str(self.project),
        'table_reference': str(self.table_reference),
        'query': str(self.query),
        'use_legacy_sql': self.use_legacy_sql,
        'use_native_datetime': self.use_native_datetime,
        'selected_fields': str(self.selected_fields),
        'row_restriction': str(self.row_restriction),
        'launchesBigQueryJobs': DisplayDataItem(
            True, label="This Dataflow job launches bigquery jobs."),
    }

  def estimate_size(self):
    # Returns the pre-filtering size of the (temporary) table being read.
    bq = bigquery_tools.BigQueryWrapper.from_pipeline_options(
        self.pipeline_options)
    if self.table_reference is not None:
      table_ref = self.table_reference
      if (isinstance(self.table_reference, vp.ValueProvider) and
          self.table_reference.is_accessible()):
        table_ref = bigquery_tools.parse_table_reference(
            self.table_reference.get(), project=self._get_project())
      elif isinstance(self.table_reference, vp.ValueProvider):
        # Size estimation is best effort. We return None as we have
        # no access to the table that we're querying.
        return None
      return self._get_table_size(bq, table_ref)
    elif self.query is not None and self.query.is_accessible():
      query_job_name = bigquery_tools.generate_bq_job_name(
          self._job_name,
          self._source_uuid,
          bigquery_tools.BigQueryJobTypes.QUERY,
          '%s_%s' % (int(time.time()), random.randint(0, 1000)))
      job = bq._start_query_job(
          self._get_parent_project(),
          self.query.get(),
          self.use_legacy_sql,
          self.flatten_results,
          job_id=query_job_name,
          priority=self.query_priority,
          dry_run=True,
          kms_key=self.kms_key,
          job_labels=self._get_bq_metadata().add_additional_bq_job_labels(
              self.bigquery_job_labels))

      if job.statistics.totalBytesProcessed is None:
        # Some queries may not have access to `totalBytesProcessed` as a
        # result of row-level security
        # > BigQuery hides sensitive statistics on all queries against
        # > tables with row-level security.
        # See cloud.google.com/bigquery/docs/managing-row-level-security
        # and cloud.google.com/bigquery/docs/best-practices-row-level-security
        return None

      return int(job.statistics.totalBytesProcessed)
    else:
      # Size estimation is best effort. We return None as we have
      # no access to the query that we're running.
      return None

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    if self.split_result is None:
      bq = bigquery_tools.BigQueryWrapper(
          temp_table_ref=(self.temp_table if self.temp_table else None),
          client=bigquery_tools.BigQueryWrapper._bigquery_client(
              self.pipeline_options))

      if self.query is not None:
        self._setup_temporary_dataset(bq)
        self.table_reference = self._execute_query(bq)

      requested_session = bq_storage.types.ReadSession()
      requested_session.table = 'projects/{}/datasets/{}/tables/{}'.format(
          self.table_reference.projectId,
          self.table_reference.datasetId,
          self.table_reference.tableId)
      Lineage.sources().add(
          'bigquery',
          self.table_reference.projectId,
          self.table_reference.datasetId,
          self.table_reference.tableId)

      if self.use_native_datetime:
        requested_session.data_format = bq_storage.types.DataFormat.ARROW
        requested_session.read_options\
          .arrow_serialization_options.buffer_compression = \
          bq_storage.types.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME
      else:
        requested_session.data_format = bq_storage.types.DataFormat.AVRO

      if self.selected_fields is not None:
        requested_session.read_options.selected_fields = self.selected_fields
      if self.row_restriction is not None:
        requested_session.read_options.row_restriction = self.row_restriction

      storage_client = bq_storage.BigQueryReadClient()
      stream_count = 0
      if desired_bundle_size > 0:
        table_size = self._get_table_size(bq, self.table_reference)
        stream_count = min(
            int(table_size / desired_bundle_size),
            _CustomBigQueryStorageSource.MAX_SPLIT_COUNT)
      stream_count = max(
          stream_count, _CustomBigQueryStorageSource.MIN_SPLIT_COUNT)

      parent = 'projects/{}'.format(self.table_reference.projectId)
      read_session = storage_client.create_read_session(
          parent=parent,
          read_session=requested_session,
          max_stream_count=stream_count)
      if self.use_native_datetime:
        display_schema = "Arrow Schema:" + str(read_session.arrow_schema)
      else:
        display_schema = "Avro Schema:" + str(read_session.avro_schema)
      _LOGGER.info(
          'Sent BigQuery Storage API CreateReadSession request: \n %s \n'
          'Received %d streams\ndata_format: %s\n'
          'estimated_total_bytes_scanned: %s\n%s.',
          requested_session,
          len(read_session.streams),
          read_session.data_format,
          read_session.estimated_total_bytes_scanned,
          display_schema)

      self.split_result = [
          _CustomBigQueryStorageStreamSource(
              stream.name, self.use_native_datetime, self.timeout)
          for stream in read_session.streams
      ]

    for source in self.split_result:
      yield SourceBundle(
          weight=1.0, source=source, start_position=None, stop_position=None)

  def get_range_tracker(self, start_position, stop_position):
    class NonePositionRangeTracker(RangeTracker):
      """A RangeTracker that always returns positions as None. Prevents the
      BigQuery Storage source from being read() before being split()."""
      def start_position(self):
        return None

      def stop_position(self):
        return None

    return NonePositionRangeTracker()

  def read(self, range_tracker):
    raise NotImplementedError(
        'BigQuery storage source must be split before being read')


class _CustomBigQueryStorageStreamSource(BoundedSource):
  """A source representing a single stream in a read session."""

  # Runner will act on this counter on scaling event, if supported
  THROTTLE_COUNTER = Metrics.counter(__name__, 'cumulativeThrottlingSeconds')

  def __init__(
      self,
      read_stream_name: str,
      use_native_datetime: Optional[bool] = True,
      timeout: Optional[float] = None):
    self.read_stream_name = read_stream_name
    self.use_native_datetime = use_native_datetime
    self.timeout = timeout

  def display_data(self):
    return {
        'output_format': 'ARROW' if self.use_native_datetime else 'AVRO',
        'read_stream': str(self.read_stream_name),
        'use_native_datetime': str(self.use_native_datetime)
    }

  def estimate_size(self):
    # The size of stream source cannot be estimate due to server-side liquid
    # sharding.
    # TODO(https://github.com/apache/beam/issues/21126): Implement progress
    # reporting.
    return None

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    # A stream source can't be split without reading from it due to
    # server-side liquid sharding. A split will simply return the current source
    # for now.
    return SourceBundle(
        weight=1.0,
        source=_CustomBigQueryStorageStreamSource(
            self.read_stream_name, self.use_native_datetime),
        start_position=None,
        stop_position=None)

  def get_range_tracker(self, start_position, stop_position):
    # TODO(https://github.com/apache/beam/issues/21127): Implement dynamic work
    # rebalancing.
    assert start_position is None
    # Defaulting to the start of the stream.
    start_position = 0
    # Since the streams are unsplittable we choose OFFSET_INFINITY as the
    # default end offset so that all data of the source gets read.
    stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    # Ensuring that all try_split() calls will be ignored by the Rangetracker.
    range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    _LOGGER.info(
        "Started BigQuery Storage API read from stream %s.",
        self.read_stream_name)
    if self.use_native_datetime:
      return self.read_arrow()
    else:
      return self.read_avro()

  @staticmethod
  def retry_delay_callback(delay):
    _LOGGER.info("retry delay: %f", delay)
    _CustomBigQueryStorageStreamSource.THROTTLE_COUNTER.inc(delay)

  def read_arrow(self):

    storage_client = bq_storage.BigQueryReadClient()
    read_rows_kwargs = {'retry_delay_callback': self.retry_delay_callback}
    if self.timeout is not None:
      read_rows_kwargs['timeout'] = self.timeout
    row_iter = iter(
        storage_client.read_rows(self.read_stream_name,
                                 **read_rows_kwargs).rows())
    row = next(row_iter, None)
    # Handling the case where the user might provide very selective filters
    # which can result in read_rows_response being empty.
    if row is None:
      return iter([])

    while row is not None:
      py_row = dict(map(lambda item: (item[0], item[1].as_py()), row.items()))
      row = next(row_iter, None)
      yield py_row

  def read_avro(self):
    storage_client = bq_storage.BigQueryReadClient()
    read_rows_kwargs = {'retry_delay_callback': self.retry_delay_callback}
    if self.timeout is not None:
      read_rows_kwargs['timeout'] = self.timeout
    read_rows_iterator = iter(
        storage_client.read_rows(self.read_stream_name, **read_rows_kwargs))
    # Handling the case where the user might provide very selective filters
    # which can result in read_rows_response being empty.
    first_read_rows_response = next(read_rows_iterator, None)
    if first_read_rows_response is None:
      return iter([])

    row_reader = _ReadReadRowsResponsesWithFastAvro(
        read_rows_iterator, first_read_rows_response)
    return iter(row_reader)


class _ReadReadRowsResponsesWithFastAvro():
  """An iterator that deserializes ReadRowsResponses using the fastavro
  library."""
  def __init__(self, read_rows_iterator, read_rows_response):
    self.read_rows_iterator = read_rows_iterator
    self.read_rows_response = read_rows_response
    self.avro_schema = fastavro.parse_schema(
        json.loads(self.read_rows_response.avro_schema.schema))
    self.bytes_reader = io.BytesIO(
        self.read_rows_response.avro_rows.serialized_binary_rows)

  def __iter__(self):
    return self

  def __next__(self):
    try:
      return fastavro.schemaless_reader(self.bytes_reader, self.avro_schema)
    except (StopIteration, EOFError):
      self.read_rows_response = next(self.read_rows_iterator, None)
      if self.read_rows_response is not None:
        self.bytes_reader = io.BytesIO(
            self.read_rows_response.avro_rows.serialized_binary_rows)
        return fastavro.schemaless_reader(self.bytes_reader, self.avro_schema)
      else:
        raise StopIteration


@deprecated(since='2.11.0', current="WriteToBigQuery")
def BigQuerySink(*args, validate=False, **kwargs):
  """A deprecated alias for WriteToBigQuery."""
  warnings.warn(
      "Native sinks no longer implemented; "
      "falling back to standard Beam sink.")
  return WriteToBigQuery(*args, validate=validate, **kwargs)


_KNOWN_TABLES = set()


class BigQueryWriteFn(DoFn):
  """A ``DoFn`` that streams writes to BigQuery once the table is created."""

  DEFAULT_MAX_BUFFERED_ROWS = 2000
  DEFAULT_MAX_BATCH_SIZE = 500

  FAILED_ROWS = 'FailedRows'
  FAILED_ROWS_WITH_ERRORS = 'FailedRowsWithErrors'
  STREAMING_API_LOGGING_FREQUENCY_SEC = 300

  def __init__(
      self,
      batch_size,
      schema=None,
      create_disposition=None,
      write_disposition=None,
      kms_key=None,
      test_client=None,
      max_buffered_rows=None,
      retry_strategy=None,
      additional_bq_parameters=None,
      ignore_insert_ids=False,
      with_batched_input=False,
      ignore_unknown_columns=False,
      max_retries=MAX_INSERT_RETRIES,
      max_insert_payload_size=MAX_INSERT_PAYLOAD_SIZE):
    """Initialize a WriteToBigQuery transform.

    Args:
      batch_size: Number of rows to be written to BQ per streaming API insert.
      schema: The schema to be used if the BigQuery table to write has to be
        created. This can be either specified as a 'bigquery.TableSchema' object
        or a single string  of the form 'field1:type1,field2:type2,field3:type3'
        that defines a comma separated list of fields. Here 'type' should
        specify the BigQuery type of the field. Single string based schemas do
        not support nested fields, repeated fields, or specifying a BigQuery
        mode for fields (mode will always be set to 'NULLABLE').
      create_disposition: A string describing what happens if the table does not
        exist. Possible values are:
        - BigQueryDisposition.CREATE_IF_NEEDED: create if does not exist.
        - BigQueryDisposition.CREATE_NEVER: fail the write if does not exist.
      write_disposition: A string describing what happens if the table has
        already some data. Possible values are:
        -  BigQueryDisposition.WRITE_TRUNCATE: delete existing rows.
        -  BigQueryDisposition.WRITE_APPEND: add to existing rows.
        -  BigQueryDisposition.WRITE_EMPTY: fail the write if table not empty.
        For streaming pipelines WriteTruncate can not be used.
      kms_key: Optional Cloud KMS key name for use when creating new tables.
      test_client: Override the default bigquery client used for testing.

      max_buffered_rows: The maximum number of rows that are allowed to stay
        buffered when running dynamic destinations. When destinations are
        dynamic, it is important to keep caches small even when a single
        batch has not been completely filled up.
      retry_strategy: The strategy to use when retrying streaming inserts
        into BigQuery. Options are shown in bigquery_tools.RetryStrategy attrs.
      additional_bq_parameters (dict, callable): A set of additional parameters
        to be passed when creating a BigQuery table. These are passed when
        triggering a load job for FILE_LOADS, and when creating a new table for
        STREAMING_INSERTS.
      ignore_insert_ids: When using the STREAMING_INSERTS method to write data
        to BigQuery, `insert_ids` are a feature of BigQuery that support
        deduplication of events. If your use case is not sensitive to
        duplication of data inserted to BigQuery, set `ignore_insert_ids`
        to True to increase the throughput for BQ writing. See:
        https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication
      with_batched_input: Whether the input has already been batched per
        destination. If not, perform best-effort batching per destination within
        a bundle.
      ignore_unknown_columns: Accept rows that contain values that do not match
        the schema. The unknown values are ignored. Default is False,
        which treats unknown values as errors. See reference:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
      max_retries: The number of times that we will retry inserting a group of
        rows into BigQuery. By default, we retry 10000 times with exponential
        backoffs (effectively retry forever).
      max_insert_payload_size: The maximum byte size for a BigQuery legacy
        streaming insert payload.
    """
    self.schema = schema
    self.test_client = test_client
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    if write_disposition in (BigQueryDisposition.WRITE_EMPTY,
                             BigQueryDisposition.WRITE_TRUNCATE):
      raise ValueError(
          'Write disposition %s is not supported for'
          ' streaming inserts to BigQuery' % write_disposition)
    self._rows_buffer = []
    self._reset_rows_buffer()

    self._total_buffered_rows = 0
    self.kms_key = kms_key
    self._max_batch_size = batch_size or BigQueryWriteFn.DEFAULT_MAX_BATCH_SIZE
    self._max_buffered_rows = (
        max_buffered_rows or BigQueryWriteFn.DEFAULT_MAX_BUFFERED_ROWS)
    self._retry_strategy = retry_strategy or RetryStrategy.RETRY_ALWAYS
    self.ignore_insert_ids = ignore_insert_ids
    self.with_batched_input = with_batched_input

    self.additional_bq_parameters = additional_bq_parameters or {}

    # accumulate the total time spent in exponential backoff
    self._throttled_secs = Metrics.counter(
        BigQueryWriteFn, "cumulativeThrottlingSeconds")
    self.batch_size_metric = Metrics.distribution(self.__class__, "batch_size")
    self.batch_latency_metric = Metrics.distribution(
        self.__class__, "batch_latency_ms")
    self.failed_rows_metric = Metrics.distribution(
        self.__class__, "rows_failed_per_batch")
    self.bigquery_wrapper = None
    self.streaming_api_logging_frequency_sec = (
        BigQueryWriteFn.STREAMING_API_LOGGING_FREQUENCY_SEC)
    self.ignore_unknown_columns = ignore_unknown_columns
    self._max_retries = max_retries
    self._max_insert_payload_size = max_insert_payload_size

  def display_data(self):
    return {
        'max_batch_size': self._max_batch_size,
        'max_buffered_rows': self._max_buffered_rows,
        'retry_strategy': self._retry_strategy,
        'create_disposition': str(self.create_disposition),
        'write_disposition': str(self.write_disposition),
        'additional_bq_parameters': str(self.additional_bq_parameters),
        'ignore_insert_ids': str(self.ignore_insert_ids),
        'ignore_unknown_columns': str(self.ignore_unknown_columns)
    }

  def _reset_rows_buffer(self):
    self._rows_buffer = collections.defaultdict(lambda: [])
    self._destination_buffer_byte_size = collections.defaultdict(lambda: 0)

  @staticmethod
  def get_table_schema(schema):
    """Transform the table schema into a bigquery.TableSchema instance.

    Args:
      schema: The schema to be used if the BigQuery table to write has to be
        created. This is a dictionary object created in the WriteToBigQuery
        transform.
    Returns:
      table_schema: The schema to be used if the BigQuery table to write has
         to be created but in the bigquery.TableSchema format.
    """
    if schema is None:
      return schema
    elif isinstance(schema, str):
      return bigquery_tools.parse_table_schema_from_json(schema)
    elif isinstance(schema, dict):
      return bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

  def start_bundle(self):
    self._reset_rows_buffer()

    if not self.bigquery_wrapper:
      self.bigquery_wrapper = bigquery_tools.BigQueryWrapper(
          client=self.test_client)

    (
        bigquery_tools.BigQueryWrapper.HISTOGRAM_METRIC_LOGGER.
        minimum_logging_frequency_msec
    ) = self.streaming_api_logging_frequency_sec * 1000

    self._backoff_calculator = iter(
        retry.FuzzedExponentialIntervals(
            initial_delay_secs=0.2,
            num_retries=self._max_retries,
            max_delay_secs=1500))

  def _create_table_if_needed(self, table_reference, schema=None):
    str_table_reference = '%s:%s.%s' % (
        table_reference.projectId,
        table_reference.datasetId,
        table_reference.tableId)
    if str_table_reference in _KNOWN_TABLES:
      return

    if self.create_disposition == BigQueryDisposition.CREATE_NEVER:
      # If we never want to create the table, we assume it already exists,
      # and avoid the get-or-create step.
      return

    _LOGGER.debug(
        'Creating or getting table %s with schema %s.', table_reference, schema)

    table_schema = self.get_table_schema(schema)

    if table_reference.projectId is None:
      table_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')
    self.bigquery_wrapper.get_or_create_table(
        table_reference.projectId,
        table_reference.datasetId,
        table_reference.tableId,
        table_schema,
        self.create_disposition,
        self.write_disposition,
        additional_create_parameters=self.additional_bq_parameters)
    _KNOWN_TABLES.add(str_table_reference)

  def _check_row_size(self, row_and_insert_id) -> Tuple[int, Optional[str]]:
    """Returns error string when the row estimated size is too big"""
    row_byte_size = get_deep_size(row_and_insert_id)

    # Check if individual row exceeds size limit
    if row_byte_size >= self._max_insert_payload_size:
      row_mb_size = row_byte_size / 1_000_000
      max_mb_size = self._max_insert_payload_size / 1_000_000
      return (
          row_byte_size,
          (
              f"Received row with size {row_mb_size}MB that exceeds "
              f"the maximum insert payload size set ({max_mb_size}MB)."))
    return (row_byte_size, None)

  def process(
      self, element, window_value=DoFn.WindowedValueParam, *schema_side_inputs):
    destination = bigquery_tools.get_hashable_destination(element[0])

    if callable(self.schema):
      schema = self.schema(destination, *schema_side_inputs)
    elif isinstance(self.schema, vp.ValueProvider):
      schema = self.schema.get()
    else:
      schema = self.schema

    self._create_table_if_needed(
        bigquery_tools.parse_table_reference(destination), schema)

    if not self.with_batched_input:
      row_and_insert_id = element[1]
      row_byte_size, row_too_big_error = self._check_row_size(row_and_insert_id)

      # send large rows that exceed BigQuery insert limits to DLQ
      if row_too_big_error is not None:
        return [
            pvalue.TaggedOutput(
                BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS,
                window_value.with_value(
                    (destination, row_and_insert_id[0], row_too_big_error))),
            pvalue.TaggedOutput(
                BigQueryWriteFn.FAILED_ROWS,
                window_value.with_value((destination, row_and_insert_id[0])))
        ]

      # Flush current batch first if adding this row will exceed our limits
      # limits: byte size; number of rows
      if ((self._destination_buffer_byte_size[destination] + row_byte_size
           > self._max_insert_payload_size) or
          len(self._rows_buffer[destination]) >= self._max_batch_size):
        flushed_batch = self._flush_batch(destination)
        # After flushing our existing batch, we now buffer the current row
        # for the next flush
        self._rows_buffer[destination].append((row_and_insert_id, window_value))
        self._destination_buffer_byte_size[destination] = row_byte_size
        return flushed_batch

      self._rows_buffer[destination].append((row_and_insert_id, window_value))
      self._destination_buffer_byte_size[destination] += row_byte_size
      self._total_buffered_rows += 1
      if self._total_buffered_rows >= self._max_buffered_rows:
        return self._flush_all_batches()
    else:
      # The input is already batched per destination
      # but we verify the payload size.
      # The batch might be split into smaller batches
      batched_rows = element[1]
      failed_outputs = []
      current_batch = []
      current_batch_size = 0

      for row in batched_rows:
        row_byte_size, row_too_big_error = self._check_row_size(row)
        # Check if individual row exceeds size limit
        if row_too_big_error is not None:
          failed_outputs.extend([
              pvalue.TaggedOutput(
                  BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS,
                  window_value.with_value(
                      (destination, row[0], row_too_big_error))),
              pvalue.TaggedOutput(
                  BigQueryWriteFn.FAILED_ROWS,
                  window_value.with_value((destination, row[0])))
          ])
          continue

        # Check if adding this row would exceed batch size limit
        if (len(current_batch) != 0 and
            current_batch_size + row_byte_size > self._max_insert_payload_size):

          self._rows_buffer[destination].extend(
              ((batch_row, window_value) for batch_row in current_batch))
          failed_outputs.extend(self._flush_batch(destination))

          # Start new batch with current row
          current_batch = [row]
          current_batch_size = row_byte_size
        else:
          current_batch.append(row)
          current_batch_size += row_byte_size

      if current_batch:
        for batch_row in current_batch:
          self._rows_buffer[destination].append((batch_row, window_value))
        failed_outputs.extend(self._flush_batch(destination))

      return failed_outputs

  def finish_bundle(self):
    bigquery_tools.BigQueryWrapper.HISTOGRAM_METRIC_LOGGER.log_metrics(
        reset_after_logging=True)
    return self._flush_all_batches()

  def _flush_all_batches(self):
    _LOGGER.debug(
        'Attempting to flush to all destinations. Total buffered: %s',
        self._total_buffered_rows)

    return itertools.chain(
        *[
            self._flush_batch(destination)
            for destination in list(self._rows_buffer.keys())
            if self._rows_buffer[destination]
        ])

  def _flush_batch(self, destination):

    # Flush the current batch of rows to BigQuery.
    rows_and_insert_ids_with_windows = self._rows_buffer[destination]
    table_reference = bigquery_tools.parse_table_reference(destination)
    if table_reference.projectId is None:
      table_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    _LOGGER.debug(
        'Flushing data to %s. Total %s rows.',
        destination,
        len(rows_and_insert_ids_with_windows))
    self.batch_size_metric.update(len(rows_and_insert_ids_with_windows))

    rows_and_insert_ids, window_values = zip(*rows_and_insert_ids_with_windows)
    rows = [r[0] for r in rows_and_insert_ids]
    if self.ignore_insert_ids:
      insert_ids = [None for r in rows_and_insert_ids]
    else:
      insert_ids = [r[1] for r in rows_and_insert_ids]

    while True:
      start = time.time()
      passed, errors = self.bigquery_wrapper.insert_rows(
          project_id=table_reference.projectId,
          dataset_id=table_reference.datasetId,
          table_id=table_reference.tableId,
          rows=rows,
          insert_ids=insert_ids,
          skip_invalid_rows=True,
          ignore_unknown_values=self.ignore_unknown_columns)
      self.batch_latency_metric.update((time.time() - start) * 1000)

      failed_rows = [(
          rows[entry['index']], entry["errors"], window_values[entry['index']])
                     for entry in errors]
      failed_insert_ids = [insert_ids[entry['index']] for entry in errors]
      retry_backoff = next(self._backoff_calculator, None)

      # If retry_backoff is None, then we will not retry and must log.
      should_retry = any(
          RetryStrategy.should_retry(
              self._retry_strategy, entry['errors'][0]['reason'])
          for entry in errors) and retry_backoff is not None

      if not passed:
        self.failed_rows_metric.update(len(failed_rows))
        message = (
            'There were errors inserting to BigQuery. Will{} retry. '
            'Errors were {}'.format(("" if should_retry else " not"), errors))

        # The log level is:
        # - WARNING when should_retry is true, else ERROR.

        if (should_retry and
            self._retry_strategy in [RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
                                     RetryStrategy.RETRY_ALWAYS]):
          log_level = logging.WARN
        else:
          log_level = logging.ERROR

        _LOGGER.log(log_level, message)

      if not should_retry:
        break
      else:
        _LOGGER.info(
            'Sleeping %s seconds before retrying insertion.', retry_backoff)
        time.sleep(retry_backoff)
        # We can now safely discard all information about successful rows and
        # just focus on the failed ones
        rows = [fr[0] for fr in failed_rows]
        window_values = [fr[2] for fr in failed_rows]
        insert_ids = failed_insert_ids
        self._throttled_secs.inc(retry_backoff)

    self._total_buffered_rows -= len(self._rows_buffer[destination])
    del self._rows_buffer[destination]
    if destination in self._destination_buffer_byte_size:
      del self._destination_buffer_byte_size[destination]

    return itertools.chain(
        [
            pvalue.TaggedOutput(
                BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS,
                w.with_value((destination, row, err)))
            for row, err, w in failed_rows
        ],
        [
            pvalue.TaggedOutput(
                BigQueryWriteFn.FAILED_ROWS, w.with_value((destination, row)))
            for row, unused_err, w in failed_rows
        ])


# The number of shards per destination when writing via streaming inserts.
DEFAULT_SHARDS_PER_DESTINATION = 500
# The max duration a batch of elements is allowed to be buffered before being
# flushed to BigQuery.
DEFAULT_BATCH_BUFFERING_DURATION_LIMIT_SEC = 0.2


class _StreamToBigQuery(PTransform):
  def __init__(
      self,
      table_reference,
      table_side_inputs,
      schema_side_inputs,
      schema,
      batch_size,
      triggering_frequency,
      create_disposition,
      write_disposition,
      kms_key,
      retry_strategy,
      additional_bq_parameters,
      ignore_insert_ids,
      ignore_unknown_columns,
      with_auto_sharding,
      num_streaming_keys=DEFAULT_SHARDS_PER_DESTINATION,
      test_client=None,
      max_retries=MAX_INSERT_RETRIES,
      max_insert_payload_size=MAX_INSERT_PAYLOAD_SIZE):
    self.table_reference = table_reference
    self.table_side_inputs = table_side_inputs
    self.schema_side_inputs = schema_side_inputs
    self.schema = schema
    self.batch_size = batch_size
    self.triggering_frequency = triggering_frequency
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self.kms_key = kms_key
    self.retry_strategy = retry_strategy
    self.test_client = test_client
    self.additional_bq_parameters = additional_bq_parameters
    self.ignore_insert_ids = ignore_insert_ids
    self.ignore_unknown_columns = ignore_unknown_columns
    self.with_auto_sharding = with_auto_sharding
    self._num_streaming_keys = num_streaming_keys
    self._max_retries = max_retries
    self._max_insert_payload_size = max_insert_payload_size

  class InsertIdPrefixFn(DoFn):
    def start_bundle(self):
      self.prefix = str(uuid.uuid4())
      self._row_count = 0

    def process(self, element):
      key = element[0]
      value = element[1]
      insert_id = '%s-%s' % (self.prefix, self._row_count)
      self._row_count += 1
      yield (key, (value, insert_id))

  def expand(self, input):
    bigquery_write_fn = BigQueryWriteFn(
        schema=self.schema,
        batch_size=self.batch_size,
        create_disposition=self.create_disposition,
        write_disposition=self.write_disposition,
        kms_key=self.kms_key,
        retry_strategy=self.retry_strategy,
        test_client=self.test_client,
        additional_bq_parameters=self.additional_bq_parameters,
        ignore_insert_ids=self.ignore_insert_ids,
        ignore_unknown_columns=self.ignore_unknown_columns,
        with_batched_input=self.with_auto_sharding,
        max_retries=self._max_retries,
        max_insert_payload_size=self._max_insert_payload_size)

    def _add_random_shard(element):
      key = element[0]
      value = element[1]
      return ((key, random.randint(0, self._num_streaming_keys)), value)

    def _restore_table_ref(sharded_table_ref_elems_kv):
      sharded_table_ref = sharded_table_ref_elems_kv[0]
      table_ref = bigquery_tools.parse_table_reference(sharded_table_ref)
      return (table_ref, sharded_table_ref_elems_kv[1])

    tagged_data = (
        input
        | 'AppendDestination' >> beam.ParDo(
            bigquery_tools.AppendDestinationsFn(self.table_reference),
            *self.table_side_inputs)
        | 'AddInsertIds' >> beam.ParDo(_StreamToBigQuery.InsertIdPrefixFn())
        |
        'ToHashableTableRef' >> beam.Map(bigquery_tools.to_hashable_table_ref))

    if not self.with_auto_sharding:
      tagged_data = (
          tagged_data
          | 'WithFixedSharding' >> beam.Map(_add_random_shard)
          | 'CommitInsertIds' >> ReshufflePerKey()
          | 'DropShard' >> beam.Map(lambda kv: (kv[0][0], kv[1])))
    else:
      # Auto-sharding is achieved via GroupIntoBatches.WithShardedKey
      # transform which shards, groups and at the same time batches the table
      # rows to be inserted to BigQuery.

      # Firstly the keys of tagged_data (table references) are converted to a
      # hashable format. This is needed to work with the keyed states used by
      # GroupIntoBatches. After grouping and batching is done, original table
      # references are restored.
      tagged_data = (
          tagged_data
          | 'WithAutoSharding' >> beam.GroupIntoBatches.WithShardedKey(
              (self.batch_size or BigQueryWriteFn.DEFAULT_MAX_BUFFERED_ROWS),
              self.triggering_frequency or
              DEFAULT_BATCH_BUFFERING_DURATION_LIMIT_SEC)
          | 'DropShard' >> beam.Map(lambda kv: (kv[0].key, kv[1])))

    return (
        tagged_data
        | 'FromHashableTableRef' >> beam.Map(_restore_table_ref)
        | 'StreamInsertRows' >> ParDo(
            bigquery_write_fn, *self.schema_side_inputs).with_outputs(
                BigQueryWriteFn.FAILED_ROWS,
                BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS,
                main='main'))


# Flag to be passed to WriteToBigQuery to force schema autodetection
SCHEMA_AUTODETECT = 'SCHEMA_AUTODETECT'


class WriteToBigQuery(PTransform):
  """Write data to BigQuery.

  This transform receives a PCollection of elements to be inserted into BigQuery
  tables. The elements would come in as Python dictionaries, or as `TableRow`
  instances.
  """
  class Method(object):
    DEFAULT = 'DEFAULT'
    STREAMING_INSERTS = 'STREAMING_INSERTS'
    FILE_LOADS = 'FILE_LOADS'
    STORAGE_WRITE_API = 'STORAGE_WRITE_API'

  def __init__(
      self,
      table,
      dataset=None,
      project=None,
      schema=None,
      create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=BigQueryDisposition.WRITE_APPEND,
      kms_key=None,
      batch_size=None,
      max_file_size=None,
      max_partition_size=None,
      max_files_per_bundle=None,
      test_client=None,
      custom_gcs_temp_location=None,
      method=None,
      insert_retry_strategy=None,
      additional_bq_parameters=None,
      table_side_inputs=None,
      schema_side_inputs=None,
      triggering_frequency=None,
      use_at_least_once=False,
      validate=True,
      temp_file_format=None,
      ignore_insert_ids=False,
      # TODO(https://github.com/apache/beam/issues/20712): Switch the default
      # when the feature is mature.
      with_auto_sharding=False,
      num_storage_api_streams=0,
      ignore_unknown_columns=False,
      load_job_project_id=None,
      max_retries=MAX_INSERT_RETRIES,
      max_insert_payload_size=MAX_INSERT_PAYLOAD_SIZE,
      num_streaming_keys=DEFAULT_SHARDS_PER_DESTINATION,
      use_cdc_writes: bool = False,
      primary_key: List[str] = None,
      expansion_service=None,
      big_lake_configuration=None):
    """Initialize a WriteToBigQuery transform.

    Args:
      table (str, callable, ValueProvider): The ID of the table, or a callable
         that returns it. The ID must contain only letters ``a-z``, ``A-Z``,
         numbers ``0-9``, or connectors ``-_``. If dataset argument is
         :data:`None` then the table argument must contain the entire table
         reference specified as: ``'DATASET.TABLE'``
         or ``'PROJECT:DATASET.TABLE'``. If it's a callable, it must receive one
         argument representing an element to be written to BigQuery, and return
         a TableReference, or a string table name as specified above.
      dataset (str): The ID of the dataset containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      project (str): The ID of the project containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      schema (str,dict,ValueProvider,callable): The schema to be used if the
        BigQuery table to write has to be created. This can be either specified
        as a :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema`. or a `ValueProvider` that has a JSON string,
        or a python dictionary, or the string or dictionary itself,
        object or a single string  of the form
        ``'field1:type1,field2:type2,field3:type3'`` that defines a comma
        separated list of fields. Here ``'type'`` should specify the BigQuery
        type of the field. Single string based schemas do not support nested
        fields, repeated fields, or specifying a BigQuery mode for fields
        (mode will always be set to ``'NULLABLE'``).
        If a callable, then it should receive a destination (in the form of
        a str, and return a str, dict or TableSchema).
        One may also pass ``SCHEMA_AUTODETECT`` here when using JSON-based
        file loads, and BigQuery will try to infer the schema for the files
        that are being loaded.
      create_disposition (BigQueryDisposition): A string describing what
        happens if the table does not exist. Possible values are:

        * :attr:`BigQueryDisposition.CREATE_IF_NEEDED`: create if does not
          exist.
        * :attr:`BigQueryDisposition.CREATE_NEVER`: fail the write if does not
          exist.

      write_disposition (BigQueryDisposition): A string describing what happens
        if the table has already some data. Possible values are:

        * :attr:`BigQueryDisposition.WRITE_TRUNCATE`: delete existing rows.
        * :attr:`BigQueryDisposition.WRITE_APPEND`: add to existing rows.
        * :attr:`BigQueryDisposition.WRITE_EMPTY`: fail the write if table not
          empty.

        For streaming pipelines WriteTruncate can not be used.
      kms_key (str): Optional Cloud KMS key name for use when creating new
        tables.
      batch_size (int): Number of rows to be written to BQ per streaming API
        insert. The default is 500.
      test_client: Override the default bigquery client used for testing.
      max_file_size (int): The maximum size for a file to be written and then
        loaded into BigQuery. The default value is 4TB, which is 80% of the
        limit of 5TB for BigQuery to load any file.
      max_partition_size (int): Maximum byte size for each load job to
        BigQuery. Defaults to 15TB. Applicable to FILE_LOADS only.
      max_files_per_bundle(int): The maximum number of files to be concurrently
        written by a worker. The default here is 20. Larger values will allow
        writing to multiple destinations without having to reshard - but they
        increase the memory burden on the workers.
      custom_gcs_temp_location (str): A GCS location to store files to be used
        for file loads into BigQuery. By default, this will use the pipeline's
        temp_location, but for pipelines whose temp_location is not appropriate
        for BQ File Loads, users should pass a specific one.
      method: The method to use to write to BigQuery. It may be
        STREAMING_INSERTS, FILE_LOADS, STORAGE_WRITE_API or DEFAULT. An
        introduction on loading data to BigQuery:
        https://cloud.google.com/bigquery/docs/loading-data.
        DEFAULT will use STREAMING_INSERTS on Streaming pipelines and
        FILE_LOADS on Batch pipelines.
        Note: FILE_LOADS currently does not support BigQuery's JSON data type:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#json_type">
      insert_retry_strategy: The strategy to use when retrying streaming inserts
        into BigQuery. Options are shown in bigquery_tools.RetryStrategy attrs.
        Default is to retry always. This means that whenever there are rows
        that fail to be inserted to BigQuery, they will be retried indefinitely.
        Other retry strategy settings will produce a deadletter PCollection
        as output. Appropriate values are:

        * `RetryStrategy.RETRY_ALWAYS`: retry all rows if
          there are any kind of errors. Note that this will hold your pipeline
          back if there are errors until you cancel or update it.
        * `RetryStrategy.RETRY_NEVER`: rows with errors
          will not be retried. Instead they will be output to a dead letter
          queue under the `'FailedRows'` tag.
        * `RetryStrategy.RETRY_ON_TRANSIENT_ERROR`: retry
          rows with transient errors (e.g. timeouts). Rows with permanent errors
          will be output to dead letter queue under `'FailedRows'` tag.

      additional_bq_parameters (dict, callable): Additional parameters to pass
        to BQ when creating / loading data into a table. If a callable, it
        should be a function that receives a table reference indicating
        the destination and returns a dictionary.
        These can be 'timePartitioning', 'clustering', etc. They are passed
        directly to the job load configuration. See
        https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload
      table_side_inputs (tuple): A tuple with ``AsSideInput`` PCollections to be
        passed to the table callable (if one is provided).
      schema_side_inputs: A tuple with ``AsSideInput`` PCollections to be
        passed to the schema callable (if one is provided).
      triggering_frequency (float):
        When method is FILE_LOADS:
        Value will be converted to int. Every triggering_frequency seconds, a
        BigQuery load job will be triggered for all the data written since the
        last load job. BigQuery has limits on how many load jobs can be
        triggered per day, so be careful not to set this duration too low, or
        you may exceed daily quota. Often this is set to 5 or 10 minutes to
        ensure that the project stays well under the BigQuery quota. See
        https://cloud.google.com/bigquery/quota-policy for more information
        about BigQuery quotas.

        When method is STREAMING_INSERTS and with_auto_sharding=True:
        A streaming inserts batch will be submitted at least every
        triggering_frequency seconds when data is waiting. The batch can be
        sent earlier if it reaches the maximum batch size set by batch_size.
        Default value is 0.2 seconds.

        When method is STORAGE_WRITE_API:
        A stream of rows will be committed every triggering_frequency seconds.
        By default, this will be 5 seconds to ensure exactly-once semantics.
      use_at_least_once: Intended only for STORAGE_WRITE_API. When True, will
        use at-least-once semantics. This is cheaper and provides lower
        latency, but will potentially duplicate records.
      validate: Indicates whether to perform validation checks on
        inputs. This parameter is primarily used for testing.
      temp_file_format: The format to use for file loads into BigQuery. The
        options are NEWLINE_DELIMITED_JSON or AVRO, with NEWLINE_DELIMITED_JSON
        being used by default. For advantages and limitations of the two
        formats, see
        https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
        and
        https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json.
      ignore_insert_ids: When using the STREAMING_INSERTS method to write data
        to BigQuery, `insert_ids` are a feature of BigQuery that support
        deduplication of events. If your use case is not sensitive to
        duplication of data inserted to BigQuery, set `ignore_insert_ids`
        to True to increase the throughput for BQ writing. See:
        https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication
      with_auto_sharding: Experimental. If true, enables using a dynamically
        determined number of shards to write to BigQuery. This can be used for
        all of FILE_LOADS, STREAMING_INSERTS, and STORAGE_WRITE_API. Only
        applicable to unbounded input.
      num_storage_api_streams: Specifies the number of write streams that the
        Storage API sink will use. This parameter is only applicable when
        writing unbounded data.
      ignore_unknown_columns: Accept rows that contain values that do not match
        the schema. The unknown values are ignored. Default is False,
        which treats unknown values as errors. This option is only valid for
        method=STREAMING_INSERTS. See reference:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
      load_job_project_id: Specifies an alternate GCP project id to use for
        billingBatch File Loads. By default, the project id of the table is
        used.
      num_streaming_keys: The number of shards per destination when writing via
        streaming inserts.
      expansion_service: The address (host:port) of the expansion service.
        If no expansion service is provided, will attempt to run the default
        GCP expansion service. Used for STORAGE_WRITE_API method.
      max_retries: The number of times that we will retry inserting a group of
        rows into BigQuery. By default, we retry 10000 times with exponential
        backoffs (effectively retry forever).
      max_insert_payload_size: The maximum byte size for a BigQuery legacy
        streaming insert payload.
      use_cdc_writes: Configure the usage of CDC writes on BigQuery.
        The argument can be used by passing True and the Beam Rows will be
        sent as they are to the BigQuery sink which expects a 'record'
        and 'row_mutation_info' properties.
        Used for STORAGE_WRITE_API, working on 'at least once' mode.
      primary_key: When using CDC write on BigQuery and
        CREATE_IF_NEEDED mode for the underlying tables a list of column names
        is required to be configured as the primary key. Used for
        STORAGE_WRITE_API, working on 'at least once' mode.
    """
    self._table = table
    self._dataset = dataset
    self._project = project
    self.table_reference = bigquery_tools.parse_table_reference(
        table, dataset, project)
    self.create_disposition = BigQueryDisposition.validate_create(
        create_disposition)
    self.write_disposition = BigQueryDisposition.validate_write(
        write_disposition)
    if schema == SCHEMA_AUTODETECT:
      self.schema = schema
    else:
      self.schema = bigquery_tools.get_dict_table_schema(schema)
    self.batch_size = batch_size
    self.kms_key = kms_key
    self.test_client = test_client

    # TODO(pabloem): Consider handling ValueProvider for this location.
    self.custom_gcs_temp_location = custom_gcs_temp_location
    self.max_file_size = max_file_size
    self.max_partition_size = max_partition_size
    self.max_files_per_bundle = max_files_per_bundle
    self.method = method or WriteToBigQuery.Method.DEFAULT
    self.triggering_frequency = triggering_frequency
    self.use_at_least_once = use_at_least_once
    self.expansion_service = expansion_service
    self.with_auto_sharding = with_auto_sharding
    self._num_storage_api_streams = num_storage_api_streams
    self.insert_retry_strategy = insert_retry_strategy
    self._validate = validate
    self._temp_file_format = temp_file_format or bigquery_tools.FileFormat.JSON

    self.additional_bq_parameters = additional_bq_parameters or {}
    self.table_side_inputs = table_side_inputs or ()
    self.schema_side_inputs = schema_side_inputs or ()
    self._ignore_insert_ids = ignore_insert_ids
    self._ignore_unknown_columns = ignore_unknown_columns
    self.load_job_project_id = load_job_project_id
    self._max_retries = max_retries
    self._max_insert_payload_size = max_insert_payload_size
    self._num_streaming_keys = num_streaming_keys
    self._use_cdc_writes = use_cdc_writes
    self._primary_key = primary_key
    self._big_lake_configuration = big_lake_configuration

  # Dict/schema methods were moved to bigquery_tools, but keep references
  # here for backward compatibility.
  get_table_schema_from_string = \
      staticmethod(bigquery_tools.get_table_schema_from_string)
  table_schema_to_dict = staticmethod(bigquery_tools.table_schema_to_dict)
  get_dict_table_schema = staticmethod(bigquery_tools.get_dict_table_schema)

  def _compute_method(self, experiments, is_streaming_pipeline):
    # If the new BQ sink is not activated for experiment flags, then we use
    # streaming inserts by default (it gets overridden in dataflow_runner.py).
    if self.method == self.Method.DEFAULT and is_streaming_pipeline:
      return self.Method.STREAMING_INSERTS
    elif self.method == self.Method.DEFAULT and not is_streaming_pipeline:
      return self.Method.FILE_LOADS
    else:
      return self.method

  def expand(self, pcoll):
    p = pcoll.pipeline

    if (isinstance(self.table_reference, TableReference) and
        self.table_reference.projectId is None):
      self.table_reference.projectId = pcoll.pipeline.options.view_as(
          GoogleCloudOptions).project

    # TODO(pabloem): Use a different method to determine if streaming or batch.
    is_streaming_pipeline = p.options.view_as(StandardOptions).streaming

    if not is_streaming_pipeline and self.with_auto_sharding:
      raise ValueError(
          'with_auto_sharding is not applicable to batch pipelines.')

    experiments = p.options.view_as(DebugOptions).experiments or []
    method_to_use = self._compute_method(experiments, is_streaming_pipeline)

    if method_to_use == WriteToBigQuery.Method.STREAMING_INSERTS:
      if self.schema == SCHEMA_AUTODETECT:
        raise ValueError(
            'Schema auto-detection is not supported for streaming '
            'inserts into BigQuery. Only for File Loads.')

      if self.triggering_frequency is not None and not self.with_auto_sharding:
        raise ValueError(
            'triggering_frequency with STREAMING_INSERTS can only be used with '
            'with_auto_sharding=True.')

      if self._max_insert_payload_size > MAX_INSERT_PAYLOAD_SIZE:
        raise ValueError(
            'max_insert_payload_size can only go up to '
            f'{MAX_INSERT_PAYLOAD_SIZE} bytes, as per BigQuery quota limits: '
            'https://cloud.google.com/bigquery/quotas#streaming_inserts.')

      if self._max_retries > MAX_INSERT_RETRIES:
        raise ValueError(
            'max_retries cannot be more than '
            f'{MAX_INSERT_RETRIES}, hence please reduce the value.')

      outputs = pcoll | _StreamToBigQuery(
          table_reference=self.table_reference,
          table_side_inputs=self.table_side_inputs,
          schema_side_inputs=self.schema_side_inputs,
          schema=self.schema,
          batch_size=self.batch_size,
          triggering_frequency=self.triggering_frequency,
          create_disposition=self.create_disposition,
          write_disposition=self.write_disposition,
          kms_key=self.kms_key,
          retry_strategy=self.insert_retry_strategy,
          additional_bq_parameters=self.additional_bq_parameters,
          ignore_insert_ids=self._ignore_insert_ids,
          ignore_unknown_columns=self._ignore_unknown_columns,
          with_auto_sharding=self.with_auto_sharding,
          test_client=self.test_client,
          max_insert_payload_size=self._max_insert_payload_size,
          max_retries=self._max_retries,
          num_streaming_keys=self._num_streaming_keys)

      return WriteResult(
          method=WriteToBigQuery.Method.STREAMING_INSERTS,
          failed_rows=outputs[BigQueryWriteFn.FAILED_ROWS],
          failed_rows_with_errors=outputs[
              BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS])

    elif method_to_use == WriteToBigQuery.Method.FILE_LOADS:
      if self._temp_file_format == bigquery_tools.FileFormat.AVRO:
        if self.schema == SCHEMA_AUTODETECT:
          raise ValueError(
              'Schema auto-detection is not supported when using Avro based '
              'file loads into BigQuery. Please specify a schema or set '
              'temp_file_format="NEWLINE_DELIMITED_JSON"')
        if self.schema is None:
          raise ValueError(
              'A schema must be provided when writing to BigQuery using '
              'Avro based file loads')

      if self.schema and type(self.schema) is dict:

        def find_in_nested_dict(schema):
          for field in schema['fields']:
            if field['type'] == 'JSON':
              logging.warning(
                  'Found JSON type in TableSchema for "File_LOADS" write '
                  'method. Make sure the TableSchema field is a parsed '
                  'JSON to ensure the read as a JSON type. Otherwise it '
                  'will read as a raw (escaped) string.')
            elif field['type'] == 'STRUCT':
              find_in_nested_dict(field)

        find_in_nested_dict(self.schema)

      from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
      # Only cast to int when a value is given.
      # We only use an int for BigQueryBatchFileLoads
      if self.triggering_frequency is not None:
        triggering_frequency = int(self.triggering_frequency)
      else:
        triggering_frequency = self.triggering_frequency
      output = pcoll | BigQueryBatchFileLoads(
          destination=self.table_reference,
          schema=self.schema,
          project=self._project,
          create_disposition=self.create_disposition,
          write_disposition=self.write_disposition,
          triggering_frequency=triggering_frequency,
          with_auto_sharding=self.with_auto_sharding,
          temp_file_format=self._temp_file_format,
          max_file_size=self.max_file_size,
          max_partition_size=self.max_partition_size,
          max_files_per_bundle=self.max_files_per_bundle,
          custom_gcs_temp_location=self.custom_gcs_temp_location,
          test_client=self.test_client,
          table_side_inputs=self.table_side_inputs,
          schema_side_inputs=self.schema_side_inputs,
          additional_bq_parameters=self.additional_bq_parameters,
          validate=self._validate,
          is_streaming_pipeline=is_streaming_pipeline,
          load_job_project_id=self.load_job_project_id)

      return WriteResult(
          method=WriteToBigQuery.Method.FILE_LOADS,
          destination_load_jobid_pairs=output[
              BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS],
          destination_file_pairs=output[
              BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS],
          destination_copy_jobid_pairs=output[
              BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS])

    elif method_to_use == WriteToBigQuery.Method.STORAGE_WRITE_API:
      return pcoll | StorageWriteToBigQuery(
          table=self.table_reference,
          schema=self.schema,
          table_side_inputs=self.table_side_inputs,
          create_disposition=self.create_disposition,
          write_disposition=self.write_disposition,
          additional_bq_parameters=self.additional_bq_parameters,
          triggering_frequency=self.triggering_frequency,
          use_at_least_once=self.use_at_least_once,
          with_auto_sharding=self.with_auto_sharding,
          num_storage_api_streams=self._num_storage_api_streams,
          use_cdc_writes=self._use_cdc_writes,
          primary_key=self._primary_key,
          big_lake_configuration=self._big_lake_configuration,
          expansion_service=self.expansion_service)
    else:
      raise ValueError(f"Unsupported method {method_to_use}")

  def display_data(self):
    res = {}
    if self.table_reference is not None and isinstance(self.table_reference,
                                                       TableReference):
      tableSpec = '{}.{}'.format(
          self.table_reference.datasetId, self.table_reference.tableId)
      if self.table_reference.projectId is not None:
        tableSpec = '{}:{}'.format(self.table_reference.projectId, tableSpec)
      res['table'] = DisplayDataItem(tableSpec, label='Table')

    res['validation'] = DisplayDataItem(
        self._validate, label="Validation Enabled")
    return res

  def to_runner_api_parameter(self, context):
    from apache_beam.internal import pickler

    # It'd be nice to name these according to their actual
    # names/positions in the orignal argument list, but such a
    # transformation is currently irreversible given how
    # remove_objects_from_args and insert_values_in_args
    # are currently implemented.
    def serialize(side_inputs):
      return {(SIDE_INPUT_PREFIX + '%s') % ix: si.to_runner_api(
                  context).SerializeToString()
              for ix, si in enumerate(side_inputs)}

    table_side_inputs = serialize(self.table_side_inputs)
    schema_side_inputs = serialize(self.schema_side_inputs)

    config = {
        'table': self._table,
        'dataset': self._dataset,
        'project': self._project,
        'schema': self.schema,
        'create_disposition': self.create_disposition,
        'write_disposition': self.write_disposition,
        'kms_key': self.kms_key,
        'batch_size': self.batch_size,
        'max_file_size': self.max_file_size,
        'max_files_per_bundle': self.max_files_per_bundle,
        'custom_gcs_temp_location': self.custom_gcs_temp_location,
        'method': self.method,
        'insert_retry_strategy': self.insert_retry_strategy,
        'additional_bq_parameters': self.additional_bq_parameters,
        'table_side_inputs': table_side_inputs,
        'schema_side_inputs': schema_side_inputs,
        'triggering_frequency': self.triggering_frequency,
        'validate': self._validate,
        'temp_file_format': self._temp_file_format,
        'ignore_insert_ids': self._ignore_insert_ids,
        'with_auto_sharding': self.with_auto_sharding,
    }
    return 'beam:transform:write_to_big_query:v0', pickler.dumps(config)

  @PTransform.register_urn('beam:transform:write_to_big_query:v0', bytes)
  def from_runner_api(unused_ptransform, payload, context):
    from apache_beam.internal import pickler
    from apache_beam.portability.api import beam_runner_api_pb2

    config = pickler.loads(payload)

    def deserialize(side_inputs):
      deserialized_side_inputs = {}
      for k, v in side_inputs.items():
        side_input = beam_runner_api_pb2.SideInput()
        side_input.ParseFromString(v)
        deserialized_side_inputs[k] = side_input

      # This is an ordered list stored as a dict (see the comments in
      # to_runner_api_parameter above).
      indexed_side_inputs = [(
          get_sideinput_index(tag),
          pvalue.AsSideInput.from_runner_api(si, context))
                             for tag, si in deserialized_side_inputs.items()]
      return [si for _, si in sorted(indexed_side_inputs)]

    config['table_side_inputs'] = deserialize(config['table_side_inputs'])
    config['schema_side_inputs'] = deserialize(config['schema_side_inputs'])

    return WriteToBigQuery(**config)


class WriteResult:
  """The result of a WriteToBigQuery transform.
  """
  def __init__(
      self,
      method: str = None,
      destination_load_jobid_pairs: PCollection[Tuple[str,
                                                      JobReference]] = None,
      destination_file_pairs: PCollection[Tuple[str, Tuple[str, int]]] = None,
      destination_copy_jobid_pairs: PCollection[Tuple[str,
                                                      JobReference]] = None,
      failed_rows: PCollection[Tuple[str, dict]] = None,
      failed_rows_with_errors: PCollection[Tuple[str, dict, list]] = None):

    self._method = method
    self._destination_load_jobid_pairs = destination_load_jobid_pairs
    self._destination_file_pairs = destination_file_pairs
    self._destination_copy_jobid_pairs = destination_copy_jobid_pairs
    self._failed_rows = failed_rows
    self._failed_rows_with_errors = failed_rows_with_errors

    from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
    self.attributes = {
        BigQueryWriteFn.FAILED_ROWS: WriteResult.failed_rows,
        BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS: WriteResult.
        failed_rows_with_errors,
        BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS: WriteResult.
        destination_load_jobid_pairs,
        BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS: WriteResult.
        destination_file_pairs,
        BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS: WriteResult.
        destination_copy_jobid_pairs,
    }

  def validate(self, valid_methods, attribute):
    if self._method not in valid_methods:
      raise AttributeError(
          f'Cannot get {attribute} because it is not produced '
          f'by the {self._method} write method. Note: only '
          f'{valid_methods} produces this attribute.')

  @property
  def destination_load_jobid_pairs(
      self) -> PCollection[Tuple[str, JobReference]]:
    """A ``FILE_LOADS`` method attribute

    Returns: A PCollection of the table destinations that were successfully
      loaded to using the batch load API, along with the load job IDs.

    Raises: AttributeError: if accessed with a write method
    besides ``FILE_LOADS``."""
    self.validate([WriteToBigQuery.Method.FILE_LOADS],
                  'DESTINATION_JOBID_PAIRS')

    return self._destination_load_jobid_pairs

  @property
  def destination_file_pairs(self) -> PCollection[Tuple[str, Tuple[str, int]]]:
    """A ``FILE_LOADS`` method attribute

    Returns: A PCollection of the table destinations along with the
      temp files used as sources to load from.

    Raises: AttributeError: if accessed with a write method
    besides ``FILE_LOADS``."""
    self.validate([WriteToBigQuery.Method.FILE_LOADS], 'DESTINATION_FILE_PAIRS')

    return self._destination_file_pairs

  @property
  def destination_copy_jobid_pairs(
      self) -> PCollection[Tuple[str, JobReference]]:
    """A ``FILE_LOADS`` method attribute

    Returns: A PCollection of the table destinations that were successfully
      copied to, along with the copy job ID.

    Raises: AttributeError: if accessed with a write method
    besides ``FILE_LOADS``."""
    self.validate([WriteToBigQuery.Method.FILE_LOADS],
                  'DESTINATION_COPY_JOBID_PAIRS')

    return self._destination_copy_jobid_pairs

  @property
  def failed_rows(self) -> PCollection[Tuple[str, dict]]:
    """A ``[STREAMING_INSERTS, STORAGE_WRITE_API]`` method attribute

    Returns: A PCollection of rows that failed when inserting to BigQuery.

    Raises: AttributeError: if accessed with a write method
    besides ``[STREAMING_INSERTS, STORAGE_WRITE_API]``."""
    self.validate([
        WriteToBigQuery.Method.STREAMING_INSERTS,
        WriteToBigQuery.Method.STORAGE_WRITE_API
    ],
                  'FAILED_ROWS')

    return self._failed_rows

  @property
  def failed_rows_with_errors(self) -> PCollection[Tuple[str, dict, list]]:
    """A ``[STREAMING_INSERTS, STORAGE_WRITE_API]`` method attribute

    Returns:
      A PCollection of rows that failed when inserting to BigQuery,
      along with their errors.

    Raises:
      AttributeError: if accessed with a write method
      besides ``[STREAMING_INSERTS, STORAGE_WRITE_API]``."""
    self.validate([
        WriteToBigQuery.Method.STREAMING_INSERTS,
        WriteToBigQuery.Method.STORAGE_WRITE_API
    ],
                  'FAILED_ROWS_WITH_ERRORS')

    return self._failed_rows_with_errors

  def __getitem__(self, key):
    if key not in self.attributes:
      raise AttributeError(
          f'Error trying to access nonexistent attribute `{key}` in write '
          'result. Please see __documentation__ for available attributes.')

    return self.attributes[key].__get__(self, WriteResult)


class StorageWriteToBigQuery(PTransform):
  """Writes data to BigQuery using Storage API.
  Supports dynamic destinations. Dynamic schemas are not supported yet.

  Experimental; no backwards compatibility guarantees.
  """
  IDENTIFIER = "beam:schematransform:org.apache.beam:bigquery_storage_write:v2"
  FAILED_ROWS = "FailedRows"
  FAILED_ROWS_WITH_ERRORS = "FailedRowsWithErrors"
  # fields for rows sent to Storage API with dynamic destinations
  DESTINATION = "destination"
  RECORD = "record"
  # field names for rows sent to Storage API for CDC functionality
  CDC_INFO = "row_mutation_info"
  CDC_MUTATION_TYPE = "mutation_type"
  CDC_SQN = "change_sequence_number"
  # magic string to tell Java that these rows are going to dynamic destinations
  DYNAMIC_DESTINATIONS = "DYNAMIC_DESTINATIONS"

  def __init__(
      self,
      table,
      table_side_inputs=None,
      schema=None,
      create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=BigQueryDisposition.WRITE_APPEND,
      additional_bq_parameters=None,
      triggering_frequency=0,
      use_at_least_once=False,
      with_auto_sharding=False,
      num_storage_api_streams=0,
      use_cdc_writes: bool = False,
      primary_key: List[str] = None,
      big_lake_configuration=None,
      expansion_service=None):
    self._table = table
    self._table_side_inputs = table_side_inputs
    self._schema = schema
    self._create_disposition = create_disposition
    self._write_disposition = write_disposition
    self.additional_bq_parameters = additional_bq_parameters
    self._triggering_frequency = triggering_frequency
    self._use_at_least_once = use_at_least_once
    self._with_auto_sharding = with_auto_sharding
    self._num_storage_api_streams = num_storage_api_streams
    self._use_cdc_writes = use_cdc_writes
    self._primary_key = primary_key
    self._big_lake_configuration = big_lake_configuration
    self._expansion_service = expansion_service or BeamJarExpansionService(
        'sdks:java:io:google-cloud-platform:expansion-service:build')

  def expand(self, input):
    if self._schema is None:
      try:
        schema = schema_from_element_type(input.element_type)
        is_rows = True
      except TypeError as exn:
        raise ValueError(
            "A schema is required in order to prepare rows "
            "for writing with STORAGE_WRITE_API.") from exn
    elif callable(self._schema):
      raise NotImplementedError(
          "Writing with dynamic schemas is not "
          "supported for this write method.")
    elif isinstance(self._schema, vp.ValueProvider):
      schema = self._schema.get()
      is_rows = False
    else:
      schema = self._schema
      is_rows = False

    table = bigquery_tools.get_hashable_destination(self._table)

    # if writing to one destination, just convert to Beam rows and send over
    if not callable(table):
      if is_rows:
        input_beam_rows = input
      else:
        input_beam_rows = (
            input
            | "Convert dict to Beam Row" >> self.ConvertToBeamRows(
                schema, False).with_output_types())

    # For dynamic destinations, we first figure out where each row is going.
    # Then we send (destination, record) rows over to Java SchemaTransform.
    # We need to do this here because there are obstacles to passing the
    # destinations function to Java
    else:
      # call function and append destination to each row
      input_rows = (
          input
          | "Append dynamic destinations" >> beam.ParDo(
              bigquery_tools.AppendDestinationsFn(table),
              *self._table_side_inputs))
      # if input type is Beam Row, just wrap everything in another Row
      if is_rows:
        input_beam_rows = (
            input_rows
            | "Wrap in Beam Row" >> beam.Map(
                lambda row: beam.Row(
                    **{
                        StorageWriteToBigQuery.DESTINATION: row[0],
                        StorageWriteToBigQuery.RECORD: row[1]
                    })).with_output_types(
                        RowTypeConstraint.from_fields([
                            (StorageWriteToBigQuery.DESTINATION, str),
                            (StorageWriteToBigQuery.RECORD, input.element_type)
                        ])))
      # otherwise, convert to Beam Rows
      else:
        input_beam_rows = (
            input_rows
            | "Convert dict to Beam Row" >> self.ConvertToBeamRows(
                schema, True).with_output_types())
      # communicate to Java that this write should use dynamic destinations
      table = StorageWriteToBigQuery.DYNAMIC_DESTINATIONS

    clustering_fields = []
    if self.additional_bq_parameters:
      if callable(self.additional_bq_parameters):
        raise NotImplementedError(
            "Currently, dynamic clustering and timepartitioning is not "
            "supported for STORAGE_WRITE_API write method.")
      clustering_fields = (
          self.additional_bq_parameters.get("clustering", {}).get("fields", []))

    output = (
        input_beam_rows
        | SchemaAwareExternalTransform(
            identifier=StorageWriteToBigQuery.IDENTIFIER,
            expansion_service=self._expansion_service,
            rearrange_based_on_discovery=True,
            table=table,
            create_disposition=self._create_disposition,
            write_disposition=self._write_disposition,
            triggering_frequency_seconds=self._triggering_frequency,
            auto_sharding=self._with_auto_sharding,
            num_streams=self._num_storage_api_streams,
            use_at_least_once_semantics=self._use_at_least_once,
            use_cdc_writes=self._use_cdc_writes,
            primary_key=self._primary_key,
            clustering_fields=clustering_fields,
            big_lake_configuration=self._big_lake_configuration,
            error_handling={
                'output': StorageWriteToBigQuery.FAILED_ROWS_WITH_ERRORS
            }))

    failed_rows_with_errors = output[
        StorageWriteToBigQuery.FAILED_ROWS_WITH_ERRORS]
    failed_rows = failed_rows_with_errors | beam.Map(
        lambda row_and_error: row_and_error[0])
    if not is_rows:
      # return back from Beam Rows to Python dict elements
      failed_rows = failed_rows | beam.Map(lambda row: row._asdict())

      failed_rows_with_errors = failed_rows_with_errors | beam.Map(
          lambda row: {
              "error_message": row.error_message, "failed_row": row.failed_row.
              _asdict()
          })

    return WriteResult(
        method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        failed_rows=failed_rows,
        failed_rows_with_errors=failed_rows_with_errors)

  class ConvertToBeamRows(PTransform):
    def __init__(self, schema, dynamic_destinations):
      self.schema = schema
      self.dynamic_destinations = dynamic_destinations

    def expand(self, input_dicts):
      if self.dynamic_destinations:
        return (
            input_dicts
            | "Convert dict to Beam Row" >> beam.Map(
                lambda row: beam.Row(
                    **{
                        StorageWriteToBigQuery.DESTINATION: row[
                            0], StorageWriteToBigQuery.RECORD: bigquery_tools.
                        beam_row_from_dict(row[1], self.schema)
                    })))
      else:
        return (
            input_dicts
            | "Convert dict to Beam Row" >> beam.Map(
                lambda row: bigquery_tools.beam_row_from_dict(row, self.schema))
        )

    def with_output_types(self):
      row_type_hints = bigquery_tools.get_beam_typehints_from_tableschema(
          self.schema)
      if self.dynamic_destinations:
        type_hint = RowTypeConstraint.from_fields([
            (StorageWriteToBigQuery.DESTINATION, str),
            (
                StorageWriteToBigQuery.RECORD,
                RowTypeConstraint.from_fields(row_type_hints))
        ])
      else:
        type_hint = RowTypeConstraint.from_fields(row_type_hints)

      return super().with_output_types(type_hint)


class ReadFromBigQuery(PTransform):
  # pylint: disable=line-too-long,W1401

  """Read data from BigQuery.

    This PTransform uses a BigQuery export job to take a snapshot of the table
    on GCS, and then reads from each produced file. File format is Avro by
    default.

  Args:
    method: The method to use to read from BigQuery. It may be EXPORT or
      DIRECT_READ. EXPORT invokes a BigQuery export request
      (https://cloud.google.com/bigquery/docs/exporting-data). DIRECT_READ reads
      directly from BigQuery storage using the BigQuery Read API
      (https://cloud.google.com/bigquery/docs/reference/storage). If
      unspecified, the default is currently EXPORT.
    timeout (float): The timeout for the read operation in seconds. This only
      impacts DIRECT_READ. If None, the client default will be used.
    use_native_datetime (bool): By default this transform exports BigQuery
      DATETIME fields as formatted strings (for example:
      2021-01-01T12:59:59). If :data:`True`, BigQuery DATETIME fields will
      be returned as native Python datetime objects. This can only be used when
      'method' is 'DIRECT_READ'.
    table (str, callable, ValueProvider): The ID of the table, or a callable
      that returns it. If dataset argument is :data:`None` then the table
      argument must contain the entire table reference specified as:
      ``'DATASET.TABLE'`` or ``'PROJECT:DATASET.TABLE'``. If it's a callable,
      it must receive one argument representing an element to be written to
      BigQuery, and return a TableReference, or a string table name as specified
      above.
    dataset (str): The ID of the dataset containing this table or
      :data:`None` if the table reference is specified entirely by the table
      argument.
    project (str): The ID of the project containing this table.
    query (str, ValueProvider): A query to be used instead of arguments
      table, dataset, and project.
    validate (bool): If :data:`True`, various checks will be done when source
      gets initialized (e.g., is table present?). This should be
      :data:`True` for most scenarios in order to catch errors as early as
      possible (pipeline construction instead of pipeline execution). It
      should be :data:`False` if the table is created during pipeline
      execution by a previous step. Set this to :data:`False`
      if the BigQuery export method is slow due to checking file existence.
    coder (~apache_beam.coders.coders.Coder): The coder for the table
      rows. If :data:`None`, then the default coder is
      _JsonToDictCoder, which will interpret every row as a JSON
      serialized dictionary.
    use_standard_sql (bool): Specifies whether to use BigQuery's standard SQL
      dialect for this query. The default value is :data:`False`.
      If set to :data:`True`, the query will use BigQuery's updated SQL
      dialect with improved standards compliance.
      This parameter is ignored for table inputs.
    flatten_results (bool): Flattens all nested and repeated fields in the
      query results. The default value is :data:`True`.
    kms_key (str): Optional Cloud KMS key name for use when creating new
      temporary tables.
    gcs_location (str, ValueProvider): The name of the Google Cloud Storage
      bucket where the extracted table should be written as a string or
      a :class:`~apache_beam.options.value_provider.ValueProvider`. If
      :data:`None`, then the temp_location parameter is used.
    bigquery_job_labels (dict): A dictionary with string labels to be passed
      to BigQuery export and query jobs created by this transform. See:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration
    use_json_exports (bool): By default, this transform works by exporting
      BigQuery data into Avro files, and reading those files. With this
      parameter, the transform will instead export to JSON files. JSON files
      are slower to read due to their larger size.
      When using JSON exports, the BigQuery types for DATE, DATETIME, TIME, and
      TIMESTAMP will be exported as strings. This behavior is consistent with
      BigQuerySource.
      When using Avro exports, these fields will be exported as native Python
      types (datetime.date, datetime.datetime, datetime.datetime,
      and datetime.datetime respectively). Avro exports are recommended.
      To learn more about BigQuery types, and Time-related type
      representations,
      see: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
      To learn more about type conversions between BigQuery and Avro, see:
      https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions
    temp_dataset (``apache_beam.io.gcp.internal.clients.bigquery.DatasetReference``):
        Temporary dataset reference to use when reading from BigQuery using a
        query. When reading using a query, BigQuery source will create a
        temporary dataset and a temporary table to store the results of the
        query. With this option, you can set an existing dataset to create the
        temporary table in. BigQuery source will create a temporary table in
        that dataset, and will remove it once it is not needed. Job needs access
        to create and delete tables within the given dataset. Dataset name
        should *not* start with the reserved prefix `beam_temp_dataset_`.
    query_priority (BigQueryQueryPriority): By default, this transform runs
      queries with BATCH priority. Use :attr:`BigQueryQueryPriority.INTERACTIVE`
      to run queries with INTERACTIVE priority. This option is ignored when
      reading from a table rather than a query. To learn more about query
      priority, see: https://cloud.google.com/bigquery/docs/running-queries
    output_type (str): By default, this source yields Python dictionaries
      (`PYTHON_DICT`). There is experimental support for producing a
      PCollection with a schema and yielding Beam Rows via the option
      `BEAM_ROW`. For more information on schemas, see
      https://beam.apache.org/documentation/programming-guide/#what-is-a-schema)
      """
  class Method(object):
    EXPORT = 'EXPORT'  #  This is currently the default.
    DIRECT_READ = 'DIRECT_READ'

  COUNTER = 0

  def __init__(
      self,
      gcs_location=None,
      method=None,
      use_native_datetime=False,
      output_type=None,
      timeout=None,
      *args,
      **kwargs):
    self.method = method or ReadFromBigQuery.Method.EXPORT
    self.use_native_datetime = use_native_datetime
    self.output_type = output_type
    self._args = args
    self._kwargs = kwargs
    if timeout is not None:
      self._kwargs['timeout'] = timeout

    if self.method == ReadFromBigQuery.Method.EXPORT \
        and self.use_native_datetime is True:
      raise TypeError(
          'The "use_native_datetime" parameter cannot be True for EXPORT.'
          ' Please set the "use_native_datetime" parameter to False *OR*'
          ' set the "method" parameter to ReadFromBigQuery.Method.DIRECT_READ.')

    if gcs_location and self.method == ReadFromBigQuery.Method.EXPORT:
      if not isinstance(gcs_location, (str, ValueProvider)):
        raise TypeError(
            '%s: gcs_location must be of type string'
            ' or ValueProvider; got %r instead' %
            (self.__class__.__name__, type(gcs_location)))
      if isinstance(gcs_location, str):
        gcs_location = StaticValueProvider(str, gcs_location)

    if self.output_type == 'BEAM_ROW' and self._kwargs.get('query',
                                                           None) is not None:
      raise ValueError(
          "Both a query and an output type of 'BEAM_ROW' were specified. "
          "'BEAM_ROW' is not currently supported with queries.")

    self.gcs_location = gcs_location
    self.bigquery_dataset_labels = {
        'type': 'bq_direct_read_' + str(uuid.uuid4())[0:10]
    }

  def expand(self, pcoll):
    if self.method == ReadFromBigQuery.Method.EXPORT:
      output_pcollection = self._expand_export(pcoll)
    elif self.method == ReadFromBigQuery.Method.DIRECT_READ:
      output_pcollection = self._expand_direct_read(pcoll)

    else:
      raise ValueError(
          'The method to read from BigQuery must be either EXPORT '
          'or DIRECT_READ.')
    return self._expand_output_type(output_pcollection)

  def _expand_output_type(self, output_pcollection):
    if self.output_type == 'PYTHON_DICT' or self.output_type is None:
      return output_pcollection
    elif self.output_type == 'BEAM_ROW':
      table_details = bigquery_tools.parse_table_reference(
          table=self._kwargs.get("table", None),
          dataset=self._kwargs.get("dataset", None),
          project=self._kwargs.get("project", None))
      if isinstance(self._kwargs['table'], ValueProvider):
        raise TypeError(
            '%s: table must be of type string'
            '; got ValueProvider instead' % self.__class__.__name__)
      elif callable(self._kwargs['table']):
        raise TypeError(
            '%s: table must be of type string'
            '; got a callable instead' % self.__class__.__name__)
      return output_pcollection | bigquery_schema_tools.convert_to_usertype(
          bigquery_tools.BigQueryWrapper().get_table(
              project_id=table_details.projectId,
              dataset_id=table_details.datasetId,
              table_id=table_details.tableId).schema,
          self._kwargs.get('selected_fields', None))
    else:
      raise ValueError(
          'The output type from BigQuery must be either PYTHON_DICT '
          'or BEAM_ROW.')

  def _expand_export(self, pcoll):
    # TODO(https://github.com/apache/beam/issues/20683): Make ReadFromBQ rely
    # on ReadAllFromBQ implementation.
    temp_location = pcoll.pipeline.options.view_as(
        GoogleCloudOptions).temp_location
    job_name = pcoll.pipeline.options.view_as(GoogleCloudOptions).job_name
    gcs_location_vp = self.gcs_location
    unique_id = str(uuid.uuid4())[0:10]

    def file_path_to_remove(unused_elm):
      gcs_location = bigquery_export_destination_uri(
          gcs_location_vp, temp_location, unique_id, True)
      return gcs_location + '/'

    files_to_remove_pcoll = beam.pvalue.AsList(
        pcoll.pipeline
        | 'FilesToRemoveImpulse' >> beam.Create([None])
        | 'MapFilesToRemove' >> beam.Map(file_path_to_remove))

    try:
      step_name = self.label
    except AttributeError:
      step_name = 'ReadFromBigQuery_%d' % ReadFromBigQuery.COUNTER
      ReadFromBigQuery.COUNTER += 1
    return (
        pcoll
        | beam.io.Read(
            _CustomBigQuerySource(
                gcs_location=self.gcs_location,
                pipeline_options=pcoll.pipeline.options,
                method=self.method,
                job_name=job_name,
                step_name=step_name,
                unique_id=unique_id,
                *self._args,
                **self._kwargs))
        | _PassThroughThenCleanup(files_to_remove_pcoll))

  def _expand_direct_read(self, pcoll):
    project_id = None
    temp_table_ref = None
    if 'temp_dataset' in self._kwargs:
      temp_table_ref = bigquery.TableReference(
          projectId=self._kwargs['temp_dataset'].projectId,
          datasetId=self._kwargs['temp_dataset'].datasetId,
          tableId='beam_temp_table_' + uuid.uuid4().hex)
    else:
      project_id = pcoll.pipeline.options.view_as(GoogleCloudOptions).project

    pipeline_details = {}
    if temp_table_ref is not None:
      pipeline_details['temp_table_ref'] = temp_table_ref
    elif project_id is not None:
      pipeline_details['project_id'] = project_id
      pipeline_details['bigquery_dataset_labels'] = self.bigquery_dataset_labels

    def _get_pipeline_details(unused_elm):
      return pipeline_details

    project_to_cleanup_pcoll = beam.pvalue.AsList(
        pcoll.pipeline
        | 'ProjectToCleanupImpulse' >> beam.Create([None])
        | 'MapProjectToCleanup' >> beam.Map(_get_pipeline_details))

    return (
        pcoll
        | beam.io.Read(
            _CustomBigQueryStorageSource(
                pipeline_options=pcoll.pipeline.options,
                method=self.method,
                use_native_datetime=self.use_native_datetime,
                temp_table=temp_table_ref,
                bigquery_dataset_labels=self.bigquery_dataset_labels,
                *self._args,
                **self._kwargs))
        | _PassThroughThenCleanupTempDatasets(project_to_cleanup_pcoll))


class ReadFromBigQueryRequest:
  """
  Class that defines data to read from BQ.
  """
  def __init__(
      self,
      query: str = None,
      use_standard_sql: bool = True,
      table: Union[str, TableReference] = None,
      flatten_results: bool = False):
    """
    Only one of query or table should be specified.

    :param query: SQL query to fetch data.
    :param use_standard_sql:
      Specifies whether to use BigQuery's standard SQL dialect for this query.
      The default value is :data:`True`. If set to :data:`False`,
      the query will use BigQuery's legacy SQL dialect.
      This parameter is ignored for table inputs.
    :param table:
      The ID of the table to read. Table should define project and dataset
      (ex.: ``'PROJECT:DATASET.TABLE'``).
    :param flatten_results:
      Flattens all nested and repeated fields in the query results.
      The default value is :data:`False`.
    """
    self.flatten_results = flatten_results
    self.query = query
    self.use_standard_sql = use_standard_sql
    self.table = table
    self.validate()

    # We use this internal object ID to generate BigQuery export directories
    # and to create BigQuery job names
    self.obj_id = '%d_%s' % (int(time.time()), secrets.token_hex(3))

  def validate(self):
    if self.table is not None and self.query is not None:
      raise ValueError(
          'Both a BigQuery table and a query were specified.'
          ' Please specify only one of these.')
    elif self.table is None and self.query is None:
      raise ValueError('A BigQuery table or a query must be specified')
    if self.table is not None:
      if isinstance(self.table, str):
        assert self.table.find('.'), (
            'Expected a table reference '
            '(PROJECT:DATASET.TABLE or DATASET.TABLE) instead of %s'
            % self.table)


class ReadAllFromBigQuery(PTransform):
  """Read data from BigQuery.

    PTransform:ReadFromBigQueryRequest->Rows

    This PTransform uses a BigQuery export job to take a snapshot of the table
    on GCS, and then reads from each produced file. Data is exported into
    a new subdirectory for each export using UUIDs generated in
    `ReadFromBigQueryRequest` objects.

    It is recommended not to use this PTransform for streaming jobs on
    GlobalWindow, since it will not be able to cleanup snapshots.

  Args:
    gcs_location (str): The name of the Google Cloud Storage
      bucket where the extracted table should be written as a string. If
      :data:`None`, then the temp_location parameter is used.
    validate (bool): If :data:`True`, various checks will be done when source
      gets initialized (e.g., is table present?). Set this to :data:`False`
      if the BigQuery export method is slow due to checking file existence.
    kms_key (str): Experimental. Optional Cloud KMS key name for use when
      creating new temporary tables.
   """
  COUNTER = 0

  def __init__(
      self,
      gcs_location: Union[str, ValueProvider] = None,
      validate: bool = False,
      kms_key: str = None,
      temp_dataset: Union[str, DatasetReference] = None,
      bigquery_job_labels: Dict[str, str] = None,
      query_priority: str = BigQueryQueryPriority.BATCH):
    if gcs_location:
      if not isinstance(gcs_location, (str, ValueProvider)):
        raise TypeError(
            '%s: gcs_location must be of type string'
            ' or ValueProvider; got %r instead' %
            (self.__class__.__name__, type(gcs_location)))

    self.gcs_location = gcs_location
    self.validate = validate
    self.kms_key = kms_key
    self.bigquery_job_labels = bigquery_job_labels
    self.temp_dataset = temp_dataset
    self.query_priority = query_priority

  def expand(self, pcoll):
    job_name = pcoll.pipeline.options.view_as(GoogleCloudOptions).job_name
    project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    unique_id = str(uuid.uuid4())[0:10]

    try:
      step_name = self.label
    except AttributeError:
      step_name = 'ReadAllFromBigQuery_%d' % ReadAllFromBigQuery.COUNTER
      ReadAllFromBigQuery.COUNTER += 1

    sources_to_read, cleanup_locations = (
        pcoll
        | beam.ParDo(
        _BigQueryReadSplit(
            options=pcoll.pipeline.options,
            gcs_location=self.gcs_location,
            validate=self.validate,
            bigquery_job_labels=self.bigquery_job_labels,
            job_name=job_name,
            step_name=step_name,
            unique_id=unique_id,
            kms_key=self.kms_key,
            project=project,
            temp_dataset=self.temp_dataset,
            query_priority=self.query_priority)).with_outputs(
        "location_to_cleanup", main="files_to_read")
    )

    return (
        sources_to_read
        | SDFBoundedSourceReader(data_to_display=self.display_data())
        | _PassThroughThenCleanup(beam.pvalue.AsIter(cleanup_locations)))
