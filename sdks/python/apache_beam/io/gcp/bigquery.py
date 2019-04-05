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

  main_table = pipeline | 'VeryBig' >> beam.io.Read(beam.io.BigQuerySource()
  side_table = pipeline | 'NotBig' >> beam.io.Read(beam.io.BigQuerySource()
  results = (
      main_table
      | 'ProcessData' >> beam.Map(
          lambda element, side_input: ..., AsList(side_table)))

There is no difference in how main and side inputs are read. What makes the
side_table a 'side input' is the AsList wrapper used when passing the table
as a parameter to the Map transform. AsList signals to the execution framework
that its input should be made available whole.

The main and side inputs are implemented differently. Reading a BigQuery table
as main input entails exporting the table to a set of GCS files (currently in
JSON format) and then processing those files. Reading the same table as a side
input entails querying the table for all its rows. The coder argument on
BigQuerySource controls the reading of the lines in the export files (i.e.,
transform a JSON object into a PCollection element). The coder is not involved
when the same table is read as a side input since there is no intermediate
format involved. We get the table rows directly from the BigQuery service with
a query.

Users may provide a query to read from rather than reading all of a BigQuery
table. If specified, the result obtained by executing the specified query will
be used as the data of the input transform.::

  query_results = pipeline | beam.io.Read(beam.io.BigQuerySource(
      query='SELECT year, mean_temp FROM samples.weather_stations'))

When creating a BigQuery input transform, users should provide either a query
or a table. Pipeline construction will fail with a validation error if neither
or both are specified.

**Time partitioned tables**

BigQuery sink currently does not fully support writing to BigQuery
time partitioned tables. But writing to a *single* partition may work if
that does not involve creating a new table (for example, when writing to an
existing table with `create_disposition=CREATE_NEVER` and
`write_disposition=WRITE_APPEND`).
BigQuery source supports reading from a single time partition with the partition
decorator specified as a part of the table identifier.

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
"""

from __future__ import absolute_import

import collections
import itertools
import json
import logging
import time
from builtins import object
from builtins import zip

from future.utils import itervalues
from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam import pvalue
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options import value_provider as vp
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import retry
from apache_beam.utils.annotations import deprecated

__all__ = [
    'TableRowJsonCoder',
    'BigQueryDisposition',
    'BigQuerySource',
    'BigQuerySink',
    'WriteToBigQuery',
    ]


@deprecated(since='2.11.0', current="bigquery_tools.parse_table_reference")
def _parse_table_reference(table, dataset=None, project=None):
  return bigquery_tools.parse_table_reference(table, dataset, project)


@deprecated(since='2.11.0',
            current="bigquery_tools.parse_table_schema_from_json")
def parse_table_schema_from_json(schema_string):
  return bigquery_tools.parse_table_schema_from_json(schema_string)


@deprecated(since='2.11.0', current="bigquery_tools.default_encoder")
def default_encoder(obj):
  return bigquery_tools.default_encoder(obj)


@deprecated(since='2.11.0', current="bigquery_tools.RowAsDictJsonCoder")
def RowAsDictJsonCoder(*args, **kwargs):
  return bigquery_tools.RowAsDictJsonCoder(*args, **kwargs)


@deprecated(since='2.11.0', current="bigquery_tools.BigQueryReader")
def BigQueryReader(*args, **kwargs):
  return bigquery_tools.BigQueryReader(*args, **kwargs)


@deprecated(since='2.11.0', current="bigquery_tools.BigQueryWriter")
def BigQueryWriter(*args, **kwargs):
  return bigquery_tools.BigQueryWriter(*args, **kwargs)


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
              zip(self.field_names,
                  [from_json_value(f.v) for f in table_row.f])),
          allow_nan=False,
          default=bigquery_tools.default_encoder)
    except ValueError as e:
      raise ValueError('%s. %s' % (e, bigquery_tools.JSON_COMPLIANCE_ERROR))

  def decode(self, encoded_table_row):
    od = json.loads(
        encoded_table_row, object_pairs_hook=collections.OrderedDict)
    return bigquery.TableRow(
        f=[bigquery.TableCell(v=to_json_value(e)) for e in itervalues(od)])


class BigQueryDisposition(object):
  """Class holding standard strings used for create and write dispositions."""

  CREATE_NEVER = 'CREATE_NEVER'
  CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
  WRITE_TRUNCATE = 'WRITE_TRUNCATE'
  WRITE_APPEND = 'WRITE_APPEND'
  WRITE_EMPTY = 'WRITE_EMPTY'

  @staticmethod
  def validate_create(disposition):
    values = (BigQueryDisposition.CREATE_NEVER,
              BigQueryDisposition.CREATE_IF_NEEDED)
    if disposition not in values:
      raise ValueError(
          'Invalid create disposition %s. Expecting %s' % (disposition, values))
    return disposition

  @staticmethod
  def validate_write(disposition):
    values = (BigQueryDisposition.WRITE_TRUNCATE,
              BigQueryDisposition.WRITE_APPEND,
              BigQueryDisposition.WRITE_EMPTY)
    if disposition not in values:
      raise ValueError(
          'Invalid write disposition %s. Expecting %s' % (disposition, values))
    return disposition


# -----------------------------------------------------------------------------
# BigQuerySource, BigQuerySink.


class BigQuerySource(dataflow_io.NativeSource):
  """A source based on a BigQuery table."""

  def __init__(self, table=None, dataset=None, project=None, query=None,
               validate=False, coder=None, use_standard_sql=False,
               flatten_results=True, kms_key=None):
    """Initialize a :class:`BigQuerySource`.

    Args:
      table (str): The ID of a BigQuery table. If specified all data of the
        table will be used as input of the current source. The ID must contain
        only letters ``a-z``, ``A-Z``, numbers ``0-9``, or underscores
        ``_``. If dataset and query arguments are :data:`None` then the table
        argument must contain the entire table reference specified as:
        ``'DATASET.TABLE'`` or ``'PROJECT:DATASET.TABLE'``.
      dataset (str): The ID of the dataset containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument or a query is specified.
      project (str): The ID of the project containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument or a query is specified.
      query (str): A query to be used instead of arguments table, dataset, and
        project.
      validate (bool): If :data:`True`, various checks will be done when source
        gets initialized (e.g., is table present?). This should be
        :data:`True` for most scenarios in order to catch errors as early as
        possible (pipeline construction instead of pipeline execution). It
        should be :data:`False` if the table is created during pipeline
        execution by a previous step.
      coder (~apache_beam.coders.coders.Coder): The coder for the table
        rows if serialized to disk. If :data:`None`, then the default coder is
        :class:`~apache_beam.io.gcp.bigquery_tools.RowAsDictJsonCoder`,
        which will interpret every line in a file as a JSON serialized
        dictionary. This argument needs a value only in special cases when
        returning table rows as dictionaries is not desirable.
      use_standard_sql (bool): Specifies whether to use BigQuery's standard SQL
        dialect for this query. The default value is :data:`False`.
        If set to :data:`True`, the query will use BigQuery's updated SQL
        dialect with improved standards compliance.
        This parameter is ignored for table inputs.
      flatten_results (bool): Flattens all nested and repeated fields in the
        query results. The default value is :data:`True`.
      kms_key (str): Experimental. Optional Cloud KMS key name for use when
        creating new tables.

    Raises:
      ~exceptions.ValueError: if any of the following is true:

        1) the table reference as a string does not match the expected format
        2) neither a table nor a query is specified
        3) both a table and a query is specified.
    """

    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apitools.base import py  # pylint: disable=unused-variable
    except ImportError:
      raise ImportError(
          'Google Cloud IO not available, '
          'please install apache_beam[gcp]')

    if table is not None and query is not None:
      raise ValueError('Both a BigQuery table and a query were specified.'
                       ' Please specify only one of these.')
    elif table is None and query is None:
      raise ValueError('A BigQuery table or a query must be specified')
    elif table is not None:
      self.table_reference = bigquery_tools.parse_table_reference(
          table, dataset, project)
      self.query = None
      self.use_legacy_sql = True
    else:
      self.query = query
      # TODO(BEAM-1082): Change the internal flag to be standard_sql
      self.use_legacy_sql = not use_standard_sql
      self.table_reference = None

    self.validate = validate
    self.flatten_results = flatten_results
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()
    self.kms_key = kms_key

  def display_data(self):
    if self.query is not None:
      res = {'query': DisplayDataItem(self.query, label='Query')}
    else:
      if self.table_reference.projectId is not None:
        tableSpec = '{}:{}.{}'.format(self.table_reference.projectId,
                                      self.table_reference.datasetId,
                                      self.table_reference.tableId)
      else:
        tableSpec = '{}.{}'.format(self.table_reference.datasetId,
                                   self.table_reference.tableId)
      res = {'table': DisplayDataItem(tableSpec, label='Table')}

    res['validation'] = DisplayDataItem(self.validate,
                                        label='Validation Enabled')
    return res

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'bigquery'

  def reader(self, test_bigquery_client=None):
    return bigquery_tools.BigQueryReader(
        source=self,
        test_bigquery_client=test_bigquery_client,
        use_legacy_sql=self.use_legacy_sql,
        flatten_results=self.flatten_results,
        kms_key=self.kms_key)


@deprecated(since='2.11.0', current="WriteToBigQuery")
class BigQuerySink(dataflow_io.NativeSink):
  """A sink based on a BigQuery table.

  This BigQuery sink triggers a Dataflow native sink for BigQuery
  that only supports batch pipelines.
  Instead of using this sink directly, please use WriteToBigQuery
  transform that works for both batch and streaming pipelines.
  """

  def __init__(self, table, dataset=None, project=None, schema=None,
               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=BigQueryDisposition.WRITE_EMPTY,
               validate=False, coder=None, kms_key=None):
    """Initialize a BigQuerySink.

    Args:
      table (str): The ID of the table. The ID must contain only letters
        ``a-z``, ``A-Z``, numbers ``0-9``, or underscores ``_``. If
        **dataset** argument is :data:`None` then the table argument must
        contain the entire table reference specified as: ``'DATASET.TABLE'`` or
        ``'PROJECT:DATASET.TABLE'``.
      dataset (str): The ID of the dataset containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      project (str): The ID of the project containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      schema (str): The schema to be used if the BigQuery table to write has
        to be created. This can be either specified as a
        :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema` object or a single string  of the form
        ``'field1:type1,field2:type2,field3:type3'`` that defines a comma
        separated list of fields. Here ``'type'`` should specify the BigQuery
        type of the field. Single string based schemas do not support nested
        fields, repeated fields, or specifying a BigQuery mode for fields (mode
        will always be set to ``'NULLABLE'``).
      create_disposition (BigQueryDisposition): A string describing what
        happens if the table does not exist. Possible values are:

          * :attr:`BigQueryDisposition.CREATE_IF_NEEDED`: create if does not
            exist.
          * :attr:`BigQueryDisposition.CREATE_NEVER`: fail the write if does not
            exist.

      write_disposition (BigQueryDisposition): A string describing what
        happens if the table has already some data. Possible values are:

          * :attr:`BigQueryDisposition.WRITE_TRUNCATE`: delete existing rows.
          * :attr:`BigQueryDisposition.WRITE_APPEND`: add to existing rows.
          * :attr:`BigQueryDisposition.WRITE_EMPTY`: fail the write if table not
            empty.

      validate (bool): If :data:`True`, various checks will be done when sink
        gets initialized (e.g., is table present given the disposition
        arguments?). This should be :data:`True` for most scenarios in order to
        catch errors as early as possible (pipeline construction instead of
        pipeline execution). It should be :data:`False` if the table is created
        during pipeline execution by a previous step.
      coder (~apache_beam.coders.coders.Coder): The coder for the
        table rows if serialized to disk. If :data:`None`, then the default
        coder is :class:`~apache_beam.io.gcp.bigquery_tools.RowAsDictJsonCoder`,
        which will interpret every element written to the sink as a dictionary
        that will be JSON serialized as a line in a file. This argument needs a
        value only in special cases when writing table rows as dictionaries is
        not desirable.
      kms_key (str): Experimental. Optional Cloud KMS key name for use when
        creating new tables.

    Raises:
      ~exceptions.TypeError: if the schema argument is not a :class:`str` or a
        :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema` object.
      ~exceptions.ValueError: if the table reference as a string does not
        match the expected format.
    """
    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apitools.base import py  # pylint: disable=unused-variable
    except ImportError:
      raise ImportError(
          'Google Cloud IO not available, '
          'please install apache_beam[gcp]')

    self.table_reference = bigquery_tools.parse_table_reference(
        table, dataset, project)
    # Transform the table schema into a bigquery.TableSchema instance.
    if isinstance(schema, (str, unicode)):
      # TODO(silviuc): Should add a regex-based validation of the format.
      table_schema = bigquery.TableSchema()
      schema_list = [s.strip(' ') for s in schema.split(',')]
      for field_and_type in schema_list:
        field_name, field_type = field_and_type.split(':')
        field_schema = bigquery.TableFieldSchema()
        field_schema.name = field_name
        field_schema.type = field_type
        field_schema.mode = 'NULLABLE'
        table_schema.fields.append(field_schema)
      self.table_schema = table_schema
    elif schema is None:
      # TODO(silviuc): Should check that table exists if no schema specified.
      self.table_schema = schema
    elif isinstance(schema, bigquery.TableSchema):
      self.table_schema = schema
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

    self.create_disposition = BigQueryDisposition.validate_create(
        create_disposition)
    self.write_disposition = BigQueryDisposition.validate_write(
        write_disposition)
    self.validate = validate
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()
    self.kms_key = kms_key

  def display_data(self):
    res = {}
    if self.table_reference is not None:
      tableSpec = '{}.{}'.format(self.table_reference.datasetId,
                                 self.table_reference.tableId)
      if self.table_reference.projectId is not None:
        tableSpec = '{}:{}'.format(self.table_reference.projectId,
                                   tableSpec)
      res['table'] = DisplayDataItem(tableSpec, label='Table')

    res['validation'] = DisplayDataItem(self.validate,
                                        label="Validation Enabled")
    return res

  def schema_as_json(self):
    """Returns the TableSchema associated with the sink as a JSON string."""

    def schema_list_as_object(schema_list):
      """Returns a list of TableFieldSchema objects as a list of dicts."""
      fields = []
      for f in schema_list:
        fs = {'name': f.name, 'type': f.type}
        if f.description is not None:
          fs['description'] = f.description
        if f.mode is not None:
          fs['mode'] = f.mode
        if f.type.lower() == 'record':
          fs['fields'] = schema_list_as_object(f.fields)
        fields.append(fs)
      return fields
    return json.dumps(
        {'fields': schema_list_as_object(self.table_schema.fields)})

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'bigquery'

  def writer(self, test_bigquery_client=None, buffer_size=None):
    return bigquery_tools.BigQueryWriter(
        sink=self, test_bigquery_client=test_bigquery_client,
        buffer_size=buffer_size)


class BigQueryWriteFn(DoFn):
  """A ``DoFn`` that streams writes to BigQuery once the table is created."""

  DEFAULT_MAX_BUFFERED_ROWS = 2000
  DEFAULT_MAX_BATCH_SIZE = 500

  FAILED_ROWS = 'FailedRows'

  def __init__(
      self,
      batch_size,
      schema=None,
      create_disposition=None,
      write_disposition=None,
      kms_key=None,
      test_client=None,
      max_buffered_rows=None,
      retry_strategy=None):
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
      kms_key: Experimental. Optional Cloud KMS key name for use when creating
        new tables.
      test_client: Override the default bigquery client used for testing.

      max_buffered_rows: The maximum number of rows that are allowed to stay
        buffered when running dynamic destinations. When destinations are
        dynamic, it is important to keep caches small even when a single
        batch has not been completely filled up.
      retry_strategy: The strategy to use when retrying streaming inserts
        into BigQuery. Options are shown in bigquery_tools.RetryStrategy attrs.
    """
    self.schema = schema
    self.test_client = test_client
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self._rows_buffer = []
    self._reset_rows_buffer()
    self._observed_tables = set()

    self._total_buffered_rows = 0
    self.kms_key = kms_key
    self._max_batch_size = batch_size or BigQueryWriteFn.DEFAULT_MAX_BATCH_SIZE
    self._max_buffered_rows = (max_buffered_rows
                               or BigQueryWriteFn.DEFAULT_MAX_BUFFERED_ROWS)
    self._retry_strategy = (
        retry_strategy or bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR)

  def display_data(self):
    return {'max_batch_size': self._max_batch_size,
            'max_buffered_rows': self._max_buffered_rows,
            'retry_strategy': self._retry_strategy}

  def _reset_rows_buffer(self):
    self._rows_buffer = collections.defaultdict(lambda: [])

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
    elif isinstance(schema, (str, unicode)):
      return bigquery_tools.parse_table_schema_from_json(schema)
    elif isinstance(schema, dict):
      return bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

  def start_bundle(self):
    self._reset_rows_buffer()

    self.bigquery_wrapper = bigquery_tools.BigQueryWrapper(
        client=self.test_client)

    self._observed_tables = set()

    self._backoff_calculator = iter(retry.FuzzedExponentialIntervals(
        initial_delay_secs=0.2,
        num_retries=10000,
        max_delay_secs=1500))

  def _create_table_if_needed(self, table_reference, schema=None):
    str_table_reference = '%s:%s.%s' % (
        table_reference.projectId,
        table_reference.datasetId,
        table_reference.tableId)
    if str_table_reference in self._observed_tables:
      return

    if self.create_disposition == BigQueryDisposition.CREATE_NEVER:
      # If we never want to create the table, we assume it already exists,
      # and avoid the get-or-create step.
      return

    logging.debug('Creating or getting table %s with schema %s.',
                  table_reference, schema)

    table_schema = self.get_table_schema(schema)
    if table_reference.projectId is None:
      table_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')
    self.bigquery_wrapper.get_or_create_table(
        table_reference.projectId,
        table_reference.datasetId,
        table_reference.tableId,
        table_schema,
        self.create_disposition, self.write_disposition)
    self._observed_tables.add(str_table_reference)

  def process(self, element, unused_create_fn_output=None):
    destination = element[0]

    if callable(self.schema):
      schema = self.schema(destination)
    elif isinstance(self.schema, vp.ValueProvider):
      schema = self.schema.get()
    else:
      schema = self.schema

    self._create_table_if_needed(
        bigquery_tools.parse_table_reference(destination),
        schema)

    destination = bigquery_tools.get_hashable_destination(destination)

    row = element[1]
    self._rows_buffer[destination].append(row)
    self._total_buffered_rows += 1
    if len(self._rows_buffer[destination]) >= self._max_batch_size:
      return self._flush_batch(destination)
    elif self._total_buffered_rows >= self._max_buffered_rows:
      return self._flush_all_batches()

  def finish_bundle(self):
    return self._flush_all_batches()

  def _flush_all_batches(self):
    logging.debug('Attempting to flush to all destinations. Total buffered: %s',
                  self._total_buffered_rows)

    return itertools.chain(*[self._flush_batch(destination)
                             for destination in list(self._rows_buffer.keys())
                             if self._rows_buffer[destination]])

  def _flush_batch(self, destination):

    # Flush the current batch of rows to BigQuery.
    rows = self._rows_buffer[destination]
    table_reference = bigquery_tools.parse_table_reference(destination)

    if table_reference.projectId is None:
      table_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    logging.debug('Flushing data to %s. Total %s rows.',
                  destination, len(rows))

    while True:
      # TODO: Figure out an insertId to make calls idempotent.
      passed, errors = self.bigquery_wrapper.insert_rows(
          project_id=table_reference.projectId,
          dataset_id=table_reference.datasetId,
          table_id=table_reference.tableId,
          rows=rows,
          skip_invalid_rows=True)

      logging.debug("Passed: %s. Errors are %s", passed, errors)
      failed_rows = [rows[entry.index] for entry in errors]
      should_retry = any(
          bigquery_tools.RetryStrategy.should_retry(
              self._retry_strategy, entry.errors[0].reason)
          for entry in errors)
      rows = failed_rows

      if not should_retry:
        break
      else:
        retry_backoff = next(self._backoff_calculator)
        logging.info('Sleeping %s seconds before retrying insertion.',
                     retry_backoff)
        time.sleep(retry_backoff)

    self._total_buffered_rows -= len(self._rows_buffer[destination])
    del self._rows_buffer[destination]

    return [pvalue.TaggedOutput(BigQueryWriteFn.FAILED_ROWS,
                                GlobalWindows.windowed_value(
                                    (destination, row))) for row in failed_rows]


class WriteToBigQuery(PTransform):

  class Method(object):
    DEFAULT = 'DEFAULT'
    STREAMING_INSERTS = 'STREAMING_INSERTS'
    FILE_LOADS = 'FILE_LOADS'

  def __init__(self,
               table,
               dataset=None,
               project=None,
               schema=None,
               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=BigQueryDisposition.WRITE_APPEND,
               kms_key=None,
               batch_size=None,
               max_file_size=None,
               max_files_per_bundle=None,
               test_client=None,
               custom_gcs_temp_location=None,
               method=None,
               insert_retry_strategy=None,
               validate=True):
    """Initialize a WriteToBigQuery transform.

    Args:
      table (str, callable, ValueProvider): The ID of the table, or a callable
         that returns it. The ID must contain only letters ``a-z``, ``A-Z``,
         numbers ``0-9``, or underscores ``_``. If dataset argument is
         :data:`None` then the table argument must contain the entire table
         reference specified as: ``'DATASET.TABLE'``
         or ``'PROJECT:DATASET.TABLE'``. If it's a callable, it must receive one
         argument representing an element to be written to BigQuery, and return
         a TableReference, or a string table name as specified above.
         Multiple destinations are only supported on Batch pipelines at the
         moment.
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
        a TableReference or a string, and return a str, dict or TableSchema.
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
      kms_key (str): Experimental. Optional Cloud KMS key name for use when
        creating new tables.
      batch_size (int): Number of rows to be written to BQ per streaming API
        insert. The default is 500.
        insert.
      test_client: Override the default bigquery client used for testing.
      max_file_size (int): The maximum size for a file to be written and then
        loaded into BigQuery. The default value is 4TB, which is 80% of the
        limit of 5TB for BigQuery to load any file.
      max_files_per_bundle(int): The maximum number of files to be concurrently
        written by a worker. The default here is 20. Larger values will allow
        writing to multiple destinations without having to reshard - but they
        increase the memory burden on the workers.
      custom_gcs_temp_location (str): A GCS location to store files to be used
        for file loads into BigQuery. By default, this will use the pipeline's
        temp_location, but for pipelines whose temp_location is not appropriate
        for BQ File Loads, users should pass a specific one.
      method: The method to use to write to BigQuery. It may be
        STREAMING_INSERTS, FILE_LOADS, or DEFAULT. An introduction on loading
        data to BigQuery: https://cloud.google.com/bigquery/docs/loading-data.
        DEFAULT will use STREAMING_INSERTS on Streaming pipelines and
        FILE_LOADS on Batch pipelines.
      insert_retry_strategy: The strategy to use when retrying streaming inserts
        into BigQuery. Options are shown in bigquery_tools.RetryStrategy attrs.
      validate: Indicates whether to perform validation checks on
        inputs. This parameter is primarily used for testing.
    """
    self.table_reference = bigquery_tools.parse_table_reference(
        table, dataset, project)
    self.create_disposition = BigQueryDisposition.validate_create(
        create_disposition)
    self.write_disposition = BigQueryDisposition.validate_write(
        write_disposition)
    self.schema = WriteToBigQuery.get_dict_table_schema(schema)
    self.batch_size = batch_size
    self.kms_key = kms_key
    self.test_client = test_client

    # TODO(pabloem): Consider handling ValueProvider for this location.
    self.custom_gcs_temp_location = custom_gcs_temp_location
    self.max_file_size = max_file_size
    self.max_files_per_bundle = max_files_per_bundle
    self.method = method or WriteToBigQuery.Method.DEFAULT
    self.insert_retry_strategy = insert_retry_strategy
    self._validate = validate

  @staticmethod
  def get_table_schema_from_string(schema):
    """Transform the string table schema into a
    :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema` instance.

    Args:
      schema (str): The sting schema to be used if the BigQuery table to write
        has to be created.

    Returns:
      ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema:
      The schema to be used if the BigQuery table to write has to be created
      but in the :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema` format.
    """
    table_schema = bigquery.TableSchema()
    schema_list = [s.strip() for s in schema.split(',')]
    for field_and_type in schema_list:
      field_name, field_type = field_and_type.split(':')
      field_schema = bigquery.TableFieldSchema()
      field_schema.name = field_name
      field_schema.type = field_type
      field_schema.mode = 'NULLABLE'
      table_schema.fields.append(field_schema)
    return table_schema

  @staticmethod
  def table_schema_to_dict(table_schema):
    """Create a dictionary representation of table schema for serialization
    """
    def get_table_field(field):
      """Create a dictionary representation of a table field
      """
      result = {}
      result['name'] = field.name
      result['type'] = field.type
      result['mode'] = getattr(field, 'mode', 'NULLABLE')
      if hasattr(field, 'description') and field.description is not None:
        result['description'] = field.description
      if hasattr(field, 'fields') and field.fields:
        result['fields'] = [get_table_field(f) for f in field.fields]
      return result

    if not isinstance(table_schema, bigquery.TableSchema):
      raise ValueError("Table schema must be of the type bigquery.TableSchema")
    schema = {'fields': []}
    for field in table_schema.fields:
      schema['fields'].append(get_table_field(field))
    return schema

  @staticmethod
  def get_dict_table_schema(schema):
    """Transform the table schema into a dictionary instance.

    Args:
      schema (~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
        The schema to be used if the BigQuery table to write has to be created.
        This can either be a dict or string or in the TableSchema format.

    Returns:
      Dict[str, Any]: The schema to be used if the BigQuery table to write has
      to be created but in the dictionary format.
    """
    if (isinstance(schema, (dict, vp.ValueProvider)) or
        callable(schema) or
        schema is None):
      return schema
    elif isinstance(schema, (str, unicode)):
      table_schema = WriteToBigQuery.get_table_schema_from_string(schema)
      return WriteToBigQuery.table_schema_to_dict(table_schema)
    elif isinstance(schema, bigquery.TableSchema):
      return WriteToBigQuery.table_schema_to_dict(schema)
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

  def _compute_method(self, pipeline, options):
    experiments = options.view_as(DebugOptions).experiments or []

    # TODO(pabloem): Use a different method to determine if streaming or batch.
    streaming_pipeline = pipeline.options.view_as(StandardOptions).streaming

    # If the new BQ sink is not activated for experiment flags, then we use
    # streaming inserts by default (it gets overridden in dataflow_runner.py).
    if 'use_beam_bq_sink' not in experiments:
      return self.Method.STREAMING_INSERTS
    elif self.method == self.Method.DEFAULT and streaming_pipeline:
      return self.Method.STREAMING_INSERTS
    elif self.method == self.Method.DEFAULT and not streaming_pipeline:
      return self.Method.FILE_LOADS
    else:
      return self.method

  def expand(self, pcoll):
    p = pcoll.pipeline

    if (isinstance(self.table_reference, bigquery.TableReference)
        and self.table_reference.projectId is None):
      self.table_reference.projectId = pcoll.pipeline.options.view_as(
          GoogleCloudOptions).project

    method_to_use = self._compute_method(p, p.options)

    if method_to_use == WriteToBigQuery.Method.STREAMING_INSERTS:
      # TODO: Support load jobs for streaming pipelines.
      bigquery_write_fn = BigQueryWriteFn(
          schema=self.schema,
          batch_size=self.batch_size,
          create_disposition=self.create_disposition,
          write_disposition=self.write_disposition,
          kms_key=self.kms_key,
          retry_strategy=self.insert_retry_strategy,
          test_client=self.test_client)

      outputs = (pcoll
                 | 'AppendDestination' >> beam.ParDo(
                     bigquery_tools.AppendDestinationsFn(self.table_reference))
                 | 'StreamInsertRows' >> ParDo(bigquery_write_fn).with_outputs(
                     BigQueryWriteFn.FAILED_ROWS, main='main'))

      return {BigQueryWriteFn.FAILED_ROWS: outputs[BigQueryWriteFn.FAILED_ROWS]}
    else:
      if p.options.view_as(StandardOptions).streaming:
        raise NotImplementedError(
            'File Loads to BigQuery are only supported on Batch pipelines.')

      from apache_beam.io.gcp import bigquery_file_loads
      return (pcoll
              | bigquery_file_loads.BigQueryBatchFileLoads(
                  destination=self.table_reference,
                  schema=self.schema,
                  create_disposition=self.create_disposition,
                  write_disposition=self.write_disposition,
                  max_file_size=self.max_file_size,
                  max_files_per_bundle=self.max_files_per_bundle,
                  custom_gcs_temp_location=self.custom_gcs_temp_location,
                  test_client=self.test_client,
                  validate=self._validate))

  def display_data(self):
    res = {}
    if self.table_reference is not None:
      tableSpec = '{}.{}'.format(self.table_reference.datasetId,
                                 self.table_reference.tableId)
      if self.table_reference.projectId is not None:
        tableSpec = '{}:{}'.format(self.table_reference.projectId,
                                   tableSpec)
      res['table'] = DisplayDataItem(tableSpec, label='Table')
    return res
