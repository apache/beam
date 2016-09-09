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
(common case) is expected to be massive and the Dataflow service will make sure
it is split into manageable chunks and processed in parallel. Side inputs are
expected to be small and will be read completely every time a ParDo DoFn gets
executed. In the example below the lambda function implementing the DoFn for the
Map transform will get on each call *one* row of the main table and *all* rows
of the side table. The execution framework may use some caching techniques to
share the side inputs between calls in order to avoid excessive reading::

  main_table = pipeline | 'very_big' >> beam.io.Read(beam.io.BigQuerySource()
  side_table = pipeline | 'not_big' >> beam.io.Read(beam.io.BigQuerySource()
  results = (
      main_table
      | beam.Map('process data',
               lambda element, side_input: ...,
               AsList(side_table)))

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
format involved.  We get the table rows directly from the BigQuery service with
a query.

Users may provide a query to read from rather than reading all of a BigQuery
table. If specified, the result obtained by executing the specified query will
be used as the data of the input transform.

  query_results = pipeline | beam.io.Read(beam.io.BigQuerySource(
      query='SELECT year, mean_temp FROM samples.weather_stations'))

When creating a BigQuery input transform, users should provide either a query
or a table. Pipeline construction will fail with a validation error if neither
or both are specified.

*** Short introduction to BigQuery concepts ***
Tables have rows (TableRow) and each row has cells (TableCell).
A table has a schema (TableSchema), which in turn describes the schema of each
cell (TableFieldSchema). The terms field and cell are used interchangeably.

TableSchema: Describes the schema (types and order) for values in each row.
  Has one attribute, 'field', which is list of TableFieldSchema objects.

TableFieldSchema: Describes the schema (type, name) for one field.
  Has several attributes, including 'name' and 'type'. Common values for
  the type attribute are: 'STRING', 'INTEGER', 'FLOAT', 'BOOLEAN'. All possible
  values are described at:
  https://cloud.google.com/bigquery/preparing-data-for-bigquery#datatypes

TableRow: Holds all values in a table row. Has one attribute, 'f', which is a
  list of TableCell instances.

TableCell: Holds the value for one cell (or field).  Has one attribute,
  'v', which is a JsonValue instance. This class is defined in
  apitools.base.py.extra_types.py module.
"""

from __future__ import absolute_import

import collections
import json
import logging
import re
import time
import uuid

from apitools.base.py.exceptions import HttpError

from apache_beam import coders
from apache_beam.internal import auth
from apache_beam.internal.json_value import from_json_value
from apache_beam.internal.json_value import to_json_value
from apache_beam.io import iobase
from apache_beam.utils import retry
from apache_beam.utils.options import GoogleCloudOptions

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.internal.clients import bigquery
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position


__all__ = [
    'TableRowJsonCoder',
    'BigQueryDisposition',
    'BigQuerySource',
    'BigQuerySink',
    ]

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'


class RowAsDictJsonCoder(coders.Coder):
  """A coder for a table row (represented as a dict) to/from a JSON string.

  This is the default coder for sources and sinks if the coder argument is not
  specified.
  """

  def encode(self, table_row):
    # The normal error when dumping NAN/INF values is:
    # ValueError: Out of range float values are not JSON compliant
    # This code will catch this error to emit an error that explains
    # to the programmer that they have used NAN/INF values.
    try:
      return json.dumps(table_row, allow_nan=False)
    except ValueError as e:
      raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

  def decode(self, encoded_table_row):
    return json.loads(encoded_table_row)


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
          allow_nan=False)
    except ValueError as e:
      raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

  def decode(self, encoded_table_row):
    od = json.loads(
        encoded_table_row, object_pairs_hook=collections.OrderedDict)
    return bigquery.TableRow(
        f=[bigquery.TableCell(v=to_json_value(e)) for e in od.itervalues()])


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


def _parse_table_reference(table, dataset=None, project=None):
  """Parses a table reference into a (project, dataset, table) tuple.

  Args:
    table: The ID of the table. The ID must contain only letters
      (a-z, A-Z), numbers (0-9), or underscores (_). If dataset argument is None
      then the table argument must contain the entire table reference:
      'DATASET.TABLE' or 'PROJECT:DATASET.TABLE'. This argument can be a
      bigquery.TableReference instance in which case dataset and project are
      ignored and the reference is returned as a result.
    dataset: The ID of the dataset containing this table or null if the table
      reference is specified entirely by the table argument.
    project: The ID of the project containing this table or null if the table
      reference is specified entirely by the table (and possibly dataset)
      argument.

  Returns:
    A bigquery.TableReference object. The object has the following attributes:
    projectId, datasetId, and tableId.

  Raises:
    ValueError: if the table reference as a string does not match the expected
      format.
  """

  if isinstance(table, bigquery.TableReference):
    return table

  table_reference = bigquery.TableReference()
  # If dataset argument is not specified, the expectation is that the
  # table argument will contain a full table reference instead of just a
  # table name.
  if dataset is None:
    match = re.match(
        r'^((?P<project>.+):)?(?P<dataset>\w+)\.(?P<table>\w+)$', table)
    if not match:
      raise ValueError(
          'Expected a table reference (PROJECT:DATASET.TABLE or '
          'DATASET.TABLE) instead of %s.' % table)
    table_reference.projectId = match.group('project')
    table_reference.datasetId = match.group('dataset')
    table_reference.tableId = match.group('table')
  else:
    table_reference.projectId = project
    table_reference.datasetId = dataset
    table_reference.tableId = table
  return table_reference


# -----------------------------------------------------------------------------
# BigQuerySource, BigQuerySink.


class BigQuerySource(iobase.NativeSource):
  """A source based on a BigQuery table."""

  def __init__(self, table=None, dataset=None, project=None, query=None,
               validate=False, coder=None):
    """Initialize a BigQuerySource.

    Args:
      table: The ID of a BigQuery table. If specified all data of the table
        will be used as input of the current source. The ID must contain only
        letters (a-z, A-Z), numbers (0-9), or underscores (_). If dataset
        and query arguments are None then the table argument must contain the
        entire table reference specified as: 'DATASET.TABLE' or
        'PROJECT:DATASET.TABLE'.
      dataset: The ID of the dataset containing this table or null if the table
        reference is specified entirely by the table argument or a query is
        specified.
      project: The ID of the project containing this table or null if the table
        reference is specified entirely by the table argument or a query is
        specified.
      query: A query to be used instead of arguments table, dataset, and
        project.
      validate: If true, various checks will be done when source gets
        initialized (e.g., is table present?). This should be True for most
        scenarios in order to catch errors as early as possible (pipeline
        construction instead of pipeline execution). It should be False if the
        table is created during pipeline execution by a previous step.
      coder: The coder for the table rows if serialized to disk. If None, then
        the default coder is RowAsDictJsonCoder, which will interpret every line
        in a file as a JSON serialized dictionary. This argument needs a value
        only in special cases when returning table rows as dictionaries is not
        desirable.

    Raises:
      ValueError: if any of the following is true
      (1) the table reference as a string does not match the expected format
      (2) neither a table nor a query is specified
      (3) both a table and a query is specified.
    """

    if table is not None and query is not None:
      raise ValueError('Both a BigQuery table and a query were specified.'
                       ' Please specify only one of these.')
    elif table is None and query is None:
      raise ValueError('A BigQuery table or a query must be specified')
    elif table is not None:
      self.table_reference = _parse_table_reference(table, dataset, project)
      self.query = None
    else:
      self.query = query
      self.table_reference = None

    self.validate = validate
    self.coder = coder or RowAsDictJsonCoder()

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'bigquery'

  def reader(self, test_bigquery_client=None):
    return BigQueryReader(
        source=self, test_bigquery_client=test_bigquery_client)


class BigQuerySink(iobase.NativeSink):
  """A sink based on a BigQuery table."""

  def __init__(self, table, dataset=None, project=None, schema=None,
               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=BigQueryDisposition.WRITE_EMPTY,
               validate=False, coder=None):
    """Initialize a BigQuerySink.

    Args:
      table: The ID of the table. The ID must contain only letters
        (a-z, A-Z), numbers (0-9), or underscores (_). If dataset argument is
        None then the table argument must contain the entire table reference
        specified as: 'DATASET.TABLE' or 'PROJECT:DATASET.TABLE'.
      dataset: The ID of the dataset containing this table or null if the table
        reference is specified entirely by the table argument.
      project: The ID of the project containing this table or null if the table
        reference is specified entirely by the table argument.
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
      validate: If true, various checks will be done when sink gets
        initialized (e.g., is table present given the disposition arguments?).
        This should be True for most scenarios in order to catch errors as early
        as possible (pipeline construction instead of pipeline execution). It
        should be False if the table is created during pipeline execution by a
        previous step.
      coder: The coder for the table rows if serialized to disk. If None, then
        the default coder is RowAsDictJsonCoder, which will interpret every
        element written to the sink as a dictionary that will be JSON serialized
        as a line in a file. This argument needs a value only in special cases
        when writing table rows as dictionaries is not desirable.

    Raises:
      TypeError: if the schema argument is not a string or a TableSchema object.
      ValueError: if the table reference as a string does not match the expected
      format.
    """
    self.table_reference = _parse_table_reference(table, dataset, project)
    # Transform the table schema into a bigquery.TableSchema instance.
    if isinstance(schema, basestring):
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
    self.coder = coder or RowAsDictJsonCoder()

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
    return BigQueryWriter(
        sink=self, test_bigquery_client=test_bigquery_client,
        buffer_size=buffer_size)


# -----------------------------------------------------------------------------
# BigQueryReader, BigQueryWriter.


class BigQueryReader(iobase.NativeSourceReader):
  """A reader for a BigQuery source."""

  def __init__(self, source, test_bigquery_client=None):
    self.source = source
    self.test_bigquery_client = test_bigquery_client
    if auth.is_running_in_gce:
      self.executing_project = auth.executing_project
    elif hasattr(source, 'pipeline_options'):
      self.executing_project = (
          source.pipeline_options.view_as(GoogleCloudOptions).project)
    else:
      self.executing_project = None

    # TODO(silviuc): Try to automatically get it from gcloud config info.
    if not self.executing_project and test_bigquery_client is None:
      raise RuntimeError(
          'Missing executing project information. Please use the --project '
          'command line option to specify it.')
    self.row_as_dict = isinstance(self.source.coder, RowAsDictJsonCoder)
    # Schema for the rows being read by the reader. It is initialized the
    # first time something gets read from the table. It is not required
    # for reading the field values in each row but could be useful for
    # getting additional details.
    self.schema = None
    if self.source.query is None:
      # If table schema did not define a project we default to executing
      # project.
      project_id = self.source.table_reference.projectId
      if not project_id:
        project_id = self.executing_project
      self.query = 'SELECT * FROM [%s:%s.%s];' % (
          project_id,
          self.source.table_reference.datasetId,
          self.source.table_reference.tableId)
    else:
      self.query = self.source.query

  def __enter__(self):
    self.client = BigQueryWrapper(client=self.test_bigquery_client)
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def __iter__(self):
    for rows, schema in self.client.run_query(
        project_id=self.executing_project, query=self.query):
      if self.schema is None:
        self.schema = schema
      for row in rows:
        if self.row_as_dict:
          yield self.client.convert_row_to_dict(row, schema)
        else:
          yield row


class BigQueryWriter(iobase.NativeSinkWriter):
  """The sink writer for a BigQuerySink."""

  def __init__(self, sink, test_bigquery_client=None, buffer_size=None):
    self.sink = sink
    self.test_bigquery_client = test_bigquery_client
    self.row_as_dict = isinstance(self.sink.coder, RowAsDictJsonCoder)
    # Buffer used to batch written rows so we reduce communication with the
    # BigQuery service.
    self.rows_buffer = []
    self.rows_buffer_flush_threshold = buffer_size or 1000
    # Figure out the project, dataset, and table used for the sink.
    self.project_id = self.sink.table_reference.projectId

    # If table schema did not define a project we default to executing project.
    if self.project_id is None and hasattr(sink, 'pipeline_options'):
      self.project_id = (
          sink.pipeline_options.view_as(GoogleCloudOptions).project)

    assert self.project_id is not None

    self.dataset_id = self.sink.table_reference.datasetId
    self.table_id = self.sink.table_reference.tableId

  def _flush_rows_buffer(self):
    if self.rows_buffer:
      logging.info('Writing %d rows to %s:%s.%s table.', len(self.rows_buffer),
                   self.project_id, self.dataset_id, self.table_id)
      passed, errors = self.client.insert_rows(
          project_id=self.project_id, dataset_id=self.dataset_id,
          table_id=self.table_id, rows=self.rows_buffer)
      self.rows_buffer = []
      if not passed:
        raise RuntimeError('Could not successfully insert rows to BigQuery'
                           ' table [%s:%s.%s]. Errors: %s'%
                           (self.project_id, self.dataset_id,
                            self.table_id, errors))

  def __enter__(self):
    self.client = BigQueryWrapper(client=self.test_bigquery_client)
    self.client.get_or_create_table(
        self.project_id, self.dataset_id, self.table_id, self.sink.table_schema,
        self.sink.create_disposition, self.sink.write_disposition)
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._flush_rows_buffer()

  def Write(self, row):
    self.rows_buffer.append(row)
    if len(self.rows_buffer) > self.rows_buffer_flush_threshold:
      self._flush_rows_buffer()


# -----------------------------------------------------------------------------
# BigQueryWrapper.


class BigQueryWrapper(object):
  """BigQuery client wrapper with utilities for querying.

  The wrapper is used to organize all the BigQuery integration points and
  offer a common place where retry logic for failures can be controlled.
  In addition it offers various functions used both in sources and sinks
  (e.g., find and create tables, query a table, etc.).
  """

  def __init__(self, client=None):
    self.client = client or bigquery.BigqueryV2(
        credentials=auth.get_service_credentials())
    self._unique_row_id = 0
    # For testing scenarios where we pass in a client we do not want a
    # randomized prefix for row IDs.
    self._row_id_prefix = '' if client else uuid.uuid4()

  @property
  def unique_row_id(self):
    """Returns a unique row ID (str) used to avoid multiple insertions.

    If the row ID is provided, BigQuery will make a best effort to not insert
    the same row multiple times for fail and retry scenarios in which the insert
    request may be issued several times. This comes into play for sinks executed
    in a local runner.

    Returns:
      a unique row ID string
    """
    self._unique_row_id += 1
    return '%s_%d' % (self._row_id_prefix, self._unique_row_id)

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _start_query_job(self, project_id, query, dry_run=False):
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                dryRun=dry_run,
                query=bigquery.JobConfigurationQuery(
                    query=query))))
    response = self.client.jobs.Insert(request)
    return response.jobReference.jobId

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _get_query_results(self, project_id, job_id,
                         page_token=None, max_results=10000):
    request = bigquery.BigqueryJobsGetQueryResultsRequest(
        jobId=job_id, pageToken=page_token, projectId=project_id,
        maxResults=max_results)
    response = self.client.jobs.GetQueryResults(request)
    return response

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _insert_all_rows(self, project_id, dataset_id, table_id, rows):
    # The rows argument is a list of
    # bigquery.TableDataInsertAllRequest.RowsValueListEntry instances as
    # required bu the InsertAll() method.
    request = bigquery.BigqueryTabledataInsertAllRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id,
        tableDataInsertAllRequest=bigquery.TableDataInsertAllRequest(
            # TODO(silviuc): Should have an option for skipInvalidRows?
            # TODO(silviuc): Should have an option for ignoreUnknownValues?
            rows=rows))
    response = self.client.tabledata.InsertAll(request)
    # response.insertErrors is not [] if errors encountered.
    return not response.insertErrors, response.insertErrors

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _get_table(self, project_id, dataset_id, table_id):
    request = bigquery.BigqueryTablesGetRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id)
    response = self.client.tables.Get(request)
    # The response is a bigquery.Table instance.
    return response

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _create_table(self, project_id, dataset_id, table_id, schema):
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=project_id, datasetId=dataset_id, tableId=table_id),
        schema=schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=project_id, datasetId=dataset_id, table=table)
    response = self.client.tables.Insert(request)
    # The response is a bigquery.Table instance.
    return response

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _is_table_empty(self, project_id, dataset_id, table_id):
    request = bigquery.BigqueryTabledataListRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id,
        maxResults=1)
    response = self.client.tabledata.List(request)
    # The response is a bigquery.TableDataList instance.
    return response.totalRows == 0

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def _delete_table(self, project_id, dataset_id, table_id):
    request = bigquery.BigqueryTablesDeleteRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id)
    self.client.tables.Delete(request)

  def get_or_create_table(
      self, project_id, dataset_id, table_id, schema,
      create_disposition, write_disposition):
    """Gets or creates a table based on create and write dispositions.

    The function mimics the behavior of BigQuery import jobs when using the
    same create and write dispositions.

    Args:
      project_id: The project id owning the table.
      dataset_id: The dataset id owning the table.
      table_id: The table id.
      schema: A bigquery.TableSchema instance or None.
      create_disposition: CREATE_NEVER or CREATE_IF_NEEDED.
      write_disposition: WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.

    Returns:
      A bigquery.Table instance if table was found or created.

    Raises:
      RuntimeError: For various mismatches between the state of the table and
        the create/write dispositions passed in. For example if the table is not
        empty and WRITE_EMPTY was specified then an error will be raised since
        the table was expected to be empty.
    """
    found_table = None
    try:
      found_table = self._get_table(project_id, dataset_id, table_id)
    except HttpError as exn:
      if exn.status_code == 404:
        if create_disposition == BigQueryDisposition.CREATE_NEVER:
          raise RuntimeError(
              'Table %s:%s.%s not found but create disposition is CREATE_NEVER.'
              % (project_id, dataset_id, table_id))
      else:
        raise

    # If table exists already then handle the semantics for WRITE_EMPTY and
    # WRITE_TRUNCATE write dispositions.
    if found_table:
      table_empty = self._is_table_empty(project_id, dataset_id, table_id)
      if (not table_empty and
          write_disposition == BigQueryDisposition.WRITE_EMPTY):
        raise RuntimeError(
            'Table %s:%s.%s is not empty but write disposition is WRITE_EMPTY.'
            % (project_id, dataset_id, table_id))
      # Delete the table and recreate it (later) if WRITE_TRUNCATE was
      # specified.
      if write_disposition == BigQueryDisposition.WRITE_TRUNCATE:
        self._delete_table(project_id, dataset_id, table_id)

    # Create a new table potentially reusing the schema from a previously
    # found table in case the schema was not specified.
    if schema is None and found_table is None:
      raise RuntimeError(
          'Table %s:%s.%s requires a schema. None can be inferred because the '
          'table does not exist.'
          % (project_id, dataset_id, table_id))
    if found_table and write_disposition != BigQueryDisposition.WRITE_TRUNCATE:
      return found_table
    else:
      # if write_disposition == BigQueryDisposition.WRITE_TRUNCATE we delete
      # the table before this point.
      return self._create_table(project_id=project_id,
                                dataset_id=dataset_id,
                                table_id=table_id,
                                schema=schema or found_table.schema)

  def run_query(self, project_id, query, dry_run=False):
    job_id = self._start_query_job(project_id, query, dry_run)
    if dry_run:
      # If this was a dry run then the fact that we get here means the
      # query has no errors. The start_query_job would raise an error otherwise.
      return
    page_token = None
    while True:
      response = self._get_query_results(project_id, job_id, page_token)
      if not response.jobComplete:
        # The jobComplete field can be False if the query request times out
        # (default is 10 seconds). Note that this is a timeout for the query
        # request not for the actual execution of the query in the service.  If
        # the request times out we keep trying. This situation is quite possible
        # if the query will return a large number of rows.
        logging.info('Waiting on response from query: %s ...', query)
        time.sleep(1.0)
        continue
      # We got some results. The last page is signalled by a missing pageToken.
      yield response.rows, response.schema
      if not response.pageToken:
        break
      page_token = response.pageToken

  def insert_rows(self, project_id, dataset_id, table_id, rows):
    """Inserts rows into the specified table.

    Args:
      project_id: The project id owning the table.
      dataset_id: The dataset id owning the table.
      table_id: The table id.
      rows: A list of plain Python dictionaries. Each dictionary is a row and
        each key in it is the name of a field.

    Returns:
      A tuple (bool, errors). If first element is False then the second element
      will be a bigquery.InserttErrorsValueListEntry instance containing
      specific errors.
    """

    # Prepare rows for insertion. Of special note is the row ID that we add to
    # each row in order to help BigQuery avoid inserting a row multiple times.
    # BigQuery will do a best-effort if unique IDs are provided. This situation
    # can happen during retries on failures.
    # TODO(silviuc): Must add support to writing TableRow's instead of dicts.
    final_rows = []
    for row in rows:
      json_object = bigquery.JsonObject()
      for k, v in row.iteritems():
        json_object.additionalProperties.append(
            bigquery.JsonObject.AdditionalProperty(
                key=k, value=to_json_value(v)))
      final_rows.append(
          bigquery.TableDataInsertAllRequest.RowsValueListEntry(
              insertId=str(self.unique_row_id),
              json=json_object))
    result, errors = self._insert_all_rows(
        project_id, dataset_id, table_id, final_rows)
    return result, errors

  def convert_row_to_dict(self, row, schema):
    """Converts a TableRow instance using the schema to a Python dict."""
    result = {}
    for index, field in enumerate(schema.fields):
      cell = row.f[index]
      if cell.v is None:
        continue  # Field not present in the row.
      # The JSON values returned by BigQuery for table fields in a row have
      # always set the string_value attribute, which means the value below will
      # be a string. Converting to the appropriate type is not tricky except
      # for boolean values. For such values the string values are 'true' or
      # 'false', which cannot be converted by simply calling bool() (it will
      # return True for both!).
      value = from_json_value(cell.v)
      if field.type == 'STRING':
        value = value
      elif field.type == 'BOOLEAN':
        value = value == 'true'
      elif field.type == 'INTEGER':
        value = int(value)
      elif field.type == 'FLOAT':
        value = float(value)
      elif field.type == 'TIMESTAMP':
        value = float(value)
      elif field.type == 'BYTES':
        value = value
      else:
        # Note that a schema field object supports also a RECORD type. However
        # when querying, the repeated and/or record fields always come
        # flattened.  For more details please read:
        # https://cloud.google.com/bigquery/docs/data
        raise RuntimeError('Unexpected field type: %s' % field.type)
      result[field.name] = value
    return result
