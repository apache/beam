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
import json
import logging
from builtins import object
from builtins import zip

from future.utils import itervalues
from past.builtins import unicode

from apache_beam import coders
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
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
  """A ``DoFn`` that streams writes to BigQuery once the table is created.
  """

  def __init__(self, table_id, dataset_id, project_id, batch_size, schema,
               create_disposition, write_disposition, kms_key, test_client):
    """Initialize a WriteToBigQuery transform.

    Args:
      table_id: The ID of the table. The ID must contain only letters
        (a-z, A-Z), numbers (0-9), or underscores (_). If dataset argument is
        None then the table argument must contain the entire table reference
        specified as: 'DATASET.TABLE' or 'PROJECT:DATASET.TABLE'.
      dataset_id: The ID of the dataset containing this table or null if the
        table reference is specified entirely by the table argument.
      project_id: The ID of the project containing this table or null if the
        table reference is specified entirely by the table argument.
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
    """
    self.table_id = table_id
    self.dataset_id = dataset_id
    self.project_id = project_id
    self.schema = schema
    self.test_client = test_client
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self._rows_buffer = []
    # The default batch size is 500
    self._max_batch_size = batch_size or 500
    self.kms_key = kms_key

  def display_data(self):
    return {'table_id': self.table_id,
            'dataset_id': self.dataset_id,
            'project_id': self.project_id,
            'schema': str(self.schema),
            'max_batch_size': self._max_batch_size}

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
    elif isinstance(schema, dict):
      return bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

  def start_bundle(self):
    self._rows_buffer = []
    self.table_schema = self.get_table_schema(self.schema)

    self.bigquery_wrapper = bigquery_tools.BigQueryWrapper(
        client=self.test_client)
    self.bigquery_wrapper.get_or_create_table(
        self.project_id, self.dataset_id, self.table_id, self.table_schema,
        self.create_disposition, self.write_disposition)

  def process(self, element, unused_create_fn_output=None):
    self._rows_buffer.append(element)
    if len(self._rows_buffer) >= self._max_batch_size:
      self._flush_batch()

  def finish_bundle(self):
    if self._rows_buffer:
      self._flush_batch()
    self._rows_buffer = []

  def _flush_batch(self):
    # Flush the current batch of rows to BigQuery.
    passed, errors = self.bigquery_wrapper.insert_rows(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=self._rows_buffer)
    if not passed:
      raise RuntimeError('Could not successfully insert rows to BigQuery'
                         ' table [%s:%s.%s]. Errors: %s'%
                         (self.project_id, self.dataset_id,
                          self.table_id, errors))
    logging.debug("Successfully wrote %d rows.", len(self._rows_buffer))
    self._rows_buffer = []


class WriteToBigQuery(PTransform):

  def __init__(self, table, dataset=None, project=None, schema=None,
               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=BigQueryDisposition.WRITE_APPEND,
               batch_size=None, kms_key=None, test_client=None):
    """Initialize a WriteToBigQuery transform.

    Args:
      table (str): The ID of the table. The ID must contain only letters
        ``a-z``, ``A-Z``, numbers ``0-9``, or underscores ``_``. If dataset
        argument is :data:`None` then the table argument must contain the
        entire table reference specified as: ``'DATASET.TABLE'`` or
        ``'PROJECT:DATASET.TABLE'``.
      dataset (str): The ID of the dataset containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      project (str): The ID of the project containing this table or
        :data:`None` if the table reference is specified entirely by the table
        argument.
      schema (str): The schema to be used if the BigQuery table to write has to
        be created. This can be either specified as a
        :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema`
        object or a single string  of the form
        ``'field1:type1,field2:type2,field3:type3'`` that defines a comma
        separated list of fields. Here ``'type'`` should specify the BigQuery
        type of the field. Single string based schemas do not support nested
        fields, repeated fields, or specifying a BigQuery mode for fields
        (mode will always be set to ``'NULLABLE'``).
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

      batch_size (int): Number of rows to be written to BQ per streaming API
        insert.
      kms_key (str): Experimental. Optional Cloud KMS key name for use when
        creating new tables.
      test_client: Override the default bigquery client used for testing.
    """
    self.table_reference = bigquery_tools.parse_table_reference(
        table, dataset, project)
    self.create_disposition = BigQueryDisposition.validate_create(
        create_disposition)
    self.write_disposition = BigQueryDisposition.validate_write(
        write_disposition)
    self.schema = schema
    self.batch_size = batch_size
    self.kms_key = kms_key
    self.test_client = test_client

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
    if isinstance(schema, dict):
      return schema
    elif schema is None:
      return schema
    elif isinstance(schema, (str, unicode)):
      table_schema = WriteToBigQuery.get_table_schema_from_string(schema)
      return WriteToBigQuery.table_schema_to_dict(table_schema)
    elif isinstance(schema, bigquery.TableSchema):
      return WriteToBigQuery.table_schema_to_dict(schema)
    else:
      raise TypeError('Unexpected schema argument: %s.' % schema)

  def expand(self, pcoll):
    if self.table_reference.projectId is None:
      self.table_reference.projectId = pcoll.pipeline.options.view_as(
          GoogleCloudOptions).project
    bigquery_write_fn = BigQueryWriteFn(
        table_id=self.table_reference.tableId,
        dataset_id=self.table_reference.datasetId,
        project_id=self.table_reference.projectId,
        batch_size=self.batch_size,
        schema=self.get_dict_table_schema(self.schema),
        create_disposition=self.create_disposition,
        write_disposition=self.write_disposition,
        kms_key=self.kms_key,
        test_client=self.test_client)
    return pcoll | 'WriteToBigQuery' >> ParDo(bigquery_write_fn)

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
