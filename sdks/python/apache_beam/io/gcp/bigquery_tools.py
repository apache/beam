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

"""Tools used by BigQuery sources and sinks.

Classes, constants and functions in this file are experimental and have no
backwards compatibility guarantees.

These tools include wrappers and clients to interact with BigQuery APIs.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

from __future__ import absolute_import

import datetime
import decimal
import json
import logging
import re
import sys
import time
import uuid
from builtins import object

from future.utils import iteritems

from apache_beam import coders
from apache_beam.internal.gcp import auth
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.internal.http_client import get_new_http
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options import value_provider
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import DoFn
from apache_beam.utils import retry

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  pass


# pylint: enable=wrong-import-order, wrong-import-position


MAX_RETRIES = 3


JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'


def default_encoder(obj):
  if isinstance(obj, decimal.Decimal):
    return str(obj)
  raise TypeError(
      "Object of type '%s' is not JSON serializable" % type(obj).__name__)


def get_hashable_destination(destination):
  """Parses a table reference into a (project, dataset, table) tuple.

  Args:
    destination: Either a TableReference object from the bigquery API.
      The object has the following attributes: projectId, datasetId, and
      tableId. Or a string representing the destination containing
      'PROJECT:DATASET.TABLE'.
  Returns:
    A string representing the destination containing
    'PROJECT:DATASET.TABLE'.
  """
  if isinstance(destination, bigquery.TableReference):
    return '%s:%s.%s' % (
        destination.projectId, destination.datasetId, destination.tableId)
  else:
    return destination


def parse_table_schema_from_json(schema_string):
  """Parse the Table Schema provided as string.

  Args:
    schema_string: String serialized table schema, should be a valid JSON.

  Returns:
    A TableSchema of the BigQuery export from either the Query or the Table.
  """
  json_schema = json.loads(schema_string)

  def _parse_schema_field(field):
    """Parse a single schema field from dictionary.

    Args:
      field: Dictionary object containing serialized schema.

    Returns:
      A TableFieldSchema for a single column in BigQuery.
    """
    schema = bigquery.TableFieldSchema()
    schema.name = field['name']
    schema.type = field['type']
    if 'mode' in field:
      schema.mode = field['mode']
    else:
      schema.mode = 'NULLABLE'
    if 'description' in field:
      schema.description = field['description']
    if 'fields' in field:
      schema.fields = [_parse_schema_field(x) for x in field['fields']]
    return schema

  fields = [_parse_schema_field(f) for f in json_schema['fields']]
  return bigquery.TableSchema(fields=fields)


def parse_table_reference(table, dataset=None, project=None):
  """Parses a table reference into a (project, dataset, table) tuple.

  Args:
    table: The ID of the table. The ID must contain only letters
      (a-z, A-Z), numbers (0-9), or underscores (_). If dataset argument is None
      then the table argument must contain the entire table reference:
      'DATASET.TABLE' or 'PROJECT:DATASET.TABLE'. This argument can be a
      bigquery.TableReference instance in which case dataset and project are
      ignored and the reference is returned as a result.  Additionally, for date
      partitioned tables, appending '$YYYYmmdd' to the table name is supported,
      e.g. 'DATASET.TABLE$YYYYmmdd'.
    dataset: The ID of the dataset containing this table or null if the table
      reference is specified entirely by the table argument.
    project: The ID of the project containing this table or null if the table
      reference is specified entirely by the table (and possibly dataset)
      argument.

  Returns:
    A TableReference object from the bigquery API. The object has the following
    attributes: projectId, datasetId, and tableId.

  Raises:
    ValueError: if the table reference as a string does not match the expected
      format.
  """

  if isinstance(table, bigquery.TableReference):
    return table
  elif callable(table):
    return table
  elif isinstance(table, value_provider.ValueProvider):
    return table

  table_reference = bigquery.TableReference()
  # If dataset argument is not specified, the expectation is that the
  # table argument will contain a full table reference instead of just a
  # table name.
  if dataset is None:
    match = re.match(
        r'^((?P<project>.+):)?(?P<dataset>\w+)\.(?P<table>[\w\$]+)$', table)
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
# BigQueryWrapper.


class BigQueryWrapper(object):
  """BigQuery client wrapper with utilities for querying.

  The wrapper is used to organize all the BigQuery integration points and
  offer a common place where retry logic for failures can be controlled.
  In addition it offers various functions used both in sources and sinks
  (e.g., find and create tables, query a table, etc.).
  """

  TEMP_TABLE = 'temp_table_'
  TEMP_DATASET = 'temp_dataset_'

  def __init__(self, client=None):
    self.client = client or bigquery.BigqueryV2(
        http=get_new_http(),
        credentials=auth.get_service_credentials(),
        response_encoding=None if sys.version_info[0] < 3 else 'utf8')
    self._unique_row_id = 0
    # For testing scenarios where we pass in a client we do not want a
    # randomized prefix for row IDs.
    self._row_id_prefix = '' if client else uuid.uuid4()
    self._temporary_table_suffix = uuid.uuid4().hex

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

  def _get_temp_table(self, project_id):
    return parse_table_reference(
        table=BigQueryWrapper.TEMP_TABLE + self._temporary_table_suffix,
        dataset=BigQueryWrapper.TEMP_DATASET + self._temporary_table_suffix,
        project=project_id)

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_query_location(self, project_id, query, use_legacy_sql):
    """
    Get the location of tables referenced in a query.

    This method returns the location of the first referenced table in the query
    and depends on the BigQuery service to provide error handling for
    queries that reference tables in multiple locations.
    """
    reference = bigquery.JobReference(jobId=uuid.uuid4().hex,
                                      projectId=project_id)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                dryRun=True,
                query=bigquery.JobConfigurationQuery(
                    query=query,
                    useLegacySql=use_legacy_sql,
                )),
            jobReference=reference))

    response = self.client.jobs.Insert(request)

    if response.statistics is None:
      # This behavior is only expected in tests
      logging.warning(
          "Unable to get location, missing response.statistics. Query: %s",
          query)
      return None

    referenced_tables = response.statistics.query.referencedTables
    if referenced_tables:  # Guards against both non-empty and non-None
      table = referenced_tables[0]
      location = self.get_table_location(
          table.projectId,
          table.datasetId,
          table.tableId)
      logging.info("Using location %r from table %r referenced by query %s",
                   location, table, query)
      return location

    logging.debug("Query %s does not reference any tables.", query)
    return None

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _insert_copy_job(self,
                       project_id,
                       job_id,
                       from_table_reference,
                       to_table_reference,
                       create_disposition=None,
                       write_disposition=None):
    reference = bigquery.JobReference()
    reference.jobId = job_id
    reference.projectId = project_id
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                copy=bigquery.JobConfigurationTableCopy(
                    destinationTable=to_table_reference,
                    sourceTable=from_table_reference,
                    createDisposition=create_disposition,
                    writeDisposition=write_disposition,
                )
            ),
            jobReference=reference,
        )
    )

    logging.info("Inserting job request: %s", request)
    response = self.client.jobs.Insert(request)
    logging.info("Response was %s", response)
    return response.jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _insert_load_job(self,
                       project_id,
                       job_id,
                       table_reference,
                       source_uris,
                       schema=None,
                       write_disposition=None,
                       create_disposition=None):
    reference = bigquery.JobReference(jobId=job_id, projectId=project_id)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                load=bigquery.JobConfigurationLoad(
                    sourceUris=source_uris,
                    destinationTable=table_reference,
                    schema=schema,
                    writeDisposition=write_disposition,
                    createDisposition=create_disposition,
                    sourceFormat='NEWLINE_DELIMITED_JSON',
                    autodetect=schema is None,
                )
            ),
            jobReference=reference,
        )
    )
    response = self.client.jobs.Insert(request)
    return response.jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _start_query_job(self, project_id, query, use_legacy_sql, flatten_results,
                       job_id, dry_run=False):
    reference = bigquery.JobReference(jobId=job_id, projectId=project_id)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                dryRun=dry_run,
                query=bigquery.JobConfigurationQuery(
                    query=query,
                    useLegacySql=use_legacy_sql,
                    allowLargeResults=True,
                    destinationTable=self._get_temp_table(project_id),
                    flattenResults=flatten_results)),
            jobReference=reference))

    response = self.client.jobs.Insert(request)
    return response.jobReference.jobId

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_query_results(self, project_id, job_id,
                         page_token=None, max_results=10000):
    request = bigquery.BigqueryJobsGetQueryResultsRequest(
        jobId=job_id, pageToken=page_token, projectId=project_id,
        maxResults=max_results)
    response = self.client.jobs.GetQueryResults(request)
    return response

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_timeout_or_quota_issues_filter)
  def _insert_all_rows(self, project_id, dataset_id, table_id, rows,
                       skip_invalid_rows=False):
    """Calls the insertAll BigQuery API endpoint.

    Docs for this BQ call: https://cloud.google.com/bigquery/docs/reference\
      /rest/v2/tabledata/insertAll."""
    # The rows argument is a list of
    # bigquery.TableDataInsertAllRequest.RowsValueListEntry instances as
    # required by the InsertAll() method.
    request = bigquery.BigqueryTabledataInsertAllRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id,
        tableDataInsertAllRequest=bigquery.TableDataInsertAllRequest(
            skipInvalidRows=skip_invalid_rows,
            # TODO(silviuc): Should have an option for ignoreUnknownValues?
            rows=rows))
    response = self.client.tabledata.InsertAll(request)
    # response.insertErrors is not [] if errors encountered.
    return not response.insertErrors, response.insertErrors

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_table(self, project_id, dataset_id, table_id):
    """Lookup a table's metadata object.

    Args:
      client: bigquery.BigqueryV2 instance
      project_id, dataset_id, table_id: table lookup parameters

    Returns:
      bigquery.Table instance
    Raises:
      HttpError if lookup failed.
    """
    request = bigquery.BigqueryTablesGetRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id)
    response = self.client.tables.Get(request)
    return response

  def _create_table(self, project_id, dataset_id, table_id, schema):
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=project_id, datasetId=dataset_id, tableId=table_id),
        schema=schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=project_id, datasetId=dataset_id, table=table)
    response = self.client.tables.Insert(request)
    logging.debug("Created the table with id %s", table_id)
    # The response is a bigquery.Table instance.
    return response

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_or_create_dataset(self, project_id, dataset_id, location=None):
    # Check if dataset already exists otherwise create it
    try:
      dataset = self.client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
          projectId=project_id, datasetId=dataset_id))
      return dataset
    except HttpError as exn:
      if exn.status_code == 404:
        dataset_reference = bigquery.DatasetReference(
            projectId=project_id, datasetId=dataset_id)
        dataset = bigquery.Dataset(datasetReference=dataset_reference)
        if location is not None:
          dataset.location = location
        request = bigquery.BigqueryDatasetsInsertRequest(
            projectId=project_id, dataset=dataset)
        response = self.client.datasets.Insert(request)
        # The response is a bigquery.Dataset instance.
        return response
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _is_table_empty(self, project_id, dataset_id, table_id):
    request = bigquery.BigqueryTabledataListRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id,
        maxResults=1)
    response = self.client.tabledata.List(request)
    # The response is a bigquery.TableDataList instance.
    return response.totalRows == 0

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _delete_table(self, project_id, dataset_id, table_id):
    request = bigquery.BigqueryTablesDeleteRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id)
    try:
      self.client.tables.Delete(request)
    except HttpError as exn:
      if exn.status_code == 404:
        logging.warning('Table %s:%s.%s does not exist', project_id,
                        dataset_id, table_id)
        return
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _delete_dataset(self, project_id, dataset_id, delete_contents=True):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=project_id, datasetId=dataset_id,
        deleteContents=delete_contents)
    try:
      self.client.datasets.Delete(request)
    except HttpError as exn:
      if exn.status_code == 404:
        logging.warning('Dataset %s:%s does not exist', project_id,
                        dataset_id)
        return
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_table_location(self, project_id, dataset_id, table_id):
    table = self.get_table(project_id, dataset_id, table_id)
    return table.location

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def create_temporary_dataset(self, project_id, location):
    dataset_id = BigQueryWrapper.TEMP_DATASET + self._temporary_table_suffix
    # Check if dataset exists to make sure that the temporary id is unique
    try:
      self.client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
          projectId=project_id, datasetId=dataset_id))
      if project_id is not None:
        # Unittests don't pass projectIds so they can be run without error
        raise RuntimeError(
            'Dataset %s:%s already exists so cannot be used as temporary.'
            % (project_id, dataset_id))
    except HttpError as exn:
      if exn.status_code == 404:
        logging.warning(
            'Dataset %s:%s does not exist so we will create it as temporary '
            'with location=%s',
            project_id, dataset_id, location)
        self.get_or_create_dataset(project_id, dataset_id, location=location)
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def clean_up_temporary_dataset(self, project_id):
    temp_table = self._get_temp_table(project_id)
    try:
      self.client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
          projectId=project_id, datasetId=temp_table.datasetId))
    except HttpError as exn:
      if exn.status_code == 404:
        logging.warning('Dataset %s:%s does not exist', project_id,
                        temp_table.datasetId)
        return
      else:
        raise
    self._delete_dataset(temp_table.projectId, temp_table.datasetId, True)

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_job(self, project, job_id, location=None):
    request = bigquery.BigqueryJobsGetRequest()
    request.jobId = job_id
    request.projectId = project
    request.location = location

    return self.client.jobs.Get(request)

  def perform_load_job(self,
                       destination,
                       files,
                       job_id,
                       schema=None,
                       write_disposition=None,
                       create_disposition=None):
    """Starts a job to load data into BigQuery.

    Returns:
      bigquery.JobReference with the information about the job that was started.
    """
    return self._insert_load_job(
        destination.projectId, job_id, destination, files,
        schema=schema,
        create_disposition=create_disposition,
        write_disposition=write_disposition)

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
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
    from apache_beam.io.gcp.bigquery import BigQueryDisposition

    found_table = None
    try:
      found_table = self.get_table(project_id, dataset_id, table_id)
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
      created_table = self._create_table(project_id=project_id,
                                         dataset_id=dataset_id,
                                         table_id=table_id,
                                         schema=schema or found_table.schema)
      logging.info('Created table %s.%s.%s with schema %s. Result: %s.',
                   project_id, dataset_id, table_id,
                   schema or found_table.schema,
                   created_table)
      # if write_disposition == BigQueryDisposition.WRITE_TRUNCATE we delete
      # the table before this point.
      if write_disposition == BigQueryDisposition.WRITE_TRUNCATE:
        # BigQuery can route data to the old table for 2 mins max so wait
        # that much time before creating the table and writing it
        logging.warning('Sleeping for 150 seconds before the write as ' +
                        'BigQuery inserts can be routed to deleted table ' +
                        'for 2 mins after the delete and create.')
        # TODO(BEAM-2673): Remove this sleep by migrating to load api
        time.sleep(150)
        return created_table
      else:
        return created_table

  def run_query(self, project_id, query, use_legacy_sql, flatten_results,
                dry_run=False):
    job_id = self._start_query_job(project_id, query, use_legacy_sql,
                                   flatten_results, job_id=uuid.uuid4().hex,
                                   dry_run=dry_run)
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

  def insert_rows(self, project_id, dataset_id, table_id, rows,
                  skip_invalid_rows=False):
    """Inserts rows into the specified table.

    Args:
      project_id: The project id owning the table.
      dataset_id: The dataset id owning the table.
      table_id: The table id.
      rows: A list of plain Python dictionaries. Each dictionary is a row and
        each key in it is the name of a field.
      skip_invalid_rows: If there are rows with insertion errors, whether they
        should be skipped, and all others should be inserted successfully.

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
      for k, v in iteritems(row):
        if isinstance(v, decimal.Decimal):
          # decimal values are converted into string because JSON does not
          # support the precision that decimal supports. BQ is able to handle
          # inserts into NUMERIC columns by receiving JSON with string attrs.
          v = str(v)
        json_object.additionalProperties.append(
            bigquery.JsonObject.AdditionalProperty(
                key=k, value=to_json_value(v)))
      final_rows.append(
          bigquery.TableDataInsertAllRequest.RowsValueListEntry(
              insertId=str(self.unique_row_id),
              json=json_object))
    result, errors = self._insert_all_rows(
        project_id, dataset_id, table_id, final_rows, skip_invalid_rows)
    return result, errors

  def _convert_cell_value_to_dict(self, value, field):
    if field.type == 'STRING':
      # Input: "XYZ" --> Output: "XYZ"
      return value
    elif field.type == 'BOOLEAN':
      # Input: "true" --> Output: True
      return value == 'true'
    elif field.type == 'INTEGER':
      # Input: "123" --> Output: 123
      return int(value)
    elif field.type == 'FLOAT':
      # Input: "1.23" --> Output: 1.23
      return float(value)
    elif field.type == 'TIMESTAMP':
      # The UTC should come from the timezone library but this is a known
      # issue in python 2.7 so we'll just hardcode it as we're reading using
      # utcfromtimestamp.
      # Input: 1478134176.985864 --> Output: "2016-11-03 00:49:36.985864 UTC"
      dt = datetime.datetime.utcfromtimestamp(float(value))
      return dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
    elif field.type == 'BYTES':
      # Input: "YmJi" --> Output: "YmJi"
      return value
    elif field.type == 'DATE':
      # Input: "2016-11-03" --> Output: "2016-11-03"
      return value
    elif field.type == 'DATETIME':
      # Input: "2016-11-03T00:49:36" --> Output: "2016-11-03T00:49:36"
      return value
    elif field.type == 'TIME':
      # Input: "00:49:36" --> Output: "00:49:36"
      return value
    elif field.type == 'RECORD':
      # Note that a schema field object supports also a RECORD type. However
      # when querying, the repeated and/or record fields are flattened
      # unless we pass the flatten_results flag as False to the source
      return self.convert_row_to_dict(value, field)
    elif field.type == 'NUMERIC':
      return decimal.Decimal(value)
    elif field.type == 'GEOGRAPHY':
      return value
    else:
      raise RuntimeError('Unexpected field type: %s' % field.type)

  def convert_row_to_dict(self, row, schema):
    """Converts a TableRow instance using the schema to a Python dict."""
    result = {}
    for index, field in enumerate(schema.fields):
      value = None
      if isinstance(schema, bigquery.TableSchema):
        cell = row.f[index]
        value = from_json_value(cell.v) if cell.v is not None else None
      elif isinstance(schema, bigquery.TableFieldSchema):
        cell = row['f'][index]
        value = cell['v'] if 'v' in cell else None
      if field.mode == 'REPEATED':
        if value is None:
          # Ideally this should never happen as repeated fields default to
          # returning an empty list
          result[field.name] = []
        else:
          result[field.name] = [self._convert_cell_value_to_dict(x['v'], field)
                                for x in value]
      elif value is None:
        if not field.mode == 'NULLABLE':
          raise ValueError('Received \'None\' as the value for the field %s '
                           'but the field is not NULLABLE.' % field.name)
        result[field.name] = None
      else:
        result[field.name] = self._convert_cell_value_to_dict(value, field)
    return result


# -----------------------------------------------------------------------------
# BigQueryReader, BigQueryWriter.


class BigQueryReader(dataflow_io.NativeSourceReader):
  """A reader for a BigQuery source."""

  def __init__(self, source, test_bigquery_client=None, use_legacy_sql=True,
               flatten_results=True, kms_key=None):
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
    self.use_legacy_sql = use_legacy_sql
    self.flatten_results = flatten_results
    self.kms_key = kms_key

    if self.source.table_reference is not None:
      # If table schema did not define a project we default to executing
      # project.
      project_id = self.source.table_reference.projectId
      if not project_id:
        project_id = self.executing_project
      self.query = 'SELECT * FROM [%s:%s.%s];' % (
          project_id,
          self.source.table_reference.datasetId,
          self.source.table_reference.tableId)
    elif self.source.query is not None:
      self.query = self.source.query
    else:
      # Enforce the "modes" enforced by BigQuerySource.__init__.
      # If this exception has been raised, the BigQuerySource "modes" have
      # changed and this method will need to be updated as well.
      raise ValueError("BigQuerySource must have either a table or query")

  def _get_source_location(self):
    """
    Get the source location (e.g. ``"EU"`` or ``"US"``) from either

    - :data:`source.table_reference`
      or
    - The first referenced table in :data:`source.query`

    See Also:
      - :meth:`BigQueryWrapper.get_query_location`
      - :meth:`BigQueryWrapper.get_table_location`

    Returns:
      Optional[str]: The source location, if any.
    """
    if self.source.table_reference is not None:
      tr = self.source.table_reference
      return self.client.get_table_location(
          tr.projectId if tr.projectId is not None else self.executing_project,
          tr.datasetId, tr.tableId)
    else:  # It's a query source
      return self.client.get_query_location(
          self.executing_project,
          self.source.query,
          self.source.use_legacy_sql)

  def __enter__(self):
    self.client = BigQueryWrapper(client=self.test_bigquery_client)
    self.client.create_temporary_dataset(
        self.executing_project, location=self._get_source_location())
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.client.clean_up_temporary_dataset(self.executing_project)

  def __iter__(self):
    for rows, schema in self.client.run_query(
        project_id=self.executing_project, query=self.query,
        use_legacy_sql=self.use_legacy_sql,
        flatten_results=self.flatten_results):
      if self.schema is None:
        self.schema = schema
      for row in rows:
        if self.row_as_dict:
          yield self.client.convert_row_to_dict(row, schema)
        else:
          yield row


class BigQueryWriter(dataflow_io.NativeSinkWriter):
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
                           ' table [%s:%s.%s]. Errors: %s' %
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
      return json.dumps(
          table_row, allow_nan=False, default=default_encoder).encode('utf-8')
    except ValueError as e:
      raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

  def decode(self, encoded_table_row):
    return json.loads(encoded_table_row.decode('utf-8'))


class RetryStrategy(object):
  RETRY_ALWAYS = 'RETRY_ALWAYS'
  RETRY_NEVER = 'RETRY_NEVER'
  RETRY_ON_TRANSIENT_ERROR = 'RETRY_ON_TRANSIENT_ERROR'

  _NON_TRANSIENT_ERRORS = {'invalid', 'invalidQuery', 'notImplemented'}

  @staticmethod
  def should_retry(strategy, error_message):
    if strategy == RetryStrategy.RETRY_ALWAYS:
      return True
    elif strategy == RetryStrategy.RETRY_NEVER:
      return False
    elif (strategy == RetryStrategy.RETRY_ON_TRANSIENT_ERROR and
          error_message not in RetryStrategy._NON_TRANSIENT_ERRORS):
      return True
    else:
      return False


class AppendDestinationsFn(DoFn):
  """Adds the destination to an element, making it a KV pair.

  Outputs a PCollection of KV-pairs where the key is a TableReference for the
  destination, and the value is the record itself.

  Experimental; no backwards compatibility guarantees.
  """

  def __init__(self, destination):
    self.destination = AppendDestinationsFn._get_table_fn(destination)

  @staticmethod
  def _value_provider_or_static_val(elm):
    if isinstance(elm, value_provider.ValueProvider):
      return elm
    else:
      # The type argument is a NoOp, because we assume the argument already has
      # the proper formatting.
      return value_provider.StaticValueProvider(lambda x: x, value=elm)

  @staticmethod
  def _get_table_fn(destination):
    if callable(destination):
      return destination
    else:
      return lambda x: AppendDestinationsFn._value_provider_or_static_val(
          destination).get()

  def process(self, element):
    yield (self.destination(element), element)
