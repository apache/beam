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

# pytype: skip-file

from __future__ import absolute_import

import datetime
import decimal
import io
import json
import logging
import re
import sys
import time
import uuid
from builtins import object

import fastavro
from future.utils import iteritems
from future.utils import raise_with_traceback
from past.builtins import unicode

from apache_beam import coders
from apache_beam.internal.gcp import auth
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.internal.http_client import get_new_http
from apache_beam.io.gcp import bigquery_avro_tools
from apache_beam.io.gcp.bigquery_io_metadata import create_bigquery_io_metadata
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options import value_provider
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import DoFn
from apache_beam.typehints.typehints import Any
from apache_beam.utils import retry

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError, HttpForbiddenError
except ImportError:
  pass

try:
  # TODO(pabloem): Remove this workaround after Python 2.7 support ends.
  from json.decoder import JSONDecodeError
except ImportError:
  JSONDecodeError = ValueError

# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 3

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'


class FileFormat(object):
  CSV = 'CSV'
  JSON = 'NEWLINE_DELIMITED_JSON'
  AVRO = 'AVRO'


class ExportCompression(object):
  GZIP = 'GZIP'
  DEFLATE = 'DEFLATE'
  SNAPPY = 'SNAPPY'
  NONE = 'NONE'


def default_encoder(obj):
  if isinstance(obj, decimal.Decimal):
    return str(obj)
  elif isinstance(obj, bytes):
    # on python 3 base64-encoded bytes are decoded to strings
    # before being sent to BigQuery
    return obj.decode('utf-8')
  elif isinstance(obj, (datetime.date, datetime.time)):
    return str(obj)
  elif isinstance(obj, datetime.datetime):
    return obj.isoformat()

  _LOGGER.error("Unable to serialize %r to JSON", obj)
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
  try:
    json_schema = json.loads(schema_string)
  except JSONDecodeError as e:
    raise ValueError(
        'Unable to parse JSON schema: %s - %r' % (schema_string, e))

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
    If the input is a TableReference object, a new object will be returned.

  Raises:
    ValueError: if the table reference as a string does not match the expected
      format.
  """

  if isinstance(table, bigquery.TableReference):
    return bigquery.TableReference(
        projectId=table.projectId,
        datasetId=table.datasetId,
        tableId=table.tableId)
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


def _build_job_labels(input_labels):
  """Builds job label protobuf structure."""
  input_labels = input_labels or {}
  result = bigquery.JobConfiguration.LabelsValue()

  for k, v in input_labels.items():
    result.additionalProperties.append(
        bigquery.JobConfiguration.LabelsValue.AdditionalProperty(
            key=k,
            value=v,
        ))
  return result


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

    This method returns the location of the first available referenced
    table for user in the query and depends on the BigQuery service to
    provide error handling for queries that reference tables in multiple
    locations.
    """
    reference = bigquery.JobReference(
        jobId=uuid.uuid4().hex, projectId=project_id)
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
      _LOGGER.warning(
          "Unable to get location, missing response.statistics. Query: %s",
          query)
      return None

    referenced_tables = response.statistics.query.referencedTables
    if referenced_tables:  # Guards against both non-empty and non-None
      for table in referenced_tables:
        try:
          location = self.get_table_location(
              table.projectId, table.datasetId, table.tableId)
        except HttpForbiddenError:
          # Permission access for table (i.e. from authorized_view),
          # try next one
          continue
        _LOGGER.info(
            "Using location %r from table %r referenced by query %s",
            location,
            table,
            query)
        return location

    _LOGGER.debug(
        "Query %s does not reference any tables or "
        "you don't have permission to inspect them.",
        query)
    return None

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _insert_copy_job(
      self,
      project_id,
      job_id,
      from_table_reference,
      to_table_reference,
      create_disposition=None,
      write_disposition=None,
      job_labels=None):
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
                ),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=reference,
        ))

    _LOGGER.info("Inserting job request: %s", request)
    response = self.client.jobs.Insert(request)
    _LOGGER.info("Response was %s", response)
    return response.jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _insert_load_job(
      self,
      project_id,
      job_id,
      table_reference,
      source_uris,
      schema=None,
      write_disposition=None,
      create_disposition=None,
      additional_load_parameters=None,
      source_format=None,
      job_labels=None):
    additional_load_parameters = additional_load_parameters or {}
    job_schema = None if schema == 'SCHEMA_AUTODETECT' else schema
    reference = bigquery.JobReference(jobId=job_id, projectId=project_id)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                load=bigquery.JobConfigurationLoad(
                    sourceUris=source_uris,
                    destinationTable=table_reference,
                    schema=job_schema,
                    writeDisposition=write_disposition,
                    createDisposition=create_disposition,
                    sourceFormat=source_format,
                    useAvroLogicalTypes=True,
                    autodetect=schema == 'SCHEMA_AUTODETECT',
                    **additional_load_parameters),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=reference,
        ))
    response = self.client.jobs.Insert(request)
    return response.jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _start_query_job(
      self,
      project_id,
      query,
      use_legacy_sql,
      flatten_results,
      job_id,
      dry_run=False,
      kms_key=None,
      job_labels=None):
    reference = bigquery.JobReference(jobId=job_id, projectId=project_id)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                dryRun=dry_run,
                query=bigquery.JobConfigurationQuery(
                    query=query,
                    useLegacySql=use_legacy_sql,
                    allowLargeResults=not dry_run,
                    destinationTable=self._get_temp_table(project_id)
                    if not dry_run else None,
                    flattenResults=flatten_results,
                    destinationEncryptionConfiguration=bigquery.
                    EncryptionConfiguration(kmsKeyName=kms_key)),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=reference))

    response = self.client.jobs.Insert(request)
    return response

  def wait_for_bq_job(self, job_reference, sleep_duration_sec=5, max_retries=0):
    """Poll job until it is DONE.

    Args:
      job_reference: bigquery.JobReference instance.
      sleep_duration_sec: Specifies the delay in seconds between retries.
      max_retries: The total number of times to retry. If equals to 0,
        the function waits forever.

    Raises:
      `RuntimeError`: If the job is FAILED or the number of retries has been
        reached.
    """
    retry = 0
    while True:
      retry += 1
      job = self.get_job(
          job_reference.projectId, job_reference.jobId, job_reference.location)
      logging.info('Job status: %s', job.status.state)
      if job.status.state == 'DONE' and job.status.errorResult:
        raise RuntimeError(
            'BigQuery job {} failed. Error Result: {}'.format(
                job_reference.jobId, job.status.errorResult))
      elif job.status.state == 'DONE':
        return True
      else:
        time.sleep(sleep_duration_sec)
        if max_retries != 0 and retry >= max_retries:
          raise RuntimeError('The maximum number of retries has been reached')

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_query_results(
      self,
      project_id,
      job_id,
      page_token=None,
      max_results=10000,
      location=None):
    request = bigquery.BigqueryJobsGetQueryResultsRequest(
        jobId=job_id,
        pageToken=page_token,
        projectId=project_id,
        maxResults=max_results,
        location=location)
    response = self.client.jobs.GetQueryResults(request)
    return response

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_timeout_or_quota_issues_filter)
  def _insert_all_rows(
      self,
      project_id,
      dataset_id,
      table_id,
      rows,
      skip_invalid_rows=False,
      latency_recoder=None):
    """Calls the insertAll BigQuery API endpoint.

    Docs for this BQ call: https://cloud.google.com/bigquery/docs/reference\
      /rest/v2/tabledata/insertAll."""
    # The rows argument is a list of
    # bigquery.TableDataInsertAllRequest.RowsValueListEntry instances as
    # required by the InsertAll() method.
    request = bigquery.BigqueryTabledataInsertAllRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id,
        tableDataInsertAllRequest=bigquery.TableDataInsertAllRequest(
            skipInvalidRows=skip_invalid_rows,
            # TODO(silviuc): Should have an option for ignoreUnknownValues?
            rows=rows))
    started_millis = int(time.time() * 1000) if latency_recoder else None
    try:
      response = self.client.tabledata.InsertAll(request)
      # response.insertErrors is not [] if errors encountered.
    finally:
      if latency_recoder:
        latency_recoder.record(int(time.time() * 1000) - started_millis)
    return not response.insertErrors, response.insertErrors

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_table(self, project_id, dataset_id, table_id):
    """Lookup a table's metadata object.

    Args:
      client: bigquery.BigqueryV2 instance
      project_id: table lookup parameter
      dataset_id: table lookup parameter
      table_id: table lookup parameter

    Returns:
      bigquery.Table instance
    Raises:
      HttpError: if lookup failed.
    """
    request = bigquery.BigqueryTablesGetRequest(
        projectId=project_id, datasetId=dataset_id, tableId=table_id)
    response = self.client.tables.Get(request)
    return response

  def _create_table(
      self,
      project_id,
      dataset_id,
      table_id,
      schema,
      additional_parameters=None):
    additional_parameters = additional_parameters or {}
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=project_id, datasetId=dataset_id, tableId=table_id),
        schema=schema,
        **additional_parameters)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=project_id, datasetId=dataset_id, table=table)
    response = self.client.tables.Insert(request)
    _LOGGER.debug("Created the table with id %s", table_id)
    # The response is a bigquery.Table instance.
    return response

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_or_create_dataset(self, project_id, dataset_id, location=None):
    # Check if dataset already exists otherwise create it
    try:
      dataset = self.client.datasets.Get(
          bigquery.BigqueryDatasetsGetRequest(
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
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id,
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
        _LOGGER.warning(
            'Table %s:%s.%s does not exist', project_id, dataset_id, table_id)
        return
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _delete_dataset(self, project_id, dataset_id, delete_contents=True):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=project_id,
        datasetId=dataset_id,
        deleteContents=delete_contents)
    try:
      self.client.datasets.Delete(request)
    except HttpError as exn:
      if exn.status_code == 404:
        _LOGGER.warning('Dataset %s:%s does not exist', project_id, dataset_id)
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
      self.client.datasets.Get(
          bigquery.BigqueryDatasetsGetRequest(
              projectId=project_id, datasetId=dataset_id))
      if project_id is not None:
        # Unittests don't pass projectIds so they can be run without error
        raise RuntimeError(
            'Dataset %s:%s already exists so cannot be used as temporary.' %
            (project_id, dataset_id))
    except HttpError as exn:
      if exn.status_code == 404:
        _LOGGER.warning(
            'Dataset %s:%s does not exist so we will create it as temporary '
            'with location=%s',
            project_id,
            dataset_id,
            location)
        self.get_or_create_dataset(project_id, dataset_id, location=location)
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def clean_up_temporary_dataset(self, project_id):
    temp_table = self._get_temp_table(project_id)
    try:
      self.client.datasets.Get(
          bigquery.BigqueryDatasetsGetRequest(
              projectId=project_id, datasetId=temp_table.datasetId))
    except HttpError as exn:
      if exn.status_code == 404:
        _LOGGER.warning(
            'Dataset %s:%s does not exist', project_id, temp_table.datasetId)
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

  def perform_load_job(
      self,
      destination,
      files,
      job_id,
      schema=None,
      write_disposition=None,
      create_disposition=None,
      additional_load_parameters=None,
      source_format=None,
      job_labels=None):
    """Starts a job to load data into BigQuery.

    Returns:
      bigquery.JobReference with the information about the job that was started.
    """
    return self._insert_load_job(
        destination.projectId,
        job_id,
        destination,
        files,
        schema=schema,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        additional_load_parameters=additional_load_parameters,
        source_format=source_format,
        job_labels=job_labels)

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def perform_extract_job(
      self,
      destination,
      job_id,
      table_reference,
      destination_format,
      project=None,
      include_header=True,
      compression=ExportCompression.NONE,
      use_avro_logical_types=False,
      job_labels=None):
    """Starts a job to export data from BigQuery.

    Returns:
      bigquery.JobReference with the information about the job that was started.
    """
    job_project = project or table_reference.projectId
    job_reference = bigquery.JobReference(jobId=job_id, projectId=job_project)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=job_project,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                extract=bigquery.JobConfigurationExtract(
                    destinationUris=destination,
                    sourceTable=table_reference,
                    printHeader=include_header,
                    destinationFormat=destination_format,
                    compression=compression,
                    useAvroLogicalTypes=use_avro_logical_types,
                ),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=job_reference,
        ))
    response = self.client.jobs.Insert(request)
    return response.jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_or_create_table(
      self,
      project_id,
      dataset_id,
      table_id,
      schema,
      create_disposition,
      write_disposition,
      additional_create_parameters=None):
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
      `RuntimeError`: For various mismatches between the state of the table and
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
          'table does not exist.' % (project_id, dataset_id, table_id))
    if found_table and write_disposition != BigQueryDisposition.WRITE_TRUNCATE:
      return found_table
    else:
      created_table = None
      try:
        created_table = self._create_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema or found_table.schema,
            additional_parameters=additional_create_parameters)
      except HttpError as exn:
        if exn.status_code == 409:
          _LOGGER.debug(
              'Skipping Creation. Table %s:%s.%s already exists.' %
              (project_id, dataset_id, table_id))
          created_table = self.get_table(project_id, dataset_id, table_id)
        else:
          raise
      _LOGGER.info(
          'Created table %s.%s.%s with schema %s. '
          'Result: %s.',
          project_id,
          dataset_id,
          table_id,
          schema or found_table.schema,
          created_table)
      # if write_disposition == BigQueryDisposition.WRITE_TRUNCATE we delete
      # the table before this point.
      if write_disposition == BigQueryDisposition.WRITE_TRUNCATE:
        # BigQuery can route data to the old table for 2 mins max so wait
        # that much time before creating the table and writing it
        _LOGGER.warning(
            'Sleeping for 150 seconds before the write as ' +
            'BigQuery inserts can be routed to deleted table ' +
            'for 2 mins after the delete and create.')
        # TODO(BEAM-2673): Remove this sleep by migrating to load api
        time.sleep(150)
        return created_table
      else:
        return created_table

  def run_query(
      self,
      project_id,
      query,
      use_legacy_sql,
      flatten_results,
      dry_run=False,
      job_labels=None):
    job = self._start_query_job(
        project_id,
        query,
        use_legacy_sql,
        flatten_results,
        job_id=uuid.uuid4().hex,
        dry_run=dry_run,
        job_labels=job_labels)
    job_id = job.jobReference.jobId
    location = job.jobReference.location

    if dry_run:
      # If this was a dry run then the fact that we get here means the
      # query has no errors. The start_query_job would raise an error otherwise.
      return
    page_token = None
    while True:
      response = self._get_query_results(
          project_id, job_id, page_token, location=location)
      if not response.jobComplete:
        # The jobComplete field can be False if the query request times out
        # (default is 10 seconds). Note that this is a timeout for the query
        # request not for the actual execution of the query in the service.  If
        # the request times out we keep trying. This situation is quite possible
        # if the query will return a large number of rows.
        _LOGGER.info('Waiting on response from query: %s ...', query)
        time.sleep(1.0)
        continue
      # We got some results. The last page is signalled by a missing pageToken.
      yield response.rows, response.schema
      if not response.pageToken:
        break
      page_token = response.pageToken

  def insert_rows(
      self,
      project_id,
      dataset_id,
      table_id,
      rows,
      insert_ids=None,
      skip_invalid_rows=False,
      latency_recoder=None):
    """Inserts rows into the specified table.

    Args:
      project_id: The project id owning the table.
      dataset_id: The dataset id owning the table.
      table_id: The table id.
      rows: A list of plain Python dictionaries. Each dictionary is a row and
        each key in it is the name of a field.
      skip_invalid_rows: If there are rows with insertion errors, whether they
        should be skipped, and all others should be inserted successfully.
      latency_recoder: The object that records request-to-response latencies.
        The object should provide `record(int)` method to be invoked with
        milliseconds latency values.

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
    for i, row in enumerate(rows):
      json_row = self._convert_to_json_row(row)
      insert_id = str(self.unique_row_id) if not insert_ids else insert_ids[i]
      final_rows.append(
          bigquery.TableDataInsertAllRequest.RowsValueListEntry(
              insertId=insert_id, json=json_row))
    result, errors = self._insert_all_rows(
        project_id, dataset_id, table_id, final_rows, skip_invalid_rows,
        latency_recoder)
    return result, errors

  def _convert_to_json_row(self, row):
    json_object = bigquery.JsonObject()
    for k, v in iteritems(row):
      if isinstance(v, decimal.Decimal):
        # decimal values are converted into string because JSON does not
        # support the precision that decimal supports. BQ is able to handle
        # inserts into NUMERIC columns by receiving JSON with string attrs.
        v = str(v)
      json_object.additionalProperties.append(
          bigquery.JsonObject.AdditionalProperty(key=k, value=to_json_value(v)))
    return json_object

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
          result[field.name] = [
              self._convert_cell_value_to_dict(x['v'], field) for x in value
          ]
      elif value is None:
        if not field.mode == 'NULLABLE':
          raise ValueError(
              'Received \'None\' as the value for the field %s '
              'but the field is not NULLABLE.' % field.name)
        result[field.name] = None
      else:
        result[field.name] = self._convert_cell_value_to_dict(value, field)
    return result


# -----------------------------------------------------------------------------
# BigQueryReader, BigQueryWriter.


class BigQueryReader(dataflow_io.NativeSourceReader):
  """A reader for a BigQuery source."""
  def __init__(
      self,
      source,
      test_bigquery_client=None,
      use_legacy_sql=True,
      flatten_results=True,
      kms_key=None):
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
    self.bigquery_job_labels = {}
    self.bq_io_metadata = None

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
          tr.datasetId,
          tr.tableId)
    else:  # It's a query source
      return self.client.get_query_location(
          self.executing_project, self.source.query, self.source.use_legacy_sql)

  def __enter__(self):
    self.client = BigQueryWrapper(client=self.test_bigquery_client)
    self.client.create_temporary_dataset(
        self.executing_project, location=self._get_source_location())
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.client.clean_up_temporary_dataset(self.executing_project)

  def __iter__(self):
    if not self.bq_io_metadata:
      self.bq_io_metadata = create_bigquery_io_metadata()
    for rows, schema in self.client.run_query(
        project_id=self.executing_project, query=self.query,
        use_legacy_sql=self.use_legacy_sql,
        flatten_results=self.flatten_results,
        job_labels=self.bq_io_metadata.add_additional_bq_job_labels(
            self.bigquery_job_labels)):
      if self.schema is None:
        self.schema = schema
      for row in rows:
        # return base64 encoded bytes as byte type on python 3
        # which matches the behavior of Beam Java SDK
        for i in range(len(row.f)):
          if self.schema.fields[i].type == 'BYTES' and row.f[i].v:
            row.f[i].v.string_value = row.f[i].v.string_value.encode('utf-8')

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
      _LOGGER.info(
          'Writing %d rows to %s:%s.%s table.',
          len(self.rows_buffer),
          self.project_id,
          self.dataset_id,
          self.table_id)
      passed, errors = self.client.insert_rows(
          project_id=self.project_id, dataset_id=self.dataset_id,
          table_id=self.table_id, rows=self.rows_buffer)
      self.rows_buffer = []
      if not passed:
        raise RuntimeError(
            'Could not successfully insert rows to BigQuery'
            ' table [%s:%s.%s]. Errors: %s' %
            (self.project_id, self.dataset_id, self.table_id, errors))

  def __enter__(self):
    self.client = BigQueryWrapper(client=self.test_bigquery_client)
    self.client.get_or_create_table(
        self.project_id,
        self.dataset_id,
        self.table_id,
        self.sink.table_schema,
        self.sink.create_disposition,
        self.sink.write_disposition)
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
      raise ValueError(
          '%s. %s. Row: %r' % (e, JSON_COMPLIANCE_ERROR, table_row))

  def decode(self, encoded_table_row):
    return json.loads(encoded_table_row.decode('utf-8'))

  def to_type_hint(self):
    return Any


class JsonRowWriter(io.IOBase):
  """
  A writer which provides an IOBase-like interface for writing table rows
  (represented as dicts) as newline-delimited JSON strings.
  """
  def __init__(self, file_handle):
    """Initialize an JsonRowWriter.

    Args:
      file_handle (io.IOBase): Output stream to write to.
    """
    if not file_handle.writable():
      raise ValueError("Output stream must be writable")

    self._file_handle = file_handle
    self._coder = RowAsDictJsonCoder()

  def close(self):
    self._file_handle.close()

  @property
  def closed(self):
    return self._file_handle.closed

  def flush(self):
    self._file_handle.flush()

  def read(self, size=-1):
    raise io.UnsupportedOperation("JsonRowWriter is not readable")

  def tell(self):
    return self._file_handle.tell()

  def writable(self):
    return self._file_handle.writable()

  def write(self, row):
    return self._file_handle.write(self._coder.encode(row) + b'\n')


class AvroRowWriter(io.IOBase):
  """
  A writer which provides an IOBase-like interface for writing table rows
  (represented as dicts) as Avro records.
  """
  def __init__(self, file_handle, schema):
    """Initialize an AvroRowWriter.

    Args:
      file_handle (io.IOBase): Output stream to write Avro records to.
      schema (Dict[Text, Any]): BigQuery table schema.
    """
    if not file_handle.writable():
      raise ValueError("Output stream must be writable")

    self._file_handle = file_handle
    avro_schema = fastavro.parse_schema(
        get_avro_schema_from_table_schema(schema))
    self._avro_writer = fastavro.write.Writer(self._file_handle, avro_schema)

  def close(self):
    if not self._file_handle.closed:
      self.flush()
      self._file_handle.close()

  @property
  def closed(self):
    return self._file_handle.closed

  def flush(self):
    if self._file_handle.closed:
      raise ValueError("flush on closed file")

    self._avro_writer.flush()
    self._file_handle.flush()

  def read(self, size=-1):
    raise io.UnsupportedOperation("AvroRowWriter is not readable")

  def tell(self):
    # Flush the fastavro Writer to the underlying stream, otherwise there isn't
    # a reliable way to determine how many bytes have been written.
    self._avro_writer.flush()
    return self._file_handle.tell()

  def writable(self):
    return self._file_handle.writable()

  def write(self, row):
    try:
      self._avro_writer.write(row)
    except (TypeError, ValueError) as ex:
      raise_with_traceback(
          ex.__class__(
              "Error writing row to Avro: {}\nSchema: {}\nRow: {}".format(
                  ex, self._avro_writer.schema, row)))


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
    self._display_destination = destination
    self.destination = AppendDestinationsFn._get_table_fn(destination)

  def display_data(self):
    return {'destination': str(self._display_destination)}

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

  def process(self, element, *side_inputs):
    yield (self.destination(element, *side_inputs), element)


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


def get_dict_table_schema(schema):
  """Transform the table schema into a dictionary instance.

  Args:
    schema (str, dict, ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
      The schema to be used if the BigQuery table to write has to be created.
      This can either be a dict or string or in the TableSchema format.

  Returns:
    Dict[str, Any]: The schema to be used if the BigQuery table to write has
    to be created but in the dictionary format.
  """
  if (isinstance(schema, (dict, value_provider.ValueProvider)) or
      callable(schema) or schema is None):
    return schema
  elif isinstance(schema, (str, unicode)):
    table_schema = get_table_schema_from_string(schema)
    return table_schema_to_dict(table_schema)
  elif isinstance(schema, bigquery.TableSchema):
    return table_schema_to_dict(schema)
  else:
    raise TypeError('Unexpected schema argument: %s.' % schema)


def get_avro_schema_from_table_schema(schema):
  """Transform the table schema into an Avro schema.

  Args:
    schema (str, dict, ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
      The TableSchema to convert to Avro schema. This can either be a dict or
      string or in the TableSchema format.

  Returns:
    Dict[str, Any]: An Avro schema, which can be used by fastavro.
  """
  dict_table_schema = get_dict_table_schema(schema)
  return bigquery_avro_tools.get_record_schema_from_dict_table_schema(
      "root", dict_table_schema)


class BigQueryJobTypes:
  EXPORT = 'EXPORT'
  COPY = 'COPY'
  LOAD = 'LOAD'
  QUERY = 'QUERY'


def generate_bq_job_name(job_name, step_id, job_type, random=None):
  from apache_beam.io.gcp.bigquery import BQ_JOB_NAME_TEMPLATE
  random = ("_%s" % random) if random else ""
  return str.format(
      BQ_JOB_NAME_TEMPLATE,
      job_type=job_type,
      job_id=job_name.replace("-", ""),
      step_id=step_id,
      random=random)
