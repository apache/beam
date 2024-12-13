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

import datetime
import decimal
import io
import json
import logging
import re
import sys
import time
import uuid
from json.decoder import JSONDecodeError
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import fastavro
import numpy as np
import regex

import apache_beam
from apache_beam import coders
from apache_beam.internal.gcp import auth
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.http_client import get_new_http
from apache_beam.internal.metrics.metric import MetricLogger
from apache_beam.internal.metrics.metric import Metrics
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import bigquery_avro_tools
from apache_beam.io.gcp import resource_identifiers
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.metrics import monitoring_infos
from apache_beam.options import value_provider
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import DoFn
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.typehints import Any
from apache_beam.utils import retry
from apache_beam.utils.histogram import LinearBucket

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.transfer import Upload
  from apitools.base.py.exceptions import HttpError, HttpForbiddenError
  from google.api_core.exceptions import ClientError, GoogleAPICallError
  from google.api_core.client_info import ClientInfo
  from google.cloud import bigquery as gcp_bigquery
except ImportError:
  gcp_bigquery = None
  pass

try:
  from orjson import dumps as fast_json_dumps
  from orjson import loads as fast_json_loads
except ImportError:
  fast_json_dumps = json.dumps
  fast_json_loads = json.loads

# pylint: enable=wrong-import-order, wrong-import-position

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.io.gcp.internal.clients.bigquery import TableReference
except ImportError:
  TableReference = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

_LOGGER = logging.getLogger(__name__)

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
MAX_RETRIES = 3
UNKNOWN_MIME_TYPE = 'application/octet-stream'

# Timeout for a BQ streaming insert RPC. Set to a maximum of 2 minutes.
BQ_STREAMING_INSERT_TIMEOUT_SEC = 120

_PROJECT_PATTERN = r'([a-z0-9.-]+:)?[a-z][a-z0-9-]*[a-z0-9]'
_DATASET_PATTERN = r'\w{1,1024}'
_TABLE_PATTERN = r'[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}$]{1,1024}'

# TODO(https://github.com/apache/beam/issues/25946): Add support for
# more Beam portable schema types as Python types
BIGQUERY_TYPE_TO_PYTHON_TYPE = {
    "STRING": str,
    "BOOL": bool,
    "BOOLEAN": bool,
    "BYTES": bytes,
    "INT64": np.int64,
    "INTEGER": np.int64,
    "FLOAT64": np.float64,
    "FLOAT": np.float64,
    "NUMERIC": decimal.Decimal,
    "TIMESTAMP": apache_beam.utils.timestamp.Timestamp,
}


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
  if isinstance(destination, TableReference):
    return '%s:%s.%s' % (
        destination.projectId, destination.datasetId, destination.tableId)
  else:
    return destination


V = TypeVar('V')


def to_hashable_table_ref(
    table_ref_elem_kv: Tuple[Union[str, TableReference], V]) -> Tuple[str, V]:
  """Turns the key of the input tuple to its string representation. The key
  should be either a string or a TableReference.

  Args:
    table_ref_elem_kv: A tuple of table reference and element.

  Returns:
    A tuple of string representation of input table and input element.
  """
  table_ref = table_ref_elem_kv[0]
  hashable_table_ref = get_hashable_destination(table_ref)
  return (hashable_table_ref, table_ref_elem_kv[1])


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
      (a-z, A-Z), numbers (0-9), connectors (-_). If dataset argument is None
      then the table argument must contain the entire table reference:
      'DATASET.TABLE' or 'PROJECT:DATASET.TABLE'. This argument can be a
      TableReference instance in which case dataset and project are
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

  if isinstance(table, TableReference):
    return TableReference(
        projectId=table.projectId,
        datasetId=table.datasetId,
        tableId=table.tableId)
  elif callable(table):
    return table
  elif isinstance(table, value_provider.ValueProvider):
    return table

  table_reference = TableReference()
  # If dataset argument is not specified, the expectation is that the
  # table argument will contain a full table reference instead of just a
  # table name.
  if dataset is None:
    pattern = (
        f'((?P<project>{_PROJECT_PATTERN})[:\\.])?'
        f'(?P<dataset>{_DATASET_PATTERN})\\.(?P<table>{_TABLE_PATTERN})')
    match = regex.fullmatch(pattern, table)
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


def _build_dataset_labels(input_labels):
  """Builds dataset label protobuf structure."""
  input_labels = input_labels or {}
  result = bigquery.Dataset.LabelsValue()

  for k, v in input_labels.items():
    result.additionalProperties.append(
        bigquery.Dataset.LabelsValue.AdditionalProperty(
            key=k,
            value=v,
        ))
  return result


def _build_filter_from_labels(labels):
  filter_str = ''
  for key, value in labels.items():
    filter_str += 'labels.' + key + ':' + value + ' '
  return filter_str


class BigQueryWrapper(object):
  """BigQuery client wrapper with utilities for querying.

  The wrapper is used to organize all the BigQuery integration points and
  offer a common place where retry logic for failures can be controlled.
  In addition, it offers various functions used both in sources and sinks
  (e.g., find and create tables, query a table, etc.).

  Note that client parameter in constructor is only for testing purposes and
  should not be used in production code.
  """

  # If updating following names, also update the corresponding pydocs in
  # bigquery.py.
  TEMP_TABLE = 'beam_temp_table_'
  TEMP_DATASET = 'beam_temp_dataset_'

  HISTOGRAM_METRIC_LOGGER = MetricLogger()

  def __init__(
      self,
      client=None,
      temp_dataset_id=None,
      temp_table_ref=None):
    self.client = client or BigQueryWrapper._bigquery_client(PipelineOptions())
    self.gcp_bq_client = client or gcp_bigquery.Client(
        client_info=ClientInfo(
            user_agent="apache-beam-%s" % apache_beam.__version__))

    self._unique_row_id = 0
    # For testing scenarios where we pass in a client we do not want a
    # randomized prefix for row IDs.
    self._row_id_prefix = '' if client else uuid.uuid4()
    self._latency_histogram_metric = Metrics.histogram(
        self.__class__,
        'latency_histogram_ms',
        LinearBucket(0, 20, 3000),
        BigQueryWrapper.HISTOGRAM_METRIC_LOGGER)

    if temp_dataset_id is not None and temp_table_ref is not None:
      raise ValueError(
          'Both a BigQuery temp_dataset_id and a temp_table_ref were specified.'
          ' Please specify only one of these.')

    if temp_dataset_id and temp_dataset_id.startswith(self.TEMP_DATASET):
      raise ValueError(
          'User provided temp dataset ID cannot start with %r' %
          self.TEMP_DATASET)

    if temp_table_ref is not None:
      self.temp_table_ref = temp_table_ref
      self.temp_dataset_id = temp_table_ref.datasetId
    else:
      self.temp_table_ref = None
      self._temporary_table_suffix = uuid.uuid4().hex
      self.temp_dataset_id = temp_dataset_id or self._get_temp_dataset()

    self.created_temp_dataset = False

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
    if self.temp_table_ref:
      return self.temp_table_ref

    return parse_table_reference(
        table=BigQueryWrapper.TEMP_TABLE + self._temporary_table_suffix,
        dataset=self.temp_dataset_id,
        project=project_id)

  def _get_temp_dataset(self):
    if self.temp_table_ref:
      return self.temp_table_ref.datasetId
    return BigQueryWrapper.TEMP_DATASET + self._temporary_table_suffix

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

    return self._start_job(request).jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _insert_load_job(
      self,
      project_id,
      job_id,
      table_reference,
      source_uris=None,
      source_stream=None,
      schema=None,
      write_disposition=None,
      create_disposition=None,
      additional_load_parameters=None,
      source_format=None,
      job_labels=None):

    if not source_uris and not source_stream:
      _LOGGER.warning(
          'Both source URIs and source stream are not provided. BigQuery load '
          'job will not load any data.')

    if source_uris and source_stream:
      raise ValueError(
          'Only one of source_uris and source_stream may be specified. '
          'Got both.')

    if source_uris is None:
      source_uris = []

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
    return self._start_job(request, stream=source_stream).jobReference

  @staticmethod
  def _parse_location_from_exc(content, job_id):
    """Parse job location from Exception content."""
    if isinstance(content, bytes):
      content = content.decode('ascii', 'replace')
    # search for "Already Exists: Job <project-id>:<location>.<job id>"
    m = re.search(r"Already Exists: Job \S+\:(\S+)\." + job_id, content)
    if not m:
      _LOGGER.warning(
          "Not able to parse BigQuery load job location for %s", job_id)
      return None
    return m.group(1)

  def _start_job(
      self,
      request: 'bigquery.BigqueryJobsInsertRequest',
      stream=None,
  ):
    """Inserts a BigQuery job.

    If the job exists already, it returns it.

    Args:
      request (bigquery.BigqueryJobsInsertRequest): An insert job request.
      stream (IO[bytes]): A bytes IO object open for reading.
    """
    try:
      upload = None
      if stream:
        upload = Upload.FromStream(stream, mime_type=UNKNOWN_MIME_TYPE)
      response = self.client.jobs.Insert(request, upload=upload)
      _LOGGER.info(
          "Started BigQuery job: %s\n "
          "bq show -j --format=prettyjson --project_id=%s %s",
          response.jobReference,
          response.jobReference.projectId,
          response.jobReference.jobId)
      return response
    except HttpError as exn:
      if exn.status_code == 409:
        jobId = request.job.jobReference.jobId
        _LOGGER.info(
            "BigQuery job %s already exists, will not retry inserting it: %s",
            request.job.jobReference,
            exn)
        job_location = self._parse_location_from_exc(exn.content, jobId)
        response = request.job
        if not response.jobReference.location and job_location:
          # Request not constructed with location
          response.jobReference.location = job_location
        return response
      else:
        _LOGGER.info(
            "Failed to insert job %s: %s", request.job.jobReference, exn)
        raise

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
      priority,
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
                    priority=priority,
                    destinationEncryptionConfiguration=bigquery.
                    EncryptionConfiguration(kmsKeyName=kms_key)),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=reference))

    return self._start_job(request)

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
      _LOGGER.info('Job %s status: %s', job.id, job.status.state)
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
      insert_ids,
      skip_invalid_rows=False,
      ignore_unknown_values=False):
    """Calls the insertAll BigQuery API endpoint.

    Docs for this BQ call: https://cloud.google.com/bigquery/docs/reference\
      /rest/v2/tabledata/insertAll."""
    # The rows argument is a list of
    # bigquery.TableDataInsertAllRequest.RowsValueListEntry instances as
    # required by the InsertAll() method.
    resource = resource_identifiers.BigQueryTable(
        project_id, dataset_id, table_id)

    labels = {
        # TODO(ajamato): Add Ptransform label.
        monitoring_infos.SERVICE_LABEL: 'BigQuery',
        # Refer to any method which writes elements to BigQuery in batches
        # as "BigQueryBatchWrite". I.e. storage API's insertAll, or future
        # APIs introduced.
        monitoring_infos.METHOD_LABEL: 'BigQueryBatchWrite',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.BIGQUERY_PROJECT_ID_LABEL: project_id,
        monitoring_infos.BIGQUERY_DATASET_LABEL: dataset_id,
        monitoring_infos.BIGQUERY_TABLE_LABEL: table_id,
    }
    service_call_metric = ServiceCallMetric(
        request_count_urn=monitoring_infos.API_REQUEST_COUNT_URN,
        base_labels=labels)

    started_millis = int(time.time() * 1000)
    try:
      table_ref_str = '%s.%s.%s' % (project_id, dataset_id, table_id)
      errors = self.gcp_bq_client.insert_rows_json(
          table_ref_str,
          json_rows=rows,
          row_ids=insert_ids,
          skip_invalid_rows=skip_invalid_rows,
          ignore_unknown_values=ignore_unknown_values,
          timeout=BQ_STREAMING_INSERT_TIMEOUT_SEC)
      if not errors:
        service_call_metric.call('ok')
      else:
        for insert_error in errors:
          service_call_metric.call(insert_error['errors'][0])
    except (ClientError, GoogleAPICallError) as e:
      # e.code contains the numeric http status code.
      service_call_metric.call(e.code)
      # Package exception with required fields
      error = {'message': e.message, 'reason': e.response.reason}
      # Add all rows to the errors list along with the error
      errors = [{"index": i, "errors": [error]} for i, _ in enumerate(rows)]
    except HttpError as e:
      service_call_metric.call(e)
      # Re-raise the exception so that we re-try appropriately.
      raise
    finally:
      self._latency_histogram_metric.update(
          int(time.time() * 1000) - started_millis)
    return not errors, errors

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_timeout_or_quota_issues_filter)
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

    valid_tablename = regex.fullmatch(_TABLE_PATTERN, table_id, regex.ASCII)
    if not valid_tablename:
      raise ValueError(
          'Invalid BigQuery table name: %s \n'
          'See https://cloud.google.com/bigquery/docs/tables#table_naming' %
          table_id)

    additional_parameters = additional_parameters or {}
    table = bigquery.Table(
        tableReference=TableReference(
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
  def get_or_create_dataset(
      self, project_id, dataset_id, location=None, labels=None):
    # Check if dataset already exists otherwise create it
    try:
      dataset = self.client.datasets.Get(
          bigquery.BigqueryDatasetsGetRequest(
              projectId=project_id, datasetId=dataset_id))
      self.created_temp_dataset = False
      return dataset
    except HttpError as exn:
      if exn.status_code == 404:
        _LOGGER.info(
            'Dataset %s:%s does not exist so we will create it as temporary '
            'with location=%s',
            project_id,
            dataset_id,
            location)
        dataset_reference = bigquery.DatasetReference(
            projectId=project_id, datasetId=dataset_id)
        dataset = bigquery.Dataset(datasetReference=dataset_reference)
        if location is not None:
          dataset.location = location
        if labels is not None:
          dataset.labels = _build_dataset_labels(labels)
        request = bigquery.BigqueryDatasetsInsertRequest(
            projectId=project_id, dataset=dataset)
        response = self.client.datasets.Insert(request)
        self.created_temp_dataset = True
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

  # Returns true if the temporary dataset was provided by the user.
  def is_user_configured_dataset(self):
    return (
        self.temp_dataset_id and
        not self.temp_dataset_id.startswith(self.TEMP_DATASET))

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def create_temporary_dataset(self, project_id, location, labels=None):
    self.get_or_create_dataset(
        project_id, self.temp_dataset_id, location=location, labels=labels)

    if (project_id is not None and not self.is_user_configured_dataset() and
        not self.created_temp_dataset):
      # Unittests don't pass projectIds so they can be run without error
      # User configured datasets are allowed to pre-exist.
      raise RuntimeError(
          'Dataset %s:%s already exists so cannot be used as temporary.' %
          (project_id, self.temp_dataset_id))

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
    try:
      # We do not want to delete temporary datasets configured by the user hence
      # we just delete the temporary table in that case.
      if not self.is_user_configured_dataset():
        self._delete_dataset(temp_table.projectId, temp_table.datasetId, True)
      else:
        self._delete_table(
            temp_table.projectId, temp_table.datasetId, temp_table.tableId)
      self.created_temp_dataset = False
    except HttpError as exn:
      if exn.status_code == 403:
        _LOGGER.warning(
            'Permission denied to delete temporary dataset %s:%s for clean up',
            temp_table.projectId,
            temp_table.datasetId)
        return
      else:
        raise

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _clean_up_beam_labelled_temporary_datasets(
      self, project_id, dataset_id=None, table_id=None, labels=None):
    if isinstance(labels, dict):
      filter_str = _build_filter_from_labels(labels)

    if not self.is_user_configured_dataset() and labels is not None:
      response = (
          self.client.datasets.List(
              bigquery.BigqueryDatasetsListRequest(
                  projectId=project_id, filter=filter_str)))
      for dataset in response.datasets:
        try:
          dataset_id = dataset.datasetReference.datasetId
          self._delete_dataset(project_id, dataset_id, True)
        except HttpError as exn:
          if exn.status_code == 403:
            _LOGGER.warning(
                'Permission denied to delete temporary dataset %s:%s for '
                'clean up.',
                project_id,
                dataset_id)
            return
          else:
            raise
    else:
      try:
        self._delete_table(project_id, dataset_id, table_id)
      except HttpError as exn:
        if exn.status_code == 403:
          _LOGGER.warning(
              'Permission denied to delete temporary table %s:%s.%s for '
              'clean up.',
              project_id,
              dataset_id,
              table_id)
          return
        else:
          raise

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
      job_id,
      source_uris=None,
      source_stream=None,
      schema=None,
      write_disposition=None,
      create_disposition=None,
      additional_load_parameters=None,
      source_format=None,
      job_labels=None,
      load_job_project_id=None):
    """Starts a job to load data into BigQuery.

    Returns:
      bigquery.JobReference with the information about the job that was started.
    """
    project_id = (
        destination.projectId
        if load_job_project_id is None else load_job_project_id)

    return self._insert_load_job(
        project_id,
        job_id,
        destination,
        source_uris=source_uris,
        source_stream=source_stream,
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
    return self._start_job(request).jobReference

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.
      retry_if_valid_input_but_server_error_and_timeout_filter)
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
    if found_table and write_disposition in (
        BigQueryDisposition.WRITE_EMPTY, BigQueryDisposition.WRITE_TRUNCATE):
      # Delete the table and recreate it (later) if WRITE_TRUNCATE was
      # specified.
      if write_disposition == BigQueryDisposition.WRITE_TRUNCATE:
        self._delete_table(project_id, dataset_id, table_id)
      elif (write_disposition == BigQueryDisposition.WRITE_EMPTY and
            not self._is_table_empty(project_id, dataset_id, table_id)):
        raise RuntimeError(
            'Table %s:%s.%s is not empty but write disposition is WRITE_EMPTY.'
            % (project_id, dataset_id, table_id))

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
      priority,
      dry_run=False,
      job_labels=None):
    job = self._start_query_job(
        project_id,
        query,
        use_legacy_sql,
        flatten_results,
        job_id=uuid.uuid4().hex,
        priority=priority,
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
      ignore_unknown_values=False):
    """Inserts rows into the specified table.

    Args:
      project_id: The project id owning the table.
      dataset_id: The dataset id owning the table.
      table_id: The table id.
      rows: A list of plain Python dictionaries. Each dictionary is a row and
        each key in it is the name of a field.
      skip_invalid_rows: If there are rows with insertion errors, whether they
        should be skipped, and all others should be inserted successfully.
      ignore_unknown_values: Set this option to true to ignore unknown column
        names. If the input rows contain columns that are not
        part of the existing table's schema, those columns are ignored, and
        the rows are successfully inserted.

    Returns:
      A tuple (bool, errors). If first element is False then the second element
      will be a bigquery.InsertErrorsValueListEntry instance containing
      specific errors.
    """

    # Prepare rows for insertion. Of special note is the row ID that we add to
    # each row in order to help BigQuery avoid inserting a row multiple times.
    # BigQuery will do a best-effort if unique IDs are provided. This situation
    # can happen during retries on failures.
    # TODO(silviuc): Must add support to writing TableRow's instead of dicts.
    insert_ids = [
        str(self.unique_row_id) if not insert_ids else insert_ids[i] for i,
        _ in enumerate(rows)
    ]
    rows = [
        fast_json_loads(fast_json_dumps(r, default=default_encoder))
        for r in rows
    ]

    result, errors = self._insert_all_rows(
        project_id, dataset_id, table_id, rows, insert_ids,
        skip_invalid_rows=skip_invalid_rows,
        ignore_unknown_values=ignore_unknown_values)
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

  @staticmethod
  def from_pipeline_options(pipeline_options: PipelineOptions):
    return BigQueryWrapper(
        client=BigQueryWrapper._bigquery_client(pipeline_options))

  @staticmethod
  def _bigquery_client(pipeline_options: PipelineOptions):
    return bigquery.BigqueryV2(
        http=get_new_http(),
        credentials=auth.get_service_credentials(pipeline_options),
        response_encoding='utf8',
        additional_http_headers={
            "user-agent": "apache-beam-%s" % apache_beam.__version__
        })


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
          table_row,
          allow_nan=False,
          ensure_ascii=False,
          default=default_encoder).encode('utf-8')
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
      _, _, tb = sys.exc_info()
      raise ex.__class__(
          "Error writing row to Avro: {}\nSchema: {}\nRow: {}".format(
              ex, self._avro_writer.schema, row)).with_traceback(tb)


class RetryStrategy(object):
  RETRY_ALWAYS = 'RETRY_ALWAYS'
  RETRY_NEVER = 'RETRY_NEVER'
  RETRY_ON_TRANSIENT_ERROR = 'RETRY_ON_TRANSIENT_ERROR'

  # Values below may be found in reasons provided either in an
  # error returned by a client method or by an http response as
  # defined in google.api_core.exceptions
  _NON_TRANSIENT_ERRORS = {
      'invalid',
      'invalidQuery',
      'notImplemented',
      'Bad Request',
      'Unauthorized',
      'Forbidden',
      'Not Found',
      'Not Implemented',
  }

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


def beam_row_from_dict(row: dict, schema):
  """Converts a dictionary row to a Beam Row.
  Nested records and lists are supported.

  Args:
    row (dict):
      The row to convert.
    schema (str, dict, ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
      The table schema. Will be used to help convert the row.

  Returns:
    ~apache_beam.pvalue.Row: The converted row.
  """
  if not isinstance(schema, (bigquery.TableSchema, bigquery.TableFieldSchema)):
    schema = get_bq_tableschema(schema)
  beam_row = {}
  for field in schema.fields:
    name = field.name
    mode = field.mode.upper()
    type = field.type.upper()
    # When writing with Storage Write API via xlang, we give the Beam Row
    # PCollection a hint on the schema using `with_output_types`.
    # This requires that each row has all the fields in the schema.
    # However, it's possible that some nullable fields don't appear in the row.
    # For this case, we create the field with a `None` value
    if name not in row and mode == "NULLABLE":
      row[name] = None

    value = row[name]
    if type in ["RECORD", "STRUCT"]:
      # if this is a list of records, we create a list of Beam Rows
      if mode == "REPEATED":
        list_of_beam_rows = []
        for record in value:
          list_of_beam_rows.append(beam_row_from_dict(record, field))
        beam_row[name] = list_of_beam_rows
      # otherwise, create a Beam Row from this record
      else:
        beam_row[name] = beam_row_from_dict(value, field)
    else:
      beam_row[name] = value
  return apache_beam.pvalue.Row(**beam_row)


def get_table_schema_from_string(schema):
  """Transform the string table schema into a
  :class:`~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema` instance.

  Args:
    schema (str): The string schema to be used if the BigQuery table to write
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
  elif isinstance(schema, str):
    table_schema = get_table_schema_from_string(schema)
    return table_schema_to_dict(table_schema)
  elif isinstance(schema, bigquery.TableSchema):
    return table_schema_to_dict(schema)
  else:
    raise TypeError('Unexpected schema argument: %s.' % schema)


def get_bq_tableschema(schema):
  """Convert the table schema to a TableSchema object.

  Args:
    schema (str, dict, ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
      The schema to be used if the BigQuery table to write has to be created.
      This can either be a dict or string or in the TableSchema format.

  Returns:
    ~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema: The schema as a TableSchema object.
  """
  if (isinstance(schema,
                 (bigquery.TableSchema, value_provider.ValueProvider)) or
      callable(schema) or schema is None):
    return schema
  elif isinstance(schema, str):
    return get_table_schema_from_string(schema)
  elif isinstance(schema, dict):
    schema_string = json.dumps(schema)
    return parse_table_schema_from_json(schema_string)
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


def get_beam_typehints_from_tableschema(schema):
  """Extracts Beam Python type hints from the schema.

  Args:
    schema (~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema):
      The TableSchema to extract type hints from.

  Returns:
    List[Tuple[str, Any]]: A list of type hints that describe the input schema.
    Nested and repeated fields are supported.
  """
  if not isinstance(schema, (bigquery.TableSchema, bigquery.TableFieldSchema)):
    schema = get_bq_tableschema(schema)
  typehints = []
  for field in schema.fields:
    name, field_type, mode = field.name, field.type.upper(), field.mode.upper()

    if field_type in ["STRUCT", "RECORD"]:
      # Structs can be represented as Beam Rows.
      typehint = RowTypeConstraint.from_fields(
          get_beam_typehints_from_tableschema(field))
    elif field_type in BIGQUERY_TYPE_TO_PYTHON_TYPE:
      typehint = BIGQUERY_TYPE_TO_PYTHON_TYPE[field_type]
    else:
      raise ValueError(
          f"Converting BigQuery type [{field_type}] to "
          "Python Beam type is not supported.")

    if mode == "REPEATED":
      typehint = Sequence[typehint]
    elif mode != "REQUIRED":
      typehint = Optional[typehint]

    typehints.append((name, typehint))
  return typehints


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


def check_schema_equal(
    left: Union['bigquery.TableSchema', 'bigquery.TableFieldSchema'],
    right: Union['bigquery.TableSchema', 'bigquery.TableFieldSchema'],
    *,
    ignore_descriptions: bool = False,
    ignore_field_order: bool = False) -> bool:
  """Check whether schemas are equivalent.

  This comparison function differs from using == to compare TableSchema
  because it ignores categories, policy tags, descriptions (optionally), and
  field ordering (optionally).

  Args:
    left (~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema, ~apache_beam.io.gcp.internal.clients.\
bigquery.bigquery_v2_messages.TableFieldSchema):
      One schema to compare.
    right (~apache_beam.io.gcp.internal.clients.bigquery.\
bigquery_v2_messages.TableSchema, ~apache_beam.io.gcp.internal.clients.\
bigquery.bigquery_v2_messages.TableFieldSchema):
      The other schema to compare.
    ignore_descriptions (bool): (optional) Whether or not to ignore field
      descriptions when comparing. Defaults to False.
    ignore_field_order (bool): (optional) Whether or not to ignore struct field
      order when comparing. Defaults to False.

  Returns:
    bool: True if the schemas are equivalent, False otherwise.
  """
  if type(left) != type(right) or not isinstance(
      left, (bigquery.TableSchema, bigquery.TableFieldSchema)):
    return False

  if isinstance(left, bigquery.TableFieldSchema):
    if left.name != right.name:
      return False

    if left.type != right.type:
      # Check for type aliases
      if sorted(
          (left.type, right.type)) not in (["BOOL", "BOOLEAN"], ["FLOAT",
                                                                 "FLOAT64"],
                                           ["INT64", "INTEGER"], ["RECORD",
                                                                  "STRUCT"]):
        return False

    if left.mode != right.mode:
      return False

    if not ignore_descriptions and left.description != right.description:
      return False

  if isinstance(left,
                bigquery.TableSchema) or left.type in ("RECORD", "STRUCT"):
    if len(left.fields) != len(right.fields):
      return False

    if ignore_field_order:
      left_fields = sorted(left.fields, key=lambda field: field.name)
      right_fields = sorted(right.fields, key=lambda field: field.name)
    else:
      left_fields = left.fields
      right_fields = right.fields

    for left_field, right_field in zip(left_fields, right_fields):
      if not check_schema_equal(left_field,
                                right_field,
                                ignore_descriptions=ignore_descriptions,
                                ignore_field_order=ignore_field_order):
        return False

  return True
