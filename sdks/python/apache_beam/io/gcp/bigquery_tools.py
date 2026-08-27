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
# pylint: disable=wrong-import-order, wrong-import-position

import datetime
import decimal
import io
import json
import logging
import re
import sys
import time
import typing
import uuid
from json.decoder import JSONDecodeError
from typing import Optional
from typing import Sequence
from typing import TypeVar
from typing import Union

import fastavro
import numpy as np

import apache_beam
from apache_beam import coders
from apache_beam.internal.gcp import auth
from apache_beam.internal.gcp import json_value
from apache_beam.internal.gcp.json_value import from_json_value
from apache_beam.internal.http_client import get_new_http
from apache_beam.internal.metrics.metric import MetricLogger
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import bigquery_avro_tools
from apache_beam.io.gcp import resource_identifiers

try:
  from apache_beam.io.gcp.internal.clients import bigquery as apitools_bigquery
except ImportError:
  apitools_bigquery = None

bigquery = apitools_bigquery
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.metric import Metrics
from apache_beam.options import value_provider
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import DoFn
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.typehints import Any
from apache_beam.utils import retry
from apache_beam.utils.histogram import LinearBucket

# Protect against environments where bigquery library is not available.
try:
  import regex
  from apitools.base.py import extra_types
  from apitools.base.py.exceptions import HttpError
  from apitools.base.py.exceptions import HttpForbiddenError
  from apitools.base.py.transfer import Upload
except ImportError:
  extra_types = None
  HttpError = type('HttpError', (Exception, ), {'status_code': None})
  HttpForbiddenError = type(
      'HttpForbiddenError', (Exception, ), {'status_code': 403})
  Upload = None

try:
  from google.api_core.client_info import ClientInfo
  from google.api_core.exceptions import ClientError
  from google.api_core.exceptions import Conflict
  from google.api_core.exceptions import Forbidden
  from google.api_core.exceptions import GoogleAPICallError
  from google.api_core.exceptions import NotFound
  from google.api_core.exceptions import ServerError
  from google.cloud import bigquery as gcp_bigquery
  from google.cloud.bigquery import job as gcp_job
except ImportError:
  ClientInfo = None
  ClientError = None
  Conflict = None
  Forbidden = None
  GoogleAPICallError = None
  NotFound = None
  ServerError = None
  gcp_bigquery = None
  gcp_job = None

try:
  from orjson import dumps as fast_json_dumps
  from orjson import loads as fast_json_loads
except ImportError:
  fast_json_dumps = json.dumps
  fast_json_loads = json.loads

# -----------------------------------------------------------------------------
# Compatibility Models for TableReference, DatasetReference, Schema, and Jobs.
#
# These classes and monkey patches bridge between legacy apitools structures
# and modern google.cloud.bigquery objects, providing camelCase attribute access
# (e.g. projectId, datasetId, tableId, tableReference) for backwards
# compatibility across pipelines, transforms, and test suites.


class _DatasetReferenceCompat(object):
  """Compatibility model for BigQuery DatasetReference when google-cloud-bigquery is unavailable.

  Supports both camelCase (projectId, datasetId) and snake_case (project, dataset_id, project_id).
  """
  def __init__(
      self,
      project=None,
      dataset_id=None,
      projectId=None,
      datasetId=None,
      project_id=None):
    p = (
        projectId if projectId is not None else
        (project_id if project_id is not None else project))
    d = datasetId if datasetId is not None else dataset_id
    self._project = p or ''
    self._dataset_id = d or ''

  @classmethod
  def from_string(cls, dataset_ref, default_project=None):
    if ':' in dataset_ref:
      p, d = dataset_ref.split(':', 1)
    elif '.' in dataset_ref:
      parts = dataset_ref.split('.', 1)
      p, d = parts[0], parts[1]
    else:
      p, d = default_project or 'default', dataset_ref
    return cls(project=p, dataset_id=d)

  @property
  def projectId(self):
    return self._project

  @projectId.setter
  def projectId(self, val):
    self._project = val

  @property
  def project(self):
    return self._project

  @project.setter
  def project(self, val):
    self._project = val

  @property
  def project_id(self):
    return self._project

  @project_id.setter
  def project_id(self, val):
    self._project = val

  @property
  def datasetId(self):
    return self._dataset_id

  @datasetId.setter
  def datasetId(self, val):
    self._dataset_id = val

  @property
  def dataset_id(self):
    return self._dataset_id

  @dataset_id.setter
  def dataset_id(self, val):
    self._dataset_id = val

  def __repr__(self):
    return f"DatasetReference('{self.project}', '{self.dataset_id}')"

  def __eq__(self, other):
    if other is None:
      return False
    if not hasattr(other, 'project') and not hasattr(other, 'projectId'):
      return NotImplemented
    other_p = getattr(other, 'projectId', None) or getattr(
        other, 'project', None)
    other_d = getattr(other, 'datasetId', None) or getattr(
        other, 'dataset_id', None)
    return (self.projectId, self.datasetId) == (other_p, other_d)

  def __hash__(self):
    return hash((self.projectId, self.datasetId))


class _TableReferenceCompat(object):
  """Compatibility model for BigQuery TableReference when google-cloud-bigquery is unavailable.

  Supports both camelCase (projectId, datasetId, tableId) and snake_case
  (project, dataset_id, table_id, project_id).
  """
  def __init__(
      self,
      dataset_ref=None,
      table_id=None,
      projectId=None,
      datasetId=None,
      tableId=None,
      project=None,
      dataset_id=None,
      project_id=None):
    p = (
        projectId if projectId is not None else
        (project_id if project_id is not None else project))
    d = datasetId if datasetId is not None else dataset_id
    t = tableId if tableId is not None else table_id
    if p is not None or d is not None or t is not None:
      self._project = p
      self._dataset_id = d
      self._table_id = t
    elif dataset_ref is not None:
      self._project = getattr(dataset_ref, 'projectId', None) or getattr(
          dataset_ref, 'project', None)
      self._dataset_id = getattr(dataset_ref, 'datasetId', None) or getattr(
          dataset_ref, 'dataset_id', None)
      self._table_id = table_id or ''
    else:
      self._project = None
      self._dataset_id = None
      self._table_id = None

  @classmethod
  def from_string(cls, table_ref, default_project=None):
    parsed = parse_table_reference(table_ref, project=default_project)
    return cls(
        projectId=parsed.projectId,
        datasetId=parsed.datasetId,
        tableId=parsed.tableId)

  @property
  def projectId(self):
    return self._project

  @projectId.setter
  def projectId(self, val):
    self._project = val

  @property
  def project(self):
    return self._project

  @project.setter
  def project(self, val):
    self._project = val

  @property
  def project_id(self):
    return self._project

  @project_id.setter
  def project_id(self, val):
    self._project = val

  @property
  def datasetId(self):
    return self._dataset_id

  @datasetId.setter
  def datasetId(self, val):
    self._dataset_id = val

  @property
  def dataset_id(self):
    return self._dataset_id

  @dataset_id.setter
  def dataset_id(self, val):
    self._dataset_id = val

  @property
  def tableId(self):
    return self._table_id

  @tableId.setter
  def tableId(self, val):
    self._table_id = val

  @property
  def table_id(self):
    return self._table_id

  @table_id.setter
  def table_id(self, val):
    self._table_id = val

  @property
  def dataset_reference(self):
    return _DatasetReferenceCompat(
        projectId=self.projectId, datasetId=self.datasetId)

  @property
  def datasetReference(self):
    return self.dataset_reference

  def __repr__(self):
    return (
        f"TableReference(projectId='{self.projectId}', "
        f"datasetId='{self.datasetId}', tableId='{self.tableId}')")

  def __eq__(self, other):
    if other is None:
      return False
    if not hasattr(other, 'tableId') and not hasattr(other, 'table_id'):
      return NotImplemented
    other_p = getattr(other, 'projectId', None) or getattr(
        other, 'project', None)
    other_d = getattr(other, 'datasetId', None) or getattr(
        other, 'dataset_id', None)
    other_t = getattr(other, 'tableId', None) or getattr(
        other, 'table_id', None)
    return (self.projectId, self.datasetId,
            self.tableId) == (other_p, other_d, other_t)

  def __hash__(self):
    return hash((self.projectId, self.datasetId, self.tableId))


class _TableFieldSchemaCompat(object):
  def __init__(
      self,
      name='',
      type='STRING',
      mode='NULLABLE',
      description=None,
      fields=(),
      field_type=None,
      **kwargs):
    ft = type or field_type or 'STRING'
    self.name = name
    self.field_type = ft
    self.mode = mode or 'NULLABLE'
    self.description = description
    self.fields = list(fields) if fields else []

  @property
  def type(self):
    return self.field_type

  @type.setter
  def type(self, val):
    self.field_type = val


class _TableSchemaCompat(list):
  def __init__(self, fields=None):
    if fields:
      super().__init__(fields)
    else:
      super().__init__()

  @property
  def fields(self):
    return self

  @fields.setter
  def fields(self, value):
    self.clear()
    if value:
      self.extend(value)


class _TableCellCompat(object):
  def __init__(self, v=None):
    self.v = v


class _TableRowCompat(object):
  def __init__(self, f=None):
    self.f = f or []


if bigquery is not None and hasattr(bigquery, 'TableReference'):
  TableReference = bigquery.TableReference
  DatasetReference = getattr(
      bigquery, 'DatasetReference', None) or _DatasetReferenceCompat
  TableFieldSchema = bigquery.TableFieldSchema
  TableSchema = bigquery.TableSchema
  TableRow = getattr(bigquery, 'TableRow', None) or _TableRowCompat
  TableCell = getattr(bigquery, 'TableCell', None) or _TableCellCompat
  Table = getattr(bigquery, 'Table', None)
  Dataset = getattr(bigquery, 'Dataset', None)
  Job = getattr(bigquery, 'Job', None)
  JobConfiguration = getattr(bigquery, 'JobConfiguration', None)
  JobConfigurationLoad = getattr(bigquery, 'JobConfigurationLoad', None)
  JobConfigurationQuery = getattr(bigquery, 'JobConfigurationQuery', None)
  JobConfigurationExtract = getattr(bigquery, 'JobConfigurationExtract', None)
  JobConfigurationTableCopy = getattr(
      bigquery, 'JobConfigurationTableCopy', None)
  JobStatistics = getattr(bigquery, 'JobStatistics', None)
  JobStatistics2 = getattr(bigquery, 'JobStatistics2', None)
  JobStatistics4 = getattr(bigquery, 'JobStatistics4', None)
  ErrorProto = getattr(bigquery, 'ErrorProto', None)
else:
  TableReference = _TableReferenceCompat
  DatasetReference = _DatasetReferenceCompat
  TableFieldSchema = _TableFieldSchemaCompat
  TableSchema = _TableSchemaCompat
  TableRow = _TableRowCompat
  TableCell = _TableCellCompat
  Table = None
  Dataset = None
  Job = None
  JobConfiguration = None
  JobConfigurationLoad = None
  JobConfigurationQuery = None
  JobConfigurationExtract = None
  JobConfigurationTableCopy = None
  JobStatistics = None
  JobStatistics2 = None
  JobStatistics4 = None
  ErrorProto = None


class JobReference(object):
  """Compatibility model for BigQuery JobReference.

  Supports both camelCase (jobId, projectId) and snake_case (job_id, project, project_id)
  initialization and attribute access.
  """
  def __init__(
      self,
      jobId=None,
      projectId=None,
      location=None,
      job_id=None,
      project=None,
      project_id=None):
    self.jobId = jobId if jobId is not None else job_id
    self.projectId = (
        projectId if projectId is not None else
        (project if project is not None else project_id))
    self.location = location

  @property
  def job_id(self):
    return self.jobId

  @job_id.setter
  def job_id(self, val):
    self.jobId = val

  @property
  def project(self):
    return self.projectId

  @project.setter
  def project(self, val):
    self.projectId = val

  @property
  def project_id(self):
    return self.projectId

  @project_id.setter
  def project_id(self, val):
    self.projectId = val

  def __eq__(self, other):
    if other is None:
      return False
    if isinstance(other, JobReference):
      return (
          self.jobId == other.jobId and self.projectId == other.projectId and
          self.location == other.location)
    if apitools_bigquery and hasattr(apitools_bigquery,
                                     'JobReference') and isinstance(
                                         other, apitools_bigquery.JobReference):
      return (
          self.jobId == getattr(other, 'jobId', None) and
          self.projectId == getattr(other, 'projectId', None) and
          self.location == getattr(other, 'location', None))
    return NotImplemented

  def __hash__(self):
    return hash((self.jobId, self.projectId, self.location))

  def __repr__(self):
    return (
        f"JobReference(jobId={self.jobId!r}, "
        f"projectId={self.projectId!r}, "
        f"location={self.location!r})")


try:
  from apitools.base.protorpclite import messages as _protorpclite_messages
  if hasattr(_protorpclite_messages, 'Message'):
    _orig_message_eq = _protorpclite_messages.Message.__eq__

    def _message_compat_eq(self, other):
      if isinstance(other, JobReference) and apitools_bigquery and hasattr(
          apitools_bigquery, 'JobReference') and isinstance(
              self, apitools_bigquery.JobReference):
        return (
            getattr(self, 'jobId', None) == other.jobId and
            getattr(self, 'projectId', None) == other.projectId and
            getattr(self, 'location', None) == other.location)
      if isinstance(other, TableReference) and apitools_bigquery and hasattr(
          apitools_bigquery, 'TableReference') and isinstance(
              self, apitools_bigquery.TableReference):
        return (
            getattr(self, 'projectId', None) == other.projectId and
            getattr(self, 'datasetId', None) == other.datasetId and
            getattr(self, 'tableId', None) == other.tableId)
      if isinstance(other, DatasetReference) and apitools_bigquery and hasattr(
          apitools_bigquery, 'DatasetReference') and isinstance(
              self, apitools_bigquery.DatasetReference):
        return (
            getattr(self, 'projectId', None) == other.projectId and
            getattr(self, 'datasetId', None) == other.datasetId)
      return _orig_message_eq(self, other)

    _protorpclite_messages.Message.__eq__ = _message_compat_eq
except ImportError:
  _protorpclite_messages = None


def _set_table_ref_prop(ref, prop, val):
  if hasattr(ref, '_properties') and isinstance(ref._properties, dict):
    ref._properties[prop] = val
  if prop == 'projectId':
    setattr(ref, '_project', val)
  elif prop == 'datasetId':
    setattr(ref, '_dataset_id', val)
  elif prop == 'tableId':
    setattr(ref, '_table_id', val)


# Monkey-patch gcp_bigquery classes to ensure full backward compatibility
if gcp_bigquery:
  if not hasattr(gcp_bigquery.TableReference, 'projectId'):
    gcp_bigquery.TableReference.projectId = property(
        lambda self: self.project,
        lambda self, val: _set_table_ref_prop(self, 'projectId', val))
    gcp_bigquery.TableReference.datasetId = property(
        lambda self: self.dataset_id,
        lambda self, val: _set_table_ref_prop(self, 'datasetId', val))
    gcp_bigquery.TableReference.tableId = property(
        lambda self: self.table_id,
        lambda self, val: _set_table_ref_prop(self, 'tableId', val))

  if not hasattr(gcp_bigquery.DatasetReference, 'projectId'):
    gcp_bigquery.DatasetReference.projectId = property(
        lambda self: self.project,
        lambda self, val: setattr(self, '_project', val))
    gcp_bigquery.DatasetReference.datasetId = property(
        lambda self: self.dataset_id,
        lambda self, val: setattr(self, '_dataset_id', val))

  if not hasattr(gcp_bigquery.SchemaField, 'type'):
    gcp_bigquery.SchemaField.type = property(
        lambda self: self.field_type,
        lambda self, val: setattr(self, '_field_type', val))

  if not hasattr(gcp_bigquery.Table, 'tableReference'):
    gcp_bigquery.Table.tableReference = property(lambda self: self.reference)
    gcp_bigquery.Table.numRows = property(lambda self: self.num_rows)
    gcp_bigquery.Table.numBytes = property(lambda self: self.num_bytes)
    gcp_bigquery.Table.timePartitioning = property(
        lambda self: self.time_partitioning)
    gcp_bigquery.Table.rangePartitioning = property(
        lambda self: self.range_partitioning)

  if hasattr(gcp_bigquery, 'TimePartitioning'):
    if not hasattr(gcp_bigquery.TimePartitioning, 'type'):
      gcp_bigquery.TimePartitioning.type = property(
          lambda self: self.type_,
          lambda self, val: setattr(self, 'type_', val))
    if not hasattr(gcp_bigquery.TimePartitioning, 'expirationMs'):
      gcp_bigquery.TimePartitioning.expirationMs = property(
          lambda self: self.expiration_ms,
          lambda self, val: setattr(self, 'expiration_ms', val))
    if not hasattr(gcp_bigquery.TimePartitioning, 'requirePartitionFilter'):
      gcp_bigquery.TimePartitioning.requirePartitionFilter = property(
          lambda self: self.require_partition_filter,
          lambda self, val: setattr(self, 'require_partition_filter', val))

  if hasattr(gcp_bigquery, 'RangePartitioning'):
    if not hasattr(gcp_bigquery.RangePartitioning, 'range'):
      gcp_bigquery.RangePartitioning.range = property(
          lambda self: self.range_,
          lambda self, val: setattr(self, 'range_', val))

  if not hasattr(gcp_bigquery.Dataset, 'datasetReference'):
    gcp_bigquery.Dataset.datasetReference = property(
        lambda self: self.reference)
    gcp_bigquery.Dataset.defaultTableExpirationMs = property(
        lambda self: self.default_table_expiration_ms,
        lambda self, val: setattr(self, 'default_table_expiration_ms', val))

  if hasattr(gcp_bigquery, 'LoadJobConfig'):
    if not hasattr(gcp_bigquery.LoadJobConfig, 'schemaUpdateOptions'):
      gcp_bigquery.LoadJobConfig.schemaUpdateOptions = property(
          lambda self: self.schema_update_options,
          lambda self, val: setattr(self, 'schema_update_options', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'ignoreUnknownValues'):
      gcp_bigquery.LoadJobConfig.ignoreUnknownValues = property(
          lambda self: self.ignore_unknown_values,
          lambda self, val: setattr(self, 'ignore_unknown_values', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'maxBadRecords'):
      gcp_bigquery.LoadJobConfig.maxBadRecords = property(
          lambda self: self.max_bad_records,
          lambda self, val: setattr(self, 'max_bad_records', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'nullMarker'):
      gcp_bigquery.LoadJobConfig.nullMarker = property(
          lambda self: self.null_marker,
          lambda self, val: setattr(self, 'null_marker', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'fieldDelimiter'):
      gcp_bigquery.LoadJobConfig.fieldDelimiter = property(
          lambda self: self.field_delimiter,
          lambda self, val: setattr(self, 'field_delimiter', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'skipLeadingRows'):
      gcp_bigquery.LoadJobConfig.skipLeadingRows = property(
          lambda self: self.skip_leading_rows,
          lambda self, val: setattr(self, 'skip_leading_rows', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'allowJaggedRows'):
      gcp_bigquery.LoadJobConfig.allowJaggedRows = property(
          lambda self: self.allow_jagged_rows,
          lambda self, val: setattr(self, 'allow_jagged_rows', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'allowQuotedNewlines'):
      gcp_bigquery.LoadJobConfig.allowQuotedNewlines = property(
          lambda self: self.allow_quoted_newlines,
          lambda self, val: setattr(self, 'allow_quoted_newlines', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'decimalTargetTypes'):
      gcp_bigquery.LoadJobConfig.decimalTargetTypes = property(
          lambda self: self.decimal_target_types,
          lambda self, val: setattr(self, 'decimal_target_types', val))
    if not hasattr(gcp_bigquery.LoadJobConfig, 'useAvroLogicalTypes'):
      gcp_bigquery.LoadJobConfig.useAvroLogicalTypes = property(
          lambda self: self.use_avro_logical_types,
          lambda self, val: setattr(self, 'use_avro_logical_types', val))

  if hasattr(gcp_bigquery, 'QueryJobConfig'):
    if not hasattr(gcp_bigquery.QueryJobConfig, 'schemaUpdateOptions'):
      gcp_bigquery.QueryJobConfig.schemaUpdateOptions = property(
          lambda self: self.schema_update_options,
          lambda self, val: setattr(self, 'schema_update_options', val))
    if not hasattr(gcp_bigquery.QueryJobConfig, 'useLegacySql'):
      gcp_bigquery.QueryJobConfig.useLegacySql = property(
          lambda self: self.use_legacy_sql,
          lambda self, val: setattr(self, 'use_legacy_sql', val))
    if not hasattr(gcp_bigquery.QueryJobConfig, 'flattenResults'):
      gcp_bigquery.QueryJobConfig.flattenResults = property(
          lambda self: self.flatten_results,
          lambda self, val: setattr(self, 'flatten_results', val))
    if not hasattr(gcp_bigquery.QueryJobConfig, 'allowLargeResults'):
      gcp_bigquery.QueryJobConfig.allowLargeResults = property(
          lambda self: self.allow_large_results,
          lambda self, val: setattr(self, 'allow_large_results', val))
    if not hasattr(gcp_bigquery.QueryJobConfig, 'maximumBytesBilled'):
      gcp_bigquery.QueryJobConfig.maximumBytesBilled = property(
          lambda self: self.maximum_bytes_billed,
          lambda self, val: setattr(self, 'maximum_bytes_billed', val))

  if hasattr(gcp_bigquery, 'Table') and hasattr(gcp_bigquery.Table, 'labels'):
    _orig_tbl_labels_setter = gcp_bigquery.Table.labels.fset
    if _orig_tbl_labels_setter:

      def _safe_tbl_labels_setter(self, value):
        if value is None:
          value = {}
        elif not isinstance(value, dict) and hasattr(value,
                                                     'additionalProperties'):
          from apitools.base.py import encoding
          value = encoding.MessageToDict(value)
        _orig_tbl_labels_setter(self, value)

      gcp_bigquery.Table.labels = gcp_bigquery.Table.labels.setter(
          _safe_tbl_labels_setter)

  if hasattr(gcp_bigquery, 'Dataset') and hasattr(gcp_bigquery.Dataset,
                                                  'labels'):
    _orig_ds_labels_setter = gcp_bigquery.Dataset.labels.fset
    if _orig_ds_labels_setter:

      def _safe_ds_labels_setter(self, value):
        if value is None:
          value = {}
        elif not isinstance(value, dict) and hasattr(value,
                                                     'additionalProperties'):
          from apitools.base.py import encoding
          value = encoding.MessageToDict(value)
        _orig_ds_labels_setter(self, value)

      gcp_bigquery.Dataset.labels = gcp_bigquery.Dataset.labels.setter(
          _safe_ds_labels_setter)

  try:
    from google.cloud.bigquery.job.base import _JobConfig as _GcpJobConfig
    if hasattr(_GcpJobConfig, 'labels') and hasattr(_GcpJobConfig.labels,
                                                    'fset'):
      _orig_job_labels_setter = _GcpJobConfig.labels.fset
      if _orig_job_labels_setter:

        def _safe_job_labels_setter(self, value):
          if value is None:
            value = {}
          elif not isinstance(value, dict) and hasattr(value,
                                                       'additionalProperties'):
            from apitools.base.py import encoding
            value = encoding.MessageToDict(value)
          _orig_job_labels_setter(self, value)

        _GcpJobConfig.labels = _GcpJobConfig.labels.setter(
            _safe_job_labels_setter)
  except ImportError:
    pass

  if hasattr(gcp_job,
             '_AsyncJob') and not hasattr(gcp_job._AsyncJob, 'jobReference'):

    class _JobStatusCompat:
      def __init__(self, job):
        self._job = job

      @property
      def state(self):
        return self._job.state

      @property
      def errorResult(self):
        return self._job.error_result

      @property
      def errors(self):
        return self._job.errors

    class _JobStatsCompat:
      def __init__(self, job):
        self._job = job

      @property
      def query(self):
        return self

      @property
      def totalBytesBilled(self):
        return getattr(self._job, 'total_bytes_billed', None)

      @property
      def totalBytesProcessed(self):
        return getattr(self._job, 'total_bytes_processed', None)

      @property
      def referencedTables(self):
        tables = getattr(self._job, 'referenced_tables', None)
        if tables is not None:
          return [
              TableReference(
                  projectId=t.project,
                  datasetId=t.dataset_id,
                  tableId=t.table_id) for t in tables
          ]
        return None

    gcp_job._AsyncJob.jobReference = property(
        lambda self: JobReference(
            job_id=self.job_id, project=self.project, location=self.location))
    gcp_job._AsyncJob.status = property(lambda self: _JobStatusCompat(self))
    gcp_job._AsyncJob.statistics = property(lambda self: _JobStatsCompat(self))


def _to_json_compatible(obj):
  """Converts an object or nested structure to JSON/API-compatible dicts/types."""
  if obj is None:
    return None
  if isinstance(obj, (str, int, float, bool)):
    return obj
  if isinstance(obj, (list, tuple, set)):
    return [_to_json_compatible(item) for item in obj]
  if isinstance(obj, dict):
    return {k: _to_json_compatible(v) for k, v in obj.items()}
  if hasattr(obj, 'to_api_repr') and callable(obj.to_api_repr):
    return obj.to_api_repr()
  if _protorpclite_messages is not None and hasattr(
      _protorpclite_messages, 'Message') and isinstance(
          obj, _protorpclite_messages.Message):
    try:
      from apitools.base.py import encoding
      return encoding.MessageToDict(obj)
    except Exception:
      pass
  return obj


def _extract_dict_labels(labels):
  """Converts labels to a non-empty dictionary or returns None."""
  if not labels:
    return None
  labels = _to_json_compatible(labels)
  if isinstance(labels, dict) and labels:
    return labels
  return None


def _to_gcp_table_ref(table_ref, default_project=None):
  """Converts a TableReference or string into a google.cloud.bigquery.TableReference."""
  if table_ref is None:
    return None
  if gcp_bigquery is not None and isinstance(
      table_ref, getattr(gcp_bigquery, 'TableReference', ())):
    return table_ref
  if isinstance(table_ref, str):
    table_ref = parse_table_reference(table_ref, project=default_project)
  proj = getattr(table_ref, 'projectId', None) or getattr(
      table_ref, 'project', None) or getattr(
          table_ref, 'project_id', None) or default_project or 'default'
  dataset_id = getattr(table_ref, 'datasetId', None) or getattr(
      table_ref, 'dataset_id', None) or getattr(table_ref, 'dataset', None)
  table_id = getattr(table_ref, 'tableId', None) or getattr(
      table_ref, 'table_id', None) or getattr(table_ref, 'table', None)
  if dataset_id and table_id:
    if gcp_bigquery is not None and hasattr(
        gcp_bigquery, 'TableReference') and hasattr(gcp_bigquery,
                                                    'DatasetReference'):
      return gcp_bigquery.TableReference(
          gcp_bigquery.DatasetReference(proj, dataset_id), table_id)
    return _TableReferenceCompat(
        projectId=proj, datasetId=dataset_id, tableId=table_id)
  return table_ref


def _to_gcp_dataset_ref(dataset_ref, project=None):
  """Converts a DatasetReference or string into a google.cloud.bigquery.DatasetReference."""
  if dataset_ref is None:
    return None
  if gcp_bigquery is not None and isinstance(
      dataset_ref, getattr(gcp_bigquery, 'DatasetReference', ())):
    return dataset_ref
  if isinstance(dataset_ref, str):
    if ':' in dataset_ref or '.' in dataset_ref:
      if gcp_bigquery is not None and hasattr(gcp_bigquery.DatasetReference,
                                              'from_string'):
        return gcp_bigquery.DatasetReference.from_string(
            dataset_ref.replace(':', '.'), default_project=project)
      if ':' in dataset_ref:
        proj, ds_id = dataset_ref.split(':', 1)
      else:
        parts = dataset_ref.split('.', 1)
        proj, ds_id = parts[0], parts[1]
      return _DatasetReferenceCompat(projectId=proj, datasetId=ds_id)
    proj = project or 'default'
    if gcp_bigquery is not None and hasattr(gcp_bigquery, 'DatasetReference'):
      return gcp_bigquery.DatasetReference(proj, dataset_ref)
    return _DatasetReferenceCompat(projectId=proj, datasetId=dataset_ref)
  if hasattr(dataset_ref, 'projectId') or hasattr(dataset_ref, 'project'):
    proj = getattr(dataset_ref, 'projectId', None) or getattr(
        dataset_ref, 'project', None) or getattr(
            dataset_ref, 'project_id', None) or project or 'default'
    ds_id = getattr(dataset_ref, 'datasetId', None) or getattr(
        dataset_ref, 'dataset_id', None)
    if gcp_bigquery is not None and hasattr(gcp_bigquery, 'DatasetReference'):
      return gcp_bigquery.DatasetReference(proj, ds_id)
    return _DatasetReferenceCompat(projectId=proj, datasetId=ds_id)
  return dataset_ref


def _to_gcp_schema(schema):
  """Converts a TableSchema, list of fields, dict, or string into a list of google.cloud.bigquery.SchemaField."""
  if schema is None:
    return None
  if isinstance(schema, (list, tuple)):
    fields = []
    for f in schema:
      if gcp_bigquery is not None and isinstance(
          f, getattr(gcp_bigquery, 'SchemaField', ())):
        fields.append(f)
      elif isinstance(f, dict) and gcp_bigquery is not None:
        fields.append(gcp_bigquery.SchemaField.from_api_repr(f))
      elif hasattr(f, 'name') and gcp_bigquery is not None:
        dict_field = table_field_to_dict(f)
        if isinstance(dict_field, dict):
          fields.append(gcp_bigquery.SchemaField.from_api_repr(dict_field))
        else:
          fields.append(f)
      else:
        fields.append(f)
    return fields
  if isinstance(schema, TableSchema) or hasattr(schema, 'fields'):
    dict_schema = get_dict_table_schema(schema)
    if isinstance(dict_schema, dict) and gcp_bigquery is not None:
      return [
          gcp_bigquery.SchemaField.from_api_repr(f)
          for f in dict_schema.get('fields', [])
      ]
    if hasattr(schema, 'fields') and schema.fields is not None:
      return list(schema.fields)
  if isinstance(schema, dict):
    if gcp_bigquery is not None:
      return [
          gcp_bigquery.SchemaField.from_api_repr(f)
          for f in schema.get('fields', [])
      ]
    return schema.get('fields', [])
  if isinstance(schema, str):
    return _to_gcp_schema(get_dict_table_schema(schema))
  return schema


def _to_table_schema(schema):
  """Converts a list of google.cloud.bigquery.SchemaField, dict, or TableSchema into a TableSchema."""
  if schema is None:
    return TableSchema()
  if isinstance(schema, TableSchema):
    return schema
  if isinstance(schema, dict):
    return _to_table_schema(schema.get('fields', []))
  if hasattr(schema, 'fields') and not isinstance(schema, (list, tuple)):
    return _to_table_schema(schema.fields)

  def _to_field_schema(f):
    if isinstance(f, TableFieldSchema):
      return f
    if isinstance(f, dict):
      f_dict = f
    elif hasattr(f, 'to_api_repr'):
      f_dict = f.to_api_repr()
    else:
      f_dict = None

    if f_dict is not None:
      name = f_dict.get('name', '')
      field_type = f_dict.get('type') or f_dict.get('type_') or 'STRING'
      mode = f_dict.get('mode', 'NULLABLE')
      description = f_dict.get('description', None)
      sub_fields = [_to_field_schema(sf) for sf in f_dict.get('fields', [])]
      return TableFieldSchema(
          name=name,
          type=field_type,
          mode=mode,
          description=description,
          fields=sub_fields)

    name = getattr(f, 'name', '')
    field_type = getattr(f, 'field_type', None) or getattr(f, 'type',
                                                           None) or 'STRING'
    mode = getattr(f, 'mode', 'NULLABLE')
    description = getattr(f, 'description', None)
    sub = getattr(f, 'fields', ())
    sub_fields = [_to_field_schema(sf) for sf in sub] if sub else ()
    return TableFieldSchema(
        name=name,
        type=field_type,
        mode=mode,
        description=description,
        fields=sub_fields)

  if isinstance(schema, (list, tuple)):
    return TableSchema(fields=[_to_field_schema(f) for f in schema])
  return TableSchema()


if gcp_bigquery:

  class _ClientTablesCompat:
    def __init__(self, client):
      self._client = client

    def Get(self, request):
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      tbl_id = getattr(request, 'tableId', None)
      if ds_id and tbl_id:
        table_ref = gcp_bigquery.TableReference(
            gcp_bigquery.DatasetReference(
                proj or getattr(self._client, 'project', None) or 'default',
                ds_id),
            tbl_id)
      else:
        t_ref = getattr(request, 'tableReference', None) or getattr(
            request, 'tableId', None) or request
        table_ref = _to_gcp_table_ref(
            t_ref,
            default_project=proj or getattr(self._client, 'project', None))
      return self._client.get_table(table_ref)

    def Insert(self, request):
      table = getattr(request, 'table', None)
      if table is not None:
        t_ref = getattr(table, 'tableReference', None)
        proj = getattr(t_ref, 'projectId', None) or getattr(
            request, 'projectId', None)
        ds_id = getattr(t_ref, 'datasetId', None) or getattr(
            request, 'datasetId', None)
        tbl_id = getattr(t_ref, 'tableId', None)
        schema = getattr(table, 'schema', None)
      else:
        proj = getattr(request, 'projectId', None)
        ds_id = getattr(request, 'datasetId', None)
        tbl_id = getattr(request, 'tableId', None)
        schema = getattr(request, 'schema', None)
      gcp_tbl_ref = gcp_bigquery.TableReference(
          gcp_bigquery.DatasetReference(
              proj or getattr(self._client, 'project', None) or 'default',
              ds_id),
          tbl_id)
      gcp_table = gcp_bigquery.Table(gcp_tbl_ref, schema=_to_gcp_schema(schema))
      if table is not None:
        tp = getattr(table, 'timePartitioning', None) or getattr(
            table, 'time_partitioning', None)
        if tp is not None:
          if isinstance(tp, gcp_bigquery.TimePartitioning):
            gcp_table.time_partitioning = tp
          else:
            tp_field = getattr(tp, 'field', None)
            tp_type = getattr(tp, 'type', None) or getattr(tp, 'type_', None)
            tp_exp = getattr(tp, 'expirationMs', None) or getattr(
                tp, 'expiration_ms', None)
            tp_req = getattr(tp, 'requirePartitionFilter', None) or getattr(
                tp, 'require_partition_filter', None)
            gcp_table.time_partitioning = gcp_bigquery.TimePartitioning(
                type_=tp_type,
                field=tp_field,
                expiration_ms=tp_exp,
                require_partition_filter=tp_req)
        rp = getattr(table, 'rangePartitioning', None) or getattr(
            table, 'range_partitioning', None)
        if rp is not None:
          if isinstance(rp, gcp_bigquery.RangePartitioning):
            gcp_table.range_partitioning = rp
          else:
            rp_field = getattr(rp, 'field', None)
            rp_range = getattr(rp, 'range', None) or getattr(rp, 'range_', None)
            if rp_range is not None and hasattr(gcp_bigquery, 'PartitionRange'):
              start = getattr(rp_range, 'start', None)
              end = getattr(rp_range, 'end', None)
              interval = getattr(rp_range, 'interval', None)
              rp_range = gcp_bigquery.PartitionRange(
                  start=start, end=end, interval=interval)
            gcp_table.range_partitioning = gcp_bigquery.RangePartitioning(
                field=rp_field, range_=rp_range)
        clustering = getattr(table, 'clustering', None)
        if clustering is not None:
          fields = getattr(clustering, 'fields', clustering)
          if isinstance(fields, (list, tuple)):
            gcp_table.clustering_fields = list(fields)
        if getattr(table, 'description', None):
          gcp_table.description = table.description
        if getattr(table, 'friendlyName', None) or getattr(
            table, 'friendly_name', None):
          gcp_table.friendly_name = getattr(
              table, 'friendlyName', None) or getattr(
                  table, 'friendly_name', None)
        dict_labels = _extract_dict_labels(getattr(table, 'labels', None))
        if dict_labels:
          gcp_table.labels = dict_labels
        kms = getattr(
            getattr(table, 'encryptionConfiguration', None),
            'kmsKeyName',
            None) or getattr(
                getattr(table, 'encryption_configuration', None),
                'kms_key_name',
                None)
        if kms:
          gcp_table.encryption_configuration = (
              gcp_bigquery.EncryptionConfiguration(kms_key_name=kms))
      return self._client.create_table(gcp_table, exists_ok=True)

    def Delete(self, request):
      t_ref = getattr(request, 'tableReference', None)
      proj = getattr(t_ref, 'projectId', None) or getattr(
          request, 'projectId', None)
      ds_id = getattr(t_ref, 'datasetId', None) or getattr(
          request, 'datasetId', None)
      tbl_id = getattr(t_ref, 'tableId', None) or getattr(
          request, 'tableId', None)
      gcp_tbl_ref = gcp_bigquery.TableReference(
          gcp_bigquery.DatasetReference(
              proj or getattr(self._client, 'project', None) or 'default',
              ds_id),
          tbl_id)
      return self._client.delete_table(gcp_tbl_ref, not_found_ok=True)

    def List(self, request):
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      ds_ref = gcp_bigquery.DatasetReference(
          proj or getattr(self._client, 'project', None) or 'default', ds_id)
      return self._client.list_tables(ds_ref)

    def Patch(self, request):
      table = getattr(request, 'table', None)
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      tbl_id = getattr(request, 'tableId', None)
      gcp_tbl_ref = gcp_bigquery.TableReference(
          gcp_bigquery.DatasetReference(
              proj or getattr(self._client, 'project', None) or 'default',
              ds_id),
          tbl_id)
      gcp_table = gcp_bigquery.Table(gcp_tbl_ref)
      if table and getattr(table, 'schema', None):
        gcp_table.schema = _to_gcp_schema(table.schema)
      return self._client.update_table(gcp_table, ['schema'])

    def Update(self, request):
      return self.Patch(request)

  class _ClientDatasetsCompat:
    def __init__(self, client):
      self._client = client

    def Get(self, request):
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      ds_ref = gcp_bigquery.DatasetReference(
          proj or getattr(self._client, 'project', None) or 'default', ds_id)
      return self._client.get_dataset(ds_ref)

    def Insert(self, request):
      dataset = getattr(request, 'dataset', None)
      ds_ref_raw = getattr(
          dataset, 'datasetReference', None) if dataset else None
      proj = getattr(ds_ref_raw, 'projectId', None) or getattr(
          request, 'projectId', None)
      ds_id = getattr(ds_ref_raw, 'datasetId', None) or getattr(
          request, 'datasetId', None)
      ds_ref = gcp_bigquery.DatasetReference(
          proj or getattr(self._client, 'project', None) or 'default', ds_id)
      gcp_ds = gcp_bigquery.Dataset(ds_ref)
      if dataset:
        if getattr(dataset, 'location', None):
          gcp_ds.location = dataset.location
        if getattr(dataset, 'defaultTableExpirationMs', None):
          gcp_ds.default_table_expiration_ms = dataset.defaultTableExpirationMs
      return self._client.create_dataset(gcp_ds, exists_ok=True)

    def Delete(self, request):
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      delete_contents = getattr(request, 'deleteContents', True)
      ds_ref = gcp_bigquery.DatasetReference(
          proj or getattr(self._client, 'project', None) or 'default', ds_id)
      return self._client.delete_dataset(
          ds_ref, delete_contents=delete_contents, not_found_ok=True)

    def List(self, request):
      proj = getattr(request, 'projectId', None) or getattr(
          self._client, 'project', None)
      return self._client.list_datasets(project=proj)

    def Patch(self, request):
      dataset = getattr(request, 'dataset', None)
      proj = getattr(request, 'projectId', None)
      ds_id = getattr(request, 'datasetId', None)
      ds_ref = gcp_bigquery.DatasetReference(
          proj or getattr(self._client, 'project', None) or 'default', ds_id)
      gcp_ds = gcp_bigquery.Dataset(ds_ref)
      fields_to_update = []
      if dataset:
        if getattr(dataset, 'defaultTableExpirationMs', None):
          gcp_ds.default_table_expiration_ms = dataset.defaultTableExpirationMs
          fields_to_update.append('default_table_expiration_ms')
      return self._client.update_dataset(gcp_ds, fields_to_update)

    def Update(self, request):
      return self.Patch(request)

  class _ClientJobsCompat:
    def __init__(self, client):
      self._client = client

    def Get(self, request):
      proj = getattr(request, 'projectId', None)
      job_id = getattr(request, 'jobId', None)
      loc = getattr(request, 'location', None)
      return self._client.get_job(job_id, project=proj, location=loc)

    def GetQueryResults(self, request):
      proj = getattr(request, 'projectId', None)
      job_id = getattr(request, 'jobId', None)
      loc = getattr(request, 'location', None)
      page_token = getattr(request, 'pageToken', None)
      max_results = getattr(request, 'maxResults', None)
      job = self._client.get_job(job_id, project=proj, location=loc)
      if page_token is not None:
        return self._client.list_rows(
            job, page_token=page_token, max_results=max_results)
      return job.result(max_results=max_results)

    def Insert(self, request, upload=None):
      job_obj = getattr(request, 'job', None)
      job_ref = (
          getattr(job_obj, 'jobReference', None)
          if job_obj else getattr(request, 'jobReference', None))
      job_id = getattr(job_ref, 'jobId', None) or getattr(
          job_ref, 'job_id', None)
      proj = (
          getattr(request, 'projectId', None) or
          getattr(job_ref, 'projectId', None) or
          getattr(job_ref, 'project', None))
      config = getattr(job_obj, 'configuration', None) if job_obj else None
      if config and getattr(config, 'query', None):
        q = config.query
        dest = None
        if getattr(q, 'destinationTable', None):
          dest = _to_gcp_table_ref(q.destinationTable, default_project=proj)
        dict_labels = _extract_dict_labels(getattr(config, 'labels', None))
        job_config = gcp_bigquery.QueryJobConfig(
            dry_run=getattr(q, 'dryRun', False),
            use_legacy_sql=getattr(q, 'useLegacySql', False)
            if getattr(q, 'useLegacySql', None) is not None else False,
            flatten_results=getattr(q, 'flattenResults', None),
            priority=getattr(q, 'priority', 'INTERACTIVE'),
            destination=dest,
        )
        if dict_labels:
          job_config.labels = dict_labels
        kms = getattr(
            getattr(q, 'destinationEncryptionConfiguration', None),
            'kmsKeyName',
            None)
        if kms:
          job_config.destination_encryption_configuration = (
              gcp_bigquery.EncryptionConfiguration(kms_key_name=kms))
        return self._client.query(
            q.query,
            job_config=job_config,
            job_id=job_id,
            project=proj,
            job_retry=None,
        )
      elif config and getattr(config, 'load', None):
        ld = config.load
        dest = _to_gcp_table_ref(
            getattr(ld, 'destinationTable', None), default_project=proj)
        uris = list(getattr(ld, 'sourceUris', []))
        if uris:
          return self._client.load_table_from_uri(
              uris, dest, job_id=job_id, project=proj)
      elif config and getattr(config, 'copy', None):
        cp = config.copy
        sources = [
            _to_gcp_table_ref(s, default_project=proj)
            for s in getattr(cp, 'sourceTables', [])
        ]
        dest = _to_gcp_table_ref(
            getattr(cp, 'destinationTable', None), default_project=proj)
        return self._client.copy_table(
            sources, dest, job_id=job_id, project=proj)
      elif config and getattr(config, 'extract', None):
        ex = config.extract
        src = _to_gcp_table_ref(
            getattr(ex, 'sourceTable', None), default_project=proj)
        uris = list(getattr(ex, 'destinationUris', []))
        return self._client.extract_table(
            src, uris, job_id=job_id, project=proj)

      return self._client.get_job(job_id, project=proj)

  if not hasattr(gcp_bigquery.Client, 'tables'):
    gcp_bigquery.Client.tables = property(
        lambda self: _ClientTablesCompat(self))
  if not hasattr(gcp_bigquery.Client, 'datasets'):
    gcp_bigquery.Client.datasets = property(
        lambda self: _ClientDatasetsCompat(self))
  if not hasattr(gcp_bigquery.Client, 'jobs'):
    gcp_bigquery.Client.jobs = property(lambda self: _ClientJobsCompat(self))

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
    "GEOGRAPHY": str,
}

# Duplicated logic with io/gcp/bigquery_change_history.py
# Default table expiration for auto-created temp datasets: 24 hours in ms.
# Tables created in the dataset auto-expire after this duration if not
# explicitly deleted, acting as a safety net for orphaned temp tables
# (e.g. pipeline crash before cleanup runs).
_DEFAULT_TABLE_EXPIRATION_MS = 24 * 60 * 60 * 1000


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
  elif isinstance(obj, apache_beam.utils.timestamp.Timestamp):
    return obj.to_utc_datetime().isoformat()
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
    table_ref_elem_kv: tuple[Union[str, TableReference], V]) -> tuple[str, V]:
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
    name = field['name']
    field_type = field.get('type') or field.get('type_') or 'STRING'
    mode = field.get('mode', 'NULLABLE')
    description = field.get('description', None)
    sub_fields = [_parse_schema_field(x)
                  for x in field.get('fields', [])] if 'fields' in field else ()
    return TableFieldSchema(
        name=name,
        type=field_type,
        mode=mode,
        description=description,
        fields=sub_fields)

  fields = [_parse_schema_field(f) for f in json_schema.get('fields', [])]
  return TableSchema(fields=fields)


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
  elif isinstance(table, getattr(gcp_bigquery, 'TableReference', ())):
    return TableReference(
        projectId=table.project,
        datasetId=table.dataset_id,
        tableId=table.table_id)
  elif callable(table):
    return table
  elif isinstance(table, value_provider.ValueProvider):
    return table

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
    return TableReference(
        projectId=match.group('project'),
        datasetId=match.group('dataset'),
        tableId=match.group('table'))
  else:
    return TableReference(projectId=project, datasetId=dataset, tableId=table)


# -----------------------------------------------------------------------------
# BigQueryWrapper.


def _build_job_labels(input_labels):
  """Builds job label dictionary or protobuf structure."""
  if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                               'JobConfiguration'):
    input_labels = input_labels or {}
    result = apitools_bigquery.JobConfiguration.LabelsValue()
    for k, v in input_labels.items():
      result.additionalProperties.append(
          apitools_bigquery.JobConfiguration.LabelsValue.AdditionalProperty(
              key=k,
              value=v,
          ))
    return result
  return input_labels or {}


def _build_dataset_labels(input_labels):
  """Builds dataset label dictionary or protobuf structure."""
  if apitools_bigquery is not None and hasattr(apitools_bigquery, 'Dataset'):
    input_labels = input_labels or {}
    result = apitools_bigquery.Dataset.LabelsValue()
    for k, v in input_labels.items():
      result.additionalProperties.append(
          apitools_bigquery.Dataset.LabelsValue.AdditionalProperty(
              key=k,
              value=v,
          ))
    return result
  return input_labels or {}


def _build_filter_from_labels(labels):
  filter_str = ''
  for key, value in labels.items():
    filter_str += 'labels.' + key + ':' + value + ' '
  return filter_str


def _build_dataset_encryption_config(kms_key):
  if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                               'EncryptionConfiguration'):
    return apitools_bigquery.EncryptionConfiguration(kmsKeyName=kms_key)
  return kms_key


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
      temp_table_ref=None,
      use_legacy_client=False):
    if client is not None:
      self.client = client
      self.gcp_bq_client = client
    else:
      self.client = BigQueryWrapper._bigquery_client(
          PipelineOptions(), use_legacy_client=use_legacy_client)
      self.gcp_bq_client = self.client

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
  def _is_modern_client(self):
    client_cls = getattr(gcp_bigquery, 'Client', None)
    if isinstance(client_cls, type) and isinstance(self.client, client_cls):
      return True
    if hasattr(self.client, '_mock_children') or hasattr(self.client,
                                                         '_mock_methods'):
      mock_keys = set(getattr(self.client, '__dict__', {}).keys()) | set(
          getattr(self.client, '_mock_children', {}).keys())
      for modern_attr in ('insert_rows_json',
                          'load_table_from_uri',
                          'load_table_from_file',
                          'query',
                          'create_table',
                          'delete_table',
                          'list_datasets'):
        if modern_attr in mock_keys:
          return True
      return False
    if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                                 'BigqueryV2'):
      if isinstance(self.client, apitools_bigquery.BigqueryV2):
        return False
    return client_cls is not None and isinstance(self.client, client_cls)

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

  def _get_temp_table_project(self, fallback_project_id):
    """Returns the project ID for temporary table operations.

    If temp_table_ref exists, returns its projectId.
    Otherwise, returns the fallback_project_id.
    """
    if self.temp_table_ref:
      return self.temp_table_ref.projectId
    else:
      return fallback_project_id

  def _get_temp_dataset(self):
    if self.temp_table_ref:
      return self.temp_table_ref.datasetId
    return BigQueryWrapper.TEMP_DATASET + self._temporary_table_suffix

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_query_location(self, project_id, query, use_legacy_sql):
    """Get the location of tables referenced in a query.

    This method returns the location of the first available referenced
    table for user in the query and depends on the BigQuery service to
    provide error handling for queries that reference tables in multiple
    locations.
    """
    if self._is_modern_client:
      job_config = gcp_bigquery.QueryJobConfig(
          dry_run=True,
          use_legacy_sql=use_legacy_sql,
      )
      try:
        response = self.client.query(
            query, job_config=job_config, project=project_id)
      except (Forbidden, HttpForbiddenError):
        # Permission access for table (i.e. from authorized_view),
        # try next one
        return None
      except Exception:
        raise

      if hasattr(response, 'referenced_tables'):
        referenced_tables = response.referenced_tables
      elif hasattr(response, 'statistics') and response.statistics is not None:
        referenced_tables = getattr(
            response.statistics.query, 'referencedTables', None)
      else:
        # This behavior is only expected in tests
        _LOGGER.warning(
            "Unable to get location, missing response.statistics. Query: %s",
            query)
        return None

      if referenced_tables:  # Guards against both non-empty and non-None
        for table in referenced_tables:
          try:
            p = getattr(table, 'project', None) or getattr(
                table, 'projectId', None)
            d = getattr(table, 'dataset_id', None) or getattr(
                table, 'datasetId', None)
            t = getattr(table, 'table_id', None) or getattr(
                table, 'tableId', None)
            location = self.get_table_location(p, d, t)
          except (Forbidden, HttpForbiddenError, ClientError):
            # Permission access for table (i.e. from authorized_view),
            # try next one
            continue
          if location:
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

    # Fallback if legacy client.jobs.Insert is mocked
    reference = (
        apitools_bigquery.JobReference(
            jobId=uuid.uuid4().hex, projectId=project_id)
        if apitools_bigquery and hasattr(apitools_bigquery, 'JobReference') else
        JobReference(jobId=uuid.uuid4().hex, projectId=project_id))
    request = apitools_bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=apitools_bigquery.Job(
            configuration=apitools_bigquery.JobConfiguration(
                dryRun=True,
                query=apitools_bigquery.JobConfigurationQuery(
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
        except (HttpForbiddenError, Forbidden):
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
    if self._is_modern_client:
      dict_labels = _extract_dict_labels(job_labels)
      job_config = gcp_bigquery.CopyJobConfig(
          create_disposition=create_disposition,
          write_disposition=write_disposition,
      )
      if dict_labels:
        job_config.labels = dict_labels
      src = from_table_reference if isinstance(
          from_table_reference, list) else [from_table_reference]
      src_refs = [_to_gcp_table_ref(t, default_project=project_id) for t in src]
      dst_ref = _to_gcp_table_ref(
          to_table_reference, default_project=project_id)
      try:
        job = self.client.copy_table(
            src_refs,
            dst_ref,
            job_id=job_id,
            job_config=job_config,
            project=project_id,
        )
        return JobReference(
            job_id=job.job_id, project=job.project, location=job.location)
      except (Conflict, HttpError) as exn:
        if getattr(exn, 'code', None) == 409 or getattr(
            exn, 'status_code', None) == 409 or isinstance(exn, Conflict):
          _LOGGER.info(
              "BigQuery copy job %s already exists, will not retry inserting it: %s",
              job_id,
              exn)
          return JobReference(job_id=job_id, project=project_id)
        raise

    # Fallback if legacy client.jobs.Insert is mocked
    reference = (
        apitools_bigquery.JobReference(jobId=job_id, projectId=project_id)
        if apitools_bigquery and hasattr(apitools_bigquery, 'JobReference') else
        JobReference(jobId=job_id, projectId=project_id))
    request = apitools_bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=apitools_bigquery.Job(
            configuration=apitools_bigquery.JobConfiguration(
                copy=apitools_bigquery.JobConfigurationTableCopy(
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

    if self._is_modern_client:
      dst_ref = _to_gcp_table_ref(table_reference, default_project=project_id)
      job_schema = None if schema == 'SCHEMA_AUTODETECT' else _to_gcp_schema(
          schema)
      autodetect = schema == 'SCHEMA_AUTODETECT'
      clean_params = _to_json_compatible(additional_load_parameters or {})
      api_repr_dict = {}
      direct_attrs = {}
      if isinstance(clean_params, dict):
        for k, v in clean_params.items():
          if '_' in k and hasattr(gcp_bigquery.LoadJobConfig, k):
            direct_attrs[k] = v
          else:
            api_repr_dict[k] = v

      if api_repr_dict:
        job_config = gcp_bigquery.LoadJobConfig.from_api_repr(
            {'load': api_repr_dict})
      else:
        job_config = gcp_bigquery.LoadJobConfig()

      for k, v in direct_attrs.items():
        try:
          setattr(job_config, k, v)
        except Exception:
          try:
            job_config._set_sub_prop(k, v)
          except Exception:
            pass

      if job_schema is not None:
        job_config.schema = job_schema
      if autodetect:
        job_config.autodetect = True
      if create_disposition is not None:
        job_config.create_disposition = create_disposition
      if write_disposition is not None:
        job_config.write_disposition = write_disposition
      if source_format is not None:
        job_config.source_format = source_format
      job_config.use_avro_logical_types = True
      dict_labels = _extract_dict_labels(job_labels)
      if dict_labels:
        job_config.labels = dict_labels
      try:
        if source_stream:
          job = self.client.load_table_from_file(
              source_stream,
              dst_ref,
              job_id=job_id,
              job_config=job_config,
              project=project_id)
        else:
          source_uris = source_uris or []
          job = self.client.load_table_from_uri(
              source_uris,
              dst_ref,
              job_id=job_id,
              job_config=job_config,
              project=project_id)
        return JobReference(
            job_id=job.job_id, project=job.project, location=job.location)
      except (Conflict, HttpError) as exn:
        if getattr(exn, 'code', None) == 409 or getattr(
            exn, 'status_code', None) == 409 or isinstance(exn, Conflict):
          _LOGGER.info(
              "BigQuery load job %s already exists, will not retry inserting it: %s",
              job_id,
              exn)
          return JobReference(job_id=job_id, project=project_id)
        raise

    # Fallback if legacy client.jobs.Insert is mocked
    if source_uris is None:
      source_uris = []
    additional_load_parameters = additional_load_parameters or {}
    if schema == 'SCHEMA_AUTODETECT':
      job_schema = None
    elif isinstance(schema, (dict, str)):
      job_schema = get_bq_tableschema(schema)
    elif isinstance(schema, (list, tuple)):
      job_schema = get_bq_tableschema(
          {'fields': [table_field_to_dict(f) for f in schema]})
    else:
      job_schema = schema
    reference = (
        apitools_bigquery.JobReference(jobId=job_id, projectId=project_id)
        if apitools_bigquery and hasattr(apitools_bigquery, 'JobReference') else
        JobReference(jobId=job_id, projectId=project_id))
    request = apitools_bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=apitools_bigquery.Job(
            configuration=apitools_bigquery.JobConfiguration(
                load=apitools_bigquery.JobConfigurationLoad(
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
      request: 'apitools_bigquery.BigqueryJobsInsertRequest',
      stream=None,
  ):
    """Inserts a BigQuery job for legacy apitools client.

    If the job exists already, it returns it.

    Args:
      request (bigquery.BigqueryJobsInsertRequest): An insert job request.
      stream (IO[bytes]): A bytes IO object open for reading.
    """
    try:
      upload = None
      if stream and Upload:
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
      job_labels=None,
      destination_table=None):
    if self._is_modern_client:
      dest_table = None
      if not dry_run:
        dest_table = _to_gcp_table_ref(
            destination_table or
            self._get_temp_table(self._get_temp_table_project(project_id)),
            default_project=project_id or getattr(self.client, 'project', None))

      dict_labels = _extract_dict_labels(job_labels)
      job_config = gcp_bigquery.QueryJobConfig(
          dry_run=dry_run,
          use_legacy_sql=use_legacy_sql,
          allow_large_results=not dry_run,
          destination=dest_table,
          flatten_results=flatten_results,
          priority=priority,
      )
      if dict_labels:
        job_config.labels = dict_labels
      if kms_key:
        job_config.destination_encryption_configuration = (
            gcp_bigquery.EncryptionConfiguration(kms_key_name=kms_key))

      try:
        job = self.client.query(
            query,
            job_config=job_config,
            job_id=job_id,
            project=project_id,
            job_retry=None,
        )
        return job
      except (Conflict, HttpError) as exn:
        if getattr(exn, 'code', None) == 409 or getattr(
            exn, 'status_code', None) == 409 or isinstance(exn, Conflict):
          return self.get_job(project_id, job_id)
        raise

    # Fallback if legacy client.jobs.Insert is mocked
    reference = (
        apitools_bigquery.JobReference(jobId=job_id, projectId=project_id)
        if apitools_bigquery and hasattr(apitools_bigquery, 'JobReference') else
        JobReference(jobId=job_id, projectId=project_id))
    request = apitools_bigquery.BigqueryJobsInsertRequest(
        projectId=project_id,
        job=apitools_bigquery.Job(
            configuration=apitools_bigquery.JobConfiguration(
                dryRun=dry_run,
                query=apitools_bigquery.JobConfigurationQuery(
                    query=query,
                    useLegacySql=use_legacy_sql,
                    allowLargeResults=not dry_run,
                    destinationTable=(
                        destination_table if destination_table is not None else
                        (
                            self._get_temp_table(
                                self._get_temp_table_project(project_id))
                            if not dry_run else None)),
                    flattenResults=flatten_results,
                    priority=priority,
                    destinationEncryptionConfiguration=apitools_bigquery.
                    EncryptionConfiguration(kmsKeyName=kms_key)),
                labels=_build_job_labels(job_labels),
            ),
            jobReference=reference))
    return self._start_job(request)

  def wait_for_bq_job(self, job_reference, sleep_duration_sec=5, max_retries=0):
    """Poll job until it is DONE.

    Args:
      job_reference: JobReference instance or job ID string.
      sleep_duration_sec: Specifies the delay in seconds between retries.
      max_retries: The total number of times to retry. If equals to 0,
        the function waits forever.

    Raises:
      `RuntimeError`: If the job is FAILED or the number of retries has been
        reached.
    """
    retry = 0
    project = getattr(job_reference, 'projectId', None) or getattr(
        job_reference, 'project', None)
    job_id = getattr(job_reference, 'jobId', None) or getattr(
        job_reference, 'job_id', None)
    location = getattr(job_reference, 'location', None)

    while True:
      retry += 1
      job = self.get_job(project, job_id, location)
      status_obj = getattr(job, 'status', None)
      if status_obj is not None and hasattr(status_obj, 'state') and isinstance(
          status_obj.state, str):
        state = status_obj.state
        error_result = getattr(status_obj, 'errorResult', None)
      elif hasattr(job, 'state') and isinstance(job.state, str):
        state = job.state
        error_result = getattr(job, 'error_result', None)
      else:
        state = getattr(job, 'state', None) or getattr(
            status_obj, 'state', None)
        error_result = getattr(job, 'error_result', None) or getattr(
            status_obj, 'errorResult', None)

      _LOGGER.info('Job %s status: %s', job_id, state)
      if state == 'DONE' and error_result:
        raise RuntimeError(
            'BigQuery job {} failed. Error Result: {}'.format(
                job_id, error_result))
      elif state == 'DONE':
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
    if hasattr(self.client, 'jobs') and hasattr(self.client.jobs,
                                                'GetQueryResults'):
      request = apitools_bigquery.BigqueryJobsGetQueryResultsRequest(
          jobId=job_id,
          pageToken=page_token,
          projectId=project_id,
          maxResults=max_results,
          location=location)
      return self.client.jobs.GetQueryResults(request)

    job = self.client.get_job(job_id, project=project_id, location=location)
    if page_token is not None:
      return self.client.list_rows(
          job, page_token=page_token, max_results=max_results)
    return job.result(max_results=max_results)

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
    # The rows argument is a list of plain Python dictionaries or rows.
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

    if self._is_modern_client:
      started_millis = int(time.time() * 1000)
      try:
        table_ref_str = '%s.%s.%s' % (project_id, dataset_id, table_id)
        row_ids_arg = insert_ids
        if row_ids_arg is not None and all(x is None for x in row_ids_arg):
          auto_uuid = getattr(
              getattr(gcp_bigquery, 'AutoRowIDs', None), 'GENERATE_UUID', None)
          if auto_uuid is not None:
            row_ids_arg = auto_uuid
        errors = self.gcp_bq_client.insert_rows_json(
            table_ref_str,
            json_rows=rows,
            row_ids=row_ids_arg,
            skip_invalid_rows=skip_invalid_rows,
            ignore_unknown_values=ignore_unknown_values,
            timeout=BQ_STREAMING_INSERT_TIMEOUT_SEC)
        if not errors:
          service_call_metric.call('ok')
        else:
          for insert_error in errors:
            for err in insert_error.get('errors', []):
              reason = err.get('reason') if isinstance(err, dict) else getattr(
                  err, 'reason', None)
              service_call_metric.call(reason or 'unknown')
      except (ClientError, GoogleAPICallError, HttpError) as e:
        # e.code contains the numeric http status code.
        status_code = getattr(e, 'code', None) or getattr(
            e, 'status_code', None) or 500
        service_call_metric.call(status_code)
        # Package exception with required fields
        reason = None
        if hasattr(e, 'response') and getattr(e.response, 'reason', None):
          reason = e.response.reason
        elif hasattr(e, 'reason') and e.reason:
          reason = e.reason
        elif hasattr(e, 'errors') and e.errors and isinstance(
            e.errors, (list, tuple)) and isinstance(e.errors[0], dict):
          reason = e.errors[0].get('reason')
        if not reason:
          reason = e.__class__.__name__
        # Add all rows to the errors list along with the error
        errors = [{
            'index': i, 'errors': [{
                'reason': reason, 'message': str(e)
            }]
        } for i in range(len(rows))]
        if not errors:
          errors = [{
              'index': 0, 'errors': [{
                  'reason': reason, 'message': str(e)
              }]
          }]
      finally:
        self._latency_histogram_metric.update(
            int(time.time() * 1000) - started_millis)
      return not errors, errors

    # Legacy apitools path
    # The rows argument is a list of
    # bigquery.TableDataInsertAllRequest.RowsValueListEntry instances as
    # required by the InsertAll() method.
    row_list = []
    for row, insert_id in zip(rows, insert_ids):
      row_list.append(
          apitools_bigquery.TableDataInsertAllRequest.RowsValueListEntry(
              insertId=insert_id,
              json=json_value.to_json_value(row, True),
          ))
    request = apitools_bigquery.BigqueryTabledataInsertAllRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id,
        tableDataInsertAllRequest=apitools_bigquery.TableDataInsertAllRequest(
            rows=row_list,
            skipInvalidRows=skip_invalid_rows,
            ignoreUnknownValues=ignore_unknown_values,
        ))
    started_millis = int(time.time() * 1000)
    try:
      response = self.client.tabledata.InsertAll(request)
      errors = [
          json.loads(extra_types.RpcError(e).to_json())
          for e in response.insertErrors
      ]
      if not errors:
        service_call_metric.call('ok')
      else:
        for insert_error in response.insertErrors:
          for error in insert_error.errors:
            service_call_metric.call(error.reason)
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
      project_id: table lookup parameter
      dataset_id: table lookup parameter
      table_id: table lookup parameter

    Returns:
      Table instance (bigquery.Table or google.cloud.bigquery.Table).
    Raises:
      NotFound or HttpError: if lookup failed.
    """
    if self._is_modern_client:
      table_ref = _to_gcp_table_ref(
          TableReference(
              projectId=project_id, datasetId=dataset_id, tableId=table_id),
          default_project=project_id)
      return self.client.get_table(table_ref)

    # Fallback for legacy client
    request = apitools_bigquery.BigqueryTablesGetRequest(
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

    if self._is_modern_client:
      table_ref = _to_gcp_table_ref(
          TableReference(
              projectId=project_id, datasetId=dataset_id, tableId=table_id),
          default_project=project_id)
      gcp_schema = _to_gcp_schema(schema)
      table = gcp_bigquery.Table(table_ref, schema=gcp_schema)
      if additional_parameters:
        for k, v in additional_parameters.items():
          setattr(table, k, v)
      response = self.client.create_table(table)
      _LOGGER.debug("Created the table with id %s", table_id)
      # The response is a Table instance.
      return response

    # Fallback for legacy client
    additional_parameters = additional_parameters or {}
    table = apitools_bigquery.Table(
        tableReference=TableReference(
            projectId=project_id, datasetId=dataset_id, tableId=table_id),
        schema=schema,
        **additional_parameters)
    request = apitools_bigquery.BigqueryTablesInsertRequest(
        projectId=project_id, datasetId=dataset_id, table=table)
    response = self.client.tables.Insert(request)
    _LOGGER.debug("Created the table with id %s", table_id)
    # The response is a bigquery.Table instance.
    return response

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def get_or_create_dataset(
      self,
      project_id,
      dataset_id,
      location=None,
      labels=None,
      kms_key=None,
      default_table_expiration_ms=None):
    # Check if dataset already exists otherwise create it
    if self._is_modern_client:
      dataset_ref = _to_gcp_dataset_ref(
          DatasetReference(projectId=project_id, datasetId=dataset_id),
          project=project_id)
      try:
        dataset = self.client.get_dataset(dataset_ref)
        self.created_temp_dataset = False
        return dataset
      except (NotFound, HttpError, ClientError) as exn:
        if getattr(exn, 'code', None) == 404 or getattr(
            exn, 'status_code', None) == 404 or isinstance(exn, NotFound):
          _LOGGER.info(
              'Dataset %s:%s does not exist so we will create it as temporary '
              'with location=%s',
              project_id,
              dataset_id,
              location)
          dataset = gcp_bigquery.Dataset(dataset_ref)
          if location is not None:
            dataset.location = location
          dict_labels = _extract_dict_labels(labels)
          if dict_labels:
            dataset.labels = dict_labels
          if kms_key is not None:
            dataset.default_encryption_configuration = (
                gcp_bigquery.EncryptionConfiguration(kms_key_name=kms_key))
          if default_table_expiration_ms is not None:
            dataset.default_table_expiration_ms = default_table_expiration_ms
          response = self.client.create_dataset(dataset)
          self.created_temp_dataset = True
          # The response is a Dataset instance.
          return response
        raise

    # Fallback for legacy client
    try:
      dataset = self.client.datasets.Get(
          apitools_bigquery.BigqueryDatasetsGetRequest(
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
        dataset_reference = DatasetReference(
            projectId=project_id, datasetId=dataset_id)
        dataset = apitools_bigquery.Dataset(datasetReference=dataset_reference)
        if location is not None:
          dataset.location = location
        if labels is not None:
          dataset.labels = _build_dataset_labels(labels)
        if kms_key is not None:
          dataset.defaultEncryptionConfiguration = (
              _build_dataset_encryption_config(kms_key))
        if default_table_expiration_ms is not None:
          dataset.defaultTableExpirationMs = default_table_expiration_ms
        request = apitools_bigquery.BigqueryDatasetsInsertRequest(
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
    if self._is_modern_client:
      table_ref = _to_gcp_table_ref(
          TableReference(
              projectId=project_id, datasetId=dataset_id, tableId=table_id),
          default_project=project_id)
      rows = self.client.list_rows(table_ref, max_results=1)
      if hasattr(rows, 'total_rows') and rows.total_rows is not None:
        return rows.total_rows == 0
      return len(list(rows)) == 0

    # Fallback for legacy client
    request = apitools_bigquery.BigqueryTabledataListRequest(
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
    if self._is_modern_client:
      table_ref = _to_gcp_table_ref(
          TableReference(
              projectId=project_id, datasetId=dataset_id, tableId=table_id),
          default_project=project_id)
      try:
        self.client.delete_table(table_ref, not_found_ok=True)
      except (NotFound, HttpError, ClientError) as exn:
        if getattr(exn, 'code', None) == 404 or getattr(
            exn, 'status_code', None) == 404 or isinstance(exn, NotFound):
          _LOGGER.warning(
              'Table %s:%s.%s does not exist', project_id, dataset_id, table_id)
          return
        raise
      return

    # Fallback for legacy client
    request = apitools_bigquery.BigqueryTablesDeleteRequest(
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
    if self._is_modern_client:
      dataset_ref = _to_gcp_dataset_ref(
          DatasetReference(projectId=project_id, datasetId=dataset_id),
          project=project_id)
      try:
        self.client.delete_dataset(
            dataset_ref, delete_contents=delete_contents, not_found_ok=True)
      except (NotFound, HttpError, ClientError) as exn:
        if getattr(exn, 'code', None) == 404 or getattr(
            exn, 'status_code', None) == 404 or isinstance(exn, NotFound):
          _LOGGER.warning(
              'Dataset %s:%s does not exist', project_id, dataset_id)
          return
        raise
      return

    # Fallback for legacy client
    request = apitools_bigquery.BigqueryDatasetsDeleteRequest(
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
  def create_temporary_dataset(
      self, project_id, location, labels=None, kms_key=None):
    self.get_or_create_dataset(
        project_id,
        self.temp_dataset_id,
        location=location,
        default_table_expiration_ms=_DEFAULT_TABLE_EXPIRATION_MS,
        labels=labels,
        kms_key=kms_key)

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
    if self._is_modern_client:
      dataset_ref = _to_gcp_dataset_ref(
          DatasetReference(
              projectId=project_id, datasetId=temp_table.datasetId),
          project=project_id)
      try:
        self.client.get_dataset(dataset_ref)
      except (NotFound, HttpError, ClientError) as exn:
        if getattr(exn, 'code', None) == 404 or getattr(
            exn, 'status_code', None) == 404 or isinstance(exn, NotFound):
          _LOGGER.warning(
              'Dataset %s:%s does not exist', project_id, temp_table.datasetId)
          return
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
      except (Forbidden, HttpForbiddenError, HttpError) as exn:
        if getattr(exn, 'code', None) == 403 or getattr(
            exn, 'status_code', None) == 403 or isinstance(exn, Forbidden):
          _LOGGER.warning(
              'Permission denied to delete temporary dataset %s:%s for clean up',
              temp_table.projectId,
              temp_table.datasetId)
          return
        raise
      return

    # Fallback for legacy client
    try:
      self.client.datasets.Get(
          apitools_bigquery.BigqueryDatasetsGetRequest(
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
    else:
      filter_str = ''

    if not self.is_user_configured_dataset() and labels is not None:
      if self._is_modern_client:
        try:
          for dataset in self.client.list_datasets(project=project_id,
                                                   filter=filter_str):
            ds_id = dataset.dataset_id
            self._delete_dataset(project_id, ds_id, True)
        except (Forbidden, HttpForbiddenError, HttpError) as exn:
          if getattr(exn, 'code', None) == 403 or getattr(
              exn, 'status_code', None) == 403 or isinstance(exn, Forbidden):
            _LOGGER.warning(
                'Permission denied to delete temporary dataset %s for clean up.',
                project_id)
            return
          raise
        return

      # Fallback for legacy client
      response = (
          self.client.datasets.List(
              apitools_bigquery.BigqueryDatasetsListRequest(
                  projectId=project_id, filter=filter_str)))
      for dataset in response.datasets:
        try:
          ds_id = dataset.datasetReference.datasetId
          self._delete_dataset(project_id, ds_id, True)
        except (HttpForbiddenError, HttpError) as exn:
          if exn.status_code == 403:
            _LOGGER.warning(
                'Permission denied to delete temporary dataset %s:%s for '
                'clean up.',
                project_id,
                ds_id)
            return
          else:
            raise
    else:
      try:
        self._delete_table(project_id, dataset_id, table_id)
      except (Forbidden, HttpForbiddenError, HttpError) as exn:
        if getattr(exn, 'code', None) == 403 or getattr(
            exn, 'status_code', None) == 403 or isinstance(exn, Forbidden):
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
    if self._is_modern_client:
      return self.client.get_job(job_id, project=project, location=location)

    # Fallback for legacy client
    request = apitools_bigquery.BigqueryJobsGetRequest()
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
      JobReference or bigquery.JobReference with the information about the job that was started.
    """
    if source_uris and source_stream:
      raise ValueError(
          'Only one of source_uris and source_stream may be specified. '
          'Got both.')

    project_id = (
        getattr(destination, 'projectId', None) or
        getattr(destination, 'project', None)
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
      JobReference or bigquery.JobReference with the information about the job that was started.
    """
    job_project = project or getattr(table_reference, 'projectId',
                                     None) or getattr(
                                         table_reference, 'project', None)
    if self._is_modern_client:
      src_ref = _to_gcp_table_ref(table_reference, default_project=job_project)
      dest_uris = destination if isinstance(destination,
                                            list) else [destination]
      dict_labels = _extract_dict_labels(job_labels)
      job_config = gcp_bigquery.ExtractJobConfig(
          destination_format=destination_format,
          print_header=include_header,
          compression=compression,
          use_avro_logical_types=use_avro_logical_types,
      )
      if dict_labels:
        job_config.labels = dict_labels
      try:
        job = self.client.extract_table(
            src_ref,
            dest_uris,
            job_id=job_id,
            job_config=job_config,
            project=job_project,
        )
        return JobReference(
            job_id=job.job_id, project=job.project, location=job.location)
      except (Conflict, HttpError) as exn:
        if getattr(exn, 'code', None) == 409 or getattr(
            exn, 'status_code', None) == 409 or isinstance(exn, Conflict):
          return JobReference(job_id=job_id, project=job_project)
        raise

    # Fallback for legacy client
    job_reference = (
        apitools_bigquery.JobReference(jobId=job_id, projectId=job_project)
        if apitools_bigquery and hasattr(apitools_bigquery, 'JobReference') else
        JobReference(jobId=job_id, projectId=job_project))
    request = apitools_bigquery.BigqueryJobsInsertRequest(
        projectId=job_project,
        job=apitools_bigquery.Job(
            configuration=apitools_bigquery.JobConfiguration(
                extract=apitools_bigquery.JobConfigurationExtract(
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
      schema: A TableSchema instance or None.
      create_disposition: CREATE_NEVER or CREATE_IF_NEEDED.
      write_disposition: WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.

    Returns:
      A Table instance if table was found or created.

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
    except (NotFound, HttpError, ClientError) as exn:
      if getattr(exn, 'code', None) == 404 or getattr(
          exn, 'status_code', None) == 404 or isinstance(exn, NotFound):
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
      except (Conflict, HttpError, ClientError) as exn:
        if getattr(exn, 'code', None) == 409 or getattr(
            exn, 'status_code', None) == 409 or isinstance(exn, Conflict):
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
          schema or (found_table.schema if found_table else None),
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
    if self._is_modern_client:
      job_config = gcp_bigquery.QueryJobConfig(
          dry_run=dry_run,
          use_legacy_sql=use_legacy_sql,
          flatten_results=flatten_results,
          priority=priority,
      )
      if job_labels:
        dict_labels = _extract_dict_labels(job_labels)
        if dict_labels:
          job_config.labels = dict_labels
      job = self.client.query(
          query,
          job_config=job_config,
          project=project_id,
      )
      if dry_run:
        return
      rows = job.result()
      yield list(rows), _to_table_schema(rows.schema)
      return

    job = self._start_query_job(
        project_id,
        query,
        use_legacy_sql,
        flatten_results,
        job_id=uuid.uuid4().hex,
        priority=priority,
        dry_run=dry_run,
        job_labels=job_labels)
    if dry_run:
      # If this was a dry run then the fact that we get here means the
      # query has no errors. The start_query_job would raise an error otherwise.
      return

    job_id = getattr(job, 'job_id', None) or getattr(
        getattr(job, 'jobReference', None), 'jobId', None)
    location = getattr(job, 'location', None) or getattr(
        getattr(job, 'jobReference', None), 'location', None)

    page_token = None
    while True:
      response = self._get_query_results(
          project_id, job_id, page_token, location=location)
      if hasattr(response, 'jobComplete'):
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
      else:
        # Modern RowIterator
        yield list(response), _to_table_schema(response.schema)
        break


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
      insert_ids: Optional list of unique row IDs to avoid duplicate inserts.
      skip_invalid_rows: If there are rows with insertion errors, whether they
        should be skipped, and all others should be inserted successfully.
      ignore_unknown_values: Set this option to true to ignore unknown column
        names. If the input rows contain columns that are not
        part of the existing table's schema, those columns are ignored, and
        the rows are successfully inserted.

    Returns:
      A tuple (bool, errors). If first element is False then the second element
      will be a list containing specific errors.
    """
    # Prepare rows for insertion. Of special note is the row ID that we add to
    # each row in order to help BigQuery avoid inserting a row multiple times.
    # BigQuery will do a best-effort if unique IDs are provided. This situation
    # can happen during retries on failures.
    # TODO(silviuc): Must add support to writing TableRow's instead of dicts.
    insert_ids = [
        str(self.unique_row_id) if not insert_ids else insert_ids[i]
        for i, _ in enumerate(rows)
    ]
    rows = [
        fast_json_loads(fast_json_dumps(r, default=default_encoder))
        for r in rows
    ]

    result, errors = self._insert_all_rows(
        project_id,
        dataset_id,
        table_id,
        rows,
        insert_ids,
        skip_invalid_rows=skip_invalid_rows,
        ignore_unknown_values=ignore_unknown_values)
    return result, errors

  def _convert_cell_value_to_dict(self, value, field):
    f_type = field.type if hasattr(field, 'type') else field.field_type
    if f_type == 'STRING':
      # Input: "XYZ" --> Output: "XYZ"
      return value
    elif f_type in ('BOOLEAN', 'BOOL'):
      # Input: "true" --> Output: True
      return value == 'true' if isinstance(value, str) else bool(value)
    elif f_type in ('INTEGER', 'INT64'):
      # Input: "123" --> Output: 123
      return int(value)
    elif f_type in ('FLOAT', 'FLOAT64'):
      # Input: "1.23" --> Output: 1.23
      return float(value)
    elif f_type == 'TIMESTAMP':
      # The UTC should come from the timezone library but this is a known
      # issue in python 2.7 so we'll just hardcode it as we're reading using
      # utcfromtimestamp.
      # Input: 1478134176.985864 --> Output: "2016-11-03 00:49:36.985864 UTC"
      if isinstance(value, (int, float)):
        dt = datetime.datetime.fromtimestamp(
            float(value), tz=datetime.timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
      return str(value)
    elif f_type == 'BYTES':
      # Input: "YmJi" --> Output: "YmJi"
      return value
    elif f_type == 'DATE':
      # Input: "2016-11-03" --> Output: "2016-11-03"
      return str(value)
    elif f_type == 'DATETIME':
      # Input: "2016-11-03T00:49:36" --> Output: "2016-11-03T00:49:36"
      return str(value)
    elif f_type == 'TIME':
      # Input: "00:49:36" --> Output: "00:49:36"
      return str(value)
    elif f_type in ('RECORD', 'STRUCT'):
      # Note that a schema field object supports also a RECORD type. However
      # when querying, the repeated and/or record fields are flattened
      # unless we pass the flatten_results flag as False to the source
      return self.convert_row_to_dict(value, field)
    elif f_type in ('NUMERIC', 'BIGNUMERIC'):
      return decimal.Decimal(str(value))
    elif f_type == 'GEOGRAPHY':
      return value
    else:
      raise RuntimeError('Unexpected field type: %s' % f_type)

  def convert_row_to_dict(self, row, schema):
    """Converts a TableRow instance using the schema to a Python dict."""
    result = {}
    fields = schema.fields if hasattr(schema, 'fields') else schema
    for index, field in enumerate(fields):
      value = None
      if hasattr(row, 'f'):
        cell = row.f[index]
        value = from_json_value(cell.v) if cell.v is not None else None
      elif isinstance(row, dict) and 'f' in row:
        cell = row['f'][index]
        value = cell['v'] if 'v' in cell else None
      elif isinstance(row, dict):
        value = row.get(field.name)
      elif hasattr(row, field.name):
        value = getattr(row, field.name)
      elif isinstance(row, (list, tuple)) and index < len(row):
        value = row[index]

      mode = getattr(field, 'mode', 'NULLABLE')
      if mode == 'REPEATED':
        if value is None:
          # Ideally this should never happen as repeated fields default to
          # returning an empty list
          result[field.name] = []
        else:
          result[field.name] = [
              self._convert_cell_value_to_dict(
                  x['v'] if isinstance(x, dict) and 'v' in x else x, field)
              for x in value
          ]
      elif value is None:
        if not mode == 'NULLABLE':
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
  def _bigquery_client(
      pipeline_options: PipelineOptions, use_legacy_client: bool = False):
    raw_credentials = auth.get_service_credentials(pipeline_options)
    google_credentials = (
        raw_credentials.get_google_auth_credentials() if hasattr(
            raw_credentials, 'get_google_auth_credentials') else
        raw_credentials)
    project = None
    experiments = []
    if pipeline_options:
      try:
        from apache_beam.options.pipeline_options import GoogleCloudOptions
        project = pipeline_options.view_as(GoogleCloudOptions).project
        if hasattr(project, 'get') and callable(project.get):
          project = project.get()
      except Exception:
        project = None
      try:
        from apache_beam.options.pipeline_options import DebugOptions
        experiments = pipeline_options.view_as(DebugOptions).experiments or []
      except Exception:
        experiments = []

    use_legacy = (
        use_legacy_client or 'use_legacy_bigquery_client' in experiments or
        'use_legacy_bq_client' in experiments)

    if not use_legacy:
      client_cls = getattr(gcp_bigquery, 'Client', None)
      if isinstance(client_cls, type):
        try:
          client_info = ClientInfo(
              user_agent="apache-beam-%s" %
              apache_beam.__version__) if ClientInfo else None
          return gcp_bigquery.Client(
              project=project,
              credentials=google_credentials,
              client_info=client_info)
        except Exception:
          pass

    return apitools_bigquery.BigqueryV2(
        http=get_new_http(),
        credentials=raw_credentials,
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
    schema (str, dict, ~apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages.TableSchema, TableSchema):
      The table schema. Will be used to help convert the row.

  Returns:
    ~apache_beam.pvalue.Row: The converted row.
  """
  if not isinstance(schema,
                    (TableSchema, TableFieldSchema)) and not hasattr(schema,
                                                                     'fields'):
    schema = get_bq_tableschema(schema)
  beam_row = {}
  fields = schema.fields if hasattr(schema, 'fields') else schema
  for field in fields:
    name = field.name
    mode = (getattr(field, 'mode', None) or 'NULLABLE').upper()
    field_type = (field.type
                  if hasattr(field, 'type') else field.field_type).upper()

    # When writing with Storage Write API via xlang, we give the Beam Row
    # PCollection a hint on the schema using `with_output_types`.
    # This requires that each row has all the fields in the schema.
    # However, it's possible that some nullable fields don't appear in the row.
    # For this case, we create the field with a `None` value
    # None is also set when a repeated field is missing as BigQuery
    # converts Null Repeated fields to empty lists
    if row and name not in row and mode != "REQUIRED":
      row[name] = None

    value = row.get(name) if row else None
    if field_type in ["RECORD", "STRUCT"] and value:
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
  """Transform the string table schema into a TableSchema instance.

  Args:
    schema (str): The string schema to be used if the BigQuery table to write
      has to be created.

  Returns:
    TableSchema:
    The schema to be used if the BigQuery table to write has to be created
    but in the TableSchema format.
  """
  table_schema = TableSchema()
  schema_list = [s.strip() for s in schema.split(',')]
  for field_and_type in schema_list:
    field_name, field_type = field_and_type.split(':')
    field_schema = TableFieldSchema(
        name=field_name, type=field_type, mode='NULLABLE')
    table_schema.fields.append(field_schema)
  return table_schema


def table_field_to_dict(field):
  """Create a dictionary representation of a table field for serialization.

  Args:
    field: A TableFieldSchema or SchemaField instance.

  Returns:
    dict: A dictionary representation of the field.
  """
  if isinstance(field, dict):
    return field
  result = {}
  result['name'] = getattr(field, 'name', '')
  result['type'] = getattr(field, 'type', None) or getattr(
      field, 'field_type', None) or 'STRING'
  result['mode'] = getattr(field, 'mode', 'NULLABLE') or 'NULLABLE'
  if hasattr(field, 'description') and field.description is not None:
    result['description'] = field.description
  if hasattr(field, 'fields') and field.fields:
    result['fields'] = [table_field_to_dict(f) for f in field.fields]
  return result


def table_schema_to_dict(table_schema):
  """Create a dictionary representation of table schema for serialization.

  Args:
    table_schema: A TableSchema or list of SchemaField instances.

  Returns:
    dict: A dictionary representation of the schema with 'fields' list.
  """
  if not isinstance(table_schema,
                    (TableSchema, list, tuple)) and not hasattr(table_schema,
                                                                'fields'):
    raise ValueError("Table schema must be of the type TableSchema or list")
  fields = table_schema.fields if hasattr(
      table_schema, 'fields') else table_schema
  return {'fields': [table_field_to_dict(field) for field in fields]}


def get_dict_table_schema(schema):
  """Transform the table schema into a dictionary instance.

  Args:
    schema (str, dict, TableSchema, SchemaField list):
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
  elif isinstance(schema, (TableSchema, list)) or hasattr(schema, 'fields'):
    return table_schema_to_dict(schema)
  else:
    raise TypeError('Unexpected schema argument: %s.' % schema)


def get_bq_tableschema(schema):
  """Convert the table schema to a TableSchema object.

  Args:
    schema (str, dict, TableSchema, SchemaField list):
      The schema to be used if the BigQuery table to write has to be created.
      This can either be a dict or string or in the TableSchema format.

  Returns:
    TableSchema: The schema as a TableSchema object.
  """
  if (isinstance(schema, (TableSchema, value_provider.ValueProvider)) or
      callable(schema) or schema is None):
    return schema
  elif isinstance(schema, (list, tuple)):
    dict_schema = get_dict_table_schema(schema)
    if isinstance(dict_schema, dict):
      return parse_table_schema_from_json(json.dumps(dict_schema))
    return TableSchema(fields=schema)
  elif isinstance(schema, str):
    return get_table_schema_from_string(schema)
  elif isinstance(schema, dict):
    schema_string = json.dumps(schema)
    return parse_table_schema_from_json(schema_string)
  elif hasattr(schema, 'fields'):
    dict_schema = get_dict_table_schema(schema)
    if isinstance(dict_schema, dict):
      return parse_table_schema_from_json(json.dumps(dict_schema))
    return TableSchema(fields=list(schema.fields))
  else:
    raise TypeError('Unexpected schema argument: %s.' % schema)


def get_avro_schema_from_table_schema(schema):
  """Transform the table schema into an Avro schema.

  Args:
    schema (str, dict, TableSchema, SchemaField list):
      The TableSchema to convert to Avro schema. This can either be a dict or
      string or in the TableSchema format.

  Returns:
    Dict[str, Any]: An Avro schema, which can be used by fastavro.
  """
  dict_table_schema = get_dict_table_schema(schema)
  return bigquery_avro_tools.get_record_schema_from_dict_table_schema(
      "root", dict_table_schema)


def get_beam_typehints_from_tableschema(schema, type_overrides=None):
  """Extracts Beam Python type hints from the schema.

  Args:
    schema (TableSchema, SchemaField list, dict, str):
      The TableSchema to extract type hints from.
    type_overrides (dict): Optional mapping of BigQuery type names (uppercase)
      to Python types. These override the default mappings in
      BIGQUERY_TYPE_TO_PYTHON_TYPE. For example:
      ``{'DATE': datetime.date, 'JSON': dict}``

  Returns:
    List[Tuple[str, Any]]: A list of type hints that describe the input schema.
    Nested and repeated fields are supported.
  """
  normalized_overrides = {
      k.upper(): v
      for k, v in (type_overrides or {}).items()
  }
  effective_types = {**BIGQUERY_TYPE_TO_PYTHON_TYPE, **normalized_overrides}
  if not isinstance(schema,
                    (TableSchema, TableFieldSchema)) and not hasattr(schema,
                                                                     'fields'):
    schema = get_bq_tableschema(schema)
  typehints = []
  fields = schema.fields if hasattr(schema, 'fields') else schema
  for field in fields:
    name = field.name
    field_type = (field.type
                  if hasattr(field, 'type') else field.field_type).upper()
    mode = (getattr(field, 'mode', None) or 'NULLABLE').upper()

    if field_type in ["STRUCT", "RECORD"]:
      # Structs can be represented as Beam Rows.
      typehint = RowTypeConstraint.from_fields(
          get_beam_typehints_from_tableschema(field, type_overrides))
    elif field_type in effective_types:
      typehint = effective_types[field_type]
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
    left: Union['TableSchema', 'TableFieldSchema', typing.Any],
    right: Union['TableSchema', 'TableFieldSchema', typing.Any],
    *,
    ignore_descriptions: bool = False,
    ignore_field_order: bool = False) -> bool:
  """Check whether schemas are equivalent.

  This comparison function differs from using == to compare TableSchema
  because it ignores categories, policy tags, descriptions (optionally), and
  field ordering (optionally).

  Args:
    left (TableSchema, TableFieldSchema, SchemaField, list, dict):
      One schema to compare.
    right (TableSchema, TableFieldSchema, SchemaField, list, dict):
      The other schema to compare.
    ignore_descriptions (bool): (optional) Whether or not to ignore field
      descriptions when comparing. Defaults to False.
    ignore_field_order (bool): (optional) Whether or not to ignore struct field
      order when comparing. Defaults to False.

  Returns:
    bool: True if the schemas are equivalent, False otherwise.
  """
  if left is None and right is None:
    return True
  if left is None or right is None:
    return False

  is_field_left = isinstance(
      left, (TableFieldSchema, getattr(gcp_bigquery, 'SchemaField', ()))) or (
          hasattr(left, 'name') and
          (hasattr(left, 'type') or hasattr(left, 'field_type')))
  is_field_right = isinstance(
      right, (TableFieldSchema, getattr(gcp_bigquery, 'SchemaField', ()))) or (
          hasattr(right, 'name') and
          (hasattr(right, 'type') or hasattr(right, 'field_type')))

  if is_field_left != is_field_right:
    return False

  if is_field_left:
    if left.name != right.name:
      return False

    l_type = (left.type if hasattr(left, 'type') else left.field_type).upper()
    r_type = (right.type
              if hasattr(right, 'type') else right.field_type).upper()
    if l_type != r_type:
      # Check for type aliases
      if sorted((l_type, r_type)) not in (["BOOL", "BOOLEAN"], ["FLOAT",
                                                                "FLOAT64"],
                                          ["INT64", "INTEGER"], ["RECORD",
                                                                 "STRUCT"]):
        return False

    l_mode = (getattr(left, 'mode', None) or 'NULLABLE').upper()
    r_mode = (getattr(right, 'mode', None) or 'NULLABLE').upper()
    if l_mode != r_mode:
      return False

    if not ignore_descriptions:
      if getattr(left, 'description', None) != getattr(right,
                                                       'description',
                                                       None):
        return False

  l_fields = left if isinstance(left,
                                (list,
                                 tuple)) else getattr(left, 'fields', None)
  r_fields = right if isinstance(right,
                                 (list,
                                  tuple)) else getattr(right, 'fields', None)
  if l_fields is not None or r_fields is not None:
    l_fields = list(l_fields or [])
    r_fields = list(r_fields or [])
    if len(l_fields) != len(r_fields):
      return False

    if ignore_field_order:
      l_fields = sorted(l_fields, key=lambda field: field.name)
      r_fields = sorted(r_fields, key=lambda field: field.name)

    for lf, rf in zip(l_fields, r_fields):
      if not check_schema_equal(lf,
                                rf,
                                ignore_descriptions=ignore_descriptions,
                                ignore_field_order=ignore_field_order):
        return False

  return True
