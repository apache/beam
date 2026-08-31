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

"""Compatibility shims and legacy client emulation for BigQuery.

This module contains temporary compatibility models, monkey patches, and
helpers designed to ease migration away from the deprecated apitools BigQuery
client to modern ``google-cloud-bigquery``.

.. note::
   This module is intended to be removed in a future Beam release once the
   apitools client dependency is completely removed.

   *Future Removal Guidance*:
   - Compatibility models (e.g. `_TableReferenceCompat`, `_DatasetReferenceCompat`,
     `_TableSchemaCompat`) and monkey-patches will be dropped. Code using
     `TableReference`, `DatasetReference`, etc. should import directly from
     `google.cloud.bigquery`.
   - Input normalization helpers (`_to_gcp_table_ref`, `_to_gcp_dataset_ref`,
     `_to_gcp_schema`, `_extract_dict_labels`, `_to_table_schema`) that
     convert string specs or dicts to modern `google.cloud.bigquery` instances
     are actively used across pipeline code paths and should be preserved in
     `bigquery_tools.py` when this compat module is excised.
"""

# pytype: skip-file

import logging

try:
  from google.cloud import bigquery as gcp_bigquery
  from google.cloud.bigquery import job as gcp_job
except ImportError:
  gcp_bigquery = None
  gcp_job = None

try:
  from apache_beam.io.gcp.internal.clients import bigquery as apitools_bigquery
except ImportError:
  apitools_bigquery = None

try:
  from apitools.base.protorpclite import messages as _protorpclite_messages
except ImportError:
  _protorpclite_messages = None

_LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Compatibility Models for TableReference, DatasetReference, Schema, and Jobs.
#
# These classes and monkey patches bridge between legacy apitools structures
# and modern google.cloud.bigquery objects, providing camelCase attribute access
# (e.g. projectId, datasetId, tableId, tableReference) for backwards
# compatibility across pipelines, transforms, and test suites.
# -----------------------------------------------------------------------------


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
    self._project = p or ""
    self._dataset_id = d or ""

  @classmethod
  def from_string(cls, dataset_ref, default_project=None):
    last_sep = max(dataset_ref.rfind("."), dataset_ref.rfind(":"))
    if last_sep != -1:
      p = dataset_ref[:last_sep]
      d = dataset_ref[last_sep + 1:]
    else:
      p = default_project or "default"
      d = dataset_ref
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
    if not hasattr(other, "project") and not hasattr(other, "projectId"):
      return NotImplemented
    other_p = getattr(other, "projectId", None) or getattr(
        other, "project", None)
    other_d = getattr(other, "datasetId", None) or getattr(
        other, "dataset_id", None)
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
      self._project = getattr(dataset_ref, "projectId", None) or getattr(
          dataset_ref, "project", None)
      self._dataset_id = getattr(dataset_ref, "datasetId", None) or getattr(
          dataset_ref, "dataset_id", None)
      self._table_id = table_id or ""
    else:
      self._project = None
      self._dataset_id = None
      self._table_id = None

  @classmethod
  def from_string(cls, table_ref, default_project=None):
    from apache_beam.io.gcp.bigquery_tools import parse_table_reference
    parsed = parse_table_reference(table_ref, project=default_project)
    return cls(
        projectId=parsed.projectId or default_project,
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
    if not hasattr(other, "tableId") and not hasattr(other, "table_id"):
      return NotImplemented
    other_p = getattr(other, "projectId", None) or getattr(
        other, "project", None)
    other_d = getattr(other, "datasetId", None) or getattr(
        other, "dataset_id", None)
    other_t = getattr(other, "tableId", None) or getattr(
        other, "table_id", None)
    return (self.projectId, self.datasetId,
            self.tableId) == (other_p, other_d, other_t)

  def __hash__(self):
    return hash((self.projectId, self.datasetId, self.tableId))


class _TableFieldSchemaCompat(object):
  def __init__(
      self,
      name="",
      type="STRING",
      mode="NULLABLE",
      description=None,
      fields=(),
      field_type=None,
      **kwargs):
    ft = type or field_type or "STRING"
    self.name = name
    self.field_type = ft
    self.mode = mode or "NULLABLE"
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


if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                             "TableReference"):
  TableReference = apitools_bigquery.TableReference
  DatasetReference = getattr(
      apitools_bigquery, "DatasetReference", None) or _DatasetReferenceCompat
  TableFieldSchema = apitools_bigquery.TableFieldSchema
  TableSchema = apitools_bigquery.TableSchema
  TableRow = getattr(apitools_bigquery, "TableRow", None) or _TableRowCompat
  TableCell = getattr(apitools_bigquery, "TableCell", None) or _TableCellCompat
  Table = getattr(apitools_bigquery, "Table", None)
  Dataset = getattr(apitools_bigquery, "Dataset", None)
  Job = getattr(apitools_bigquery, "Job", None)
  JobConfiguration = getattr(apitools_bigquery, "JobConfiguration", None)
  JobConfigurationLoad = getattr(apitools_bigquery, "JobConfigurationLoad", None)
  JobConfigurationQuery = getattr(
      apitools_bigquery, "JobConfigurationQuery", None)
  JobConfigurationExtract = getattr(
      apitools_bigquery, "JobConfigurationExtract", None)
  JobConfigurationTableCopy = getattr(
      apitools_bigquery, "JobConfigurationTableCopy", None)
  JobStatistics = getattr(apitools_bigquery, "JobStatistics", None)
  JobStatistics2 = getattr(apitools_bigquery, "JobStatistics2", None)
  JobStatistics4 = getattr(apitools_bigquery, "JobStatistics4", None)
  ErrorProto = getattr(apitools_bigquery, "ErrorProto", None)
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
                                     "JobReference") and isinstance(
                                         other, apitools_bigquery.JobReference):
      return (
          self.jobId == getattr(other, "jobId", None) and
          self.projectId == getattr(other, "projectId", None) and
          self.location == getattr(other, "location", None))
    return NotImplemented

  def __hash__(self):
    return hash((self.jobId, self.projectId, self.location))

  def __repr__(self):
    return (
        f"JobReference(jobId={self.jobId!r}, "
        f"projectId={self.projectId!r}, "
        f"location={self.location!r})")


def _patch_protorpclite_equality():
  if _protorpclite_messages is not None and hasattr(_protorpclite_messages,
                                                    "Message"):
    _orig_message_eq = _protorpclite_messages.Message.__eq__

    def _message_compat_eq(self, other):
      if isinstance(other, JobReference) and apitools_bigquery and hasattr(
          apitools_bigquery, "JobReference") and isinstance(
              self, apitools_bigquery.JobReference):
        return (
            getattr(self, "jobId", None) == other.jobId and
            getattr(self, "projectId", None) == other.projectId and
            getattr(self, "location", None) == other.location)
      if isinstance(other, TableReference) and apitools_bigquery and hasattr(
          apitools_bigquery, "TableReference") and isinstance(
              self, apitools_bigquery.TableReference):
        return (
            getattr(self, "projectId", None) == other.projectId and
            getattr(self, "datasetId", None) == other.datasetId and
            getattr(self, "tableId", None) == other.tableId)
      if isinstance(other, DatasetReference) and apitools_bigquery and hasattr(
          apitools_bigquery, "DatasetReference") and isinstance(
              self, apitools_bigquery.DatasetReference):
        return (
            getattr(self, "projectId", None) == other.projectId and
            getattr(self, "datasetId", None) == other.datasetId)
      return _orig_message_eq(self, other)

    _protorpclite_messages.Message.__eq__ = _message_compat_eq


def _set_table_ref_prop(ref, prop, val):
  if hasattr(ref, "_properties") and isinstance(ref._properties, dict):
    ref._properties[prop] = val
  if prop == "projectId":
    setattr(ref, "_project", val)
  elif prop == "datasetId":
    setattr(ref, "_dataset_id", val)
  elif prop == "tableId":
    setattr(ref, "_table_id", val)


# -----------------------------------------------------------------------------
# Compatibility Monkey-Patching for google.cloud.bigquery Classes
# -----------------------------------------------------------------------------


def _patch_gcp_bigquery():
  if not gcp_bigquery:
    return

  if not hasattr(gcp_bigquery.TableReference, "projectId"):
    gcp_bigquery.TableReference.projectId = property(
        lambda self: self.project,
        lambda self, val: _set_table_ref_prop(self, "projectId", val))
    gcp_bigquery.TableReference.datasetId = property(
        lambda self: self.dataset_id,
        lambda self, val: _set_table_ref_prop(self, "datasetId", val))
    gcp_bigquery.TableReference.tableId = property(
        lambda self: self.table_id,
        lambda self, val: _set_table_ref_prop(self, "tableId", val))

  if not hasattr(gcp_bigquery.DatasetReference, "projectId"):
    gcp_bigquery.DatasetReference.projectId = property(
        lambda self: self.project,
        lambda self, val: setattr(self, "_project", val))
    gcp_bigquery.DatasetReference.datasetId = property(
        lambda self: self.dataset_id,
        lambda self, val: setattr(self, "_dataset_id", val))

  if not hasattr(gcp_bigquery.SchemaField, "type"):
    gcp_bigquery.SchemaField.type = property(
        lambda self: self.field_type,
        lambda self, val: setattr(self, "_field_type", val))

  if not hasattr(gcp_bigquery.Table, "tableReference"):
    gcp_bigquery.Table.tableReference = property(lambda self: self.reference)
    gcp_bigquery.Table.numRows = property(lambda self: self.num_rows)
    gcp_bigquery.Table.numBytes = property(lambda self: self.num_bytes)
    gcp_bigquery.Table.timePartitioning = property(
        lambda self: self.time_partitioning,
        lambda self, val: setattr(self, "time_partitioning", val))
    gcp_bigquery.Table.rangePartitioning = property(
        lambda self: self.range_partitioning,
        lambda self, val: setattr(self, "range_partitioning", val))

  if hasattr(gcp_bigquery, "TimePartitioning"):
    if not hasattr(gcp_bigquery.TimePartitioning, "type"):
      gcp_bigquery.TimePartitioning.type = property(
          lambda self: self.type_,
          lambda self, val: setattr(self, "type_", val))
    if not hasattr(gcp_bigquery.TimePartitioning, "expirationMs"):
      gcp_bigquery.TimePartitioning.expirationMs = property(
          lambda self: self.expiration_ms,
          lambda self, val: setattr(self, "expiration_ms", val))
    if not hasattr(gcp_bigquery.TimePartitioning, "requirePartitionFilter"):
      gcp_bigquery.TimePartitioning.requirePartitionFilter = property(
          lambda self: self.require_partition_filter,
          lambda self, val: setattr(self, "require_partition_filter", val))

  if hasattr(gcp_bigquery, "RangePartitioning"):
    if not hasattr(gcp_bigquery.RangePartitioning, "range"):
      gcp_bigquery.RangePartitioning.range = property(
          lambda self: self.range_,
          lambda self, val: setattr(self, "range_", val))

  if not hasattr(gcp_bigquery.Dataset, "datasetReference"):
    gcp_bigquery.Dataset.datasetReference = property(
        lambda self: self.reference)
    gcp_bigquery.Dataset.defaultTableExpirationMs = property(
        lambda self: self.default_table_expiration_ms,
        lambda self, val: setattr(self, "default_table_expiration_ms", val))

  if hasattr(gcp_bigquery, "LoadJobConfig"):
    if not hasattr(gcp_bigquery.LoadJobConfig, "schemaUpdateOptions"):
      gcp_bigquery.LoadJobConfig.schemaUpdateOptions = property(
          lambda self: self.schema_update_options,
          lambda self, val: setattr(self, "schema_update_options", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "ignoreUnknownValues"):
      gcp_bigquery.LoadJobConfig.ignoreUnknownValues = property(
          lambda self: self.ignore_unknown_values,
          lambda self, val: setattr(self, "ignore_unknown_values", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "maxBadRecords"):
      gcp_bigquery.LoadJobConfig.maxBadRecords = property(
          lambda self: self.max_bad_records,
          lambda self, val: setattr(self, "max_bad_records", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "nullMarker"):
      gcp_bigquery.LoadJobConfig.nullMarker = property(
          lambda self: self.null_marker,
          lambda self, val: setattr(self, "null_marker", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "fieldDelimiter"):
      gcp_bigquery.LoadJobConfig.fieldDelimiter = property(
          lambda self: self.field_delimiter,
          lambda self, val: setattr(self, "field_delimiter", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "skipLeadingRows"):
      gcp_bigquery.LoadJobConfig.skipLeadingRows = property(
          lambda self: self.skip_leading_rows,
          lambda self, val: setattr(self, "skip_leading_rows", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "allowJaggedRows"):
      gcp_bigquery.LoadJobConfig.allowJaggedRows = property(
          lambda self: self.allow_jagged_rows,
          lambda self, val: setattr(self, "allow_jagged_rows", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "allowQuotedNewlines"):
      gcp_bigquery.LoadJobConfig.allowQuotedNewlines = property(
          lambda self: self.allow_quoted_newlines,
          lambda self, val: setattr(self, "allow_quoted_newlines", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "decimalTargetTypes"):
      gcp_bigquery.LoadJobConfig.decimalTargetTypes = property(
          lambda self: self.decimal_target_types,
          lambda self, val: setattr(self, "decimal_target_types", val))
    if not hasattr(gcp_bigquery.LoadJobConfig, "useAvroLogicalTypes"):
      gcp_bigquery.LoadJobConfig.useAvroLogicalTypes = property(
          lambda self: self.use_avro_logical_types,
          lambda self, val: setattr(self, "use_avro_logical_types", val))

  if hasattr(gcp_bigquery, "QueryJobConfig"):
    if not hasattr(gcp_bigquery.QueryJobConfig, "schemaUpdateOptions"):
      gcp_bigquery.QueryJobConfig.schemaUpdateOptions = property(
          lambda self: self.schema_update_options,
          lambda self, val: setattr(self, "schema_update_options", val))
    if not hasattr(gcp_bigquery.QueryJobConfig, "useLegacySql"):
      gcp_bigquery.QueryJobConfig.useLegacySql = property(
          lambda self: self.use_legacy_sql,
          lambda self, val: setattr(self, "use_legacy_sql", val))
    if not hasattr(gcp_bigquery.QueryJobConfig, "flattenResults"):
      gcp_bigquery.QueryJobConfig.flattenResults = property(
          lambda self: self.flatten_results,
          lambda self, val: setattr(self, "flatten_results", val))
    if not hasattr(gcp_bigquery.QueryJobConfig, "allowLargeResults"):
      gcp_bigquery.QueryJobConfig.allowLargeResults = property(
          lambda self: self.allow_large_results,
          lambda self, val: setattr(self, "allow_large_results", val))
    if not hasattr(gcp_bigquery.QueryJobConfig, "maximumBytesBilled"):
      gcp_bigquery.QueryJobConfig.maximumBytesBilled = property(
          lambda self: self.maximum_bytes_billed,
          lambda self, val: setattr(self, "maximum_bytes_billed", val))

  if hasattr(gcp_bigquery, "Table") and hasattr(gcp_bigquery.Table, "labels"):
    _orig_tbl_labels_setter = gcp_bigquery.Table.labels.fset
    if _orig_tbl_labels_setter:

      def _safe_tbl_labels_setter(self, value):
        if value is None:
          value = {}
        elif not isinstance(value, dict) and hasattr(value,
                                                     "additionalProperties"):
          from apitools.base.py import encoding
          value = encoding.MessageToDict(value)
        _orig_tbl_labels_setter(self, value)

      gcp_bigquery.Table.labels = gcp_bigquery.Table.labels.setter(
          _safe_tbl_labels_setter)

  if hasattr(gcp_bigquery, "Dataset") and hasattr(gcp_bigquery.Dataset,
                                                  "labels"):
    _orig_ds_labels_setter = gcp_bigquery.Dataset.labels.fset
    if _orig_ds_labels_setter:

      def _safe_ds_labels_setter(self, value):
        if value is None:
          value = {}
        elif not isinstance(value, dict) and hasattr(value,
                                                     "additionalProperties"):
          from apitools.base.py import encoding
          value = encoding.MessageToDict(value)
        _orig_ds_labels_setter(self, value)

      gcp_bigquery.Dataset.labels = gcp_bigquery.Dataset.labels.setter(
          _safe_ds_labels_setter)

  try:
    from google.cloud.bigquery.job.base import _JobConfig as _GcpJobConfig
    if hasattr(_GcpJobConfig, "labels") and hasattr(_GcpJobConfig.labels,
                                                    "fset"):
      _orig_job_labels_setter = _GcpJobConfig.labels.fset
      if _orig_job_labels_setter:

        def _safe_job_labels_setter(self, value):
          if value is None:
            value = {}
          elif not isinstance(value, dict) and hasattr(value,
                                                       "additionalProperties"):
            from apitools.base.py import encoding
            value = encoding.MessageToDict(value)
          _orig_job_labels_setter(self, value)

        _GcpJobConfig.labels = _GcpJobConfig.labels.setter(
            _safe_job_labels_setter)
  except ImportError:
    pass

  if hasattr(gcp_job,
             "_AsyncJob") and not hasattr(gcp_job._AsyncJob, "jobReference"):
    gcp_job._AsyncJob.jobReference = property(
        lambda self: JobReference(
            job_id=self.job_id, project=self.project, location=self.location))
    gcp_job._AsyncJob.status = property(lambda self: _JobStatusCompat(self))
    gcp_job._AsyncJob.statistics = property(lambda self: _JobStatsCompat(self))

  if not hasattr(gcp_bigquery.Client, "tables"):
    gcp_bigquery.Client.tables = property(
        lambda self: _ClientTablesCompat(self))
  if not hasattr(gcp_bigquery.Client, "datasets"):
    gcp_bigquery.Client.datasets = property(
        lambda self: _ClientDatasetsCompat(self))
  if not hasattr(gcp_bigquery.Client, "jobs"):
    gcp_bigquery.Client.jobs = property(lambda self: _ClientJobsCompat(self))


# -----------------------------------------------------------------------------
# Compatibility Helpers
# -----------------------------------------------------------------------------


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
  if hasattr(obj, "to_api_repr") and callable(obj.to_api_repr):
    return obj.to_api_repr()
  if _protorpclite_messages is not None and hasattr(
      _protorpclite_messages, "Message") and isinstance(
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
      table_ref, getattr(gcp_bigquery, "TableReference", ())):
    return table_ref
  if isinstance(table_ref, str):
    from apache_beam.io.gcp.bigquery_tools import parse_table_reference
    table_ref = parse_table_reference(table_ref, project=default_project)
  proj = getattr(table_ref, "projectId", None) or getattr(
      table_ref, "project", None) or getattr(
          table_ref, "project_id", None) or default_project or "default"
  dataset_id = getattr(table_ref, "datasetId", None) or getattr(
      table_ref, "dataset_id", None) or getattr(table_ref, "dataset", None)
  table_id = getattr(table_ref, "tableId", None) or getattr(
      table_ref, "table_id", None) or getattr(table_ref, "table", None)
  if dataset_id and table_id:
    if gcp_bigquery is not None and hasattr(
        gcp_bigquery, "TableReference") and hasattr(gcp_bigquery,
                                                    "DatasetReference"):
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
      dataset_ref, getattr(gcp_bigquery, "DatasetReference", ())):
    return dataset_ref
  if isinstance(dataset_ref, str):
    last_sep = max(dataset_ref.rfind("."), dataset_ref.rfind(":"))
    if last_sep != -1:
      proj = dataset_ref[:last_sep]
      ds_id = dataset_ref[last_sep + 1:]
      if gcp_bigquery is not None and hasattr(gcp_bigquery, "DatasetReference"):
        return gcp_bigquery.DatasetReference(proj, ds_id)
      return _DatasetReferenceCompat(projectId=proj, datasetId=ds_id)
    proj = project or "default"
    if gcp_bigquery is not None and hasattr(gcp_bigquery, "DatasetReference"):
      return gcp_bigquery.DatasetReference(proj, dataset_ref)
    return _DatasetReferenceCompat(projectId=proj, datasetId=dataset_ref)
  if hasattr(dataset_ref, "projectId") or hasattr(dataset_ref, "project"):
    proj = getattr(dataset_ref, "projectId", None) or getattr(
        dataset_ref, "project", None) or getattr(
            dataset_ref, "project_id", None) or project or "default"
    ds_id = getattr(dataset_ref, "datasetId", None) or getattr(
        dataset_ref, "dataset_id", None)
    if gcp_bigquery is not None and hasattr(gcp_bigquery, "DatasetReference"):
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
          f, getattr(gcp_bigquery, "SchemaField", ())):
        fields.append(f)
      elif isinstance(f, dict) and gcp_bigquery is not None:
        fields.append(gcp_bigquery.SchemaField.from_api_repr(f))
      elif hasattr(f, "name") and gcp_bigquery is not None:
        from apache_beam.io.gcp.bigquery_tools import table_field_to_dict
        dict_field = table_field_to_dict(f)
        if isinstance(dict_field, dict):
          fields.append(gcp_bigquery.SchemaField.from_api_repr(dict_field))
        else:
          fields.append(f)
      else:
        fields.append(f)
    return fields
  if isinstance(schema, TableSchema) or hasattr(schema, "fields"):
    from apache_beam.io.gcp.bigquery_tools import get_dict_table_schema
    dict_schema = get_dict_table_schema(schema)
    if isinstance(dict_schema, dict) and gcp_bigquery is not None:
      return [
          gcp_bigquery.SchemaField.from_api_repr(f)
          for f in dict_schema.get("fields", [])
      ]
    if hasattr(schema, "fields") and schema.fields is not None:
      return list(schema.fields)
  if isinstance(schema, dict):
    if gcp_bigquery is not None:
      return [
          gcp_bigquery.SchemaField.from_api_repr(f)
          for f in schema.get("fields", [])
      ]
    return schema.get("fields", [])
  if isinstance(schema, str):
    from apache_beam.io.gcp.bigquery_tools import get_dict_table_schema
    return _to_gcp_schema(get_dict_table_schema(schema))
  return schema


def _to_table_schema(schema):
  """Converts a list of google.cloud.bigquery.SchemaField, dict, or TableSchema into a TableSchema."""
  if schema is None:
    return TableSchema()
  if isinstance(schema, TableSchema):
    return schema
  if isinstance(schema, dict):
    return _to_table_schema(schema.get("fields", []))
  if hasattr(schema, "fields") and not isinstance(schema, (list, tuple)):
    return _to_table_schema(schema.fields)

  def _to_field_schema(f):
    if isinstance(f, TableFieldSchema):
      return f
    if isinstance(f, dict):
      f_dict = f
    elif hasattr(f, "to_api_repr"):
      f_dict = f.to_api_repr()
    else:
      f_dict = None

    if f_dict is not None:
      name = f_dict.get("name", "")
      field_type = f_dict.get("type") or f_dict.get("type_") or "STRING"
      mode = f_dict.get("mode", "NULLABLE")
      description = f_dict.get("description", None)
      sub_fields = [_to_field_schema(sf) for sf in f_dict.get("fields", [])]
      return TableFieldSchema(
          name=name,
          type=field_type,
          mode=mode,
          description=description,
          fields=sub_fields)

    name = getattr(f, "name", "")
    field_type = getattr(f, "field_type", None) or getattr(f, "type",
                                                           None) or "STRING"
    mode = getattr(f, "mode", "NULLABLE")
    description = getattr(f, "description", None)
    sub = getattr(f, "fields", ())
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


# -----------------------------------------------------------------------------
# Emulated Client Compatibility Interfaces
# -----------------------------------------------------------------------------


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
    return getattr(self._job, "total_bytes_billed", None)

  @property
  def totalBytesProcessed(self):
    return getattr(self._job, "total_bytes_processed", None)

  @property
  def referencedTables(self):
    tables = getattr(self._job, "referenced_tables", None)
    if tables is not None:
      return [
          TableReference(
              projectId=t.project, datasetId=t.dataset_id, tableId=t.table_id)
          for t in tables
      ]
    return None


class _ClientTablesCompat:
  def __init__(self, client):
    self._client = client

  def Get(self, request):
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    tbl_id = getattr(request, "tableId", None)
    if ds_id and tbl_id:
      table_ref = gcp_bigquery.TableReference(
          gcp_bigquery.DatasetReference(
              proj or getattr(self._client, "project", None) or "default",
              ds_id),
          tbl_id)
    else:
      t_ref = getattr(request, "tableReference", None) or getattr(
          request, "tableId", None) or request
      table_ref = _to_gcp_table_ref(
          t_ref, default_project=proj or getattr(self._client, "project", None))
    return self._client.get_table(table_ref)

  def Insert(self, request):
    table = getattr(request, "table", None)
    if table is not None:
      t_ref = getattr(table, "tableReference", None)
      proj = getattr(t_ref, "projectId", None) or getattr(
          request, "projectId", None)
      ds_id = getattr(t_ref, "datasetId", None) or getattr(
          request, "datasetId", None)
      tbl_id = getattr(t_ref, "tableId", None)
      schema = getattr(table, "schema", None)
    else:
      proj = getattr(request, "projectId", None)
      ds_id = getattr(request, "datasetId", None)
      tbl_id = getattr(request, "tableId", None)
      schema = getattr(request, "schema", None)
    gcp_tbl_ref = gcp_bigquery.TableReference(
        gcp_bigquery.DatasetReference(
            proj or getattr(self._client, "project", None) or "default", ds_id),
        tbl_id)
    gcp_table = gcp_bigquery.Table(gcp_tbl_ref, schema=_to_gcp_schema(schema))
    if table is not None:
      tp = getattr(table, "timePartitioning", None) or getattr(
          table, "time_partitioning", None)
      if tp is not None:
        if isinstance(tp, gcp_bigquery.TimePartitioning):
          gcp_table.time_partitioning = tp
        else:
          tp_field = getattr(tp, "field", None)
          tp_type = getattr(tp, "type", None) or getattr(tp, "type_", None)
          tp_exp = getattr(tp, "expirationMs", None) or getattr(
              tp, "expiration_ms", None)
          tp_req = getattr(tp, "requirePartitionFilter", None) or getattr(
              tp, "require_partition_filter", None)
          gcp_table.time_partitioning = gcp_bigquery.TimePartitioning(
              type_=tp_type,
              field=tp_field,
              expiration_ms=tp_exp,
              require_partition_filter=tp_req)
      rp = getattr(table, "rangePartitioning", None) or getattr(
          table, "range_partitioning", None)
      if rp is not None:
        if isinstance(rp, gcp_bigquery.RangePartitioning):
          gcp_table.range_partitioning = rp
        else:
          rp_field = getattr(rp, "field", None)
          rp_range = getattr(rp, "range", None) or getattr(rp, "range_", None)
          if rp_range is not None and hasattr(gcp_bigquery, "PartitionRange"):
            start = getattr(rp_range, "start", None)
            end = getattr(rp_range, "end", None)
            interval = getattr(rp_range, "interval", None)
            rp_range = gcp_bigquery.PartitionRange(
                start=start, end=end, interval=interval)
          gcp_table.range_partitioning = gcp_bigquery.RangePartitioning(
              field=rp_field, range_=rp_range)
      clustering = getattr(table, "clustering", None)
      if clustering is not None:
        fields = getattr(clustering, "fields", clustering)
        if isinstance(fields, (list, tuple)):
          gcp_table.clustering_fields = list(fields)
      if getattr(table, "description", None):
        gcp_table.description = table.description
      if getattr(table, "friendlyName", None) or getattr(
          table, "friendly_name", None):
        gcp_table.friendly_name = getattr(
            table, "friendlyName", None) or getattr(
                table, "friendly_name", None)
      dict_labels = _extract_dict_labels(getattr(table, "labels", None))
      if dict_labels:
        gcp_table.labels = dict_labels
      kms = getattr(
          getattr(table, "encryptionConfiguration", None),
          "kmsKeyName",
          None) or getattr(
              getattr(table, "encryption_configuration", None),
              "kms_key_name",
              None)
      if kms:
        gcp_table.encryption_configuration = (
            gcp_bigquery.EncryptionConfiguration(kms_key_name=kms))
    return self._client.create_table(gcp_table, exists_ok=True)

  def Delete(self, request):
    t_ref = getattr(request, "tableReference", None)
    proj = getattr(t_ref, "projectId", None) or getattr(
        request, "projectId", None)
    ds_id = getattr(t_ref, "datasetId", None) or getattr(
        request, "datasetId", None)
    tbl_id = getattr(t_ref, "tableId", None) or getattr(
        request, "tableId", None)
    gcp_tbl_ref = gcp_bigquery.TableReference(
        gcp_bigquery.DatasetReference(
            proj or getattr(self._client, "project", None) or "default", ds_id),
        tbl_id)
    return self._client.delete_table(gcp_tbl_ref, not_found_ok=True)

  def List(self, request):
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    ds_ref = gcp_bigquery.DatasetReference(
        proj or getattr(self._client, "project", None) or "default", ds_id)
    return self._client.list_tables(ds_ref)

  def Patch(self, request):
    table = getattr(request, "table", None)
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    tbl_id = getattr(request, "tableId", None)
    gcp_tbl_ref = gcp_bigquery.TableReference(
        gcp_bigquery.DatasetReference(
            proj or getattr(self._client, "project", None) or "default", ds_id),
        tbl_id)
    gcp_table = gcp_bigquery.Table(gcp_tbl_ref)
    if table and getattr(table, "schema", None):
      gcp_table.schema = _to_gcp_schema(table.schema)
    return self._client.update_table(gcp_table, ["schema"])

  def Update(self, request):
    return self.Patch(request)


class _ClientDatasetsCompat:
  def __init__(self, client):
    self._client = client

  def Get(self, request):
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    ds_ref = gcp_bigquery.DatasetReference(
        proj or getattr(self._client, "project", None) or "default", ds_id)
    return self._client.get_dataset(ds_ref)

  def Insert(self, request):
    dataset = getattr(request, "dataset", None)
    ds_ref_raw = getattr(dataset, "datasetReference", None) if dataset else None
    proj = getattr(ds_ref_raw, "projectId", None) or getattr(
        request, "projectId", None)
    ds_id = getattr(ds_ref_raw, "datasetId", None) or getattr(
        request, "datasetId", None)
    ds_ref = gcp_bigquery.DatasetReference(
        proj or getattr(self._client, "project", None) or "default", ds_id)
    gcp_ds = gcp_bigquery.Dataset(ds_ref)
    if dataset:
      if getattr(dataset, "location", None):
        gcp_ds.location = dataset.location
      if getattr(dataset, "defaultTableExpirationMs", None):
        gcp_ds.default_table_expiration_ms = dataset.defaultTableExpirationMs
    return self._client.create_dataset(gcp_ds, exists_ok=True)

  def Delete(self, request):
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    delete_contents = getattr(request, "deleteContents", True)
    ds_ref = gcp_bigquery.DatasetReference(
        proj or getattr(self._client, "project", None) or "default", ds_id)
    return self._client.delete_dataset(
        ds_ref, delete_contents=delete_contents, not_found_ok=True)

  def List(self, request):
    proj = getattr(request, "projectId", None) or getattr(
        self._client, "project", None)
    return self._client.list_datasets(project=proj)

  def Patch(self, request):
    dataset = getattr(request, "dataset", None)
    proj = getattr(request, "projectId", None)
    ds_id = getattr(request, "datasetId", None)
    ds_ref = gcp_bigquery.DatasetReference(
        proj or getattr(self._client, "project", None) or "default", ds_id)
    gcp_ds = gcp_bigquery.Dataset(ds_ref)
    fields_to_update = []
    if dataset:
      if getattr(dataset, "defaultTableExpirationMs", None):
        gcp_ds.default_table_expiration_ms = dataset.defaultTableExpirationMs
        fields_to_update.append("default_table_expiration_ms")
    return self._client.update_dataset(gcp_ds, fields_to_update)

  def Update(self, request):
    return self.Patch(request)


class _ClientJobsCompat:
  def __init__(self, client):
    self._client = client

  def Get(self, request):
    proj = getattr(request, "projectId", None)
    job_id = getattr(request, "jobId", None)
    loc = getattr(request, "location", None)
    return self._client.get_job(job_id, project=proj, location=loc)

  def GetQueryResults(self, request):
    proj = getattr(request, "projectId", None)
    job_id = getattr(request, "jobId", None)
    loc = getattr(request, "location", None)
    page_token = getattr(request, "pageToken", None)
    max_results = getattr(request, "maxResults", None)
    job = self._client.get_job(job_id, project=proj, location=loc)
    if page_token is not None:
      return self._client.list_rows(
          job, page_token=page_token, max_results=max_results)
    return job.result(max_results=max_results)

  def Insert(self, request, upload=None):
    job_obj = getattr(request, "job", None)
    job_ref = (
        getattr(job_obj, "jobReference", None)
        if job_obj else getattr(request, "jobReference", None))
    job_id = getattr(job_ref, "jobId", None) or getattr(job_ref, "job_id", None)
    proj = (
        getattr(request, "projectId", None) or
        getattr(job_ref, "projectId", None) or
        getattr(job_ref, "project", None))
    config = getattr(job_obj, "configuration", None) if job_obj else None
    if config and getattr(config, "query", None):
      q = config.query
      dest = None
      if getattr(q, "destinationTable", None):
        dest = _to_gcp_table_ref(q.destinationTable, default_project=proj)
      dict_labels = _extract_dict_labels(getattr(config, "labels", None))
      job_config = gcp_bigquery.QueryJobConfig(
          dry_run=getattr(q, "dryRun", False),
          use_legacy_sql=getattr(q, "useLegacySql", False)
          if getattr(q, "useLegacySql", None) is not None else False,
          flatten_results=getattr(q, "flattenResults", None),
          priority=getattr(q, "priority", "INTERACTIVE"),
          destination=dest,
      )
      if dict_labels:
        job_config.labels = dict_labels
      kms = getattr(
          getattr(q, "destinationEncryptionConfiguration", None),
          "kmsKeyName",
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
    elif config and getattr(config, "load", None):
      ld = config.load
      dest = _to_gcp_table_ref(
          getattr(ld, "destinationTable", None), default_project=proj)
      uris = list(getattr(ld, "sourceUris", []))
      if uris:
        job_config = gcp_bigquery.LoadJobConfig()
        if dict_labels:
          job_config.labels = dict_labels
        if getattr(ld, "schema", None):
          job_config.schema = _to_gcp_schema(ld.schema)
        if getattr(ld, "writeDisposition", None):
          job_config.write_disposition = ld.writeDisposition
        if getattr(ld, "createDisposition", None):
          job_config.create_disposition = ld.createDisposition
        return self._client.load_table_from_uri(
            uris, dest, job_config=job_config, job_id=job_id, project=proj)
    elif config and getattr(config, "copy", None):
      cp = config.copy
      sources = [
          _to_gcp_table_ref(s, default_project=proj)
          for s in getattr(cp, "sourceTables", [])
      ]
      dest = _to_gcp_table_ref(
          getattr(cp, "destinationTable", None), default_project=proj)
      job_config = gcp_bigquery.CopyJobConfig()
      if dict_labels:
        job_config.labels = dict_labels
      if getattr(cp, "writeDisposition", None):
        job_config.write_disposition = cp.writeDisposition
      if getattr(cp, "createDisposition", None):
        job_config.create_disposition = cp.createDisposition
      return self._client.copy_table(
          sources, dest, job_config=job_config, job_id=job_id, project=proj)
    elif config and getattr(config, "extract", None):
      ex = config.extract
      src = _to_gcp_table_ref(
          getattr(ex, "sourceTable", None), default_project=proj)
      uris = list(getattr(ex, "destinationUris", []))
      job_config = gcp_bigquery.ExtractJobConfig()
      if dict_labels:
        job_config.labels = dict_labels
      if getattr(ex, "destinationFormat", None):
        job_config.destination_format = ex.destinationFormat
      return self._client.extract_table(
          src, uris, job_config=job_config, job_id=job_id, project=proj)

    return self._client.get_job(job_id, project=proj)


# Automatically execute compatibility setup and monkey-patching upon import
_patch_protorpclite_equality()
_patch_gcp_bigquery()
