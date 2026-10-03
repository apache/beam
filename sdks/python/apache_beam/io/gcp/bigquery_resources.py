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

"""Beam-owned BigQuery resource types and input normalizers.

This module defines small, immutable references to BigQuery resources
(:class:`DatasetRef`, :class:`TableRef`, :class:`JobRef`), the table-spec
parser, and normalizers that convert the inputs BigQuery IO accepts (strings,
REST-shaped dicts, ``google.cloud.bigquery`` objects and, through
``bigquery_compat``, legacy apitools messages) into those types or into a
REST-shaped schema dict.

This module is internal to Beam's BigQuery IO. Classes, constants and
functions in this file are experimental and have NO BACKWARDS COMPATIBILITY
GUARANTEES.

The module deliberately imports neither apitools nor ``google.cloud.bigquery``
nor ``google.api_core``, so it can be used in environments without the GCP
extras. Library objects are recognized by the module and name of their class.
"""

# pytype: skip-file

import copy
import dataclasses
import re
import warnings
from typing import Any
from typing import Optional

from apache_beam.options.value_provider import ValueProvider
from apache_beam.utils.annotations import BeamDeprecationWarning

try:
  import regex
except ImportError:
  regex = None

__all__ = [
    'DatasetRef',
    'JobRef',
    'SchemaDict',
    'TableRef',
    'is_deferred',
    'to_dataset_ref',
    'to_job_ref',
    'to_schema_dict',
    'to_table_ref',
]

# A REST-shaped BigQuery table schema: {'fields': [{'name', 'type', ...}]}.
SchemaDict = dict[str, Any]

# Patterns are copied from bigquery_tools.parse_table_reference so that
# TableRef.parse has the same behavior for string inputs.
_PROJECT_PATTERN = r'([a-z0-9.-]+:)?[a-z][a-z0-9-]*[a-z0-9]'
_DATASET_PATTERN = r'\w{1,1024}'
_TABLE_PATTERN_REGEX = r'[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}$]{1,1024}'
# Used when the third-party `regex` module is not installed. Stdlib `re` has no
# Unicode property classes, so this accepts `\w` letters, digits and `_`,
# whitespace, `-` and `$`. Unlike the `regex` pattern it rejects combining
# marks (\p{M}) and non-ASCII dashes and connectors.
_TABLE_PATTERN_FALLBACK = r'[\w\s\-$]{1,1024}'

_TABLE_SPEC_ERROR = (
    'Expected a table reference (PROJECT:DATASET.TABLE or '
    'DATASET.TABLE) instead of %s.')
_DATASET_SPEC_ERROR = (
    'Expected a dataset reference (PROJECT:DATASET or DATASET) instead of %s.')


def _compile_patterns(use_regex):
  """Returns the compiled full table-spec pattern.

  Args:
    use_regex: If True, compile with the third-party `regex` module and the
      Unicode-property table pattern. Otherwise use stdlib `re` and the
      fallback table pattern.
  """
  table_pattern = _TABLE_PATTERN_REGEX if use_regex else _TABLE_PATTERN_FALLBACK
  pattern = (
      f'((?P<project>{_PROJECT_PATTERN})[:\\.])?'
      f'(?P<dataset>{_DATASET_PATTERN})\\.(?P<table>{table_pattern})')
  if use_regex:
    if regex is None:
      raise ImportError('The regex module is not installed.')
    return regex.compile(pattern)
  return re.compile(pattern)


_TABLE_SPEC_RE = _compile_patterns(regex is not None)
_DATASET_SPEC_RE = re.compile(
    f'((?P<project>{_PROJECT_PATTERN})[:\\.])?(?P<dataset>{_DATASET_PATTERN})')


def _check_str(cls_name, name, value, optional=False):
  if optional and value is None:
    return
  if not isinstance(value, str):
    raise TypeError(
        '%s.%s must be a str%s, got %r.' %
        (cls_name, name, ' or None' if optional else '', value))


def _from_rest_dict(cls_name, value, required, optional):
  """Validates REST dict keys and returns the values keyed by REST name."""
  unknown = set(value) - set(required) - set(optional)
  if unknown:
    raise ValueError(
        'Unexpected keys %s for %s; expected REST keys %s.' %
        (sorted(unknown), cls_name, list(required) + list(optional)))
  missing = [k for k in required if value.get(k) is None]
  if missing:
    raise ValueError('Missing keys %s for %s: %r.' % (missing, cls_name, value))
  return value


@dataclasses.dataclass(frozen=True)
class DatasetRef:
  """An immutable reference to a BigQuery dataset.

  ``project`` may be None until the reference is resolved against a default
  project with :meth:`resolve`.
  """
  project: Optional[str]
  dataset_id: str

  def __post_init__(self):
    _check_str('DatasetRef', 'project', self.project, optional=True)
    _check_str('DatasetRef', 'dataset_id', self.dataset_id)

  @classmethod
  def from_api_repr(cls, value: dict[str, Any]) -> 'DatasetRef':
    """Builds a DatasetRef from a REST ``{'projectId'?, 'datasetId'}`` dict."""
    _from_rest_dict('DatasetRef', value, ('datasetId', ), ('projectId', ))
    return cls(project=value.get('projectId'), dataset_id=value['datasetId'])

  def with_project(self, project: str) -> 'DatasetRef':
    return dataclasses.replace(self, project=project)

  def resolve(self, default_project: Optional[str]) -> 'DatasetRef':
    """Returns self if project is set, else a copy with default_project."""
    if self.project is not None or default_project is None:
      return self
    return self.with_project(default_project)

  def to_api_repr(self) -> dict[str, str]:
    result = {'datasetId': self.dataset_id}
    if self.project is not None:
      result['projectId'] = self.project
    return result

  def __str__(self) -> str:
    if self.project is None:
      return self.dataset_id
    return '%s:%s' % (self.project, self.dataset_id)


@dataclasses.dataclass(frozen=True)
class TableRef:
  """An immutable reference to a BigQuery table.

  ``project`` may be None until the reference is resolved against a default
  project with :meth:`resolve`. ``table_id`` may include a ``$`` partition
  decorator.
  """
  project: Optional[str]
  dataset_id: str
  table_id: str

  def __post_init__(self):
    _check_str('TableRef', 'project', self.project, optional=True)
    _check_str('TableRef', 'dataset_id', self.dataset_id)
    _check_str('TableRef', 'table_id', self.table_id)

  @classmethod
  def parse(
      cls,
      spec: str,
      dataset: Optional[str] = None,
      project: Optional[str] = None) -> 'TableRef':
    """Parses a table spec into a TableRef.

    Matches ``bigquery_tools.parse_table_reference`` for string inputs.

    Args:
      spec: If ``dataset`` is None, a full table spec:
        ``'DATASET.TABLE'``, ``'PROJECT:DATASET.TABLE'`` or
        ``'PROJECT.DATASET.TABLE'``, optionally with a ``$`` partition
        decorator. Otherwise the table ID alone, which is not validated.
      dataset: The dataset ID, or None if ``spec`` is a full table spec.
      project: The project ID. Only used when ``dataset`` is given; when
        ``spec`` is a full table spec it is ignored (use :meth:`resolve`).

    Raises:
      TypeError: if ``spec`` is not a str.
      ValueError: if ``spec`` does not match the expected format.
    """
    if not isinstance(spec, str):
      raise TypeError(
          'TableRef.parse expects a str, got %r. Deferred values '
          '(callables, ValueProviders) must be resolved first.' % (spec, ))
    if dataset is not None:
      return cls(project=project, dataset_id=dataset, table_id=spec)
    match = _TABLE_SPEC_RE.fullmatch(spec)
    if not match:
      raise ValueError(_TABLE_SPEC_ERROR % spec)
    return cls(
        project=match.group('project'),
        dataset_id=match.group('dataset'),
        table_id=match.group('table'))

  @classmethod
  def from_api_repr(cls, value: dict[str, Any]) -> 'TableRef':
    """Builds a TableRef from a REST
    ``{'projectId'?, 'datasetId', 'tableId'}`` dict."""
    _from_rest_dict(
        'TableRef', value, ('datasetId', 'tableId'), ('projectId', ))
    return cls(
        project=value.get('projectId'),
        dataset_id=value['datasetId'],
        table_id=value['tableId'])

  def with_project(self, project: str) -> 'TableRef':
    return dataclasses.replace(self, project=project)

  def with_table_id(self, table_id: str) -> 'TableRef':
    return dataclasses.replace(self, table_id=table_id)

  def resolve(self, default_project: Optional[str]) -> 'TableRef':
    """Returns self if project is set, else a copy with default_project."""
    if self.project is not None or default_project is None:
      return self
    return self.with_project(default_project)

  def dataset(self) -> DatasetRef:
    return DatasetRef(project=self.project, dataset_id=self.dataset_id)

  def to_spec(self) -> str:
    """Returns ``'project:dataset.table'``, or ``'dataset.table'``."""
    if self.project is None:
      return '%s.%s' % (self.dataset_id, self.table_id)
    return '%s:%s.%s' % (self.project, self.dataset_id, self.table_id)

  def to_sql(self, legacy: bool = False) -> str:
    """Returns the fully qualified table name quoted for SQL.

    Standard SQL: ``'`project.dataset.table`'``. Legacy SQL:
    ``'[project:dataset.table]'``.

    Raises:
      ValueError: if the project is not set.
    """
    if self.project is None:
      raise ValueError(
          'TableRef %s has no project; call resolve() before to_sql().' %
          self.to_spec())
    if legacy:
      return '[%s:%s.%s]' % (self.project, self.dataset_id, self.table_id)
    return '`%s.%s.%s`' % (self.project, self.dataset_id, self.table_id)

  def to_api_repr(self) -> dict[str, str]:
    result = {'datasetId': self.dataset_id, 'tableId': self.table_id}
    if self.project is not None:
      result['projectId'] = self.project
    return result

  def __str__(self) -> str:
    return self.to_spec()


@dataclasses.dataclass(frozen=True)
class JobRef:
  """An immutable reference to a BigQuery job."""
  project: str
  job_id: str
  location: Optional[str] = None

  def __post_init__(self):
    _check_str('JobRef', 'project', self.project)
    _check_str('JobRef', 'job_id', self.job_id)
    _check_str('JobRef', 'location', self.location, optional=True)

  @classmethod
  def from_api_repr(cls, value: dict[str, Any]) -> 'JobRef':
    """Builds a JobRef from a REST
    ``{'projectId', 'jobId', 'location'?}`` dict."""
    _from_rest_dict('JobRef', value, ('projectId', 'jobId'), ('location', ))
    return cls(
        project=value['projectId'],
        job_id=value['jobId'],
        location=value.get('location'))

  def to_api_repr(self) -> dict[str, str]:
    result = {'projectId': self.project, 'jobId': self.job_id}
    if self.location is not None:
      result['location'] = self.location
    return result

  # Transitional read-only camelCase aliases for users of the apitools
  # JobReference emitted by BigQueryBatchFileLoads. Removed with apitools.
  @property
  def jobId(self) -> str:  # pylint: disable=invalid-name
    warnings.warn(
        'JobRef.jobId is deprecated; use JobRef.job_id instead.',
        BeamDeprecationWarning,
        stacklevel=2)
    return self.job_id

  @property
  def projectId(self) -> str:  # pylint: disable=invalid-name
    warnings.warn(
        'JobRef.projectId is deprecated; use JobRef.project instead.',
        BeamDeprecationWarning,
        stacklevel=2)
    return self.project


# Input detection helpers.

_LEGACY_MODULE_PREFIX = 'apache_beam.io.gcp.internal.clients.bigquery'
_CLOUD_MODULE_PREFIX = 'google.cloud.bigquery'

_CLOUD_TABLE_CLASSES = ('TableReference', 'Table', 'TableListItem')
_CLOUD_DATASET_CLASSES = ('DatasetReference', 'Dataset', 'DatasetListItem')
_CLOUD_JOB_CLASSES = (
    'QueryJob', 'LoadJob', 'CopyJob', 'ExtractJob', 'UnknownJob')


def _is_legacy_message(value) -> bool:
  """True for apitools messages from internal.clients.bigquery."""
  return type(value).__module__.startswith(_LEGACY_MODULE_PREFIX)


def _is_cloud_client_object(value, *class_names) -> bool:
  """True for google.cloud.bigquery objects of one of the named classes."""
  t = type(value)
  return (
      t.__module__.startswith(_CLOUD_MODULE_PREFIX) and
      t.__name__ in class_names)


def is_deferred(value) -> bool:
  """True for a ValueProvider or callable, which callers resolve at runtime."""
  return isinstance(value, ValueProvider) or callable(value)


def _deferred_error(kind, value):
  return TypeError(
      'Cannot normalize deferred %s %r (a callable or ValueProvider); '
      'resolve it first.' % (kind, value))


def _parse_dataset_spec(spec, project):
  match = _DATASET_SPEC_RE.fullmatch(spec)
  if not match:
    raise ValueError(_DATASET_SPEC_ERROR % spec)
  return DatasetRef(
      project=match.group('project') or project,
      dataset_id=match.group('dataset'))


def to_table_ref(
    value,
    dataset: Optional[str] = None,
    project: Optional[str] = None) -> TableRef:
  """Converts a table reference input into a TableRef.

  Accepts a TableRef, a table spec string (see :meth:`TableRef.parse`), a REST
  ``{'projectId'?, 'datasetId', 'tableId'}`` dict, a
  ``google.cloud.bigquery`` TableReference, Table or TableListItem, or a legacy
  apitools TableReference (deprecated). ``dataset`` and ``project`` are only
  used for string inputs.

  Raises:
    TypeError: for deferred values and unsupported types.
    ValueError: for malformed strings or dicts.
  """
  if isinstance(value, TableRef):
    return value
  if isinstance(value, str):
    return TableRef.parse(value, dataset=dataset, project=project)
  if isinstance(value, dict):
    return TableRef.from_api_repr(value)
  if is_deferred(value):
    raise _deferred_error('table reference', value)
  if _is_cloud_client_object(value, *_CLOUD_TABLE_CLASSES):
    return TableRef(
        project=value.project,
        dataset_id=value.dataset_id,
        table_id=value.table_id)
  if _is_legacy_message(value):
    from apache_beam.io.gcp import bigquery_compat
    return bigquery_compat.table_ref_from_legacy(value)
  raise TypeError('Unexpected table reference argument: %r.' % (value, ))


def to_dataset_ref(value, project: Optional[str] = None) -> DatasetRef:
  """Converts a dataset reference input into a DatasetRef.

  Accepts a DatasetRef, a ``'PROJECT:DATASET'``, ``'PROJECT.DATASET'`` or
  ``'DATASET'`` string, a REST ``{'projectId'?, 'datasetId'}`` dict, a
  ``google.cloud.bigquery`` DatasetReference, Dataset or DatasetListItem, or a
  legacy apitools DatasetReference (deprecated). ``project`` is only used for
  strings that do not name a project.

  Raises:
    TypeError: for deferred values and unsupported types.
    ValueError: for malformed strings or dicts.
  """
  if isinstance(value, DatasetRef):
    return value
  if isinstance(value, str):
    return _parse_dataset_spec(value, project)
  if isinstance(value, dict):
    return DatasetRef.from_api_repr(value)
  if is_deferred(value):
    raise _deferred_error('dataset reference', value)
  if _is_cloud_client_object(value, *_CLOUD_DATASET_CLASSES):
    return DatasetRef(project=value.project, dataset_id=value.dataset_id)
  if _is_legacy_message(value):
    from apache_beam.io.gcp import bigquery_compat
    return bigquery_compat.dataset_ref_from_legacy(value)
  raise TypeError('Unexpected dataset reference argument: %r.' % (value, ))


def to_job_ref(value) -> JobRef:
  """Converts a job reference input into a JobRef.

  Accepts a JobRef, a REST ``{'projectId', 'jobId', 'location'?}`` dict, a
  ``google.cloud.bigquery`` job (QueryJob, LoadJob, CopyJob, ExtractJob,
  UnknownJob), or a legacy apitools JobReference (deprecated).

  Raises:
    TypeError: for strings, deferred values and unsupported types.
    ValueError: for malformed dicts.
  """
  if isinstance(value, JobRef):
    return value
  if isinstance(value, str):
    raise TypeError(
        'Cannot convert the string %r to a JobRef: a job reference needs a '
        'project. Use a JobRef or a {"projectId", "jobId"} dict.' % value)
  if isinstance(value, dict):
    return JobRef.from_api_repr(value)
  if is_deferred(value):
    raise _deferred_error('job reference', value)
  if _is_cloud_client_object(value, *_CLOUD_JOB_CLASSES):
    return JobRef(
        project=value.project, job_id=value.job_id, location=value.location)
  if _is_legacy_message(value):
    from apache_beam.io.gcp import bigquery_compat
    return bigquery_compat.job_ref_from_legacy(value)
  raise TypeError('Unexpected job reference argument: %r.' % (value, ))


def _drop_none(value):
  """Returns a deep copy of value without None-valued dict entries."""
  if isinstance(value, dict):
    return {k: _drop_none(v) for k, v in value.items() if v is not None}
  if isinstance(value, (list, tuple)):
    return [_drop_none(v) for v in value]
  return copy.deepcopy(value)


def _schema_from_string(schema):
  # Same algorithm as bigquery_tools.get_table_schema_from_string followed by
  # table_schema_to_dict.
  fields = []
  for field_and_type in [s.strip() for s in schema.split(',')]:
    field_name, field_type = field_and_type.split(':')
    fields.append({'name': field_name, 'type': field_type, 'mode': 'NULLABLE'})
  return {'fields': fields}


def to_schema_dict(value) -> SchemaDict:
  """Converts a table schema input into a REST-shaped schema dict.

  Accepts a dict (deep-copied, not validated), a ``'name:TYPE,...'`` string
  (every field NULLABLE), a list or tuple of ``google.cloud.bigquery``
  SchemaField objects, or a legacy apitools TableSchema or list of
  TableFieldSchema (deprecated).

  Raises:
    TypeError: for deferred values, None, and unsupported types.
    ValueError: for malformed strings.
  """
  if isinstance(value, dict):
    return copy.deepcopy(value)
  if isinstance(value, str):
    return _schema_from_string(value)
  if value is None or is_deferred(value):
    raise _deferred_error('schema', value)
  if isinstance(value, (list, tuple)):
    if all(_is_cloud_client_object(f, 'SchemaField') for f in value):
      return {'fields': [_drop_none(f.to_api_repr()) for f in value]}
    if all(_is_legacy_message(f) for f in value):
      from apache_beam.io.gcp import bigquery_compat
      return bigquery_compat.schema_from_legacy(value)
  elif _is_legacy_message(value):
    from apache_beam.io.gcp import bigquery_compat
    return bigquery_compat.schema_from_legacy(value)
  raise TypeError('Unexpected schema argument: %s.' % (value, ))
