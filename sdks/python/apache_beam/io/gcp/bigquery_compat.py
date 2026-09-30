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

"""Bridge for legacy apitools BigQuery message inputs.

Converts instances of ``apache_beam.io.gcp.internal.clients.bigquery``
messages (TableReference, DatasetReference, JobReference, TableSchema,
TableFieldSchema) into Beam's BigQuery domain types. Using those messages is
deprecated; this module is removed together with the apitools client.

Conversion goes through ``apitools.base.py.encoding.MessageToDict``, so it is
lossless: every field set on the message appears in the REST-shaped result.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

# pytype: skip-file

import warnings
from typing import Union

from apache_beam.io.gcp import bigquery_resources
from apache_beam.io.gcp.bigquery_resources import DatasetRef
from apache_beam.io.gcp.bigquery_resources import JobRef
from apache_beam.io.gcp.bigquery_resources import SchemaDict
from apache_beam.io.gcp.bigquery_resources import TableRef
from apache_beam.utils.annotations import BeamDeprecationWarning

__all__ = [
    'dataset_ref_from_legacy',
    'from_legacy',
    'job_ref_from_legacy',
    'schema_from_legacy',
    'table_ref_from_legacy',
]

# Filled in once the release that removes apitools is known.
REMOVAL_VERSION = 'a future release'

# What to use instead of each legacy message class.
_ALTERNATIVES = {
    'TableReference': (
        "a 'project:dataset.table' string, a dict, or "
        "google.cloud.bigquery.TableReference"),
    'DatasetReference': (
        "a 'project:dataset' string, a dict, or "
        "google.cloud.bigquery.DatasetReference"),
    'JobReference': (
        'a dict, or a google.cloud.bigquery job such as '
        'google.cloud.bigquery.QueryJob'),
    'TableSchema': (
        "a 'name:TYPE,...' string, a dict, or a list of "
        "google.cloud.bigquery.SchemaField"),
    'TableFieldSchema': (
        "a 'name:TYPE,...' string, a dict, or a list of "
        "google.cloud.bigquery.SchemaField"),
}


def _message_to_dict(msg):
  from apitools.base.py import encoding
  return encoding.MessageToDict(msg)


def _warn(class_name):
  # Frames: _warn, the public converter, the bigquery_resources normalizer (or
  # from_legacy), then the code that passed in the message. stacklevel=4
  # attributes the warning to that code.
  warnings.warn(
      'Passing apitools %s to BigQuery IO is deprecated and will be removed '
      'in %s; use %s instead.' %
      (class_name, REMOVAL_VERSION, _ALTERNATIVES[class_name]),
      BeamDeprecationWarning,
      stacklevel=4)


def _check_legacy(msg, *class_names):
  if not (bigquery_resources._is_legacy_message(msg) and
          type(msg).__name__ in class_names):
    raise TypeError(
        'Expected an apitools %s, got %s.' %
        (' or '.join(class_names), _describe(msg)))


def _describe(value):
  t = type(value)
  return '%s.%s' % (t.__module__, t.__qualname__)


def table_ref_from_legacy(msg) -> TableRef:
  """Converts an apitools TableReference into a TableRef."""
  _check_legacy(msg, 'TableReference')
  _warn('TableReference')
  return TableRef.from_api_repr(_message_to_dict(msg))


def dataset_ref_from_legacy(msg) -> DatasetRef:
  """Converts an apitools DatasetReference into a DatasetRef."""
  _check_legacy(msg, 'DatasetReference')
  _warn('DatasetReference')
  return DatasetRef.from_api_repr(_message_to_dict(msg))


def job_ref_from_legacy(msg) -> JobRef:
  """Converts an apitools JobReference into a JobRef."""
  _check_legacy(msg, 'JobReference')
  _warn('JobReference')
  return JobRef.from_api_repr(_message_to_dict(msg))


def schema_from_legacy(msg) -> SchemaDict:
  """Converts an apitools TableSchema, or a list of TableFieldSchema, into a
  REST-shaped schema dict. The conversion is lossless."""
  if isinstance(msg, (list, tuple)):
    for field in msg:
      _check_legacy(field, 'TableFieldSchema')
    _warn('TableFieldSchema')
    return {'fields': [_message_to_dict(f) for f in msg]}
  _check_legacy(msg, 'TableSchema')
  _warn('TableSchema')
  result = _message_to_dict(msg)
  result.setdefault('fields', [])
  return result


_CONVERTERS = {
    'TableReference': table_ref_from_legacy,
    'DatasetReference': dataset_ref_from_legacy,
    'JobReference': job_ref_from_legacy,
    'TableSchema': schema_from_legacy,
}


def from_legacy(value) -> Union[TableRef, DatasetRef, JobRef, SchemaDict]:
  """Converts a legacy apitools message into the matching domain value.

  Dispatches on the message class name: TableReference, DatasetReference,
  JobReference, TableSchema, or a list of TableFieldSchema.

  Raises:
    TypeError: for unsupported classes (e.g. Table, Job, TableRow) and
      non-apitools inputs.
  """
  if isinstance(value, (list, tuple)):
    return schema_from_legacy(value)
  converter = _CONVERTERS.get(type(value).__name__)
  if converter is None or not bigquery_resources._is_legacy_message(value):
    raise TypeError(
        'Unsupported legacy BigQuery input %s; supported apitools classes are '
        '%s and lists of TableFieldSchema.' %
        (_describe(value), ', '.join(_CONVERTERS)))
  return converter(value)
