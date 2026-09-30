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

"""Unit tests for the legacy apitools input bridge in bigquery_compat.

.. note::
   This test suite is removed together with ``bigquery_compat.py`` when the
   apitools client is removed.
"""

# pytype: skip-file

import unittest

from apache_beam.io.gcp.bigquery_resources import DatasetRef
from apache_beam.io.gcp.bigquery_resources import JobRef
from apache_beam.io.gcp.bigquery_resources import TableRef
from apache_beam.io.gcp.bigquery_resources import to_dataset_ref
from apache_beam.io.gcp.bigquery_resources import to_job_ref
from apache_beam.io.gcp.bigquery_resources import to_schema_dict
from apache_beam.io.gcp.bigquery_resources import to_table_ref
from apache_beam.utils.annotations import BeamDeprecationWarning

try:
  from apitools.base.py import encoding  # pylint: disable=unused-import

  from apache_beam.io.gcp import bigquery_compat
  from apache_beam.io.gcp.internal.clients import bigquery as apitools_bigquery
except ImportError:
  bigquery_compat = None
  apitools_bigquery = None

_DEPRECATED = (
    'Passing apitools %s to BigQuery IO is deprecated and will be removed in '
    'a future release; use ')


@unittest.skipIf(apitools_bigquery is None, 'apitools is not installed')
class BigQueryCompatTest(unittest.TestCase):
  def _check_message(self, ctx, class_name, alternative):
    message = str(ctx.warning)
    self.assertTrue(message.startswith(_DEPRECATED % class_name), message)
    self.assertIn(alternative, message)

  def test_table_ref(self):
    msg = apitools_bigquery.TableReference(
        projectId='p', datasetId='d', tableId='t$20240101')
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      ref = bigquery_compat.table_ref_from_legacy(msg)
    self.assertEqual(ref, TableRef('p', 'd', 't$20240101'))
    self._check_message(
        ctx, 'TableReference', 'google.cloud.bigquery.TableReference')
    self.assertIn("'project:dataset.table' string", str(ctx.warning))

  def test_table_ref_without_project(self):
    msg = apitools_bigquery.TableReference(datasetId='d', tableId='t')
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          bigquery_compat.table_ref_from_legacy(msg), TableRef(None, 'd', 't'))

  def test_table_ref_missing_table_id(self):
    msg = apitools_bigquery.TableReference(projectId='p', datasetId='d')
    with self.assertWarns(BeamDeprecationWarning):
      with self.assertRaises(ValueError):
        bigquery_compat.table_ref_from_legacy(msg)

  def test_dataset_ref(self):
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      ref = bigquery_compat.dataset_ref_from_legacy(
          apitools_bigquery.DatasetReference(projectId='p', datasetId='d'))
    self.assertEqual(ref, DatasetRef('p', 'd'))
    self._check_message(
        ctx, 'DatasetReference', 'google.cloud.bigquery.DatasetReference')
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          bigquery_compat.dataset_ref_from_legacy(
              apitools_bigquery.DatasetReference(datasetId='d')),
          DatasetRef(None, 'd'))

  def test_job_ref(self):
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      ref = bigquery_compat.job_ref_from_legacy(
          apitools_bigquery.JobReference(projectId='p', jobId='j'))
    self.assertEqual(ref, JobRef('p', 'j'))
    self.assertIsNone(ref.location)
    self._check_message(ctx, 'JobReference', 'google.cloud.bigquery')
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          bigquery_compat.job_ref_from_legacy(
              apitools_bigquery.JobReference(
                  projectId='p', jobId='j', location='EU')),
          JobRef('p', 'j', 'EU'))

  def test_job_ref_without_project(self):
    with self.assertWarns(BeamDeprecationWarning):
      with self.assertRaises(ValueError):
        bigquery_compat.job_ref_from_legacy(
            apitools_bigquery.JobReference(jobId='j'))

  def _schema(self):
    tfs = apitools_bigquery.TableFieldSchema
    return apitools_bigquery.TableSchema(
        fields=[
            tfs(name='a', type='STRING', mode='REQUIRED', description='desc'),
            tfs(
                name='s',
                type='STRING',
                maxLength=5,
                defaultValueExpression="'x'",
                policyTags=tfs.PolicyTagsValue(names=['tag'])),
            tfs(name='n', type='NUMERIC', precision=10, scale=2),
            tfs(
                name='r',
                type='RECORD',
                mode='REPEATED',
                fields=[
                    tfs(name='c', type='INTEGER'),
                    tfs(
                        name='rr',
                        type='RECORD',
                        fields=[tfs(name='x', type='STRING', mode='NULLABLE')]),
                ]),
        ])

  _EXPECTED_FIELDS = [
      {
          'name': 'a',
          'type': 'STRING',
          'mode': 'REQUIRED',
          'description': 'desc'
      },
      {
          'name': 's',
          'type': 'STRING',
          'maxLength': '5',
          'defaultValueExpression': "'x'",
          'policyTags': {
              'names': ['tag']
          },
      },
      {
          'name': 'n', 'type': 'NUMERIC', 'precision': '10', 'scale': '2'
      },
      {
          'name': 'r',
          'type': 'RECORD',
          'mode': 'REPEATED',
          'fields': [
              {
                  'name': 'c', 'type': 'INTEGER'
              },
              {
                  'name': 'rr',
                  'type': 'RECORD',
                  'fields': [{
                      'name': 'x', 'type': 'STRING', 'mode': 'NULLABLE'
                  }],
              },
          ],
      },
  ]

  def test_schema_lossless(self):
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      result = bigquery_compat.schema_from_legacy(self._schema())
    # int64 fields (maxLength, precision, scale) use the REST JSON string form.
    self.assertEqual(result, {'fields': self._EXPECTED_FIELDS})
    self._check_message(
        ctx, 'TableSchema', 'a list of google.cloud.bigquery.SchemaField')

  def test_schema_field_list(self):
    fields = list(self._schema().fields)
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      result = bigquery_compat.schema_from_legacy(fields)
    self.assertEqual(result, {'fields': self._EXPECTED_FIELDS})
    self._check_message(ctx, 'TableFieldSchema', 'SchemaField')
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          bigquery_compat.schema_from_legacy(tuple(fields)), result)

  def test_empty_schema(self):
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          bigquery_compat.schema_from_legacy(apitools_bigquery.TableSchema()),
          {'fields': []})

  def test_from_legacy_dispatch(self):
    cases = [
        (
            apitools_bigquery.TableReference(
                projectId='p', datasetId='d', tableId='t'),
            TableRef('p', 'd', 't')),
        (
            apitools_bigquery.DatasetReference(projectId='p', datasetId='d'),
            DatasetRef('p', 'd')),
        (
            apitools_bigquery.JobReference(projectId='p', jobId='j'),
            JobRef('p', 'j')),
        (self._schema(), {
            'fields': self._EXPECTED_FIELDS
        }),
        (list(self._schema().fields), {
            'fields': self._EXPECTED_FIELDS
        }),
    ]
    for value, expected in cases:
      with self.assertWarns(BeamDeprecationWarning):
        self.assertEqual(bigquery_compat.from_legacy(value), expected)

  def test_unsupported_legacy_classes(self):
    for value in (apitools_bigquery.Table(),
                  apitools_bigquery.Job(),
                  apitools_bigquery.TableRow(),
                  apitools_bigquery.TableFieldSchema(name='a', type='STRING')):
      with self.assertRaises(TypeError) as ctx:
        bigquery_compat.from_legacy(value)
      self.assertIn(type(value).__name__, str(ctx.exception))

  def test_non_legacy_inputs_rejected(self):
    for value in ('p:d.t', {'datasetId': 'd', 'tableId': 't'},
                  TableRef('p', 'd', 't'),
                  None):
      with self.assertRaises(TypeError):
        bigquery_compat.from_legacy(value)

  def test_wrong_class_for_converter(self):
    table_ref = apitools_bigquery.TableReference(datasetId='d', tableId='t')
    dataset_ref = apitools_bigquery.DatasetReference(datasetId='d')
    bad = [
        (
            bigquery_compat.table_ref_from_legacy,
            dataset_ref,
            'DatasetReference'),
        (bigquery_compat.table_ref_from_legacy, 'p:d.t', 'str'),
        (bigquery_compat.dataset_ref_from_legacy, table_ref, 'TableReference'),
        (bigquery_compat.job_ref_from_legacy, table_ref, 'TableReference'),
        (bigquery_compat.schema_from_legacy, table_ref, 'TableReference'),
        (bigquery_compat.schema_from_legacy, [table_ref], 'TableReference'),
        (
            bigquery_compat.schema_from_legacy,
            self._schema().fields[0],
            'TableFieldSchema'),
    ]
    for converter, value, class_name in bad:
      with self.assertRaises(TypeError) as ctx:
        converter(value)
      self.assertIn(class_name, str(ctx.exception))

  def test_warning_attributed_to_normalizer_caller(self):
    msg = apitools_bigquery.TableReference(
        projectId='p', datasetId='d', tableId='t')
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      to_table_ref(msg)
    self.assertEqual(ctx.filename, __file__)
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      bigquery_compat.from_legacy(msg)
    self.assertEqual(ctx.filename, __file__)

  def test_normalizers_route_through_bridge(self):
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_table_ref(
              apitools_bigquery.TableReference(
                  projectId='p', datasetId='d', tableId='t')),
          TableRef('p', 'd', 't'))
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_dataset_ref(
              apitools_bigquery.DatasetReference(projectId='p', datasetId='d')),
          DatasetRef('p', 'd'))
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_job_ref(
              apitools_bigquery.JobReference(
                  projectId='p', jobId='j', location='US')),
          JobRef('p', 'j', 'US'))
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_schema_dict(self._schema()), {'fields': self._EXPECTED_FIELDS})
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_schema_dict(list(self._schema().fields)),
          {'fields': self._EXPECTED_FIELDS})

  def test_no_opt_in_or_patching(self):
    # The bridge has no env-var gate and patches nothing on import.
    for name in ('_check_compat_opt_in',
                 'BIGQUERY_COMPAT_ENV_VAR',
                 '_patch_gcp_bigquery',
                 '_patch_protorpclite_equality'):
      self.assertFalse(hasattr(bigquery_compat, name), name)
    self.assertEqual(bigquery_compat.REMOVAL_VERSION, 'a future release')


if __name__ == '__main__':
  unittest.main()
