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

"""Golden REST request tests for BigQueryWrapper (Phase 0, cases G1-G40).

Each test drives a real ``BigQueryWrapper`` (built with no arguments, see
``bigquery_http_recorder.recording_wrapper``) against scripted HTTP responses
and asserts:

* the normalized request sequence matches
  ``tests/goldens/bigquery/<case>.json``, and
* the return value / raised exception, written against today's types.

Regenerate goldens with ``BEAM_UPDATE_BQ_GOLDENS=1`` (reviewer sign-off
required; see ``tests/goldens/bigquery/README.md``).
"""

# pytype: skip-file

import datetime
import decimal
import io
import json
import logging
import unittest

from apache_beam.io.gcp.tests import bigquery_http_recorder as rec
from apache_beam.io.gcp.tests.bigquery_http_recorder import HAS_GCP_DEPS
from apache_beam.io.gcp.tests.bigquery_http_recorder import CannedResponse
from apache_beam.io.gcp.tests.bigquery_http_recorder import Responder
from apache_beam.io.gcp.tests.bigquery_http_recorder import message_to_dict

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.protorpclite.messages import ValidationError

  from apache_beam.io.gcp import bigquery_tools
  from apache_beam.io.gcp.internal.clients import bigquery
except ImportError:
  ValidationError = None
  bigquery_tools = None
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

HEX32 = r'[0-9a-f]{32}'
TABLE_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t'
TABLES_PATH = r'/bigquery/v2/projects/p/datasets/d/tables'
TABLEDATA_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t/data'
DATASET_PATH = r'/bigquery/v2/projects/p/datasets/d'
DATASETS_PATH = r'/bigquery/v2/projects/p/datasets'
JOBS_PATH = r'/bigquery/v2/projects/p/jobs'
UPLOAD_JOBS_PATH = r'/upload/bigquery/v2/projects/p/jobs'
INSERT_ALL_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t/insertAll'

SCHEMA_DICT = {
    'fields': [
        {
            'name': 'a', 'type': 'STRING', 'mode': 'REQUIRED'
        },
        {
            'name': 'r',
            'type': 'RECORD',
            'mode': 'REPEATED',
            'fields': [
                {
                    'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'
                },
                {
                    'name': 'y',
                    'type': 'TIMESTAMP',
                    'mode': 'NULLABLE',
                    'description': 'nested ts'
                },
            ]
        },
    ]
}


def _schema():
  return bigquery.TableSchema(
      fields=[
          bigquery.TableFieldSchema(name='a', type='STRING', mode='REQUIRED'),
          bigquery.TableFieldSchema(
              name='r',
              type='RECORD',
              mode='REPEATED',
              fields=[
                  bigquery.TableFieldSchema(
                      name='x', type='INTEGER', mode='NULLABLE'),
                  bigquery.TableFieldSchema(
                      name='y',
                      type='TIMESTAMP',
                      mode='NULLABLE',
                      description='nested ts'),
              ]),
      ])


def _table_ref(project='p', dataset='d', table='t'):
  return bigquery.TableReference(
      projectId=project, datasetId=dataset, tableId=table)


def _table_echo(request):
  """tables.insert: echo the request body into a full Table resource."""
  body = request.json()
  ref = body['tableReference']
  resource = rec.table_resource(
      ref['projectId'], ref['datasetId'], ref['tableId'])
  resource.update(body)
  return CannedResponse(200, resource)


def _dataset_echo(request):
  """datasets.insert: echo the request body into a full Dataset resource."""
  body = request.json()
  ref = body['datasetReference']
  resource = rec.dataset_resource(ref['projectId'], ref['datasetId'])
  resource.update(body)
  return CannedResponse(200, resource)


@unittest.skipIf(not HAS_GCP_DEPS, 'GCP dependencies are not installed')
class BigQueryWrapperGoldenTest(unittest.TestCase):
  def setUp(self):
    self.responder = Responder()
    self.sleeps = []

  def capture(self, call, expect_exception=None, **wrapper_kwargs):
    """Runs ``call(wrapper)`` under the recorder.

    Returns:
      ``(result, recorder)``; ``result`` is the raised exception when
      ``expect_exception`` is given.
    """
    with rec.no_sleep() as sleeps, rec.recording_wrapper(
        self.responder, **wrapper_kwargs) as (wrapper, recorder):
      self.wrapper = wrapper
      if expect_exception is None:
        result = call(wrapper)
      else:
        with self.assertRaises(expect_exception) as cm:
          call(wrapper)
        result = cm.exception
    self.sleeps = sleeps
    self.assertEqual(self.responder.unmatched, [])
    return result, recorder

  def golden(self, case_id, description, recorder, generated_job_id=False):
    rec.assert_matches_golden(
        self, case_id, description, recorder, generated_job_id)

  # ---------------------------------------------------------------------------
  # Tables (G1-G11).

  def test_g01_get_table__basic(self):
    resource = rec.table_resource('p', 'd', 't', schema=SCHEMA_DICT, num_rows=3)
    self.responder.on('GET', TABLE_PATH, CannedResponse(200, resource))
    result, recorder = self.capture(lambda w: w.get_table('p', 'd', 't'))
    self.golden('get_table__basic', "get_table('p','d','t')", recorder)
    self.assertIsInstance(result, bigquery.Table)
    self.assertEqual(message_to_dict(result), resource)

  def test_g02_get_table__domain_scoped_project(self):
    resource = rec.table_resource('google.com:p', 'd', 't')
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/google\.com:p/datasets/d/tables/t',
        CannedResponse(200, resource))
    result, recorder = self.capture(
        lambda w: w.get_table('google.com:p', 'd', 't'))
    self.golden(
        'get_table__domain_scoped_project',
        "get_table('google.com:p','d','t'): ':' is sent unencoded",
        recorder)
    self.assertEqual(message_to_dict(result), resource)

  def test_g03_create_table__schema_only(self):
    self.responder.on('POST', TABLES_PATH, _table_echo)
    result, recorder = self.capture(
        lambda w: w._create_table('p', 'd', 't', _schema()))
    self.golden(
        'create_table__schema_only',
        "_create_table('p','d','t', nested/repeated schema)",
        recorder)
    self.assertEqual(message_to_dict(result.schema), SCHEMA_DICT)
    self.assertEqual(
        message_to_dict(result.tableReference), {
            'projectId': 'p', 'datasetId': 'd', 'tableId': 't'
        })

  def test_g04_create_table__additional_params(self):
    # int64 fields must be Python ints: apitools message constructors reject
    # REST-style strings (see test_g04b_*).
    additional = {
        'timePartitioning': {
            'type': 'DAY', 'field': 'ts', 'expirationMs': 3600000
        },
        'clustering': {
            'fields': ['a']
        },
        'friendlyName': 'friendly',
        'description': 'desc',
        'encryptionConfiguration': {
            'kmsKeyName': 'projects/p/locations/l/keyRings/r/cryptoKeys/k'
        },
        'rangePartitioning': {
            'field': 'n', 'range': {
                'start': 0, 'end': 100, 'interval': 10
            }
        },
    }
    self.responder.on('POST', TABLES_PATH, _table_echo)
    result, recorder = self.capture(
        lambda w: w._create_table(
            'p', 'd', 't', _schema(), additional_parameters=additional))
    self.golden(
        'create_table__additional_params',
        '_create_table with additional_parameters passthrough '
        '(additional_bq_parameters): partitioning, clustering, '
        'encryption, friendlyName, description',
        recorder)
    self.assertEqual(
        json.loads(json.dumps(message_to_dict(result.timePartitioning))),
        json.loads(json.dumps(recorder.requests[0].json()['timePartitioning'])))
    self.assertEqual(message_to_dict(result.clustering), {'fields': ['a']})
    self.assertEqual(result.friendlyName, 'friendly')
    self.assertEqual(result.description, 'desc')

  def test_g04b_create_table__additional_params_rejected_before_send(self):
    # Characterization (not a golden). additional_bq_parameters are fed to
    # apitools message constructors, so two REST-shaped inputs fail before any
    # request is sent (INCIDENTAL, see the classification report):
    # * int64 fields given as JSON strings (the REST wire form) raise
    #   protorpclite ValidationError;
    # * 'labels' given as a plain dict raises AttributeError.
    for additional, exc_type in (
        ({'timePartitioning': {'type': 'DAY', 'expirationMs': '3600000'}},
         ValidationError),
        ({'labels': {'k': 'v'}}, AttributeError)):
      with self.subTest(additional=additional):
        self.responder = Responder()
        _, recorder = self.capture(
            lambda w: w._create_table(
                'p', 'd', 't', _schema(), additional_parameters=additional),
            expect_exception=exc_type)
        self.assertEqual(recorder.requests, [])

  def test_g05_get_or_create_table__exists_append(self):
    resource = rec.table_resource('p', 'd', 't', schema=SCHEMA_DICT)
    self.responder.on('GET', TABLE_PATH, CannedResponse(200, resource))
    result, recorder = self.capture(
        lambda w: w.get_or_create_table(
            'p', 'd', 't', _schema(), 'CREATE_IF_NEEDED', 'WRITE_APPEND'))
    self.golden(
        'get_or_create_table__exists_append',
        'get_or_create_table on an existing table with WRITE_APPEND: '
        'a single GET',
        recorder)
    self.assertEqual(message_to_dict(result), resource)

  def test_g06_get_or_create_table__missing_create(self):
    self.responder.on('GET', TABLE_PATH, rec.error(404, 'notFound'))
    self.responder.on('POST', TABLES_PATH, _table_echo)
    result, recorder = self.capture(
        lambda w: w.get_or_create_table(
            'p', 'd', 't', _schema(), 'CREATE_IF_NEEDED', 'WRITE_APPEND'))
    self.golden(
        'get_or_create_table__missing_create',
        'get_or_create_table on a missing table: GET 404 then tables.insert',
        recorder)
    self.assertEqual(message_to_dict(result.schema), SCHEMA_DICT)

  def test_g07_get_or_create_table__truncate(self):
    existing = rec.table_resource('p', 'd', 't', schema=SCHEMA_DICT)
    self.responder.on('GET', TABLE_PATH, CannedResponse(200, existing))
    self.responder.on('DELETE', TABLE_PATH, CannedResponse(204))
    self.responder.on('POST', TABLES_PATH, _table_echo)
    result, recorder = self.capture(
        lambda w: w.get_or_create_table(
            'p', 'd', 't', None, 'CREATE_IF_NEEDED', 'WRITE_TRUNCATE'))
    self.golden(
        'get_or_create_table__truncate',
        'WRITE_TRUNCATE on an existing table: GET, DELETE, re-create with '
        'the schema of the found table',
        recorder)
    self.assertEqual(message_to_dict(result.schema), SCHEMA_DICT)
    # The 150 s wait after re-creating a truncated table.
    self.assertEqual(self.sleeps, [150])

  def test_g08_get_or_create_table__write_empty_nonempty(self):
    existing = rec.table_resource('p', 'd', 't', schema=SCHEMA_DICT)
    self.responder.on('GET', TABLE_PATH, CannedResponse(200, existing))
    self.responder.on(
        'GET',
        TABLEDATA_PATH,
        CannedResponse(200, rec.tabledata_list(total_rows=5)))
    result, recorder = self.capture(
        lambda w: w.get_or_create_table(
            'p', 'd', 't', _schema(), 'CREATE_IF_NEEDED', 'WRITE_EMPTY'),
        expect_exception=RuntimeError)
    # The RuntimeError is retried by get_or_create_table's decorator
    # (retry_if_valid_input_but_server_error_and_timeout_filter only
    # excludes ValueError), so the GET + tabledata.list pair is sent
    # 1 + MAX_RETRIES times.
    self.golden(
        'get_or_create_table__write_empty_nonempty',
        'WRITE_EMPTY on a non-empty table: GET + tabledata.list '
        '(maxResults=1), RuntimeError retried by the Beam decorator',
        recorder)
    self.assertIn(
        'is not empty but write disposition is WRITE_EMPTY', str(result))
    self.assertEqual(
        len(recorder.requests), 2 * (1 + bigquery_tools.MAX_RETRIES))

  def test_g09_get_or_create_table__create_conflict(self):
    table = rec.table_resource('p', 'd', 't', schema=SCHEMA_DICT)
    self.responder.on(
        'GET',
        TABLE_PATH,
        rec.error(404, 'notFound'),
        CannedResponse(200, table))
    self.responder.on(
        'POST', TABLES_PATH, rec.error(409, 'duplicate', 'Already Exists'))
    result, recorder = self.capture(
        lambda w: w.get_or_create_table(
            'p', 'd', 't', _schema(), 'CREATE_IF_NEEDED', 'WRITE_APPEND'))
    self.golden(
        'get_or_create_table__create_conflict',
        'create races with another writer: GET 404, POST 409, re-GET',
        recorder)
    self.assertEqual(message_to_dict(result), table)

  def test_g10_is_table_empty(self):
    self.responder.on(
        'GET',
        TABLEDATA_PATH,
        CannedResponse(200, rec.tabledata_list(total_rows=0)))
    result, recorder = self.capture(lambda w: w._is_table_empty('p', 'd', 't'))
    self.golden('is_table_empty', "_is_table_empty('p','d','t')", recorder)
    self.assertIs(result, True)

  def test_g11_delete_table__ok(self):
    self.responder.on('DELETE', TABLE_PATH, CannedResponse(204))
    result, recorder = self.capture(lambda w: w._delete_table('p', 'd', 't'))
    self.golden('delete_table__ok', "_delete_table('p','d','t')", recorder)
    self.assertIsNone(result)

  def test_g11_delete_table__404(self):
    self.responder.on('DELETE', TABLE_PATH, rec.error(404, 'notFound'))
    result, recorder = self.capture(lambda w: w._delete_table('p', 'd', 't'))
    self.golden(
        'delete_table__404',
        "_delete_table('p','d','t') on a missing table: 404 swallowed",
        recorder)
    self.assertIsNone(result)

  # ---------------------------------------------------------------------------
  # Datasets and temporary dataset lifecycle (G12-G20).

  def test_g12_get_or_create_dataset__exists(self):
    resource = rec.dataset_resource('p', 'd')
    self.responder.on('GET', DATASET_PATH, CannedResponse(200, resource))
    result, recorder = self.capture(lambda w: w.get_or_create_dataset('p', 'd'))
    self.golden(
        'get_or_create_dataset__exists',
        "get_or_create_dataset('p','d') on an existing dataset",
        recorder)
    self.assertEqual(message_to_dict(result), resource)
    self.assertFalse(self.wrapper.created_temp_dataset)

  def test_g13_get_or_create_dataset__create_full(self):
    self.responder.on('GET', DATASET_PATH, rec.error(404, 'notFound'))
    self.responder.on('POST', DATASETS_PATH, _dataset_echo)
    result, recorder = self.capture(
        lambda w: w.get_or_create_dataset(
            'p',
            'd',
            location='EU',
            labels={
                'k1': 'v1', 'k2': 'v2'
            },
            kms_key='projects/p/locations/eu/keyRings/r/cryptoKeys/k',
            default_table_expiration_ms=3600000))
    self.golden(
        'get_or_create_dataset__create_full',
        'get_or_create_dataset on a missing dataset with location, labels, '
        'kms_key and default_table_expiration_ms',
        recorder)
    result_dict = message_to_dict(result)
    self.assertEqual(result_dict['location'], 'EU')
    self.assertEqual(result_dict['labels'], {'k1': 'v1', 'k2': 'v2'})
    self.assertEqual(result_dict['defaultTableExpirationMs'], '3600000')
    self.assertTrue(self.wrapper.created_temp_dataset)

  def test_g14_create_temporary_dataset__beam_generated(self):
    temp_path = r'/bigquery/v2/projects/p/datasets/beam_temp_dataset_' + HEX32
    self.responder.on('GET', temp_path, rec.error(404, 'notFound'))
    self.responder.on('POST', DATASETS_PATH, _dataset_echo)
    result, recorder = self.capture(
        lambda w: w.create_temporary_dataset('p', 'US'))
    self.golden(
        'create_temporary_dataset__beam_generated',
        "create_temporary_dataset('p','US') with a Beam-generated temp "
        'dataset name (24 h default table expiration)',
        recorder)
    self.assertIsNone(result)
    self.assertTrue(self.wrapper.created_temp_dataset)

  def test_g15_create_temporary_dataset__preexisting_raises(self):
    temp_path = r'/bigquery/v2/projects/p/datasets/beam_temp_dataset_' + HEX32
    self.responder.on(
        'GET', temp_path, CannedResponse(200, rec.dataset_resource('p', 'x')))
    result, recorder = self.capture(
        lambda w: w.create_temporary_dataset('p', 'US'),
        expect_exception=RuntimeError)
    # The RuntimeError is retried by create_temporary_dataset's decorator
    # (retry_on_server_errors_and_timeout_filter retries non-HTTP errors).
    self.golden(
        'create_temporary_dataset__preexisting_raises',
        'Beam-generated temp dataset already exists: RuntimeError, retried '
        'by the Beam decorator',
        recorder)
    self.assertIn('already exists so cannot be used as temporary', str(result))
    self.assertEqual(len(recorder.requests), 1 + bigquery_tools.MAX_RETRIES)

  def test_g16_delete_dataset__contents(self):
    self.responder.on('DELETE', DATASET_PATH, CannedResponse(204))
    result, recorder = self.capture(lambda w: w._delete_dataset('p', 'd', True))
    self.golden(
        'delete_dataset__contents',
        "_delete_dataset('p','d', delete_contents=True)",
        recorder)
    self.assertIsNone(result)

  def test_g16_delete_dataset__404(self):
    self.responder.on('DELETE', DATASET_PATH, rec.error(404, 'notFound'))
    result, recorder = self.capture(lambda w: w._delete_dataset('p', 'd', True))
    self.golden(
        'delete_dataset__404',
        "_delete_dataset('p','d', True) on a missing dataset: 404 swallowed",
        recorder)
    self.assertIsNone(result)

  def test_g17_clean_up_temporary_dataset__generated(self):
    temp_path = r'/bigquery/v2/projects/p/datasets/beam_temp_dataset_' + HEX32
    self.responder.on(
        'GET', temp_path, CannedResponse(200, rec.dataset_resource('p', 'x')))
    self.responder.on('DELETE', temp_path, CannedResponse(204))
    result, recorder = self.capture(lambda w: w.clean_up_temporary_dataset('p'))
    self.golden(
        'clean_up_temporary_dataset__generated',
        "clean_up_temporary_dataset('p') with a Beam-generated temp dataset: "
        'dataset deleted with contents',
        recorder)
    self.assertIsNone(result)
    self.assertFalse(self.wrapper.created_temp_dataset)

  def test_g18_clean_up_temporary_dataset__user_configured(self):
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/p/datasets/user_ds',
        CannedResponse(200, rec.dataset_resource('p', 'user_ds')))
    self.responder.on(
        'DELETE',
        r'/bigquery/v2/projects/p/datasets/user_ds/tables/beam_temp_table_' +
        HEX32,
        CannedResponse(204))
    result, recorder = self.capture(
        lambda w: w.clean_up_temporary_dataset('p'),
        temp_dataset_id='user_ds')
    self.golden(
        'clean_up_temporary_dataset__user_configured',
        "clean_up_temporary_dataset('p') with temp_dataset_id='user_ds': "
        'only the temp table is deleted',
        recorder)
    self.assertIsNone(result)

  def test_g19_clean_up_temporary_dataset__403(self):
    temp_path = r'/bigquery/v2/projects/p/datasets/beam_temp_dataset_' + HEX32
    self.responder.on(
        'GET', temp_path, CannedResponse(200, rec.dataset_resource('p', 'x')))
    self.responder.on('DELETE', temp_path, rec.error(403, 'accessDenied'))
    result, recorder = self.capture(lambda w: w.clean_up_temporary_dataset('p'))
    self.golden(
        'clean_up_temporary_dataset__403',
        'clean_up_temporary_dataset: DELETE 403 is swallowed',
        recorder)
    self.assertIsNone(result)

  def test_g20_clean_up_labelled_datasets(self):
    self.responder.on(
        'GET',
        DATASETS_PATH,
        CannedResponse(
            200,
            rec.dataset_list(
                'p', ['beam_ds_1', 'beam_ds_2'], labels={'beam': 'temp'})))
    self.responder.on(
        'DELETE',
        r'/bigquery/v2/projects/p/datasets/beam_ds_[12]',
        CannedResponse(204))
    result, recorder = self.capture(
        lambda w: w._clean_up_beam_labelled_temporary_datasets(
            'p', labels={
                'beam': 'temp', 'step': 's1'
            }))
    self.golden(
        'clean_up_labelled_datasets',
        "_clean_up_beam_labelled_temporary_datasets('p', labels=...): "
        'datasets.list with a label filter, then delete each dataset',
        recorder)
    self.assertIsNone(result)

  # ---------------------------------------------------------------------------
  # Jobs (G21-G34).

  def test_g21_insert_load_job__uris_schema_labels(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_load_job(
            _table_ref(),
            'job_1',
            source_uris=['gs://b/a.json', 'gs://b/b.json'],
            schema=_schema(),
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            source_format='NEWLINE_DELIMITED_JSON',
            job_labels={'step_name': 's'}))
    self.golden(
        'insert_load_job__uris_schema_labels',
        'perform_load_job with GCS URIs, explicit schema, dispositions, '
        'source format and labels',
        recorder)
    self.assertEqual(
        message_to_dict(result), {
            'projectId': 'p', 'jobId': 'job_1', 'location': 'US'
        })

  def test_g22_insert_load_job__autodetect(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_load_job(
            _table_ref(),
            'job_1',
            source_uris=['gs://b/a.avro'],
            schema='SCHEMA_AUTODETECT',
            source_format='AVRO'))
    self.golden(
        'insert_load_job__autodetect',
        "perform_load_job with schema='SCHEMA_AUTODETECT'",
        recorder)
    self.assertEqual(result.jobId, 'job_1')

  def test_g23_insert_load_job__additional_params(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_load_job(
            _table_ref(),
            'job_1',
            source_uris=['gs://b/a.json'],
            schema=_schema(),
            additional_load_parameters={
                'timePartitioning': {
                    'type': 'DAY', 'field': 'ts'
                },
                'clustering': {
                    'fields': ['a']
                },
                'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION'],
                'ignoreUnknownValues': True,
                'maxBadRecords': 5,
            }))
    self.golden(
        'insert_load_job__additional_params',
        'perform_load_job with additional_load_parameters passthrough',
        recorder)
    self.assertEqual(result.jobId, 'job_1')

  def test_g24_insert_load_job__source_stream(self):
    self.responder.on('POST', UPLOAD_JOBS_PATH, rec.job_echo())

    def call(w):
      return [
          # UpdateDestinationSchema usage (bigquery_file_loads.py).
          w.perform_load_job(
              _table_ref(),
              'job_empty',
              source_stream=io.BytesIO(),
              schema=_schema(),
              write_disposition='WRITE_APPEND',
              create_disposition='CREATE_NEVER',
              additional_load_parameters={
                  'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION']
              }),
          w.perform_load_job(
              _table_ref(),
              'job_data',
              source_stream=io.BytesIO(b'some,data'),
              source_format='CSV'),
      ]

    result, recorder = self.capture(call)
    self.golden(
        'insert_load_job__source_stream',
        'perform_load_job with source_stream: multipart/related media upload '
        '(empty stream as used by UpdateDestinationSchema, then data)',
        recorder)
    self.assertEqual([r.jobId for r in result], ['job_empty', 'job_data'])

  def test_g25_insert_load_job__load_job_project_id(self):
    self.responder.on(
        'POST', r'/bigquery/v2/projects/other/jobs', rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_load_job(
            _table_ref(),
            'job_1',
            source_uris=['gs://b/a.json'],
            load_job_project_id='other'))
    self.golden(
        'insert_load_job__load_job_project_id',
        "perform_load_job with load_job_project_id='other' (job project != "
        'destination project)',
        recorder)
    self.assertEqual(
        message_to_dict(result), {
            'projectId': 'other', 'jobId': 'job_1', 'location': 'US'
        })

  def test_g26_insert_copy_job__basic(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w._insert_copy_job(
            'p',
            'job_1',
            _table_ref(table='src'),
            _table_ref(table='dst'),
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            job_labels={'step_name': 's'}))
    self.golden('insert_copy_job__basic', '_insert_copy_job', recorder)
    self.assertEqual(
        message_to_dict(result), {
            'projectId': 'p', 'jobId': 'job_1', 'location': 'US'
        })

  def test_g27_start_job__409_location_parse(self):
    self.responder.on(
        'POST',
        JOBS_PATH,
        rec.error(409, 'duplicate', 'Already Exists: Job p:EU.job_1'))
    result, recorder = self.capture(
        lambda w: w._insert_copy_job(
            'p', 'job_1', _table_ref(table='src'), _table_ref(table='dst')))
    self.golden(
        'start_job__409_location_parse',
        'jobs.insert 409 Already Exists: no retry; location parsed from the '
        'error message onto the returned reference',
        recorder)
    self.assertEqual(
        message_to_dict(result), {
            'projectId': 'p', 'jobId': 'job_1', 'location': 'EU'
        })

  def test_g28_perform_extract_job__avro(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_extract_job(['gs://b/o-*'],
                                        'job_1',
                                        _table_ref(),
                                        'AVRO',
                                        use_avro_logical_types=True,
                                        job_labels={'step_name': 's'}))
    self.golden(
        'perform_extract_job__avro',
        "perform_extract_job(['gs://b/o-*'], 'job_1', ref, 'AVRO', "
        'use_avro_logical_types=True)',
        recorder)
    self.assertEqual(result.jobId, 'job_1')

  def test_g28_perform_extract_job__json_gzip(self):
    self.responder.on(
        'POST', r'/bigquery/v2/projects/other/jobs', rec.job_echo())
    result, recorder = self.capture(
        lambda w: w.perform_extract_job(['gs://b/o-*.json.gz'],
                                        'job_1',
                                        _table_ref(),
                                        'NEWLINE_DELIMITED_JSON',
                                        project='other',
                                        include_header=False,
                                        compression='GZIP'))
    self.golden(
        'perform_extract_job__json_gzip',
        'perform_extract_job to JSON with GZIP, no header, job project '
        "'other'",
        recorder)
    self.assertEqual(result.projectId, 'other')

  def test_g29_start_query_job__standard(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo(state='RUNNING'))
    result, recorder = self.capture(
        lambda w: w._start_query_job(
            'p',
            'SELECT 1',
            False,
            False,
            'job_1',
            'BATCH',
            kms_key='projects/p/locations/l/keyRings/r/cryptoKeys/k',
            job_labels={'step_name': 's'}),
        temp_dataset_id='temp_ds')
    self.golden(
        'start_query_job__standard',
        '_start_query_job with temp_dataset_id, kms_key, labels and BATCH '
        'priority',
        recorder)
    self.assertIsInstance(result, bigquery.Job)
    self.assertEqual(
        message_to_dict(result.jobReference), {
            'projectId': 'p', 'jobId': 'job_1', 'location': 'US'
        })
    self.assertEqual(result.status.state, 'RUNNING')

  def test_g30_start_query_job__dry_run(self):
    self.responder.on('POST', JOBS_PATH, rec.job_echo())
    result, recorder = self.capture(
        lambda w: w._start_query_job(
            'p', 'SELECT 1', True, True, 'job_1', 'INTERACTIVE', dry_run=True))
    self.golden(
        'start_query_job__dry_run',
        '_start_query_job(dry_run=True): no destination table',
        recorder)
    self.assertEqual(result.jobReference.jobId, 'job_1')

  def test_g31_get_query_location__referenced_tables(self):
    stats = {
        'query': {
            'referencedTables': [
                {
                    'projectId': 'p', 'datasetId': 'd', 'tableId': 'a'
                },
                {
                    'projectId': 'p', 'datasetId': 'd', 'tableId': 'b'
                },
            ],
            'totalBytesProcessed': '0',
        },
        'totalBytesProcessed': '0',
    }
    self.responder.on('POST', JOBS_PATH, rec.job_echo(statistics=stats))
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/p/datasets/d/tables/a',
        rec.error(403, 'accessDenied'))
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/p/datasets/d/tables/b',
        CannedResponse(200, rec.table_resource('p', 'd', 'b', location='EU')))
    result, recorder = self.capture(
        lambda w: w.get_query_location('p', 'SELECT * FROM d.a, d.b', False))
    self.golden(
        'get_query_location__referenced_tables',
        'get_query_location: dry-run job (generated job id), then table GETs; '
        'a 403 table is skipped',
        recorder,
        generated_job_id=True)
    self.assertEqual(result, 'EU')

  def test_g32_get_job__with_location(self):
    job = rec.job_resource(
        job_reference={
            'projectId': 'p', 'jobId': 'job_1', 'location': 'EU'
        },
        configuration={
            'jobType': 'QUERY', 'query': {
                'query': 'SELECT 1'
            }
        })
    self.responder.on(
        'GET', r'/bigquery/v2/projects/p/jobs/job_1', CannedResponse(200, job))
    result, recorder = self.capture(lambda w: w.get_job('p', 'job_1', 'EU'))
    self.golden('get_job__with_location', "get_job('p','job_1','EU')", recorder)
    self.assertEqual(message_to_dict(result), job)

  def _job_status(self, state, error_result=None):
    return CannedResponse(
        200,
        rec.job_resource(
            job_reference={
                'projectId': 'p', 'jobId': 'job_1', 'location': 'US'
            },
            configuration={
                'jobType': 'LOAD',
                'load': {
                    'destinationTable': {
                        'projectId': 'p', 'datasetId': 'd', 'tableId': 't'
                    }
                }
            },
            state=state,
            error_result=error_result))

  def test_g33_wait_for_bq_job__running_then_done(self):
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/p/jobs/job_1',
        self._job_status('RUNNING'),
        self._job_status('RUNNING'),
        self._job_status('DONE'))
    ref = bigquery.JobReference(projectId='p', jobId='job_1', location='US')
    result, recorder = self.capture(
        lambda w: w.wait_for_bq_job(ref, sleep_duration_sec=5))
    self.golden(
        'wait_for_bq_job__running_then_done',
        'wait_for_bq_job polls jobs.get until DONE',
        recorder)
    self.assertIs(result, True)
    self.assertEqual(self.sleeps, [5, 5])

  def test_g34_wait_for_bq_job__done_with_error(self):
    self.responder.on(
        'GET',
        r'/bigquery/v2/projects/p/jobs/job_1',
        self._job_status(
            'DONE', error_result={
                'reason': 'invalid', 'message': 'boom'
            }))
    ref = bigquery.JobReference(projectId='p', jobId='job_1', location='US')
    result, recorder = self.capture(
        lambda w: w.wait_for_bq_job(ref), expect_exception=RuntimeError)
    self.golden(
        'wait_for_bq_job__done_with_error',
        'wait_for_bq_job on a job DONE with errorResult raises RuntimeError',
        recorder)
    self.assertRegex(str(result), r'BigQuery job job_1 failed\. Error Result:')
    self.assertIn('boom', str(result))

  # ---------------------------------------------------------------------------
  # Queries (G35-G36).

  def _script_query(self, pages, schema):
    self.responder.on('POST', JOBS_PATH, rec.job_echo(state='RUNNING'))
    self.responder.on(
        'GET', r'/bigquery/v2/projects/p/queries/' + HEX32, *pages)
    return schema

  def test_g35_run_query__paginated(self):
    schema = {'fields': [{'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}
    page1 = [{'f': [{'v': '1'}]}, {'f': [{'v': '2'}]}]
    page2 = [{'f': [{'v': '3'}]}]
    self._script_query([
        CannedResponse(200, rec.query_results(job_complete=False)),
        CannedResponse(
            200,
            rec.query_results(
                page1, schema, page_token='page_2_token', total_rows=3)),
        CannedResponse(200, rec.query_results(page2, schema, total_rows=3)),
    ],
                       schema)
    result, recorder = self.capture(
        lambda w: list(
            w.run_query('p', 'SELECT x FROM d.t', False, False, 'INTERACTIVE')),
        temp_dataset_id='temp_ds')
    self.golden(
        'run_query__paginated',
        'run_query: query job (generated id), getQueryResults jobComplete='
        'false retried, then two pages',
        recorder,
        generated_job_id=True)
    self.assertEqual([[message_to_dict(row) for row in rows]
                      for rows, _ in result], [page1, page2])
    self.assertEqual([message_to_dict(s) for _, s in result], [schema, schema])
    self.assertEqual(self.sleeps, [1.0])

  def test_g36_run_query__row_shape(self):
    schema = {
        'fields': [
            {
                'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'i', 'type': 'INTEGER', 'mode': 'NULLABLE'
            },
            {
                'name': 'f', 'type': 'FLOAT', 'mode': 'NULLABLE'
            },
            {
                'name': 'b', 'type': 'BOOLEAN', 'mode': 'NULLABLE'
            },
            {
                'name': 'n', 'type': 'NUMERIC', 'mode': 'NULLABLE'
            },
            {
                'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'
            },
            {
                'name': 'g', 'type': 'GEOGRAPHY', 'mode': 'NULLABLE'
            },
            {
                'name': 'by', 'type': 'BYTES', 'mode': 'NULLABLE'
            },
            {
                'name': 'd', 'type': 'DATE', 'mode': 'NULLABLE'
            },
            {
                'name': 'dt', 'type': 'DATETIME', 'mode': 'NULLABLE'
            },
            {
                'name': 'tm', 'type': 'TIME', 'mode': 'NULLABLE'
            },
            {
                'name': 'nul', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'rep', 'type': 'INTEGER', 'mode': 'REPEATED'
            },
            {
                'name': 'rec',
                'type': 'RECORD',
                'mode': 'NULLABLE',
                'fields': [
                    {
                        'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'
                    },
                    {
                        'name': 'y', 'type': 'STRING', 'mode': 'NULLABLE'
                    },
                ]
            },
            {
                'name': 'recs',
                'type': 'RECORD',
                'mode': 'REPEATED',
                'fields': [{
                    'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'
                }]
            },
        ]
    }
    row = {
        'f': [
            {
                'v': 'abc'
            },
            {
                'v': '42'
            },
            {
                'v': '1.5'
            },
            {
                'v': 'true'
            },
            {
                'v': '123.456'
            },
            {
                'v': '1.4781341765E9'
            },
            {
                'v': 'POINT(1 2)'
            },
            {
                'v': 'YWJj'
            },
            {
                'v': '2016-11-03'
            },
            {
                'v': '2016-11-03T00:49:36'
            },
            {
                'v': '00:49:36'
            },
            {
                'v': None
            },
            {
                'v': [{
                    'v': '1'
                }, {
                    'v': '2'
                }]
            },
            {
                'v': {
                    'f': [{
                        'v': '7'
                    }, {
                        'v': 'seven'
                    }]
                }
            },
            {
                'v': [{
                    'v': {
                        'f': [{
                            'v': '8'
                        }]
                    }
                }]
            },
        ]
    }
    self._script_query([CannedResponse(200, rec.query_results([row], schema))],
                       schema)

    def call(w):
      return [(rows, s, [w.convert_row_to_dict(r, s) for r in rows])
              for rows, s in w.run_query(
                  'p', 'SELECT * FROM d.t', False, False, 'INTERACTIVE')]

    result, recorder = self.capture(call, temp_dataset_id='temp_ds')
    self.golden(
        'run_query__row_shape',
        'run_query single page; row shape baseline (REST f/v rows through '
        'convert_row_to_dict)',
        recorder,
        generated_job_id=True)
    self.assertEqual(len(result), 1)
    rows, returned_schema, dicts = result[0]
    # F8: rows are apitools TableRow messages in the REST f/v shape.
    self.assertIsInstance(rows[0], bigquery.TableRow)
    self.assertIsInstance(returned_schema, bigquery.TableSchema)
    # A null cell ({'v': null}) comes back from apitools with 'v' unset.
    expected_row = {'f': [c if c['v'] is not None else {} for c in row['f']]}
    self.assertEqual(message_to_dict(rows[0]), expected_row)
    self.assertEqual(
        dicts,
        [{
            's': 'abc',
            'i': 42,
            'f': 1.5,
            'b': True,
            'n': decimal.Decimal('123.456'),
            'ts': '2016-11-03 00:49:36.500000 UTC',
            'g': 'POINT(1 2)',
            'by': 'YWJj',
            'd': '2016-11-03',
            'dt': '2016-11-03T00:49:36',
            'tm': '00:49:36',
            'nul': None,
            'rep': [1, 2],
            'rec': {
                'x': 7, 'y': 'seven'
            },
            'recs': [{
                'x': 8
            }],
        }])

  # ---------------------------------------------------------------------------
  # Streaming inserts (G37-G40).

  def test_g37_insert_rows__basic(self):
    self.responder.on(
        'POST', INSERT_ALL_PATH, CannedResponse(200, rec.insert_all_response()))
    result, recorder = self.capture(
        lambda w: w.insert_rows(
            'p',
            'd',
            't', [{
                'a': 1, 'b': 'x'
            }, {
                'a': 2, 'b': 'y'
            }],
            insert_ids=['id1', 'id2']))
    self.golden(
        'insert_rows__basic',
        "insert_rows('p','d','t', rows, insert_ids=[...])",
        recorder)
    self.assertEqual(result, (True, []))

  @unittest.skipIf(
      bigquery_tools is not None and
      bigquery_tools.fast_json_dumps is json.dumps,
      'orjson is not installed; datetime encoding differs (see report)')
  def test_g38_insert_rows__flags_and_encoding(self):
    self.responder.on(
        'POST', INSERT_ALL_PATH, CannedResponse(200, rec.insert_all_response()))
    rows = [{
        'dec': decimal.Decimal('1.23'),
        'dt': datetime.datetime(2020, 1, 2, 3, 4, 5, 600000),
        'd': datetime.date(2020, 1, 2),
        't': datetime.time(3, 4, 5),
        'b': b'Ynl0ZXM=',
        'nested': {
            'n': decimal.Decimal('10')
        },
        'none': None,
    }]
    result, recorder = self.capture(
        lambda w: w.insert_rows(
            'p',
            'd',
            't',
            rows,
            insert_ids=['id1'],
            skip_invalid_rows=True,
            ignore_unknown_values=True))
    self.golden(
        'insert_rows__flags_and_encoding',
        'insert_rows with skip_invalid_rows/ignore_unknown_values and '
        'Decimal/datetime/date/time/bytes values (orjson + default_encoder)',
        recorder)
    self.assertEqual(result, (True, []))

  def test_g39_insert_rows__domain_scoped_project(self):
    self.responder.on(
        'POST',
        r'/bigquery/v2/projects/google\.com:p/datasets/d/tables/t/insertAll',
        CannedResponse(200, rec.insert_all_response()))
    result, recorder = self.capture(
        lambda w: w.insert_rows(
            'google.com:p', 'd', 't', [{
                'a': 1
            }], insert_ids=['id1']))
    self.golden(
        'insert_rows__domain_scoped_project',
        "insert_rows('google.com:p', ...): path built by the cloud client",
        recorder)
    self.assertEqual(result, (True, []))

  def test_g40_insert_rows__row_errors(self):
    insert_errors = [{
        'index': 1,
        'errors': [{
            'reason': 'invalid',
            'location': 'a',
            'debugInfo': '',
            'message': 'no such field: a.'
        }]
    }]
    self.responder.on(
        'POST',
        INSERT_ALL_PATH,
        CannedResponse(200, rec.insert_all_response(insert_errors)))
    result, recorder = self.capture(
        lambda w: w.insert_rows(
            'p', 'd', 't', [{
                'a': 1
            }, {
                'a': 2
            }], insert_ids=['id1', 'id2']))
    self.golden(
        'insert_rows__row_errors',
        'insert_rows where the 200 response carries insertErrors',
        recorder)
    # Returned as-is from the response.
    self.assertEqual(result, (False, insert_errors))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
