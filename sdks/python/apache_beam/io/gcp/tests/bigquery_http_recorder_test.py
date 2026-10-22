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

"""Unit tests for the BigQuery HTTP recording harness."""

# pytype: skip-file

import hashlib
import io
import json
import os
import shutil
import socket
import tempfile
import time
import unittest
from unittest import mock

import apache_beam
from apache_beam.io.gcp.tests import bigquery_http_recorder as rec
from apache_beam.io.gcp.tests.bigquery_http_recorder import HAS_GCP_DEPS
from apache_beam.io.gcp.tests.bigquery_http_recorder import CannedResponse
from apache_beam.io.gcp.tests.bigquery_http_recorder import RecordedRequest
from apache_beam.io.gcp.tests.bigquery_http_recorder import Responder
from apache_beam.io.gcp.tests.bigquery_http_recorder import normalize

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py import transfer
  from google.cloud import bigquery as gcp_bigquery

  from apache_beam.io.gcp import bigquery_tools
  from apache_beam.io.gcp.internal.clients import bigquery
except ImportError:
  transfer = None
  gcp_bigquery = None
  bigquery_tools = None
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

BASE = 'https://bigquery.googleapis.com'


def _req(
    method='GET',
    url=BASE + '/bigquery/v2/projects/p',
    body=None,
    headers=None):
  if isinstance(body, (dict, list)):
    body = json.dumps(body).encode('utf-8')
    headers = dict(headers or {}, **{'content-type': 'application/json'})
  return RecordedRequest(method, url, dict(headers or {}), body)


class ResponderTest(unittest.TestCase):
  def test_route_matching_uses_method_and_full_neutral_path(self):
    responder = Responder().on(
        'GET',
        r'/bigquery/v2/projects/p/datasets/d',
        CannedResponse(200, {'r': 1})).on(
            'DELETE',
            r'/bigquery/v2/projects/p/datasets/d',
            CannedResponse(204))
    self.assertEqual(
        responder.respond(
            _req(
                'GET', BASE + '/bigquery/v2/projects/p/'
                'datasets/d?alt=json')).body, {'r': 1})
    self.assertEqual(
        responder.respond(
            _req(
                'DELETE',
                'http://other-host/bigquery/v2/projects/p/'
                'datasets/d')).status,
        204)
    # Prefix of a longer path does not match (fullmatch).
    with self.assertRaises(rec.UnmatchedRequestError):
      responder.respond(
          _req('GET', BASE + '/bigquery/v2/projects/p/datasets/d/tables/t'))

  def test_ordered_consumption_and_repeat_last(self):
    responder = Responder().on(
        'GET', r'.*', CannedResponse(500), CannedResponse(200))
    statuses = [responder.respond(_req()).status for _ in range(4)]
    self.assertEqual(statuses, [500, 200, 200, 200])
    self.assertEqual(responder.route_consumption(), [('GET', '.*', 4)])

  def test_once_falls_through_to_next_route(self):
    responder = Responder().on('GET', r'.*', CannedResponse(404)).once().on(
        'GET', r'.*', CannedResponse(200))
    statuses = [responder.respond(_req()).status for _ in range(3)]
    self.assertEqual(statuses, [404, 200, 200])

  def test_unmatched_request_fails_and_is_recorded(self):
    responder = Responder().on('GET', r'/x', CannedResponse(200))
    request = _req('POST')
    with self.assertRaises(rec.UnmatchedRequestError):
      responder.respond(request)
    self.assertEqual(responder.unmatched, [request])
    self.assertTrue(issubclass(rec.UnmatchedRequestError, AssertionError))

  def test_once_is_exhausted_then_unmatched(self):
    responder = Responder().on('GET', r'.*', CannedResponse(200)).once()
    responder.respond(_req())
    with self.assertRaises(rec.UnmatchedRequestError):
      responder.respond(_req())

  def test_callable_response_receives_request(self):
    responder = Responder().on(
        'POST', r'.*', lambda r: CannedResponse(200, r.json()))
    self.assertEqual(
        responder.respond(_req('POST', body={'a': 1})).body, {'a': 1})

  def test_raise_exception_response(self):
    responder = Responder().on(
        'GET', r'.*', rec.raise_exception(socket.error('boom')))
    with self.assertRaises(OSError):
      responder.respond(_req())

  def test_runaway_watchdog(self):
    responder = Responder(max_requests=3).on('GET', r'.*', CannedResponse(200))
    for _ in range(3):
      responder.respond(_req())
    with self.assertRaises(rec.RunawayRequestError):
      responder.respond(_req())

  def test_on_requires_a_response_and_once_requires_a_route(self):
    with self.assertRaises(ValueError):
      Responder().on('GET', r'.*')
    with self.assertRaises(ValueError):
      Responder().once()

  def test_error_builder(self):
    response = rec.error(403, 'accessDenied', 'Access Denied: x')
    self.assertEqual(response.status, 403)
    self.assertEqual(
        response.body,
        {
            'error': {
                'code': 403,
                'message': 'Access Denied: x',
                'status': 'PERMISSION_DENIED',
                'errors': [{
                    'reason': 'accessDenied',
                    'message': 'Access Denied: x',
                    'domain': 'global'
                }],
            }
        })

  def test_job_resource_echoes_request(self):
    request = _req(
        'POST',
        BASE + '/bigquery/v2/projects/p/jobs',
        body={
            'jobReference': {
                'projectId': 'p', 'jobId': 'j'
            },
            'configuration': {
                'query': {
                    'query': 'SELECT 1'
                }
            }
        })
    job = rec.job_resource(request, state='RUNNING')
    self.assertEqual(
        job['jobReference'], {
            'projectId': 'p', 'jobId': 'j', 'location': 'US'
        })
    self.assertEqual(job['configuration'], {'query': {'query': 'SELECT 1'}})
    self.assertEqual(job['status'], {'state': 'RUNNING'})
    self.assertEqual(job['id'], 'p:US.j')


class NormalizeTest(unittest.TestCase):
  def test_n1_strips_scheme_host_and_base_url(self):
    for url, path in [
        (BASE + '/bigquery/v2/projects/p/jobs', '/bigquery/v2/projects/p/jobs'),
        ('https://www.googleapis.com/bigquery/v2/projects/p/jobs',
         '/bigquery/v2/projects/p/jobs'),
        (BASE + '/upload/bigquery/v2/projects/p/jobs',
         '/upload/bigquery/v2/projects/p/jobs'),
        ('http://localhost:9050/prefix/bigquery/v2/projects/p',
         '/bigquery/v2/projects/p'),
        # Percent-encoding is preserved as sent.
        (BASE + '/bigquery/v2/projects/google.com%3Ap',
         '/bigquery/v2/projects/google.com%3Ap'),
    ]:
      with self.subTest(url=url):
        self.assertEqual(normalize(_req(url=url))['path'], path)

  def test_n2_query_sorted_and_transport_params_dropped(self):
    url = (
        BASE + '/bigquery/v2/projects/p/datasets?alt=json&prettyPrint=false'
        '&fields=a&%24.xgafv=2&filter=labels.a%3Ab+&all=true&z=1&z=2')
    query = normalize(_req(url=url))['query']
    self.assertEqual(
        query, {
            'all': 'true', 'filter': 'labels.a:b ', 'z': ['1', '2']
        })
    self.assertEqual(list(query), ['all', 'filter', 'z'])

  def test_n3_body_keys_sorted_recursively(self):
    body = normalize(
        _req('POST', body={
            'b': {
                'y': 1, 'x': [{
                    'd': 1, 'c': 2
                }]
            }, 'a': 0
        }))['body']
    self.assertEqual(
        json.dumps(body), '{"a": 0, "b": {"x": [{"c": 2, "d": 1}], "y": 1}}')

  def test_n4_null_keys_removed(self):
    body = normalize(
        _req(
            'POST',
            body={
                'a': None, 'b': {
                    'c': None, 'd': 1
                }, 'e': [None, {
                    'f': None
                }]
            }))['body']
    self.assertEqual(body, {'b': {'d': 1}, 'e': [None, {}]})

  def test_n5_temp_names_redacted(self):
    hex32 = 'abcdef0123456789abcdef0123456789'
    request = _req(
        'POST',
        BASE + '/bigquery/v2/projects/p/datasets/beam_temp_dataset_' + hex32,
        body={
            'tableId': 'beam_temp_table_' + hex32, 'jobId': hex32
        })
    normalized = normalize(request)
    self.assertEqual(
        normalized['path'],
        '/bigquery/v2/projects/p/datasets/beam_temp_dataset_<UUID>')
    # Standalone job IDs are only redacted for generated_job_id cases.
    self.assertEqual(
        normalized['body'], {
            'jobId': hex32, 'tableId': 'beam_temp_table_<UUID>'
        })
    self.assertEqual(
        normalize(request, generated_job_id=True)['body'], {
            'jobId': '<UUID>', 'tableId': 'beam_temp_table_<UUID>'
        })

  def test_n5_generated_job_id_in_path_and_query(self):
    hex32 = '0123456789abcdef0123456789abcdef'
    request = _req(
        url=BASE + '/bigquery/v2/projects/p/queries/' + hex32 +
        '?location=US&pageToken=' + hex32 + 'x')
    normalized = normalize(request, generated_job_id=True)
    self.assertEqual(
        normalized['path'], '/bigquery/v2/projects/p/queries/<UUID>')
    # Not standalone (followed by 'x'): kept.
    self.assertEqual(normalized['query']['pageToken'], hex32 + 'x')

  def test_n6_multipart_related(self):
    boundary = '===============123=='
    metadata = {'b': None, 'a': {'z': 1, 'y': 2}}
    body = (
        '--{b}\nContent-Type: application/json\nMIME-Version: 1.0\n\n{m}\n'
        '--{b}\nContent-Type: application/octet-stream\nMIME-Version: 1.0\n'
        'Content-Transfer-Encoding: binary\n\nsome,data\n--{b}--\n').format(
            b=boundary, m=json.dumps(metadata)).encode('utf-8')
    request = RecordedRequest(
        'POST',
        BASE + '/upload/bigquery/v2/projects/p/jobs?uploadType=multipart',
        {'content-type': "multipart/related; boundary='%s'" % boundary},
        body)
    self.assertEqual(
        normalize(request)['body'],
        {
            'metadata': {
                'a': {
                    'y': 2, 'z': 1
                }
            },
            'media': {
                'content_type': 'application/octet-stream',
                'length': 9,
                'sha256': hashlib.sha256(b'some,data').hexdigest(),
            },
        })
    self.assertEqual(request.json(), metadata)

  def test_n6_multipart_crlf_and_empty_media(self):
    body = (
        b'--B\r\nContent-Type: application/json\r\n\r\n{"a": 1}\r\n'
        b'--B\r\nContent-Type: application/octet-stream\r\n\r\n\r\n--B--\r\n')
    metadata, media = rec.parse_multipart_related(
        body, 'multipart/related; boundary="B"')
    self.assertEqual(metadata, {'a': 1})
    self.assertEqual(media['length'], 0)
    self.assertEqual(media['sha256'], hashlib.sha256(b'').hexdigest())

  def test_n7_headers_excluded(self):
    normalized = normalize(
        _req(headers={
            'authorization': 'Bearer x', 'user-agent': 'y'
        }))
    self.assertEqual(set(normalized), {'method', 'path', 'query', 'body'})
    self.assertNotIn('Bearer', json.dumps(normalized))

  def test_empty_and_non_json_bodies(self):
    self.assertIsNone(normalize(_req(body=b''))['body'])
    self.assertEqual(
        normalize(_req(body=b'not json'))['body'],
        {'non_json_body': 'not json'})


class GoldenIOTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    patcher = mock.patch.object(rec, 'GOLDEN_DIR', self.tmpdir)
    patcher.start()
    self.addCleanup(patcher.stop)
    self.addCleanup(shutil.rmtree, self.tmpdir)
    self.requests = [normalize(_req('POST', body={'b': 1, 'a': None}))]

  def _env(self, update):
    env = {rec.UPDATE_GOLDENS_ENV: '1' if update else ''}
    return mock.patch.dict(os.environ, env)

  def test_missing_golden_fails_outside_update_mode(self):
    with self._env(False), self.assertRaisesRegex(AssertionError, 'is missing'):
      rec.assert_matches_golden(self, 'case_x', 'desc', self.requests)

  def test_update_mode_writes_stable_file_and_is_idempotent(self):
    with self._env(True):
      rec.assert_matches_golden(self, 'case_x', 'desc', self.requests)
    path = os.path.join(self.tmpdir, 'case_x.json')
    with open(path) as f:
      first = f.read()
    self.assertEqual(
        json.loads(first),
        {
            'case': 'case_x',
            'description': 'desc',
            'captured_from': rec.DEFAULT_CAPTURED_FROM,
            'requests': self.requests,
        })
    self.assertTrue(first.endswith('}\n'))
    self.assertEqual(
        first, json.dumps(json.loads(first), indent=2, sort_keys=True) + '\n')
    mtime = os.stat(path).st_mtime_ns
    with self._env(True):
      rec.assert_matches_golden(self, 'case_x', 'desc', self.requests)
    with open(path) as f:
      self.assertEqual(f.read(), first)
    self.assertEqual(os.stat(path).st_mtime_ns, mtime)
    # And it now passes outside update mode.
    with self._env(False):
      rec.assert_matches_golden(self, 'case_x', 'desc', self.requests)

  def test_mismatch_prints_unified_diff(self):
    with self._env(True):
      rec.assert_matches_golden(self, 'case_x', 'desc', self.requests)
    other = [dict(self.requests[0], method='PUT')]
    with self._env(False), self.assertRaises(AssertionError) as cm:
      rec.assert_matches_golden(self, 'case_x', 'desc', other)
    self.assertIn('-    "method": "POST"', str(cm.exception))
    self.assertIn('+    "method": "PUT"', str(cm.exception))

  def test_invalid_case_id(self):
    with self.assertRaises(ValueError):
      rec.load_golden('../x')


class NoSleepTest(unittest.TestCase):
  def test_records_and_skips_sleeps(self):
    with rec.no_sleep() as sleeps:
      start = time.monotonic()
      time.sleep(100)
      time.sleep(0.5)
    self.assertLess(time.monotonic() - start, 5)
    self.assertEqual(sleeps, [100, 0.5])


@unittest.skipIf(not HAS_GCP_DEPS, 'GCP dependencies are not installed')
class TransportTest(unittest.TestCase):
  TABLE_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t'

  def _responder(self):
    return Responder().on(
        'GET',
        self.TABLE_PATH,
        CannedResponse(200, rec.table_resource('p', 'd', 't', num_rows=7)))

  def test_raw_apitools_call_round_trips(self):
    recorder = rec.Recorder()
    responder = self._responder()
    client = bigquery.BigqueryV2(
        http=rec.RecordingHttp(recorder, responder),
        credentials=rec._PassthroughCredentials(),
        response_encoding='utf8')
    table = client.tables.Get(
        bigquery.BigqueryTablesGetRequest(
            projectId='p', datasetId='d', tableId='t'))
    self.assertEqual(table.numRows, 7)
    self.assertEqual(len(recorder.requests), 1)
    self.assertEqual(recorder.requests[0].method, 'GET')

  def test_raw_cloud_client_call_round_trips(self):
    recorder = rec.Recorder()
    responder = self._responder()
    client = gcp_bigquery.Client(
        project='p',
        credentials=rec.AnonymousCredentials(),
        _http=rec.recording_session(recorder, responder))
    table = client.get_table('p.d.t')
    self.assertEqual(table.num_rows, 7)
    self.assertEqual(len(recorder.requests), 1)

  def test_tables_get_normalizes_identically_across_libraries(self):
    apitools_recorder = rec.Recorder()
    bigquery.BigqueryV2(
        http=rec.RecordingHttp(apitools_recorder, self._responder()),
        credentials=rec._PassthroughCredentials(),
        response_encoding='utf8').tables.Get(
            bigquery.BigqueryTablesGetRequest(
                projectId='p', datasetId='d', tableId='t'))
    cloud_recorder = rec.Recorder()
    gcp_bigquery.Client(
        project='p',
        credentials=rec.AnonymousCredentials(),
        _http=rec.recording_session(cloud_recorder,
                                    self._responder())).get_table('p.d.t')
    # The raw URLs differ (alt=json vs prettyPrint=false) ...
    self.assertNotEqual(
        apitools_recorder.requests[0].url, cloud_recorder.requests[0].url)
    # ... but the golden forms are identical.
    self.assertEqual(
        apitools_recorder.normalized(), cloud_recorder.normalized())
    self.assertEqual(
        cloud_recorder.normalized(),
        [{
            'method': 'GET',
            'path': '/bigquery/v2/projects/p/datasets/d/tables/t',
            'query': {},
            'body': None
        }])

  def test_adapter_reports_http_reason_phrase(self):
    recorder = rec.Recorder()
    responder = Responder().on('GET', r'.*', rec.error(403, 'accessDenied'))
    session = rec.recording_session(recorder, responder)
    response = session.get(BASE + '/bigquery/v2/projects/p')
    self.assertEqual(response.status_code, 403)
    self.assertEqual(response.reason, 'Forbidden')
    self.assertEqual(response.json()['error']['code'], 403)

  def test_recording_http_raises_scripted_exception(self):
    recorder = rec.Recorder()
    responder = Responder().on(
        'GET', r'.*', rec.raise_exception(ConnectionResetError('reset')))
    http = rec.RecordingHttp(recorder, responder)
    with self.assertRaises(ConnectionResetError):
      http.request(BASE + '/bigquery/v2/projects/p')
    self.assertEqual(len(recorder.requests), 1)

  def test_apitools_uses_multipart_for_bytesio_upload(self):
    # S1 spike: Upload.FromStream(BytesIO()) has no total size, and
    # jobs.insert supports simple multipart, so apitools picks the simple
    # (multipart/related) strategy, not a resumable upload.
    responder = Responder().on(
        'POST', r'/upload/bigquery/v2/projects/p/jobs', rec.job_echo())
    with rec.recording_wrapper(responder) as (wrapper, recorder):
      wrapper.perform_load_job(
          bigquery.TableReference(projectId='p', datasetId='d', tableId='t'),
          'job_1',
          source_stream=io.BytesIO())
    self.assertEqual(len(recorder.requests), 1)
    request = recorder.requests[0]
    self.assertIn('uploadType=multipart', request.url)
    self.assertTrue(
        request.headers['content-type'].startswith('multipart/related'))
    self.assertEqual(transfer.SIMPLE_UPLOAD, 'simple')


@unittest.skipIf(not HAS_GCP_DEPS, 'GCP dependencies are not installed')
class RecordingWrapperTest(unittest.TestCase):
  def test_wrapper_built_without_client_argument(self):
    responder = Responder()
    # google.cloud.bigquery.Client is replaced by a factory inside the block.
    client_cls = gcp_bigquery.Client
    with rec.recording_wrapper(responder) as (wrapper, _):
      # F5: without client= the two clients are distinct production objects.
      self.assertIsInstance(wrapper.client, bigquery.BigqueryV2)
      self.assertIsInstance(wrapper.gcp_bq_client, client_cls)
      self.assertIsNot(wrapper.client, wrapper.gcp_bq_client)
      self.assertEqual(wrapper.gcp_bq_client.project, rec.HARNESS_PROJECT)
      # Beam-generated temp dataset and random row-id prefix (F6).
      self.assertTrue(
          wrapper.temp_dataset_id.startswith(
              bigquery_tools.BigQueryWrapper.TEMP_DATASET))
      self.assertNotEqual(wrapper._row_id_prefix, '')

  def test_user_agent_on_both_transports(self):
    responder = Responder().on(
        'GET',
        r'/bigquery/v2/projects/p/datasets/d/tables/t',
        CannedResponse(200, rec.table_resource('p', 'd', 't'))).on(
            'POST',
            r'/bigquery/v2/projects/p/datasets/d/tables/t/insertAll',
            CannedResponse(200, rec.insert_all_response()))
    with rec.recording_wrapper(responder) as (wrapper, recorder):
      wrapper.get_table('p', 'd', 't')
      wrapper.insert_rows('p', 'd', 't', [{'a': 1}], insert_ids=['i'])
    expected = 'apache-beam-%s' % apache_beam.__version__
    apitools_request, cloud_request = recorder.requests
    self.assertIn('alt=json', apitools_request.url)
    self.assertEqual(apitools_request.headers['user-agent'], expected)
    self.assertIn('prettyPrint=false', cloud_request.url)
    self.assertTrue(
        cloud_request.headers['user-agent'].startswith(expected + ' '),
        cloud_request.headers['user-agent'])
    # N7: no credentials reach the recorded headers.
    for request in recorder.requests:
      self.assertNotIn('authorization', request.headers)

  def test_temp_dataset_id_passthrough(self):
    with rec.recording_wrapper(Responder(),
                               temp_dataset_id='user_ds') as (wrapper, _):
      self.assertEqual(wrapper.temp_dataset_id, 'user_ds')
      self.assertTrue(wrapper.is_user_configured_dataset())


class ImportWithoutGcpTest(unittest.TestCase):
  def test_module_imports_and_normalizes_without_gcp(self):
    # The pure-Python parts (responder, normalizer, golden I/O) never touch
    # GCP libraries; transports raise ImportError when they are missing.
    with mock.patch.object(rec, 'HAS_GCP_DEPS', False):
      with self.assertRaises(ImportError):
        rec.RecordingHttp(rec.Recorder(), Responder())
      self.assertEqual(normalize(_req())['path'], '/bigquery/v2/projects/p')


if __name__ == '__main__':
  unittest.main()
