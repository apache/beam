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

"""Retry, error and metrics characterization of BigQueryWrapper on master.

Phase 0 of the BigQuery client migration. The values asserted here were
observed on master@5d9ab78838a and then frozen ("observe, then freeze"). Each
assertion carries a class tag:

* INTENDED: deliberate behavior that later phases must preserve.
* INCIDENTAL: an artifact of the current implementation; later phases may
  change it (the classification report proposes the replacement contract).
* INFORMATIONAL: recorded for information only (decision P0-D1: HTTP attempt
  counts are not part of the contract).

F-numbers refer to the findings in the Phase 0 plan / classification report.

Control-plane cases C1-C8 run through the apitools transport; streaming cases
S1-S9 run through the google-cloud-bigquery transport (HTTP level) or, for
S6-S8, by replacing ``insert_rows_json`` on the wrapper's own client (mock
level). All run under ``no_sleep()``.
"""

# pytype: skip-file

import contextlib
import datetime
import json
import logging
import unittest
from unittest import mock

from apache_beam.io.gcp.tests import bigquery_http_recorder as rec
from apache_beam.io.gcp.tests.bigquery_http_recorder import HAS_GCP_DEPS
from apache_beam.io.gcp.tests.bigquery_http_recorder import CannedResponse
from apache_beam.io.gcp.tests.bigquery_http_recorder import Responder
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment

# pylint: disable=wrong-import-order, wrong-import-position
try:
  import requests
  from apitools.base.py.exceptions import BadStatusCodeError
  from apitools.base.py.exceptions import HttpBadRequestError
  from apitools.base.py.exceptions import HttpError
  from apitools.base.py.exceptions import HttpForbiddenError
  from google.api_core import exceptions as api_exceptions
  from google.api_core.retry import retry_base as api_retry_base
  from google.api_core.retry import retry_unary as api_retry_unary

  from apache_beam.io.gcp import bigquery_tools
  from apache_beam.io.gcp.bigquery_tools import RetryStrategy
  from apache_beam.io.gcp.internal.clients import bigquery
except ImportError:
  requests = None
  BadStatusCodeError = None
  HttpBadRequestError = None
  HttpError = None
  HttpForbiddenError = None
  api_exceptions = None
  api_retry_base = None
  api_retry_unary = None
  bigquery_tools = None
  RetryStrategy = None
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

TABLE_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t'
TABLES_PATH = r'/bigquery/v2/projects/p/datasets/d/tables'
DATASET_PATH = r'/bigquery/v2/projects/p/datasets/d'
JOBS_PATH = r'/bigquery/v2/projects/p/jobs'
INSERT_ALL_PATH = r'/bigquery/v2/projects/p/datasets/d/tables/t/insertAll'

# Capped scripts: CAP transient responses, then a non-retryable sentinel. If a
# layer is still retrying when the sentinel arrives, the cap was reached,
# meaning "this layer would have kept retrying" (plan §4.4).
CAP = 30


def _sentinel():
  return rec.error(400, 'invalid', 'capped-script sentinel')


class _BeamRetryCounter(logging.Handler):
  """Counts retries performed by apache_beam.utils.retry decorators."""
  def __init__(self):
    super().__init__(level=logging.DEBUG)
    self.retried = []

  def emit(self, record):
    message = record.getMessage()
    if message.startswith('Retry with exponential backoff'):
      # 'Retry ... before retrying <fn> because we caught exception: <exc>'
      fn = message.split(' before retrying ', 1)[1].split(' ', 1)[0]
      exc = message.split('because we caught exception: ',
                          1)[1].split(':', 1)[0].strip()
      self.retried.append((fn, exc.rsplit('.', 1)[-1]))


@contextlib.contextmanager
def _count_beam_retries():
  logger = logging.getLogger('apache_beam.utils.retry')
  handler = _BeamRetryCounter()
  old_level = logger.level
  logger.addHandler(handler)
  logger.setLevel(logging.DEBUG)
  try:
    yield handler.retried
  finally:
    logger.removeHandler(handler)
    logger.setLevel(old_level)


class _FakeApiCoreClock(object):
  """A clock for google-api-core retry modules only (``time`` replacement).

  ``sleep`` advances ``monotonic``, so DEFAULT_RETRY's 600 s deadline is
  reached without real waiting. Patched onto ``retry_unary.time`` and
  ``retry_base.time`` so nothing else sees it.
  """
  def __init__(self):
    self.now = 1000.0
    self.sleeps = []

  def monotonic(self):
    return self.now

  def sleep(self, seconds):
    self.sleeps.append(seconds)
    self.now += seconds

  def time(self):
    return self.now


def _request_count_metrics(table_id):
  """Returns {status: count} of API_REQUEST_COUNT for table ``table_id``."""
  result = {}
  infos = MetricsEnvironment.process_wide_container(
  ).to_runner_api_monitoring_infos(None).values()
  for info in infos:
    if info.urn != monitoring_infos.API_REQUEST_COUNT_URN:
      continue
    if info.labels.get(monitoring_infos.BIGQUERY_TABLE_LABEL) != table_id:
      continue
    count = monitoring_infos.extract_counter_value(info)
    if count:
      status = info.labels[monitoring_infos.STATUS_LABEL]
      result[status] = result.get(status, 0) + count
  return result


@unittest.skipIf(not HAS_GCP_DEPS, 'GCP dependencies are not installed')
class _CharacterizationBase(unittest.TestCase):
  def setUp(self):
    MetricsEnvironment.process_wide_container().reset()

  def run_wrapper(self, responder, call, mock_insert_rows_json=None):
    """Runs ``call(wrapper)``; returns an outcome dict (never raises)."""
    outcome = {'result': None, 'exception': None}
    with rec.no_sleep() as sleeps, _count_beam_retries() as beam_retries, \
        rec.recording_wrapper(responder) as (wrapper, recorder):
      if mock_insert_rows_json is not None:
        wrapper.gcp_bq_client.insert_rows_json = mock_insert_rows_json
      try:
        outcome['result'] = call(wrapper)
      except Exception as e:  # pylint: disable=broad-except
        outcome['exception'] = e
    outcome['http_attempts'] = len(recorder.requests)
    outcome['beam_retries'] = list(beam_retries)
    outcome['sleeps'] = list(sleeps)
    outcome['recorder'] = recorder
    self.assertEqual(responder.unmatched, [])
    return outcome


# -----------------------------------------------------------------------------
# 6.1 Control plane.


def _control_ops():
  ref = bigquery.TableReference(projectId='p', datasetId='d', tableId='t')
  return {
      # op: (method, path, success response, call, beam retry filter)
      'get_table': (
          'GET',
          TABLE_PATH,
          CannedResponse(200, rec.table_resource('p', 'd', 't')),
          lambda w: w.get_table('p', 'd', 't')),
      'insert_load_job': (
          'POST',
          JOBS_PATH,
          rec.job_echo(),
          lambda w: w.perform_load_job(ref, 'job_1', source_uris=['gs://b/o'])),
      'get_or_create_dataset': (
          'GET',
          DATASET_PATH,
          CannedResponse(200, rec.dataset_resource('p', 'd')),
          lambda w: w.get_or_create_dataset('p', 'd')),
      'delete_table': (
          'DELETE',
          TABLE_PATH,
          CannedResponse(204), lambda w: w._delete_table('p', 'd', 't')),
  }


class ControlPlaneCharacterizationTest(_CharacterizationBase):
  """C1-C8 over get_table, _insert_load_job, get_or_create_dataset and
  _delete_table (plan §6.1)."""
  def run_script(self, op, responses):
    method, path, _, call = _control_ops()[op]
    responder = Responder().on(method, path, *responses)
    return self.run_wrapper(responder, call)

  def assert_outcome(
      self, outcome, http_attempts, beam_retries, exc_type, status=None):
    # INFORMATIONAL (P0-D1): attempt counts per layer.
    self.assertEqual(outcome['http_attempts'], http_attempts)
    self.assertEqual(len(outcome['beam_retries']), beam_retries)
    if exc_type is None:
      self.assertIsNone(outcome['exception'])
    else:
      self.assertIs(type(outcome['exception']), exc_type)
      if status is not None:
        self.assertEqual(outcome['exception'].status_code, status)

  def test_c1_503_twice_then_ok(self):
    # F1: apitools absorbs transient 5xx inside http_wrapper.MakeRequest; the
    # Beam decorator never fires.
    for op, (_, _, ok, _) in _control_ops().items():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.error(503, 'backendError')] * 2 + [ok])
        # INTENDED: transient 5xx followed by success returns normally.
        self.assertIsNone(outcome['exception'])
        # INFORMATIONAL: 3 HTTP attempts, 0 Beam retries.
        self.assert_outcome(outcome, 3, 0, None)

  def test_c2_503_persistent(self):
    # F1 (corrected): apitools makes num_retries=5 attempts in total (not 1+5)
    # and each of the 1+MAX_RETRIES=4 Beam attempts repeats that, so a
    # persistent 503 costs 5 x 4 = 20 HTTP attempts, not 24.
    for op in _control_ops():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.error(503, 'backendError')] * CAP + [_sentinel()])
        # INFORMATIONAL: 20 attempts, 3 Beam retries; the cap is not reached.
        # INTENDED: an error carrying status 503 escapes after retries.
        # INCIDENTAL: its type is apitools' BadStatusCodeError (raised by
        # http_wrapper when its own retries run out), not a
        # status-specific HttpError subclass.
        self.assert_outcome(outcome, 20, 3, BadStatusCodeError, 503)

  def test_c3_429_persistent(self):
    # apitools retries 429 like 5xx. Only get_table's filter
    # (retry_on_server_errors_timeout_or_quota_issues_filter) retries 429 at
    # the Beam layer; the other methods use
    # retry_on_server_errors_and_timeout_filter, which does not.
    expected = {
        'get_table': (20, 3),
        'insert_load_job': (5, 0),
        'get_or_create_dataset': (5, 0),
        'delete_table': (5, 0),
    }
    for op, (http_attempts, beam_retries) in expected.items():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.error(429, 'rateLimitExceeded')] * CAP + [_sentinel()])
        # INTENDED: 429 is retried (by some layer), then an error with status
        # 429 escapes. INFORMATIONAL: which layer and how many attempts.
        # INCIDENTAL: the escaping type is BadStatusCodeError.
        self.assert_outcome(
            outcome, http_attempts, beam_retries, BadStatusCodeError, 429)

  def test_c4_403_rate_limit_exceeded(self):
    # apitools does not retry 403. Only get_table's quota filter retries a 403
    # whose errors[0].reason is rateLimitExceeded.
    expected = {
        'get_table': (4, 3),
        'insert_load_job': (1, 0),
        'get_or_create_dataset': (1, 0),
        'delete_table': (1, 0),
    }
    for op, (http_attempts, beam_retries) in expected.items():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.error(403, 'rateLimitExceeded')] * CAP + [_sentinel()])
        # INTENDED: get_table retries 403 rateLimitExceeded (explicit filter).
        # INCIDENTAL: the other methods do not (filter choice per method);
        # design §10 treats rate limiting as transient for all control-plane
        # calls. INFORMATIONAL: attempt counts.
        self.assert_outcome(
            outcome, http_attempts, beam_retries, HttpForbiddenError, 403)

  def test_c5_403_access_denied(self):
    for op in _control_ops():
      with self.subTest(op=op):
        outcome = self.run_script(op, [rec.error(403, 'accessDenied')])
        # INTENDED: not retried; HttpForbiddenError escapes.
        self.assert_outcome(outcome, 1, 0, HttpForbiddenError, 403)

  def test_c6_400_invalid(self):
    for op in _control_ops():
      with self.subTest(op=op):
        outcome = self.run_script(op, [rec.error(400, 'invalid')])
        # INTENDED: not retried; HttpBadRequestError escapes.
        self.assert_outcome(outcome, 1, 0, HttpBadRequestError, 400)

  def test_c7_408(self):
    for op in _control_ops():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.error(408, 'timeout')] * CAP + [_sentinel()])
        # INTENDED: 408 is retried by the Beam filter (apitools does not
        # retry it), then a plain HttpError(408) escapes.
        # INFORMATIONAL: 4 HTTP attempts, 3 Beam retries.
        self.assert_outcome(outcome, 4, 3, HttpError, 408)

  def test_c8_connection_error(self):
    for op in _control_ops():
      with self.subTest(op=op):
        outcome = self.run_script(
            op, [rec.raise_exception(ConnectionResetError('reset'))] * CAP +
            [_sentinel()])
        # apitools' HandleExceptionsAndRebuildHttpConnections retries
        # socket errors (5 attempts), then re-raises; the Beam filter retries
        # any non-HTTP exception.
        # INTENDED: connection errors are retried.
        # INCIDENTAL: the raw ConnectionResetError escapes after retries.
        # INFORMATIONAL: 5 x 4 = 20 HTTP attempts, 3 Beam retries.
        self.assert_outcome(outcome, 20, 3, ConnectionResetError)

  def test_c9_nested_decorators_multiply(self):
    # get_or_create_table (decorated) calls get_table (decorated), so a
    # persistent 503 on tables.get is retried at three layers:
    # apitools (5) x get_table (4) x get_or_create_table (4) = 80 attempts.
    responder = Responder(max_requests=1000).on(
        'GET', TABLE_PATH, *([rec.error(503, 'backendError')] * 200))
    outcome = self.run_wrapper(
        responder, lambda w: w.get_or_create_table(
            'p', 'd', 't', None, 'CREATE_IF_NEEDED', 'WRITE_APPEND'))
    # INFORMATIONAL: 80 HTTP attempts, 3 + 4 x 3 = 15 Beam retries.
    self.assertEqual(outcome['http_attempts'], 80)
    self.assertEqual(len(outcome['beam_retries']), 15)
    # INTENDED: the 503 escapes. INCIDENTAL: as BadStatusCodeError.
    self.assertIs(type(outcome['exception']), BadStatusCodeError)

  def test_c10_runtime_errors_are_retried(self):
    # get_or_create_table raises RuntimeError for disposition violations and
    # its retry filter (retry_if_valid_input_but_server_error_and_timeout_
    # filter) only excludes ValueError, so the RuntimeError is retried
    # MAX_RETRIES times (see also goldens G8 and G15).
    responder = Responder().on('GET', TABLE_PATH, rec.error(404, 'notFound'))
    outcome = self.run_wrapper(
        responder, lambda w: w.get_or_create_table(
            'p', 'd', 't', None, 'CREATE_NEVER', 'WRITE_APPEND'))
    # INTENDED: RuntimeError for CREATE_NEVER on a missing table.
    self.assertIs(type(outcome['exception']), RuntimeError)
    self.assertIn(
        'create disposition is CREATE_NEVER', str(outcome['exception']))
    # INCIDENTAL: a deterministic RuntimeError is retried 3 times.
    self.assertEqual(
        outcome['beam_retries'], [('get_or_create_table', 'RuntimeError')] * 3)
    # INFORMATIONAL.
    self.assertEqual(outcome['http_attempts'], 4)


# -----------------------------------------------------------------------------
# 6.2 Streaming inserts.


class StreamingInsertCharacterizationTest(_CharacterizationBase):
  """S1-S9 for insert_rows (plan §6.2)."""
  ROWS = [{'a': 1}, {'a': 2}]
  INSERT_IDS = ['id1', 'id2']

  def insert(self, responses=None, mock_insert_rows_json=None, rows=None):
    responder = Responder(max_requests=2000)
    if responses:
      responder.on('POST', INSERT_ALL_PATH, *responses)
    rows = rows if rows is not None else self.ROWS
    insert_ids = ['id%d' % (i + 1) for i in range(len(rows))]
    return self.run_wrapper(
        responder,
        lambda w: w.insert_rows('p', 'd', 't', rows, insert_ids=insert_ids),
        mock_insert_rows_json=mock_insert_rows_json)

  def assert_packaged(self, outcome, reason, status_line_message):
    self.assertIsNone(outcome['exception'])
    ok, errors = outcome['result']
    self.assertFalse(ok)
    # INTENDED: every row is packaged with the same error.
    self.assertEqual([e['index'] for e in errors], list(range(len(self.ROWS))))
    for entry in errors:
      self.assertEqual(len(entry['errors']), 1)
      # INTENDED (F9 / P0-D2): 'reason' is the HTTP reason phrase
      # (e.response.reason), not errors[0].reason.
      self.assertEqual(entry['errors'][0]['reason'], reason)
      self.assertEqual(set(entry['errors'][0]), {'message', 'reason'})
      # INCIDENTAL: 'message' is api_core's '<METHOD> <url>: <message>', so it
      # includes the transport's full URL.
      self.assertEqual(
          entry['errors'][0]['message'],
          'POST https://bigquery.googleapis.com/bigquery/v2/projects/p/'
          'datasets/d/tables/t/insertAll?prettyPrint=false: ' +
          status_line_message)
    return errors

  def test_s1_500_backend_error_twice_then_ok(self):
    outcome = self.insert([rec.error(500, 'backendError')] * 2 +
                          [CannedResponse(200, rec.insert_all_response())])
    # INTENDED: success after transient errors.
    self.assertEqual(outcome['result'], (True, []))
    # F2, INFORMATIONAL: google-cloud-bigquery DEFAULT_RETRY retries
    # internally (3 HTTP attempts); the Beam decorator is not involved.
    self.assertEqual(outcome['http_attempts'], 3)
    self.assertEqual(outcome['beam_retries'], [])
    # INCIDENTAL: metrics are recorded per wrapper attempt, so library-level
    # retries are invisible (one 'ok', no 'internal').
    self.assertEqual(_request_count_metrics('t'), {'ok': 1})

  def test_s2_500_backend_error_persistent_capped(self):
    outcome = self.insert([rec.error(500, 'backendError')] * CAP +
                          [_sentinel()])
    # F2, INFORMATIONAL: the library retried all CAP transient responses in
    # one wrapper attempt and only stopped at the sentinel, i.e. it would
    # have kept retrying until its 600 s deadline.
    self.assertEqual(outcome['http_attempts'], CAP + 1)
    self.assertEqual(outcome['beam_retries'], [])
    # The packaged sentinel is an artifact of the capped script.
    self.assert_packaged(outcome, 'Bad Request', 'capped-script sentinel')

  def test_s2b_500_backend_error_until_library_deadline(self):
    # Same as S2, but with a fake clock for google-api-core only, so the
    # DEFAULT_RETRY 600 s deadline is actually reached (no cap).
    clock = _FakeApiCoreClock()
    with mock.patch.object(api_retry_unary, 'time', clock), \
        mock.patch.object(api_retry_base, 'time', clock):
      outcome = self.insert([rec.error(500, 'backendError')])
    # F4, INCIDENTAL: api_core raises RetryError when its deadline is
    # exhausted. RetryError is not a GoogleAPICallError, so it escapes the
    # packaging 'except', is retried by the Beam decorator (MAX_RETRIES), and
    # is finally raised to the caller instead of being packaged.
    self.assertIs(type(outcome['exception']), api_exceptions.RetryError)
    self.assertEqual(
        outcome['beam_retries'], [('_insert_all_rows', 'RetryError')] * 3)
    # INCIDENTAL: no request-count metric at all (RetryError is not caught).
    self.assertEqual(_request_count_metrics('t'), {})
    # INFORMATIONAL: each wrapper attempt spent the full library deadline
    # (the attempt count per deadline depends on jitter).
    self.assertGreaterEqual(sum(clock.sleeps), 4 * 540)
    self.assertGreater(outcome['http_attempts'], 4 * 5)

  def test_s3_non_transient_http_errors_are_packaged(self):
    # status, errors[0].reason, packaged reason (HTTP phrase), metric status,
    # RetryStrategy.should_retry(RETRY_ON_TRANSIENT_ERROR, packaged reason).
    cases = [
        (400, 'invalid', 'Bad Request', 'out_of_range', False),
        (401, 'unauthorized', 'Unauthorized', 'unauthenticated', False),
        (403, 'accessDenied', 'Forbidden', 'permission_denied', False),
        (403, 'quotaExceeded', 'Forbidden', 'permission_denied', False),
        (404, 'notFound', 'Not Found', 'not_found', False),
        (501, 'notImplemented', 'Not Implemented', 'not_implemented', False),
        # 5xx/429 whose errors[0].reason is not in the library's retryable
        # reasons are not retried by the library either; their phrase is not
        # in RetryStrategy._NON_TRANSIENT_ERRORS, so they count as transient.
        (500, 'invalid', 'Internal Server Error', 'internal', True),
        (503, 'invalid', 'Service Unavailable', 'unavailable', True),
        (429, 'invalid', 'Too Many Requests', 'resource_exhausted', True),
    ]
    for status, reason, phrase, metric, transient in cases:
      with self.subTest(status=status, reason=reason):
        MetricsEnvironment.process_wide_container().reset()
        outcome = self.insert([rec.error(status, reason)] + [_sentinel()])
        # INFORMATIONAL: one HTTP attempt, no retries at any layer.
        self.assertEqual(outcome['http_attempts'], 1)
        self.assertEqual(outcome['beam_retries'], [])
        errors = self.assert_packaged(
            outcome, phrase, '%s: %s' % (phrase, reason))
        # INTENDED (F9): the end-to-end retry/DLQ decision per status code.
        self.assertEqual(
            RetryStrategy.should_retry(
                RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
                errors[0]['errors'][0]['reason']),
            transient)
        # INTENDED: one metric for the attempt, canonical status of e.code.
        self.assertEqual(_request_count_metrics('t'), {metric: 1})

  def test_s4_403_rate_limit_exceeded(self):
    outcome = self.insert([rec.error(403, 'rateLimitExceeded')] * 2 +
                          [CannedResponse(200, rec.insert_all_response())])
    # INTENDED: rate limiting is transient; the insert succeeds.
    self.assertEqual(outcome['result'], (True, []))
    # INFORMATIONAL: retried by the library ('rateLimitExceeded' is in
    # google.cloud.bigquery.retry._RETRYABLE_REASONS), not by Beam.
    self.assertEqual(outcome['http_attempts'], 3)
    self.assertEqual(outcome['beam_retries'], [])
    self.assertEqual(_request_count_metrics('t'), {'ok': 1})

  def test_s4b_403_rate_limit_exceeded_persistent_capped(self):
    outcome = self.insert([rec.error(403, 'rateLimitExceeded')] * CAP +
                          [_sentinel()])
    # INFORMATIONAL: the library keeps retrying (cap reached). A persistent
    # 403 rateLimitExceeded therefore ends as RetryError (S2b), never as a
    # packaged 'Forbidden' row error.
    self.assertEqual(outcome['http_attempts'], CAP + 1)
    self.assertEqual(outcome['beam_retries'], [])

  def test_s5_404_not_found(self):
    outcome = self.insert([rec.error(404, 'notFound')])
    # INTENDED: packaged with 'Not Found' (DLQ under RETRY_ON_TRANSIENT_ERROR).
    self.assert_packaged(outcome, 'Not Found', 'Not Found: notFound')
    # INTENDED: metric status 'not_found'.
    self.assertEqual(_request_count_metrics('t'), {'not_found': 1})

  def test_s6_response_less_errors_then_success(self):
    # Master's test_insert_rows_sets_metric_on_failure, through a wrapper
    # built like production (mock level).
    insert_rows_json = mock.Mock(
        side_effect=[
            api_exceptions.DeadlineExceeded('Deadline Exceeded'),
            api_exceptions.InternalServerError('Internal Error'),
            [],
        ])
    outcome = self.insert(mock_insert_rows_json=insert_rows_json)
    # INTENDED: the insert ends successfully.
    self.assertEqual(outcome['result'], (True, []))
    # F3, INCIDENTAL: the retries happen only because packaging reads
    # e.response.reason on exceptions that have no response, raising
    # AttributeError, which the Beam filter retries.
    self.assertEqual(insert_rows_json.call_count, 3)
    self.assertEqual(
        outcome['beam_retries'], [('_insert_all_rows', 'AttributeError')] * 2)
    self.assertIsNone(api_exceptions.DeadlineExceeded('x').response)
    # INTENDED: one metric per attempt (recorded before the AttributeError).
    self.assertEqual(
        _request_count_metrics('t'), {
            'deadline_exceeded': 1, 'internal': 1, 'ok': 1
        })

  def test_s7_response_less_error_persistent(self):
    insert_rows_json = mock.Mock(
        side_effect=api_exceptions.DeadlineExceeded('x'))
    outcome = self.insert(mock_insert_rows_json=insert_rows_json)
    # F3, INCIDENTAL: after 1 + MAX_RETRIES attempts the caller gets the
    # AttributeError from the packaging code, not packaged row errors and not
    # the DeadlineExceeded.
    self.assertIs(type(outcome['exception']), AttributeError)
    self.assertIn("no attribute 'reason'", str(outcome['exception']))
    self.assertEqual(insert_rows_json.call_count, 4)
    # INTENDED: one metric per attempt.
    self.assertEqual(_request_count_metrics('t'), {'deadline_exceeded': 4})

  def test_s8_retry_error(self):
    insert_rows_json = mock.Mock(
        side_effect=api_exceptions.RetryError(
            'Deadline of 600.0s exceeded',
            cause=api_exceptions.ServiceUnavailable('unavailable')))
    outcome = self.insert(mock_insert_rows_json=insert_rows_json)
    # F4, INCIDENTAL: RetryError escapes packaging, is retried by the Beam
    # decorator, then re-raised.
    self.assertIs(type(outcome['exception']), api_exceptions.RetryError)
    self.assertEqual(insert_rows_json.call_count, 4)
    self.assertEqual(
        outcome['beam_retries'], [('_insert_all_rows', 'RetryError')] * 3)
    # INCIDENTAL: no request-count metric is recorded.
    self.assertEqual(_request_count_metrics('t'), {})

  def test_s9_partial_insert_errors(self):
    insert_errors = [
        {
            'index': 0,
            'errors': [{
                'reason': 'invalid', 'message': 'bad value'
            }]
        },
        {
            'index': 2, 'errors': [{
                'reason': 'stopped', 'message': ''
            }]
        },
    ]
    outcome = self.insert(
        [CannedResponse(200, rec.insert_all_response(insert_errors))],
        rows=[{
            'a': 1
        }, {
            'a': 2
        }, {
            'a': 3
        }])
    # INTENDED: per-row errors are returned as-is with ok=False.
    self.assertEqual(outcome['result'], (False, insert_errors))
    self.assertEqual(outcome['http_attempts'], 1)
    # INCIDENTAL (bug): ServiceCallMetric.call() is given the error dict,
    # which it cannot convert, so the status label is the string 'None'
    # (one per failed row), not the row's reason.
    self.assertEqual(_request_count_metrics('t'), {'None': 2})

  def test_s10_connection_error_twice_then_ok(self):
    outcome = self.insert([
        rec.raise_exception(requests.exceptions.ConnectionError('reset')),
        rec.raise_exception(requests.exceptions.ConnectionError('reset')),
        CannedResponse(200, rec.insert_all_response()),
    ])
    # INTENDED: connection errors are transient.
    self.assertEqual(outcome['result'], (True, []))
    # INFORMATIONAL: retried by the library.
    self.assertEqual(outcome['http_attempts'], 3)
    self.assertEqual(outcome['beam_retries'], [])


@unittest.skipIf(not HAS_GCP_DEPS, 'GCP dependencies are not installed')
class RowEncodingCharacterizationTest(unittest.TestCase):
  def test_datetime_encoding_depends_on_orjson(self):
    # insert_rows serializes rows with orjson when available (a 'gcp' extra
    # dependency) and falls back to json + default_encoder otherwise. The two
    # disagree on datetime: default_encoder hits the datetime.date branch
    # first and uses str() (space separator), orjson emits ISO 8601 ('T').
    # INCIDENTAL; golden G38 pins the orjson form.
    value = datetime.datetime(2020, 1, 2, 3, 4, 5)
    self.assertEqual(
        bigquery_tools.default_encoder(value), '2020-01-02 03:04:05')
    self.assertEqual(
        json.loads(
            json.dumps({'v': value}, default=bigquery_tools.default_encoder)),
        {'v': '2020-01-02 03:04:05'})
    if bigquery_tools.fast_json_dumps is not json.dumps:
      self.assertEqual(
          bigquery_tools.fast_json_loads(
              bigquery_tools.fast_json_dumps(
                  {'v': value}, default=bigquery_tools.default_encoder)),
          {'v': '2020-01-02T03:04:05'})


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
