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

"""HTTP recording harness for BigQuery client characterization tests.

This module records the HTTP requests that ``BigQueryWrapper`` sends and
serves scripted responses, without touching the network. It supports both
client libraries used by the wrapper:

* apitools ``BigqueryV2`` (``httplib2`` interface), via :class:`RecordingHttp`.
* ``google.cloud.bigquery.Client`` (``requests`` interface), via
  :class:`RecordingAdapter` / :func:`recording_session`.

Recorded requests are normalized into a library-neutral form (rules N1-N7, see
``goldens/bigquery/README.md``) and compared with golden files.

Typical use::

  responder = Responder().on(
      'GET', r'/bigquery/v2/projects/p/datasets/d/tables/t',
      CannedResponse(200, table_resource('p', 'd', 't')))
  with no_sleep(), recording_wrapper(responder) as (wrapper, recorder):
    wrapper.get_table('p', 'd', 't')
  assert_matches_golden(self, 'get_table__basic', 'desc', recorder)

The module imports cleanly without GCP dependencies; the transport shims and
``recording_wrapper`` then raise ``unittest.SkipTest``-friendly errors only
when used. Tests should guard with ``@unittest.skipIf(not HAS_GCP_DEPS, ...)``.

This is test-only code. It has no backwards compatibility guarantees.
"""

# pytype: skip-file

import contextlib
import dataclasses
import difflib
import hashlib
import http.client
import json
import os
import re
import threading
import time
import urllib.parse
from typing import Any
from typing import Callable
from typing import Mapping
from typing import Optional
from typing import Union
from unittest import mock

# Protect against environments where GCP dependencies are not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  import httplib2
  import requests
  import requests.adapters
  import requests.structures
  from apitools.base.py import encoding as apitools_encoding
  from apitools.base.py.exceptions import HttpError
  from google.auth.credentials import AnonymousCredentials
  from google.cloud import bigquery as gcp_bigquery

  from apache_beam.io.gcp import bigquery_tools
  HAS_GCP_DEPS = True
except ImportError:
  httplib2 = None
  requests = None
  apitools_encoding = None
  HttpError = None
  AnonymousCredentials = None
  gcp_bigquery = None
  bigquery_tools = None
  HAS_GCP_DEPS = False
# pylint: enable=wrong-import-order, wrong-import-position

__all__ = [
    'HAS_GCP_DEPS',
    'CannedResponse',
    'RecordedRequest',
    'Recorder',
    'Responder',
    'RecordingHttp',
    'RecordingAdapter',
    'UnmatchedRequestError',
    'RunawayRequestError',
    'recording_session',
    'recording_wrapper',
    'no_sleep',
    'normalize',
    'parse_multipart_related',
    'raise_exception',
    'job_resource',
    'table_resource',
    'dataset_resource',
    'dataset_list',
    'tabledata_list',
    'query_results',
    'insert_all_response',
    'error',
    'message_to_dict',
    'GOLDEN_DIR',
    'UPDATE_GOLDENS_ENV',
    'load_golden',
    'assert_matches_golden',
]

# Default project given to the google-cloud-bigquery client built inside
# recording_wrapper(). Only used when the wrapper does not pass one.
HARNESS_PROJECT = 'harness-project'

# Environment variable that switches golden tests into update mode.
UPDATE_GOLDENS_ENV = 'BEAM_UPDATE_BQ_GOLDENS'
# Optional override for the "captured_from" field written in update mode.
CAPTURED_FROM_ENV = 'BEAM_BQ_GOLDENS_CAPTURED_FROM'
DEFAULT_CAPTURED_FROM = (
    'master@5d9ab78838a (apitools control plane, '
    'google-cloud-bigquery insertAll)')

GOLDEN_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'goldens', 'bigquery')

# Placeholder used by redaction rule N5.
UUID_PLACEHOLDER = '<UUID>'

# N2: transport-only query parameters dropped from goldens.
_DROPPED_QUERY_PARAMS = frozenset(['alt', 'prettyPrint', 'fields', '$.xgafv'])

# N1: path prefixes that mark the start of the library-neutral path.
_PATH_MARKERS = ('/upload/bigquery/v2/', '/bigquery/v2/')

# N5: volatile identifiers.
_TEMP_NAME_RE = re.compile(r'beam_temp_(dataset|table)_[0-9a-f]{32}')
_STANDALONE_HEX32_RE = re.compile(
    r'(?<![0-9A-Za-z_])[0-9a-f]{32}(?![0-9A-Za-z_])')

# Canonical google.rpc status names used in REST error bodies.
_CANONICAL_STATUS = {
    400: 'INVALID_ARGUMENT',
    401: 'UNAUTHENTICATED',
    403: 'PERMISSION_DENIED',
    404: 'NOT_FOUND',
    408: 'DEADLINE_EXCEEDED',
    409: 'ALREADY_EXISTS',
    429: 'RESOURCE_EXHAUSTED',
    500: 'INTERNAL',
    501: 'NOT_IMPLEMENTED',
    502: 'UNAVAILABLE',
    503: 'UNAVAILABLE',
    504: 'DEADLINE_EXCEEDED',
}

# Fixed timestamps (ms since epoch, as strings) so response bodies are
# deterministic.
_FIXED_TIME_MS = '1700000000000'

# -----------------------------------------------------------------------------
# Core types.


@dataclasses.dataclass(frozen=True)
class RecordedRequest:
  """A request as sent by a client library to the transport."""
  method: str  # 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'
  url: str  # full URL as sent
  headers: Mapping[str, str]  # lower-cased keys
  body: Optional[bytes]

  @property
  def path(self) -> str:
    """The URL path (percent-encoding preserved), without the query."""
    return urllib.parse.urlsplit(self.url).path

  @property
  def neutral_path(self) -> str:
    """The URL path with scheme, host and base-URL prefix stripped (N1)."""
    return _neutral_path(self.path)

  def json(self) -> Any:
    """The decoded JSON body (or the multipart metadata part), else None."""
    content_type = self.headers.get('content-type', '')
    if content_type.startswith('multipart/related'):
      metadata, _ = parse_multipart_related(self.body or b'', content_type)
      return metadata
    if not self.body:
      return None
    return json.loads(self.body.decode('utf-8'))


@dataclasses.dataclass(frozen=True)
class CannedResponse:
  """A scripted HTTP response.

  ``body`` may be a JSON-serializable dict/list, raw bytes, or None (empty).
  """
  status: int
  body: Union[dict, list, bytes, None] = None
  headers: Mapping[str, str] = dataclasses.field(default_factory=dict)

  def encoded_body(self) -> bytes:
    if self.body is None:
      return b''
    if isinstance(self.body, bytes):
      return self.body
    return json.dumps(self.body).encode('utf-8')


ResponseSpec = Union[CannedResponse,
                     Callable[[RecordedRequest], CannedResponse]]


class UnmatchedRequestError(AssertionError):
  """Raised by the responder when no route matches a request."""


class RunawayRequestError(AssertionError):
  """Raised by the responder when a test sends more requests than allowed.

  This is the watchdog for retry loops that would otherwise spin (e.g. the
  google-cloud-bigquery DEFAULT_RETRY deadline is measured with
  time.monotonic(), which no_sleep() does not advance).
  """


def raise_exception(exc: BaseException) -> Callable[[RecordedRequest], Any]:
  """A response spec that makes the transport raise ``exc``.

  Use it to simulate connection-level failures (e.g. ``socket.error`` for
  httplib2, ``requests.exceptions.ConnectionError`` for requests).
  """
  def _raise(unused_request):
    raise exc

  _raise.raises = exc
  return _raise


class _Route(object):
  def __init__(self, method, path_regex, responses):
    self.method = method.upper()
    self.path_regex = path_regex
    self._pattern = re.compile(path_regex)
    self.responses = list(responses)
    self.repeat_last = True
    self.consumed = 0

  def matches(self, request: RecordedRequest) -> bool:
    return (
        request.method.upper() == self.method and
        self._pattern.fullmatch(request.neutral_path) is not None)

  @property
  def exhausted(self) -> bool:
    return not self.repeat_last and self.consumed >= len(self.responses)

  def next_response(self) -> ResponseSpec:
    index = min(self.consumed, len(self.responses) - 1)
    self.consumed += 1
    return self.responses[index]


class Responder(object):
  """Routes requests to scripted responses. Unmatched requests fail the test.

  Routes are tried in registration order; the first non-exhausted route whose
  method matches and whose ``path_regex`` fully matches the N1-normalized path
  (e.g. ``/bigquery/v2/projects/p/datasets/d``) is used. Responses for a
  route are consumed in order; the last one repeats unless ``once()`` is
  called right after ``on()``, in which case the route is exhausted after its
  responses are used and later matching routes are tried.

  A response may be a :class:`CannedResponse` or a callable taking the
  :class:`RecordedRequest` and returning a :class:`CannedResponse` (or raising,
  see :func:`raise_exception`).
  """
  def __init__(self, max_requests: int = 500):
    self._routes = []
    self._lock = threading.Lock()
    self.max_requests = max_requests
    self.request_count = 0
    self.unmatched = []

  def on(
      self, method: str, path_regex: str, *responses:
      ResponseSpec) -> 'Responder':
    if not responses:
      raise ValueError('At least one response is required.')
    self._routes.append(_Route(method, path_regex, responses))
    return self

  def once(self) -> 'Responder':
    """Makes the most recently added route non-repeating."""
    if not self._routes:
      raise ValueError('once() must follow on().')
    self._routes[-1].repeat_last = False
    return self

  def respond(self, request: RecordedRequest) -> CannedResponse:
    with self._lock:
      self.request_count += 1
      if self.request_count > self.max_requests:
        raise RunawayRequestError(
            'More than %d requests were sent; last: %s %s' %
            (self.max_requests, request.method, request.url))
      for route in self._routes:
        if route.exhausted or not route.matches(request):
          continue
        spec = route.next_response()
        break
      else:
        self.unmatched.append(request)
        raise UnmatchedRequestError(
            'No scripted response for %s %s (neutral path %r). Routes: %s' % (
                request.method,
                request.url,
                request.neutral_path, [(r.method, r.path_regex)
                                       for r in self._routes]))
    if callable(spec):
      return spec(request)
    return spec

  def route_consumption(self):
    """Returns [(method, path_regex, times_used)] for each route."""
    return [(r.method, r.path_regex, r.consumed) for r in self._routes]


class Recorder(object):
  """Collects every request that reaches a recording transport."""
  def __init__(self):
    self.requests = []
    self._lock = threading.Lock()

  def record(self, request: RecordedRequest) -> None:
    with self._lock:
      self.requests.append(request)

  def clear(self) -> None:
    with self._lock:
      del self.requests[:]

  def normalized(self, generated_job_id: bool = False) -> list:
    """Returns the golden (N1-N7) form of every recorded request."""
    return [
        normalize(r, generated_job_id=generated_job_id) for r in self.requests
    ]

  def count(
      self, method: Optional[str] = None, path_regex: Optional[str] = None):
    """Counts recorded requests, optionally filtered by method/path."""
    pattern = re.compile(path_regex) if path_regex else None
    return sum(
        1 for r in self.requests
        if (method is None or r.method.upper() == method.upper()) and
        (pattern is None or pattern.fullmatch(r.neutral_path)))


# -----------------------------------------------------------------------------
# Transport shims.


def _lower_headers(headers) -> dict:
  result = {}
  for key, value in (headers or {}).items():
    if isinstance(key, bytes):
      key = key.decode('latin-1')
    if isinstance(value, bytes):
      value = value.decode('latin-1')
    result[str(key).lower()] = str(value)
  return result


def _to_bytes(body) -> Optional[bytes]:
  if body is None:
    return None
  if isinstance(body, bytes):
    return body
  if isinstance(body, str):
    return body.encode('utf-8')
  if hasattr(body, 'read'):
    return body.read()
  return bytes(body)


def _require_gcp_deps():
  if not HAS_GCP_DEPS:
    raise ImportError(
        'bigquery_http_recorder transports require GCP dependencies '
        '(apitools, httplib2, requests, google-cloud-bigquery).')


class RecordingHttp(object):
  """An ``httplib2.Http`` stand-in used by the apitools client."""
  def __init__(self, recorder: Recorder, responder: Responder):
    _require_gcp_deps()
    self._recorder = recorder
    self._responder = responder
    # apitools probes (and may clear) this attribute; see
    # http_wrapper.RebuildHttpConnections / _MakeRequestNoRetry.
    self.connections = {}
    self.timeout = None

  def request(
      self,
      uri,
      method='GET',
      body=None,
      headers=None,
      redirections=5,
      connection_type=None):
    req = RecordedRequest(
        method=str(method).upper(),
        url=str(uri),
        headers=_lower_headers(headers),
        body=_to_bytes(body))
    self._recorder.record(req)
    resp = self._responder.respond(req)
    info = {'status': str(resp.status), 'content-type': 'application/json'}
    info.update(_lower_headers(resp.headers))
    return httplib2.Response(info), resp.encoded_body()


if requests is not None:
  _BaseAdapter = requests.adapters.BaseAdapter
else:
  _BaseAdapter = object


class RecordingAdapter(_BaseAdapter):
  """A ``requests`` transport adapter used by google-cloud-bigquery.

  The response ``reason`` is set to the standard HTTP/1.1 reason phrase for
  the status code (``http.client.responses``), which is what urllib3 reports
  for real BigQuery responses. google-api-core surfaces it as
  ``exc.response.reason`` and ``BigQueryWrapper._insert_all_rows`` packages it
  into row errors (finding F9).
  """
  def __init__(self, recorder: Recorder, responder: Responder):
    _require_gcp_deps()
    super().__init__()
    self._recorder = recorder
    self._responder = responder
    self.timeouts = []

  def send(
      self,
      request,
      stream=False,
      timeout=None,
      verify=True,
      cert=None,
      proxies=None):
    req = RecordedRequest(
        method=str(request.method).upper(),
        url=str(request.url),
        headers=_lower_headers(request.headers),
        body=_to_bytes(request.body))
    self._recorder.record(req)
    self.timeouts.append(timeout)
    resp = self._responder.respond(req)
    response = requests.Response()
    response.status_code = resp.status
    response.reason = http.client.responses.get(resp.status, '')
    headers = {'content-type': 'application/json; charset=UTF-8'}
    headers.update(_lower_headers(resp.headers))
    response.headers = requests.structures.CaseInsensitiveDict(headers)
    response._content = resp.encoded_body()
    response.encoding = 'utf-8'
    response.url = request.url
    response.request = request
    return response

  def close(self):
    pass


def recording_session(recorder: Recorder, responder: Responder):
  """Returns a ``requests.Session`` whose traffic goes to the recorder."""
  _require_gcp_deps()
  session = requests.Session()
  # google.cloud._http probes this attribute (set by AuthorizedSession).
  session.is_mtls = False
  adapter = RecordingAdapter(recorder, responder)
  session.mount('https://', adapter)
  session.mount('http://', adapter)
  return session


class _PassthroughCredentials(object):
  """Stands in for Beam's apitools credentials adapter.

  ``BaseApiClient`` calls ``credentials.authorize(http)``; returning the
  recording transport unchanged keeps auth out of the request stream (N7) and
  prevents apitools from probing for credentials on its own.
  """
  def authorize(self, http):
    return http


@contextlib.contextmanager
def recording_wrapper(
    responder: Responder,
    *,
    temp_dataset_id=None,
    temp_table_ref=None,
    project: str = HARNESS_PROJECT):
  """Builds a real ``BigQueryWrapper`` whose transports are recorded.

  The wrapper is constructed with no ``client`` argument (finding F5), so
  master's ``BigQueryWrapper._bigquery_client()`` factory runs unchanged
  (user agent header, ``response_encoding``...). Only the transports and the
  credentials are swapped:

  * ``bigquery_tools.get_new_http`` returns a :class:`RecordingHttp`.
  * ``bigquery_tools.auth.get_service_credentials`` returns pass-through
    credentials.
  * ``google.cloud.bigquery.Client`` is wrapped so that it receives a
    recording ``requests`` session, anonymous credentials, and a default
    project, while keeping the ``client_info`` passed by Beam.

  Yields:
    ``(wrapper, recorder)``. The patches stay active for the duration of the
    ``with`` block (clients created lazily inside it are recorded too).
  """
  _require_gcp_deps()
  recorder = Recorder()
  real_client_cls = gcp_bigquery.Client

  def client_factory(*args, **kwargs):
    kwargs.setdefault('project', project)
    kwargs['credentials'] = AnonymousCredentials()
    kwargs['_http'] = recording_session(recorder, responder)
    return real_client_cls(*args, **kwargs)

  with mock.patch.object(bigquery_tools, 'get_new_http',
                         lambda: RecordingHttp(recorder, responder)), \
       mock.patch.object(bigquery_tools.auth, 'get_service_credentials',
                         return_value=_PassthroughCredentials()), \
       mock.patch.object(bigquery_tools.gcp_bigquery, 'Client',
                         client_factory):
    wrapper = bigquery_tools.BigQueryWrapper(
        temp_dataset_id=temp_dataset_id, temp_table_ref=temp_table_ref)
    yield wrapper, recorder


@contextlib.contextmanager
def no_sleep():
  """Patches ``time.sleep`` so retry loops run instantly.

  Every layer that backs off goes through ``time.sleep`` looked up at call
  time: Beam's ``retry.Clock``, apitools ``http_wrapper``, google-api-core
  ``retry_unary``, and ``BigQueryWrapper`` (``wait_for_bq_job``,
  ``run_query``, the 150 s ``WRITE_TRUNCATE`` wait).

  Yields:
    The list of requested sleep durations, in call order.
  """
  sleeps = []
  with mock.patch.object(time, 'sleep', side_effect=sleeps.append):
    yield sleeps


# -----------------------------------------------------------------------------
# Response builders. Responses are complete REST resources (not the minimum
# apitools needs) so the same scripts can drive google-cloud-bigquery.


def _parse_ref(project, dataset=None, table=None):
  return {'projectId': project, 'datasetId': dataset, 'tableId': table}


def job_resource(
    request: Optional[RecordedRequest] = None,
    state: str = 'DONE',
    error_result: Optional[dict] = None,
    statistics: Optional[dict] = None,
    location: Optional[str] = 'US',
    job_reference: Optional[dict] = None,
    configuration: Optional[dict] = None,
    errors: Optional[list] = None) -> dict:
  """Builds a ``Job`` resource.

  When ``request`` is a ``jobs.insert`` request, ``configuration`` and
  ``jobReference`` are echoed from its body (as the real service does), with
  ``location`` filled in if the request did not set one.
  """
  body = request.json() if request is not None else None
  body = body or {}
  ref = dict(job_reference or body.get('jobReference') or {})
  if location and not ref.get('location'):
    ref['location'] = location
  config = configuration if configuration is not None else body.get(
      'configuration', {})
  status = {'state': state}
  if error_result is not None:
    status['errorResult'] = error_result
    status['errors'] = errors if errors is not None else [error_result]
  stats = {'creationTime': _FIXED_TIME_MS}
  if state in ('RUNNING', 'DONE'):
    stats['startTime'] = _FIXED_TIME_MS
  if state == 'DONE':
    stats['endTime'] = _FIXED_TIME_MS
  if statistics:
    stats.update(statistics)
  job_id = ref.get('jobId', 'job')
  project = ref.get('projectId', 'p')
  return {
      'kind': 'bigquery#job',
      'etag': 'etag',
      'id': '%s:%s.%s' % (project, ref.get('location', ''), job_id),
      'selfLink': 'https://bigquery.googleapis.com/bigquery/v2/projects/'
      '%s/jobs/%s' % (project, job_id),
      'user_email': 'harness@example.com',
      'jobReference': ref,
      'configuration': config,
      'status': status,
      'statistics': stats,
  }


def job_echo(**kwargs) -> Callable[[RecordedRequest], CannedResponse]:
  """A response spec returning ``job_resource(request, **kwargs)``."""
  def _echo(request):
    return CannedResponse(200, job_resource(request, **kwargs))

  return _echo


def table_resource(
    project: str,
    dataset: str,
    table: str,
    schema: Optional[dict] = None,
    num_rows: int = 0,
    location: str = 'US',
    **extra) -> dict:
  """Builds a ``Table`` resource. ``extra`` keys are REST field names."""
  resource = {
      'kind': 'bigquery#table',
      'etag': 'etag',
      'id': '%s:%s.%s' % (project, dataset, table),
      'selfLink': 'https://bigquery.googleapis.com/bigquery/v2/projects/'
      '%s/datasets/%s/tables/%s' % (project, dataset, table),
      'tableReference': _parse_ref(project, dataset, table),
      'numBytes': '0',
      'numLongTermBytes': '0',
      'numRows': str(num_rows),
      'creationTime': _FIXED_TIME_MS,
      'lastModifiedTime': _FIXED_TIME_MS,
      'type': 'TABLE',
      'location': location,
  }
  if schema is not None:
    resource['schema'] = schema
  resource.update(extra)
  return resource


def dataset_resource(
    project: str,
    dataset: str,
    location: str = 'US',
    labels: Optional[dict] = None,
    **extra) -> dict:
  """Builds a ``Dataset`` resource. ``extra`` keys are REST field names."""
  resource = {
      'kind': 'bigquery#dataset',
      'etag': 'etag',
      'id': '%s:%s' % (project, dataset),
      'selfLink': 'https://bigquery.googleapis.com/bigquery/v2/projects/'
      '%s/datasets/%s' % (project, dataset),
      'datasetReference': {
          'projectId': project, 'datasetId': dataset
      },
      'creationTime': _FIXED_TIME_MS,
      'lastModifiedTime': _FIXED_TIME_MS,
      'location': location,
      'type': 'DEFAULT',
  }
  if labels:
    resource['labels'] = dict(labels)
  resource.update(extra)
  return resource


def dataset_list(project: str, dataset_ids, labels=None) -> dict:
  """Builds a ``datasets.list`` response."""
  return {
      'kind': 'bigquery#datasetList',
      'etag': 'etag',
      'datasets': [{
          'kind': 'bigquery#dataset',
          'id': '%s:%s' % (project, d),
          'datasetReference': {
              'projectId': project, 'datasetId': d
          },
          'labels': dict(labels or {}),
          'location': 'US',
      } for d in dataset_ids],
  }


def tabledata_list(rows=(), total_rows: int = 0, page_token=None) -> dict:
  """Builds a ``tabledata.list`` response. ``rows`` are REST f/v rows."""
  resource = {
      'kind': 'bigquery#tableDataList',
      'etag': 'etag',
      'totalRows': str(total_rows),
      'rows': list(rows),
  }
  if page_token:
    resource['pageToken'] = page_token
  return resource


def query_results(
    rows=(),
    schema: Optional[dict] = None,
    job_complete: bool = True,
    page_token: Optional[str] = None,
    total_rows: Optional[int] = None,
    project: str = 'p',
    job_id: str = 'job',
    location: str = 'US') -> dict:
  """Builds a ``jobs.getQueryResults`` response."""
  resource = {
      'kind': 'bigquery#getQueryResultsResponse',
      'etag': 'etag',
      'jobReference': {
          'projectId': project, 'jobId': job_id, 'location': location
      },
      'jobComplete': job_complete,
  }
  if job_complete:
    resource['schema'] = schema or {'fields': []}
    resource['rows'] = list(rows)
    resource['totalRows'] = str(
        total_rows if total_rows is not None else len(rows))
    resource['totalBytesProcessed'] = '0'
    resource['cacheHit'] = False
  if page_token:
    resource['pageToken'] = page_token
  return resource


def insert_all_response(insert_errors=()) -> dict:
  """Builds a ``tabledata.insertAll`` response."""
  resource = {'kind': 'bigquery#tableDataInsertAllResponse'}
  if insert_errors:
    resource['insertErrors'] = list(insert_errors)
  return resource


def error(status: int, reason: str, message: str = None) -> CannedResponse:
  """Builds a standard Google API JSON error response."""
  message = message or '%s: %s' % (
      http.client.responses.get(status, ''), reason)
  return CannedResponse(
      status,
      {
          'error': {
              'code': status,
              'message': message,
              'status': _CANONICAL_STATUS.get(status, 'UNKNOWN'),
              'errors': [{
                  'reason': reason, 'message': message, 'domain': 'global'
              }],
          }
      })


def message_to_dict(message) -> Any:
  """``apitools.base.py.encoding.MessageToDict`` (None passes through)."""
  _require_gcp_deps()
  if message is None:
    return None
  return apitools_encoding.MessageToDict(message)


# -----------------------------------------------------------------------------
# Normalization (N1-N7).


def _neutral_path(path: str) -> str:
  """N1: keep the path from ``/bigquery/v2/`` or ``/upload/bigquery/v2/``."""
  for marker in _PATH_MARKERS:
    index = path.find(marker)
    if index >= 0:
      return path[index:]
  return path


def _normalize_query(query: str) -> dict:
  """N2: sorted dict; transport-only parameters dropped."""
  result = {}
  for key, value in urllib.parse.parse_qsl(query, keep_blank_values=True):
    if key in _DROPPED_QUERY_PARAMS:
      continue
    if key in result:
      existing = result[key]
      result[key] = (existing
                     if isinstance(existing, list) else [existing]) + [value]
    else:
      result[key] = value
  return {k: result[k] for k in sorted(result)}


def _strip_nulls_and_sort(value):
  """N3 + N4: drop keys whose value is null; sort keys recursively."""
  if isinstance(value, dict):
    return {
        k: _strip_nulls_and_sort(value[k])
        for k in sorted(value) if value[k] is not None
    }
  if isinstance(value, list):
    return [_strip_nulls_and_sort(v) for v in value]
  return value


def _redact_str(value: str, generated_job_id: bool) -> str:
  value = _TEMP_NAME_RE.sub(
      lambda m: 'beam_temp_%s_%s' % (m.group(1), UUID_PLACEHOLDER), value)
  if generated_job_id:
    value = _STANDALONE_HEX32_RE.sub(UUID_PLACEHOLDER, value)
  return value


def _redact(value, generated_job_id: bool):
  """N5: redact volatile identifiers in every string value (and dict key)."""
  if isinstance(value, dict):
    return {
        _redact_str(k, generated_job_id): _redact(v, generated_job_id)
        for k, v in value.items()
    }
  if isinstance(value, list):
    return [_redact(v, generated_job_id) for v in value]
  if isinstance(value, str):
    return _redact_str(value, generated_job_id)
  return value


def _boundary_from_content_type(content_type: str) -> str:
  match = re.search(r'boundary=("[^"]*"|\'[^\']*\'|[^;\s]+)', content_type)
  if not match:
    raise ValueError('No boundary in content type %r' % content_type)
  return match.group(1).strip('"\'')


def parse_multipart_related(body: bytes, content_type: str):
  """Parses a two-part ``multipart/related`` upload body.

  Returns:
    ``(metadata, media)`` where ``metadata`` is the decoded JSON of the first
    part and ``media`` is ``{'content_type', 'length', 'sha256'}`` for the
    second part (N6). Boundaries are random, so they are discarded.
  """
  boundary = _boundary_from_content_type(content_type).encode('ascii')
  delimiter = b'--' + boundary
  chunks = body.split(delimiter)
  # chunks[0] is the preamble, chunks[-1] is the closing '--' epilogue.
  parts = []
  for chunk in chunks[1:]:
    if chunk.startswith(b'--'):
      break
    # Each part starts with a line break after the delimiter and ends with the
    # line break that precedes the next delimiter.
    if chunk.startswith(b'\r\n'):
      chunk = chunk[2:]
    elif chunk.startswith(b'\n'):
      chunk = chunk[1:]
    if chunk.endswith(b'\r\n'):
      chunk = chunk[:-2]
    elif chunk.endswith(b'\n'):
      chunk = chunk[:-1]
    for separator in (b'\r\n\r\n', b'\n\n'):
      head, sep, payload = chunk.partition(separator)
      if sep:
        break
    else:
      head, payload = chunk, b''
    headers = {}
    for line in re.split(rb'\r?\n', head):
      if b':' in line:
        name, _, value = line.partition(b':')
        headers[name.strip().decode('latin-1').lower()] = (
            value.strip().decode('latin-1'))
    parts.append((headers, payload))
  if len(parts) != 2:
    raise ValueError('Expected 2 multipart parts, got %d' % len(parts))
  metadata = json.loads(parts[0][1].decode('utf-8'))
  media_headers, media_payload = parts[1]
  media = {
      'content_type': media_headers.get('content-type'),
      'length': len(media_payload),
      'sha256': hashlib.sha256(media_payload).hexdigest(),
  }
  return metadata, media


def _normalize_body(request: RecordedRequest, generated_job_id: bool):
  content_type = request.headers.get('content-type', '')
  if content_type.startswith('multipart/related'):
    metadata, media = parse_multipart_related(request.body or b'', content_type)
    return {
        'metadata': _redact(_strip_nulls_and_sort(metadata), generated_job_id),
        'media': media,
    }
  if not request.body:
    return None
  text = request.body.decode('utf-8')
  try:
    decoded = json.loads(text)
  except ValueError:
    return {'non_json_body': _redact_str(text, generated_job_id)}
  return _redact(_strip_nulls_and_sort(decoded), generated_job_id)


def normalize(request: RecordedRequest, generated_job_id: bool = False) -> dict:
  """Returns the golden form of a recorded request (rules N1-N7).

  N1  path from ``/bigquery/v2/`` or ``/upload/bigquery/v2/``; scheme and host
      dropped. Percent-encoding is preserved as sent.
  N2  query parsed into a sorted dict; ``alt``, ``prettyPrint``, ``fields``,
      ``$.xgafv`` dropped.
  N3  JSON body parsed, keys sorted recursively.
  N4  keys whose value is ``null`` removed.
  N5  ``beam_temp_(dataset|table)_<32 hex>`` -> ``beam_temp_\\1_<UUID>``; with
      ``generated_job_id`` also every standalone 32-hex token -> ``<UUID>``.
  N6  ``multipart/related`` bodies -> ``{'metadata': ..., 'media': {...}}``.
  N7  headers excluded.
  """
  split = urllib.parse.urlsplit(request.url)
  return {
      'method': request.method.upper(),
      'path': _redact_str(_neutral_path(split.path), generated_job_id),
      'query': _redact(_normalize_query(split.query), generated_job_id),
      'body': _normalize_body(request, generated_job_id),
  }


# -----------------------------------------------------------------------------
# Golden files.


def _golden_path(case_id: str) -> str:
  if not re.fullmatch(r'[A-Za-z0-9_]+', case_id):
    raise ValueError('Invalid golden case id %r' % case_id)
  return os.path.join(GOLDEN_DIR, case_id + '.json')


def _dump_json(value) -> str:
  return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + '\n'


def update_mode() -> bool:
  return os.environ.get(UPDATE_GOLDENS_ENV, '') not in ('', '0', 'false')


def load_golden(case_id: str) -> Optional[dict]:
  path = _golden_path(case_id)
  if not os.path.exists(path):
    return None
  with open(path, encoding='utf-8') as f:
    return json.load(f)


def _write_golden(case_id: str, description: str, requests_: list) -> None:
  existing = load_golden(case_id)
  captured_from = os.environ.get(CAPTURED_FROM_ENV) or (
      existing or {}).get('captured_from') or DEFAULT_CAPTURED_FROM
  content = {
      'case': case_id,
      'description': description,
      'captured_from': captured_from,
      'requests': requests_,
  }
  os.makedirs(GOLDEN_DIR, exist_ok=True)
  with open(_golden_path(case_id), 'w', encoding='utf-8') as f:
    f.write(_dump_json(content))


def assert_matches_golden(
    testcase,
    case_id: str,
    description: str,
    recorded: Union[Recorder, list],
    generated_job_id: bool = False) -> None:
  """Asserts that the recorded requests match ``goldens/bigquery/<case>.json``.

  With ``BEAM_UPDATE_BQ_GOLDENS=1`` the golden file is (re)written instead.
  A golden file is rewritten only when its requests or description change, so
  update mode is idempotent. A missing golden file fails outside update mode.
  """
  if isinstance(recorded, Recorder):
    actual = recorded.normalized(generated_job_id=generated_job_id)
  else:
    actual = list(recorded)
  # Round-trip through JSON so tuples/ints compare like the stored form.
  actual = json.loads(json.dumps(actual))
  golden = load_golden(case_id)
  if update_mode():
    if (golden is None or golden.get('requests') != actual or
        golden.get('description') != description or
        golden.get('case') != case_id):
      _write_golden(case_id, description, actual)
    return
  if golden is None:
    testcase.fail(
        'Golden file %s is missing. Generate it with %s=1 and review the '
        'result.' % (_golden_path(case_id), UPDATE_GOLDENS_ENV))
  expected = golden.get('requests')
  if expected != actual:
    diff = ''.join(
        difflib.unified_diff(
            _dump_json(expected).splitlines(True),
            _dump_json(actual).splitlines(True),
            fromfile='golden/%s.json' % case_id,
            tofile='actual'))
    testcase.fail(
        'Requests for %s do not match the golden file. Rerun with %s=1 to '
        'update (requires reviewer sign-off).\n%s' %
        (case_id, UPDATE_GOLDENS_ENV, diff))
