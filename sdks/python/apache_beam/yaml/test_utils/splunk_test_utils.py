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

"""Helper utilities for Splunk integration tests."""

import contextlib
import gzip
import http.server
import io
import json
import logging
import threading

_LOGGER = logging.getLogger(__name__)

_HEC_PATH = "/services/collector/event"


class SplunkConnection:
  def __init__(self, url, token):
    self.url = url
    self.token = token


def _parse_hec_payload(payload):
  """Parses a HEC request body, which is a sequence of JSON objects that are
  concatenated rather than wrapped in a list."""
  decoder = json.JSONDecoder()
  records = []
  pos = 0
  while True:
    while pos < len(payload) and payload[pos].isspace():
      pos += 1
    if pos >= len(payload):
      return records
    record, pos = decoder.raw_decode(payload, pos)
    records.append(record)


class MockSplunkHandler(http.server.BaseHTTPRequestHandler):
  def do_POST(self):
    if self.path != _HEC_PATH:
      self.send_response(404)
      self.end_headers()
      return

    if self.headers.get('Authorization') != f"Splunk {self.server.token}":
      self.send_response(403)
      self.send_header('Content-Type', 'application/json')
      self.end_headers()
      self.wfile.write(b'{"text": "Invalid token", "code": 4}')
      return

    is_chunked = self.headers.get('Transfer-Encoding', '').lower() == 'chunked'
    is_gzip = self.headers.get('Content-Encoding', '').lower() == 'gzip'
    content_len = int(self.headers.get('Content-Length', 0))

    try:
      raw_data = b''
      if is_chunked:
        while True:
          line = self.rfile.readline().strip()
          if not line:
            break
          chunk_len = int(line, 16)
          if chunk_len == 0:
            self.rfile.readline()  # Clear trail
            break
          raw_data += self.rfile.read(chunk_len)
          self.rfile.readline()  # Clear trail
      elif content_len > 0:
        raw_data = self.rfile.read(content_len)

      if raw_data and is_gzip:
        with gzip.GzipFile(fileobj=io.BytesIO(raw_data)) as f:
          raw_data = f.read()

      if raw_data:
        records = _parse_hec_payload(raw_data.decode('utf-8'))
        with self.server.record_lock:
          self.server.received_records.extend(records)
    except Exception as e:
      logging.error("CRITICAL: Failure unpacking mock splunk payload: %s", e)

    self.send_response(200)
    self.send_header('Content-Type', 'application/json')
    self.end_headers()
    self.wfile.write(b'{"text": "Success", "code": 0}')

  def log_message(self, format, *args):
    pass


@contextlib.contextmanager
def temp_splunk_mock_server(received_records, token):
  server = http.server.ThreadingHTTPServer(('localhost', 0), MockSplunkHandler)
  server.received_records = received_records
  server.record_lock = threading.Lock()
  server.token = token
  ip, port = server.server_address
  thread = threading.Thread(target=server.serve_forever)
  thread.daemon = True
  thread.start()
  try:
    yield f"http://{ip}:{port}"
  finally:
    server.shutdown()
    server.server_close()
    thread.join()


@contextlib.contextmanager
def temp_fake_splunk_server(expected_records=None):
  """Context manager to provide a temporary fake Splunk HEC server for testing.
  """
  received = []
  token = "dummy_token_for_testing"
  with temp_splunk_mock_server(received, token) as mock_url:
    try:
      yield SplunkConnection(url=mock_url, token=token)
    except Exception as err:
      logging.error(
          "Error interacting with temporary fake Splunk server: %s", err)
      raise err
    finally:
      if expected_records is not None:

        canonicalize = lambda rec: json.dumps(rec, sort_keys=True)

        actual_strs = sorted([canonicalize(r) for r in received])
        expected_strs = sorted([canonicalize(e) for e in expected_records])

        assert actual_strs == expected_strs, (
            f"Mismatch in recorded Splunk events!\n"
            f"Expected: {expected_strs}\n"
            f"Actual:   {actual_strs}"
        )
