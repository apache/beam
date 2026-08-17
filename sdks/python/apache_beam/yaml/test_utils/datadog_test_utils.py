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

"""Helper utilities for Datadog integration tests."""

import contextlib
import gzip
import http.server
import io
import json
import logging
import threading

_LOGGER = logging.getLogger(__name__)


class DatadogConnection:
  def __init__(self, url, api_key):
    self.url = url
    self.api_key = api_key


class MockDatadogHandler(http.server.BaseHTTPRequestHandler):
  def do_POST(self):
    if self.path == "/api/v2/logs":
      is_chunked = self.headers.get('Transfer-Encoding',
                                    '').lower() == 'chunked'
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
          data = json.loads(raw_data)
          with self.server.record_lock:
            if isinstance(data, list):
              self.server.received_records.extend(data)
            else:
              self.server.received_records.append(data)
      except Exception as e:
        logging.error("CRITICAL: Failure unpacking mock datadog payload: %s", e)

      self.send_response(200)
      self.send_header('Content-Type', 'application/json')
      self.end_headers()
      self.wfile.write(b'{"status": "ok"}')
    else:
      self.send_response(404)
      self.end_headers()

  def log_message(self, format, *args):
    pass


@contextlib.contextmanager
def temp_datadog_mock_server(received_records):
  server = http.server.ThreadingHTTPServer(('localhost', 0), MockDatadogHandler)
  server.received_records = received_records
  server.record_lock = threading.Lock()
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
def temp_fake_datadog_server(expected_records=None):
  """Context manager to provide a temporary fake Datadog server for testing.
  """
  received = []
  with temp_datadog_mock_server(received) as mock_url:
    try:
      yield DatadogConnection(
          url=mock_url,
          api_key="dummy_key_for_testing",
      )
    except Exception as err:
      logging.error(
          "Error interacting with temporary fake Datadog server: %s", err)
      raise err
    finally:
      if expected_records is not None:

        canonicalize = lambda rec: json.dumps(rec, sort_keys=True)

        actual_strs = sorted([canonicalize(r) for r in received])
        expected_strs = sorted([canonicalize(e) for e in expected_records])

        assert actual_strs == expected_strs, (
            f"Mismatch in recorded Datadog events!\n"
            f"Expected: {expected_strs}\n"
            f"Actual:   {actual_strs}"
        )
