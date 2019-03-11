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
"""Tests for apache_beam.runners.worker.health_daemon."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import http.server
import logging
import threading
import time
import unittest
from builtins import object

from apache_beam.runners.worker.health_daemon import HealthDaemon


class MockHealthServer(object):
  """An object that sets up a HTTP server for test purposes"""

  def __init__(self, listening_port, http_status_code=200):
    self._http_status_code = http_status_code
    self._httpd = None
    self._ready = False
    self.port = listening_port

  def start(self):
    """Executes the serving loop for the health server"""
    http_status_code = self._http_status_code

    class HttpHandler(http.server.BaseHTTPRequestHandler):
      """HTTP handler for serving stacktraces of all threads."""

      def do_PUT(self):  # pylint: disable=invalid-name
        """Return all thread stacktraces information for GET request."""
        self.send_response(http_status_code)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()

      def do_GET(self):  # pylint: disable=invalid-name
        """Return all thread stacktraces information for GET request."""
        self.send_response(501)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()

      def log_message(self, f, *args):
        """Do not log any messages."""
        pass

    self._httpd = http.server.HTTPServer(('localhost', self.port),
                                         HttpHandler)
    logging.info('Health HTTP server running at %s:%s', self._httpd.server_name,
                 self._httpd.server_port)
    self._ready = True
    self.port = self._httpd.server_port
    self._httpd.serve_forever()
    logging.info('Health HTTP server is now shutdown')

  def wait_until_ready(self):
    """Waits until the HTTP server is ready to receive connections."""
    while not self._ready:
      time.sleep(0.1)
    logging.info('Health HTTP server is now ready')

  def shutdown(self):
    """Shuts down the HTTP server."""
    logging.info('Health HTTP server is shutting down')
    if self._httpd:
      self._httpd.shutdown()


class MockLoggingHandler(logging.Handler):
  """Mock logging handler to check for expected logs."""

  def __init__(self, *args, **kwargs):
    self.reset()
    logging.Handler.__init__(self, *args, **kwargs)

  def emit(self, record):
    self.messages[record.levelname.lower()].append(record.getMessage())

  def reset(self):
    self.messages = {
        'debug': [],
        'info': [],
        'warning': [],
        'error': [],
        'critical': [],
    }


class HealthDaemonTest(unittest.TestCase):
  """Tests the expected behavior the the HealthDaemon."""

  def setUp(self):
    self.health_server = None
    self.health_thread = None

    self.addCleanup(self.shutDown)

  def shutDown(self):
    if self.health_server:
      self.health_server.shutdown()
      self.health_server = None

    if self.health_thread:
      self.health_thread.join()
      self.health_thread = None

  def start_mock_server(self, health_port, http_status_code):
    """Starts the mock HTTP server on the given port returning the given
       http_status_code.

    Args:
      health_port(int): Binding port for the debug server.
        Default is 0 which means any free unsecured port
      http_status_code(int): HTTP Status Code to return for the PUT method.

    Returns:
      The port that the HTTP server is listening on.
    """
    # Set up a local HTTP server to server fake successful responses.
    self.health_server = health_server = MockHealthServer(
        health_port, http_status_code=http_status_code)
    self.health_thread = health_thread = threading.Thread(
        target=health_server.start)
    health_thread.daemon = True
    health_thread.setName('health-server-demon')
    health_thread.start()

    self.health_server.wait_until_ready()
    return self.health_server.port

  def test_ping_succeeds(self):
    """Tests that the HealthDaemon can successfully connect to a HealthServer
       and send a ping."""
    health_port = self.start_mock_server(health_port=0, http_status_code=200)

    # Connect to server and assert that the health ping succeeds.
    health_server_conn = HealthDaemon.connect_to_server(health_port)
    self.assertTrue(HealthDaemon.try_health_ping(health_server_conn))

  def test_ping_fails(self):
    """Tests that the HealthDaemon can handle a bad HTTP Status Code."""
    # Set up the mock server to return a 501 METHOD UNIMPLEMENTED.
    health_port = self.start_mock_server(health_port=0, http_status_code=501)

    # Connect to server and assert that the health ping fails.
    health_server_conn = HealthDaemon.connect_to_server(health_port)
    self.assertFalse(HealthDaemon.try_health_ping(health_server_conn))

  def test_no_server_fails(self):
    """Tests that the HealthDaemon does not crash when there is no server."""
    health_port = 0

    health_logger = MockLoggingHandler()
    logging.getLogger().addHandler(health_logger)

    health_server_conn = HealthDaemon.connect_to_server(health_port)
    result = HealthDaemon.try_health_ping(health_server_conn)

    self.assertFalse(result)
    self.assertIn('Connection refused by server',
                  health_logger.messages['error'])

  def test_health_daemon_recovers(self):
    """Tests that the HealthDaemon recovers with a flaky server."""
    health_port = 8080
    health_logger = MockLoggingHandler()
    logging.getLogger().addHandler(health_logger)

    # First, test a successful ping.
    health_port = self.start_mock_server(health_port=health_port,
                                         http_status_code=200)
    health_server_conn = HealthDaemon.connect_to_server(health_port)

    self.assertTrue(HealthDaemon.try_health_ping(health_server_conn))

    # Now, shut down the health server and assert that we fail.
    self.shutDown()
    self.assertFalse(HealthDaemon.try_health_ping(health_server_conn))

    # Using the same connection, the HealthDaemon should try to reconnect on its
    # own. Restart the server and assert that we make a final successful ping.
    health_port = self.start_mock_server(health_port=health_port,
                                         http_status_code=200)
    self.assertTrue(HealthDaemon.try_health_ping(health_server_conn))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
