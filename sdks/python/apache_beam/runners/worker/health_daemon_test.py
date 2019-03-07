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
import unittest
import threading
import time

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.worker.health_daemon import HealthDaemon

class MockHealthServer:
    def __init__(self, listening_port, http_status_code=200):
        self.listening_port = listening_port
        self.http_status_code = http_status_code
        self.httpd = None
        self.ready = False
        self.port = 0

    def start(self):
        http_status_code = self.http_status_code
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

        self.httpd = http.server.HTTPServer(('localhost', self.listening_port), HttpHandler)
        logging.info('Health HTTP server running at %s:%s', self.httpd.server_name,
                     self.httpd.server_port)
        self.ready = True
        self.port = self.httpd.server_port
        self.httpd.serve_forever()
        logging.info('Health HTTP server is now shutdown')

    def wait_until_ready(self):
        while not self.ready:
            time.sleep(0.1)
        logging.info('Health HTTP server is now ready')

    def shutdown(self):
        logging.info('Health HTTP server is shutting down')
        if self.httpd:
            self.httpd.shutdown()


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
        # Set up a local HTTP server to server fake successful responses.
        self.health_server = health_server = MockHealthServer(health_port,
                                                              http_status_code=http_status_code)
        self.health_thread = health_thread = threading.Thread(target=health_server.start)
        health_thread.daemon = True
        health_thread.setName('health-server-demon')
        health_thread.start()


        self.health_server.wait_until_ready()
        return self.health_server.port

    def test_ping_succeeds(self):
        health_port = self.start_mock_server(health_port=0, http_status_code=200)

        health_server_conn = HealthDaemon.connect_to_server(health_port)

        # Create a timeout for 5 seconds from now.
        timeout = time.time() + 5
        result = False
        while not result:
            result = HealthDaemon.try_health_ping(health_server_conn)
            time.sleep(0.5)
            if result or time.time() > timeout:
                break

        # This fails if we could not connect to the server after 5 seconds.
        self.assertTrue(result, 'Could not connect to server')

    def test_ping_fails(self):
        health_port = self.start_mock_server(health_port=0, http_status_code=501)

        health_server_conn = HealthDaemon.connect_to_server(health_port)

        # Create a timeout for 5 seconds from now.
        timeout = time.time() + 1
        result = False
        while not result:
            result = HealthDaemon.try_health_ping(health_server_conn)
            time.sleep(0.5)
            if result or time.time() > timeout:
                break
        self.assertFalse(result)

    def test_no_server_fails(self):
        health_port = 0

        health_logger = MockLoggingHandler()
        logging.getLogger().addHandler(health_logger)

        health_server_conn = HealthDaemon.connect_to_server(health_port)
        result = HealthDaemon.try_health_ping(health_server_conn)

        self.assertFalse(result)
        self.assertIn('Connection refused by server', health_logger.messages['error'])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()