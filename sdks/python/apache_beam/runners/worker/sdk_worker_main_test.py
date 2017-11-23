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
"""Tests for apache_beam.runners.worker.sdk_worker_main."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import httplib
import logging
import threading
import unittest

from apache_beam.runners.worker import sdk_worker_main


class SdkWorkerMainTest(unittest.TestCase):

  def test_status_server(self):
    status_server = sdk_worker_main.StatusServer()
    condition = threading.Condition()

    def callback():
      condition.acquire()
      # Notify the test thread to execute
      condition.notify_all()
      condition.release()

    thread = threading.Thread(
        target=functools.partial(
            status_server.start, started_callback=callback))
    thread.daemon = True
    thread.start()
    condition.acquire()
    # Wait for maximum 10 sec before the server is started.
    # Though the server should not take this long to start.
    condition.wait(10)
    condition.release()

    self.assertGreater(status_server.httpd.server_port, 0)

    # Wrapping the method to see if appears in threadump

    def wrapped_method_for_test():
      conn = httplib.HTTPConnection(
          host=status_server.httpd.server_name,
          port=status_server.httpd.server_port)
      conn.request("GET", "/")
      response = conn.getresponse()
      threaddump = response.read()
      self.assertRegexpMatches(threaddump, ".*wrapped_method_for_test.*")
      conn.close()

    wrapped_method_for_test()
    status_server.httpd.shutdown()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
