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

import json
import logging
import unittest

from apache_beam.runners.worker import sdk_worker_main


class SdkWorkerMainTest(unittest.TestCase):

  def test_status_server(self):

    # Wrapping the method to see if it appears in threadump
    def wrapped_method_for_test():
      lines = sdk_worker_main.StatusServer.get_thread_dump()
      threaddump = '\n'.join(lines)
      self.assertRegexpMatches(threaddump, '.*wrapped_method_for_test.*')

    wrapped_method_for_test()

  def test_work_count_default_value(self):
    self._check_worker_count('{}', 12)

  def test_work_count_custom_value(self):
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads=1"]}}', 1)
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads=4"]}}', 4)
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads=12"]}}', 12)

  def test_work_count_wrong_format(self):
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads="]}}', exception=True)
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads=a"]}}', exception=True)
    self._check_worker_count(
        '{"options": {"experiments":["worker_threads=1a"]}}', exception=True)

  def _check_worker_count(self, pipeline_options, expected=0, exception=False):
    if exception:
      self.assertRaises(Exception, sdk_worker_main._get_worker_count,
                        json.loads(pipeline_options))
    else:
      self.assertEquals(
          sdk_worker_main._get_worker_count(json.loads(pipeline_options)),
          expected)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
