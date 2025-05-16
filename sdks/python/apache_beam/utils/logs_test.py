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

import uuid
import unittest

from apache_beam.utils import logs
from unittest import mock


class LogsTest(unittest.TestCase):
  def test_allow_infrequent_logging_always_allows_if_interval_is_zero(self):
    random_id = str(uuid.uuid4())
    self.assertTrue(
        logs.allow_infrequent_logging(min_interval_sec=0, message_id=random_id))
    self.assertTrue(
        logs.allow_infrequent_logging(min_interval_sec=0, message_id=random_id))

  def test_allow_infrequent_logging_allows_no_message_id(self):
    self.assertTrue(logs.allow_infrequent_logging(min_interval_sec=0))

  def test_allow_infrequent_logging_prohibit_sequential_logs_with_same_id(self):
    random_id = str(uuid.uuid4())
    with mock.patch("time.time_ns") as mock_time:
      mock_time.return_value = 0  # Initial time
      self.assertTrue(
          logs.allow_infrequent_logging(
              min_interval_sec=1000, message_id=random_id))
      mock_time.return_value = 1 * 1_000_000_000  # 1 second.
      self.assertFalse(
          logs.allow_infrequent_logging(
              min_interval_sec=1000, message_id=random_id))
      mock_time.return_value = 2000 * 1_000_000_000  # 2000 seconds.
      self.assertTrue(
          logs.allow_infrequent_logging(
              min_interval_sec=1000, message_id=random_id))

  def test_allow_log_once_allows_only_once(self):
    random_id = str(uuid.uuid4())
    self.assertTrue(logs.allow_log_once(message_id=random_id))
    self.assertFalse(logs.allow_log_once(message_id=random_id))


if __name__ == '__main__':
  unittest.main()
