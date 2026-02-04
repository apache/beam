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

import logging
import unittest
from unittest.mock import patch

import pytest

from apache_beam.utils.logger import _LOG_COUNTER
from apache_beam.utils.logger import _LOG_TIMER
from apache_beam.utils.logger import log_every_n
from apache_beam.utils.logger import log_every_n_seconds
from apache_beam.utils.logger import log_first_n


@pytest.mark.no_xdist
class TestLogFirstN(unittest.TestCase):
  def setUp(self):
    _LOG_COUNTER.clear()
    _LOG_TIMER.clear()

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_first_n_once(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for _ in range(5):
      log_first_n(logging.INFO, "Test message %s", "arg", n=1)
    mock_logger.log.assert_called_once_with(
        logging.INFO, "Test message %s", "arg")

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_first_n_multiple(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for _ in range(5):
      log_first_n(logging.INFO, "Test message %s", "arg", n=3)
    self.assertEqual(mock_logger.log.call_count, 3)
    mock_logger.log.assert_called_with(logging.INFO, "Test message %s", "arg")

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_first_n_with_different_callers(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for _ in range(5):
      log_first_n(logging.INFO, "Test message", n=2)

    # call from another "caller" (another line)
    for _ in range(5):
      log_first_n(logging.INFO, "Test message", n=2)

    self.assertEqual(mock_logger.log.call_count, 4)

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_first_n_with_message_key(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    log_first_n(logging.INFO, "Test message", n=1, key="message")
    log_first_n(logging.INFO, "Test message", n=1, key="message")
    self.assertEqual(mock_logger.log.call_count, 1)

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_first_n_with_caller_and_message_key(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for message in ["Test message", "Another message"]:
      for _ in range(5):
        log_first_n(logging.INFO, message, n=1, key=("caller", "message"))
    self.assertEqual(mock_logger.log.call_count, 2)

  @patch('apache_beam.utils.logger.logging.getLogger')
  def test_log_every_n_multiple(self, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for _ in range(9):
      log_every_n(logging.INFO, "Test message", n=2)

    self.assertEqual(mock_logger.log.call_count, 5)

  @patch('apache_beam.utils.logger.logging.getLogger')
  @patch('apache_beam.utils.logger.time.time')
  def test_log_every_n_seconds_always(self, mock_time, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for i in range(3):
      mock_time.return_value = i
      log_every_n_seconds(logging.INFO, "Test message", n=0)
    self.assertEqual(mock_logger.log.call_count, 3)

  @patch('apache_beam.utils.logger.logging.getLogger')
  @patch('apache_beam.utils.logger.time.time')
  def test_log_every_n_seconds_multiple(self, mock_time, mock_get_logger):
    mock_logger = mock_get_logger.return_value
    for i in range(4):
      mock_time.return_value = i
      log_every_n_seconds(logging.INFO, "Test message", n=2)
    self.assertEqual(mock_logger.log.call_count, 2)


if __name__ == '__main__':
  unittest.main()
