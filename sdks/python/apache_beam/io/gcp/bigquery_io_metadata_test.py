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

"""Tests for bigquery_io_metadata."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

from apache_beam.io.gcp import bigquery_io_metadata


class BigqueryIoMetadataTest(unittest.TestCase):
  def test_is_valid_cloud_label_value(self):
    # A dataflow job ID.
    # Lowercase letters, numbers, underscores and hyphens are allowed.
    test_str = '2020-06-29_15_26_09-12838749047888422749'
    self.assertTrue(bigquery_io_metadata._is_valid_cloud_label_value(test_str))

    # At least one character.
    test_str = '0'
    self.assertTrue(bigquery_io_metadata._is_valid_cloud_label_value(test_str))

    # Up to 63 characters.
    test_str = '0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij012'
    self.assertTrue(bigquery_io_metadata._is_valid_cloud_label_value(test_str))

    # Lowercase letters allowed
    test_str = 'abcdefghijklmnopqrstuvwxyz'
    for test_char in test_str:
      self.assertTrue(
          bigquery_io_metadata._is_valid_cloud_label_value(test_char))

    # Empty strings not allowed.
    test_str = ''
    self.assertFalse(bigquery_io_metadata._is_valid_cloud_label_value(test_str))

    # 64 or more characters not allowed.
    test_str = (
        '0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij0123')
    self.assertFalse(bigquery_io_metadata._is_valid_cloud_label_value(test_str))

    # Uppercase letters not allowed
    test_str = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    for test_char in test_str:
      self.assertFalse(
          bigquery_io_metadata._is_valid_cloud_label_value(test_char))

    # Special characters besides hyphens are not allowed
    test_str = '!@#$%^&*()+=[{]};:\'\"\\|,<.>?/`~'
    for test_char in test_str:
      self.assertFalse(
          bigquery_io_metadata._is_valid_cloud_label_value(test_char))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
