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

"""Unittest for GCP testing utils."""

import logging
import unittest
from mock import Mock, patch

from apache_beam.io.gcp.tests import utils
from apache_beam.testing.test_utils import patch_retry

# Protect against environments where bigquery library is not available.
try:
  from google.cloud import bigquery
except ImportError:
  bigquery = None


@unittest.skipIf(bigquery is None, 'Bigquery dependencies are not installed.')
class UtilsTest(unittest.TestCase):

  def setUp(self):
    self._mock_result = Mock()
    patch_retry(self, utils)

  @patch('google.cloud.bigquery.Table.delete')
  @patch('google.cloud.bigquery.Table.exists', side_effect=[True, False])
  @patch('google.cloud.bigquery.Dataset.exists', return_value=True)
  def test_delete_bq_table_succeeds(self, *_):
    utils.delete_bq_table('unused_project',
                          'unused_dataset',
                          'unused_table')

  @patch('google.cloud.bigquery.Table.delete', side_effect=Exception)
  @patch('google.cloud.bigquery.Table.exists', return_value=True)
  @patch('google.cloud.bigquery.Dataset.exists', return_vaue=True)
  def test_delete_bq_table_fails_with_server_error(self, *_):
    with self.assertRaises(Exception):
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')

  @patch('google.cloud.bigquery.Table.delete')
  @patch('google.cloud.bigquery.Table.exists', return_value=[True, True])
  @patch('google.cloud.bigquery.Dataset.exists', return_vaue=True)
  def test_delete_bq_table_fails_with_delete_error(self, *_):
    with self.assertRaises(RuntimeError):
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
