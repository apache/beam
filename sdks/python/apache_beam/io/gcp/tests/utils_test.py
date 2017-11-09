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

from mock import Mock
from mock import patch

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

  @patch.object(bigquery, 'Client')
  def test_delete_table_succeeds(self, mock_client):
    mock_dataset = Mock()
    mock_client.return_value.dataset = mock_dataset
    mock_dataset.return_value.exists.return_value = True

    mock_table = Mock()
    mock_dataset.return_value.table = mock_table
    mock_table.return_value.exists.side_effect = [True, False]

    utils.delete_bq_table('unused_project',
                          'unused_dataset',
                          'unused_table')

  @patch.object(bigquery, 'Client')
  def test_delete_table_fails_dataset_not_exist(self, mock_client):
    mock_dataset = Mock()
    mock_client.return_value.dataset = mock_dataset
    mock_dataset.return_value.exists.return_value = False

    with self.assertRaises(Exception) as e:
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')
    self.assertTrue(
        e.exception.message.startswith('Failed to cleanup. Bigquery dataset '
                                       'unused_dataset doesn\'t exist'))

  @patch.object(bigquery, 'Client')
  def test_delete_table_fails_table_not_exist(self, mock_client):
    mock_dataset = Mock()
    mock_client.return_value.dataset = mock_dataset
    mock_dataset.return_value.exists.return_value = True

    mock_table = Mock()
    mock_dataset.return_value.table = mock_table
    mock_table.return_value.exists.return_value = False

    with self.assertRaises(Exception) as e:
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')
    self.assertTrue(
        e.exception.message.startswith('Failed to cleanup. Bigquery table '
                                       'unused_table doesn\'t exist'))

  @patch.object(bigquery, 'Client')
  def test_delete_table_fails_service_error(self, mock_client):
    mock_dataset = Mock()
    mock_client.return_value.dataset = mock_dataset
    mock_dataset.return_value.exists.return_value = True

    mock_table = Mock()
    mock_dataset.return_value.table = mock_table
    mock_table.return_value.exists.return_value = True

    with self.assertRaises(Exception) as e:
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')
    self.assertTrue(
        e.exception.message.startswith('Failed to cleanup. Bigquery table '
                                       'unused_table still exists'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
