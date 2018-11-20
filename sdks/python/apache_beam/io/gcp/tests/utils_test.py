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

from __future__ import absolute_import

import logging
import unittest

import mock

from apache_beam.io.gcp.tests import utils
from apache_beam.testing.test_utils import patch_retry

# Protect against environments where bigquery library is not available.
try:
  from google.cloud import bigquery
  from google.cloud.exceptions import NotFound
except ImportError:
  bigquery = None
  NotFound = None


@unittest.skipIf(bigquery is None, 'Bigquery dependencies are not installed.')
@mock.patch.object(bigquery, 'Client')
class UtilsTest(unittest.TestCase):

  def setUp(self):
    patch_retry(self, utils)

  @mock.patch.object(bigquery, 'Dataset')
  def test_create_bq_dataset(self, mock_dataset, mock_client):
    mock_client.dataset.return_value = 'dataset_ref'
    mock_dataset.return_value = 'dataset_obj'

    utils.create_bq_dataset('project', 'dataset_base_name')
    mock_client.return_value.create_dataset.assert_called_with('dataset_obj')

  def test_delete_bq_dataset(self, mock_client):
    utils.delete_bq_dataset('project', 'dataset_ref')
    mock_client.return_value.delete_dataset.assert_called_with(
        'dataset_ref', delete_contents=mock.ANY)

  def test_delete_table_succeeds(self, mock_client):
    mock_client.return_value.dataset.return_value.table.return_value = (
        'table_ref')

    utils.delete_bq_table('unused_project',
                          'unused_dataset',
                          'unused_table')
    mock_client.return_value.delete_table.assert_called_with('table_ref')

  def test_delete_table_fails_not_found(self, mock_client):
    mock_client.return_value.dataset.return_value.table.return_value = (
        'table_ref')
    mock_client.return_value.delete_table.side_effect = NotFound('test')

    with self.assertRaisesRegexp(Exception, r'does not exist:.*table_ref'):
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
