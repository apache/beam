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

"""Unit test for Bigquery verifier"""

import logging
import unittest

from hamcrest import assert_that as hc_assert_that
from mock import Mock, patch

from apache_beam.io.gcp.tests import bigquery_matcher as bq_verifier
from apache_beam.tests.test_utils import patch_retry

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import bigquery
  from google.cloud.exceptions import NotFound
except ImportError:
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(bigquery is None, 'Bigquery dependencies are not installed.')
class BigqueryMatcherTest(unittest.TestCase):

  def setUp(self):
    self._mock_result = Mock()
    patch_retry(self, bq_verifier)

  @patch.object(bigquery, 'Client')
  def test_bigquery_matcher_success(self, mock_bigquery):
    mock_query = Mock()
    mock_client = mock_bigquery.return_value
    mock_client.run_sync_query.return_value = mock_query
    mock_query.fetch_data.return_value = ([], None, None)

    matcher = bq_verifier.BigqueryMatcher(
        'mock_project',
        'mock_query',
        'da39a3ee5e6b4b0d3255bfef95601890afd80709')
    hc_assert_that(self._mock_result, matcher)

  @patch.object(bigquery, 'Client')
  def test_bigquery_matcher_query_run_error(self, mock_bigquery):
    mock_query = Mock()
    mock_client = mock_bigquery.return_value
    mock_client.run_sync_query.return_value = mock_query
    mock_query.run.side_effect = ValueError('job is already running')

    matcher = bq_verifier.BigqueryMatcher('mock_project',
                                          'mock_query',
                                          'mock_checksum')
    with self.assertRaises(ValueError):
      hc_assert_that(self._mock_result, matcher)
    self.assertTrue(mock_query.run.called)
    self.assertEqual(bq_verifier.MAX_RETRIES + 1, mock_query.run.call_count)

  @patch.object(bigquery, 'Client')
  def test_bigquery_matcher_fetch_data_error(self, mock_bigquery):
    mock_query = Mock()
    mock_client = mock_bigquery.return_value
    mock_client.run_sync_query.return_value = mock_query
    mock_query.fetch_data.side_effect = ValueError('query job not executed')

    matcher = bq_verifier.BigqueryMatcher('mock_project',
                                          'mock_query',
                                          'mock_checksum')
    with self.assertRaises(ValueError):
      hc_assert_that(self._mock_result, matcher)
    self.assertTrue(mock_query.fetch_data.called)
    self.assertEqual(bq_verifier.MAX_RETRIES + 1,
                     mock_query.fetch_data.call_count)

  @patch.object(bigquery, 'Client')
  def test_bigquery_matcher_query_responds_error_code(self, mock_bigquery):
    mock_query = Mock()
    mock_client = mock_bigquery.return_value
    mock_client.run_sync_query.return_value = mock_query
    mock_query.run.side_effect = NotFound('table is not found')

    matcher = bq_verifier.BigqueryMatcher('mock_project',
                                          'mock_query',
                                          'mock_checksum')
    with self.assertRaises(NotFound):
      hc_assert_that(self._mock_result, matcher)
    self.assertTrue(mock_query.run.called)
    self.assertEqual(bq_verifier.MAX_RETRIES + 1, mock_query.run.call_count)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
