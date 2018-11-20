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

from __future__ import absolute_import

import logging
import unittest

import mock
from hamcrest import assert_that as hc_assert_that

from apache_beam.io.gcp.tests import bigquery_matcher as bq_verifier
from apache_beam.testing.test_utils import patch_retry

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  # TODO: fix usage
  from google.cloud import bigquery
  from google.cloud.exceptions import NotFound
except ImportError:
  bigquery = None
  NotFound = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(bigquery is None, 'Bigquery dependencies are not installed.')
@mock.patch.object(bigquery, 'Client')
class BigqueryMatcherTest(unittest.TestCase):

  def setUp(self):
    self._mock_result = mock.Mock()
    patch_retry(self, bq_verifier)

  def test_bigquery_matcher_success(self, mock_bigquery):
    mock_query_result = [mock.Mock(), mock.Mock(), mock.Mock()]
    mock_query_result[0].values.return_value = []
    mock_query_result[1].values.return_value = None
    mock_query_result[2].values.return_value = None

    mock_bigquery.return_value.query.return_value = mock_query_result

    matcher = bq_verifier.BigqueryMatcher(
        'mock_project',
        'mock_query',
        '59f9d6bdee30d67ea73b8aded121c3a0280f9cd8')
    hc_assert_that(self._mock_result, matcher)

  def test_bigquery_matcher_query_error_retry(self, mock_bigquery):
    mock_query = mock_bigquery.return_value.query
    mock_query.side_effect = NotFound('table not found')

    matcher = bq_verifier.BigqueryMatcher('mock_project',
                                          'mock_query',
                                          'mock_checksum')
    with self.assertRaises(NotFound):
      hc_assert_that(self._mock_result, matcher)
    self.assertEqual(bq_verifier.MAX_RETRIES + 1, mock_query.call_count)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
