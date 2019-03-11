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

"""Bigquery data verifier for end-to-end test."""

from __future__ import absolute_import

import logging

from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.testing.test_utils import compute_hash
from apache_beam.utils import retry

__all__ = ['BigqueryMatcher']


# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import bigquery
  from google.cloud.exceptions import GoogleCloudError
except ImportError:
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

MAX_RETRIES = 4


def retry_on_http_and_value_error(exception):
  """Filter allowing retries on Bigquery errors and value error."""
  return isinstance(exception, (GoogleCloudError, ValueError))


class BigqueryMatcher(BaseMatcher):
  """Matcher that verifies the checksum of Bigquery data with given query.

  Fetch Bigquery data with given query, compute a hash string and compare
  with expected checksum.
  """

  def __init__(self, project, query, checksum):
    """Initialize BigQueryMatcher object.
    Args:
      project: The name (string) of the project.
      query: The query (string) to perform.
      checksum: SHA-1 hash generated from a sorted list of lines
        read from expected output.
    """
    if bigquery is None:
      raise ImportError(
          'Bigquery dependencies are not installed.')
    if not query or not isinstance(query, str):
      raise ValueError(
          'Invalid argument: query. Please use non-empty string')
    if not checksum or not isinstance(checksum, str):
      raise ValueError(
          'Invalid argument: checksum. Please use non-empty string')
    self.project = project
    self.query = query
    self.expected_checksum = checksum

  def _matches(self, _):
    logging.info('Start verify Bigquery data.')
    # Run query
    bigquery_client = bigquery.Client(project=self.project)
    response = self._query_with_retry(bigquery_client)
    logging.info('Read from given query (%s), total rows %d',
                 self.query, len(response))

    # Compute checksum
    self.checksum = compute_hash(response)
    logging.info('Generate checksum: %s', self.checksum)

    # Verify result
    return self.checksum == self.expected_checksum

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_http_and_value_error)
  def _query_with_retry(self, bigquery_client):
    """Run Bigquery query with retry if got error http response"""
    query_job = bigquery_client.query(self.query)
    return [row.values() for row in query_job]

  def describe_to(self, description):
    description \
      .append_text("Expected checksum is ") \
      .append_text(self.expected_checksum)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual checksum is ") \
      .append_text(self.checksum)


class BigqueryFullResultMatcher(BaseMatcher):
  """Matcher that verifies Bigquery data with given query.

  Fetch Bigquery data with given query, compare to the expected data.
  """

  def __init__(self, project, query, data):
    """Initialize BigQueryMatcher object.
    Args:
      project: The name (string) of the project.
      query: The query (string) to perform.
      data: List of tuples with the expected data.
    """
    if bigquery is None:
      raise ImportError(
          'Bigquery dependencies are not installed.')
    if not query or not isinstance(query, str):
      raise ValueError(
          'Invalid argument: query. Please use non-empty string')

    self.project = project
    self.query = query
    self.expected_data = [sorted(i) for i in data]

  def _matches(self, _):
    logging.info('Start verify Bigquery data.')
    # Run query
    bigquery_client = bigquery.Client(project=self.project)
    response = self._query_with_retry(bigquery_client)
    logging.info('Read from given query (%s), total rows %d',
                 self.query, len(response))

    self.actual_data = [sorted(i) for i in response]

    # Verify result
    return sorted(self.expected_data) == sorted(self.actual_data)

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_http_and_value_error)
  def _query_with_retry(self, bigquery_client):
    """Run Bigquery query with retry if got error http response"""
    query_job = bigquery_client.query(self.query)
    return [row.values() for row in query_job]

  def describe_to(self, description):
    description \
      .append_text("Expected data is ") \
      .append_text(self.expected_data)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual data is ") \
      .append_text(self.actual_data)
