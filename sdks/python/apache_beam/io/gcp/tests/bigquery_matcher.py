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

# pytype: skip-file

import concurrent
import logging
import time

from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.io.gcp import bigquery_tools
from apache_beam.testing.test_utils import compute_hash
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import equal_to
from apache_beam.utils import retry

__all__ = ['BigqueryMatcher', 'BigQueryTableMatcher']

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import bigquery
  from google.cloud.exceptions import GoogleCloudError
except ImportError:
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

MAX_RETRIES = 5

_LOGGER = logging.getLogger(__name__)


def retry_on_http_timeout_and_value_error(exception):
  """Filter allowing retries on Bigquery errors and value error."""
  return isinstance(
      exception,
      (GoogleCloudError, ValueError, concurrent.futures.TimeoutError))


class BigqueryMatcher(BaseMatcher):
  """Matcher that verifies the checksum of Bigquery data with given query.

  Fetch Bigquery data with given query, compute a hash string and compare
  with expected checksum.
  """
  def __init__(self, project, query, checksum, timeout_secs=0):
    """Initialize BigQueryMatcher object.
    Args:
      project: The name (string) of the project.
      query: The query (string) to perform.
      checksum: SHA-1 hash generated from a sorted list of lines
        read from expected output.
      timeout_secs: Duration to retry query until checksum matches. This
        is useful for DF streaming pipelines or BQ streaming inserts. The
        default (0) never retries.
    """
    if bigquery is None:
      raise ImportError('Bigquery dependencies are not installed.')
    if not query or not isinstance(query, str):
      raise ValueError('Invalid argument: query. Please use non-empty string')
    if not checksum or not isinstance(checksum, str):
      raise ValueError(
          'Invalid argument: checksum. Please use non-empty string')
    self.project = project
    self.query = query
    self.expected_checksum = checksum
    self.checksum = None
    self.timeout_secs = timeout_secs

  def _matches(self, _):
    @retry.with_exponential_backoff(
        num_retries=1000,
        initial_delay_secs=0.5,
        max_delay_secs=30,
        stop_after_secs=self.timeout_secs,
    )
    def get_checksum():
      response = self._query_with_retry()
      _LOGGER.info(
          'Read from given query (%s), total rows %d',
          self.query,
          len(response))
      self.checksum = compute_hash(response)
      _LOGGER.info('Generate checksum: %s', self.checksum)
      if self.checksum != self.expected_checksum:
        # This exception is never raised beyond the enclosing method.
        raise ValueError(
            'Checksums do not match. Expected: %s, got: %s' %
            (self.expected_checksum, self.checksum))

    if self.checksum is None:
      try:
        get_checksum()
      except ValueError:
        pass

    return self.checksum == self.expected_checksum

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_http_timeout_and_value_error)
  def _query_with_retry(self):
    """Run Bigquery query with retry if got error http response"""
    _LOGGER.info('Attempting to perform query %s to BQ', self.query)
    # Create client here since it throws an exception if pickled.
    bigquery_client = bigquery.Client(self.project)
    query_job = bigquery_client.query(self.query)
    rows = query_job.result(timeout=60)
    return [row.values() for row in rows]

  def describe_to(self, description):
    description \
      .append_text("Expected checksum is ") \
      .append_text(self.expected_checksum)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual checksum is ") \
      .append_text(self.checksum)


class BigqueryFullResultMatcher(BigqueryMatcher):
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
    super().__init__(project, query, 'unused_checksum')
    self.expected_data = data
    self.actual_data = None

  def _matches(self, _):
    if self.actual_data is None:
      self.actual_data = self._get_query_result()
      _LOGGER.info('Result of query is: %r', self.actual_data)

    try:
      equal_to(self.expected_data)(self.actual_data)
      return True
    except BeamAssertException:
      return False

  def _get_query_result(self):
    return self._query_with_retry()

  def describe_to(self, description):
    description \
      .append_text("Expected data is ") \
      .append_text(self.expected_data)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual data is ") \
      .append_text(self.actual_data)


class BigqueryFullResultStreamingMatcher(BigqueryFullResultMatcher):
  """
  Matcher that verifies Bigquery data with given query.

  Fetch Bigquery data with given query, compare to the expected data.
  This matcher polls BigQuery until the no. of records in BigQuery is
  equal to the no. of records in expected data.
  A timeout can be specified.
  """

  DEFAULT_TIMEOUT = 5 * 60

  def __init__(self, project, query, data, timeout=DEFAULT_TIMEOUT):
    super().__init__(project, query, data)
    self.timeout = timeout

  def _get_query_result(self):
    start_time = time.time()
    while time.time() - start_time <= self.timeout:
      response = self._query_with_retry()
      if len(response) >= len(self.expected_data):
        return response
      _LOGGER.debug('Query result contains %d rows' % len(response))
      time.sleep(1)
    raise TimeoutError('Timeout exceeded for matcher.')  # noqa: F821


class BigQueryTableMatcher(BaseMatcher):
  """Matcher that verifies the properties of a Table in BigQuery."""
  def __init__(self, project, dataset, table, expected_properties):
    if bigquery is None:
      raise ImportError('Bigquery dependencies are not installed.')

    self.project = project
    self.dataset = dataset
    self.table = table
    self.expected_properties = expected_properties

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_http_timeout_and_value_error)
  def _get_table_with_retry(self, bigquery_wrapper):
    return bigquery_wrapper.get_table(self.project, self.dataset, self.table)

  def _matches(self, _):
    _LOGGER.info('Start verify Bigquery table properties.')
    # Run query
    bigquery_wrapper = bigquery_tools.BigQueryWrapper()

    self.actual_table = self._get_table_with_retry(bigquery_wrapper)

    _LOGGER.info('Table proto is %s', self.actual_table)

    return all(
        self._match_property(v, self._get_or_none(self.actual_table, k))
        for k, v in self.expected_properties.items())

  @staticmethod
  def _get_or_none(obj, attr):
    try:
      return obj.__getattribute__(attr)
    except AttributeError:
      try:
        return obj.get(attr, None)
      except TypeError:
        return None

  @staticmethod
  def _match_property(expected, actual):
    _LOGGER.info("Matching %s to %s", expected, actual)
    if isinstance(expected, dict):
      return all(
          BigQueryTableMatcher._match_property(
              v, BigQueryTableMatcher._get_or_none(actual, k))
          for k, v in expected.items())
    else:
      return expected == actual

  def describe_to(self, description):
    description \
      .append_text("Expected table attributes are ") \
      .append_text(sorted((k, v)
                          for k, v in self.expected_properties.items()))

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual table attributes are ") \
      .append_text(sorted((k, self._get_or_none(self.actual_table, k))
                          for k in self.expected_properties))
