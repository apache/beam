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

"""End-to-end test result verifiers

A set of verifiers that are used in end-to-end tests to verify state/output
of test pipeline job. Customized verifier should extend
`hamcrest.core.base_matcher.BaseMatcher` and override _matches.
"""

import hashlib
import logging

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.io.fileio import ChannelFactory
from apache_beam.runners.runner import PipelineState
from apache_beam.utils import retry

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None

DEFAULT_HASHING_ALG = 'sha1'
MAX_RETRIES = 4


class PipelineStateMatcher(BaseMatcher):
  """Matcher that verify pipeline job terminated in expected state

  Matcher compares the actual pipeline terminate state with expected.
  By default, `PipelineState.DONE` is used as expected state.
  """

  def __init__(self, expected_state=PipelineState.DONE):
    self.expected_state = expected_state

  def _matches(self, pipeline_result):
    return pipeline_result.state == self.expected_state

  def describe_to(self, description):
    description \
      .append_text("Test pipeline expected terminated in state: ") \
      .append_text(self.expected_state)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Test pipeline job terminated in state: ") \
      .append_text(pipeline_result.state)


def retry_on_io_error_and_server_error(exception):
  """Filter allowing retries on file I/O errors and service error."""
  if isinstance(exception, IOError) or \
          (HttpError is not None and isinstance(exception, HttpError)):
    return True
  else:
    return False


def retry_on_http_and_value_error(exception):
  """Filter allowing retries on Bigquery errors and value error."""
  if isinstance(exception, GoogleCloudError) or \
          isinstance(exception, ValueError):
    return True
  else:
    return False


def compute_hash(list, hashing_alg=DEFAULT_HASHING_ALG):
  """Compute a hash value from a list of string using SHA-1 as default."""
  list.sort()
  m = hashlib.new(hashing_alg)
  for elem in list:
    m.update(str(elem))
  return m.hexdigest()


class FileChecksumMatcher(BaseMatcher):
  """Matcher that verifies file(s) content by comparing file checksum.

  Use apache_beam.io.fileio to fetch file(s) from given path. File checksum
  is a hash string computed from content of file(s).
  """

  def __init__(self, file_path, expected_checksum):
    self.file_path = file_path
    self.expected_checksum = expected_checksum

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_io_error_and_server_error)
  def _read_with_retry(self):
    """Read path with retry if I/O failed"""
    read_lines = []
    matched_path = ChannelFactory.glob(self.file_path)
    if not matched_path:
      raise IOError('No such file or directory: %s' % self.file_path)
    for path in matched_path:
      with ChannelFactory.open(path, 'r') as f:
        for line in f:
          read_lines.append(line)
    return read_lines

  def _matches(self, _):
    # Read from given file(s) path
    read_lines = self._read_with_retry()

    # Compute checksum
    self.checksum = compute_hash(read_lines)
    logging.info('Read from given path %s, %d lines, checksum: %s.',
                 self.file_path, len(read_lines), self.checksum)
    return self.checksum == self.expected_checksum

  def describe_to(self, description):
    description \
      .append_text("Expected checksum is ") \
      .append_text(self.expected_checksum)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual checksum is ") \
      .append_text(self.checksum)


class BigqueryMatcher(BaseMatcher):
  """Matcher that verifies Bigquery data with given query.

  Fetch Bigquery data with given query, compute a SHA-1 hash string and
  compare with expected checksum.
  """

  def __init__(self, project, query, checksum):
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

    # Compute checksum
    self.checksum = compute_hash(response)
    logging.info('Read from given query (%s), total rows %d, checksum %s',
                 self.query, len(response), self.checksum)

    # Verify result
    return self.checksum == self.expected_checksum

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry_on_http_and_value_error)
  def _query_with_retry(self, bigquery_client):
    """Run Bigquery query with retry if got error http response"""
    query = bigquery_client.run_sync_query(self.query)
    query.run()

    # Fetch query data one page at a time.
    page_token = None
    results = []
    while True:
      rows, _, page_token = query.fetch_data(page_token=page_token)
      results.extend(rows)
      if not page_token:
        break

    return results

  def describe_to(self, description):
    description \
      .append_text("Expected checksum is ") \
      .append_text(self.expected_checksum)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual checksum is ") \
      .append_text(self.checksum)
