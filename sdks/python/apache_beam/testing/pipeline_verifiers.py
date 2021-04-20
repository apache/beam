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

# pytype: skip-file

import logging
import time

from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils as utils
from apache_beam.utils import retry

__all__ = [
    'PipelineStateMatcher',
    'FileChecksumMatcher',
    'retry_on_io_error_and_server_error',
]

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None

MAX_RETRIES = 4

_LOGGER = logging.getLogger(__name__)


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
  return isinstance(exception, IOError) or \
          (HttpError is not None and isinstance(exception, HttpError))


class FileChecksumMatcher(BaseMatcher):
  """Matcher that verifies file(s) content by comparing file checksum.

  Use apache_beam.io.filebasedsink to fetch file(s) from given path.
  File checksum is a hash string computed from content of file(s).
  """
  def __init__(self, file_path, expected_checksum, sleep_secs=None):
    """Initialize a FileChecksumMatcher object

    Args:
      file_path : A string that is the full path of output file. This path
        can contain globs.
      expected_checksum : A hash string that is computed from expected
        result.
      sleep_secs : Number of seconds to wait before verification start.
        Extra time are given to make sure output files are ready on FS.
    """
    if sleep_secs is not None:
      if isinstance(sleep_secs, int):
        self.sleep_secs = sleep_secs
      else:
        raise ValueError(
            'Sleep seconds, if received, must be int. '
            'But received: %r, %s' % (sleep_secs, type(sleep_secs)))
    else:
      self.sleep_secs = None

    self.file_path = file_path
    self.expected_checksum = expected_checksum

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES, retry_filter=retry_on_io_error_and_server_error)
  def _read_with_retry(self):
    """Read path with retry if I/O failed"""
    read_lines = []
    match_result = FileSystems.match([self.file_path])[0]
    matched_path = [f.path for f in match_result.metadata_list]
    if not matched_path:
      raise IOError('No such file or directory: %s' % self.file_path)

    _LOGGER.info(
        'Find %d files in %s: \n%s',
        len(matched_path),
        self.file_path,
        '\n'.join(matched_path))
    for path in matched_path:
      with FileSystems.open(path, 'r') as f:
        for line in f:
          read_lines.append(line)
    return read_lines

  def _matches(self, _):
    if self.sleep_secs:
      # Wait to have output file ready on FS
      _LOGGER.info('Wait %d seconds...', self.sleep_secs)
      time.sleep(self.sleep_secs)

    # Read from given file(s) path
    read_lines = self._read_with_retry()

    # Compute checksum
    self.checksum = utils.compute_hash(read_lines)
    _LOGGER.info(
        'Read from given path %s, %d lines, checksum: %s.',
        self.file_path,
        len(read_lines),
        self.checksum)
    return self.checksum == self.expected_checksum

  def describe_to(self, description):
    description \
      .append_text("Expected checksum is ") \
      .append_text(self.expected_checksum)

  def describe_mismatch(self, pipeline_result, mismatch_description):
    mismatch_description \
      .append_text("Actual checksum is ") \
      .append_text(self.checksum)
