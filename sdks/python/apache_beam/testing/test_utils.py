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

"""Utility methods for testing

For internal use only; no backwards-compatibility guarantees.
"""

import hashlib
import imp
import logging
import os
import shutil
import tempfile
import time

from mock import Mock
from mock import patch

from apache_beam.io.filesystems import FileSystems
from apache_beam.utils import retry

DEFAULT_HASHING_ALG = 'sha1'


class TempDir(object):
  """Context Manager to create and clean-up a temporary directory."""

  def __init__(self):
    self._tempdir = tempfile.mkdtemp()

  def __enter__(self):
    return self

  def __exit__(self, *args):
    if os.path.exists(self._tempdir):
      shutil.rmtree(self._tempdir)

  def get_path(self):
    """Returns the path to the temporary directory."""
    return self._tempdir

  def create_temp_file(self, suffix='', lines=None):
    """Creates a temporary file in the temporary directory.

    Args:
      suffix (str): The filename suffix of the temporary file (e.g. '.txt')
      lines (List[str]): A list of lines that will be written to the temporary
        file.
    Returns:
      The name of the temporary file created.
    """
    with tempfile.NamedTemporaryFile(
        delete=False, dir=self._tempdir, suffix=suffix) as f:
      if lines:
        for line in lines:
          f.write(line)

      return f.name


def compute_hash(content, hashing_alg=DEFAULT_HASHING_ALG):
  """Compute a hash value from a list of string."""
  content.sort()
  m = hashlib.new(hashing_alg)
  for elem in content:
    m.update(str(elem))
  return m.hexdigest()


def patch_retry(testcase, module):
  """A function to patch retry module to use mock clock and logger.

  Clock and logger that defined in retry decorator will be replaced in test
  in order to skip sleep phase when retry happens.

  Args:
    testcase: An instance of unittest.TestCase that calls this function to
      patch retry module.
    module: The module that uses retry and need to be replaced with mock
      clock and logger in test.
  """
  real_retry_with_exponential_backoff = retry.with_exponential_backoff

  def patched_retry_with_exponential_backoff(num_retries, retry_filter):
    """A patch for retry decorator to use a mock dummy clock and logger."""
    return real_retry_with_exponential_backoff(
        num_retries=num_retries, retry_filter=retry_filter, logger=Mock(),
        clock=Mock())

  patch.object(retry, 'with_exponential_backoff',
               side_effect=patched_retry_with_exponential_backoff).start()

  # Reload module after patching.
  imp.reload(module)

  def remove_patches():
    patch.stopall()
    # Reload module again after removing patch.
    imp.reload(module)

  testcase.addCleanup(remove_patches)


@retry.with_exponential_backoff(
    num_retries=3,
    retry_filter=retry.retry_on_beam_io_error_filter)
def delete_files(file_paths):
  """A function to clean up files or directories using ``FileSystems``.

  Glob is supported in file path and directories will be deleted recursively.

  Args:
    file_paths: A list of strings contains file paths or directories.
  """
  if len(file_paths) == 0:
    raise RuntimeError('Clean up failed. Invalid file path: %s.' %
                       file_paths)
  FileSystems.delete(file_paths)


def wait_for_subscriptions_created(subs, timeout=60):
  """Wait for all PubSub subscriptions are created."""
  return _wait_until_all_exist(subs, timeout)


def wait_for_topics_created(topics, timeout=60):
  """Wait for all PubSub topics are created."""
  return _wait_until_all_exist(topics, timeout)


def _wait_until_all_exist(components, timeout):
  unchecked_components = set(components)
  start_time = time.time()
  while time.time() - start_time <= timeout:
    unchecked_components = set(
        [c for c in unchecked_components if not c.exists()])
    if len(unchecked_components) == 0:
      return True
    time.sleep(2)

  raise RuntimeError(
      'Timeout after %d seconds. %d of %d topics/subscriptions not exist. '
      'They are %s.' % (timeout, len(unchecked_components),
                        len(components), list(unchecked_components)))


def cleanup_subscriptions(subs):
  """Cleanup PubSub subscriptions if exist."""
  _cleanup_pubsub(subs)


def cleanup_topics(topics):
  """Cleanup PubSub topics if exist."""
  _cleanup_pubsub(topics)


def _cleanup_pubsub(components):
  for c in components:
    if c.exists():
      c.delete()
    else:
      logging.debug('Cannot delete topic/subscription. %s does not exist.',
                    c.full_name)
