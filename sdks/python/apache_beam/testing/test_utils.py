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

"""Utility methods for testing"""

import hashlib
import imp
from mock import Mock, patch

from apache_beam.utils import retry

DEFAULT_HASHING_ALG = 'sha1'


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
