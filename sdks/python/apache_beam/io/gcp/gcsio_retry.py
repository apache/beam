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

"""
Throttling Handler for GCSIO
"""

import logging
import math
import random
import time

from apache_beam.metrics.metric import Metrics
from google.api_core import retry
from google.api_core import exceptions as api_exceptions
from google.cloud.storage.retry import _should_retry

_LOGGER = logging.getLogger(__name__)

__all__ = ['DEFAULT_RETRY_WITH_THROTTLING']


class ThrottlingHandler(object):
  _THROTTLED_SECS = Metrics.counter('gcsio', "cumulativeThrottlingSeconds")

  def __init__(self, max_retries=10, max_retry_wait=600):
    self._max_retries = max_retries
    self._max_retry_wait = max_retry_wait
    self._num_retries = 0
    self._total_retry_wait = 0

  def _get_next_wait_time(self):
    wait_time = 2**self._num_retries
    max_jitter = wait_time / 4.0
    wait_time += random.uniform(-max_jitter, max_jitter)
    return max(1, min(wait_time, self._max_retry_wait))

  def __call__(self, exc):
    if isinstance(exc, api_exceptions.TooManyRequests):
      _LOGGER.debug('Caught GCS quota error (%s), retrying.', exc.reason)
      self._num_retries += 1
      sleep_seconds = self._get_next_wait_time()
      ThrottlingHandler._THROTTLED_SECS.inc(math.ceil(sleep_seconds))
      time.sleep(sleep_seconds)


DEFAULT_RETRY_WITH_THROTTLING = retry.Retry(
    predicate=_should_retry, on_error=ThrottlingHandler())
