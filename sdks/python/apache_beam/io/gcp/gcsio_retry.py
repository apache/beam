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

import inspect
import logging
import math

from google.api_core import exceptions as api_exceptions
from google.api_core import retry
from google.cloud.storage.retry import DEFAULT_RETRY
from google.cloud.storage.retry import _should_retry  # pylint: disable=protected-access

from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions

_LOGGER = logging.getLogger(__name__)

__all__ = ['DEFAULT_RETRY_WITH_THROTTLING_COUNTER']


class ThrottlingHandler(object):
  _THROTTLED_SECS = Metrics.counter('gcsio', "cumulativeThrottlingSeconds")

  def __call__(self, exc):
    if isinstance(exc, api_exceptions.TooManyRequests):
      _LOGGER.debug('Caught GCS quota error (%s), retrying.', exc.reason)
      # TODO: revisit the logic here when gcs client library supports error
      # callbacks
      frame = inspect.currentframe()
      if frame is None:
        _LOGGER.warning('cannot inspect the current stack frame')
        return

      prev_frame = frame.f_back
      if prev_frame is None:
        _LOGGER.warning('cannot inspect the caller stack frame')
        return

      # next_sleep is one of the arguments in the caller
      # i.e. _retry_error_helper() in google/api_core/retry/retry_base.py
      sleep_seconds = prev_frame.f_locals.get("next_sleep", 0)
      ThrottlingHandler._THROTTLED_SECS.inc(math.ceil(sleep_seconds))


DEFAULT_RETRY_WITH_THROTTLING_COUNTER = retry.Retry(
    predicate=_should_retry, on_error=ThrottlingHandler())


def get_retry(pipeline_options):
  if pipeline_options.view_as(GoogleCloudOptions).no_gcsio_throttling_counter:
    return DEFAULT_RETRY
  else:
    return DEFAULT_RETRY_WITH_THROTTLING_COUNTER
