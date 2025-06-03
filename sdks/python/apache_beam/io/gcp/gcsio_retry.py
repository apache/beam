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
from itertools import tee

from google.api_core import __version__ as _api_core_version
from google.api_core import exceptions as api_exceptions
from google.api_core import retry
from google.cloud.storage.retry import DEFAULT_RETRY
from google.cloud.storage.retry import _should_retry  # pylint: disable=protected-access
from packaging import version

from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions

_LOGGER = logging.getLogger(__name__)

__all__ = ['DEFAULT_RETRY_WITH_THROTTLING_COUNTER']
_MIN_SLEEP_ARG_SWITCH_VERSION = version.parse("2.25.0rc0")
_LEGACY_SLEEP_ARG_NAME = "next_sleep"
_CURRENT_SLEEP_ARG_NAME = "sleep_iterator"


class ThrottlingHandler(object):
  _THROTTLED_SECS = Metrics.counter('gcsio', "cumulativeThrottlingSeconds")

  def __init__(self):
    # decide which arg name google-api-core uses
    try:
      core_ver = version.parse(_api_core_version)
    except Exception:
      core_ver = version.parse("0")
    if core_ver < _MIN_SLEEP_ARG_SWITCH_VERSION:
      self._sleep_arg = _LEGACY_SLEEP_ARG_NAME
    else:
      self._sleep_arg = _CURRENT_SLEEP_ARG_NAME

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

      # Determine which retry helper argument to inspect in
      # google/api_core/retry/retry_base.py’s _retry_error_helper():
      #  - versions < 2.25.0rc0 use “next_sleep”
      #  - versions ≥ 2.25.0rc0 use “sleep_iterator”
      if self._sleep_arg == _LEGACY_SLEEP_ARG_NAME:
        sleep_seconds = prev_frame.f_locals.get(self._sleep_arg, 0)
      else:
        sleep_iterator = prev_frame.f_locals.get(self._sleep_arg, iter([]))
        sleep_iterator, sleep_iterator_copy = tee(sleep_iterator)
        try:
          sleep_seconds = next(sleep_iterator_copy)
        except StopIteration:
          sleep_seconds = 0
      ThrottlingHandler._THROTTLED_SECS.inc(math.ceil(sleep_seconds))


DEFAULT_RETRY_WITH_THROTTLING_COUNTER = retry.Retry(
    predicate=_should_retry, on_error=ThrottlingHandler())


def get_retry(pipeline_options):
  if pipeline_options.view_as(GoogleCloudOptions).no_gcsio_throttling_counter:
    return DEFAULT_RETRY
  else:
    return DEFAULT_RETRY_WITH_THROTTLING_COUNTER
