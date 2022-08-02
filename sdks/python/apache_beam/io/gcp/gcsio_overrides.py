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

# pytype: skip-file

import logging
import math
import time

from apache_beam.metrics.metric import Metrics
from apitools.base.py import exceptions
from apitools.base.py import http_wrapper
from apitools.base.py import util

_LOGGER = logging.getLogger(__name__)


class GcsIOOverrides(object):
  """Functions for overriding Google Cloud Storage I/O client."""

  _THROTTLED_SECS = Metrics.counter('StorageV1', "cumulativeThrottlingSeconds")

  @classmethod
  def retry_func(cls, retry_args):
    # handling GCS download throttling errors (BEAM-7424)
    if (isinstance(retry_args.exc, exceptions.BadStatusCodeError) and
        retry_args.exc.status_code == http_wrapper.TOO_MANY_REQUESTS):
      _LOGGER.debug(
          'Caught GCS quota error (%s), retrying.', retry_args.exc.status_code)
    else:
      return http_wrapper.HandleExceptionsAndRebuildHttpConnections(retry_args)

    http_wrapper.RebuildHttpConnections(retry_args.http)
    _LOGGER.debug(
        'Retrying request to url %s after exception %s',
        retry_args.http_request.url,
        retry_args.exc)
    sleep_seconds = util.CalculateWaitForRetry(
        retry_args.num_retries, max_wait=retry_args.max_retry_wait)
    cls._THROTTLED_SECS.inc(math.ceil(sleep_seconds))
    time.sleep(sleep_seconds)
