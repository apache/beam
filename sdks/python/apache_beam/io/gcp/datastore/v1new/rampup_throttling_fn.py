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

import datetime
import logging
import time
from typing import TypeVar

from apache_beam import typehints
from apache_beam.io.gcp.datastore.v1new import util
from apache_beam.metrics.metric import Metrics
from apache_beam.transforms import DoFn
from apache_beam.utils.retry import FuzzedExponentialIntervals

T = TypeVar('T')

_LOG = logging.getLogger(__name__)


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class RampupThrottlingFn(DoFn):
  """A ``DoFn`` that throttles ramp-up following an exponential function.

  An implementation of a client-side throttler that enforces a gradual ramp-up,
  broadly in line with Datastore best practices. See also
  https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
  """

  _BASE_BUDGET = 500
  _RAMP_UP_INTERVAL = datetime.timedelta(minutes=5)

  def __init__(self, num_workers, *unused_args, **unused_kwargs):
    """Initializes a ramp-up throttler transform.

     Args:
       num_workers: A hint for the expected number of workers, used to derive
                    the local rate limit.
     """
    super().__init__(*unused_args, **unused_kwargs)
    self._num_workers = num_workers
    self._successful_ops = util.MovingSum(window_ms=1000, bucket_ms=1000)
    self._first_instant = datetime.datetime.now()
    self._throttled_secs = Metrics.counter(
        RampupThrottlingFn, "cumulativeThrottlingSeconds")

  def _calc_max_ops_budget(
      self,
      first_instant: datetime.datetime,
      current_instant: datetime.datetime):
    """Function that returns per-second budget according to best practices.

    The exact function is `500 / num_workers * 1.5^max(0, (x-5)/5)`, where x is
    the number of minutes since start time.
    """
    timedelta_since_first = current_instant - first_instant
    growth = max(
        0.0, (timedelta_since_first - self._RAMP_UP_INTERVAL) /
        self._RAMP_UP_INTERVAL)
    try:
      max_ops_budget = int(
          self._BASE_BUDGET / self._num_workers * (1.5**growth))
    except OverflowError:
      max_ops_budget = float('inf')
    return max(1, max_ops_budget)

  def process(self, element, **kwargs):
    backoff = iter(
        FuzzedExponentialIntervals(initial_delay_secs=1, num_retries=10000))

    while True:
      instant = datetime.datetime.now()
      max_ops_budget = self._calc_max_ops_budget(self._first_instant, instant)
      current_op_count = self._successful_ops.sum(instant.timestamp() * 1000)
      available_ops = max_ops_budget - current_op_count

      if available_ops > 0:
        self._successful_ops.add(instant.timestamp() * 1000, 1)
        yield element
        break
      else:
        backoff_secs = next(backoff)
        _LOG.info(
            'Delaying by %sms to conform to gradual ramp-up.',
            int(1000 * backoff_secs))
        time.sleep(backoff_secs)
        self._throttled_secs.inc(int(backoff_secs))
