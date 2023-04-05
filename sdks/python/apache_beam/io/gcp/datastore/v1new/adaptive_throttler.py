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

# Utility functions & classes that are _not_ specific to the datastore client.
#
# For internal use only; no backwards-compatibility guarantees.

# pytype: skip-file

import random

from apache_beam.io.gcp.datastore.v1new import util


class AdaptiveThrottler(object):
  """Implements adaptive throttling.

  See
  https://landing.google.com/sre/book/chapters/handling-overload.html#client-side-throttling-a7sYUg
  for a full discussion of the use case and algorithm applied.
  """

  # The target minimum number of requests per samplePeriodMs, even if no
  # requests succeed. Must be greater than 0, else we could throttle to zero.
  # Because every decision is probabilistic, there is no guarantee that the
  # request rate in any given interval will not be zero. (This is the +1 from
  # the formula in
  # https://landing.google.com/sre/book/chapters/handling-overload.html )
  MIN_REQUESTS = 1

  def __init__(self, window_ms, bucket_ms, overload_ratio):
    """Initializes AdaptiveThrottler.

      Args:
        window_ms: int, length of history to consider, in ms, to set
                   throttling.
        bucket_ms: int, granularity of time buckets that we store data in, in
                   ms.
        overload_ratio: float, the target ratio between requests sent and
                        successful requests. This is "K" in the formula in
                        https://landing.google.com/sre/book/chapters/handling-overload.html.
    """
    self._all_requests = util.MovingSum(window_ms, bucket_ms)
    self._successful_requests = util.MovingSum(window_ms, bucket_ms)
    self._overload_ratio = float(overload_ratio)
    self._random = random.Random()

  def _throttling_probability(self, now):
    if not self._all_requests.has_data(now):
      return 0
    all_requests = self._all_requests.sum(now)
    successful_requests = self._successful_requests.sum(now)
    return max(
        0, (all_requests - self._overload_ratio * successful_requests) /
        (all_requests + AdaptiveThrottler.MIN_REQUESTS))

  def throttle_request(self, now):
    """Determines whether one RPC attempt should be throttled.

    This should be called once each time the caller intends to send an RPC; if
    it returns true, drop or delay that request (calling this function again
    after the delay).

    Args:
      now: int, time in ms since the epoch
    Returns:
      bool, True if the caller should throttle or delay the request.
    """
    throttling_probability = self._throttling_probability(now)
    self._all_requests.add(now, 1)
    return self._random.uniform(0, 1) < throttling_probability

  def successful_request(self, now):
    """Notifies the throttler of a successful request.

    Must be called once for each request (for which throttle_request was
    previously called) that succeeded.

    Args:
      now: int, time in ms since the epoch
    """
    self._successful_requests.add(now, 1)
