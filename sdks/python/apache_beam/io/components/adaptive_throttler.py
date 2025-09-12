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

import logging
import random
import time

from apache_beam.io.components import util
from apache_beam.metrics.metric import Metrics

_SECONDS_TO_MILLISECONDS = 1_000


class ThrottlingSignaler(object):
  """A class that handles signaling throttling of remote requests to the
  SDK harness.
  """
  def __init__(self, namespace: str = ""):
    self.throttling_metric = Metrics.counter(
        namespace, "cumulativeThrottlingSeconds")

  def signal_throttled(self, seconds: int):
    """Signals to the runner that requests have been throttled for some amount
    of time.

    Args:
      seconds: int, duration of throttling in seconds.
    """
    self.throttling_metric.inc(seconds)


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


class ReactiveThrottler(AdaptiveThrottler):
  """ A wrapper around the AdaptiveThrottler that also handles logging and
  signaling throttling to the SDK harness using the provided namespace.

  For usage, instantiate one instance of a ReactiveThrottler class for a
  PTransform. When making remote calls to a service, preface that call with
  the throttle() method to potentially pre-emptively throttle the request.
  This will throttle future calls based on the failure rate of preceding calls,
  with higher failure rates leading to longer periods of throttling to allow
  system recovery. capture the timestamp of the attempted request, then execute
  the request code. On a success, call successful_request(timestamp) to report
  the success to the throttler. This flow looks like the following:
  
  def remote_call():
    throttler.throttle()

    try:
      timestamp = time.time()
      result = make_request()
      throttler.successful_request(timestamp)
      return result
    except Exception as e:
      # do any error handling you want to do
      raise
  """
  def __init__(
      self,
      window_ms: int,
      bucket_ms: int,
      overload_ratio: float,
      namespace: str = '',
      throttle_delay_secs: int = 5):
    """Initializes the ReactiveThrottler.

    Args:
      window_ms: int, length of history to consider, in ms, to set
        throttling.
      bucket_ms: int, granularity of time buckets that we store data in, in
        ms.
      overload_ratio: float, the target ratio between requests sent and
        successful requests. This is "K" in the formula in
        https://landing.google.com/sre/book/chapters/handling-overload.html.
      namespace: str, the namespace to use for logging and signaling
        throttling is occurring
      throttle_delay_secs: int, the amount of time in seconds to wait
        after preemptively throttled requests
    """
    self.throttling_signaler = ThrottlingSignaler(namespace=namespace)
    self.logger = logging.getLogger(namespace)
    self.throttle_delay_secs = throttle_delay_secs
    super().__init__(
        window_ms=window_ms, bucket_ms=bucket_ms, overload_ratio=overload_ratio)

  def throttle(self):
    """ Stops request code from advancing while the underlying
    AdaptiveThrottler is signaling to preemptively throttle the request.
    Automatically handles logging the throttling and signaling to the SDK
    harness that the request is being throttled. This should be called in any
    context where a call to a remote service is being contacted prior to the
    call being performed.
    """
    while self.throttle_request(time.time() * _SECONDS_TO_MILLISECONDS):
      self.logger.info(
          "Delaying request for %d seconds due to previous failures",
          self.throttle_delay_secs)
      time.sleep(self.throttle_delay_secs)
      self.throttling_signaler.signal_throttled(self.throttle_delay_secs)
