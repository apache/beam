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
Rate Limiter classes for controlling access to external resources.
"""

import abc
import logging
import math
import random
import threading
import time
from typing import Dict
from typing import List

import grpc
from envoy_data_plane.envoy.extensions.common.ratelimit.v3 import RateLimitDescriptor
from envoy_data_plane.envoy.extensions.common.ratelimit.v3 import RateLimitDescriptorEntry
from envoy_data_plane.envoy.service.ratelimit.v3 import RateLimitRequest
from envoy_data_plane.envoy.service.ratelimit.v3 import RateLimitResponse
from envoy_data_plane.envoy.service.ratelimit.v3 import RateLimitResponseCode

from apache_beam.io.components import adaptive_throttler
from apache_beam.metrics import Metrics

_LOGGER = logging.getLogger(__name__)

_RPC_MAX_RETRIES = 5
_RPC_RETRY_DELAY_SECONDS = 10


class RateLimiter(abc.ABC):
  """Abstract base class for RateLimiters."""
  def __init__(self, namespace: str = ""):
    # Metrics collected from the RateLimiter
    # Metric updates are thread safe
    self.throttling_signaler = adaptive_throttler.ThrottlingSignaler(
        namespace=namespace)
    self.requests_counter = Metrics.counter(namespace, 'RatelimitRequestsTotal')
    self.requests_allowed = Metrics.counter(
        namespace, 'RatelimitRequestsAllowed')
    self.requests_throttled = Metrics.counter(
        namespace, 'RatelimitRequestsThrottled')
    self.rpc_errors = Metrics.counter(namespace, 'RatelimitRpcErrors')
    self.rpc_retries = Metrics.counter(namespace, 'RatelimitRpcRetries')
    self.rpc_latency = Metrics.distribution(namespace, 'RatelimitRpcLatencyMs')

  @abc.abstractmethod
  def allow(self, **kwargs) -> bool:
    """Applies rate limiting to the request.

    This method checks if the request is permitted by the rate limiting policy.
    Depending on the implementation and configuration, it may block (sleep)
    until the request is allowed, or return false if the rate limit retry is
    exceeded.

    Args:
      **kwargs: Keyword arguments specific to the RateLimiter implementation.

    Returns:
      bool: True if the request is allowed, False if retries exceeded.

    Raises:
      Exception: If an underlying infrastructure error occurs (e.g. RPC
        failure).
    """
    pass


class EnvoyRateLimiter(RateLimiter):
  """Rate limiter implementation that uses an external Envoy Rate Limit Service.

  This limiter connects to a gRPC Envoy Rate Limit Service (RLS) to determine
  whether a request should be allowed. It supports defining a domain and a
  list of descriptors that correspond to the rate limit configuration in the
  RLS.
  """
  def __init__(
      self,
      service_address: str,
      domain: str,
      descriptors: List[Dict[str, str]],
      timeout: float = 5.0,
      block_until_allowed: bool = True,
      retries: int = 3,
      namespace: str = ''):
    """
    Args:
      service_address: Address of the Envoy RLS (e.g., 'localhost:8081').
      domain: The rate limit domain.
      descriptors: List of descriptors (key-value pairs).
      retries: Number of retries to attempt if rate limited, respected only if
        block_until_allowed is False.
      timeout: gRPC timeout in seconds.
      block_until_allowed: If enabled blocks until RateLimiter gets
        the token.
      namespace: the namespace to use for logging and signaling
        throttling is occurring.
    """
    super().__init__(namespace=namespace)

    self.service_address = service_address
    self.domain = domain
    self.descriptors = descriptors
    self.retries = retries
    self.timeout = timeout
    self.block_until_allowed = block_until_allowed
    self._stub = None
    self._lock = threading.Lock()

  class RateLimitServiceStub(object):
    """ 
    Wrapper for gRPC stub to be compatible with envoy_data_plane messages.
    
    The envoy-data-plane package uses 'betterproto' which generates async stubs
    for 'grpclib'. As Beam uses standard synchronous 'grpcio',
    RateLimitServiceStub is a bridge class to use the betterproto Message types
    (RateLimitRequest) with a standard grpcio Channel.
    """
    def __init__(self, channel):
      self.ShouldRateLimit = channel.unary_unary(
          '/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit',
          request_serializer=RateLimitRequest.SerializeToString,
          response_deserializer=RateLimitResponse.FromString,
      )

  def init_connection(self):
    if self._stub is None:
      # Acquire lock to safegaurd againest multiple DoFn threads sharing the
      # same RateLimiter instance, which is the case when using Shared().
      with self._lock:
        if self._stub is None:
          channel = grpc.insecure_channel(self.service_address)
          self._stub = EnvoyRateLimiter.RateLimitServiceStub(channel)

  def allow(self, hits_added: int = 1) -> bool:
    """Calls the Envoy RLS to apply rate limits.

    Sends a rate limit request to the configured Envoy Rate Limit Service.
    If 'block_until_allowed' is True, this method will sleep and retry
    if the limit is exceeded, effectively blocking until the request is
    permitted.

    If 'block_until_allowed' is False, it will return False after the retry
    limit is exceeded.

    Args:
      hits_added: Number of hits to add to the rate limit.

    Returns:
      bool: True if the request is allowed, False if retries exceeded.
    """
    self.init_connection()

    # execute thread-safe gRPC call
    # Convert descriptors to proto format
    proto_descriptors = []
    for d in self.descriptors:
      entries = []
      for k, v in d.items():
        entries.append(RateLimitDescriptorEntry(key=k, value=v))
      proto_descriptors.append(RateLimitDescriptor(entries=entries))

    request = RateLimitRequest(
        domain=self.domain,
        descriptors=proto_descriptors,
        hits_addend=hits_added)

    self.requests_counter.inc()
    attempt = 0
    throttled = False
    while True:
      if not self.block_until_allowed and attempt > self.retries:
        break

      # retry loop
      for retry_attempt in range(_RPC_MAX_RETRIES):
        try:
          start_time = time.time()
          response = self._stub.ShouldRateLimit(request, timeout=self.timeout)
          self.rpc_latency.update(int((time.time() - start_time) * 1000))
          break
        except grpc.RpcError as e:
          if retry_attempt == _RPC_MAX_RETRIES - 1:
            _LOGGER.error(
                "[EnvoyRateLimiter] ratelimit service call failed: %s", e)
            self.rpc_errors.inc()
            raise e
          self.rpc_retries.inc()
          _LOGGER.warning(
              "[EnvoyRateLimiter] ratelimit service call failed, retrying: %s",
              e)
          time.sleep(_RPC_RETRY_DELAY_SECONDS)

      if response.overall_code == RateLimitResponseCode.OK:
        self.requests_allowed.inc()
        throttled = True
        break
      elif response.overall_code == RateLimitResponseCode.OVER_LIMIT:
        self.requests_throttled.inc()
        # Ratelimit exceeded, sleep for duration until reset and retry
        # multiple rules can be set in the RLS config, so we need to find the
        # max duration
        sleep_s = 0.0
        if response.statuses:
          for status in response.statuses:
            if status.code == RateLimitResponseCode.OVER_LIMIT:
              dur = status.duration_until_reset
              # duration_until_reset is converted to timedelta by betterproto
              val = dur.total_seconds()
              if val > sleep_s:
                sleep_s = val

        # Add 1% additive jitter to prevent thundering herd
        jitter = random.uniform(0, 0.01 * sleep_s)
        sleep_s += jitter

        _LOGGER.warning("[EnvoyRateLimiter] Throttled for %s seconds", sleep_s)
        # signal throttled time to backend
        self.throttling_signaler.signal_throttled(math.ceil(sleep_s))
        time.sleep(sleep_s)
        attempt += 1
      else:
        _LOGGER.error(
            "[EnvoyRateLimiter] Unknown code from RLS: %s",
            response.overall_code)
        break
    return throttled

  def __getstate__(self):
    state = self.__dict__.copy()
    if '_lock' in state:
      del state['_lock']
    if '_stub' in state:
      del state['_stub']
    return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    self._lock = threading.Lock()
    self._stub = None
