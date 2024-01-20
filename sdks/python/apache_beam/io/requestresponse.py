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

"""``PTransform`` for reading from and writing to Web APIs."""
import abc
import concurrent.futures
import contextlib
import logging
import sys
import time
from typing import Generic
from typing import Optional
from typing import TypeVar

from google.api_core.exceptions import TooManyRequests

import apache_beam as beam
from apache_beam.io.components.adaptive_throttler import AdaptiveThrottler
from apache_beam.metrics import Metrics
from apache_beam.ml.inference.vertex_ai_inference import MSEC_TO_SEC
from apache_beam.utils import retry

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')

DEFAULT_TIMEOUT_SECS = 30  # seconds

_LOGGER = logging.getLogger(__name__)


class UserCodeExecutionException(Exception):
  """Base class for errors related to calling Web APIs."""


class UserCodeQuotaException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal specifically that
  the Web API client encountered a Quota or API overuse related error.
  """


class UserCodeTimeoutException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal a user code timeout."""


def retry_on_exception(exception: Exception):
  """retry on exceptions caused by unavailability of the remote server."""
  return isinstance(
      exception,
      (TooManyRequests, UserCodeTimeoutException, UserCodeQuotaException))


class _MetricsCollector:
  """A metrics collector that tracks RequestResponseIO related usage."""
  def __init__(self, namespace: str):
    """
    Args:
      namespace: Namespace for the metrics.
    """
    self.requests = Metrics.counter(namespace, 'requests')
    self.responses = Metrics.counter(namespace, 'responses')
    self.failures = Metrics.counter(namespace, 'failures')
    self.throttled_requests = Metrics.counter(namespace, 'throttled_requests')
    self.throttled_secs = Metrics.counter(
        namespace, 'cumulativeThrottlingSeconds')
    self.timeout_requests = Metrics.counter(namespace, 'requests_timed_out')
    self.call_counter = Metrics.counter(namespace, 'call_invocations')
    self.setup_counter = Metrics.counter(namespace, 'setup_counter')
    self.teardown_counter = Metrics.counter(namespace, 'teardown_counter')
    self.backoff_counter = Metrics.counter(namespace, 'backoff_counter')
    self.sleeper_counter = Metrics.counter(namespace, 'sleeper_counter')
    self.should_backoff_counter = Metrics.counter(
        namespace, 'should_backoff_counter')


class Caller(contextlib.AbstractContextManager,
             abc.ABC,
             Generic[RequestT, ResponseT]):
  """Interface for user custom code intended for API calls.
  For setup and teardown of clients when applicable, implement the
  ``__enter__`` and ``__exit__`` methods respectively."""
  @abc.abstractmethod
  def __call__(self, request: RequestT, *args, **kwargs) -> ResponseT:
    """Calls a Web API with the ``RequestT``  and returns a
    ``ResponseT``. ``RequestResponseIO`` expects implementations of the
    ``__call__`` method to throw either a ``UserCodeExecutionException``,
    ``UserCodeQuotaException``, or ``UserCodeTimeoutException``.
    """
    pass

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    return None


class ShouldBackOff(abc.ABC):
  """
  ShouldBackOff provides mechanism to apply adaptive throttling.
  """
  pass


class Repeater(abc.ABC):
  """Repeater provides mechanism to repeat requests for a
  configurable condition."""
  @abc.abstractmethod
  def repeat(
      self,
      caller: Caller[RequestT, ResponseT],
      request: RequestT,
      timeout: float,
      metrics_collector: Optional[_MetricsCollector]) -> ResponseT:
    """repeat method is called from the RequestResponseIO when
    a repeater is enabled.

    Args:
      caller: :class:`apache_beam.io.requestresponse.Caller` object that calls
        the API.
      request: input request to repeat.
      timeout: time to wait for the request to complete.
      metrics_collector: (Optional) a
        ``:class:`apache_beam.io.requestresponse._MetricsCollector``` object to
        collect the metrics for RequestResponseIO.
    """
    pass


def _execute_request(
    caller: Caller[RequestT, ResponseT],
    request: RequestT,
    timeout: float,
    metrics_collector: Optional[_MetricsCollector] = None) -> ResponseT:
  with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(caller, request)
    try:
      return future.result(timeout=timeout)
    except TooManyRequests as e:
      _LOGGER.info(
          'request could not be completed. got code %i from the service.',
          e.code)
      raise e
    except concurrent.futures.TimeoutError:
      if metrics_collector:
        metrics_collector.timeout_requests.inc(1)
      raise UserCodeTimeoutException(
          f'Timeout {timeout} exceeded '
          f'while completing request: {request}')
    except RuntimeError:
      if metrics_collector:
        metrics_collector.failures.inc(1)
      raise UserCodeExecutionException('could not complete request')


class ExponentialBackOffRepeater(Repeater):
  """Exponential BackOff Repeater uses exponential backoff retry strategy for
  exceptions due to the remote service such as TooManyRequests (HTTP 429),
  UserCodeTimeoutException, UserCodeQuotaException.

  It utilizes the decorator
  :func:`apache_beam.utils.retry.with_exponential_backoff`.
  """
  def __init__(self):
    pass

  @retry.with_exponential_backoff(
      num_retries=2, retry_filter=retry_on_exception)
  def repeat(
      self,
      caller: Caller[RequestT, ResponseT],
      request: RequestT,
      timeout: float,
      metrics_collector: Optional[_MetricsCollector] = None) -> ResponseT:
    """repeat method is called from the RequestResponseIO when
    a repeater is enabled.

    Args:
      caller: :class:`apache_beam.io.requestresponse.Caller` object that
        calls the API.
      request: input request to repeat.
      timeout: time to wait for the request to complete.
      metrics_collector: (Optional) a
        ``:class:`apache_beam.io.requestresponse._MetricsCollector``` object to
        collect the metrics for RequestResponseIO.
    """
    return _execute_request(caller, request, timeout, metrics_collector)


class NoOpsRepeater(Repeater):
  """
  NoOpsRepeater executes a request just once irrespective of any exception.
  """
  def repeat(
      self,
      caller: Caller[RequestT, ResponseT],
      request: RequestT,
      timeout: float,
      metrics_collector: Optional[_MetricsCollector]) -> ResponseT:
    return _execute_request(caller, request, timeout, metrics_collector)


class CacheReader(abc.ABC):
  """CacheReader provides mechanism to read from the cache."""
  pass


class CacheWriter(abc.ABC):
  """CacheWriter provides mechanism to write to the cache."""
  pass


class PreCallThrottler(abc.ABC):
  """PreCallThrottler provides a throttle mechanism before sending request."""
  pass


class DefaultThrottler(PreCallThrottler):
  """Default throttler that uses
  :class:`apache_beam.io.components.adaptive_throttler.AdaptiveThrottler`

  Args:
    window_ms (int): length of history to consider, in ms, to set throttling.
    bucket_ms (int): granularity of time buckets that we store data in, in ms.
    overload_ratio (float): the target ratio between requests sent and
      successful requests. This is "K" in the formula in
      https://landing.google.com/sre/book/chapters/handling-overload.html.
    delay_secs (int): minimum number of seconds to throttle a request.
  """
  def __init__(
      self,
      window_ms: int = 1,
      bucket_ms: int = 1,
      overload_ratio: float = 2,
      delay_secs: int = 5):
    self.throttler = AdaptiveThrottler(
        window_ms=window_ms, bucket_ms=bucket_ms, overload_ratio=overload_ratio)
    self.delay_secs = delay_secs


class RequestResponseIO(beam.PTransform[beam.PCollection[RequestT],
                                        beam.PCollection[ResponseT]]):
  """A :class:`RequestResponseIO` transform to read and write to APIs.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of requests
  by making a call to the API as defined in :class:`Caller`'s `__call__`
  and returns a :class:`~apache_beam.pvalue.PCollection` of responses.
  """
  def __init__(
      self,
      caller: Caller[RequestT, ResponseT],
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Repeater = ExponentialBackOffRepeater(),
      cache_reader: Optional[CacheReader] = None,
      cache_writer: Optional[CacheWriter] = None,
      throttler: PreCallThrottler = DefaultThrottler(),
  ):
    """
    Instantiates a RequestResponseIO transform.

    Args:
      caller (~apache_beam.io.requestresponse.Caller): an implementation of
        `Caller` object that makes call to the API.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff (~apache_beam.io.requestresponse.ShouldBackOff):
        (Optional) provides methods for backoff.
      repeater (~apache_beam.io.requestresponse.Repeater): provides method to
        repeat failed requests to API due to service errors. Defaults to
        :class:`apache_beam.io.requestresponse.ExponentialBackOffRepeater` to
        repeat requests with exponential backoff.
      cache_reader (~apache_beam.io.requestresponse.CacheReader): (Optional)
        provides methods to read external cache.
      cache_writer (~apache_beam.io.requestresponse.CacheWriter): (Optional)
        provides methods to write to external cache.
      throttler (~apache_beam.io.requestresponse.PreCallThrottler):
        provides methods to pre-throttle a request. Defaults to
        :class:`apache_beam.io.requestresponse.DefaultThrottler` for
        client-side adaptive throttling using
        :class:`apache_beam.io.components.adaptive_throttler.AdaptiveThrottler`
    """
    self._caller = caller
    self._timeout = timeout
    self._should_backoff = should_backoff
    if repeater:
      self._repeater = repeater
    else:
      self._repeater = NoOpsRepeater()
    self._cache_reader = cache_reader
    self._cache_writer = cache_writer
    self._throttler = throttler

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    # TODO(riteshghorse): handle Cache and Throttle PTransforms when available.
    if isinstance(self._throttler, DefaultThrottler):
      return requests | _Call(
          caller=self._caller,
          timeout=self._timeout,
          should_backoff=self._should_backoff,
          repeater=self._repeater,
          throttler=self._throttler)
    else:
      return requests | _Call(
          caller=self._caller,
          timeout=self._timeout,
          should_backoff=self._should_backoff,
          repeater=self._repeater)


class _Call(beam.PTransform[beam.PCollection[RequestT],
                            beam.PCollection[ResponseT]]):
  """(Internal-only) PTransform that invokes a remote function on each element
   of the input PCollection.

  This PTransform uses a `Caller` object to invoke the actual API calls,
  and uses ``__enter__`` and ``__exit__`` to manage setup and teardown of
  clients when applicable. Additionally, a timeout value is specified to
  regulate the duration of each call, defaults to 30 seconds.

  Args:
      caller (:class:`apache_beam.io.requestresponse.Caller`): a callable
        object that invokes API call.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff (~apache_beam.io.requestresponse.ShouldBackOff):
        (Optional) provides methods for backoff.
      repeater (~apache_beam.io.requestresponse.Repeater): (Optional) provides
        methods to repeat requests to API.
      throttler (~apache_beam.io.requestresponse.PreCallThrottler):
        (Optional) provides methods to pre-throttle a request.
  """
  def __init__(
      self,
      caller: Caller[RequestT, ResponseT],
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Repeater = None,
      throttler: PreCallThrottler = None,
  ):
    self._caller = caller
    self._timeout = timeout
    self._should_backoff = should_backoff
    self._repeater = repeater
    self._throttler = throttler

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    return requests | beam.ParDo(
        _CallDoFn(self._caller, self._timeout, self._repeater, self._throttler))


class _CallDoFn(beam.DoFn):
  def setup(self):
    self._caller.__enter__()
    self._metrics_collector = _MetricsCollector(self._caller.__str__())
    self._metrics_collector.setup_counter.inc(1)

  def __init__(
      self,
      caller: Caller[RequestT, ResponseT],
      timeout: float,
      repeater: Repeater,
      throttler: PreCallThrottler):
    self._metrics_collector = None
    self._caller = caller
    self._timeout = timeout
    self._repeater = repeater
    self._throttler = throttler

  def process(self, request: RequestT, *args, **kwargs):
    self._metrics_collector.requests.inc(1)

    is_throttled_request = False
    if self._throttler:
      while self._throttler.throttler.throttle_request(time.time() *
                                                       MSEC_TO_SEC):
        _LOGGER.info(
            "Delaying request for %d seconds" % self._throttler.delay_secs)
        time.sleep(self._throttler.delay_secs)
        self._metrics_collector.throttled_secs.inc(self._throttler.delay_secs)
        is_throttled_request = True

    if is_throttled_request:
      self._metrics_collector.throttled_requests.inc(1)

    try:
      req_time = time.time()
      response = self._repeater.repeat(
          self._caller, request, self._timeout, self._metrics_collector)
      self._metrics_collector.responses.inc(1)
      self._throttler.throttler.successful_request(req_time * MSEC_TO_SEC)
      yield response
    except Exception as e:
      raise e

  def teardown(self):
    self._metrics_collector.teardown_counter.inc(1)
    self._caller.__exit__(*sys.exc_info())
