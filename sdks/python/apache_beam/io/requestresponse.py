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
import enum
import json
import logging
import sys
import time
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Generic
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import redis
from google.api_core.exceptions import TooManyRequests

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import coders
from apache_beam.io.components.adaptive_throttler import AdaptiveThrottler
from apache_beam.metrics import Metrics
from apache_beam.ml.inference.vertex_ai_inference import MSEC_TO_SEC
from apache_beam.transforms.util import BatchElements
from apache_beam.utils import retry

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')

# DEFAULT_TIMEOUT_SECS represents the time interval for completing the request
# with external source.
DEFAULT_TIMEOUT_SECS = 30

# DEFAULT_CACHE_ENTRY_TTL_SEC represents the total time-to-live
# for cache record.
DEFAULT_CACHE_ENTRY_TTL_SEC = 24 * 60 * 60

_LOGGER = logging.getLogger(__name__)

__all__ = [
    'RequestResponseIO',
    'ExponentialBackOffRepeater',
    'DefaultThrottler',
    'NoOpsRepeater',
    'RedisCache',
]


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

  def get_cache_key(self, request: RequestT) -> str:
    """Returns the request to be cached.

    This is how the response will be looked up in the cache as well.
    By default, entire request is cached as the key for the cache.
    Implement this method to override the key for the cache.
    For example, in `BigTableEnrichmentHandler`, the row key for the element
    is returned here.
    """
    return ""

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns a kwargs suitable for `beam.BatchElements`."""
    return {}


class ShouldBackOff(abc.ABC):
  """
  Provides mechanism to apply adaptive throttling.
  """
  pass


class Repeater(abc.ABC):
  """Provides mechanism to repeat requests for a
  configurable condition."""
  @abc.abstractmethod
  def repeat(
      self,
      caller: Caller[RequestT, ResponseT],
      request: RequestT,
      timeout: float,
      metrics_collector: Optional[_MetricsCollector]) -> ResponseT:
    """Implements a repeater strategy for RequestResponseIO when a repeater
    is enabled.

    Args:
      caller: a `~apache_beam.io.requestresponse.Caller` object that
        calls the API.
      request: input request to repeat.
      timeout: time to wait for the request to complete.
      metrics_collector: (Optional) a
        `~apache_beam.io.requestresponse._MetricsCollector` object
        to collect the metrics for RequestResponseIO.
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
  """Configure exponential backoff retry strategy.

  It retries for exceptions due to the remote service such as
  TooManyRequests (HTTP 429), UserCodeTimeoutException, UserCodeQuotaException.

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
      caller: a `~apache_beam.io.requestresponse.Caller` object that
        calls the API.
      request: input request to repeat.
      timeout: time to wait for the request to complete.
      metrics_collector: (Optional) a
        `~apache_beam.io.requestresponse._MetricsCollector` object to
        collect the metrics for RequestResponseIO.
    """
    return _execute_request(caller, request, timeout, metrics_collector)


class NoOpsRepeater(Repeater):
  """Executes a request just once irrespective of any exception.
  """
  def repeat(
      self,
      caller: Caller[RequestT, ResponseT],
      request: RequestT,
      timeout: float,
      metrics_collector: Optional[_MetricsCollector]) -> ResponseT:
    return _execute_request(caller, request, timeout, metrics_collector)


class PreCallThrottler(abc.ABC):
  """Provides a throttle mechanism before sending request."""
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


class _FilterCacheReadFn(beam.DoFn):
  """A `DoFn` that partitions cache reads.

  It emits to main output for successful cache read requests or
  to the tagged output - `cache_misses` - otherwise."""
  def process(self, element: Tuple[RequestT, ResponseT], *args, **kwargs):
    if not element[1]:
      yield pvalue.TaggedOutput('cache_misses', element[0])
    else:
      yield element


class _Call(beam.PTransform[beam.PCollection[RequestT],
                            beam.PCollection[ResponseT]]):
  """(Internal-only) PTransform that invokes a remote function on each element
   of the input PCollection.

  This PTransform uses a `Caller` object to invoke the actual API calls,
  and uses ``__enter__`` and ``__exit__`` to manage setup and teardown of
  clients when applicable. Additionally, a timeout value is specified to
  regulate the duration of each call, defaults to 30 seconds.

  Args:
      caller: a `Caller` object that invokes API call.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff: (Optional) provides methods for backoff.
      repeater: (Optional) provides methods to repeat requests to API.
      throttler: (Optional) provides methods to pre-throttle a request.
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


class Cache(abc.ABC):
  """Base Cache class for
  :class:`apache_beam.io.requestresponse.RequestResponseIO`.

  For adding cache support to RequestResponseIO, implement this class.
  """
  @abc.abstractmethod
  def get_read(self):
    """returns a PTransform that reads from the cache."""
    pass

  @abc.abstractmethod
  def get_write(self):
    """returns a PTransform that writes to the cache."""
    pass

  @property
  @abc.abstractmethod
  def request_coder(self):
    """request coder to use with Cache."""
    pass

  @request_coder.setter
  @abc.abstractmethod
  def request_coder(self, request_coder: coders.Coder):
    """sets the request coder to use with Cache."""
    pass

  @property
  @abc.abstractmethod
  def source_caller(self):
    """Actual caller that is using the cache."""
    pass

  @source_caller.setter
  @abc.abstractmethod
  def source_caller(self, caller: Caller):
    """Sets the source caller for
    :class:`apache_beam.io.requestresponse.RequestResponseIO` to pull
    cache request key from respective callers."""
    pass


class _RedisMode(enum.Enum):
  """
  Mode of operation for redis cache when using
  `~apache_beam.io.requestresponse._RedisCaller`.
  """
  READ = 0
  WRITE = 1


class _RedisCaller(Caller):
  """An implementation of
  `~apache_beam.io.requestresponse.Caller` for Redis client.

  It provides the functionality for making requests to Redis server using
  :class:`apache_beam.io.requestresponse.RequestResponseIO`.
  """
  def __init__(
      self,
      host: str,
      port: int,
      time_to_live: Union[int, timedelta],
      *,
      request_coder: Optional[coders.Coder],
      response_coder: Optional[coders.Coder],
      kwargs: Optional[Dict[str, Any]] = None,
      source_caller: Optional[Caller] = None,
      mode: _RedisMode,
  ):
    """
    Args:
      host (str): The hostname or IP address of the Redis server.
      port (int): The port number of the Redis server.
      time_to_live: `(Union[int, timedelta])` The time-to-live (TTL) for
        records stored in Redis. Provide an integer (in seconds) or a
        `datetime.timedelta` object.
      request_coder: (Optional[`coders.Coder`]) coder for requests stored
        in Redis.
      response_coder: (Optional[`coders.Coder`]) coder for decoding responses
        received from Redis.
      kwargs: Optional(Dict[str, Any]) additional keyword arguments that
        are required to connect to your redis server. Same as `redis.Redis()`.
      source_caller: (Optional[`Caller`]): The source caller using this Redis
        cache in case of fetching the cache request to store in Redis.
      mode: `_RedisMode` An enum type specifying the operational mode of
        the `_RedisCaller`.
    """
    self.host, self.port = host, port
    self.time_to_live = time_to_live
    self.request_coder = request_coder
    self.response_coder = response_coder
    self.kwargs = kwargs
    self.source_caller = source_caller
    self.mode = mode

  def __enter__(self):
    self.client = redis.Redis(self.host, self.port, **self.kwargs)

  def _read_cache(self, element):
    cache_request = self.source_caller.get_cache_key(element)
    # check if the caller is a enrichment handler. EnrichmentHandler
    # provides the request format for cache.
    if cache_request:
      encoded_request = self.request_coder.encode(cache_request)
    else:
      encoded_request = self.request_coder.encode(element)

    encoded_response = self.client.get(encoded_request)
    if not encoded_response:
      # no cache entry present for this request.
      return element, None

    if self.response_coder is None:
      try:
        response_dict = json.loads(encoded_response.decode('utf-8'))
        response = beam.Row(**response_dict)
      except Exception:
        _LOGGER.warning(
            'cannot decode response from redis cache for %s.' % element)
        return element, None
    else:
      response = self.response_coder.decode(encoded_response)
    return element, response

  def _write_cache(self, element):
    cache_request = self.source_caller.get_cache_key(element[0])
    if cache_request:
      encoded_request = self.request_coder.encode(cache_request)
    else:
      encoded_request = self.request_coder.encode(element[0])
    if self.response_coder is None:
      try:
        encoded_response = json.dumps(element[1]._asdict()).encode('utf-8')
      except Exception:
        _LOGGER.warning(
            'cannot encode response %s for %s to store in '
            'redis cache.' % (element[1], element[0]))
        return element
    else:
      encoded_response = self.response_coder.encode(element[1])
    # Write to cache with TTL. Set nx to True to prevent overwriting for the
    # same key.
    self.client.set(
        encoded_request, encoded_response, self.time_to_live, nx=True)
    return element

  def __call__(self, element, *args, **kwargs):
    if self.mode == _RedisMode.READ:
      if isinstance(element, List):
        responses = [self._read_cache(e) for e in element]
        return responses
      else:
        return self._read_cache(element)
    else:
      if isinstance(element, List):
        responses = [self._write_cache(e) for e in element]
        return responses
      else:
        return self._write_cache(element)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()


class _ReadFromRedis(beam.PTransform[beam.PCollection[RequestT],
                                     beam.PCollection[ResponseT]]):
  """A `PTransform` that performs Redis cache read."""
  def __init__(
      self,
      host: str,
      port: int,
      time_to_live: Union[int, timedelta],
      *,
      kwargs: Optional[Dict[str, Any]] = None,
      request_coder: Optional[coders.Coder],
      response_coder: Optional[coders.Coder],
      source_caller: Optional[Caller[RequestT, ResponseT]] = None,
  ):
    """
    Args:
      host (str): The hostname or IP address of the Redis server.
      port (int): The port number of the Redis server.
      time_to_live: `(Union[int, timedelta])` The time-to-live (TTL) for
        records stored in Redis. Provide an integer (in seconds) or a
        `datetime.timedelta` object.
      kwargs: Optional(Dict[str, Any]) additional keyword arguments that
        are required to connect to your redis server. Same as `redis.Redis()`.
      request_coder: (Optional[`coders.Coder`]) coder for requests stored
        in Redis.
      response_coder: (Optional[`coders.Coder`]) coder for decoding responses
        received from Redis.
      source_caller: (Optional[`Caller`]): The source caller using this Redis
        cache in case of fetching the cache request to store in Redis.
    """
    self.request_coder = request_coder
    self.response_coder = response_coder
    self.redis_caller = _RedisCaller(
        host,
        port,
        time_to_live,
        request_coder=self.request_coder,
        response_coder=self.response_coder,
        kwargs=kwargs,
        source_caller=source_caller,
        mode=_RedisMode.READ)

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    return requests | RequestResponseIO(self.redis_caller)


class _WriteToRedis(beam.PTransform[beam.PCollection[Tuple[RequestT,
                                                           ResponseT]],
                                    beam.PCollection[ResponseT]]):
  """A `PTransfrom` that performs write to Redis cache."""
  def __init__(
      self,
      host: str,
      port: int,
      time_to_live: Union[int, timedelta],
      *,
      kwargs: Optional[Dict[str, Any]] = None,
      request_coder: Optional[coders.Coder],
      response_coder: Optional[coders.Coder],
      source_caller: Optional[Caller[RequestT, ResponseT]] = None,
  ):
    """
    Args:
      host (str): The hostname or IP address of the Redis server.
      port (int): The port number of the Redis server.
      time_to_live: `(Union[int, timedelta])` The time-to-live (TTL) for
        records stored in Redis. Provide an integer (in seconds) or a
        `datetime.timedelta` object.
      kwargs: Optional(Dict[str, Any]) additional keyword arguments that
        are required to connect to your redis server. Same as `redis.Redis()`.
      request_coder: (Optional[`coders.Coder`]) coder for requests stored
        in Redis.
      response_coder: (Optional[`coders.Coder`]) coder for decoding responses
        received from Redis.
      source_caller: (Optional[`Caller`]): The source caller using this Redis
        cache in case of fetching the cache request to store in Redis.
      """
    self.request_coder = request_coder
    self.response_coder = response_coder
    self.redis_caller = _RedisCaller(
        host,
        port,
        time_to_live,
        request_coder=self.request_coder,
        response_coder=self.response_coder,
        kwargs=kwargs,
        source_caller=source_caller,
        mode=_RedisMode.WRITE)

  def expand(
      self, elements: beam.PCollection[Tuple[RequestT, ResponseT]]
  ) -> beam.PCollection[ResponseT]:
    return elements | RequestResponseIO(self.redis_caller)


def ensure_coders_exist(request_coder):
  """checks if the coder exists to encode the request for caching."""
  if not request_coder:
    raise ValueError(
        'need request coder to be able to use '
        'Cache with RequestResponseIO.')


class RedisCache(Cache):
  """Configure cache using Redis for
  :class:`apache_beam.io.requestresponse.RequestResponseIO`."""
  def __init__(
      self,
      host: str,
      port: int,
      time_to_live: Union[int, timedelta] = DEFAULT_CACHE_ENTRY_TTL_SEC,
      *,
      request_coder: Optional[coders.Coder] = None,
      response_coder: Optional[coders.Coder] = None,
      **kwargs,
  ):
    """
    Args:
      host (str): The hostname or IP address of the Redis server.
      port (int): The port number of the Redis server.
      time_to_live: `(Union[int, timedelta])` The time-to-live (TTL) for
        records stored in Redis. Provide an integer (in seconds) or a
        `datetime.timedelta` object.
      request_coder: (Optional[`coders.Coder`]) coder for encoding requests.
      response_coder: (Optional[`coders.Coder`]) coder for decoding responses
        received from Redis.
      kwargs: Optional additional keyword arguments that
        are required to connect to your redis server. Same as `redis.Redis()`.
    """
    self._host = host
    self._port = port
    self._time_to_live = time_to_live
    self._request_coder = request_coder
    self._response_coder = response_coder
    self._kwargs = kwargs if kwargs else {}
    self._source_caller = None

  def get_read(self):
    """get_read returns a PTransform for reading from the cache."""
    ensure_coders_exist(self._request_coder)
    return _ReadFromRedis(
        self._host,
        self._port,
        time_to_live=self._time_to_live,
        kwargs=self._kwargs,
        request_coder=self._request_coder,
        response_coder=self._response_coder,
        source_caller=self._source_caller)

  def get_write(self):
    """returns a PTransform for writing to the cache."""
    ensure_coders_exist(self._request_coder)
    return _WriteToRedis(
        self._host,
        self._port,
        time_to_live=self._time_to_live,
        kwargs=self._kwargs,
        request_coder=self._request_coder,
        response_coder=self._response_coder,
        source_caller=self._source_caller)

  @property
  def source_caller(self):
    return self._source_caller

  @source_caller.setter
  def source_caller(self, source_caller: Caller):
    self._source_caller = source_caller

  @property
  def request_coder(self):
    return self._request_coder

  @request_coder.setter
  def request_coder(self, request_coder: coders.Coder):
    self._request_coder = request_coder


class FlattenBatch(beam.DoFn):
  """Flatten a batched PCollection."""
  def process(self, elements, *args, **kwargs):
    for element in elements:
      yield element


class RequestResponseIO(beam.PTransform[beam.PCollection[RequestT],
                                        beam.PCollection[ResponseT]]):
  """A :class:`RequestResponseIO` transform to read and write to APIs.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of requests
  by making a call to the API as defined in `Caller`'s `__call__` method
  and returns a :class:`~apache_beam.pvalue.PCollection` of responses.
  """
  def __init__(
      self,
      caller: Caller[RequestT, ResponseT],
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Repeater = ExponentialBackOffRepeater(),
      cache: Optional[Cache] = None,
      throttler: PreCallThrottler = DefaultThrottler(),
  ):
    """
    Instantiates a RequestResponseIO transform.

    Args:
      caller: an implementation of
        `Caller` object that makes call to the API.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff: (Optional) provides methods for backoff.
      repeater: provides method to repeat failed requests to API due to service
        errors. Defaults to
        :class:`apache_beam.io.requestresponse.ExponentialBackOffRepeater` to
        repeat requests with exponential backoff.
      cache: (Optional) a `~apache_beam.io.requestresponse.Cache` object
        to use the appropriate cache.
      throttler: provides methods to pre-throttle a request. Defaults to
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
    self._cache = cache
    self._throttler = throttler
    self._batching_kwargs = self._caller.batch_elements_kwargs()

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    # TODO(riteshghorse): handle Throttle PTransforms when available.

    if self._cache:
      self._cache.source_caller = self._caller

    inputs = requests

    if self._cache:
      # read from cache.
      outputs = inputs | self._cache.get_read()
      # filter responses that are None and send them to the Call transform
      # to fetch a value from external service.
      cached_responses, inputs = (outputs
                                  | beam.ParDo(_FilterCacheReadFn()
                                               ).with_outputs(
                                    'cache_misses', main='cached_responses'))

    # Batch elements if batching is enabled.
    if self._batching_kwargs:
      inputs = inputs | BatchElements(**self._batching_kwargs)

    if isinstance(self._throttler, DefaultThrottler):
      # DefaultThrottler applies throttling in the DoFn of
      # Call PTransform.
      responses = (
          inputs
          | _Call(
              caller=self._caller,
              timeout=self._timeout,
              should_backoff=self._should_backoff,
              repeater=self._repeater,
              throttler=self._throttler))
    else:
      # No throttling mechanism. The requests are made to the external source
      # as they come.
      responses = (
          inputs
          | _Call(
              caller=self._caller,
              timeout=self._timeout,
              should_backoff=self._should_backoff,
              repeater=self._repeater))

    # if batching is enabled then handle accordingly.
    if self._batching_kwargs:
      responses = responses | "FlattenBatch" >> beam.ParDo(FlattenBatch())

    if self._cache:
      # write to cache.
      _ = responses | self._cache.get_write()
      return (cached_responses, responses) | beam.Flatten()

    return responses
