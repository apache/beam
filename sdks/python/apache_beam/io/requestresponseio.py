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
from typing import Generic
from typing import Optional
from typing import TypeVar

import apache_beam as beam
from apache_beam.pvalue import PCollection

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


class Caller(contextlib.AbstractContextManager, abc.ABC):
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
  pass


class CacheReader(abc.ABC):
  """CacheReader provides mechanism to read from the cache."""
  pass


class CacheWriter(abc.ABC):
  """CacheWriter provides mechanism to write to the cache."""
  pass


class PreCallThrottler(abc.ABC):
  """PreCallThrottler provides a throttle mechanism before sending request."""
  pass


class RequestResponseIO(beam.PTransform[beam.PCollection[RequestT],
                                        beam.PCollection[ResponseT]]):
  """A :class:`RequestResponseIO` transform to read and write to APIs.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of requests
  by making a call to the API as defined in :class:`Caller`'s `__call__`
  and returns a :class:`~apache_beam.pvalue.PCollection` of responses.
  """
  def __init__(
      self,
      caller: [Caller],
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Optional[Repeater] = None,
      cache_reader: Optional[CacheReader] = None,
      cache_writer: Optional[CacheWriter] = None,
      throttler: Optional[PreCallThrottler] = None,
  ):
    """
    Instantiates a RequestResponseIO transform.

    Args:
      caller (~apache_beam.io.requestresponseio.Caller): an implementation of
        `Caller` object that makes call to the API.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff (~apache_beam.io.requestresponseio.ShouldBackOff):
        (Optional) provides methods for backoff.
      repeater (~apache_beam.io.requestresponseio.Repeater): (Optional)
        provides methods to repeat requests to API.
      cache_reader (~apache_beam.io.requestresponseio.CacheReader): (Optional)
        provides methods to read external cache.
      cache_writer (~apache_beam.io.requestresponseio.CacheWriter): (Optional)
        provides methods to write to external cache.
      throttler (~apache_beam.io.requestresponseio.PreCallThrottler):
        (Optional) provides methods to pre-throttle a request.
    """
    self._caller = caller
    self._timeout = timeout
    self._should_backoff = should_backoff
    self._repeater = repeater
    self._cache_reader = cache_reader
    self._cache_writer = cache_writer
    self._throttler = throttler

  def expand(self, requests: PCollection[RequestT]) -> PCollection[ResponseT]:
    # TODO(riteshghorse): add Cache and Throttle PTransforms.
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
      caller (:class:`apache_beam.io.requestresponseio.Caller`): a callable
        object that invokes API call.
      timeout (float): timeout value in seconds to wait for response from API.
  """
  def __init__(
      self,
      caller: Caller,
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Optional[Repeater] = None,
  ):
    """Initialize the _Call transform.
    Args:
      caller (:class:`apache_beam.io.requestresponseio.Caller`): a callable
        object that invokes API call.
      timeout (float): timeout value in seconds to wait for response from API.
      should_backoff (~apache_beam.io.requestresponseio.ShouldBackOff):
        (Optional) provides methods for backoff.
      repeater (~apache_beam.io.requestresponseio.Repeater): (Optional) provides
        methods to repeat requests to API.
    """
    self._caller = caller
    self._timeout = timeout
    self._should_backoff = should_backoff
    self._repeater = repeater

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    return requests | beam.ParDo(_CallDoFn(self._caller, self._timeout))


class _CallDoFn(beam.DoFn, Generic[RequestT, ResponseT]):
  def setup(self):
    self._caller.__enter__()

  def __init__(self, caller: Caller, timeout: float):
    self._caller = caller
    self._timeout = timeout

  def process(self, request, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
      future = executor.submit(self._caller, request)
      try:
        yield future.result(timeout=self._timeout)
      except concurrent.futures.TimeoutError:
        raise UserCodeTimeoutException(
            f'Timeout {self._timeout} exceeded '
            f'while completing request: {request}')
      except RuntimeError:
        raise UserCodeExecutionException('could not complete request')

  def teardown(self):
    self._caller.__exit__(*sys.exc_info())
