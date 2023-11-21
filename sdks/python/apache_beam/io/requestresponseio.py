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
import datetime
from datetime import timedelta
from typing import TypeVar
from typing import Generic

import apache_beam as beam
from apache_beam.pvalue import PCollection

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')

DEFAULT_TIMEOUT = timedelta(seconds=30)


class UserCodeExecutionException(Exception):
  """Base class for errors related to calling Web APIs."""


class UserCodeQuotaException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal specifically that
  the Web API client encountered a Quota or API overuse related error.
  """


class UserCodeTimeoutException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal a user code timeout."""


class Caller(metaclass=abc.ABCMeta):
  """Interfaces user custom code intended for API calls."""
  @abc.abstractmethod
  def __call__(self, request: RequestT, *args, **kwargs) -> ResponseT:
    """Calls a Web API with the ``RequestT``  and returns a
    ``ResponseT``. ``RequestResponseIO`` expects implementations of the
    ``__call__`` method to throw either a ``UserCodeExecutionException``,
    ``UserCodeQuotaException``, or ``UserCodeTimeoutException``.
    """
    pass


class SetupTeardown(metaclass=abc.ABCMeta):
  """Interfaces user custom code to set up and teardown the API clients.
    Called by ``RequestResponseIO`` as a context manager to instantiate
    and teardown clients.
    """
  def __enter__(self):
    """Instantiates a connection to the API client."""
    pass

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Closes the connection to the API client."""
    pass


class RequestResponseIO(beam.PTransform[beam.PCollection[RequestT],
                                        beam.PCollection[ResponseT]]):
  """A :class:`RequestResponseIO` transform to read and write to APIs.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of requests
  by making a call to the API as defined in :class:`Caller`'s `__call__`
  and returns a :class:`~apache_beam.pvalue.PCollection` of responses.

  Args:
    requests (~apache_beam.pvalue.PCollection):
      a :class:`~apache_beam.pvalue.PCollection` to be processed.
    caller (~apache_beam.io.requestresponseio.Caller): an implementation of
      `Caller` or a callable object that makes call to the API.
    setup_teardown (~apache_beam.io.requestresponseio.SetupTeardown):
      a context manager class implementing the `__enter__` and
      `__exit`__` methods to set up and teardown the API clients.
  """
  def __init__(self, caller: Caller, setup_teardown: SetupTeardown):
    self._caller = caller
    self._setup_teardown = setup_teardown

  def expand(self, requests: PCollection[RequestT]) -> PCollection[ResponseT]:
    pass


class _CallDoFn(beam.DoFn, Generic[RequestT, ResponseT]):
  def __init__(self, caller: Caller, timeout: datetime.datetime):
    self._caller = caller
    self._timeout = timeout

  def process(self, request, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor as executor:
      future = executor.submit(self._caller, request)
      try:
        yield future.result(timeout=self._timeout)
      except concurrent.futures.TimeoutError:
        raise UserCodeTimeoutException
      except RuntimeError:
        raise UserCodeExecutionException('could not complete request')


class _Call(beam.PTransform[beam.PCollection[RequestT],
                            beam.PCollection[ResponseT]]):
  def __init__(
      self,
      caller: Caller,
      setup_teardown: SetupTeardown,
      timeout: datetime.datetime):
    self._caller = caller
    self._setup_teardown = setup_teardown
    self._timeout = timeout

  def expand(
      self,
      requests: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    with self._setup_teardown():
      return requests | beam.ParDo(_CallDoFn(self._caller, self._timeout))


class _RequestResponseDoFn(beam.DoFn, Generic[RequestT, ResponseT]):
  def __init__(self, caller: Caller):
    self._caller = caller

  def process(self, element, *args, **kwargs):
    yield self._caller(element)
