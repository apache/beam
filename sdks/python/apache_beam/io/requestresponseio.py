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

"""``PTransform`` for reading from and writing to Web APIs.

``RequestResponseIO`` minimally requires implementing the ``Caller`` interface:

    requests = ...

    responses = (requests
                 | RequestResponseIO(MyCaller())
                )
"""
import abc
from typing import TypeVar

from apache_beam.transforms import PTransform
from apache_beam import PCollection

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')


# TODO(damondouglas,riteshghorse): https://github.com/apache/beam/issues/28934
class RequestResponseIO(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from and writing to Web APIs.

  ``RequestResponseIO`` minimally requires implementing the ``Caller``:

    class MyCaller(Caller):
        def call(self, request: SomeRequest) -> SomeResponse:
            # invoke an API client with SomeRequest to retrieve SomeResponse

    requests = ... # PCollection of SomeRequest
    responses = (requests
                 | RequestResponseIO(MyCaller())
                )
  """
  def expand(self, requests: PCollection[RequestT]) -> PCollection[ResponseT]:
    pass


class Caller(metaclass=abc.ABCMeta):
  """Interfaces user custom code intended for API calls."""
  @abc.abstractmethod
  def call(self, request: RequestT) -> ResponseT:
    """Calls a Web API with the ``RequestT``  and returns a
        ``ResponseT``. ``RequestResponseIO`` expects implementations of the
        call method to throw either a ``UserCodeExecutionException``,
        ``UserCodeQuotaException``, or ``UserCodeTimeoutException``.
        """
    pass


class SetupTeardown(metaclass=abc.ABCMeta):
  """Interfaces user custom code to setup and teardown the API clients.
    Called by ``RequestResponseIO`` within its DoFn's setup and teardown
    methods.
    """
  @abc.abstractmethod
  def setup(self) -> None:
    """Called during the DoFn's setup lifecycle method."""
    pass

  @abc.abstractmethod
  def teardown(self) -> None:
    """Called during the DoFn's teardown lifecycle method."""
    pass


class UserCodeExecutionException(Exception):
  """Base class for errors related to calling Web APIs."""


class UserCodeQuotaException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal specifically that
  the Web API client encountered a Quota or API overuse related error.
  """


class UserCodeTimeoutException(UserCodeExecutionException):
  """Extends ``UserCodeExecutionException`` to signal a user code timeout."""
