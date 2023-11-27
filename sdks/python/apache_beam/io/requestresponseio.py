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
from typing import TypeVar

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')


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
