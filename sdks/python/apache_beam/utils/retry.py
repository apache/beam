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

"""Retry decorators for calls raising exceptions.

For internal use only; no backwards-compatibility guarantees.

This module is used mostly to decorate all integration points where the code
makes calls to remote services. Searching through the code base for @retry
should find all such places. For this reason even places where retry is not
needed right now use a @retry.no_retries decorator.
"""

# pytype: skip-file

from __future__ import absolute_import

import functools
import logging
import random
import sys
import time
import traceback
from builtins import next
from builtins import object
from builtins import range

from future.utils import raise_with_traceback

from apache_beam.io.filesystem import BeamIOError

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
# TODO(sourabhbajaj): Remove the GCP specific error code to a submodule
try:
  from apitools.base.py.exceptions import HttpError
except ImportError as e:
  HttpError = None

# Protect against environments where aws tools are not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.io.aws.clients.s3 import messages as _s3messages
except ImportError:
  S3ClientError = None
else:
  S3ClientError = _s3messages.S3ClientError
# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)


class PermanentException(Exception):
  """Base class for exceptions that should not be retried."""
  pass


class FuzzedExponentialIntervals(object):
  """Iterable for intervals that are exponentially spaced, with fuzzing.

  On iteration, yields retry interval lengths, in seconds. Every iteration over
  this iterable will yield differently fuzzed interval lengths, as long as fuzz
  is nonzero.

  Args:
    initial_delay_secs: The delay before the first retry, in seconds.
    num_retries: The total number of times to retry.
    factor: The exponential factor to use on subsequent retries.
      Default is 2 (doubling).
    fuzz: A value between 0 and 1, indicating the fraction of fuzz. For a
      given delay d, the fuzzed delay is randomly chosen between
      [(1 - fuzz) * d, d].
    max_delay_secs: Maximum delay (in seconds). After this limit is reached,
      further tries use max_delay_sec instead of exponentially increasing
      the time. Defaults to 1 hour.
  """
  def __init__(
      self,
      initial_delay_secs,
      num_retries,
      factor=2,
      fuzz=0.5,
      max_delay_secs=60 * 60 * 1):
    self._initial_delay_secs = initial_delay_secs
    if num_retries > 10000:
      raise ValueError('num_retries parameter cannot exceed 10000.')
    self._num_retries = num_retries
    self._factor = factor
    if not 0 <= fuzz <= 1:
      raise ValueError('fuzz parameter expected to be in [0, 1] range.')
    self._fuzz = fuzz
    self._max_delay_secs = max_delay_secs

  def __iter__(self):
    current_delay_secs = min(self._max_delay_secs, self._initial_delay_secs)
    for _ in range(self._num_retries):
      fuzz_multiplier = 1 - self._fuzz + random.random() * self._fuzz
      yield current_delay_secs * fuzz_multiplier
      current_delay_secs = min(
          self._max_delay_secs, current_delay_secs * self._factor)


def retry_on_server_errors_filter(exception):
  """Filter allowing retries on server errors and non-HttpErrors."""
  if (HttpError is not None) and isinstance(exception, HttpError):
    return exception.status_code >= 500
  if (S3ClientError is not None) and isinstance(exception, S3ClientError):
    return exception.code >= 500
  return not isinstance(exception, PermanentException)


# TODO(BEAM-6202): Dataflow returns 404 for job ids that actually exist.
# Retry on those errors.
def retry_on_server_errors_and_notfound_filter(exception):
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 404:  # 404 Not Found
      return True
  return retry_on_server_errors_filter(exception)


def retry_on_server_errors_and_timeout_filter(exception):
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 408:  # 408 Request Timeout
      return True
  if S3ClientError is not None and isinstance(exception, S3ClientError):
    if exception.code == 408:  # 408 Request Timeout
      return True
  return retry_on_server_errors_filter(exception)


def retry_on_server_errors_timeout_or_quota_issues_filter(exception):
  """Retry on server, timeout and 403 errors.

  403 errors can be accessDenied, billingNotEnabled, and also quotaExceeded,
  rateLimitExceeded."""
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 403:
      return True
  if S3ClientError is not None and isinstance(exception, S3ClientError):
    if exception.code == 403:
      return True
  return retry_on_server_errors_and_timeout_filter(exception)


def retry_on_beam_io_error_filter(exception):
  """Filter allowing retries on Beam IO errors."""
  return isinstance(exception, BeamIOError)


def retry_if_valid_input_but_server_error_and_timeout_filter(exception):
  if isinstance(exception, ValueError):
    return False
  return retry.retry_on_server_errors_and_timeout_filter(exception)


SERVER_ERROR_OR_TIMEOUT_CODES = [408, 500, 502, 503, 504, 598, 599]


class Clock(object):
  """A simple clock implementing sleep()."""
  def sleep(self, value):
    time.sleep(value)


def no_retries(fun):
  """A retry decorator for places where we do not want retries."""
  return with_exponential_backoff(retry_filter=lambda _: False, clock=None)(fun)


def with_exponential_backoff(
    num_retries=7,
    initial_delay_secs=5.0,
    logger=_LOGGER.warning,
    retry_filter=retry_on_server_errors_filter,
    clock=Clock(),
    fuzz=True,
    factor=2,
    max_delay_secs=60 * 60):
  """Decorator with arguments that control the retry logic.

  Args:
    num_retries: The total number of times to retry.
    initial_delay_secs: The delay before the first retry, in seconds.
    logger: A callable used to report an exception. Must have the same signature
      as functions in the standard logging module. The default is
      _LOGGER.warning.
    retry_filter: A callable getting the exception raised and returning True
      if the retry should happen. For instance we do not want to retry on
      404 Http errors most of the time. The default value will return true
      for server errors (HTTP status code >= 500) and non Http errors.
    clock: A clock object implementing a sleep method. The default clock will
      use time.sleep().
    fuzz: True if the delay should be fuzzed (default). During testing False
      can be used so that the delays are not randomized.
    factor: The exponential factor to use on subsequent retries.
      Default is 2 (doubling).
    max_delay_secs: Maximum delay (in seconds). After this limit is reached,
      further tries use max_delay_sec instead of exponentially increasing
      the time. Defaults to 1 hour.

  Returns:
    As per Python decorators with arguments pattern returns a decorator
    for the function which in turn will return the wrapped (decorated) function.

  The decorator is intended to be used on callables that make HTTP or RPC
  requests that can temporarily timeout or have transient errors. For instance
  the make_http_request() call below will be retried 16 times with exponential
  backoff and fuzzing of the delay interval (default settings).

  from apache_beam.utils import retry
  # ...
  @retry.with_exponential_backoff()
  make_http_request(args)
  """
  def real_decorator(fun):
    """The real decorator whose purpose is to return the wrapped function."""
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
      retry_intervals = iter(
          FuzzedExponentialIntervals(
              initial_delay_secs,
              num_retries,
              factor,
              fuzz=0.5 if fuzz else 0,
              max_delay_secs=max_delay_secs))
      while True:
        try:
          return fun(*args, **kwargs)
        except Exception as exn:  # pylint: disable=broad-except
          if not retry_filter(exn):
            raise
          # Get the traceback object for the current exception. The
          # sys.exc_info() function returns a tuple with three elements:
          # exception type, exception value, and exception traceback.
          exn_traceback = sys.exc_info()[2]
          try:
            try:
              sleep_interval = next(retry_intervals)
            except StopIteration:
              # Re-raise the original exception since we finished the retries.
              raise_with_traceback(exn, exn_traceback)

            logger(
                'Retry with exponential backoff: waiting for %s seconds before '
                'retrying %s because we caught exception: %s '
                'Traceback for above exception (most recent call last):\n%s',
                sleep_interval,
                getattr(fun, '__name__', str(fun)),
                ''.join(traceback.format_exception_only(exn.__class__, exn)),
                ''.join(traceback.format_tb(exn_traceback)))
            clock.sleep(sleep_interval)
          finally:
            # Traceback objects in locals can cause reference cycles that will
            # prevent garbage collection. Clear it now since we do not need
            # it anymore.
            exn_traceback = None

    return wrapper

  return real_decorator
