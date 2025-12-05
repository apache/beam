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

import functools
import json
import logging
import random
import sys
import time
import traceback

from apache_beam.io.filesystem import BeamIOError

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
# TODO(sourabhbajaj): Remove the GCP specific error code to a submodule
try:
  from apitools.base.py.exceptions import HttpError
  from google.api_core.exceptions import GoogleAPICallError
except ImportError as e:
  HttpError = None
  GoogleAPICallError = None  # type: ignore

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
_RETRYABLE_REASONS = ["rateLimitExceeded", "internalError", "backendError"]


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
    stop_after_secs: Places a limit on the sum of intervals returned (in
      seconds), such that the sum is <= stop_after_secs. Defaults to disabled
      (None). You may need to increase num_retries to effectively use this
      feature.
  """
  def __init__(
      self,
      initial_delay_secs,
      num_retries,
      factor=2,
      fuzz=0.5,
      max_delay_secs=60 * 60 * 1,
      stop_after_secs=None):
    self._initial_delay_secs = initial_delay_secs
    if num_retries > 10000:
      raise ValueError('num_retries parameter cannot exceed 10000.')
    self._num_retries = num_retries
    self._factor = factor
    if not 0 <= fuzz <= 1:
      raise ValueError('fuzz parameter expected to be in [0, 1] range.')
    self._fuzz = fuzz
    self._max_delay_secs = max_delay_secs
    self._stop_after_secs = stop_after_secs

  def __iter__(self):
    current_delay_secs = min(self._max_delay_secs, self._initial_delay_secs)
    total_delay_secs = 0
    for _ in range(self._num_retries):
      fuzz_multiplier = 1 - self._fuzz + random.random() * self._fuzz
      delay_secs = current_delay_secs * fuzz_multiplier
      total_delay_secs += delay_secs
      if (self._stop_after_secs is not None and
          total_delay_secs > self._stop_after_secs):
        break
      yield delay_secs
      current_delay_secs = min(
          self._max_delay_secs, current_delay_secs * self._factor)


def retry_on_server_errors_filter(exception):
  """Filter allowing retries on server errors and non-HttpErrors."""
  if (HttpError is not None) and isinstance(exception, HttpError):
    return exception.status_code >= 500
  if GoogleAPICallError is not None and isinstance(exception,
                                                   GoogleAPICallError):
    if exception.code >= 500:  # 500 are internal server errors
      return True
    else:
      # If we have a GoogleAPICallError with a code that doesn't
      # indicate a server error, we do not need to retry.
      return False
  if (S3ClientError is not None) and isinstance(exception, S3ClientError):
    return exception.code is None or exception.code >= 500
  return not isinstance(exception, PermanentException)


# TODO(https://github.com/apache/beam/issues/19350): Dataflow returns 404 for
# job ids that actually exist. Retry on those errors.
def retry_on_server_errors_and_notfound_filter(exception):
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 404:  # 404 Not Found
      return True
  if GoogleAPICallError is not None and isinstance(exception,
                                                   GoogleAPICallError):
    if exception.code == 404:  # 404 Not found
      return True
  return retry_on_server_errors_filter(exception)


def retry_on_server_errors_and_timeout_filter(exception):
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 408:  # 408 Request Timeout
      return True
  if GoogleAPICallError is not None and isinstance(exception,
                                                   GoogleAPICallError):
    if exception.code == 408:  # 408 Request Timeout
      return True
  if S3ClientError is not None and isinstance(exception, S3ClientError):
    if exception.code == 408:  # 408 Request Timeout
      return True
  return retry_on_server_errors_filter(exception)


def retry_on_server_errors_timeout_or_quota_issues_filter(exception):
  """Retry on server, timeout, 429, and some 403 errors.

  403 errors from BigQuery include both non-transient (accessDenied,
  billingNotEnabled) and transient errors (rateLimitExceeded).
  Only retry transient errors."""
  if HttpError is not None and isinstance(exception, HttpError):
    if exception.status_code == 429:
      return True
    if exception.status_code == 403:
      try:
        # attempt to extract the reason and check if it's retryable
        content = exception.content
        if not isinstance(content, dict):
          content = json.loads(exception.content)
        return content["error"]["errors"][0]["reason"] in _RETRYABLE_REASONS
      except (KeyError, IndexError, TypeError) as e:
        _LOGGER.warning(
            "Could not determine if HttpError is transient. "
            "Will not retry: %s",
            e)
      return False
  if GoogleAPICallError is not None and isinstance(exception,
                                                   GoogleAPICallError):
    if exception.code == 429:
      return True
    if exception.code == 403:
      if not hasattr(exception, "errors") or len(exception.errors) == 0:
        # default to not retrying
        return False

      reason = exception.errors[0]["reason"]
      return reason in _RETRYABLE_REASONS
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
  return retry_on_server_errors_and_timeout_filter(exception)


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
    max_delay_secs=60 * 60,
    stop_after_secs=None):
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
    stop_after_secs: Places a limit on the sum of delays between retries, such
      that the sum is <= stop_after_secs. Retries will stop after the limit is
      reached. Defaults to disabled (None). You may need to increase num_retries
      to effectively use this feature.

  Returns:
    As per Python decorators with arguments pattern returns a decorator
    for the function which in turn will return the wrapped (decorated) function.

  The decorator is intended to be used on callables that make HTTP or RPC
  requests that can temporarily timeout or have transient errors. For instance
  the make_http_request() call below will be retried 16 times with exponential
  backoff and fuzzing of the delay interval (default settings). The callable
  should return values directly instead of yielding them, as generators are not
  evaluated within the try-catch block and will not be retried on exception.

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
              max_delay_secs=max_delay_secs,
              stop_after_secs=stop_after_secs))
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
              raise exn.with_traceback(exn_traceback)

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
