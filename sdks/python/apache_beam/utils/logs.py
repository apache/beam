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

import inspect
import time

_LAST_TIME_INVOCATION_ALLOWED_NS = {}


def _allow_infrequent_invocation(
    min_interval_sec: int, periodic_action_id: str):

  if periodic_action_id in _LAST_TIME_INVOCATION_ALLOWED_NS:
    last_observed_ns = _LAST_TIME_INVOCATION_ALLOWED_NS[periodic_action_id]
    if time.time_ns() < last_observed_ns + min_interval_sec * 10**9:
      return False
  _LAST_TIME_INVOCATION_ALLOWED_NS[periodic_action_id] = time.time_ns()
  return True


def allow_infrequent_logging(
    min_interval_sec: int = 5 * 60, message_id: str = None):
  """Checks whether to allow printing a log message every so often.

  Sample usages:
  ```
  if logs.allow_infrequent_logging():
    _LOGGER.info("A message to log no more than once in 5 min per process")

  if logs.allow_infrequent_logging(min_interval_sec=20*60,
                                   message_id="Data plane debug logs"):
    _LOGGER.info("Waiting to receive elements in input queue.")

  if logs.allow_infrequent_logging(min_interval_sec=20*60,
                                   message_id="Data plane debug logs"):
    _LOGGER.info("Received elements in input queue.")

  ```

  Args:
    min_interval_sec: Minimal time interval to wait between logs, in seconds.
    message_id: Optional identifier of a log message. If not provided, the
      identifier is derived from the location of the caller.
      Do not include the message being logged if it can be large.

  Returns:
    True, if a log message should be produced, False otherwise.
  """
  if not message_id:
    # Use a location of the code where the helper was invoked.
    cf = inspect.currentframe()
    message_id = f"{cf.f_back.f_code.co_filename}:{cf.f_back.f_lineno}"

  return _allow_infrequent_invocation(min_interval_sec, message_id)


def allow_log_once(message_id: str = None):
  """Checks whether to allow logging a message only once per process.

  Args:
    message_id: An identifier of a log message. If not provided, the
      identifier is derived from the location of the caller.

  Returns:
    True, if a log message should be produced, False otherwise.

  Sample usage:
  ```
  if logs.allow_log_once():
    _LOGGER.info("Some message to log no more than once per process")
  ```

  See also `allow_infrequent_logging` for logging a message every so often.
  """
  if not message_id:
    # Use a location of the code where the helper was invoked.
    cf = inspect.currentframe()
    message_id = f"{cf.f_back.f_code.co_filename}:{cf.f_back.f_lineno}"

  return _allow_infrequent_invocation(
      min_interval_sec=10**10, periodic_action_id=message_id)
