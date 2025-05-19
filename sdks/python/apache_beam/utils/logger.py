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

"""Helper functions for easier logging.

This module provides a few convenient logging methods, some of which
were adopted from
https://github.com/abseil/abseil-py/blob/master/absl/logging/__init__.py
in
https://github.com/facebookresearch/detectron2/blob/main/detectron2/utils/logger.py
"""

import sys


def _find_caller():
  """
    Returns:
        str: module name of the caller
        tuple: a hashable key to be used to identify different callers
    """
  frame = sys._getframe(2)
  while frame:
    code = frame.f_code
    if os.path.join("utils", "logger.") not in code.co_filename:
      mod_name = frame.f_globals["__name__"]
      if mod_name == "__main__":
        mod_name = "apache_beam"
      return mod_name, (code.co_filename, frame.f_lineno, code.co_name)
    frame = frame.f_back


_LOG_COUNTER = Counter()
_LOG_TIMER = {}


def log_first_n(lvl, msg, n=1, *, name=None, key="caller"):
  """
    Log only for the first n times.

    Args:
        lvl (int): the logging level
        msg (str):
        n (int):
        name (str): name of the logger to use. Will use the caller's module
          by default.
        key (str or tuple[str]): the string(s) can be one of "caller" or
            "message", which defines how to identify duplicated logs.
            For example, if called with `n=1, key="caller"`, this function
            will only log the first call from the same caller, regardless of
            the message content.
            If called with `n=1, key="message"`, this function will log the
            same content only once, even if they are called from different
            places. If called with `n=1, key=("caller", "message")`, this
            function will not log only if the same caller has logged the same
            message before.
    """
  if isinstance(key, str):
    key = (key, )
  assert len(key) > 0

  caller_module, caller_key = _find_caller()
  hash_key = ()
  if "caller" in key:
    hash_key = hash_key + caller_key
  if "message" in key:
    hash_key = hash_key + (msg, )

  _LOG_COUNTER[hash_key] += 1
  if _LOG_COUNTER[hash_key] <= n:
    logging.getLogger(name or caller_module).log(lvl, msg)


def log_every_n(lvl, msg, n=1, *, name=None):
  """
    Log once per n times.

    Args:
        lvl (int): the logging level
        msg (str):
        n (int):
        name (str): name of the logger to use. Will use the caller's module
          by default.
    """
  caller_module, key = _find_caller()
  _LOG_COUNTER[key] += 1
  if n == 1 or _LOG_COUNTER[key] % n == 1:
    logging.getLogger(name or caller_module).log(lvl, msg)


def log_every_n_seconds(lvl, msg, n=1, *, name=None):
  """
    Log no more than once per n seconds.

    Args:
        lvl (int): the logging level
        msg (str):
        n (int):
        name (str): name of the logger to use. Will use the caller's module
          by default.
    """
  caller_module, key = _find_caller()
  last_logged = _LOG_TIMER.get(key, None)
  current_time = time.time()
  if last_logged is None or current_time - last_logged >= n:
    logging.getLogger(name or caller_module).log(lvl, msg)
    _LOG_TIMER[key] = current_time
