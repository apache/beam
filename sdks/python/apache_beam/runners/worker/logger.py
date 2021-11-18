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

# cython: language_level=3

"""Python worker logging."""

# pytype: skip-file
# mypy: disallow-untyped-defs

import contextlib
import json
import logging
import threading
import traceback
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List

from apache_beam.runners.worker import statesampler

# This module is experimental. No backwards-compatibility guarantees.


# Per-thread worker information. This is used only for logging to set
# context information that changes while work items get executed:
# work_item_id, step_name, stage_name.
class _PerThreadWorkerData(threading.local):
  def __init__(self):
    # type: () -> None
    super().__init__()
    # in the list, as going up and down all the way to zero incurs several
    # reallocations.
    self.stack = []  # type: List[Dict[str, Any]]

  def get_data(self):
    # type: () -> Dict[str, Any]
    all_data = {}
    for datum in self.stack:
      all_data.update(datum)
    return all_data


per_thread_worker_data = _PerThreadWorkerData()


@contextlib.contextmanager
def PerThreadLoggingContext(**kwargs):
  # type: (**Any) -> Iterator[None]

  """A context manager to add per thread attributes."""
  stack = per_thread_worker_data.stack
  stack.append(kwargs)
  yield
  stack.pop()


class JsonLogFormatter(logging.Formatter):
  """A JSON formatter class as expected by the logging standard module."""
  def __init__(self, job_id, worker_id):
    # type: (str, str) -> None
    super().__init__()
    self.job_id = job_id
    self.worker_id = worker_id

  def format(self, record):
    # type: (logging.LogRecord) -> str

    """Returns a JSON string based on a LogRecord instance.

    Args:
      record: A LogRecord instance. See below for details.

    Returns:
      A JSON string representing the record.

    A LogRecord instance has the following attributes and is used for
    formatting the final message.

    Attributes:
      created: A double representing the timestamp for record creation
        (e.g., 1438365207.624597). Note that the number contains also msecs and
        microsecs information. Part of this is also available in the 'msecs'
        attribute.
      msecs: A double representing the msecs part of the record creation
        (e.g., 624.5970726013184).
      msg: Logging message containing formatting instructions or an arbitrary
        object. This is the first argument of a log call.
      args: A tuple containing the positional arguments for the logging call.
      levelname: A string. Possible values are: INFO, WARNING, ERROR, etc.
      exc_info: None or a 3-tuple with exception information as it is
        returned by a call to sys.exc_info().
      name: Logger's name. Most logging is done using the default root logger
        and therefore the name will be 'root'.
      filename: Basename of the file where logging occurred.
      funcName: Name of the function where logging occurred.
      process: The PID of the process running the worker.
      thread:  An id for the thread where the record was logged. This is not a
        real TID (the one provided by OS) but rather the id (address) of a
        Python thread object. Nevertheless having this value can allow to
        filter log statement from only one specific thread.
    """
    output = {}  # type: Dict[str, Any]
    output['timestamp'] = {
        'seconds': int(record.created), 'nanos': int(record.msecs * 1000000)
    }
    # ERROR. INFO, DEBUG log levels translate into the same for severity
    # property. WARNING becomes WARN.
    output['severity'] = (
        record.levelname if record.levelname != 'WARNING' else 'WARN')

    # msg could be an arbitrary object, convert it to a string first.
    record_msg = str(record.msg)

    # Prepare the actual message using the message formatting string and the
    # positional arguments as they have been used in the log call.
    if record.args:
      try:
        output['message'] = record_msg % record.args
      except (TypeError, ValueError):
        output['message'] = '%s with args (%s)' % (record_msg, record.args)
    else:
      output['message'] = record_msg

    # The thread ID is logged as a combination of the process ID and thread ID
    # since workers can run in multiple processes.
    output['thread'] = '%s:%s' % (record.process, record.thread)
    # job ID and worker ID. These do not change during the lifetime of a worker.
    output['job'] = self.job_id
    output['worker'] = self.worker_id
    # Stage, step and work item ID come from thread local storage since they
    # change with every new work item leased for execution. If there is no
    # work item ID then we make sure the step is undefined too.
    data = per_thread_worker_data.get_data()
    if 'work_item_id' in data:
      output['work'] = data['work_item_id']

    tracker = statesampler.get_current_tracker()
    if tracker:
      output['stage'] = tracker.stage_name

      if tracker.current_state() and tracker.current_state().name_context:
        output['step'] = tracker.current_state().name_context.logging_name()

    # All logging happens using the root logger. We will add the basename of the
    # file and the function name where the logging happened to make it easier
    # to identify who generated the record.
    output['logger'] = '%s:%s:%s' % (
        record.name, record.filename, record.funcName)
    # Add exception information if any is available.
    if record.exc_info:
      output['exception'] = ''.join(
          traceback.format_exception(*record.exc_info))

    return json.dumps(output)


def initialize(job_id, worker_id, log_path):
  # type: (str, str, str) -> None

  """Initialize root logger so that we log JSON to a file and text to stdout."""

  file_handler = logging.FileHandler(log_path)
  file_handler.setFormatter(JsonLogFormatter(job_id, worker_id))
  logging.getLogger().addHandler(file_handler)

  # Set default level to INFO to avoid logging various DEBUG level log calls
  # sprinkled throughout the code.
  logging.getLogger().setLevel(logging.INFO)
