# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Python Dataflow worker logging."""

import json
import logging
import threading
import traceback


# Per-thread worker information. This is used only for logging to set
# context information that changes while work items get executed:
# work_item_id, step_name, stage_name.
per_thread_worker_data = threading.local()


class PerThreadLoggingContext(object):
  """A context manager to add per thread attributes."""

  def __init__(self, *args, **kwargs):
    if args:
      raise ValueError(
          'PerThreadLoggingContext expects only keyword arguments.')
    self.kwargs = kwargs
    self.previous = {}

  def __enter__(self):
    for key in self.kwargs:
      if hasattr(per_thread_worker_data, key):
        self.previous[key] = getattr(per_thread_worker_data, key)
      setattr(per_thread_worker_data, key, self.kwargs[key])
    return self

  def __exit__(self, exn_type, exn_value, exn_traceback):
    for key in self.kwargs:
      if key in self.previous:
        setattr(per_thread_worker_data, key, self.previous[key])
      else:
        delattr(per_thread_worker_data, key)


class JsonLogFormatter(logging.Formatter):
  """A JSON formatter class as expected by the logging standard module."""

  def __init__(self, job_id, worker_id):
    super(JsonLogFormatter, self).__init__()
    self.job_id = job_id
    self.worker_id = worker_id

  def format(self, record):
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
      msg: Logging message containing formatting instructions. This is the first
        argument of a log call.
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
    output = {}
    output['timestamp'] = {
        'seconds': int(record.created),
        'nanos': int(record.msecs * 1000000)}
    # ERROR. INFO, DEBUG log levels translate into the same for severity
    # property. WARNING becomes WARN.
    output['severity'] = (
        record.levelname if record.levelname != 'WARNING' else 'WARN')
    # Prepare the actual message using the message formatting string and the
    # positional arguments as they have been used in the log call.
    output['message'] = record.msg % record.args
    # The thread ID is logged as a combination of the process ID and thread ID
    # since workers can run in multiple processes.
    output['thread'] = '%s:%s' % (record.process, record.thread)
    # job ID and worker ID. These do not change during the lifetime of a worker.
    output['job'] = self.job_id
    output['worker'] = self.worker_id
    # Stage, step and work item ID come from thread local storage since they
    # change with every new work item leased for execution. If there is no
    # work item ID then we make sure the step is undefined too.
    if hasattr(per_thread_worker_data, 'work_item_id'):
      output['work'] = getattr(per_thread_worker_data, 'work_item_id')
    if hasattr(per_thread_worker_data, 'stage_name'):
      output['stage'] = getattr(per_thread_worker_data, 'stage_name')
    if hasattr(per_thread_worker_data, 'step_name'):
      output['step'] = getattr(per_thread_worker_data, 'step_name')
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
  """Initialize root logger so that we log JSON to a file and text to stdout."""

  file_handler = logging.FileHandler(log_path)
  file_handler.setFormatter(JsonLogFormatter(job_id, worker_id))
  logging.getLogger().addHandler(file_handler)

  # Set default level to INFO to avoid logging various DEBUG level log calls
  # sprinkled throughout the code.
  logging.getLogger().setLevel(logging.INFO)
