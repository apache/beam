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
"""Beam fn API log handler."""

import logging
import math
import Queue as queue
import threading

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor

# This module is experimental. No backwards-compatibility guarantees.


class FnApiLogRecordHandler(logging.Handler):
  """A handler that writes log records to the fn API."""

  # Maximum number of log entries in a single stream request.
  _MAX_BATCH_SIZE = 1000
  # Used to indicate the end of stream.
  _FINISHED = object()

  # Mapping from logging levels to LogEntry levels.
  LOG_LEVEL_MAP = {
      logging.FATAL: beam_fn_api_pb2.LogEntry.Severity.CRITICAL,
      logging.ERROR: beam_fn_api_pb2.LogEntry.Severity.ERROR,
      logging.WARNING: beam_fn_api_pb2.LogEntry.Severity.WARN,
      logging.INFO: beam_fn_api_pb2.LogEntry.Severity.INFO,
      logging.DEBUG: beam_fn_api_pb2.LogEntry.Severity.DEBUG
  }

  def __init__(self, log_service_descriptor):
    super(FnApiLogRecordHandler, self).__init__()
    self._log_channel = grpc.intercept_channel(
        grpc.insecure_channel(log_service_descriptor.url),
        WorkerIdInterceptor())
    self._logging_stub = beam_fn_api_pb2_grpc.BeamFnLoggingStub(
        self._log_channel)
    self._log_entry_queue = queue.Queue()

    log_control_messages = self._logging_stub.Logging(self._write_log_entries())
    self._reader = threading.Thread(
        target=lambda: self._read_log_control_messages(log_control_messages),
        name='read_log_control_messages')
    self._reader.daemon = True
    self._reader.start()

  def emit(self, record):
    log_entry = beam_fn_api_pb2.LogEntry()
    log_entry.severity = self.LOG_LEVEL_MAP[record.levelno]
    log_entry.message = self.format(record)
    log_entry.thread = record.threadName
    log_entry.log_location = record.module + '.' + record.funcName
    (fraction, seconds) = math.modf(record.created)
    nanoseconds = 1e9 * fraction
    log_entry.timestamp.seconds = int(seconds)
    log_entry.timestamp.nanos = int(nanoseconds)
    self._log_entry_queue.put(log_entry)

  def close(self):
    """Flush out all existing log entries and unregister this handler."""
    # Acquiring the handler lock ensures ``emit`` is not run until the lock is
    # released.
    self.acquire()
    self._log_entry_queue.put(self._FINISHED)
    # wait on server to close.
    self._reader.join()
    self.release()
    # Unregister this handler.
    super(FnApiLogRecordHandler, self).close()

  def _write_log_entries(self):
    done = False
    while not done:
      log_entries = [self._log_entry_queue.get()]
      try:
        for _ in range(self._MAX_BATCH_SIZE):
          log_entries.append(self._log_entry_queue.get_nowait())
      except queue.Empty:
        pass
      if log_entries[-1] is self._FINISHED:
        done = True
        log_entries.pop()
      if log_entries:
        yield beam_fn_api_pb2.LogEntry.List(log_entries=log_entries)

  def _read_log_control_messages(self, log_control_iterator):
    # TODO(vikasrk): Handle control messages.
    for _ in log_control_iterator:
      pass
