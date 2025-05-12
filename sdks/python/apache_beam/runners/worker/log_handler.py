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

# pytype: skip-file
# mypy: disallow-untyped-defs

import logging
import math
import queue
import sys
import threading
import time
import traceback
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Union
from typing import cast

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor
from apache_beam.utils.sentinel import Sentinel

# Mapping from logging levels to LogEntry levels.
LOG_LEVEL_TO_LOGENTRY_MAP = {
    logging.FATAL: beam_fn_api_pb2.LogEntry.Severity.CRITICAL,
    logging.ERROR: beam_fn_api_pb2.LogEntry.Severity.ERROR,
    logging.WARNING: beam_fn_api_pb2.LogEntry.Severity.WARN,
    logging.INFO: beam_fn_api_pb2.LogEntry.Severity.INFO,
    logging.DEBUG: beam_fn_api_pb2.LogEntry.Severity.DEBUG,
    logging.NOTSET: beam_fn_api_pb2.LogEntry.Severity.UNSPECIFIED,
    -float('inf'): beam_fn_api_pb2.LogEntry.Severity.DEBUG,
}

# Mapping from LogEntry levels to logging levels
LOGENTRY_TO_LOG_LEVEL_MAP = {
    beam_fn_api_pb2.LogEntry.Severity.CRITICAL: logging.CRITICAL,
    beam_fn_api_pb2.LogEntry.Severity.ERROR: logging.ERROR,
    beam_fn_api_pb2.LogEntry.Severity.WARN: logging.WARNING,
    beam_fn_api_pb2.LogEntry.Severity.NOTICE: logging.INFO + 1,
    beam_fn_api_pb2.LogEntry.Severity.INFO: logging.INFO,
    beam_fn_api_pb2.LogEntry.Severity.DEBUG: logging.DEBUG,
    beam_fn_api_pb2.LogEntry.Severity.TRACE: logging.DEBUG - 1,
    beam_fn_api_pb2.LogEntry.Severity.UNSPECIFIED: logging.NOTSET,
}


class FnApiLogRecordHandler(logging.Handler):
  """A handler that writes log records to the fn API."""

  # Maximum number of log entries in a single stream request.
  _MAX_BATCH_SIZE = 1000
  # Used to indicate the end of stream.
  _FINISHED = Sentinel.sentinel
  # Size of the queue used to buffer messages. Once full, messages will be
  # dropped. If the average log size is 1KB this may use up to 10MB of memory.
  _QUEUE_SIZE = 10000

  def __init__(
      self, log_service_descriptor: endpoints_pb2.ApiServiceDescriptor) -> None:
    super().__init__()

    self._alive = True
    self._dropped_logs = 0
    self._log_entry_queue: queue.Queue[Union[beam_fn_api_pb2.LogEntry,
                                             Sentinel]] = queue.Queue(
                                                 maxsize=self._QUEUE_SIZE)

    ch = GRPCChannelFactory.insecure_channel(log_service_descriptor.url)
    # Make sure the channel is ready to avoid [BEAM-4649]
    grpc.channel_ready_future(ch).result(timeout=60)
    self._log_channel = grpc.intercept_channel(ch, WorkerIdInterceptor())
    self._reader = threading.Thread(
        target=lambda: self._read_log_control_messages(),
        name='read_log_control_messages')
    self._reader.daemon = True
    self._reader.start()

  def connect(self) -> Iterable:
    if hasattr(self, '_logging_stub'):
      del self._logging_stub  # type: ignore[has-type]
    self._logging_stub = beam_fn_api_pb2_grpc.BeamFnLoggingStub(
        self._log_channel)
    return self._logging_stub.Logging(self._write_log_entries())

  def map_log_level(
      self, level: int) -> beam_fn_api_pb2.LogEntry.Severity.Enum.ValueType:
    try:
      return LOG_LEVEL_TO_LOGENTRY_MAP[level]
    except KeyError:
      return max(
          beam_level
          for python_level, beam_level in LOG_LEVEL_TO_LOGENTRY_MAP.items()
          if python_level <= level)

  def emit(self, record: logging.LogRecord) -> None:
    log_entry = beam_fn_api_pb2.LogEntry()
    log_entry.severity = self.map_log_level(record.levelno)
    try:
      log_entry.message = self.format(record)
    except Exception:
      # record.msg could be an arbitrary object, convert it to a string first.
      log_entry.message = (
          "Failed to format '%s' with args '%s' during logging." %
          (str(record.msg), record.args))
    log_entry.thread = record.threadName  # type: ignore[assignment]
    log_entry.log_location = '%s:%s' % (
        record.pathname or record.module, record.lineno or record.funcName)
    (fraction, seconds) = math.modf(record.created)
    nanoseconds = 1e9 * fraction
    log_entry.timestamp.seconds = int(seconds)
    log_entry.timestamp.nanos = int(nanoseconds)
    if record.exc_info:
      log_entry.trace = ''.join(traceback.format_exception(*record.exc_info))
    instruction_id = statesampler.get_current_instruction_id()
    if instruction_id:
      log_entry.instruction_id = instruction_id
    tracker = statesampler.get_current_tracker()
    if tracker:
      current_state = tracker.current_state()
      if (current_state and current_state.name_context and
          current_state.name_context.transform_id):
        log_entry.transform_id = current_state.name_context.transform_id

    try:
      self._log_entry_queue.put(log_entry, block=False)
    except queue.Full:
      self._dropped_logs += 1

  def close(self) -> None:
    """Flush out all existing log entries and unregister this handler."""
    try:
      self._alive = False
      # Acquiring the handler lock ensures ``emit`` is not run until the lock is
      # released.
      self.acquire()
      self._log_entry_queue.put(self._FINISHED, timeout=5)
      # wait on server to close.
      self._reader.join()
      self.release()
      # Unregister this handler.
      super().close()
    except Exception:
      # Log rather than raising exceptions, to avoid clobbering
      # underlying errors that may have caused this to close
      # prematurely.
      logging.error("Error closing the logging channel.", exc_info=True)

  def _write_log_entries(self) -> Iterator[beam_fn_api_pb2.LogEntry.List]:
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
        # typing: log_entries was initialized as List[Union[..., Sentinel]],
        # but now that we've popped the sentinel out (above) we can safely cast
        yield beam_fn_api_pb2.LogEntry.List(
            log_entries=cast(List[beam_fn_api_pb2.LogEntry], log_entries))

  def _read_log_control_messages(self) -> None:
    # Only reconnect when we are alive.
    # We can drop some logs in the unlikely event of logging connection
    # dropped(not closed) during termination when we still have logs to be sent.
    # This case is unlikely and the chance of reconnection and successful
    # transmission of logs is also very less as the process is terminating.
    # I choose not to handle this case to avoid un-necessary code complexity.

    alive = True  # Force at least one connection attempt.
    while alive:
      # Loop for reconnection.
      log_control_iterator = self.connect()
      if self._dropped_logs > 0:
        logging.warning(
            "Dropped %d logs while logging client disconnected",
            self._dropped_logs)
        self._dropped_logs = 0
      try:
        for _ in log_control_iterator:
          # Loop for consuming messages from server.
          # TODO(vikasrk): Handle control messages.
          pass
        # iterator is closed
        return
      except Exception as ex:
        print(
            "Logging client failed: {}... resetting".format(ex),
            file=sys.stderr)
        # Wait a bit before trying a reconnect
        time.sleep(0.5)  # 0.5 seconds
      alive = self._alive
