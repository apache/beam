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

"""Implementation of ``DataChannel``s to communicate across the data plane."""

# pytype: skip-file
# mypy: disallow-untyped-defs

import abc
import collections
import json
import logging
import queue
import threading
import time
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Collection
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import grpc

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor

if TYPE_CHECKING:
  import apache_beam.coders.slow_stream

  OutputStream = apache_beam.coders.slow_stream.OutputStream
  DataOrTimers = Union[beam_fn_api_pb2.Elements.Data,
                       beam_fn_api_pb2.Elements.Timers]
else:
  OutputStream = type(coder_impl.create_OutputStream())

_LOGGER = logging.getLogger(__name__)

_DEFAULT_SIZE_FLUSH_THRESHOLD = 10 << 20  # 10MB
_DEFAULT_TIME_FLUSH_THRESHOLD_MS = 0  # disable time-based flush by default
_FLUSH_MAX_SIZE = (2 << 30) - 100  # 2GB less some overhead, protobuf/grpc limit
# Keep a set of completed instructions to discard late received data. The set
# can have up to _MAX_CLEANED_INSTRUCTIONS items. See _GrpcDataChannel.
_MAX_CLEANED_INSTRUCTIONS = 10000

# retry on transient UNAVAILABLE grpc error from data channels.
_GRPC_SERVICE_CONFIG = json.dumps({
    "methodConfig": [{
        "name": [{
            "service": "org.apache.beam.model.fn_execution.v1.BeamFnData"
        }],
        "retryPolicy": {
            "maxAttempts": 5,
            "initialBackoff": "0.1s",
            "maxBackoff": "5s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE"],
        },
    }]
})


class ClosableOutputStream(OutputStream):
  """A Outputstream for use with CoderImpls that has a close() method."""
  def __init__(
      self,
      close_callback=None  # type: Optional[Callable[[bytes], None]]
  ):
    # type: (...) -> None
    super().__init__()
    self._close_callback = close_callback

  def close(self):
    # type: () -> None
    if self._close_callback:
      self._close_callback(self.get())

  def maybe_flush(self):
    # type: () -> None
    pass

  def flush(self):
    # type: () -> None
    pass

  @staticmethod
  def create(
      close_callback,  # type: Optional[Callable[[bytes], None]]
      flush_callback,  # type: Optional[Callable[[bytes], None]]
      data_buffer_time_limit_ms  # type: int
  ):
    # type: (...) -> ClosableOutputStream
    if data_buffer_time_limit_ms > 0:
      return TimeBasedBufferingClosableOutputStream(
          close_callback,
          flush_callback=flush_callback,
          time_flush_threshold_ms=data_buffer_time_limit_ms)
    else:
      return SizeBasedBufferingClosableOutputStream(
          close_callback, flush_callback=flush_callback)


class SizeBasedBufferingClosableOutputStream(ClosableOutputStream):
  """A size-based buffering OutputStream."""

  _large_flush_last_observed_timestamp = 0.0

  def __init__(
      self,
      close_callback=None,  # type: Optional[Callable[[bytes], None]]
      flush_callback=None,  # type: Optional[Callable[[bytes], None]]
      size_flush_threshold=_DEFAULT_SIZE_FLUSH_THRESHOLD,  # type: int
      large_buffer_warn_threshold_bytes=512 << 20  # type: int
  ):
    super().__init__(close_callback)
    self._flush_callback = flush_callback
    self._size_flush_threshold = size_flush_threshold
    self._large_buffer_warn_threshold_bytes = large_buffer_warn_threshold_bytes

  # This must be called explicitly to avoid flushing partial elements.
  def maybe_flush(self):
    # type: () -> None
    if self.size() > self._size_flush_threshold:
      self.flush()

  def flush(self):
    # type: () -> None
    if self._flush_callback:
      size = self.size()
      if (self._large_buffer_warn_threshold_bytes and
          size > self._large_buffer_warn_threshold_bytes):
        if size > _FLUSH_MAX_SIZE:
          raise ValueError(
              f'Buffer size {size} exceeds GRPC limit {_FLUSH_MAX_SIZE}. '
              'This is likely due to a single element that is too large. '
              'To resolve, prefer multiple small elements over single large '
              'elements in PCollections. If needed, store large blobs in '
              'external storage systems, and use PCollections to pass their '
              'metadata, or use a custom coder that reduces the element\'s '
              'size.')

        if self._large_flush_last_observed_timestamp + 600 < time.time():
          self._large_flush_last_observed_timestamp = time.time()
          _LOGGER.warning(
              'Data output stream buffer size %s exceeds %s bytes. '
              'This is likely due to a large element in a PCollection. '
              'Large elements increase pipeline RAM requirements and '
              'can cause runtime errors. '
              'Prefer multiple small elements over single large elements '
              'in PCollections. If needed, store large blobs in external '
              'storage systems, and use PCollections to pass their metadata, '
              'or use a custom coder that reduces the element\'s size.',
              size,
              self._large_buffer_warn_threshold_bytes)

      self._flush_callback(self.get())
      self._clear()


class TimeBasedBufferingClosableOutputStream(
    SizeBasedBufferingClosableOutputStream):
  """A buffering OutputStream with both time-based and size-based."""
  _periodic_flusher = None  # type: Optional[PeriodicThread]

  def __init__(
      self,
      close_callback=None,  # type: Optional[Callable[[bytes], None]]
      flush_callback=None,  # type: Optional[Callable[[bytes], None]]
      size_flush_threshold=_DEFAULT_SIZE_FLUSH_THRESHOLD,  # type: int
      time_flush_threshold_ms=_DEFAULT_TIME_FLUSH_THRESHOLD_MS  # type: int
  ):
    # type: (...) -> None
    super().__init__(close_callback, flush_callback, size_flush_threshold)
    assert time_flush_threshold_ms > 0
    self._time_flush_threshold_ms = time_flush_threshold_ms
    self._flush_lock = threading.Lock()
    self._schedule_lock = threading.Lock()
    self._closed = False
    self._schedule_periodic_flush()

  def flush(self):
    # type: () -> None
    with self._flush_lock:
      super().flush()

  def close(self):
    # type: () -> None
    with self._schedule_lock:
      self._closed = True
      if self._periodic_flusher:
        self._periodic_flusher.cancel()
        self._periodic_flusher = None
    super().close()

  def _schedule_periodic_flush(self):
    # type: () -> None
    def _flush():
      # type: () -> None
      with self._schedule_lock:
        if not self._closed:
          self.flush()

    self._periodic_flusher = PeriodicThread(
        self._time_flush_threshold_ms / 1000.0, _flush)
    self._periodic_flusher.daemon = True
    self._periodic_flusher.start()


class PeriodicThread(threading.Thread):
  """Call a function periodically with the specified number of seconds"""
  def __init__(
      self,
      interval,  # type: float
      function,  # type: Callable
      args=None,  # type: Optional[Iterable]
      kwargs=None  # type: Optional[Mapping[str, Any]]
  ):
    # type: (...) -> None
    threading.Thread.__init__(self)
    self._interval = interval
    self._function = function
    self._args = args if args is not None else []
    self._kwargs = kwargs if kwargs is not None else {}
    self._finished = threading.Event()

  def run(self):
    # type: () -> None
    next_call = time.time() + self._interval
    while not self._finished.wait(next_call - time.time()):
      next_call = next_call + self._interval
      self._function(*self._args, **self._kwargs)

  def cancel(self):
    # type: () -> None

    """Stop the thread if it hasn't finished yet."""
    self._finished.set()


class DataChannel(metaclass=abc.ABCMeta):
  """Represents a channel for reading and writing data over the data plane.

  Read data and timer from this channel with the input_elements method::

    for elements_data in data_channel.input_elements(
        instruction_id, transform_ids, timers):
      [process elements_data]

  Write data to this channel using the output_stream method::

    out1 = data_channel.output_stream(instruction_id, transform_id)
    out1.write(...)
    out1.close()

  Write timer to this channel using the output_timer_stream method::

    out1 = data_channel.output_timer_stream(instruction_id,
                                            transform_id,
                                            timer_family_id)
    out1.write(...)
    out1.close()

  When all data/timer for all instructions is written, close the channel::

    data_channel.close()
  """
  @abc.abstractmethod
  def input_elements(
      self,
      instruction_id,  # type: str
      expected_inputs,  # type: Collection[Union[str, Tuple[str, str]]]
      abort_callback=None  # type: Optional[Callable[[], bool]]
  ):
    # type: (...) -> Iterator[DataOrTimers]

    """Returns an iterable of all Element.Data and Element.Timers bundles for
    instruction_id.

    This iterable terminates only once the full set of data has been recieved
    for each of the expected transforms. It may block waiting for more data.

    Args:
        instruction_id: which instruction the results must belong to
        expected_inputs: which transforms to wait on for completion
        abort_callback: a callback to invoke if blocking returning whether
            to abort before consuming all the data
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def output_stream(
      self,
      instruction_id,  # type: str
      transform_id  # type: str
  ):
    # type: (...) -> ClosableOutputStream

    """Returns an output stream writing elements to transform_id.

    Args:
        instruction_id: which instruction this stream belongs to
        transform_id: the transform_id of the returned stream
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def output_timer_stream(
      self,
      instruction_id,  # type: str
      transform_id,  # type: str
      timer_family_id  # type: str
  ):
    # type: (...) -> ClosableOutputStream

    """Returns an output stream written timers to transform_id.

    Args:
        instruction_id: which instruction this stream belongs to
        transform_id: the transform_id of the returned stream
        timer_family_id: the timer family of the written timer
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
    # type: () -> None

    """Closes this channel, indicating that all data has been written.

    Data can continue to be read.

    If this channel is shared by many instructions, should only be called on
    worker shutdown.
    """
    raise NotImplementedError(type(self))


class InMemoryDataChannel(DataChannel):
  """An in-memory implementation of a DataChannel.

  This channel is two-sided.  What is written to one side is read by the other.
  The inverse() method returns the other side of a instance.
  """
  def __init__(self, inverse=None, data_buffer_time_limit_ms=0):
    # type: (Optional[InMemoryDataChannel], int) -> None
    self._inputs = []  # type: List[DataOrTimers]
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    self._inverse = inverse or InMemoryDataChannel(
        self, data_buffer_time_limit_ms=data_buffer_time_limit_ms)

  def inverse(self):
    # type: () -> InMemoryDataChannel
    return self._inverse

  def input_elements(
      self,
      instruction_id,  # type: str
      unused_expected_inputs,  # type: Any
      abort_callback=None  # type: Optional[Callable[[], bool]]
  ):
    # type: (...) -> Iterator[DataOrTimers]
    other_inputs = []
    for element in self._inputs:
      if element.instruction_id == instruction_id:
        if isinstance(element, beam_fn_api_pb2.Elements.Timers):
          if not element.is_last:
            yield element
        if isinstance(element, beam_fn_api_pb2.Elements.Data):
          if element.data or element.is_last:
            yield element
      else:
        other_inputs.append(element)
    self._inputs = other_inputs

  def output_timer_stream(
      self,
      instruction_id,  # type: str
      transform_id,  # type: str
      timer_family_id  # type: str
  ):
    # type: (...) -> ClosableOutputStream
    def add_to_inverse_output(timer):
      # type: (bytes) -> None
      if timer:
        self._inverse._inputs.append(
            beam_fn_api_pb2.Elements.Timers(
                instruction_id=instruction_id,
                transform_id=transform_id,
                timer_family_id=timer_family_id,
                timers=timer,
                is_last=False))

    def close_stream(timer):
      # type: (bytes) -> None
      add_to_inverse_output(timer)
      self._inverse._inputs.append(
          beam_fn_api_pb2.Elements.Timers(
              instruction_id=instruction_id,
              transform_id=transform_id,
              timer_family_id='',
              is_last=True))

    return ClosableOutputStream.create(
        add_to_inverse_output, close_stream, self._data_buffer_time_limit_ms)

  def output_stream(self, instruction_id, transform_id):
    # type: (str, str) -> ClosableOutputStream
    def add_to_inverse_output(data):
      # type: (bytes) -> None
      self._inverse._inputs.append(  # pylint: disable=protected-access
          beam_fn_api_pb2.Elements.Data(
              instruction_id=instruction_id,
              transform_id=transform_id,
              data=data))

    return ClosableOutputStream.create(
        add_to_inverse_output,
        add_to_inverse_output,
        self._data_buffer_time_limit_ms)

  def close(self):
    # type: () -> None
    pass


class _GrpcDataChannel(DataChannel):
  """Base class for implementing a BeamFnData-based DataChannel."""

  _WRITES_FINISHED = beam_fn_api_pb2.Elements.Data()

  def __init__(self, data_buffer_time_limit_ms=0):
    # type: (int) -> None
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    self._to_send = queue.Queue()  # type: queue.Queue[DataOrTimers]
    self._received = collections.defaultdict(
        lambda: queue.Queue(maxsize=5)
    )  # type: DefaultDict[str, queue.Queue[DataOrTimers]]

    # Keep a cache of completed instructions. Data for completed instructions
    # must be discarded. See input_elements() and _clean_receiving_queue().
    # OrderedDict is used as FIFO set with the value being always `True`.
    self._cleaned_instruction_ids = collections.OrderedDict(
    )  # type: collections.OrderedDict[str, bool]

    self._receive_lock = threading.Lock()
    self._reads_finished = threading.Event()
    self._closed = False
    self._exception = None  # type: Optional[Exception]

  def close(self):
    # type: () -> None
    self._to_send.put(self._WRITES_FINISHED)
    self._closed = True

  def wait(self, timeout=None):
    # type: (Optional[int]) -> None
    self._reads_finished.wait(timeout)

  def _receiving_queue(self, instruction_id):
    # type: (str) -> Optional[queue.Queue[DataOrTimers]]

    """
    Gets or creates queue for a instruction_id. Or, returns None if the
    instruction_id is already cleaned up. This is best-effort as we track
    a limited number of cleaned-up instructions.
    """
    with self._receive_lock:
      if instruction_id in self._cleaned_instruction_ids:
        return None
      return self._received[instruction_id]

  def _clean_receiving_queue(self, instruction_id):
    # type: (str) -> None

    """
    Removes the queue and adds the instruction_id to the cleaned-up list. The
    instruction_id cannot be reused for new queue.
    """
    with self._receive_lock:
      self._received.pop(instruction_id)
      self._cleaned_instruction_ids[instruction_id] = True
      while len(self._cleaned_instruction_ids) > _MAX_CLEANED_INSTRUCTIONS:
        self._cleaned_instruction_ids.popitem(last=False)

  def input_elements(
      self,
      instruction_id,  # type: str
      expected_inputs,  # type: Collection[Union[str, Tuple[str, str]]]
      abort_callback=None  # type: Optional[Callable[[], bool]]
  ):

    # type: (...) -> Iterator[DataOrTimers]

    """
    Generator to retrieve elements for an instruction_id
    input_elements should be called only once for an instruction_id

    Args:
      instruction_id(str): instruction_id for which data is read
      expected_inputs(collection): expected inputs, include both data and timer.
    """
    received = self._receiving_queue(instruction_id)
    if received is None:
      raise RuntimeError('Instruction cleaned up already %s' % instruction_id)
    done_inputs = set()  # type: Set[Union[str, Tuple[str, str]]]
    abort_callback = abort_callback or (lambda: False)
    log_interval_sec = 5 * 60
    try:
      start_time = time.time()
      next_waiting_log_time = start_time + log_interval_sec
      while len(done_inputs) < len(expected_inputs):
        try:
          element = received.get(timeout=1)
        except queue.Empty:
          if self._closed:
            raise RuntimeError('Channel closed prematurely.')
          if abort_callback():
            return
          if self._exception:
            raise self._exception from None
          current_time = time.time()
          if next_waiting_log_time <= current_time:
            # If at the same time another instruction is waiting on input queue
            # to become available, it is a sign of inefficiency in data plane.
            _LOGGER.info(
                'Detected input queue delay longer than %s seconds. '
                'Waiting to receive elements in input queue '
                'for instruction: %s for %.2f seconds.',
                log_interval_sec,
                instruction_id,
                current_time - start_time)
            next_waiting_log_time = current_time + log_interval_sec
        else:
          start_time = time.time()
          next_waiting_log_time = start_time + log_interval_sec
          if isinstance(element, beam_fn_api_pb2.Elements.Timers):
            if element.is_last:
              done_inputs.add((element.transform_id, element.timer_family_id))
            else:
              yield element
          elif isinstance(element, beam_fn_api_pb2.Elements.Data):
            if element.is_last:
              done_inputs.add(element.transform_id)
            else:
              assert element.transform_id not in done_inputs
              yield element
          else:
            raise ValueError('Unexpected input element type %s' % type(element))
    finally:
      # Instruction_ids are not reusable so Clean queue once we are done with
      #  an instruction_id
      self._clean_receiving_queue(instruction_id)

  def output_stream(self, instruction_id, transform_id):
    # type: (str, str) -> ClosableOutputStream
    def add_to_send_queue(data):
      # type: (bytes) -> None
      if data:
        self._to_send.put(
            beam_fn_api_pb2.Elements.Data(
                instruction_id=instruction_id,
                transform_id=transform_id,
                data=data))

    def close_callback(data):
      # type: (bytes) -> None
      add_to_send_queue(data)
      # End of stream marker.
      self._to_send.put(
          beam_fn_api_pb2.Elements.Data(
              instruction_id=instruction_id,
              transform_id=transform_id,
              is_last=True))

    return ClosableOutputStream.create(
        close_callback, add_to_send_queue, self._data_buffer_time_limit_ms)

  def output_timer_stream(
      self,
      instruction_id,  # type: str
      transform_id,  # type: str
      timer_family_id  # type: str
  ):
    # type: (...) -> ClosableOutputStream
    def add_to_send_queue(timer):
      # type: (bytes) -> None
      if timer:
        self._to_send.put(
            beam_fn_api_pb2.Elements.Timers(
                instruction_id=instruction_id,
                transform_id=transform_id,
                timer_family_id=timer_family_id,
                timers=timer,
                is_last=False))

    def close_callback(timer):
      # type: (bytes) -> None
      add_to_send_queue(timer)
      self._to_send.put(
          beam_fn_api_pb2.Elements.Timers(
              instruction_id=instruction_id,
              transform_id=transform_id,
              timer_family_id=timer_family_id,
              is_last=True))

    return ClosableOutputStream.create(
        close_callback, add_to_send_queue, self._data_buffer_time_limit_ms)

  def _write_outputs(self):
    # type: () -> Iterator[beam_fn_api_pb2.Elements]
    stream_done = False
    while not stream_done:
      streams = [self._to_send.get()]
      try:
        # Coalesce up to 100 other items.
        total_size_bytes = streams[0].ByteSize()
        while (total_size_bytes < _DEFAULT_SIZE_FLUSH_THRESHOLD and
               len(streams) <= 100):
          data_or_timer = self._to_send.get_nowait()
          total_size_bytes += data_or_timer.ByteSize()
          streams.append(data_or_timer)
      except queue.Empty:
        pass
      if streams[-1] is self._WRITES_FINISHED:
        stream_done = True
        streams.pop()
      if streams:
        data_stream = []
        timer_stream = []
        for stream in streams:
          if isinstance(stream, beam_fn_api_pb2.Elements.Timers):
            timer_stream.append(stream)
          elif isinstance(stream, beam_fn_api_pb2.Elements.Data):
            data_stream.append(stream)
          else:
            raise ValueError('Unexpected output element type %s' % type(stream))
        yield beam_fn_api_pb2.Elements(data=data_stream, timers=timer_stream)

  def _read_inputs(self, elements_iterator):
    # type: (Iterable[beam_fn_api_pb2.Elements]) -> None

    next_discard_log_time = 0  # type: float

    def _put_queue(instruction_id, element):
      # type: (str, Union[beam_fn_api_pb2.Elements.Data, beam_fn_api_pb2.Elements.Timers]) -> None

      """
      Puts element to the queue of the instruction_id, or discards it if the
      instruction_id is already cleaned up.
      """
      nonlocal next_discard_log_time
      start_time = time.time()
      next_waiting_log_time = start_time + 300
      while True:
        input_queue = self._receiving_queue(instruction_id)
        if input_queue is None:
          current_time = time.time()
          if next_discard_log_time <= current_time:
            # Log every 10 seconds across all _put_queue calls
            _LOGGER.info(
                'Discard inputs for cleaned up instruction: %s', instruction_id)
            next_discard_log_time = current_time + 10
          return
        try:
          input_queue.put(element, timeout=1)
          return
        except queue.Full:
          current_time = time.time()
          if next_waiting_log_time <= current_time:
            # Log every 5 mins in each _put_queue call
            _LOGGER.info(
                'Waiting on input queue of instruction: %s for %.2f seconds',
                instruction_id,
                current_time - start_time)
            next_waiting_log_time = current_time + 300

    try:
      for elements in elements_iterator:
        for timer in elements.timers:
          _put_queue(timer.instruction_id, timer)
        for data in elements.data:
          _put_queue(data.instruction_id, data)
    except Exception as e:
      if not self._closed:
        _LOGGER.exception('Failed to read inputs in the data plane.')
        self._exception = e
        raise
    finally:
      self._closed = True
      self._reads_finished.set()

  def set_inputs(self, elements_iterator):
    # type: (Iterable[beam_fn_api_pb2.Elements]) -> None
    reader = threading.Thread(
        target=lambda: self._read_inputs(elements_iterator),
        name='read_grpc_client_inputs')
    reader.daemon = True
    reader.start()


class GrpcClientDataChannel(_GrpcDataChannel):
  """A DataChannel wrapping the client side of a BeamFnData connection."""
  def __init__(
      self,
      data_stub,  # type: beam_fn_api_pb2_grpc.BeamFnDataStub
      data_buffer_time_limit_ms=0  # type: int
  ):
    # type: (...) -> None
    super().__init__(data_buffer_time_limit_ms)
    self.set_inputs(data_stub.Data(self._write_outputs()))


class BeamFnDataServicer(beam_fn_api_pb2_grpc.BeamFnDataServicer):
  """Implementation of BeamFnDataServicer for any number of clients"""
  def __init__(
      self,
      data_buffer_time_limit_ms=0  # type: int
  ):
    self._lock = threading.Lock()
    self._connections_by_worker_id = collections.defaultdict(
        lambda: _GrpcDataChannel(data_buffer_time_limit_ms)
    )  # type: DefaultDict[str, _GrpcDataChannel]

  def get_conn_by_worker_id(self, worker_id):
    # type: (str) -> _GrpcDataChannel
    with self._lock:
      return self._connections_by_worker_id[worker_id]

  def Data(
      self,
      elements_iterator,  # type: Iterable[beam_fn_api_pb2.Elements]
      context  # type: Any
  ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements]
    worker_id = dict(context.invocation_metadata())['worker_id']
    data_conn = self.get_conn_by_worker_id(worker_id)
    data_conn.set_inputs(elements_iterator)
    for elements in data_conn._write_outputs():
      yield elements


class DataChannelFactory(metaclass=abc.ABCMeta):
  """An abstract factory for creating ``DataChannel``."""
  @abc.abstractmethod
  def create_data_channel(self, remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> GrpcClientDataChannel

    """Returns a ``DataChannel`` from the given RemoteGrpcPort."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def create_data_channel_from_url(self, url):
    # type: (str) -> Optional[GrpcClientDataChannel]

    """Returns a ``DataChannel`` from the given url."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
    # type: () -> None

    """Close all channels that this factory owns."""
    raise NotImplementedError(type(self))


class GrpcClientDataChannelFactory(DataChannelFactory):
  """A factory for ``GrpcClientDataChannel``.

  Caches the created channels by ``data descriptor url``.
  """
  def __init__(
      self,
      credentials=None,  # type: Any
      worker_id=None,  # type: Optional[str]
      data_buffer_time_limit_ms=0  # type: int
  ):
    # type: (...) -> None
    self._data_channel_cache = {}  # type: Dict[str, GrpcClientDataChannel]
    self._lock = threading.Lock()
    self._credentials = None
    self._worker_id = worker_id
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    if credentials is not None:
      _LOGGER.info('Using secure channel creds.')
      self._credentials = credentials

  def create_data_channel_from_url(self, url):
    # type: (str) -> Optional[GrpcClientDataChannel]
    if not url:
      return None
    if url not in self._data_channel_cache:
      with self._lock:
        if url not in self._data_channel_cache:
          _LOGGER.info('Creating client data channel for %s', url)
          # Options to have no limits (-1) on the size of the messages
          # received or sent over the data plane. The actual buffer size
          # is controlled in a layer above.
          channel_options = [("grpc.max_receive_message_length", -1),
                             ("grpc.max_send_message_length", -1),
                             ("grpc.service_config", _GRPC_SERVICE_CONFIG)]
          grpc_channel = None
          if self._credentials is None:
            grpc_channel = GRPCChannelFactory.insecure_channel(
                url, options=channel_options)
          else:
            grpc_channel = GRPCChannelFactory.secure_channel(
                url, self._credentials, options=channel_options)
          _LOGGER.info('Data channel established.')
          # Add workerId to the grpc channel
          grpc_channel = grpc.intercept_channel(
              grpc_channel, WorkerIdInterceptor(self._worker_id))
          self._data_channel_cache[url] = GrpcClientDataChannel(
              beam_fn_api_pb2_grpc.BeamFnDataStub(grpc_channel),
              self._data_buffer_time_limit_ms)

    return self._data_channel_cache[url]

  def create_data_channel(self, remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> GrpcClientDataChannel
    url = remote_grpc_port.api_service_descriptor.url
    # TODO(https://github.com/apache/beam/issues/19737): this can return None
    #  if url is falsey, but this seems incorrect, as code that calls this
    #  method seems to always expect non-Optional values.
    return self.create_data_channel_from_url(url)  # type: ignore[return-value]

  def close(self):
    # type: () -> None
    _LOGGER.info('Closing all cached grpc data channels.')
    for _, channel in self._data_channel_cache.items():
      channel.close()
    self._data_channel_cache.clear()


class InMemoryDataChannelFactory(DataChannelFactory):
  """A singleton factory for ``InMemoryDataChannel``."""
  def __init__(self, in_memory_data_channel):
    # type: (GrpcClientDataChannel) -> None
    self._in_memory_data_channel = in_memory_data_channel

  def create_data_channel(self, unused_remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> GrpcClientDataChannel
    return self._in_memory_data_channel

  def create_data_channel_from_url(self, url):
    # type: (Any) -> GrpcClientDataChannel
    return self._in_memory_data_channel

  def close(self):
    # type: () -> None
    pass
