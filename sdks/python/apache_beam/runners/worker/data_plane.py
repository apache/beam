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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections
import logging
import queue
import sys
import threading
import time
from builtins import object
from builtins import range
from typing import TYPE_CHECKING
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional

import grpc
from future.utils import raise_
from future.utils import with_metaclass

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor

if TYPE_CHECKING:
  # TODO: remove from TYPE_CHECKING scope when we drop support for python < 3.6
  from typing import Collection

# This module is experimental. No backwards-compatibility guarantees.

_LOGGER = logging.getLogger(__name__)

_DEFAULT_SIZE_FLUSH_THRESHOLD = 10 << 20  # 10MB
_DEFAULT_TIME_FLUSH_THRESHOLD_MS = 0  # disable time-based flush by default


if TYPE_CHECKING:
  import apache_beam.coders.slow_stream
  OutputStream = apache_beam.coders.slow_stream.OutputStream
else:
  OutputStream = type(coder_impl.create_OutputStream())


class ClosableOutputStream(OutputStream):
  """A Outputstream for use with CoderImpls that has a close() method."""

  def __init__(self, close_callback=None):
    super(ClosableOutputStream, self).__init__()
    self._close_callback = close_callback

  def close(self):
    if self._close_callback:
      self._close_callback(self.get())

  @staticmethod
  def create(close_callback,
             flush_callback,
             data_buffer_time_limit_ms):
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

  def __init__(self,
               close_callback=None,  # type: Optional[Callable[[bytes], None]]
               flush_callback=None,  # type: Optional[Callable[[bytes], None]]
               size_flush_threshold=_DEFAULT_SIZE_FLUSH_THRESHOLD):
    super(SizeBasedBufferingClosableOutputStream, self).__init__(close_callback)
    self._flush_callback = flush_callback
    self._size_flush_threshold = size_flush_threshold

  # This must be called explicitly to avoid flushing partial elements.
  def maybe_flush(self):
    if self.size() > self._size_flush_threshold:
      self.flush()

  def flush(self):
    if self._flush_callback:
      self._flush_callback(self.get())
      self._clear()


class TimeBasedBufferingClosableOutputStream(
    SizeBasedBufferingClosableOutputStream):
  """A buffering OutputStream with both time-based and size-based."""

  def __init__(self,
               close_callback=None,
               flush_callback=None,
               size_flush_threshold=_DEFAULT_SIZE_FLUSH_THRESHOLD,
               time_flush_threshold_ms=_DEFAULT_TIME_FLUSH_THRESHOLD_MS):
    super(TimeBasedBufferingClosableOutputStream, self).__init__(
        close_callback, flush_callback, size_flush_threshold)
    assert time_flush_threshold_ms > 0
    self._time_flush_threshold_ms = time_flush_threshold_ms
    self._flush_lock = threading.Lock()
    self._schedule_lock = threading.Lock()
    self._closed = False
    self._schedule_periodic_flush()

  def flush(self):
    with self._flush_lock:
      super(TimeBasedBufferingClosableOutputStream, self).flush()

  def close(self):
    with self._schedule_lock:
      self._closed = True
      if self._periodic_flusher:
        self._periodic_flusher.cancel()
        self._periodic_flusher = None
    super(TimeBasedBufferingClosableOutputStream, self).close()

  def _schedule_periodic_flush(self):
    def _flush():
      with self._schedule_lock:
        if not self._closed:
          self.flush()

    self._periodic_flusher = PeriodicThread(
        self._time_flush_threshold_ms / 1000.0, _flush)
    self._periodic_flusher.daemon = True
    self._periodic_flusher.start()


class PeriodicThread(threading.Thread):
  """Call a function periodically with the specified number of seconds"""

  def __init__(self,
               interval,
               function,
               args=None,
               kwargs=None):
    threading.Thread.__init__(self)
    self._interval = interval
    self._function = function
    self._args = args if args is not None else []
    self._kwargs = kwargs if kwargs is not None else {}
    self._finished = threading.Event()

  def run(self):
    next_call = time.time() + self._interval
    while not self._finished.wait(next_call - time.time()):
      next_call = next_call + self._interval
      self._function(*self._args, **self._kwargs)

  def cancel(self):
    """Stop the thread if it hasn't finished yet."""
    self._finished.set()


class DataChannel(with_metaclass(abc.ABCMeta, object)):  # type: ignore[misc]
  """Represents a channel for reading and writing data over the data plane.

  Read from this channel with the input_elements method::

    for elements_data in data_channel.input_elements(
        instruction_id, transform_ids):
      [process elements_data]

  Write to this channel using the output_stream method::

    out1 = data_channel.output_stream(instruction_id, transform_id)
    out1.write(...)
    out1.close()

  When all data for all instructions is written, close the channel::

    data_channel.close()
  """

  @abc.abstractmethod
  def input_elements(self,
                     instruction_id,  # type: str
                     expected_transforms,  # type: Collection[str]
                     abort_callback=None  # type: Optional[Callable[[], bool]]
                    ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements.Data]
    """Returns an iterable of all Element.Data bundles for instruction_id.

    This iterable terminates only once the full set of data has been recieved
    for each of the expected transforms. It may block waiting for more data.

    Args:
        instruction_id: which instruction the results must belong to
        expected_transforms: which transforms to wait on for completion
        abort_callback: a callback to invoke if blocking returning whether
            to abort before consuming all the data
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def output_stream(self,
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
    # type: (Optional[InMemoryDataChannel], Optional[int]) -> None
    self._inputs = []  # type: List[beam_fn_api_pb2.Elements.Data]
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    self._inverse = inverse or InMemoryDataChannel(
        self, data_buffer_time_limit_ms=data_buffer_time_limit_ms)

  def inverse(self):
    # type: () -> InMemoryDataChannel
    return self._inverse

  def input_elements(self,
                     instruction_id,  # type: str
                     unused_expected_transforms=None,  # type: Optional[Collection[str]]
                     abort_callback=None  # type: Optional[Callable[[], bool]]
                    ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements.Data]
    other_inputs = []
    for data in self._inputs:
      if data.instruction_id == instruction_id:
        if data.data:
          yield data
      else:
        other_inputs.append(data)
    self._inputs = other_inputs

  def output_stream(self, instruction_id, transform_id):
    # type: (str, str) -> ClosableOutputStream
    def add_to_inverse_output(data):
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
    pass


class _GrpcDataChannel(DataChannel):
  """Base class for implementing a BeamFnData-based DataChannel."""

  _WRITES_FINISHED = object()

  def __init__(self, data_buffer_time_limit_ms=0):
    # type: (Optional[int]) -> None
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    self._to_send = queue.Queue()  # type: queue.Queue[beam_fn_api_pb2.Elements.Data]
    self._received = collections.defaultdict(lambda: queue.Queue(maxsize=5))  # type: DefaultDict[str, queue.Queue[beam_fn_api_pb2.Elements.Data]]
    self._receive_lock = threading.Lock()
    self._reads_finished = threading.Event()
    self._closed = False
    self._exc_info = None

  def close(self):
    self._to_send.put(self._WRITES_FINISHED)
    self._closed = True

  def wait(self, timeout=None):
    self._reads_finished.wait(timeout)

  def _receiving_queue(self, instruction_id):
    # type: (str) -> queue.Queue[beam_fn_api_pb2.Elements.Data]
    with self._receive_lock:
      return self._received[instruction_id]

  def _clean_receiving_queue(self, instruction_id):
    # type: (str) -> None
    with self._receive_lock:
      self._received.pop(instruction_id)

  def input_elements(self,
                     instruction_id,  # type: str
                     expected_transforms,  # type: Collection[str]
                     abort_callback=None  # type: Optional[Callable[[], bool]]
                    ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements.Data]
    """
    Generator to retrieve elements for an instruction_id
    input_elements should be called only once for an instruction_id

    Args:
      instruction_id(str): instruction_id for which data is read
      expected_transforms(collection): expected transforms
    """
    received = self._receiving_queue(instruction_id)
    done_transforms = []  # type: List[str]
    abort_callback = abort_callback or (lambda: False)
    try:
      while len(done_transforms) < len(expected_transforms):
        try:
          data = received.get(timeout=1)
        except queue.Empty:
          if self._closed:
            raise RuntimeError('Channel closed prematurely.')
          if abort_callback():
            return
          if self._exc_info:
            t, v, tb = self._exc_info
            raise_(t, v, tb)
        else:
          if not data.data and data.transform_id in expected_transforms:
            done_transforms.append(data.transform_id)
          else:
            assert data.transform_id not in done_transforms
            yield data
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
              data=b''))

    return ClosableOutputStream.create(
        close_callback,
        add_to_send_queue,
        self._data_buffer_time_limit_ms)

  def _write_outputs(self):
    # type: () -> Iterator[beam_fn_api_pb2.Elements]
    done = False
    while not done:
      data = [self._to_send.get()]
      try:
        # Coalesce up to 100 other items.
        for _ in range(100):
          data.append(self._to_send.get_nowait())
      except queue.Empty:
        pass
      if data[-1] is self._WRITES_FINISHED:
        done = True
        data.pop()
      if data:
        yield beam_fn_api_pb2.Elements(data=data)

  def _read_inputs(self, elements_iterator):
    # type: (Iterable[beam_fn_api_pb2.Elements]) -> None
    try:
      for elements in elements_iterator:
        for data in elements.data:
          self._receiving_queue(data.instruction_id).put(data)
    except:  # pylint: disable=bare-except
      if not self._closed:
        _LOGGER.exception('Failed to read inputs in the data plane.')
        self._exc_info = sys.exc_info()
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

  def __init__(self,
               data_stub,  # type: beam_fn_api_pb2_grpc.BeamFnDataStub
               data_buffer_time_limit_ms=0  # type: Optional[int]
               ):
    # type: (...) -> None
    super(GrpcClientDataChannel, self).__init__(data_buffer_time_limit_ms)
    self.set_inputs(data_stub.Data(self._write_outputs()))


class BeamFnDataServicer(beam_fn_api_pb2_grpc.BeamFnDataServicer):
  """Implementation of BeamFnDataServicer for any number of clients"""

  def __init__(self,
               data_buffer_time_limit_ms=0  # type: Optional[int]
               ):
    self._lock = threading.Lock()
    self._connections_by_worker_id = collections.defaultdict(
        lambda: _GrpcDataChannel(data_buffer_time_limit_ms))  # type: DefaultDict[str, _GrpcDataChannel]

  def get_conn_by_worker_id(self, worker_id):
    # type: (str) -> _GrpcDataChannel
    with self._lock:
      return self._connections_by_worker_id[worker_id]

  def Data(self,
           elements_iterator,  # type: Iterable[beam_fn_api_pb2.Elements]
           context
          ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements]
    worker_id = dict(context.invocation_metadata())['worker_id']
    data_conn = self.get_conn_by_worker_id(worker_id)
    data_conn.set_inputs(elements_iterator)
    for elements in data_conn._write_outputs():
      yield elements


class DataChannelFactory(with_metaclass(abc.ABCMeta, object)):  # type: ignore[misc]
  """An abstract factory for creating ``DataChannel``."""

  @abc.abstractmethod
  def create_data_channel(self, remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> GrpcClientDataChannel
    """Returns a ``DataChannel`` from the given RemoteGrpcPort."""
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

  def __init__(self,
               credentials=None,
               worker_id=None,  # type: Optional[str]
               data_buffer_time_limit_ms=0  # type: Optional[int]
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

  def create_data_channel(self, remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> GrpcClientDataChannel
    url = remote_grpc_port.api_service_descriptor.url
    if url not in self._data_channel_cache:
      with self._lock:
        if url not in self._data_channel_cache:
          _LOGGER.info('Creating client data channel for %s', url)
          # Options to have no limits (-1) on the size of the messages
          # received or sent over the data plane. The actual buffer size
          # is controlled in a layer above.
          channel_options = [("grpc.max_receive_message_length", -1),
                             ("grpc.max_send_message_length", -1)]
          grpc_channel = None
          if self._credentials is None:
            grpc_channel = GRPCChannelFactory.insecure_channel(
                url, options=channel_options)
          else:
            grpc_channel = GRPCChannelFactory.secure_channel(
                url, self._credentials, options=channel_options)
          # Add workerId to the grpc channel
          grpc_channel = grpc.intercept_channel(
              grpc_channel, WorkerIdInterceptor(self._worker_id))
          self._data_channel_cache[url] = GrpcClientDataChannel(
              beam_fn_api_pb2_grpc.BeamFnDataStub(grpc_channel),
              self._data_buffer_time_limit_ms)

    return self._data_channel_cache[url]

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

  def close(self):
    # type: () -> None
    pass
