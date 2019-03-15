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

"""Implementation of DataChannels for communicating across the data plane."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections
import logging
import queue
import sys
import threading
from builtins import object
from builtins import range

import grpc
from future.utils import raise_
from future.utils import with_metaclass

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor

# This module is experimental. No backwards-compatibility guarantees.


_DEFAULT_FLUSH_THRESHOLD = 10 << 20  # 10MB


class ClosableOutputStream(type(coder_impl.create_OutputStream())):
  """A Outputstream for use with CoderImpls that has a close() method."""

  def __init__(self,
               close_callback=None,
               flush_callback=None,
               flush_threshold=_DEFAULT_FLUSH_THRESHOLD):
    super(ClosableOutputStream, self).__init__()
    self._close_callback = close_callback
    self._flush_callback = flush_callback
    self._flush_threshold = flush_threshold

  # This must be called explicitly to avoid flushing partial elements.
  def maybe_flush(self):
    if self._flush_callback and self.size() > self._flush_threshold:
      self._flush_callback(self.get())
      self._clear()

  def close(self):
    if self._close_callback:
      self._close_callback(self.get())


class DataChannel(with_metaclass(abc.ABCMeta, object)):
  """Represents a channel for reading and writing data over the data plane.

  Read from this channel with the input_elements method::

    for elements_data in data_channel.input_elements(instruction_id, targets):
      [process elements_data]

  Write to this channel using the output_stream method::

    out1 = data_channel.output_stream(instruction_id, target1)
    out1.write(...)
    out1.close()

  When all data for all instructions is written, close the channel::

    data_channel.close()
  """

  @abc.abstractmethod
  def input_elements(
      self, instruction_id, expected_targets, abort_callback=None):
    """Returns an iterable of all Element.Data bundles for instruction_id.

    This iterable terminates only once the full set of data has been recieved
    for each of the expected targets. It may block waiting for more data.

    Args:
        instruction_id: which instruction the results must belong to
        expected_targets: which targets to wait on for completion
        abort_callback: a callback to invoke if blocking returning whether
            to abort before consuming all the data
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def output_stream(self, instruction_id, target):
    """Returns an output stream writing elements to target.

    Args:
        instruction_id: which instruction this stream belongs to
        target: the target of the returned stream
    """
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
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

  def __init__(self, inverse=None):
    self._inputs = []
    self._inverse = inverse or InMemoryDataChannel(self)

  def inverse(self):
    return self._inverse

  def input_elements(self, instruction_id, unused_expected_targets=None,
                     abort_callback=None):
    other_inputs = []
    for data in self._inputs:
      if data.instruction_reference == instruction_id:
        if data.data:
          yield data
      else:
        other_inputs.append(data)
    self._inputs = other_inputs

  def output_stream(self, instruction_id, target):
    def add_to_inverse_output(data):
      self._inverse._inputs.append(  # pylint: disable=protected-access
          beam_fn_api_pb2.Elements.Data(
              instruction_reference=instruction_id,
              target=target,
              data=data))
    return ClosableOutputStream(
        add_to_inverse_output, flush_callback=add_to_inverse_output)

  def close(self):
    pass


class _GrpcDataChannel(DataChannel):
  """Base class for implementing a BeamFnData-based DataChannel."""

  _WRITES_FINISHED = object()

  def __init__(self):
    self._to_send = queue.Queue()
    self._received = collections.defaultdict(queue.Queue)
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
    with self._receive_lock:
      return self._received[instruction_id]

  def _clean_receiving_queue(self, instruction_id):
    with self._receive_lock:
      self._received.pop(instruction_id)

  def input_elements(self, instruction_id, expected_targets,
                     abort_callback=None):
    """
    Generator to retrieve elements for an instruction_id
    input_elements should be called only once for an instruction_id

    Args:
      instruction_id(str): instruction_id for which data is read
      expected_targets(collection): expected targets
    """
    received = self._receiving_queue(instruction_id)
    done_targets = []
    abort_callback = abort_callback or (lambda: False)
    try:
      while len(done_targets) < len(expected_targets):
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
          if not data.data and data.target in expected_targets:
            done_targets.append(data.target)
          else:
            assert data.target not in done_targets
            yield data
    finally:
      # Instruction_ids are not reusable so Clean queue once we are done with
      #  an instruction_id
      self._clean_receiving_queue(instruction_id)

  def output_stream(self, instruction_id, target):
    def add_to_send_queue(data):
      if data:
        self._to_send.put(
            beam_fn_api_pb2.Elements.Data(
                instruction_reference=instruction_id,
                target=target,
                data=data))

    def close_callback(data):
      add_to_send_queue(data)
      # End of stream marker.
      self._to_send.put(
          beam_fn_api_pb2.Elements.Data(
              instruction_reference=instruction_id,
              target=target,
              data=b''))
    return ClosableOutputStream(
        close_callback, flush_callback=add_to_send_queue)

  def _write_outputs(self):
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
    # TODO(robertwb): Pushback/throttling to avoid unbounded buffering.
    try:
      for elements in elements_iterator:
        for data in elements.data:
          self._receiving_queue(data.instruction_reference).put(data)
    except:  # pylint: disable=bare-except
      if not self._closed:
        logging.exception('Failed to read inputs in the data plane')
        self._exc_info = sys.exc_info()
        raise
    finally:
      self._closed = True
      self._reads_finished.set()

  def _start_reader(self, elements_iterator):
    reader = threading.Thread(
        target=lambda: self._read_inputs(elements_iterator),
        name='read_grpc_client_inputs')
    reader.daemon = True
    reader.start()


class GrpcClientDataChannel(_GrpcDataChannel):
  """A DataChannel wrapping the client side of a BeamFnData connection."""

  def __init__(self, data_stub):
    super(GrpcClientDataChannel, self).__init__()
    self._start_reader(data_stub.Data(self._write_outputs()))


class GrpcServerDataChannel(
    beam_fn_api_pb2_grpc.BeamFnDataServicer, _GrpcDataChannel):
  """A DataChannel wrapping the server side of a BeamFnData connection."""

  def Data(self, elements_iterator, context):
    self._start_reader(elements_iterator)
    for elements in self._write_outputs():
      yield elements


class DataChannelFactory(with_metaclass(abc.ABCMeta, object)):
  """An abstract factory for creating ``DataChannel``."""

  @abc.abstractmethod
  def create_data_channel(self, remote_grpc_port):
    """Returns a ``DataChannel`` from the given RemoteGrpcPort."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
    """Close all channels that this factory owns."""
    raise NotImplementedError(type(self))


class GrpcClientDataChannelFactory(DataChannelFactory):
  """A factory for ``GrpcClientDataChannel``.

  Caches the created channels by ``data descriptor url``.
  """

  def __init__(self, credentials=None):
    self._data_channel_cache = {}
    self._lock = threading.Lock()
    self._credentials = None
    if credentials is not None:
      logging.info('Using secure channel creds.')
      self._credentials = credentials

  def create_data_channel(self, remote_grpc_port):
    url = remote_grpc_port.api_service_descriptor.url
    if url not in self._data_channel_cache:
      with self._lock:
        if url not in self._data_channel_cache:
          logging.info('Creating channel for %s', url)
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
          grpc_channel = grpc.intercept_channel(grpc_channel,
                                                WorkerIdInterceptor())
          self._data_channel_cache[url] = GrpcClientDataChannel(
              beam_fn_api_pb2_grpc.BeamFnDataStub(grpc_channel))

    return self._data_channel_cache[url]

  def close(self):
    logging.info('Closing all cached grpc data channels.')
    for _, channel in self._data_channel_cache.items():
      channel.close()
    self._data_channel_cache.clear()


class InMemoryDataChannelFactory(DataChannelFactory):
  """A singleton factory for ``InMemoryDataChannel``."""

  def __init__(self, in_memory_data_channel):
    self._in_memory_data_channel = in_memory_data_channel

  def create_data_channel(self, unused_remote_grpc_port):
    return self._in_memory_data_channel

  def close(self):
    pass
