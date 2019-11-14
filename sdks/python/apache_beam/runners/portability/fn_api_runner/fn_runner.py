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

"""A PipelineRunner using the SDK harness.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import collections
import contextlib
import copy
import itertools
import logging
import os
import queue
import subprocess
import sys
import threading
import time
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast
from typing import overload

import grpc
from typing_extensions import Protocol

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam import coders
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.metrics import metric
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricResult
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import beam_provision_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners import runner
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import portable_metrics
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.portability.fn_api_runner.translations import split_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import unique_name
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.sdk_worker import _Future
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.transforms import environments
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import profiler
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

if TYPE_CHECKING:
  from apache_beam.pipeline import Pipeline
  from apache_beam.coders.coder_impl import CoderImpl
  from apache_beam.coders.coder_impl import WindowedValueCoderImpl
  from apache_beam.portability.api import metrics_pb2
  from apache_beam.transforms.window import BoundedWindow

T = TypeVar('T')
ConstructorFn = Callable[[
    Union['message.Message', bytes],
    'FnApiRunner.StateServicer',
    Optional['ExtendedProvisionInfo'],
    'GrpcServer'
],
                         'WorkerHandler']
DataSideInput = Dict[Tuple[str, str],
                     Tuple[bytes, beam_runner_api_pb2.FunctionSpec]]
DataOutput = Dict[str, bytes]
BundleProcessResult = Tuple[beam_fn_api_pb2.InstructionResponse,
                            List[beam_fn_api_pb2.ProcessBundleSplitResponse]]

# This module is experimental. No backwards-compatibility guarantees.

ENCODED_IMPULSE_VALUE = beam.coders.WindowedValueCoder(
    beam.coders.BytesCoder(),
    beam.coders.coders.GlobalWindowCoder()).get_impl().encode_nested(
        beam.transforms.window.GlobalWindows.windowed_value(b''))

# State caching is enabled in the fn_api_runner for testing, except for one
# test which runs without state caching (FnApiRunnerTestWithDisabledCaching).
# The cache is disabled in production for other runners.
STATE_CACHE_SIZE = 100

# Time-based flush is enabled in the fn_api_runner by default.
DATA_BUFFER_TIME_LIMIT_MS = 1000

_LOGGER = logging.getLogger(__name__)


class ControlConnection(object):

  _uid_counter = 0
  _lock = threading.Lock()

  def __init__(self):
    self._push_queue = queue.Queue(
    )  # type: queue.Queue[beam_fn_api_pb2.InstructionRequest]
    self._input = None  # type: Optional[Iterable[beam_fn_api_pb2.InstructionResponse]]
    self._futures_by_id = dict()  # type: Dict[str, ControlFuture]
    self._read_thread = threading.Thread(
        name='beam_control_read', target=self._read)
    self._state = BeamFnControlServicer.UNSTARTED_STATE

  def _read(self):
    for data in self._input:
      self._futures_by_id.pop(data.instruction_id).set(data)

  @overload
  def push(self, req):
    # type: (BeamFnControlServicer.DoneMarker) -> None
    pass

  @overload
  def push(self, req):
    # type: (beam_fn_api_pb2.InstructionRequest) -> ControlFuture
    pass

  def push(self, req):
    if req == BeamFnControlServicer._DONE_MARKER:
      self._push_queue.put(req)
      return None
    if not req.instruction_id:
      with ControlConnection._lock:
        ControlConnection._uid_counter += 1
        req.instruction_id = 'control_%s' % ControlConnection._uid_counter
    future = ControlFuture(req.instruction_id)
    self._futures_by_id[req.instruction_id] = future
    self._push_queue.put(req)
    return future

  def get_req(self):
    # type: () -> beam_fn_api_pb2.InstructionRequest
    return self._push_queue.get()

  def set_input(self, input):
    # type: (Iterable[beam_fn_api_pb2.InstructionResponse]) -> None
    with ControlConnection._lock:
      if self._input:
        raise RuntimeError('input is already set.')
      self._input = input
      self._read_thread.start()
      self._state = BeamFnControlServicer.STARTED_STATE

  def close(self):
    # type: () -> None
    with ControlConnection._lock:
      if self._state == BeamFnControlServicer.STARTED_STATE:
        self.push(BeamFnControlServicer._DONE_MARKER)
        self._read_thread.join()
      self._state = BeamFnControlServicer.DONE_STATE


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):
  """Implementation of BeamFnControlServicer for clients."""

  UNSTARTED_STATE = 'unstarted'
  STARTED_STATE = 'started'
  DONE_STATE = 'done'

  class DoneMarker(object):
    pass

  _DONE_MARKER = DoneMarker()

  def __init__(self):
    self._lock = threading.Lock()
    self._uid_counter = 0
    self._state = self.UNSTARTED_STATE
    # following self._req_* variables are used for debugging purpose, data is
    # added only when self._log_req is True.
    self._req_sent = collections.defaultdict(int)
    self._req_worker_mapping = {}
    self._log_req = logging.getLogger().getEffectiveLevel() <= logging.DEBUG
    self._connections_by_worker_id = collections.defaultdict(
        ControlConnection)  # type: DefaultDict[str, ControlConnection]

  def get_conn_by_worker_id(self, worker_id):
    # type: (str) -> ControlConnection
    with self._lock:
      return self._connections_by_worker_id[worker_id]

  def Control(self,
              iterator,  # type: Iterable[beam_fn_api_pb2.InstructionResponse]
              context
             ):
    # type: (...) -> Iterator[beam_fn_api_pb2.InstructionRequest]
    with self._lock:
      if self._state == self.DONE_STATE:
        return
      else:
        self._state = self.STARTED_STATE

    worker_id = dict(context.invocation_metadata()).get('worker_id')
    if not worker_id:
      raise RuntimeError(
          'All workers communicate through gRPC should have '
          'worker_id. Received None.')

    control_conn = self.get_conn_by_worker_id(worker_id)
    control_conn.set_input(iterator)

    while True:
      to_push = control_conn.get_req()
      if to_push is self._DONE_MARKER:
        return
      yield to_push
      if self._log_req:
        self._req_sent[to_push.instruction_id] += 1

  def done(self):
    self._state = self.DONE_STATE
    _LOGGER.debug(
        'Runner: Requests sent by runner: %s',
        [(str(req), cnt) for req, cnt in self._req_sent.items()])
    _LOGGER.debug(
        'Runner: Requests multiplexing info: %s',
        [(str(req), worker)
         for req, worker in self._req_worker_mapping.items()])


class Buffer(Protocol):
  def __iter__(self):
    # type: () -> Iterator[bytes]
    pass

  def append(self, item):
    # type: (bytes) -> None
    pass


class PartitionableBuffer(Buffer, Protocol):
  def partition(self, n):
    # type: (int) -> List[List[bytes]]
    pass


class _ListBuffer():
  """Used to support parititioning of a list."""
  def __init__(self, coder_impl):
    self._coder_impl = coder_impl
    self._inputs = []  # type: List[bytes]
    self._grouped_output = None
    self.cleared = False

  def append(self, element):
    # type: (bytes) -> None
    if self.cleared:
      raise RuntimeError('Trying to append to a cleared ListBuffer.')
    if self._grouped_output:
      raise RuntimeError('ListBuffer append after read.')
    self._inputs.append(element)

  def partition(self, n):
    # type: (int) -> List[List[bytes]]
    if self.cleared:
      raise RuntimeError('Trying to partition a cleared ListBuffer.')
    if len(self._inputs) >= n or len(self._inputs) == 0:
      return [self._inputs[k::n] for k in range(n)]
    else:
      if not self._grouped_output:
        output_stream_list = [create_OutputStream() for _ in range(n)]
        idx = 0
        for input in self._inputs:
          input_stream = create_InputStream(input)
          while input_stream.size() > 0:
            decoded_value = self._coder_impl.decode_from_stream(
                input_stream, True)
            self._coder_impl.encode_to_stream(
                decoded_value, output_stream_list[idx], True)
            idx = (idx + 1) % n
        self._grouped_output = [[output_stream.get()]
                                for output_stream in output_stream_list]
      return self._grouped_output

  def __iter__(self):
    # type: () -> Iterator[bytes]
    if self.cleared:
      raise RuntimeError('Trying to iterate through a cleared ListBuffer.')
    return iter(self._inputs)

  def clear(self):
    # type: () -> None
    self.cleared = True
    self._inputs = []
    self._grouped_output = None


class _GroupingBuffer(object):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(self,
               pre_grouped_coder,  # type: coders.Coder
               post_grouped_coder,  # type: coders.Coder
               windowing
              ):
    # type: (...) -> None
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table = collections.defaultdict(
        list)  # type: DefaultDict[bytes, List[Any]]
    self._windowing = windowing
    self._grouped_output = None  # type: Optional[List[List[bytes]]]

  def append(self, elements_data):
    # type: (bytes) -> None
    if self._grouped_output:
      raise RuntimeError('Grouping table append after read.')
    input_stream = create_InputStream(elements_data)
    coder_impl = self._pre_grouped_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    # TODO(robertwb): We could optimize this even more by using a
    # window-dropping coder for the data plane.
    is_trivial_windowing = self._windowing.is_default()
    while input_stream.size() > 0:
      windowed_key_value = coder_impl.decode_from_stream(input_stream, True)
      key, value = windowed_key_value.value
      self._table[key_coder_impl.encode(key)].append(
          value if is_trivial_windowing else windowed_key_value.
          with_value(value))

  def extend(self, input_buffer):
    raise NotImplementedError(
        'GroupingBuffer is not expected to merge with others')

  def partition(self, n):
    # type: (int) -> List[List[bytes]]

    """ It is used to partition _GroupingBuffer to N parts. Once it is
    partitioned, it would not be re-partitioned with diff N. Re-partition
    is not supported now.
    """
    if not self._grouped_output:
      if self._windowing.is_default():
        globally_window = GlobalWindows.windowed_value(
            None,
            timestamp=GlobalWindow().max_timestamp(),
            pane_info=windowed_value.PaneInfo(
                is_first=True,
                is_last=True,
                timing=windowed_value.PaneInfoTiming.ON_TIME,
                index=0,
                nonspeculative_index=0)).with_value
        windowed_key_values = lambda key, values: [
            globally_window((key, values))]
      else:
        # TODO(pabloem, BEAM-7514): Trigger driver needs access to the clock
        #   note that this only comes through if windowing is default - but what
        #   about having multiple firings on the global window.
        #   May need to revise.
        trigger_driver = trigger.create_trigger_driver(self._windowing, True)
        windowed_key_values = trigger_driver.process_entire_key
      coder_impl = self._post_grouped_coder.get_impl()
      key_coder_impl = self._key_coder.get_impl()
      self._grouped_output = [[] for _ in range(n)]
      output_stream_list = [create_OutputStream() for _ in range(n)]
      for idx, (encoded_key, windowed_values) in enumerate(self._table.items()):
        key = key_coder_impl.decode(encoded_key)
        for wkvs in windowed_key_values(key, windowed_values):
          coder_impl.encode_to_stream(wkvs, output_stream_list[idx % n], True)
      for ix, output_stream in enumerate(output_stream_list):
        self._grouped_output[ix] = [output_stream.get()]
      self._table.clear()
    return self._grouped_output

  def __repr__(self):
    return '<%s at 0x%x>' % (self.__str__(), id(self))

  def __str__(self):
    return '[%s %s]' % (self.__class__.__name__, None)
    # TODO(pabloem, MUST): REMOVE NEXT LINE.
                        #list(itertools.chain(*self.partition(1))))

  def __iter__(self):
    # type: () -> Iterator[bytes]

    """ Since partition() returns a list of lists, add this __iter__ to return
    a list to simplify code when we need to iterate through ALL elements of
    _GroupingBuffer.
    """
    return itertools.chain(*self.partition(1))


class _WindowGroupingBuffer(object):
  """Used to partition windowed side inputs."""
  def __init__(
      self,
      access_pattern,
      coder  # type: coders.WindowedValueCoder
  ):
    # type: (...) -> None
    # Here's where we would use a different type of partitioning
    # (e.g. also by key) for a different access pattern.
    if access_pattern.urn == common_urns.side_inputs.ITERABLE.urn:
      self._kv_extractor = lambda value: ('', value)
      self._key_coder = coders.SingletonCoder('')  # type: coders.Coder
      self._value_coder = coder.wrapped_value_coder
    elif access_pattern.urn == common_urns.side_inputs.MULTIMAP.urn:
      self._kv_extractor = lambda value: value
      self._key_coder = coder.wrapped_value_coder.key_coder()
      self._value_coder = (coder.wrapped_value_coder.value_coder())
    else:
      raise ValueError("Unknown access pattern: '%s'" % access_pattern.urn)
    self._windowed_value_coder = coder
    self._window_coder = coder.window_coder
    self._values_by_window = collections.defaultdict(
        list)  # type: DefaultDict[Tuple[str, BoundedWindow], List[Any]]

  def append(self, elements_data):
    # type: (bytes) -> None
    input_stream = create_InputStream(elements_data)
    while input_stream.size() > 0:
      windowed_val_coder_impl = self._windowed_value_coder.get_impl(
      )  # type: WindowedValueCoderImpl
      windowed_value = windowed_val_coder_impl.decode_from_stream(
          input_stream, True)
      key, value = self._kv_extractor(windowed_value.value)
      for window in windowed_value.windows:
        self._values_by_window[key, window].append(value)

  def encoded_items(self):
    # type: () -> Iterator[Tuple[bytes, bytes, bytes]]
    value_coder_impl = self._value_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    for (key, window), values in self._values_by_window.items():
      encoded_window = self._window_coder.encode(window)
      encoded_key = key_coder_impl.encode_nested(key)
      output_stream = create_OutputStream()
      for value in values:
        value_coder_impl.encode_to_stream(value, output_stream, True)
      yield encoded_key, encoded_window, output_stream.get()


class _FnApiRunnerExecution(object):
  """Provides a set of utilities for the execution of a pipeline by FnApiRunner.

  This class must not be instantiated. It is meant to provide static utilites.
  """

  @staticmethod
  def make_iterable_state_write(worker_handler):
    def iterable_state_write(values, element_coder_impl):
      token = unique_name(None, 'iter').encode('ascii')
      out = create_OutputStream()
      for element in values:
        element_coder_impl.encode_to_stream(element, out, True)
      worker_handler.state.append_raw(
          beam_fn_api_pb2.StateKey(
              runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
          out.get())
      return token
    return iterable_state_write

  @staticmethod
  def make_input_coder_getter(pipeline_context,
                              process_bundle_descriptor,
                              safe_coders):
    def input_coder_getter_impl(transform_id):
      return pipeline_context.coders[safe_coders[
          beam_fn_api_pb2.RemoteGrpcPort.FromString(
              process_bundle_descriptor.transforms[transform_id].spec.payload
          ).coder_id
      ]].get_impl()
    return input_coder_getter_impl

  @staticmethod
  def make_input_buffer_fetcher(
      pipeline_context,  # type: PipelineContext
      pcoll_buffers,  # type: Dict[str, Union[_ListBuffer, _GroupingBuffer]]
      pipeline_components,  # type: beam_runner_api_pb2.Components
      safe_coders  # type: Dict[str, str]
  ):
    # type: (...) -> Callable
    """Returns a callable to fetch the buffer containing a PCollection to input
       to a PTransform.

      For grouping-typed operations, we produce a ``_GroupingBuffer``. For
      others, we produce a ``_ListBuffer``.
    """
    def buffer_fetcher(buffer_id):
      # type: (str) -> Union[_ListBuffer, _GroupingBuffer]
      kind, name = split_buffer_id(buffer_id)
      if kind in ('materialize', 'timers'):
        # If `buffer_id` is not a key in `pcoll_buffers`, it will be added by
        # the `defaultdict`.
        return pcoll_buffers[buffer_id]
      elif kind == 'group':
        # This is a grouping write, create a grouping buffer if needed.
        if buffer_id not in pcoll_buffers:
          original_gbk_transform = name
          transform_proto = pipeline_components.transforms[
              original_gbk_transform]
          input_pcoll = only_element(list(transform_proto.inputs.values()))
          output_pcoll = only_element(list(transform_proto.outputs.values()))
          pre_gbk_coder = pipeline_context.coders[safe_coders[
              pipeline_components.pcollections[input_pcoll].coder_id]]
          post_gbk_coder = pipeline_context.coders[safe_coders[
              pipeline_components.pcollections[output_pcoll].coder_id]]
          windowing_strategy = pipeline_context.windowing_strategies[
              pipeline_components.pcollections[
                  output_pcoll].windowing_strategy_id]
          pcoll_buffers[buffer_id] = _GroupingBuffer(
              pre_gbk_coder, post_gbk_coder, windowing_strategy)
      else:
        # These should be the only two identifiers we produce for now,
        # but special side input writes may go here.
        raise NotImplementedError(buffer_id)
      return pcoll_buffers[buffer_id]
    return buffer_fetcher

  @staticmethod
  def get_input_operation_name(
      process_bundle_descriptor, transform_id, input_id):
    # type: (beam_fn_api_pb2.ProcessBundleDescriptor, str, str) -> str
    """Returns a callable to find the ID of the data input operation that
    feeds an input PCollection to a PTransform. """
    input_pcoll = process_bundle_descriptor.transforms[
        transform_id].inputs[input_id]
    for read_id, proto in process_bundle_descriptor.transforms.items():
      if (proto.spec.urn == bundle_processor.DATA_INPUT_URN
          and input_pcoll in proto.outputs.values()):
        return read_id
    raise RuntimeError(
        'No IO transform feeds %s' % transform_id)

  @staticmethod
  def _store_side_inputs_in_state(self,
      worker_handler,  # type: WorkerHandler
      context,  # type: pipeline_context.PipelineContext
      pipeline_components,  # type: beam_runner_api_pb2.Components
      side_input_infos,  # type: List[fn_api_runner_transforms.SideInputInfo]
      pcoll_buffers,  # type: Mapping[bytes, _ListBuffer]
      safe_coders):
    for transform_id, tag, buffer_id, si in side_input_infos:
      _, pcoll_id = split_buffer_id(buffer_id)
      value_coder = context.coders[safe_coders[
          pipeline_components.pcollections[pcoll_id].coder_id]]
      elements_by_window = _WindowGroupingBuffer(si, value_coder)
      for element_data in pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)
      for key, window, elements_data in elements_by_window.encoded_items():
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id=transform_id,
                side_input_id=tag,
                window=window,
                key=key))
        worker_handler.state.append_raw(state_key, elements_data)

  @staticmethod
  def _add_residuals_and_channel_splits_to_deferred_inputs(
      process_bundle_descriptor, splits, get_input_coder_callable,
      last_sent, deferred_inputs):
    prev_stops = {}
    for split in splits:
      for delayed_application in split.residual_roots:

        input_op_name = _FnApiRunnerExecution.get_input_operation_name(
            process_bundle_descriptor,
            delayed_application.application.transform_id,
            delayed_application.application.input_id)

        deferred_inputs[input_op_name].append(
            delayed_application.application.element)
      for channel_split in split.channel_splits:
        coder_impl = get_input_coder_callable(channel_split.transform_id)
        # TODO(SDF): This requires deterministic ordering of buffer iteration.
        # TODO(SDF): The return split is in terms of indices.  Ideally,
        # a runner could map these back to actual positions to effectively
        # describe the two "halves" of the now-split range.  Even if we have
        # to buffer each element we send (or at the very least a bit of
        # metadata, like position, about each of them) this should be doable
        # if they're already in memory and we are bounding the buffer size
        # (e.g. to 10mb plus whatever is eagerly read from the SDK).  In the
        # case of non-split-points, we can either immediately replay the
        # "non-split-position" elements or record them as we do the other
        # delayed applications.

        # Decode and recode to split the encoded buffer by element index.
        all_elements = list(
            coder_impl.decode_all(
                b''.join(last_sent[channel_split.transform_id])))
        residual_elements = all_elements[
            channel_split.first_residual_element : prev_stops.get(
                channel_split.transform_id, len(all_elements)) + 1]
        if residual_elements:
          deferred_inputs[channel_split.transform_id].append(
              coder_impl.encode_all(residual_elements))
        prev_stops[
            channel_split.transform_id] = channel_split.last_primary_element


class _ProcessingQueueManager(object):
  """Manages the queues for ProcessBundle inputs.

  There are three queues:
   - ready_inputs(_ProcessingQueueManager.KeyedQueue). This queue contains input
       data that is ready to be processed. These are data such as timers past
       their trigger time, and data to be processed.
       The ready_inputs_queue contains tuples of (stage_name, inputs), where
       inputs are dictionaries mapping PCollection name to data buffers.

   - watermark_pending_inputs(_ProcessingQueueManager.KeyedQueue). This queue
       contains input data that is not yet ready to be processed, and is blocked
       on the watermark advancing. ((stage_name, watermark), inputs), where
       the watermark is the watermark at which the inputs should be scheudled,
       and inputs are dictionaries mapping PCollection name to data buffers.
  """
  class KeyedQueue(object):
    def __init__(self):
      self._q = collections.deque()
      self._keyed_elements = {}

    def enque(
        self,
        elm  # type: Tuple[str, Dict[str, Union[_ListBuffer, _GroupingBuffer]]]
    ):
      # type: (...) -> None
      key = elm[0]
      incoming_inputs = elm[1]
      if key in self._keyed_elements:
        existing_inputs = self._keyed_elements[key][1]
        for pcoll in incoming_inputs:
          existing_inputs[pcoll].extend(incoming_inputs[pcoll])
      else:
        self._keyed_elements[key] = elm
        self._q.appendleft(elm)

    def deque(self):
      elm = self._q.pop()
      key = elm[0]
      del self._keyed_elements[key]
      return elm

    def __len__(self):
      return len(self._q)

    def __repr__(self):
      return '<%s at 0x%x>' % (str(self),
                               id(self))

    def __str__(self):
      return '<%s %s>' % (self.__class__.__name__,
                          list(self._q))

  def __init__(self):
    self.watermark_pending_inputs = _ProcessingQueueManager.KeyedQueue()
    self.ready_inputs = _ProcessingQueueManager.KeyedQueue()

  def __str__(self):
    return '_ProcessingQueueManager(%s)' % self.__dict__


class FnApiRunner(runner.PipelineRunner):

  def __init__(
      self,
      default_environment=None,  # type: Optional[environments.Environment]
      bundle_repeat=0,
      use_state_iterables=False,
      provision_info=None,  # type: Optional[ExtendedProvisionInfo]
      progress_request_frequency=None):
    # type: (...) -> None

    """Creates a new Fn API Runner.

    Args:
      default_environment: the default environment to use for UserFns.
      bundle_repeat: replay every bundle this many extra times, for profiling
          and debugging
      use_state_iterables: Intentionally split gbk iterables over state API
          (for testing).
      provision_info: provisioning info to make available to workers, or None
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
    """
    super(FnApiRunner, self).__init__()
    self._last_uid = -1
    self._default_environment = (
        default_environment or environments.EmbeddedPythonEnvironment())
    self._bundle_repeat = bundle_repeat
    self._num_workers = 1
    self._progress_frequency = progress_request_frequency
    self._profiler_factory = None  # type: Optional[Callable[[...], profiler.Profile]]
    self._use_state_iterables = use_state_iterables
    self._provision_info = provision_info or ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            retrieval_token='unused-retrieval-token'))

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  @staticmethod
  def supported_requirements():
    return (
        common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn,
        common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn,
        common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn,
    )

  def run_pipeline(self,
                   pipeline,  # type: Pipeline
                   options  # type: pipeline_options.PipelineOptions
                  ):
    # type: (...) -> RunnerResult
    RuntimeValueProvider.set_runtime_options({})

    # Setup "beam_fn_api" experiment options if lacked.
    experiments = (
        options.view_as(pipeline_options.DebugOptions).experiments or [])
    if not 'beam_fn_api' in experiments:
      experiments.append('beam_fn_api')
    options.view_as(pipeline_options.DebugOptions).experiments = experiments

    # This is sometimes needed if type checking is disabled
    # to enforce that the inputs (and outputs) of GroupByKey operations
    # are known to be KVs.
    from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
    # TODO: Move group_by_key_input_visitor() to a non-dataflow specific file.
    pipeline.visit(DataflowRunner.group_by_key_input_visitor())
    self._bundle_repeat = self._bundle_repeat or options.view_as(
        pipeline_options.DirectOptions).direct_runner_bundle_repeat
    self._num_workers = options.view_as(
        pipeline_options.DirectOptions).direct_num_workers or self._num_workers

    # set direct workers running mode if it is defined with pipeline options.
    running_mode = \
      options.view_as(pipeline_options.DirectOptions).direct_running_mode
    if running_mode == 'multi_threading':
      self._default_environment = environments.EmbeddedPythonGrpcEnvironment()
    elif running_mode == 'multi_processing':
      command_string = '%s -m apache_beam.runners.worker.sdk_worker_main' \
                    % sys.executable
      self._default_environment = environments.SubprocessSDKEnvironment(
          command_string=command_string)

    self._profiler_factory = profiler.Profile.factory_from_options(
        options.view_as(pipeline_options.ProfilingOptions))

    self._latest_run_result = self.run_via_runner_api(
        pipeline.to_runner_api(default_environment=self._default_environment))
    return self._latest_run_result

  def run_via_runner_api(self, pipeline_proto):
    # type: (beam_runner_api_pb2.Pipeline) -> RunnerResult
    self._validate_requirements(pipeline_proto)
    self._check_requirements(pipeline_proto)
    stage_context, stages = self.create_stages(pipeline_proto)
    # TODO(pabloem, BEAM-7514): Create a watermark manager (that has access to
    #   the teststream (if any), and all the stages).
    return self.run_stages(stage_context, stages)

  @contextlib.contextmanager
  def maybe_profile(self):
    if self._profiler_factory:
      try:
        profile_id = 'direct-' + subprocess.check_output([
            'git', 'rev-parse', '--abbrev-ref', 'HEAD'
        ]).decode(errors='ignore').strip()
      except subprocess.CalledProcessError:
        profile_id = 'direct-unknown'
      profiler = self._profiler_factory(profile_id, time_prefix='')
    else:
      profiler = None

    if profiler:
      with profiler:
        yield
      if not self._bundle_repeat:
        _LOGGER.warning(
            'The --direct_runner_bundle_repeat option is not set; '
            'a significant portion of the profile may be one-time overhead.')
      path = profiler.profile_output
      print('CPU Profile written to %s' % path)
      try:
        import gprof2dot  # pylint: disable=unused-import
        if not subprocess.call([sys.executable,
                                '-m',
                                'gprof2dot',
                                '-f',
                                'pstats',
                                path,
                                '-o',
                                path + '.dot']):
          if not subprocess.call(
              ['dot', '-Tsvg', '-o', path + '.svg', path + '.dot']):
            print(
                'CPU Profile rendering at file://%s.svg' %
                os.path.abspath(path))
      except ImportError:
        # pylint: disable=superfluous-parens
        print('Please install gprof2dot and dot for profile renderings.')

    else:
      # Empty context.
      yield

  def _validate_requirements(self, pipeline_proto):
    """As a test runner, validate requirements were set correctly."""
    expected_requirements = set()

    def add_requirements(transform_id):
      transform = pipeline_proto.components.transforms[transform_id]
      if transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if payload.requests_finalization:
          expected_requirements.add(
              common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn)
        if (payload.state_specs or payload.timer_specs or
            payload.timer_family_specs):
          expected_requirements.add(
              common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn)
        if payload.requires_stable_input:
          expected_requirements.add(
              common_urns.requirements.REQUIRES_STABLE_INPUT.urn)
        if payload.requires_time_sorted_input:
          expected_requirements.add(
              common_urns.requirements.REQUIRES_TIME_SORTED_INPUT.urn)
        if payload.restriction_coder_id:
          expected_requirements.add(
              common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn)
      else:
        for sub in transform.subtransforms:
          add_requirements(sub)

    for root in pipeline_proto.root_transform_ids:
      add_requirements(root)
    if not expected_requirements.issubset(pipeline_proto.requirements):
      raise ValueError(
          'Missing requirement declaration: %s' %
          (expected_requirements - set(pipeline_proto.requirements)))

  def _check_requirements(self, pipeline_proto):
    """Check that this runner can satisfy all pipeline requirements."""
    supported_requirements = set(self.supported_requirements())
    for requirement in pipeline_proto.requirements:
      if requirement not in supported_requirements:
        raise ValueError(
            'Unable to run pipeline with requirement: %s' % requirement)

  def create_stages(
      self,
      pipeline_proto  # type: beam_runner_api_pb2.Pipeline
  ):
    # type: (...) -> Tuple[translations.TransformContext, List[translations.Stage]]
    return translations.create_and_optimize_stages(
        copy.deepcopy(pipeline_proto),
        phases=[
            translations.annotate_downstream_side_inputs,
            translations.fix_side_input_pcoll_coders,
            translations.lift_combiners,
            translations.expand_sdf,
            translations.expand_gbk,
            translations.sink_flattens,
            translations.greedily_fuse,
            translations.read_to_impulse,
            translations.impulse_to_input,
            translations.inject_timer_pcollections,
            translations.sort_stages,
            translations.window_pcollection_coders
        ],
        known_runner_urns=frozenset([
            common_urns.primitives.FLATTEN.urn,
            common_urns.primitives.GROUP_BY_KEY.urn
        ]),
        use_state_iterables=self._use_state_iterables)

  @staticmethod
  def _enqueue_all_initial_inputs(
      stages,  # type: List[fn_api_runner_transforms.Stage]
      input_queue_manager  # type: _ProcessingQueueManager
  ):
    # type: (...) -> None
    """Put all initial inputs to the pipeline in the input queue."""
    for stage in stages:
      data_inputs = {}
      ready_to_schedule = True
      for transform in stage.transforms:
        if (transform.spec.urn == bundle_processor.DATA_INPUT_URN and
            transform.spec.payload == fn_api_runner_transforms.IMPULSE_BUFFER):
          # If a transform is a DATA_INPUT or DATA_OUTPUT transform, it will
          # receive data from / deliver data to the runner.
          # For data input transforms, they only have one input.
          # We map the transform name to a buffer containing the
          # encoded input.
          data_inputs[transform.unique_name] = _ListBuffer(
              [ENCODED_IMPULSE_VALUE])
        elif transform.spec.urn in fn_api_runner_transforms.PAR_DO_URNS:
          payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
          if payload.side_inputs:
            # If the stage needs side inputs, then it's not ready to be
            # executed.
            ready_to_schedule = False
      if data_inputs and ready_to_schedule:
        # We push the data inputs, along with the name of the consuming stage.
        logging.debug('Scheduling stage for execution. Stage name: %s',
                      stage.name)
        input_queue_manager.ready_inputs.enque((stage.name, data_inputs))
      elif data_inputs and not ready_to_schedule:
        logging.debug('Enqueuing stage pending watermark. Stage name: %s',
                      stage.name)
        input_queue_manager.watermark_pending_inputs.enque(
            ((stage.name, timestamp.MAX_TIMESTAMP), data_inputs))

  def run_stages(self,
                 stage_context,  # type: translations.TransformContext
                 stages  # type: List[translations.Stage]
                ):
    # type: (...) -> RunnerResult

    """Run a list of topologically-sorted stages in batch mode.

    Args:
      stage_context (translations.TransformContext)
      stages (list[fn_api_runner.translations.Stage])
    """
    worker_handler_manager = WorkerHandlerManager(
        stage_context.components.environments, self._provision_info)
    input_queue_manager = _ProcessingQueueManager()
    pcoll_buffers = collections.defaultdict(_ListBuffer)  # type: DefaultDict[bytes, _ListBuffer]
    monitoring_infos_by_stage = {}
    metrics_by_stage = {}

    execution_context = PipelineExecutionContext(stage_context.components,
                                                 stages)
    execution_context.show()

    self._enqueue_all_initial_inputs(stages, input_queue_manager)
    try:
      with self.maybe_profile():
        while len(input_queue_manager.ready_inputs) > 0:
          stage_and_ready_elements = input_queue_manager.ready_inputs.deque()
          consuming_stage_name = stage_and_ready_elements[0]
          next_ready_elements = stage_and_ready_elements[1]

          stage = execution_context.stages_per_name[consuming_stage_name].stage
          data_input, data_output = execution_context.endpoints_per_stage[
              stage.name]

          for transform_name, _ in data_input.items():
            if transform_name in next_ready_elements:
              continue
            # Empty-filling the data input.
            next_ready_elements[transform_name] = _ListBuffer()
          endpoints_for_execution = (next_ready_elements, data_output)

          logging.info('Executing bundle for stage %s', stage.name)
          logging.debug('Endpoints for execution: %s', endpoints_for_execution)
          stage_results, output_bundles, deferred_inputs = self._execute_bundle(
              execution_context,
              worker_handler_manager.get_worker_handlers,
              stage_context.components,
              stage,
              pcoll_buffers,
              stage_context.safe_coders,
              endpoints_for_execution)
          self._merge_metrics_from_stage_execution(monitoring_infos_by_stage,
                                                   metrics_by_stage,
                                                   stage_results, stage.name)

          output_bundles_with_timestamps = self._update_watermarks(
              execution_context, stage, data_input, output_bundles,
              deferred_inputs)

          for k, v in deferred_inputs.items():
            # TODO(pabloem, MUST): SOMEHOW ENQUEUE THE DEFERRED INPUTS.
            #  this is not a nice way.
            input_queue_manager.ready_inputs.enque((stage.name, {k: v}))

          self._process_output_bundles(execution_context,
                                       output_bundles_with_timestamps,
                                       input_queue_manager)
          self._schedule_newly_ready_bundles(execution_context,
                                             input_queue_manager)

    finally:
      worker_handler_manager.close_all()

    if len(input_queue_manager.watermark_pending_inputs) > 0:
      raise RuntimeError('There are bundles pending processing that can not '
                         'be scheduled because the watermark is stuck. '
                         'This represents an error in Apache Beam.'
                         '\n\t%s', input_queue_manager.watermark_pending_inputs)
    return RunnerResult(
        runner.PipelineState.DONE, monitoring_infos_by_stage, metrics_by_stage)

  @staticmethod
  def _merge_metrics_from_stage_execution(
      monitoring_infos_by_stage,  # type: Dict[str, List[Any]]
      metrics_by_stage,  # type: Dict[str, List[beam_fn_api_pb2.Metrics]]
      stage_results,  # type: beam_fn_api_pb2.InstructionResponse
      stage_name  # type: str
  ):
    # type: (...) -> None
    stage_minfos = stage_results.process_bundle.monitoring_infos
    if stage_name in monitoring_infos_by_stage:
      stage_minfos = monitoring_infos.consolidate(
          itertools.chain(stage_minfos, monitoring_infos_by_stage[stage_name]))

    monitoring_infos_by_stage[stage_name] = stage_minfos

    # TODO(pabloem): Note that these are not merged.
    metrics_by_stage[stage_name] = stage_results.process_bundle.metrics

  @staticmethod
  def _schedule_newly_ready_bundles(execution_context, input_queue_manager):
    # type: (PipelineExecutionContext, _ProcessingQueueManager) -> None
    """Inspect watermark pending bundles, and schedule ready ones.

    This function should be called after the watermarks have been recalculated.
    """
    requed_inputs = []
    while len(input_queue_manager.watermark_pending_inputs) > 0:
      next_elm = input_queue_manager.watermark_pending_inputs.deque()
      (stage_name, bundle_watermark), inputs = next_elm
      stage_wm_mgr = execution_context.watermark_manager.get_node(stage_name)
      stage_watermark = stage_wm_mgr.input_watermark()
      if stage_watermark >= bundle_watermark:
        logging.debug('Enqueuing bundle for execution. Stage: %s', stage_name)
        input_queue_manager.ready_inputs.enque((stage_name, inputs))
      else:
        logging.debug(
            'Unable to enqueue bundle for stage %s'
            '\n\tStage input watermark: %s'
            '\n\tBundle scheduled watermark: %s',
            stage_name, stage_watermark, bundle_watermark)
        requed_inputs.append(((stage_name, bundle_watermark), inputs))

    for elm in requed_inputs:
      input_queue_manager.watermark_pending_inputs.enque(elm)

  def _update_watermarks(
      self,
      execution_context,  # type: PipelineExecutionContext
      stage,  # type: fn_api_runner_transforms.Stage
      input_bundles,  # type: Dict[str, bytes]
      output_bundles,  # type: List[Tuple[bytes, List[bytes]]]
      deferred_inputs  # type: Dict[str, bytes]
  ):
    # type: (...) -> List[Tuple[Tuple[timestam.Timestamp, bytes], List[bytes]]]
    """Update the watermarks after execution of a bundle.

    It also recomputes the output bundle list to contain consumer and target
    watermark (i.e. the input watermark of the consumer to be scheduled)."""

    # Map Timer reading transform to Timer write PCollection
    timer_read_transform_to_written_pcoll_map = {}
    timer_written_pcoll_to_read_pcoll_map = {}
    for reading_transform, written_pcoll in stage.timer_pcollections:
      timer_read_transform_to_written_pcoll_map[
          reading_transform] = written_pcoll

    for consumer, buffer_id in input_bundles.items():
      if consumer in deferred_inputs and deferred_inputs[consumer]:
        # If there are any deferred inputs, we hold the watermark back.
        # This should also hold back the watermarks for all downstream
        # PCollections.
        kind, pcoll_name = split_buffer_id(buffer_id)
        logging.debug('Setting watermark due to deferred inputs '
                      'from PCollection: %s', pcoll_name)
        execution_context.update_pcoll_watermark(pcoll_name,
                                                 timestamp.MIN_TIMESTAMP)
        continue

      if buffer_id == fn_api_runner_transforms.IMPULSE_BUFFER:
        pcoll_name = '%s%s' % (stage.name,
                               fn_api_runner_transforms.IMPULSE_BUFFER)
        # An impulse-typed input immediately moves to +inf
        execution_context.update_pcoll_watermark(pcoll_name,
                                                 timestamp.MAX_TIMESTAMP)
        continue
      else:
        kind, pcoll_name = split_buffer_id(buffer_id)
        if kind == 'timers':
          # For timers, we map the PCollection that's written, and the
          # PCollection that's read by the runner. These will be observed in the
          # data output later.
          written_pcoll = timer_read_transform_to_written_pcoll_map[consumer]
          timer_written_pcoll_to_read_pcoll_map[written_pcoll] = pcoll_name
          continue
        else:
          # TODO(pabloem, MUST): We'll have to handle this better on streaming
          execution_context.update_pcoll_watermark(pcoll_name,
                                                   timestamp.MAX_TIMESTAMP)

    output_bundles_with_target_watermarks = []

    # Starting with timers, as they affect input PCollections, and therefore
    # the input watermark of the current stage.
    for buffer_id, output_bytes in output_bundles:
      kind, pcoll_name = split_buffer_id(buffer_id)
      watermark_hold = timestamp.MAX_TIMESTAMP
      single_timer_per_window_key = _ListBuffer()
      if kind == 'timers':
        # For timers, pcoll_name is the written PCollection.
        if output_bytes:
          coder_id = execution_context.pipeline_components.pcollections[
              pcoll_name].coder_id
          windowed_timer_coder_impl = self.pipeline_context.coders[
              coder_id].get_impl()

          timer_data_per_key_window = {}
          # Keep only the "last" timer set per key and window.
          for elements_data in output_bytes:
            input_stream = create_InputStream(elements_data)
            while input_stream.size() > 0:
              windowed_key_timer = windowed_timer_coder_impl.decode_from_stream(
                  input_stream, True)
              timer_timestamp = windowed_key_timer.timestamp
              # TODO(pabloem, MUST): What to do about multiple windows?
              key_window_pair = (windowed_key_timer.windows[0],
                                 windowed_key_timer.value[0])
              timer_data_per_key_window[key_window_pair] = windowed_key_timer
              watermark_hold = min(watermark_hold, timer_timestamp)

          for t in timer_data_per_key_window.values():
            out = create_OutputStream()
            windowed_timer_coder_impl.encode_to_stream(t, out, True)
            single_timer_per_window_key.append(out.get())

          # For batch, after nothing comes in from the timers, we're done.
          # TODO(pabloem, MUST): What if timers are set BEFORE the current
          #   watermark for this pcollection? This should be handled by SDK?
        # We update the written and read collections' watermars.
        read_pcoll_name = timer_written_pcoll_to_read_pcoll_map[pcoll_name]
        execution_context.update_pcoll_watermark(read_pcoll_name,
                                                 watermark_hold)
        execution_context.update_pcoll_watermark(pcoll_name,
                                                 watermark_hold)
        output_bundles_with_target_watermarks.append(
            ((watermark_hold, buffer_id), single_timer_per_window_key))

    for buffer_id, output_bytes in output_bundles:
      kind, pcoll_name = split_buffer_id(buffer_id)
      if kind != 'timers':
        output_bundles_with_target_watermarks.append(
            ((timestamp.MAX_TIMESTAMP, buffer_id), output_bytes))

    return output_bundles_with_target_watermarks

  @staticmethod
  def _process_output_bundles(execution_context,
                              output_bundles,
                              input_queue_manager):
    """TODO(pabloem)"""
    logging.debug('Processing output bundles: %s', output_bundles)
    for (bundle_watermark, buffer_id), data in output_bundles:
      consumer_stage = execution_context.get_consuming_stage(buffer_id)
      consumer_transform = execution_context.get_consuming_transform(buffer_id)

      consuming_stage_name = (consumer_stage.name if consumer_stage else None)
      consuming_transform_name = (consumer_transform.unique_name
                                  if consumer_transform else None)
      kind, _ = split_buffer_id(buffer_id)
      if not consuming_stage_name or not consuming_transform_name:
        # This means that the PCollection is not consumed by any transforms,
        # and we can discard the elements.
        continue

      if not data and kind == 'timers':
        # Empty timer collections should not be rescheduled.
        continue

      # In this case, `data` may be an empty buffer, but we still schedule it
      # for execution (unless it's a timer pcollection) because otherwise the
      # watermark for the consumer will not be pushed forward later.
      # As mentioned above, empty timer bundles are not scheduled again,
      # because they hold back the pipeline.
      stage_watermark_mgr = execution_context.watermark_manager.get_node(
          consuming_stage_name)
      stage_input_watermark = stage_watermark_mgr.input_watermark()
      if stage_input_watermark >= bundle_watermark:
        logging.debug('Enqueuing bundle output after execution, '
                      'to be consumed by stage: %s', consuming_stage_name)
        input_queue_manager.ready_inputs.enque(
            (consuming_stage_name, {consuming_transform_name: data}))
      else:
        input_queue_manager.watermark_pending_inputs.enque(
            ((consuming_stage_name, bundle_watermark),
             {consuming_transform_name: data}))

  def _run_bundle_multiple_times_for_testing(
      self,
      worker_handler_list,  # type: Sequence[WorkerHandler]
      process_bundle_descriptor,
      data_input,
      data_output,  # type: DataOutput
      get_input_coder_callable,
      cache_token_generator
  ):
    # type: (...) -> None

    """
    If bundle_repeat > 0, replay every bundle for profiling and debugging.
    """
    # all workers share state, so use any worker_handler.
    worker_handler = worker_handler_list[0]
    for k in range(self._bundle_repeat):
      try:
        worker_handler.state.checkpoint()
        testing_bundle_manager = ParallelBundleManager(
            worker_handler_list,
            lambda pcoll_id,
            transform_id: _ListBuffer(coder_impl=get_input_coder_callable),
            get_input_coder_callable,
            process_bundle_descriptor,
            self._progress_frequency,
            k,
            num_workers=self._num_workers,
            cache_token_generator=cache_token_generator)
        testing_bundle_manager.process_bundle(data_input, data_output)
      finally:
        worker_handler.state.restore()

  def _execute_bundle(self,
                      execution_context,
                      worker_handler_factory,
                      pipeline_components,
                      stage,
                      pcoll_buffers,
                      safe_coders,
                      stage_endpoints):
    """Run an individual stage.

    Args:
      execution_context (PipelineExecutionContext): Context with
        execution information for the whole pipeline.
      worker_handler_factory: A ``callable`` that takes in an environment, and
        returns a ``WorkerHandler`` class.
      pipeline_components (beam_runner_api_pb2.Components): TODO
      stage (translations.Stage)
      pcoll_buffers (collections.defaultdict of str: list): Mapping of
        PCollection IDs to list that functions as buffer for the
        ``beam.PCollection``.
      safe_coders (dict): A map from coder URNs to the URN of a safe coder.

    Returns:
      A tuple of result and output bundles.
    """
    worker_handler_list = worker_handler_factory(
        stage.environment, self._num_workers)

    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    worker_handler = next(iter(worker_handler_list))
    self.pipeline_context = PipelineContext(
        pipeline_components,
        iterable_state_write=_FnApiRunnerExecution.make_iterable_state_write(
            worker_handler))

    # TODO(pabloem, MUST): Deal with this hack.
    pipeline_context = self.pipeline_context
    data_api_service_descriptor = worker_handler.data_api_service_descriptor()

    _LOGGER.info('Running %s', stage.name)
    input_bundle, data_output = stage_endpoints
    self._update_transform_spec_with_data_spec(stage,
                                               pipeline_components,
                                               data_api_service_descriptor)

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms={
            transform.unique_name: transform
            for transform in stage.transforms
        },
        pcollections=dict(pipeline_components.pcollections.items()),
        coders=dict(pipeline_components.coders.items()),
        windowing_strategies=dict(
            pipeline_components.windowing_strategies.items()),
        environments=dict(pipeline_components.environments.items()))

    state_api_service_descriptor = worker_handler.state_api_service_descriptor()
    if state_api_service_descriptor:
      process_bundle_descriptor.state_api_service_descriptor.url = (
          state_api_service_descriptor.url)

    # Change cache token across bundle repeats
    cache_token_generator = FnApiRunner.get_cache_token_generator(static=False)

    self._run_bundle_multiple_times_for_testing(
        worker_handler_list,
        process_bundle_descriptor,
        input_bundle,
        data_output,
        _FnApiRunnerExecution.make_input_coder_getter(pipeline_context,
                                                      process_bundle_descriptor,
                                                      safe_coders),
        cache_token_generator=cache_token_generator)

    bundle_manager = ParallelBundleManager(
        worker_handler_list,

        _FnApiRunnerExecution.make_input_buffer_fetcher(pipeline_context,
                                                        pcoll_buffers,
                                                        pipeline_components,
                                                        safe_coders),
        _FnApiRunnerExecution.make_input_coder_getter(pipeline_context,
                                                      process_bundle_descriptor,
                                                      safe_coders),
        process_bundle_descriptor,
        self._progress_frequency,
        num_workers=self._num_workers,
        cache_token_generator=cache_token_generator)

    result, splits = bundle_manager.process_bundle(input_bundle, data_output)

    for si in stage.downstream_side_inputs:
      side_input_infos = execution_context.get_side_input_infos(si)
      _FnApiRunnerExecution._store_side_inputs_in_state(worker_handler,
                                                        pipeline_context,
                                                        pipeline_components,
                                                        side_input_infos,
                                                        pcoll_buffers,
                                                        safe_coders)

    output_bundles = []
    for _, buffer_id in data_output.items():
      # A buffer_id is a kind:pcollection string.
      new_bundle = pcoll_buffers[buffer_id]
      del pcoll_buffers[buffer_id]
      output_bundles.append((buffer_id, new_bundle))
    last_result = result
    last_sent = input_bundle

    deferred_inputs = FnApiRunner._collect_deferred_inputs(
        pipeline_context,
        process_bundle_descriptor,
        safe_coders,
        last_result,
        splits,
        last_sent)  # type: DefaultDict[str, _ListBuffer]

    # TODO(pabloem, MUST): NOTE when working with deferred inputs:
    #  We cannot split deferred_input until we include residual_roots to
    #  merged results. Without residual_roots, pipeline stops earlier and we
    #  may miss some data.

    return result, output_bundles, deferred_inputs

  @staticmethod
  def _collect_deferred_inputs(pipeline_context,
                               process_bundle_descriptor,
                               safe_coders,
                               last_result,
                               splits,
                               last_input):
    deferred_inputs = collections.defaultdict(_ListBuffer)

    # Queue any process-initiated delayed bundle applications.
    for delayed_application in last_result.process_bundle.residual_roots:
      residual_root_op_name = _FnApiRunnerExecution.get_input_operation_name(
          process_bundle_descriptor,
          delayed_application.application.transform_id,
          delayed_application.application.input_id)
      deferred_inputs[residual_root_op_name].append(
          delayed_application.application.element)

    # Queue any runner-initiated delayed bundle applications.
    _FnApiRunnerExecution._add_residuals_and_channel_splits_to_deferred_inputs(
        process_bundle_descriptor,
        splits,
        _FnApiRunnerExecution.make_input_coder_getter(
            pipeline_context,
            process_bundle_descriptor,
            safe_coders),
        last_input,
        deferred_inputs)
    return deferred_inputs

  @staticmethod
  def _update_transform_spec_with_data_spec(stage,
                                            pipeline_components,
                                            data_api_service_descriptor):
    """Annotate a transform's with the data endpoint URL, and coder ID."""
    for transform in stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        # If a transform is a DATA_INPUT or DATA_OUTPUT transform, it will
        # receive data from / deliver data to the runner.
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = pipeline_components.pcollections[
              only_element(transform.outputs.values())].coder_id
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          coder_id = pipeline_components.pcollections[
              only_element(transform.inputs.values())].coder_id
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()

  # These classes are used to interact with the worker.

  class StateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer,
                      sdk_worker.StateHandler):
    class CopyOnWriteState(object):
      def __init__(self, underlying):
        # type: (DefaultDict[bytes, Buffer]) -> None
        self._underlying = underlying
        self._overlay = {}  # type: Dict[bytes, Buffer]

      def __getitem__(self, key):
        # type: (bytes) -> Buffer
        if key in self._overlay:
          return self._overlay[key]
        else:
          return FnApiRunner.StateServicer.CopyOnWriteList(
              self._underlying, self._overlay, key)

      def __delitem__(self, key):
        # type: (bytes) -> None
        self._overlay[key] = []

      def commit(self):
        # type: () -> DefaultDict[bytes, Buffer]
        self._underlying.update(self._overlay)
        return self._underlying

    class CopyOnWriteList(object):
      def __init__(self,
                   underlying,  # type: DefaultDict[bytes, Buffer]
                   overlay,  # type: Dict[bytes, Buffer]
                   key  # type: bytes
                  ):
        # type: (...) -> None
        self._underlying = underlying
        self._overlay = overlay
        self._key = key

      def __iter__(self):
        # type: () -> Iterator[bytes]
        if self._key in self._overlay:
          return iter(self._overlay[self._key])
        else:
          return iter(self._underlying[self._key])

      def append(self, item):
        # type: (bytes) -> None
        if self._key not in self._overlay:
          self._overlay[self._key] = list(self._underlying[self._key])
        self._overlay[self._key].append(item)

    StateType = Union[CopyOnWriteState, DefaultDict[bytes, Buffer]]

    def __init__(self):
      # type: () -> None
      self._lock = threading.Lock()
      self._state = collections.defaultdict(
          list)  # type: FnApiRunner.StateServicer.StateType
      self._checkpoint = None  # type: Optional[FnApiRunner.StateServicer.StateType]
      self._use_continuation_tokens = False
      self._continuations = {}  # type: Dict[bytes, Tuple[bytes, ...]]

    def checkpoint(self):
      # type: () -> None
      assert self._checkpoint is None and not \
        isinstance(self._state, FnApiRunner.StateServicer.CopyOnWriteState)
      self._checkpoint = self._state
      self._state = FnApiRunner.StateServicer.CopyOnWriteState(self._state)

    def commit(self):
      # type: () -> None
      assert isinstance(self._state,
                        FnApiRunner.StateServicer.CopyOnWriteState) and \
             isinstance(self._checkpoint,
                        FnApiRunner.StateServicer.CopyOnWriteState)
      self._state.commit()
      self._state = self._checkpoint.commit()
      self._checkpoint = None

    def restore(self):
      # type: () -> None
      assert self._checkpoint is not None
      self._state = self._checkpoint
      self._checkpoint = None

    @contextlib.contextmanager
    def process_instruction_id(self, unused_instruction_id):
      yield

    def get_raw(self,
                state_key,  # type: beam_fn_api_pb2.StateKey
                continuation_token=None  # type: Optional[bytes]
               ):
      # type: (...) -> Tuple[bytes, Optional[bytes]]
      with self._lock:
        full_state = self._state[self._to_key(state_key)]
        if self._use_continuation_tokens:
          # The token is "nonce:index".
          if not continuation_token:
            token_base = b'token_%x' % len(self._continuations)
            self._continuations[token_base] = tuple(full_state)
            return b'', b'%s:0' % token_base
          else:
            token_base, index = continuation_token.split(b':')
            ix = int(index)
            full_state_cont = self._continuations[token_base]
            if ix == len(full_state_cont):
              return b'', None
            else:
              return full_state_cont[ix], b'%s:%d' % (token_base, ix + 1)
        else:
          assert not continuation_token
          return b''.join(full_state), None

    def append_raw(
        self,
        state_key,  # type: beam_fn_api_pb2.StateKey
        data  # type: bytes
    ):
      # type: (...) -> _Future
      with self._lock:
        self._state[self._to_key(state_key)].append(data)
      return _Future.done()

    def clear(self, state_key):
      # type: (beam_fn_api_pb2.StateKey) -> _Future
      with self._lock:
        try:
          del self._state[self._to_key(state_key)]
        except KeyError:
          # This may happen with the caching layer across bundles. Caching may
          # skip this storage layer for a blocking_get(key) request. Without
          # the caching, the state for a key would be initialized via the
          # defaultdict that _state uses.
          pass
      return _Future.done()

    @staticmethod
    def _to_key(state_key):
      # type: (beam_fn_api_pb2.StateKey) -> bytes
      return state_key.SerializeToString()

  class GrpcStateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer):
    def __init__(self, state):
      # type: (FnApiRunner.StateServicer) -> None
      self._state = state

    def State(self,
              request_stream,  # type: Iterable[beam_fn_api_pb2.StateRequest]
              context=None
             ):
      # type: (...) -> Iterator[beam_fn_api_pb2.StateResponse]
      # Note that this eagerly mutates state, assuming any failures are fatal.
      # Thus it is safe to ignore instruction_id.
      for request in request_stream:
        request_type = request.WhichOneof('request')
        if request_type == 'get':
          data, continuation_token = self._state.get_raw(
              request.state_key, request.get.continuation_token)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              get=beam_fn_api_pb2.StateGetResponse(
                  data=data, continuation_token=continuation_token))
        elif request_type == 'append':
          self._state.append_raw(request.state_key, request.append.data)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id, append=beam_fn_api_pb2.StateAppendResponse())
        elif request_type == 'clear':
          self._state.clear(request.state_key)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id, clear=beam_fn_api_pb2.StateClearResponse())
        else:
          raise NotImplementedError('Unknown state request: %s' % request_type)

  class SingletonStateHandlerFactory(sdk_worker.StateHandlerFactory):
    """A singleton cache for a StateServicer."""
    def __init__(self, state_handler):
      # type: (sdk_worker.CachingStateHandler) -> None
      self._state_handler = state_handler

    def create_state_handler(self, api_service_descriptor):
      # type: (endpoints_pb2.ApiServiceDescriptor) -> sdk_worker.CachingStateHandler

      """Returns the singleton state handler."""
      return self._state_handler

    def close(self):
      # type: () -> None

      """Does nothing."""
      pass

  @staticmethod
  def get_cache_token_generator(static=True):
    """A generator for cache tokens.
       :arg static If True, generator always returns the same cache token
                   If False, generator returns a new cache token each time
       :return A generator which returns a cache token on next(generator)
    """
    def generate_token(identifier):
      return beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
          user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState(
          ),
          token="cache_token_{}".format(identifier).encode("utf-8"))

    class StaticGenerator(object):
      def __init__(self):
        self._token = generate_token(1)

      def __iter__(self):
        # pylint: disable=non-iterator-returned
        return self

      def __next__(self):
        return self._token

    class DynamicGenerator(object):
      def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

      def __iter__(self):
        # pylint: disable=non-iterator-returned
        return self

      def __next__(self):
        with self._lock:
          self._counter += 1
          return generate_token(self._counter)

    return StaticGenerator() if static else DynamicGenerator()


class WorkerHandler(object):
  """worker_handler for a worker.

  It provides utilities to start / stop the worker, provision any resources for
  it, as well as provide descriptors for the data, state and logging APIs for
  it.
  """

  _registered_environments = {}  # type: Dict[str, Tuple[ConstructorFn, type]]
  _worker_id_counter = -1
  _lock = threading.Lock()

  control_conn = None  # type: ControlConnection
  data_conn = None  # type: data_plane._GrpcDataChannel

  def __init__(self,
               control_handler,
               data_plane_handler,
               state,  # type: FnApiRunner.StateServicer
               provision_info  # type: Optional[ExtendedProvisionInfo]
              ):
    # type: (...) -> None

    """Initialize a WorkerHandler.

    Args:
      control_handler:
      data_plane_handler (data_plane.DataChannel):
      state:
      provision_info:
    """
    self.control_handler = control_handler
    self.data_plane_handler = data_plane_handler
    self.state = state
    self.provision_info = provision_info

    with WorkerHandler._lock:
      WorkerHandler._worker_id_counter += 1
      self.worker_id = 'worker_%s' % WorkerHandler._worker_id_counter

  def close(self):
    # type: () -> None
    self.stop_worker()

  def start_worker(self):
    # type: () -> None
    raise NotImplementedError

  def stop_worker(self):
    # type: () -> None
    raise NotImplementedError

  def data_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  def state_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  def logging_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  @classmethod
  def register_environment(
      cls,
      urn,  # type: str
      payload_type  # type: Optional[Type[T]]
  ):
    # type: (...) -> Callable[[Callable[[T, FnApiRunner.StateServicer, Optional[ExtendedProvisionInfo], GrpcServer], WorkerHandler]], Callable[[T, FnApiRunner.StateServicer, Optional[ExtendedProvisionInfo], GrpcServer], WorkerHandler]]
    def wrapper(constructor):
      cls._registered_environments[urn] = constructor, payload_type
      return constructor

    return wrapper

  @classmethod
  def create(cls,
             environment,  # type: beam_runner_api_pb2.Environment
             state,  # type: FnApiRunner.StateServicer
             provision_info,  # type: Optional[ExtendedProvisionInfo]
             grpc_server  # type: GrpcServer
            ):
    # type: (...) -> WorkerHandler
    constructor, payload_type = cls._registered_environments[environment.urn]
    return constructor(
        proto_utils.parse_Bytes(environment.payload, payload_type),
        state,
        provision_info,
        grpc_server)


@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON, None)
class EmbeddedWorkerHandler(WorkerHandler):
  """An in-memory worker_handler for fn API control, state and data planes."""

  def __init__(self,
               unused_payload,  # type: None
               state,  # type: sdk_worker.StateHandler
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               unused_grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(EmbeddedWorkerHandler, self).__init__(
        self, data_plane.InMemoryDataChannel(), state, provision_info)
    self.control_conn = self  # type: ignore  # need Protocol to describe this
    self.data_conn = self.data_plane_handler
    state_cache = StateCache(STATE_CACHE_SIZE)
    self.bundle_processor_cache = sdk_worker.BundleProcessorCache(
        FnApiRunner.SingletonStateHandlerFactory(
            sdk_worker.CachingStateHandler(state_cache, state)),
        data_plane.InMemoryDataChannelFactory(
            self.data_plane_handler.inverse()), {})
    self.worker = sdk_worker.SdkWorker(
        self.bundle_processor_cache,
        state_cache_metrics_fn=state_cache.get_monitoring_infos)
    self._uid_counter = 0

  def push(self, request):
    if not request.instruction_id:
      self._uid_counter += 1
      request.instruction_id = 'control_%s' % self._uid_counter
    response = self.worker.do_instruction(request)
    return ControlFuture(request.instruction_id, response)

  def start_worker(self):
    # type: () -> None
    pass

  def stop_worker(self):
    # type: () -> None
    self.bundle_processor_cache.shutdown()

  def done(self):
    # type: () -> None
    pass

  def data_api_service_descriptor(self):
    # type: () -> None
    return None

  def state_api_service_descriptor(self):
    # type: () -> None
    return None

  def logging_api_service_descriptor(self):
    # type: () -> None
    return None


class BasicLoggingService(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):

  LOG_LEVEL_MAP = {
      beam_fn_api_pb2.LogEntry.Severity.CRITICAL: logging.CRITICAL,
      beam_fn_api_pb2.LogEntry.Severity.ERROR: logging.ERROR,
      beam_fn_api_pb2.LogEntry.Severity.WARN: logging.WARNING,
      beam_fn_api_pb2.LogEntry.Severity.NOTICE: logging.INFO + 1,
      beam_fn_api_pb2.LogEntry.Severity.INFO: logging.INFO,
      beam_fn_api_pb2.LogEntry.Severity.DEBUG: logging.DEBUG,
      beam_fn_api_pb2.LogEntry.Severity.TRACE: logging.DEBUG - 1,
      beam_fn_api_pb2.LogEntry.Severity.UNSPECIFIED: logging.NOTSET,
  }

  def Logging(self, log_messages, context=None):
    yield beam_fn_api_pb2.LogControl()
    for log_message in log_messages:
      for log in log_message.log_entries:
        logging.log(self.LOG_LEVEL_MAP[log.severity], str(log))


class BasicProvisionService(beam_provision_api_pb2_grpc.ProvisionServiceServicer
                            ):
  def __init__(self, base_info, worker_manager):
    # type: (Optional[beam_provision_api_pb2.ProvisionInfo], WorkerHandlerManager) -> None
    self._base_info = base_info
    self._worker_manager = worker_manager

  def GetProvisionInfo(self, request, context=None):
    # type: (...) -> beam_provision_api_pb2.GetProvisionInfoResponse
    info = copy.copy(self._base_info)
    logging.error(('info', info, 'context', context))
    if context:
      worker_id = dict(context.invocation_metadata())['worker_id']
      worker = self._worker_manager.get_worker(worker_id)
      info.logging_endpoint.CopyFrom(worker.logging_api_service_descriptor())
      info.artifact_endpoint.CopyFrom(worker.artifact_api_service_descriptor())
      info.control_endpoint.CopyFrom(worker.control_api_service_descriptor())
      logging.error(('info', info, 'worker_id', worker_id))
    return beam_provision_api_pb2.GetProvisionInfoResponse(info=info)


class EmptyArtifactRetrievalService(
    beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer):
  def GetManifest(self, request, context=None):
    return beam_artifact_api_pb2.GetManifestResponse(
        manifest=beam_artifact_api_pb2.Manifest())

  def GetArtifact(self, request, context=None):
    raise ValueError('No artifacts staged.')


class GrpcServer(object):

  _DEFAULT_SHUTDOWN_TIMEOUT_SECS = 5

  def __init__(self,
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               worker_manager,  # type: WorkerHandlerManager
              ):
    # type: (...) -> None
    self.state = state
    self.provision_info = provision_info
    self.control_server = grpc.server(UnboundedThreadPoolExecutor())
    self.control_port = self.control_server.add_insecure_port('[::]:0')
    self.control_address = 'localhost:%s' % self.control_port

    # Options to have no limits (-1) on the size of the messages
    # received or sent over the data plane. The actual buffer size
    # is controlled in a layer above.
    no_max_message_sizes = [("grpc.max_receive_message_length", -1),
                            ("grpc.max_send_message_length", -1)]
    self.data_server = grpc.server(
        UnboundedThreadPoolExecutor(), options=no_max_message_sizes)
    self.data_port = self.data_server.add_insecure_port('[::]:0')

    self.state_server = grpc.server(
        UnboundedThreadPoolExecutor(), options=no_max_message_sizes)
    self.state_port = self.state_server.add_insecure_port('[::]:0')

    self.control_handler = BeamFnControlServicer()
    beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
        self.control_handler, self.control_server)

    # If we have provision info, serve these off the control port as well.
    if self.provision_info:
      if self.provision_info.provision_info:
        beam_provision_api_pb2_grpc.add_ProvisionServiceServicer_to_server(
            BasicProvisionService(
                self.provision_info.provision_info, worker_manager),
            self.control_server)

      if self.provision_info.artifact_staging_dir:
        service = artifact_service.BeamFilesystemArtifactService(
            self.provision_info.artifact_staging_dir
        )  # type: beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer
      else:
        service = EmptyArtifactRetrievalService()
      beam_artifact_api_pb2_grpc.add_ArtifactRetrievalServiceServicer_to_server(
          service, self.control_server)

    self.data_plane_handler = data_plane.BeamFnDataServicer(
        DATA_BUFFER_TIME_LIMIT_MS)
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
        self.data_plane_handler, self.data_server)

    beam_fn_api_pb2_grpc.add_BeamFnStateServicer_to_server(
        FnApiRunner.GrpcStateServicer(state), self.state_server)

    self.logging_server = grpc.server(
        UnboundedThreadPoolExecutor(), options=no_max_message_sizes)
    self.logging_port = self.logging_server.add_insecure_port('[::]:0')
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        BasicLoggingService(), self.logging_server)

    _LOGGER.info('starting control server on port %s', self.control_port)
    _LOGGER.info('starting data server on port %s', self.data_port)
    _LOGGER.info('starting state server on port %s', self.state_port)
    _LOGGER.info('starting logging server on port %s', self.logging_port)
    self.logging_server.start()
    self.state_server.start()
    self.data_server.start()
    self.control_server.start()

  def close(self):
    self.control_handler.done()
    to_wait = [
        self.control_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.data_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.state_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.logging_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS)
    ]
    for w in to_wait:
      w.wait()


class GrpcWorkerHandler(WorkerHandler):
  """An grpc based worker_handler for fn API control, state and data planes."""

  def __init__(self,
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    self._grpc_server = grpc_server
    super(GrpcWorkerHandler, self).__init__(
        self._grpc_server.control_handler,
        self._grpc_server.data_plane_handler,
        state,
        provision_info)
    self.state = state

    self.control_address = self.port_from_worker(self._grpc_server.control_port)
    self.control_conn = self._grpc_server.control_handler.get_conn_by_worker_id(
        self.worker_id)

    self.data_conn = self._grpc_server.data_plane_handler.get_conn_by_worker_id(
        self.worker_id)

  def control_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.control_port))

  def artifact_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.control_port))

  def data_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.data_port))

  def state_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.state_port))

  def logging_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.logging_port))

  def close(self):
    # type: () -> None
    self.control_conn.close()
    self.data_conn.close()
    super(GrpcWorkerHandler, self).close()

  def port_from_worker(self, port):
    return '%s:%s' % (self.host_from_worker(), port)

  def host_from_worker(self):
    return 'localhost'


@WorkerHandler.register_environment(
    common_urns.environments.EXTERNAL.urn, beam_runner_api_pb2.ExternalPayload)
class ExternalWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               external_payload,  # type: beam_runner_api_pb2.ExternalPayload
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(ExternalWorkerHandler,
          self).__init__(state, provision_info, grpc_server)
    self._external_payload = external_payload

  def start_worker(self):
    # type: () -> None
    stub = beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolStub(
        GRPCChannelFactory.insecure_channel(
            self._external_payload.endpoint.url))
    control_descriptor = endpoints_pb2.ApiServiceDescriptor(
        url=self.control_address)
    response = stub.StartWorker(
        beam_fn_api_pb2.StartWorkerRequest(
            worker_id=self.worker_id,
            control_endpoint=control_descriptor,
            artifact_endpoint=control_descriptor,
            provision_endpoint=control_descriptor,
            logging_endpoint=self.logging_api_service_descriptor(),
            params=self._external_payload.params))
    if response.error:
      raise RuntimeError("Error starting worker: %s" % response.error)

  def stop_worker(self):
    # type: () -> None
    pass

  def host_from_worker(self):
    # TODO(BEAM-8646): Reconcile the behavior on Windows platform.
    if sys.platform == 'win32':
      return 'localhost'
    import socket
    return socket.getfqdn()


@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON_GRPC, bytes)
class EmbeddedGrpcWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               payload,  # type: bytes
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(EmbeddedGrpcWorkerHandler,
          self).__init__(state, provision_info, grpc_server)

    from apache_beam.transforms.environments import EmbeddedPythonGrpcEnvironment
    config = EmbeddedPythonGrpcEnvironment.parse_config(payload.decode('utf-8'))
    self._state_cache_size = config.get('state_cache_size') or STATE_CACHE_SIZE
    self._data_buffer_time_limit_ms = \
        config.get('data_buffer_time_limit_ms') or DATA_BUFFER_TIME_LIMIT_MS

  def start_worker(self):
    # type: () -> None
    self.worker = sdk_worker.SdkHarness(
        self.control_address,
        state_cache_size=self._state_cache_size,
        data_buffer_time_limit_ms=self._data_buffer_time_limit_ms,
        worker_id=self.worker_id)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.daemon = True
    self.worker_thread.start()

  def stop_worker(self):
    # type: () -> None
    self.worker_thread.join()


# The subprocesses module is not threadsafe on Python 2.7. Use this lock to
# prevent concurrent calls to POpen().
SUBPROCESS_LOCK = threading.Lock()


@WorkerHandler.register_environment(python_urns.SUBPROCESS_SDK, bytes)
class SubprocessSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               worker_command_line,  # type: bytes
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(SubprocessSdkWorkerHandler,
          self).__init__(state, provision_info, grpc_server)
    self._worker_command_line = worker_command_line

  def start_worker(self):
    # type: () -> None
    from apache_beam.runners.portability import local_job_service
    self.worker = local_job_service.SubprocessSdkWorker(
        self._worker_command_line, self.control_address, self.worker_id)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.start()

  def stop_worker(self):
    # type: () -> None
    self.worker_thread.join()


@WorkerHandler.register_environment(
    common_urns.environments.DOCKER.urn, beam_runner_api_pb2.DockerPayload)
class DockerSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               payload,  # type: beam_runner_api_pb2.DockerPayload
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(DockerSdkWorkerHandler,
          self).__init__(state, provision_info, grpc_server)
    self._container_image = payload.container_image
    self._container_id = None  # type: Optional[bytes]

  def host_from_worker(self):
    if sys.platform == "darwin":
      # See https://docs.docker.com/docker-for-mac/networking/
      return 'host.docker.internal'
    else:
      return super(DockerSdkWorkerHandler, self).host_from_worker()

  def start_worker(self):
    # type: () -> None
    with SUBPROCESS_LOCK:
      try:
        subprocess.check_call(['docker', 'pull', self._container_image])
      except Exception:
        _LOGGER.info('Unable to pull image %s' % self._container_image)
      self._container_id = subprocess.check_output([
          'docker',
          'run',
          '-d',
          # TODO:  credentials
          '--network=host',
          self._container_image,
          '--id=%s' % self.worker_id,
          '--logging_endpoint=%s' % self.logging_api_service_descriptor().url,
          '--control_endpoint=%s' % self.control_address,
          '--artifact_endpoint=%s' % self.control_address,
          '--provision_endpoint=%s' % self.control_address,
      ]).strip()
      assert self._container_id is not None
      while True:
        status = subprocess.check_output([
            'docker', 'inspect', '-f', '{{.State.Status}}', self._container_id
        ]).strip()
        _LOGGER.info(
            'Waiting for docker to start up.Current status is %s' %
            status.decode('utf-8'))
        if status == b'running':
          _LOGGER.info(
              'Docker container is running. container_id = %s, '
              'worker_id = %s',
              self._container_id,
              self.worker_id)
          break
        elif status in (b'dead', b'exited'):
          subprocess.call(['docker', 'container', 'logs', self._container_id])
          raise RuntimeError(
              'SDK failed to start. Final status is %s' %
              status.decode('utf-8'))
      time.sleep(1)

  def stop_worker(self):
    # type: () -> None
    if self._container_id:
      with SUBPROCESS_LOCK:
        subprocess.call(['docker', 'kill', self._container_id])


class WorkerHandlerManager(object):
  """
  Manages creation of ``WorkerHandler``s.

  Caches ``WorkerHandler``s based on environment id.
  """
  def __init__(self,
               environments,  # type: Mapping[str, beam_runner_api_pb2.Environment]
               job_provision_info  # type: Optional[ExtendedProvisionInfo]
              ):
    # type: (...) -> None
    self._environments = environments
    self._job_provision_info = job_provision_info
    self._cached_handlers = collections.defaultdict(
        list)  # type: DefaultDict[str, List[WorkerHandler]]
    self._workers_by_id = {}  # type: Dict[str, WorkerHandler]
    self._state = FnApiRunner.StateServicer()  # rename?
    self._grpc_server = None  # type: Optional[GrpcServer]

  def get_worker_handlers(
      self,
      environment_id,  # type: Optional[str]
      num_workers  # type: int
  ):
    # type: (...) -> List[WorkerHandler]
    if environment_id is None:
      # Any environment will do, pick one arbitrarily.
      environment_id = next(iter(self._environments.keys()))
    environment = self._environments[environment_id]

    # assume all environments except EMBEDDED_PYTHON use gRPC.
    if environment.urn == python_urns.EMBEDDED_PYTHON:
      # special case for EmbeddedWorkerHandler: there's no need for a gRPC
      # server, but to pass the type check on WorkerHandler.create() we
      # make like we have a GrpcServer instance.
      self._grpc_server = cast(GrpcServer, None)
    elif self._grpc_server is None:
      self._grpc_server = GrpcServer(
          self._state, self._job_provision_info, self)

    worker_handler_list = self._cached_handlers[environment_id]
    if len(worker_handler_list) < num_workers:
      for _ in range(len(worker_handler_list), num_workers):
        worker_handler = WorkerHandler.create(
            environment,
            self._state,
            self._job_provision_info,
            self._grpc_server)
        _LOGGER.info(
            "Created Worker handler %s for environment %s",
            worker_handler,
            environment)
        self._cached_handlers[environment_id].append(worker_handler)
        self._workers_by_id[worker_handler.worker_id] = worker_handler
        worker_handler.start_worker()
    return self._cached_handlers[environment_id][:num_workers]

  def close_all(self):
    for worker_handler_list in self._cached_handlers.values():
      for worker_handler in set(worker_handler_list):
        try:
          worker_handler.close()
        except Exception:
          _LOGGER.error(
              "Error closing worker_handler %s" % worker_handler, exc_info=True)
    self._cached_handlers = {}
    self._workers_by_id = {}
    if self._grpc_server is not None:
      self._grpc_server.close()
      self._grpc_server = None

  def get_worker(self, worker_id):
    return self._workers_by_id[worker_id]


class ExtendedProvisionInfo(object):
  def __init__(self,
               provision_info=None,  # type: Optional[beam_provision_api_pb2.ProvisionInfo]
               artifact_staging_dir=None,
               job_name=None,  # type: Optional[str]
              ):
    self.provision_info = (
        provision_info or beam_provision_api_pb2.ProvisionInfo())
    self.artifact_staging_dir = artifact_staging_dir
    self.job_name = job_name


_split_managers = []


@contextlib.contextmanager
def split_manager(stage_name, split_manager):
  """Registers a split manager to control the flow of elements to a given stage.

  Used for testing.

  A split manager should be a coroutine yielding desired split fractions,
  receiving the corresponding split results. Currently, only one input is
  supported.
  """
  try:
    _split_managers.append((stage_name, split_manager))
    yield
  finally:
    _split_managers.pop()


class BundleManager(object):
  """Manages the execution of a bundle from the runner-side.

  This class receives a bundle descriptor, and performs the following tasks:
  - Registration of the bundle with the worker.
  - Splitting of the bundle
  - Setting up any other bundle requirements (e.g. side inputs).
  - Submitting the bundle to worker for execution
  - Passing bundle input data to the worker
  - Collecting bundle output data from the worker
  - Finalizing the bundle.
  """

  _uid_counter = 0
  _lock = threading.Lock()

  def __init__(self,
               worker_handler_list,  # type: Sequence[WorkerHandler]
               get_buffer,  # type: Callable[[bytes, str], PartitionableBuffer]
               get_input_coder_impl,  # type: Callable[[str], CoderImpl]
               bundle_descriptor,  # type: beam_fn_api_pb2.ProcessBundleDescriptor
               progress_frequency=None,
               skip_registration=False,
               cache_token_generator=FnApiRunner.get_cache_token_generator()
              ):
    """Set up a bundle manager.

    Args:
      worker_handler_list
      get_buffer (Callable[[str], list])
      get_input_coder_impl (Callable[[str], Coder])
      bundle_descriptor (beam_fn_api_pb2.ProcessBundleDescriptor)
      progress_frequency
      skip_registration
    """
    self._worker_handler_list = worker_handler_list
    self._get_buffer = get_buffer
    self._get_input_coder_impl = get_input_coder_impl
    self._bundle_descriptor = bundle_descriptor
    self._registered = skip_registration
    self._progress_frequency = progress_frequency
    self._worker_handler = None  # type: Optional[WorkerHandler]
    self._cache_token_generator = cache_token_generator

  def _send_input_to_worker(self,
                            process_bundle_id,  # type: str
                            read_transform_id,  # type: str
                            byte_streams
                           ):
    # type: (...) -> None
    assert self._worker_handler is not None
    data_out = self._worker_handler.data_conn.output_stream(
        process_bundle_id, read_transform_id)
    for byte_stream in byte_streams:
      data_out.write(byte_stream)
    data_out.close()

  def _register_bundle_descriptor(self):
    # type: () -> Optional[ControlFuture]
    if self._registered:
      registration_future = None
    else:
      assert self._worker_handler is not None
      process_bundle_registration = beam_fn_api_pb2.InstructionRequest(
          register=beam_fn_api_pb2.RegisterRequest(
              process_bundle_descriptor=[self._bundle_descriptor]))
      registration_future = self._worker_handler.control_conn.push(
          process_bundle_registration)
      self._registered = True

    return registration_future

  def _select_split_manager(self):
    """TODO(pabloem) WHAT DOES THIS DO"""
    unique_names = set(
        t.unique_name for t in self._bundle_descriptor.transforms.values())
    for stage_name, candidate in reversed(_split_managers):
      if (stage_name in unique_names or
          (stage_name + '/Process') in unique_names):
        split_manager = candidate
        break
    else:
      split_manager = None

    return split_manager

  def _generate_splits_for_testing(self,
                                   split_manager,
                                   inputs,  # type: Mapping[str, PartitionableBuffer]
                                   process_bundle_id):
    # type: (...) -> List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    read_transform_id, buffer_data = only_element(inputs.items())
    byte_stream = b''.join(buffer_data)
    num_elements = len(
        list(
            self._get_input_coder_impl(read_transform_id).decode_all(
                byte_stream)))

    # Start the split manager in case it wants to set any breakpoints.
    split_manager_generator = split_manager(num_elements)
    try:
      split_fraction = next(split_manager_generator)
      done = False
    except StopIteration:
      done = True

    # Send all the data.
    self._send_input_to_worker(
        process_bundle_id, read_transform_id, [byte_stream])

    assert self._worker_handler is not None

    # Execute the requested splits.
    while not done:
      if split_fraction is None:
        split_result = None
      else:
        split_request = beam_fn_api_pb2.InstructionRequest(
            process_bundle_split=beam_fn_api_pb2.ProcessBundleSplitRequest(
                instruction_id=process_bundle_id,
                desired_splits={
                    read_transform_id: beam_fn_api_pb2.
                    ProcessBundleSplitRequest.DesiredSplit(
                        fraction_of_remainder=split_fraction,
                        estimated_input_elements=num_elements)
                }))
        split_response = self._worker_handler.control_conn.push(
            split_request).get()  # type: beam_fn_api_pb2.InstructionResponse
        for t in (0.05, 0.1, 0.2):
          waiting = ('Instruction not running', 'not yet scheduled')
          if any(msg in split_response.error for msg in waiting):
            time.sleep(t)
            split_response = self._worker_handler.control_conn.push(
                split_request).get()
        if 'Unknown process bundle' in split_response.error:
          # It may have finished too fast.
          split_result = None
        elif split_response.error:
          raise RuntimeError(split_response.error)
        else:
          split_result = split_response.process_bundle_split
          split_results.append(split_result)
      try:
        split_fraction = split_manager_generator.send(split_result)
      except StopIteration:
        break
    return split_results

  def process_bundle(
      self,
      inputs,  # type: Mapping[str, PartitionableBuffer]
      expected_outputs  # type: DataOutput
  ):
    # type: (...) -> Tuple[beam_fn_api_pb2.ProcessBundleProgressResponse, List]
    # Unique id for the instruction processing this bundle.
    with BundleManager._lock:
      BundleManager._uid_counter += 1
      process_bundle_id = 'bundle_%s' % BundleManager._uid_counter
      self._worker_handler = self._worker_handler_list[
          BundleManager._uid_counter % len(self._worker_handler_list)]

    # Register the bundle descriptor, if needed - noop if already registered.
    registration_future = self._register_bundle_descriptor()
    # Check that the bundle was successfully registered.
    if registration_future and registration_future.get().error:
      raise RuntimeError(registration_future.get().error)

    split_manager = self._select_split_manager()
    if not split_manager:
      # If there is no split_manager, write all input data to the channel.
      for transform_id, elements in inputs.items():
        self._send_input_to_worker(process_bundle_id, transform_id, elements)

    # Actually start the bundle.
    process_bundle_req = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=self._bundle_descriptor.id,
            cache_tokens=[next(self._cache_token_generator)]))
    result_future = self._worker_handler.control_conn.push(process_bundle_req)

    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]

    # This represents the data that is output from the execution of the bundle.
    # this list will contain dictionaries mapping PCollection ids to encoded
    # elements. If those bundles should be consumed by an operation downstream,
    # they will be added to the processing queues.
    output_data = []
    with ProgressRequester(
        self._worker_handler, process_bundle_id, self._progress_frequency):

      if split_manager:
        split_results = self._generate_splits_for_testing(
            split_manager, inputs, process_bundle_id)

      # Retrieve the expected output data from the worker.
      for output in self._worker_handler.data_conn.input_elements(
          process_bundle_id,
          expected_outputs.keys(),
          abort_callback=lambda:
          (result_future.is_done() and result_future.get().error)):
        if output.transform_id in expected_outputs:
          with BundleManager._lock:
            self._get_buffer(
                expected_outputs[output.transform_id]).append(output.data)
            output_data.append({output.transform_id: output.data})

      _LOGGER.debug('Wait for the bundle %s to finish.' % process_bundle_id)
      result = result_future.get()  # type: beam_fn_api_pb2.InstructionResponse

    if result.error:
      raise RuntimeError(result.error)

    if result.process_bundle.requires_finalization:
      finalize_request = beam_fn_api_pb2.InstructionRequest(
          finalize_bundle=beam_fn_api_pb2.FinalizeBundleRequest(
              instruction_id=process_bundle_id))
      self._worker_handler.control_conn.push(finalize_request)

    return result, split_results


class ParallelBundleManager(BundleManager):

  def __init__(
      self,
      worker_handler_list,  # type: Sequence[WorkerHandler]
      get_buffer,  # type: Callable[[bytes, str], PartitionableBuffer]
      get_input_coder_impl,  # type: Callable[[str], CoderImpl]
      bundle_descriptor,  # type: beam_fn_api_pb2.ProcessBundleDescriptor
      progress_frequency=None,
      skip_registration=False,
      cache_token_generator=None,
      **kwargs):
    # type: (...) -> None
    super(ParallelBundleManager, self).__init__(
        worker_handler_list,
        get_buffer,
        get_input_coder_impl,
        bundle_descriptor,
        progress_frequency,
        skip_registration,
        cache_token_generator=cache_token_generator)
    self._num_workers = kwargs.pop('num_workers', 1)

  def process_bundle(self,
                     inputs,  # type: Mapping[str, PartitionableBuffer]
                     expected_outputs  # type: DataOutput
                    ):
    # type: (...) -> BundleProcessResult
    part_inputs = [{} for _ in range(self._num_workers)
                   ]  # type: List[Dict[str, List[bytes]]]
    for name, input in inputs.items():
      for ix, part in enumerate(input.partition(self._num_workers)):
        part_inputs[ix][name] = part

    merged_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]
    split_result_list = [
    ]  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]

    def execute(part_map):
      # type: (...) -> BundleProcessResult
      bundle_manager = BundleManager(
          self._worker_handler_list,
          self._get_buffer,
          self._get_input_coder_impl,
          self._bundle_descriptor,
          self._progress_frequency,
          self._registered,
          cache_token_generator=self._cache_token_generator)
      return bundle_manager.process_bundle(part_map, expected_outputs)

    with UnboundedThreadPoolExecutor() as executor:
      for result, split_result in executor.map(execute, part_inputs):

        split_result_list += split_result
        if merged_result is None:
          merged_result = result
        else:
          merged_result = beam_fn_api_pb2.InstructionResponse(
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  monitoring_infos=monitoring_infos.consolidate(
                      itertools.chain(
                          result.process_bundle.monitoring_infos,
                          merged_result.process_bundle.monitoring_infos))),
              error=result.error or merged_result.error)
    assert merged_result is not None

    return merged_result, split_result_list


class ProgressRequester(threading.Thread):
  """ Thread that asks SDK Worker for progress reports with a certain frequency.

  A callback can be passed to call with progress updates.
  """

  def __init__(self,
               worker_handler,  # type: WorkerHandler
               instruction_id,
               frequency,
               callback=None
              ):
    # type: (...) -> None
    super(ProgressRequester, self).__init__()
    self._worker_handler = worker_handler
    self._instruction_id = instruction_id
    self._frequency = frequency
    self._done = False
    self._latest_progress = None
    self._callback = callback
    self.daemon = True

  def __enter__(self):
    if self._frequency:
      self.start()

  def __exit__(self, *unused_exc_info):
    if self._frequency:
      self.stop()

  def run(self):
    while not self._done:
      try:
        progress_result = self._worker_handler.control_conn.push(
            beam_fn_api_pb2.InstructionRequest(
                process_bundle_progress=beam_fn_api_pb2.
                ProcessBundleProgressRequest(
                    instruction_id=self._instruction_id))).get()
        self._latest_progress = progress_result.process_bundle_progress
        if self._callback:
          self._callback(self._latest_progress)
      except Exception as exn:
        _LOGGER.error("Bad progress: %s", exn)
      time.sleep(self._frequency)

  def stop(self):
    self._done = True


class ControlFuture(object):
  def __init__(self, instruction_id, response=None):
    self.instruction_id = instruction_id
    if response:
      self._response = response
    else:
      self._response = None
      self._condition = threading.Condition()

  def is_done(self):
    return self._response is not None

  def set(self, response):
    with self._condition:
      self._response = response
      self._condition.notify_all()

  def get(self, timeout=None):
    if not self._response:
      with self._condition:
        if not self._response:
          self._condition.wait(timeout)
    return self._response


class FnApiMetrics(metric.MetricResults):
  def __init__(self, step_monitoring_infos, user_metrics_only=True):
    """Used for querying metrics from the PipelineResult object.

      step_monitoring_infos: Per step metrics specified as MonitoringInfos.
      user_metrics_only: If true, includes user metrics only.
    """
    self._counters = {}
    self._distributions = {}
    self._gauges = {}
    self._user_metrics_only = user_metrics_only
    self._monitoring_infos = step_monitoring_infos

    for smi in step_monitoring_infos.values():
      counters, distributions, gauges = \
          portable_metrics.from_monitoring_infos(smi, user_metrics_only)
      self._counters.update(counters)
      self._distributions.update(distributions)
      self._gauges.update(gauges)

  def query(self, filter=None):
    counters = [
        MetricResult(k, v, v) for k,
        v in self._counters.items() if self.matches(filter, k)
    ]
    distributions = [
        MetricResult(k, v, v) for k,
        v in self._distributions.items() if self.matches(filter, k)
    ]
    gauges = [
        MetricResult(k, v, v) for k,
        v in self._gauges.items() if self.matches(filter, k)
    ]

    return {
        self.COUNTERS: counters,
        self.DISTRIBUTIONS: distributions,
        self.GAUGES: gauges
    }

  def monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]
    return [
        item for sublist in self._monitoring_infos.values() for item in sublist
    ]


class RunnerResult(runner.PipelineResult):
  def __init__(self, state, monitoring_infos_by_stage, metrics_by_stage):
    super(RunnerResult, self).__init__(state)
    self._monitoring_infos_by_stage = monitoring_infos_by_stage
    self._metrics_by_stage = metrics_by_stage
    self._metrics = None
    self._monitoring_metrics = None

  def wait_until_finish(self, duration=None):
    return self._state

  def metrics(self):
    """Returns a queryable object including user metrics only."""
    if self._metrics is None:
      self._metrics = FnApiMetrics(
          self._monitoring_infos_by_stage, user_metrics_only=True)
    return self._metrics

  def monitoring_metrics(self):
    """Returns a queryable object including all metrics."""
    if self._monitoring_metrics is None:
      self._monitoring_metrics = FnApiMetrics(
          self._monitoring_infos_by_stage, user_metrics_only=False)
    return self._monitoring_metrics
