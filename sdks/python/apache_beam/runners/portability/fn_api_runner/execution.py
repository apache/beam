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

"""Set of utilities for execution of a pipeline by the FnApiRunner."""

from __future__ import absolute_import

import collections
import copy
import itertools
from typing import TYPE_CHECKING
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import Iterator
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Tuple

from typing_extensions import Protocol

from apache_beam import coders
from apache_beam.coders import BytesCoder
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.coders.coders import GlobalWindowCoder
from apache_beam.coders.coders import WindowedValueCoder
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.portability.fn_api_runner.translations import split_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import unique_name
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import proto_utils
from apache_beam.utils import windowed_value

if TYPE_CHECKING:
  from apache_beam.coders.coder_impl import CoderImpl
  from apache_beam.runners.portability.fn_api_runner import worker_handlers
  from apache_beam.runners.portability.fn_api_runner.translations import DataSideInput
  from apache_beam.transforms.window import BoundedWindow

ENCODED_IMPULSE_VALUE = WindowedValueCoder(
    BytesCoder(), GlobalWindowCoder()).get_impl().encode_nested(
        GlobalWindows.windowed_value(b''))

SAFE_WINDOW_FNS = set(window.WindowFn._known_urns.keys()) - set(
    [python_urns.PICKLED_WINDOWFN])


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


class ListBuffer(object):
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

  def reset(self):
    """Resets a cleared buffer for reuse."""
    if not self.cleared:
      raise RuntimeError('Trying to reset a non-cleared ListBuffer.')
    self.cleared = False


class GroupingBuffer(object):
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

  def __iter__(self):
    # type: () -> Iterator[bytes]

    """ Since partition() returns a list of lists, add this __iter__ to return
    a list to simplify code when we need to iterate through ALL elements of
    _GroupingBuffer.
    """
    return itertools.chain(*self.partition(1))


class WindowGroupingBuffer(object):
  """Used to partition windowed side inputs."""
  def __init__(
      self,
      access_pattern,
      coder  # type: WindowedValueCoder
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


class GenericNonMergingWindowFn(window.NonMergingWindowFn):

  URN = 'internal-generic-non-merging'

  def __init__(self, coder):
    self._coder = coder

  def assign(self, assign_context):
    raise NotImplementedError()

  def get_window_coder(self):
    return self._coder

  @staticmethod
  @window.urns.RunnerApiFn.register_urn(URN, bytes)
  def from_runner_api_parameter(window_coder_id, context):
    return GenericNonMergingWindowFn(
        context.coders[window_coder_id.decode('utf-8')])


class FnApiRunnerExecutionContext(object):
  """
 :var pcoll_buffers: (dict): Mapping of
       PCollection IDs to list that functions as buffer for the
       ``beam.PCollection``.
 """
  def __init__(self,
      stages,  # type: List[translations.Stage]
      worker_handler_manager,  # type: worker_handlers.WorkerHandlerManager
      pipeline_components,  # type: beam_runner_api_pb2.Components
      safe_coders,
      data_channel_coders,
               ):
    """
    :param worker_handler_manager: This class manages the set of worker
        handlers, and the communication with state / control APIs.
    :param pipeline_components:  (beam_runner_api_pb2.Components): TODO
    :param safe_coders:
    :param data_channel_coders:
    """
    self.stages = stages
    self.side_input_descriptors_by_stage = (
        self._build_data_side_inputs_map(stages))
    self.pcoll_buffers = {}  # type: MutableMapping[bytes, PartitionableBuffer]
    self.timer_buffers = {}  # type: MutableMapping[bytes, ListBuffer]
    self.worker_handler_manager = worker_handler_manager
    self.pipeline_components = pipeline_components
    self.safe_coders = safe_coders
    self.data_channel_coders = data_channel_coders

    self.pipeline_context = pipeline_context.PipelineContext(
        self.pipeline_components,
        iterable_state_write=self._iterable_state_write)
    self._last_uid = -1
    self.safe_windowing_strategies = {
        id: self._make_safe_windowing_strategy(id)
        for id in self.pipeline_components.windowing_strategies.keys()
    }

  @staticmethod
  def _build_data_side_inputs_map(stages):
    # type: (Iterable[translations.Stage]) -> MutableMapping[str, DataSideInput]

    """Builds an index mapping stages to side input descriptors.

    A side input descriptor is a map of side input IDs to side input access
    patterns for all of the outputs of a stage that will be consumed as a
    side input.
    """
    transform_consumers = collections.defaultdict(
        list)  # type: DefaultDict[str, List[beam_runner_api_pb2.PTransform]]
    stage_consumers = collections.defaultdict(
        list)  # type: DefaultDict[str, List[translations.Stage]]

    def get_all_side_inputs():
      # type: () -> Set[str]
      all_side_inputs = set()  # type: Set[str]
      for stage in stages:
        for transform in stage.transforms:
          for input in transform.inputs.values():
            transform_consumers[input].append(transform)
            stage_consumers[input].append(stage)
        for si in stage.side_inputs():
          all_side_inputs.add(si)
      return all_side_inputs

    all_side_inputs = frozenset(get_all_side_inputs())
    data_side_inputs_by_producing_stage = {}

    producing_stages_by_pcoll = {}

    for s in stages:
      data_side_inputs_by_producing_stage[s.name] = {}
      for transform in s.transforms:
        for o in transform.outputs.values():
          if o in s.side_inputs():
            continue
          if o in producing_stages_by_pcoll:
            continue
          producing_stages_by_pcoll[o] = s

    for side_pc in all_side_inputs:
      for consuming_transform in transform_consumers[side_pc]:
        if consuming_transform.spec.urn not in translations.PAR_DO_URNS:
          continue
        if side_pc not in producing_stages_by_pcoll:
          raise ValueError(
              'Unknown side input PCollection: %s'
              ' - to be consumed by transform %s' % (
                  side_pc, consuming_transform.unique_name))
        producing_stage = producing_stages_by_pcoll[side_pc]
        payload = proto_utils.parse_Bytes(
            consuming_transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for si_tag in payload.side_inputs:
          if consuming_transform.inputs[si_tag] == side_pc:
            side_input_id = (consuming_transform.unique_name, si_tag)
            data_side_inputs_by_producing_stage[
                producing_stage.name][side_input_id] = (
                    translations.create_buffer_id(side_pc),
                    payload.side_inputs[si_tag].access_pattern)

    return data_side_inputs_by_producing_stage

  def _make_safe_windowing_strategy(self, id):
    windowing_strategy_proto = self.pipeline_components.windowing_strategies[id]
    if windowing_strategy_proto.window_fn.urn in SAFE_WINDOW_FNS:
      return id
    elif (windowing_strategy_proto.merge_status ==
          beam_runner_api_pb2.MergeStatus.NON_MERGING) or True:
      safe_id = id + '_safe'
      while safe_id in self.pipeline_components.windowing_strategies:
        safe_id += '_'
      safe_proto = copy.copy(windowing_strategy_proto)
      safe_proto.window_fn.urn = GenericNonMergingWindowFn.URN
      safe_proto.window_fn.payload = (
          windowing_strategy_proto.window_coder_id.encode('utf-8'))
      self.pipeline_context.windowing_strategies.put_proto(safe_id, safe_proto)
      return safe_id
    elif windowing_strategy_proto.window_fn.urn == python_urns.PICKLED_WINDOWFN:
      return id
    else:
      raise NotImplementedError(
          '[BEAM-10119] Unknown merging WindowFn: %s' %
          windowing_strategy_proto)

  @property
  def state_servicer(self):
    # TODO(BEAM-9625): Ensure FnApiRunnerExecutionContext owns StateServicer
    return self.worker_handler_manager.state_servicer

  def next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def _iterable_state_write(self, values, element_coder_impl):
    # type: (...) -> bytes
    token = unique_name(None, 'iter').encode('ascii')
    out = create_OutputStream()
    for element in values:
      element_coder_impl.encode_to_stream(element, out, True)
    self.worker_handler_manager.state_servicer.append_raw(
        beam_fn_api_pb2.StateKey(
            runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
        out.get())
    return token

  def commit_side_inputs_to_state(
      self,
      data_side_input,  # type: DataSideInput
  ):
    # type: (...) -> None
    for (consuming_transform_id, tag), (buffer_id,
                                        func_spec) in data_side_input.items():
      _, pcoll_id = split_buffer_id(buffer_id)
      value_coder = self.pipeline_context.coders[self.safe_coders[
          self.data_channel_coders[pcoll_id]]]
      elements_by_window = WindowGroupingBuffer(func_spec, value_coder)
      if buffer_id not in self.pcoll_buffers:
        self.pcoll_buffers[buffer_id] = ListBuffer(
            coder_impl=value_coder.get_impl())
      for element_data in self.pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)

      if func_spec.urn == common_urns.side_inputs.ITERABLE.urn:
        for _, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window))
          self.state_servicer.append_raw(state_key, elements_data)
      elif func_spec.urn == common_urns.side_inputs.MULTIMAP.urn:
        for key, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window,
                  key=key))
          self.state_servicer.append_raw(state_key, elements_data)
      else:
        raise ValueError("Unknown access pattern: '%s'" % func_spec.urn)


class BundleContextManager(object):

  def __init__(self,
               execution_context, # type: FnApiRunnerExecutionContext
               stage,  # type: translations.Stage
               num_workers,  # type: int
              ):
    self.execution_context = execution_context
    self.stage = stage
    self.bundle_uid = self.execution_context.next_uid()
    self.num_workers = num_workers

    # Properties that are lazily initialized
    self._process_bundle_descriptor = None
    self._worker_handlers = None
    # a mapping of {(transform_id, timer_family_id): timer_coder_id}. The map
    # is built after self._process_bundle_descriptor is initialized.
    # This field can be used to tell whether current bundle has timers.
    self._timer_coder_ids = None

  @property
  def worker_handlers(self):
    if self._worker_handlers is None:
      self._worker_handlers = (
          self.execution_context.worker_handler_manager.get_worker_handlers(
              self.stage.environment, self.num_workers))
    return self._worker_handlers

  def data_api_service_descriptor(self):
    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    return self.worker_handlers[0].data_api_service_descriptor()

  def state_api_service_descriptor(self):
    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    return self.worker_handlers[0].state_api_service_descriptor()

  @property
  def process_bundle_descriptor(self):
    if self._process_bundle_descriptor is None:
      self._process_bundle_descriptor = self._build_process_bundle_descriptor()
      self._timer_coder_ids = self._build_timer_coders_id_map()
    return self._process_bundle_descriptor

  def _build_process_bundle_descriptor(self):
    # Cannot be invoked until *after* _extract_endpoints is called.
    # Always populate the timer_api_service_descriptor.
    return beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self.bundle_uid,
        transforms={
            transform.unique_name: transform
            for transform in self.stage.transforms
        },
        pcollections=dict(
            self.execution_context.pipeline_components.pcollections.items()),
        coders=dict(self.execution_context.pipeline_components.coders.items()),
        windowing_strategies=dict(
            self.execution_context.pipeline_components.windowing_strategies.
            items()),
        environments=dict(
            self.execution_context.pipeline_components.environments.items()),
        state_api_service_descriptor=self.state_api_service_descriptor(),
        timer_api_service_descriptor=self.data_api_service_descriptor())

  def extract_bundle_inputs_and_outputs(self):
    # type: (...) -> Tuple[Dict[str, PartitionableBuffer], DataOutput, Dict[Tuple[str, str], str]]

    """Returns maps of transform names to PCollection identifiers.

    Also mutates IO stages to point to the data ApiServiceDescriptor.

    Returns:
      A tuple of (data_input, data_output, expected_timer_output) dictionaries.
        `data_input` is a dictionary mapping (transform_name, output_name) to a
        PCollection buffer; `data_output` is a dictionary mapping
        (transform_name, output_name) to a PCollection ID.
        `expected_timer_output` is a dictionary mapping transform_id and
        timer family ID to a buffer id for timers.
    """
    data_input = {}  # type: Dict[str, PartitionableBuffer]
    data_output = {}  # type: DataOutput
    # A mapping of {(transform_id, timer_family_id) : buffer_id}
    expected_timer_output = {}  # type: Dict[Tuple[str, str], str]
    for transform in self.stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = self.execution_context.data_channel_coders[only_element(
              transform.outputs.values())]
          coder = self.execution_context.pipeline_context.coders[
              self.execution_context.safe_coders.get(coder_id, coder_id)]
          if pcoll_id == translations.IMPULSE_BUFFER:
            data_input[transform.unique_name] = ListBuffer(
                coder_impl=coder.get_impl())
            data_input[transform.unique_name].append(ENCODED_IMPULSE_VALUE)
          else:
            if pcoll_id not in self.execution_context.pcoll_buffers:
              self.execution_context.pcoll_buffers[pcoll_id] = ListBuffer(
                  coder_impl=coder.get_impl())
            data_input[transform.unique_name] = (
                self.execution_context.pcoll_buffers[pcoll_id])
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          data_output[transform.unique_name] = pcoll_id
          coder_id = self.execution_context.data_channel_coders[only_element(
              transform.inputs.values())]
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        data_api_service_descriptor = self.data_api_service_descriptor()
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for timer_family_id in payload.timer_family_specs.keys():
          expected_timer_output[(transform.unique_name, timer_family_id)] = (
              create_buffer_id(timer_family_id, 'timers'))
    return data_input, data_output, expected_timer_output

  def get_input_coder_impl(self, transform_id):
    # type: (str) -> CoderImpl
    coder_id = beam_fn_api_pb2.RemoteGrpcPort.FromString(
        self.process_bundle_descriptor.transforms[transform_id].spec.payload
    ).coder_id
    assert coder_id
    return self.get_coder_impl(coder_id)

  def _build_timer_coders_id_map(self):
    timer_coder_ids = {}
    for transform_id, transform_proto in (self._process_bundle_descriptor
        .transforms.items()):
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for id, timer_family_spec in pardo_payload.timer_family_specs.items():
          timer_coder_ids[(transform_id, id)] = (
              timer_family_spec.timer_family_coder_id)
    return timer_coder_ids

  def get_coder_impl(self, coder_id):
    if coder_id in self.execution_context.safe_coders:
      return self.execution_context.pipeline_context.coders[
          self.execution_context.safe_coders[coder_id]].get_impl()
    else:
      return self.execution_context.pipeline_context.coders[coder_id].get_impl()

  def get_timer_coder_impl(self, transform_id, timer_family_id):
    return self.get_coder_impl(
        self._timer_coder_ids[(transform_id, timer_family_id)])

  def get_buffer(self, buffer_id, transform_id):
    # type: (bytes, str) -> PartitionableBuffer

    """Returns the buffer for a given (operation_type, PCollection ID).
    For grouping-typed operations, we produce a ``GroupingBuffer``. For
    others, we produce a ``ListBuffer``.
    """
    kind, name = split_buffer_id(buffer_id)
    if kind == 'materialize':
      if buffer_id not in self.execution_context.pcoll_buffers:
        self.execution_context.pcoll_buffers[buffer_id] = ListBuffer(
            coder_impl=self.get_input_coder_impl(transform_id))
      return self.execution_context.pcoll_buffers[buffer_id]
    # For timer buffer, name = timer_family_id
    elif kind == 'timers':
      if buffer_id not in self.execution_context.timer_buffers:
        timer_coder_impl = self.get_timer_coder_impl(transform_id, name)
        self.execution_context.timer_buffers[buffer_id] = ListBuffer(
            timer_coder_impl)
      return self.execution_context.timer_buffers[buffer_id]
    elif kind == 'group':
      # This is a grouping write, create a grouping buffer if needed.
      if buffer_id not in self.execution_context.pcoll_buffers:
        original_gbk_transform = name
        transform_proto = self.execution_context.pipeline_components.transforms[
            original_gbk_transform]
        input_pcoll = only_element(list(transform_proto.inputs.values()))
        output_pcoll = only_element(list(transform_proto.outputs.values()))
        pre_gbk_coder = self.execution_context.pipeline_context.coders[
            self.execution_context.safe_coders[
                self.execution_context.data_channel_coders[input_pcoll]]]
        post_gbk_coder = self.execution_context.pipeline_context.coders[
            self.execution_context.safe_coders[
                self.execution_context.data_channel_coders[output_pcoll]]]
        windowing_strategy = (
            self.execution_context.pipeline_context.windowing_strategies[
                self.execution_context.safe_windowing_strategies[
                    self.execution_context.pipeline_components.
                    pcollections[output_pcoll].windowing_strategy_id]])
        self.execution_context.pcoll_buffers[buffer_id] = GroupingBuffer(
            pre_gbk_coder, post_gbk_coder, windowing_strategy)
    else:
      # These should be the only two identifiers we produce for now,
      # but special side input writes may go here.
      raise NotImplementedError(buffer_id)
    return self.execution_context.pcoll_buffers[buffer_id]

  def input_for(self, transform_id, input_id):
    # type: (str, str) -> str
    input_pcoll = self.process_bundle_descriptor.transforms[
        transform_id].inputs[input_id]
    for read_id, proto in self.process_bundle_descriptor.transforms.items():
      # The GrpcRead is followed by the SDF/Process.
      if (proto.spec.urn == bundle_processor.DATA_INPUT_URN and
          input_pcoll in proto.outputs.values()):
        return read_id
      # The GrpcRead is followed by the SDF/Truncate -> SDF/Process.
      if (proto.spec.urn ==
          common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn and
          input_pcoll in proto.outputs.values()):
        read_input = list(
            self.process_bundle_descriptor.transforms[read_id].inputs.values()
        )[0]
        for (grpc_read,
             transform_proto) in self.process_bundle_descriptor.transforms.items():  # pylint: disable=line-too-long
          if (transform_proto.spec.urn == bundle_processor.DATA_INPUT_URN and
              read_input in transform_proto.outputs.values()):
            return grpc_read

    raise RuntimeError('No IO transform feeds %s' % transform_id)
