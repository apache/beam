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
import itertools
import logging
from typing import TYPE_CHECKING
from typing import Callable
from typing import Iterator
from typing import List
from typing_extensions import Protocol

from apache_beam import coders
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.fn_api_runner_transforms import IMPULSE_BUFFER
from apache_beam.runners.portability.fn_api_runner_transforms import PAR_DO_URNS
from apache_beam.runners.portability.fn_api_runner_transforms import create_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import only_element
from apache_beam.runners.portability.fn_api_runner_transforms import split_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import unique_name
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow  # pylint: disable=ungrouped-imports

# TODO(pabloem): Add Pydoc to methods in this file.

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

class _ListBuffer(PartitionableBuffer):
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

  def __len__(self):
    return len(self._inputs)

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


class _GroupingBuffer(PartitionableBuffer):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(
      self,
      pre_grouped_coder,  # type: coders.Coder
      post_grouped_coder,  # type: coders.Coder
      windowing
  ):  # type: (...) -> None
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table = collections.defaultdict(list)  # type: Optional[DefaultDict[bytes, List[Any]]]
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
          value if is_trivial_windowing
          else windowed_key_value.with_value(value))

  def extend(self, input_buffer):
    # type: (_GroupingBuffer) -> None
    for key, values in input_buffer._table.items():
      self._table[key].extend(values)

  def partition(self, n):
    # type: (int) -> List[List[bytes]]
    """ It is used to partition _GroupingBuffer to N parts. Once it is
    partitioned, it would not be re-partitioned with different N. Re-partition
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
      output_stream_list = []
      for _ in range(n):
        output_stream_list.append(create_OutputStream())
      for idx, (encoded_key, windowed_values) in enumerate(self._table.items()):
        key = key_coder_impl.decode(encoded_key)
        for wkvs in windowed_key_values(key, windowed_values):
          coder_impl.encode_to_stream(wkvs, output_stream_list[idx % n], True)
      for ix, output_stream in enumerate(output_stream_list):
        self._grouped_output[ix] = [output_stream.get()]
      self._table = None
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
      self._value_coder = (
          coder.wrapped_value_coder.value_coder())
    else:
      raise ValueError(
          "Unknown access pattern: '%s'" % access_pattern.urn)
    self._windowed_value_coder = coder
    self._window_coder = coder.window_coder
    self._values_by_window = collections.defaultdict(list)  # type: DefaultDict[Tuple[str, BoundedWindow], List[Any]]

  def append(self, elements_data):
    # type: (bytes) -> None
    input_stream = create_InputStream(elements_data)
    while input_stream.size() > 0:
      windowed_value = self._windowed_value_coder.get_impl(
      ).decode_from_stream(input_stream, True)
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


def make_input_coder_getter(pipeline_context,
                            process_bundle_descriptor,
                            safe_coders):
  def input_coder_getter_impl(transform_id):
    # type: (str) -> CoderImpl
    coder_id = beam_fn_api_pb2.RemoteGrpcPort.FromString(
        process_bundle_descriptor.transforms[transform_id].spec.payload
    ).coder_id
    assert coder_id
    if coder_id in safe_coders:
      return pipeline_context.coders[safe_coders[coder_id]].get_impl()
    else:
      return pipeline_context.coders[coder_id].get_impl()
  return input_coder_getter_impl


def make_input_buffer_fetcher(
    pipeline_context,  # type: PipelineContext
    pcoll_buffers,  # type: Dict[str, Union[_ListBuffer, _GroupingBuffer]]
    pipeline_components,  # type: beam_runner_api_pb2.Components
    safe_coders,  # type: Dict[str, str]
    coder_getter, # type: Callable[[str], CoderImpl]
):
  # type: (...) -> Callable
  """Returns a callable to fetch the buffer containing a PCollection to input
     to a PTransform.

    For grouping-typed operations, we produce a ``_GroupingBuffer``. For
    others, we produce a ``_ListBuffer``.
  """
  def buffer_fetcher(buffer_id, transform_id):
    # type: (bytes, str) -> PartitionableBuffer
    kind, name = split_buffer_id(buffer_id)
    if kind in ('materialize', 'timers'):
      if buffer_id not in pcoll_buffers:
        pcoll_buffers[buffer_id] = _ListBuffer(
            coder_impl=coder_getter(transform_id))
      return pcoll_buffers[buffer_id]

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


def store_side_inputs_in_state(
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
    if buffer_id not in pcoll_buffers:
      pcoll_buffers[buffer_id] = _ListBuffer(
          coder_impl=value_coder.get_impl())
    for element_data in pcoll_buffers[buffer_id]:
      elements_by_window.append(element_data)

    if si.urn == common_urns.side_inputs.ITERABLE.urn:
      for _, window, elements_data in elements_by_window.encoded_items():
        state_key = beam_fn_api_pb2.StateKey(
            iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                transform_id=transform_id,
                side_input_id=tag,
                window=window))
        worker_handler.state.append_raw(state_key, elements_data)
    elif si.urn == common_urns.side_inputs.MULTIMAP.urn:
      for key, window, elements_data in elements_by_window.encoded_items():
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id=transform_id,
                side_input_id=tag,
                window=window,
                key=key))
        worker_handler.state.append_raw(state_key, elements_data)


def add_residuals_and_channel_splits_to_deferred_inputs(
    process_bundle_descriptor, splits, get_input_coder_callable,
    last_sent, deferred_inputs):
  prev_stops = {}
  for split in splits:
    for delayed_application in split.residual_roots:

      input_op_name = get_input_operation_name(
          process_bundle_descriptor,
          delayed_application.application.transform_id,
          delayed_application.application.input_id)

      if input_op_name not in deferred_inputs:
        deferred_inputs[input_op_name] = _ListBuffer(
            coder_impl=get_input_coder_callable(input_op_name))
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
        if channel_split.transform_id not in deferred_inputs:
          coder_impl = get_input_coder_callable(channel_split.transform_id)
          deferred_inputs[channel_split.transform_id] = _ListBuffer(
              coder_impl=coder_impl)
        deferred_inputs[channel_split.transform_id].append(
            coder_impl.encode_all(residual_elements))
      prev_stops[
          channel_split.transform_id] = channel_split.last_primary_element


SideInputInfo = collections.namedtuple(
    'SideInputInfo', ['consumer', 'tag', 'buffer_id', 'access_pattern'])


StageInfo = collections.namedtuple(
    'StageInfo', ['stage', 'inputs', 'outputs'])


class _WatermarkManager(object):
  """Manages the watermarks of a pipeline's stages.

  It works by constructing an internal graph representation of the pipeline,
  and keeping track of dependencies."""

  class WatermarkNode(object):
    def __init__(self, name):
      self.name = name

  class PCollectionNode(WatermarkNode):
    def __init__(self, name):
      super(_WatermarkManager.PCollectionNode, self).__init__(name)
      self._watermark = timestamp.MIN_TIMESTAMP
      self.producers = set()

    def set_watermark(self, wm):
      self._watermark = min(self.upstream_watermark(), wm)

    def upstream_watermark(self):
      if self.producers:
        return min(p.output_watermark() for p in self.producers)
      else:
        return timestamp.MAX_TIMESTAMP

    def watermark(self):
      if self._watermark:
        return self._watermark
      else:
        return self.upstream_watermark()

  class StageNode(WatermarkNode):

    def __init__(self, name):
      super(_WatermarkManager.StageNode, self).__init__(name)
      # We keep separate inputs and side inputs because side inputs
      # should hold back a stage's input watermark, to hold back execution
      # for that stage; but they should not be considered when calculating
      # the output watermark of the stage, because only the main input
      # can actually advance that watermark.
      self.inputs = set()
      self.side_inputs = set()

    def set_watermark(self, wm):
      raise NotImplementedError('Stages do not have a watermark')

    def output_watermark(self):
      w = min(i.watermark() for i in self.inputs)
      return w

    def input_watermark(self):
      w = min(i.upstream_watermark() for i in self.inputs)

      if self.side_inputs:
        w = min(w,
                min(i.upstream_watermark() for i in self.side_inputs))
      return w

  def __init__(self, stages, execution_context):
    # type: (List[Stage], PipelineExecutionContext) -> None
    self._watermarks_by_name = {}
    for s in stages:
      stage_name = s.name
      stage_node = _WatermarkManager.StageNode(stage_name)
      self._watermarks_by_name[stage_name] = stage_node

      data_input, data_output = execution_context.endpoints_per_stage[s.name]

      for _, buffer_id in data_input.items():
        if buffer_id == IMPULSE_BUFFER:
          pcoll_name = '%s%s' % (stage_name, IMPULSE_BUFFER)
        else:
          _, pcoll_name = split_buffer_id(buffer_id)
        if pcoll_name not in self._watermarks_by_name:
          self._watermarks_by_name[
              pcoll_name] = _WatermarkManager.PCollectionNode(pcoll_name)

        stage_node.inputs.add(self._watermarks_by_name[pcoll_name])

      for _, buffer_id in data_output.items():
        _, pcoll_name = split_buffer_id(buffer_id)
        if pcoll_name not in self._watermarks_by_name:
          self._watermarks_by_name[
              pcoll_name] = _WatermarkManager.PCollectionNode(pcoll_name)
        self._watermarks_by_name[pcoll_name].producers.add(stage_node)

      side_inputs = PipelineExecutionContext._get_data_side_input_per_pcoll_id(
          s)
      for si_name, _ in side_inputs:
        if si_name not in self._watermarks_by_name:
          self._watermarks_by_name[
              si_name] = _WatermarkManager.PCollectionNode(si_name)
        stage_node.side_inputs.add(self._watermarks_by_name[si_name])

  def get_node(self, name):
    # type: (str) -> WatermarkNode
    return self._watermarks_by_name[name]

  def get_watermark(self, name):
    element = self._watermarks_by_name[name]
    return element.watermark()

  def set_watermark(self, name, watermark):
    element = self._watermarks_by_name[name]
    element.set_watermark(watermark)


class PipelineExecutionContext(object):
  """A set of utilities for the FnApiRunner."""

  def __init__(self, pipeline_components, stages):
    self.pipeline_components = pipeline_components
    self._stages = stages
    self.endpoints_per_stage = {
        s.name: self._extract_stage_endpoints(s) for s in stages}

    self._consuming_stages_per_input_buffer_id = {
        k: [subv[0] for subv in v]
        for k, v in self._compute_pcoll_consumer_per_buffer_id(stages).items()}

    self._consuming_transforms_per_input_buffer_id = {
        k: [subv[1] for subv in v]
        for k, v in self._compute_pcoll_consumer_per_buffer_id(stages).items()}

    self.pcoll_producers = self._compute_producer_per_pcollection_name(
        stages)

    self.pcoll_to_side_input_info = self._compute_side_input_to_consumer_map(
        stages)

    self.stages_per_name = {s.name: self._compute_stage_info(s) for s in stages}
    self.watermark_manager = _WatermarkManager(stages, self)

  def get_side_input_infos(self, pcoll_name):
    # type: (str) -> List[SideInputInfo]
    return self.pcoll_to_side_input_info.get(pcoll_name, [])

  def get_consuming_stages(self, buffer_id):
    # type: (str) -> List[Stage]
    return self._consuming_stages_per_input_buffer_id.get(buffer_id, None)

  def get_consuming_transforms(self, buffer_id):
    # type: (str) -> List[beam_runner_api_pb2.PTransform]
    return self._consuming_transforms_per_input_buffer_id.get(
        buffer_id, None)

  def update_pcoll_watermark(self, pcoll_name, watermark):
    # type: (str, timestamp.Timestamp) -> None
    logging.debug(
        'Updating watermark to %s for PCollection: %s', watermark, pcoll_name)
    self.watermark_manager.set_watermark(pcoll_name, watermark)
    if watermark != self.watermark_manager.get_watermark(pcoll_name):
      logging.debug('Due to upstream holds, watermark could not be '
                    'updated to value desired. Set to: %s',
                    self.watermark_manager.get_watermark(pcoll_name))

  @staticmethod
  def _compute_producer_per_pcollection_name(
      stages  # type: List[Stage]
  ):
    # type: (...) -> Dict[str, str]
    pcollection_name_to_producer = {}
    for s in stages:
      for t in s.transforms:
        if (t.spec.urn == bundle_processor.DATA_INPUT_URN and
            t.spec.payload != IMPULSE_BUFFER):
          _, input_pcoll = split_buffer_id(t.spec.payload)
          # Adding PCollections that may not have a producer.
          # This is necessary, for example, for the case where we pass an empty
          # list of PCollections into a Flatten transform.
          if input_pcoll not in pcollection_name_to_producer:
            pcollection_name_to_producer[input_pcoll] = None
        elif (t.spec.urn == bundle_processor.DATA_INPUT_URN and
              t.spec.payload == IMPULSE_BUFFER):
          # Impulse data is not produced by any PTransform.
          pcollection_name_to_producer[IMPULSE_BUFFER] = None
        elif t.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          _, output_pcoll = split_buffer_id(t.spec.payload)
          pcollection_name_to_producer[output_pcoll] = t.unique_name


    return pcollection_name_to_producer

  @staticmethod
  def _compute_pcoll_consumer_per_buffer_id(
      stages  # type: List[Stage]
  ):
    # type: (...) -> Dict[bytes, List[Tuple(Stage, PTransform)]]
    """Computes the stages that consume a PCollection as main input.

    This means either data-input operations, or timer pcollections. It does NOT
    include side input consumption."""

    result = collections.defaultdict(list)
    known_consumers = collections.defaultdict(set)

    for s in stages:
      for t in s.transforms:
        if t.spec.urn == bundle_processor.DATA_INPUT_URN:
          consumer_pair = (s.name, t.unique_name)
          if consumer_pair not in known_consumers[t.spec.payload]:
            known_consumers[t.spec.payload].add(consumer_pair)
            result[t.spec.payload].append((s, t))

    for s in stages:
      transforms_per_unique_name = {t.unique_name:t for t in s.transforms}
      for input_id, timer_pcoll in s.timer_pcollections:
        # We do not need to review known_consumers for timer PCollections
        # because they are always only consumed by a single transform.
        # Specifically, the transform that outputs them.
        buffer_id = create_buffer_id(timer_pcoll, 'timers')
        result[buffer_id].append((s, transforms_per_unique_name[input_id]))

    return result

  @staticmethod
  def _compute_side_input_to_consumer_map(stages):
    pcoll_side_input_info_pairs = itertools.chain(*[
        PipelineExecutionContext._get_data_side_input_per_pcoll_id(s)
        for s in stages])
    result = {}
    for pcoll_name, side_input_info in pcoll_side_input_info_pairs:
      if pcoll_name not in result:
        result[pcoll_name] = []
      result[pcoll_name].append(side_input_info)
    return result

  @staticmethod
  def _build_watermark_dict(stage_infos):
    stage_crossing_pcolls = set()
    for stage_info in stage_infos:
      stage_crossing_pcolls = stage_crossing_pcolls.union(stage_info.inputs)
      stage_crossing_pcolls = stage_crossing_pcolls.union(stage_info.outputs)

    return {pcoll: timestamp.MIN_TIMESTAMP for pcoll in stage_crossing_pcolls}

  @staticmethod
  def _compute_stage_info(stage):
    # type: (Stage) -> StageInfo
    result_inputs = set()
    result_outputs = set()
    side_input_infos = (PipelineExecutionContext
                        ._get_data_side_input_per_pcoll_id(stage))
    main_inputs, outputs = (PipelineExecutionContext
                            ._extract_stage_endpoints(stage))

    for pcoll_id, unused_si_info in side_input_infos:
      result_inputs.add(pcoll_id)

    for _, buffer_id in main_inputs.items():
      if buffer_id == IMPULSE_BUFFER:
        pcoll_id = '%s%s' % (stage.name, IMPULSE_BUFFER)
      else:
        _, pcoll_id = split_buffer_id(buffer_id)
      result_inputs.add(pcoll_id)

    for _, buffer_id in outputs.items():
      _, pcoll_id = split_buffer_id(buffer_id)
      result_outputs.add(pcoll_id)

    # TODO(pabloem, MUST): What about timer PCollections?
    return StageInfo(stage, result_inputs, result_outputs)

  @staticmethod
  def _get_data_side_input_per_pcoll_id(stage):
    """
    Args:
      stage (fn_api_runner_transforms.Stage): The stage that consumes side
        inputs that we will extract.
    """
    consumer_side_input_pairs = []
    for transform in stage.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        # If a transform is a PARDO, then it may receive side inputs.
        # For each side input in a PARDO we map the side input PCollection name
        # to a SideInputInfo tuple.
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for tag, si in payload.side_inputs.items():
          pcoll_id = transform.inputs[tag]
          sinfo = SideInputInfo(
              transform.unique_name,
              tag,
              create_buffer_id(pcoll_id),
              si.access_pattern)
          consumer_side_input_pairs.append((pcoll_id, sinfo))
    return consumer_side_input_pairs

  @staticmethod
  def _extract_stage_endpoints(stage):
    """Returns maps of transform names to PCollection identifiers.

    Also mutates IO stages to point to the data ApiServiceDescriptor.

    Args:
      stage (fn_api_runner_transforms.Stage): The stage to extract endpoints
        for.
    Returns:
      A tuple of (data_input, data_output) dictionaries.
        `data_input` is a tuple of transforms that receive inputs for the stage.
        `data_output` is a dictionary mapping
        (transform_name) to a PCollection ID.
    """
    data_input = {}
    data_output = {}
    for transform in stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        # If a transform is a DATA_INPUT or DATA_OUTPUT transform, it will
        # receive data from / deliver data to the runner.
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          # For data input transforms, they only have one input.
          if pcoll_id == IMPULSE_BUFFER:
            pass
          data_input[transform.unique_name] = pcoll_id
        else:
          assert transform.spec.urn == bundle_processor.DATA_OUTPUT_URN
          # For data output transforms, we map the transform name to the
          # output PCollection name
          data_output[transform.unique_name] = pcoll_id
    return data_input, data_output
