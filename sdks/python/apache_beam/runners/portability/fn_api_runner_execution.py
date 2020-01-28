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
from typing import TYPE_CHECKING
from typing import Iterator
from typing import List

from typing_extensions import Protocol

from apache_beam import coders
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.portability.fn_api_runner_transforms import only_element
from apache_beam.runners.portability.fn_api_runner_transforms import split_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import unique_name
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import windowed_value

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow  # pylint: disable=ungrouped-imports


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


class _ListBuffer(List[bytes]):
  """Used to support parititioning of a list."""
  def partition(self, n):
    # type: (int) -> List[List[bytes]]
    return [self[k::n] for k in range(n)]


class _GroupingBuffer(object):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(
      self,
      pre_grouped_coder,  # type: coders.Coder
      post_grouped_coder,  # type: coders.Coder
      windowing
  ):
    # type: (...) -> None
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table = collections.defaultdict(list)  # type: DefaultDict[bytes, List[Any]]
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
      output_stream_list = []
      for _ in range(n):
        output_stream_list.append(create_OutputStream())
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
      windowed_val_coder_impl = self._windowed_value_coder.get_impl()  # type: WindowedValueCoderImpl
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
    return pipeline_context.coders[safe_coders[
        beam_fn_api_pb2.RemoteGrpcPort.FromString(
            process_bundle_descriptor.transforms[transform_id].spec.payload
        ).coder_id
    ]].get_impl()
  return input_coder_getter_impl


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
    data_side_input,  # type: DataSideInput
    pcoll_buffers,  # type: Mapping[bytes, PartitionableBuffer]
    safe_coders
):
  # type: (...) -> None
  for (transform_id, tag), (buffer_id, si) in data_side_input.items():
    _, pcoll_id = split_buffer_id(buffer_id)
    value_coder = context.coders[safe_coders[
        pipeline_components.pcollections[pcoll_id].coder_id]]
    elements_by_window = _WindowGroupingBuffer(si, value_coder)
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
    else:
      raise ValueError(
          "Unknown access pattern: '%s'" % si.urn)

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
