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

"""SDK harness for executing Python Fns via the Fn API."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import bisect
import collections
import json
import logging
import random
import threading
from builtins import next
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Container
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast

from future.utils import itervalues
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

import apache_beam as beam
from apache_beam import coders
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders import coder_impl
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.metrics import monitoring_infos
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import common
from apache_beam.runners import pipeline_context
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.runners.worker import statesampler
from apache_beam.transforms import TimeDomain
from apache_beam.transforms import sideinputs
from apache_beam.transforms import userstate
from apache_beam.utils import counters
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp

if TYPE_CHECKING:
  from google.protobuf import message  # pylint: disable=ungrouped-imports
  from apache_beam import pvalue
  from apache_beam.portability.api import metrics_pb2
  from apache_beam.runners.sdf_utils import SplitResultPrimary
  from apache_beam.runners.sdf_utils import SplitResultResidual
  from apache_beam.runners.worker import data_plane
  from apache_beam.runners.worker import sdk_worker
  from apache_beam.transforms import window
  from apache_beam.utils import windowed_value

# This module is experimental. No backwards-compatibility guarantees.
T = TypeVar('T')
ConstructorFn = Callable[[
    'BeamTransformFactory',
    Any,
    beam_runner_api_pb2.PTransform,
    Union['message.Message', bytes],
    Dict[str, List[operations.Operation]]
],
                         operations.Operation]
OperationT = TypeVar('OperationT', bound=operations.Operation)

DATA_INPUT_URN = 'beam:runner:source:v1'
DATA_OUTPUT_URN = 'beam:runner:sink:v1'
IDENTITY_DOFN_URN = 'beam:dofn:identity:0.1'
# TODO(vikasrk): Fix this once runner sends appropriate common_urns.
OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN = 'beam:dofn:javasdk:0.1'
OLD_DATAFLOW_RUNNER_HARNESS_READ_URN = 'beam:source:java:0.1'
URNS_NEEDING_PCOLLECTIONS = set([
    monitoring_infos.ELEMENT_COUNT_URN, monitoring_infos.SAMPLED_BYTE_SIZE_URN
])

_LOGGER = logging.getLogger(__name__)


class RunnerIOOperation(operations.Operation):
  """Common baseclass for runner harness IO operations."""

  def __init__(self,
               name_context,  # type: Union[str, common.NameContext]
               step_name,
               consumers,  # type: Mapping[Any, Iterable[operations.Operation]]
               counter_factory,
               state_sampler,
               windowed_coder,  # type: coders.Coder
               transform_id,  # type: str
               data_channel  # type: data_plane.DataChannel
              ):
    # type: (...) -> None
    super(RunnerIOOperation,
          self).__init__(name_context, None, counter_factory, state_sampler)
    self.windowed_coder = windowed_coder
    self.windowed_coder_impl = windowed_coder.get_impl()
    # transform_id represents the consumer for the bytes in the data plane for a
    # DataInputOperation or a producer of these bytes for a DataOutputOperation.
    self.transform_id = transform_id
    self.data_channel = data_channel
    for _, consumer_ops in consumers.items():
      for consumer in consumer_ops:
        self.add_receiver(consumer, 0)


class DataOutputOperation(RunnerIOOperation):
  """A sink-like operation that gathers outputs to be sent back to the runner.
  """
  def set_output_stream(self, output_stream):
    # type: (data_plane.ClosableOutputStream) -> None
    self.output_stream = output_stream

  def process(self, windowed_value):
    # type: (windowed_value.WindowedValue) -> None
    self.windowed_coder_impl.encode_to_stream(
        windowed_value, self.output_stream, True)
    self.output_stream.maybe_flush()

  def finish(self):
    # type: () -> None
    self.output_stream.close()
    super(DataOutputOperation, self).finish()


class DataInputOperation(RunnerIOOperation):
  """A source-like operation that gathers input from the runner."""

  def __init__(self,
               operation_name,  # type: Union[str, common.NameContext]
               step_name,
               consumers,  # type: Mapping[Any, Iterable[operations.Operation]]
               counter_factory,
               state_sampler,
               windowed_coder,  # type: coders.Coder
               transform_id,
               data_channel  # type: data_plane.GrpcClientDataChannel
              ):
    # type: (...) -> None
    super(DataInputOperation, self).__init__(
        operation_name,
        step_name,
        consumers,
        counter_factory,
        state_sampler,
        windowed_coder,
        transform_id=transform_id,
        data_channel=data_channel)
    # We must do this manually as we don't have a spec or spec.output_coders.
    self.receivers = [
        operations.ConsumerSet.create(
            self.counter_factory,
            self.name_context.step_name,
            0,
            next(iter(itervalues(consumers))),
            self.windowed_coder,
            self._get_runtime_performance_hints())
    ]
    self.splitting_lock = threading.Lock()
    self.index = -1
    self.stop = float('inf')
    self.started = False

  def start(self):
    # type: () -> None
    super(DataInputOperation, self).start()
    with self.splitting_lock:
      self.started = True

  def process(self, windowed_value):
    # type: (windowed_value.WindowedValue) -> None
    self.output(windowed_value)

  def process_encoded(self, encoded_windowed_values):
    # type: (bytes) -> None
    input_stream = coder_impl.create_InputStream(encoded_windowed_values)
    while input_stream.size() > 0:
      with self.splitting_lock:
        if self.index == self.stop - 1:
          return
        self.index += 1
      decoded_value = self.windowed_coder_impl.decode_from_stream(
          input_stream, True)
      self.output(decoded_value)

  def monitoring_infos(self, transform_id, tag_to_pcollection_id):
    # type: (str, Dict[str, str]) -> Dict[FrozenSet, metrics_pb2.MonitoringInfo]
    all_monitoring_infos = super(DataInputOperation, self).monitoring_infos(
        transform_id, tag_to_pcollection_id)
    read_progress_info = monitoring_infos.int64_counter(
        monitoring_infos.DATA_CHANNEL_READ_INDEX,
        self.index,
        ptransform=transform_id)
    all_monitoring_infos[monitoring_infos.to_key(
        read_progress_info)] = read_progress_info
    return all_monitoring_infos

  def try_split(
      self, fraction_of_remainder, total_buffer_size, allowed_split_points):
    # type: (...) -> Optional[Tuple[int, Iterable[operations.SdfSplitResultsPrimary], Iterable[operations.SdfSplitResultsResidual], int]]
    with self.splitting_lock:
      if not self.started:
        return None
      if self.index == -1:
        # We are "finished" with the (non-existent) previous element.
        current_element_progress = 1.0
      else:
        current_element_progress_object = (
            self.receivers[0].current_element_progress())
        if current_element_progress_object is None:
          current_element_progress = 0.5
        else:
          current_element_progress = (
              current_element_progress_object.fraction_completed)
      # Now figure out where to split.
      split = self._compute_split(
          self.index,
          current_element_progress,
          self.stop,
          fraction_of_remainder,
          total_buffer_size,
          allowed_split_points,
          self.receivers[0].try_split)
      if split:
        self.stop = split[-1]
      return split

  @staticmethod
  def _compute_split(
      index,
      current_element_progress,
      stop,
      fraction_of_remainder,
      total_buffer_size,
      allowed_split_points=(),
      try_split=lambda fraction: None):
    def is_valid_split_point(index):
      return not allowed_split_points or index in allowed_split_points

    if total_buffer_size < index + 1:
      total_buffer_size = index + 1
    elif total_buffer_size > stop:
      total_buffer_size = stop
    # The units here (except for keep_of_element_remainder) are all in
    # terms of number of (possibly fractional) elements.
    remainder = total_buffer_size - index - current_element_progress
    keep = remainder * fraction_of_remainder
    if current_element_progress < 1:
      keep_of_element_remainder = keep / (1 - current_element_progress)
      # If it's less than what's left of the current element,
      # try splitting at the current element.
      if (keep_of_element_remainder < 1 and is_valid_split_point(index) and
          is_valid_split_point(index + 1)):
        split = try_split(
            keep_of_element_remainder
        )  # type: Optional[Tuple[Iterable[operations.SdfSplitResultsPrimary], Iterable[operations.SdfSplitResultsResidual]]]
        if split:
          element_primaries, element_residuals = split
          return index - 1, element_primaries, element_residuals, index + 1
    # Otherwise, split at the closest element boundary.
    # pylint: disable=round-builtin
    stop_index = index + max(1, int(round(current_element_progress + keep)))
    if allowed_split_points and stop_index not in allowed_split_points:
      # Choose the closest allowed split point.
      allowed_split_points = sorted(allowed_split_points)
      closest = bisect.bisect(allowed_split_points, stop_index)
      if closest == 0:
        stop_index = allowed_split_points[0]
      elif closest == len(allowed_split_points):
        stop_index = allowed_split_points[-1]
      else:
        prev = allowed_split_points[closest - 1]
        next = allowed_split_points[closest]
        if index < prev and stop_index - prev < next - stop_index:
          stop_index = prev
        else:
          stop_index = next
    if index < stop_index < stop:
      return stop_index - 1, [], [], stop_index
    else:
      return None

  def finish(self):
    # type: () -> None
    with self.splitting_lock:
      self.index += 1
      self.started = False

  def reset(self):
    # type: () -> None
    self.index = -1
    self.stop = float('inf')
    super(DataInputOperation, self).reset()


class _StateBackedIterable(object):
  def __init__(self,
               state_handler,  # type: sdk_worker.CachingStateHandler
               state_key,  # type: beam_fn_api_pb2.StateKey
               coder_or_impl,  # type: Union[coders.Coder, coder_impl.CoderImpl]
              ):
    # type: (...) -> None
    self._state_handler = state_handler
    self._state_key = state_key
    if isinstance(coder_or_impl, coders.Coder):
      self._coder_impl = coder_or_impl.get_impl()
    else:
      self._coder_impl = coder_or_impl

  def __iter__(self):
    # type: () -> Iterator[Any]
    return iter(
        self._state_handler.blocking_get(self._state_key, self._coder_impl))

  def __reduce__(self):
    return list, (list(self), )


coder_impl.FastPrimitivesCoderImpl.register_iterable_like_type(
    _StateBackedIterable)


class StateBackedSideInputMap(object):
  def __init__(self,
               state_handler,  # type: sdk_worker.CachingStateHandler
               transform_id,  # type: str
               tag,  # type: Optional[str]
               side_input_data,  # type: pvalue.SideInputData
               coder  # type: WindowedValueCoder
              ):
    # type: (...) -> None
    self._state_handler = state_handler
    self._transform_id = transform_id
    self._tag = tag
    self._side_input_data = side_input_data
    self._element_coder = coder.wrapped_value_coder
    self._target_window_coder = coder.window_coder
    # TODO(robertwb): Limit the cache size.
    self._cache = {}  # type: Dict[window.BoundedWindow, Any]

  def __getitem__(self, window):
    target_window = self._side_input_data.window_mapping_fn(window)
    if target_window not in self._cache:
      state_handler = self._state_handler
      access_pattern = self._side_input_data.access_pattern

      if access_pattern == common_urns.side_inputs.ITERABLE.urn:
        state_key = beam_fn_api_pb2.StateKey(
            iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                transform_id=self._transform_id,
                side_input_id=self._tag,
                window=self._target_window_coder.encode(target_window)))
        raw_view = _StateBackedIterable(
            state_handler, state_key, self._element_coder)

      elif access_pattern == common_urns.side_inputs.MULTIMAP.urn:
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id=self._transform_id,
                side_input_id=self._tag,
                window=self._target_window_coder.encode(target_window),
                key=b''))
        cache = {}
        key_coder_impl = self._element_coder.key_coder().get_impl()
        value_coder = self._element_coder.value_coder()

        class MultiMap(object):
          def __getitem__(self, key):
            if key not in cache:
              keyed_state_key = beam_fn_api_pb2.StateKey()
              keyed_state_key.CopyFrom(state_key)
              keyed_state_key.multimap_side_input.key = (
                  key_coder_impl.encode_nested(key))
              cache[key] = _StateBackedIterable(
                  state_handler, keyed_state_key, value_coder)
            return cache[key]

          def __reduce__(self):
            # TODO(robertwb): Figure out how to support this.
            raise TypeError(common_urns.side_inputs.MULTIMAP.urn)

        raw_view = MultiMap()

      else:
        raise ValueError("Unknown access pattern: '%s'" % access_pattern)

      self._cache[target_window] = self._side_input_data.view_fn(raw_view)
    return self._cache[target_window]

  def is_globally_windowed(self):
    # type: () -> bool
    return (
        self._side_input_data.window_mapping_fn ==
        sideinputs._global_window_mapping_fn)

  def reset(self):
    # type: () -> None
    # TODO(BEAM-5428): Cross-bundle caching respecting cache tokens.
    self._cache = {}


class ReadModifyWriteRuntimeState(userstate.ReadModifyWriteRuntimeState):
  def __init__(self, underlying_bag_state):
    self._underlying_bag_state = underlying_bag_state

  def read(self):  # type: () -> Any
    values = list(self._underlying_bag_state.read())
    if not values:
      return None
    return values[0]

  def write(self, value):  # type: (Any) -> None
    self.clear()
    self._underlying_bag_state.add(value)

  def clear(self):  # type: () -> None
    self._underlying_bag_state.clear()

  def commit(self):  # type: () -> None
    self._underlying_bag_state.commit()


class CombiningValueRuntimeState(userstate.CombiningValueRuntimeState):
  def __init__(self, underlying_bag_state, combinefn):
    self._combinefn = combinefn
    self._underlying_bag_state = underlying_bag_state

  def _read_accumulator(self, rewrite=True):
    merged_accumulator = self._combinefn.merge_accumulators(
        self._underlying_bag_state.read())
    if rewrite:
      self._underlying_bag_state.clear()
      self._underlying_bag_state.add(merged_accumulator)
    return merged_accumulator

  def read(self):
    # type: () -> Iterable[Any]
    return self._combinefn.extract_output(self._read_accumulator())

  def add(self, value):
    # type: (Any) -> None
    # Prefer blind writes, but don't let them grow unboundedly.
    # This should be tuned to be much lower, but for now exercise
    # both paths well.
    if random.random() < 0.5:
      accumulator = self._read_accumulator(False)
      self._underlying_bag_state.clear()
    else:
      accumulator = self._combinefn.create_accumulator()
    self._underlying_bag_state.add(
        self._combinefn.add_input(accumulator, value))

  def clear(self):
    # type: () -> None
    self._underlying_bag_state.clear()

  def commit(self):
    self._underlying_bag_state.commit()


class _ConcatIterable(object):
  """An iterable that is the concatination of two iterables.

  Unlike itertools.chain, this allows reiteration.
  """
  def __init__(self, first, second):
    # type: (Iterable[Any], Iterable[Any]) -> None
    self.first = first
    self.second = second

  def __iter__(self):
    # type: () -> Iterator[Any]
    for elem in self.first:
      yield elem
    for elem in self.second:
      yield elem


coder_impl.FastPrimitivesCoderImpl.register_iterable_like_type(_ConcatIterable)


class SynchronousBagRuntimeState(userstate.BagRuntimeState):

  def __init__(self,
               state_handler,  # type: sdk_worker.CachingStateHandler
               state_key,  # type: beam_fn_api_pb2.StateKey
               value_coder  # type: coders.Coder
              ):
    # type: (...) -> None
    self._state_handler = state_handler
    self._state_key = state_key
    self._value_coder = value_coder
    self._cleared = False
    self._added_elements = []  # type: List[Any]

  def read(self):
    # type: () -> Iterable[Any]
    return _ConcatIterable([] if self._cleared else cast(
        'Iterable[Any]',
        _StateBackedIterable(
            self._state_handler, self._state_key, self._value_coder)),
                           self._added_elements)

  def add(self, value):
    # type: (Any) -> None
    self._added_elements.append(value)

  def clear(self):
    # type: () -> None
    self._cleared = True
    self._added_elements = []

  def commit(self):
    to_await = None
    if self._cleared:
      to_await = self._state_handler.clear(self._state_key)
    if self._added_elements:
      to_await = self._state_handler.extend(
          self._state_key, self._value_coder.get_impl(), self._added_elements)
    if to_await:
      # To commit, we need to wait on the last state request future to complete.
      to_await.get()


class SynchronousSetRuntimeState(userstate.SetRuntimeState):

  def __init__(self,
               state_handler,  # type: sdk_worker.CachingStateHandler
               state_key,  # type: beam_fn_api_pb2.StateKey
               value_coder  # type: coders.Coder
              ):
    # type: (...) -> None
    self._state_handler = state_handler
    self._state_key = state_key
    self._value_coder = value_coder
    self._cleared = False
    self._added_elements = set()  # type: Set[Any]

  def _compact_data(self, rewrite=True):
    accumulator = set(
        _ConcatIterable(
            set() if self._cleared else _StateBackedIterable(
                self._state_handler, self._state_key, self._value_coder),
            self._added_elements))

    if rewrite and accumulator:
      self._state_handler.clear(self._state_key)
      self._state_handler.extend(
          self._state_key, self._value_coder.get_impl(), accumulator)

      # Since everthing is already committed so we can safely reinitialize
      # added_elements here.
      self._added_elements = set()

    return accumulator

  def read(self):
    # type: () -> Set[Any]
    return self._compact_data(rewrite=False)

  def add(self, value):
    # type: (Any) -> None
    if self._cleared:
      # This is a good time explicitly clear.
      self._state_handler.clear(self._state_key)
      self._cleared = False

    self._added_elements.add(value)
    if random.random() > 0.5:
      self._compact_data()

  def clear(self):
    # type: () -> None
    self._cleared = True
    self._added_elements = set()

  def commit(self):
    # type: () -> None
    to_await = None
    if self._cleared:
      to_await = self._state_handler.clear(self._state_key)
    if self._added_elements:
      to_await = self._state_handler.extend(
          self._state_key, self._value_coder.get_impl(), self._added_elements)
    if to_await:
      # To commit, we need to wait on the last state request future to complete.
      to_await.get()


class OutputTimer(object):
  def __init__(self,
               key,
               window,  # type: windowed_value.BoundedWindow
               timestamp,  # type: timestamp.Timestamp
               paneinfo,  # type: windowed_value.PaneInfo
               time_domain, # type: str
               timer_family_id,  # type: str
               timer_coder_impl,  # type: coder_impl.TimerCoderImpl
               output_stream  # type: data_plane.ClosableOutputStream
              ):
    self._key = key
    self._window = window
    self._input_timestamp = timestamp
    self._paneinfo = paneinfo
    self._time_domain = time_domain
    self._timer_family_id = timer_family_id
    self._output_stream = output_stream
    self._timer_coder_impl = timer_coder_impl

  def set(self, ts):
    ts = timestamp.Timestamp.of(ts)
    timer = userstate.Timer(
        user_key=self._key,
        dynamic_timer_tag='',
        windows=(self._window, ),
        clear_bit=False,
        fire_timestamp=ts,
        hold_timestamp=ts if TimeDomain.is_event_time(self._time_domain) else
        self._input_timestamp,
        paneinfo=self._paneinfo)
    self._timer_coder_impl.encode_to_stream(timer, self._output_stream, True)
    self._output_stream.maybe_flush()

  def clear(self):
    # type: () -> None
    timer = userstate.Timer(
        user_key=self._key,
        dynamic_timer_tag='',
        windows=(self._window, ),
        clear_bit=True,
        fire_timestamp=None,
        hold_timestamp=None,
        paneinfo=None)
    self._timer_coder_impl.encode_to_stream(timer, self._output_stream, True)
    self._output_stream.maybe_flush()


class TimerInfo(object):
  """A data class to store information related to a timer."""
  def __init__(self, timer_coder_impl, output_stream=None):
    self.timer_coder_impl = timer_coder_impl
    self.output_stream = output_stream


class FnApiUserStateContext(userstate.UserStateContext):
  """Interface for state and timers from SDK to Fn API servicer of state.."""

  def __init__(self,
               state_handler,  # type: sdk_worker.CachingStateHandler
               transform_id,  # type: str
               key_coder,  # type: coders.Coder
               window_coder,  # type: coders.Coder
              ):
    # type: (...) -> None

    """Initialize a ``FnApiUserStateContext``.

    Args:
      state_handler: A StateServicer object.
      transform_id: The name of the PTransform that this context is associated.
      key_coder:
      window_coder:
      timer_family_specs: A list of ``userstate.TimerSpec`` objects specifying
        the timers associated with this operation.
    """
    self._state_handler = state_handler
    self._transform_id = transform_id
    self._key_coder = key_coder
    self._window_coder = window_coder
    # A mapping of {timer_family_id: TimerInfo}
    self._timers_info = {}
    self._all_states = {}  # type: Dict[tuple, userstate.RuntimeState]

  def add_timer_info(self, timer_family_id, timer_info):
    self._timers_info[timer_family_id] = timer_info

  def get_timer(
      self,
      timer_spec,
      key,
      window,  # type: windowed_value.BoundedWindow
      timestamp,
      pane):
    # type: (...) -> OutputTimer
    assert self._timers_info[timer_spec.name].output_stream is not None
    return OutputTimer(
        key,
        window,
        timestamp,
        pane,
        timer_spec.time_domain,
        timer_spec.name,
        self._timers_info[timer_spec.name].timer_coder_impl,
        self._timers_info[timer_spec.name].output_stream)

  def get_state(self, *args):
    state_handle = self._all_states.get(args)
    if state_handle is None:
      state_handle = self._all_states[args] = self._create_state(*args)
    return state_handle

  def _create_state(self,
                    state_spec,  # type: userstate.StateSpec
                    key,
                    window  # type: windowed_value.BoundedWindow
                   ):
    # type: (...) -> userstate.RuntimeState
    if isinstance(state_spec,
                  (userstate.BagStateSpec,
                   userstate.CombiningValueStateSpec,
                   userstate.ReadModifyWriteStateSpec)):
      bag_state = SynchronousBagRuntimeState(
          self._state_handler,
          state_key=beam_fn_api_pb2.StateKey(
              bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
                  transform_id=self._transform_id,
                  user_state_id=state_spec.name,
                  window=self._window_coder.encode(window),
                  # State keys are expected in nested encoding format
                  key=self._key_coder.encode_nested(key))),
          value_coder=state_spec.coder)
      if isinstance(state_spec, userstate.BagStateSpec):
        return bag_state
      elif isinstance(state_spec, userstate.ReadModifyWriteStateSpec):
        return ReadModifyWriteRuntimeState(bag_state)
      else:
        return CombiningValueRuntimeState(bag_state, state_spec.combine_fn)
    elif isinstance(state_spec, userstate.SetStateSpec):
      return SynchronousSetRuntimeState(
          self._state_handler,
          state_key=beam_fn_api_pb2.StateKey(
              bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
                  transform_id=self._transform_id,
                  user_state_id=state_spec.name,
                  window=self._window_coder.encode(window),
                  # State keys are expected in nested encoding format
                  key=self._key_coder.encode_nested(key))),
          value_coder=state_spec.coder)
    else:
      raise NotImplementedError(state_spec)

  def commit(self):
    # type: () -> None
    for state in self._all_states.values():
      state.commit()

  def reset(self):
    # type: () -> None
    self._all_states = {}
    self._timer_output_streams = {}
    self._timer_coders_impl = {}


def memoize(func):
  cache = {}
  missing = object()

  def wrapper(*args):
    result = cache.get(args, missing)
    if result is missing:
      result = cache[args] = func(*args)
    return result

  return wrapper


def only_element(iterable):
  # type: (Iterable[T]) -> T
  element, = iterable
  return element


class BundleProcessor(object):
  """ A class for processing bundles of elements. """

  def __init__(self,
               process_bundle_descriptor,  # type: beam_fn_api_pb2.ProcessBundleDescriptor
               state_handler,  # type: sdk_worker.CachingStateHandler
               data_channel_factory  # type: data_plane.DataChannelFactory
              ):
    # type: (...) -> None

    """Initialize a bundle processor.

    Args:
      process_bundle_descriptor (``beam_fn_api_pb2.ProcessBundleDescriptor``):
        a description of the stage that this ``BundleProcessor``is to execute.
      state_handler (CachingStateHandler).
      data_channel_factory (``data_plane.DataChannelFactory``).
    """
    self.process_bundle_descriptor = process_bundle_descriptor
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory

    # There is no guarantee that the runner only set
    # timer_api_service_descriptor when having timers. So this field cannot be
    # used as an indicator of timers.
    if self.process_bundle_descriptor.timer_api_service_descriptor.url:
      self.timer_data_channel = (
          data_channel_factory.create_data_channel_from_url(
              self.process_bundle_descriptor.timer_api_service_descriptor.url))
    else:
      self.timer_data_channel = None

    # A mapping of
    # {(transform_id, timer_family_id): TimerInfo}
    # The mapping is empty when there is no timer_family_specs in the
    # ProcessBundleDescriptor.
    self.timers_info = {}

    # TODO(robertwb): Figure out the correct prefix to use for output counters
    # from StateSampler.
    self.counter_factory = counters.CounterFactory()
    self.state_sampler = statesampler.StateSampler(
        'fnapi-step-%s' % self.process_bundle_descriptor.id,
        self.counter_factory)
    self.ops = self.create_execution_tree(self.process_bundle_descriptor)
    for op in self.ops.values():
      op.setup()
    self.splitting_lock = threading.Lock()

  def create_execution_tree(
      self,
      descriptor  # type: beam_fn_api_pb2.ProcessBundleDescriptor
  ):
    # type: (...) -> collections.OrderedDict[str, operations.Operation]
    transform_factory = BeamTransformFactory(
        descriptor,
        self.data_channel_factory,
        self.counter_factory,
        self.state_sampler,
        self.state_handler)

    self.timers_info = transform_factory.extract_timers_info()

    def is_side_input(transform_proto, tag):
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        return tag in proto_utils.parse_Bytes(
            transform_proto.spec.payload,
            beam_runner_api_pb2.ParDoPayload).side_inputs

    pcoll_consumers = collections.defaultdict(
        list)  # type: DefaultDict[str, List[str]]
    for transform_id, transform_proto in descriptor.transforms.items():
      for tag, pcoll_id in transform_proto.inputs.items():
        if not is_side_input(transform_proto, tag):
          pcoll_consumers[pcoll_id].append(transform_id)

    @memoize
    def get_operation(transform_id):
      # type: (str) -> operations.Operation
      transform_consumers = {
          tag: [get_operation(op) for op in pcoll_consumers[pcoll_id]]
          for tag,
          pcoll_id in descriptor.transforms[transform_id].outputs.items()
      }
      return transform_factory.create_operation(
          transform_id, transform_consumers)

    # Operations must be started (hence returned) in order.
    @memoize
    def topological_height(transform_id):
      # type: (str) -> int
      return 1 + max([0] + [
          topological_height(consumer)
          for pcoll in descriptor.transforms[transform_id].outputs.values()
          for consumer in pcoll_consumers[pcoll]
      ])

    return collections.OrderedDict([
        (transform_id, get_operation(transform_id)) for transform_id in sorted(
            descriptor.transforms, key=topological_height, reverse=True)
    ])

  def reset(self):
    # type: () -> None
    self.counter_factory.reset()
    self.state_sampler.reset()
    # Side input caches.
    for op in self.ops.values():
      op.reset()

  def process_bundle(self, instruction_id):
    # type: (str) -> Tuple[List[beam_fn_api_pb2.DelayedBundleApplication], bool]

    expected_inputs = []

    for op in self.ops.values():
      if isinstance(op, DataOutputOperation):
        # TODO(robertwb): Is there a better way to pass the instruction id to
        # the operation?
        op.set_output_stream(
            op.data_channel.output_stream(instruction_id, op.transform_id))
      elif isinstance(op, DataInputOperation):
        # We must wait until we receive "end of stream" for each of these ops.
        expected_inputs.append(op)

    try:
      execution_context = ExecutionContext()
      self.state_sampler.start()
      # Start all operations.
      for op in reversed(self.ops.values()):
        _LOGGER.debug('start %s', op)
        op.execution_context = execution_context
        op.start()

      # Each data_channel is mapped to a list of expected inputs which includes
      # both data input and timer input. The data input is identied by
      # transform_id. The data input is identified by
      # (transform_id, timer_family_id).
      data_channels = collections.defaultdict(
          list
      )  # type: DefaultDict[data_plane.GrpcClientDataChannel, List[str]]

      # Add expected data inputs for each data channel.
      input_op_by_transform_id = {}
      for input_op in expected_inputs:
        data_channels[input_op.data_channel].append(input_op.transform_id)
        input_op_by_transform_id[input_op.transform_id] = input_op

      # Update timer_data channel with expected timer inputs.
      if self.timer_data_channel:
        data_channels[self.timer_data_channel].extend(
            list(self.timers_info.keys()))

      # Set up timer output stream for DoOperation.
      for ((transform_id, timer_family_id),
           timer_info) in self.timers_info.items():
        output_stream = self.timer_data_channel.output_timer_stream(
            instruction_id, transform_id, timer_family_id)
        timer_info.output_stream = output_stream
        self.ops[transform_id].add_timer_info(timer_family_id, timer_info)

      # Process data and timer inputs
      for data_channel, expected_inputs in data_channels.items():
        for element in data_channel.input_elements(instruction_id,
                                                   expected_inputs):
          if isinstance(element, beam_fn_api_pb2.Elements.Timers):
            timer_coder_impl = (
                self.timers_info[(
                    element.transform_id,
                    element.timer_family_id)].timer_coder_impl)
            for timer_data in timer_coder_impl.decode_all(element.timers):
              self.ops[element.transform_id].process_timer(
                  element.timer_family_id, timer_data)
          elif isinstance(element, beam_fn_api_pb2.Elements.Data):
            input_op_by_transform_id[element.transform_id].process_encoded(
                element.data)

      # Finish all operations.
      for op in self.ops.values():
        _LOGGER.debug('finish %s', op)
        op.finish()

      # Close every timer output stream
      for timer_info in self.timers_info.values():
        assert timer_info.output_stream is not None
        timer_info.output_stream.close()

      return ([
          self.delayed_bundle_application(op, residual) for op,
          residual in execution_context.delayed_applications
      ],
              self.requires_finalization())

    finally:
      # Ensure any in-flight split attempts complete.
      with self.splitting_lock:
        pass
      self.state_sampler.stop_if_still_running()

  def finalize_bundle(self):
    # type: () -> beam_fn_api_pb2.FinalizeBundleResponse
    for op in self.ops.values():
      op.finalize_bundle()
    return beam_fn_api_pb2.FinalizeBundleResponse()

  def requires_finalization(self):
    # type: () -> bool
    return any(op.needs_finalization() for op in self.ops.values())

  def try_split(self, bundle_split_request):
    # type: (...) -> beam_fn_api_pb2.ProcessBundleSplitResponse
    split_response = beam_fn_api_pb2.ProcessBundleSplitResponse()
    with self.splitting_lock:
      for op in self.ops.values():
        if isinstance(op, DataInputOperation):
          desired_split = bundle_split_request.desired_splits.get(
              op.transform_id)
          if desired_split:
            split = op.try_split(
                desired_split.fraction_of_remainder,
                desired_split.estimated_input_elements,
                desired_split.allowed_split_points)
            if split:
              (
                  primary_end,
                  element_primaries,
                  element_residuals,
                  residual_start,
              ) = split
              for element_primary in element_primaries:
                split_response.primary_roots.add().CopyFrom(
                    self.bundle_application(*element_primary))
              for element_residual in element_residuals:
                split_response.residual_roots.add().CopyFrom(
                    self.delayed_bundle_application(*element_residual))
              split_response.channel_splits.extend([
                  beam_fn_api_pb2.ProcessBundleSplitResponse.ChannelSplit(
                      transform_id=op.transform_id,
                      last_primary_element=primary_end,
                      first_residual_element=residual_start)
              ])

    return split_response

  def delayed_bundle_application(self,
                                 op,  # type: operations.DoOperation
                                 deferred_remainder  # type: SplitResultResidual
                                ):
    # type: (...) -> beam_fn_api_pb2.DelayedBundleApplication
    assert op.input_info is not None
    # TODO(SDF): For non-root nodes, need main_input_coder + residual_coder.
    (element_and_restriction, current_watermark, deferred_timestamp) = (
        deferred_remainder)
    if deferred_timestamp:
      assert isinstance(deferred_timestamp, timestamp.Duration)
      proto_deferred_watermark = proto_utils.from_micros(
          duration_pb2.Duration,
          deferred_timestamp.micros)  # type: Optional[duration_pb2.Duration]
    else:
      proto_deferred_watermark = None
    return beam_fn_api_pb2.DelayedBundleApplication(
        requested_time_delay=proto_deferred_watermark,
        application=self.construct_bundle_application(
            op, current_watermark, element_and_restriction))

  def bundle_application(self,
                         op,  # type: operations.DoOperation
                         primary  # type: SplitResultPrimary
                        ):
    # type: (...) -> beam_fn_api_pb2.BundleApplication
    return self.construct_bundle_application(op, None, primary.primary_value)

  def construct_bundle_application(self,
                                   op,  # type: operations.DoOperation
                                   output_watermark,  # type: Optional[timestamp.Timestamp]
                                   element
                                  ):
    # type: (...) -> beam_fn_api_pb2.BundleApplication
    transform_id, main_input_tag, main_input_coder, outputs = op.input_info

    if output_watermark:
      proto_output_watermark = proto_utils.from_micros(
          timestamp_pb2.Timestamp, output_watermark.micros)
      output_watermarks = {
          output: proto_output_watermark
          for output in outputs
      }  # type: Optional[Dict[str, timestamp_pb2.Timestamp]]
    else:
      output_watermarks = None
    return beam_fn_api_pb2.BundleApplication(
        transform_id=transform_id,
        input_id=main_input_tag,
        output_watermarks=output_watermarks,
        element=main_input_coder.get_impl().encode_nested(element))

  def monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]

    """Returns the list of MonitoringInfos collected processing this bundle."""
    # Construct a new dict first to remove duplicates.
    all_monitoring_infos_dict = {}
    for transform_id, op in self.ops.items():
      tag_to_pcollection_id = self.process_bundle_descriptor.transforms[
          transform_id].outputs
      all_monitoring_infos_dict.update(
          op.monitoring_infos(transform_id, dict(tag_to_pcollection_id)))

    return list(all_monitoring_infos_dict.values())

  def shutdown(self):
    # type: () -> None
    for op in self.ops.values():
      op.teardown()


class ExecutionContext(object):
  def __init__(self):
    self.delayed_applications = [
    ]  # type: List[Tuple[operations.DoOperation, common.SplitResultType]]


class BeamTransformFactory(object):
  """Factory for turning transform_protos into executable operations."""
  def __init__(self,
               descriptor,  # type: beam_fn_api_pb2.ProcessBundleDescriptor
               data_channel_factory,  # type: data_plane.DataChannelFactory
               counter_factory,
               state_sampler,  # type: statesampler.StateSampler
               state_handler  # type: sdk_worker.CachingStateHandler
              ):
    self.descriptor = descriptor
    self.data_channel_factory = data_channel_factory
    self.counter_factory = counter_factory
    self.state_sampler = state_sampler
    self.state_handler = state_handler
    self.context = pipeline_context.PipelineContext(
        descriptor,
        iterable_state_read=lambda token,
        element_coder_impl: _StateBackedIterable(
            state_handler,
            beam_fn_api_pb2.StateKey(
                runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
            element_coder_impl))

  _known_urns = {
  }  # type: Dict[str, Tuple[ConstructorFn, Union[Type[message.Message], Type[bytes], None]]]

  @classmethod
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type  # type: Optional[Type[T]]
  ):
    # type: (...) -> Callable[[Callable[[BeamTransformFactory, str, beam_runner_api_pb2.PTransform, T, Dict[str, List[operations.Operation]]], operations.Operation]], Callable[[BeamTransformFactory, str, beam_runner_api_pb2.PTransform, T, Dict[str, List[operations.Operation]]], operations.Operation]]
    def wrapper(func):
      cls._known_urns[urn] = func, parameter_type
      return func

    return wrapper

  def create_operation(self,
                       transform_id,  # type: str
                       consumers  # type: Dict[str, List[operations.Operation]]
                      ):
    # type: (...) -> operations.Operation
    transform_proto = self.descriptor.transforms[transform_id]
    if not transform_proto.unique_name:
      _LOGGER.debug("No unique name set for transform %s" % transform_id)
      transform_proto.unique_name = transform_id
    creator, parameter_type = self._known_urns[transform_proto.spec.urn]
    payload = proto_utils.parse_Bytes(
        transform_proto.spec.payload, parameter_type)
    return creator(self, transform_id, transform_proto, payload, consumers)

  def extract_timers_info(self):
    timers_info = {}
    for transform_id, transform_proto in self.descriptor.transforms.items():
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for (timer_family_id,
             timer_family_spec) in pardo_payload.timer_family_specs.items():
          timer_coder_impl = self.get_coder(
              timer_family_spec.timer_family_coder_id).get_impl()
          # The output_stream should be updated when processing a bundle.
          timers_info[(transform_id, timer_family_id)] = TimerInfo(
              timer_coder_impl=timer_coder_impl)
    return timers_info

  def get_coder(self, coder_id):
    # type: (str) -> coders.Coder
    if coder_id not in self.descriptor.coders:
      raise KeyError("No such coder: %s" % coder_id)
    coder_proto = self.descriptor.coders[coder_id]
    if coder_proto.spec.urn:
      return self.context.coders.get_by_id(coder_id)
    else:
      # No URN, assume cloud object encoding json bytes.
      return operation_specs.get_coder_from_spec(
          json.loads(coder_proto.spec.payload.decode('utf-8')))

  def get_windowed_coder(self, pcoll_id):
    # type: (str) -> coders.Coder
    coder = self.get_coder(self.descriptor.pcollections[pcoll_id].coder_id)
    # TODO(robertwb): Remove this condition once all runners are consistent.
    if not isinstance(coder, WindowedValueCoder):
      windowing_strategy = self.descriptor.windowing_strategies[
          self.descriptor.pcollections[pcoll_id].windowing_strategy_id]
      return WindowedValueCoder(
          coder, self.get_coder(windowing_strategy.window_coder_id))
    else:
      return coder

  def get_output_coders(self, transform_proto):
    # type: (beam_runner_api_pb2.PTransform) -> Dict[str, coders.Coder]
    return {
        tag: self.get_windowed_coder(pcoll_id)
        for tag,
        pcoll_id in transform_proto.outputs.items()
    }

  def get_only_output_coder(self, transform_proto):
    # type: (beam_runner_api_pb2.PTransform) -> coders.Coder
    return only_element(self.get_output_coders(transform_proto).values())

  def get_input_coders(self, transform_proto):
    # type: (beam_runner_api_pb2.PTransform) -> Dict[str, coders.Coder]
    return {
        tag: self.get_windowed_coder(pcoll_id)
        for tag,
        pcoll_id in transform_proto.inputs.items()
    }

  def get_only_input_coder(self, transform_proto):
    # type: (beam_runner_api_pb2.PTransform) -> coders.Coder
    return only_element(list(self.get_input_coders(transform_proto).values()))

  def get_input_windowing(self, transform_proto):
    pcoll_id = only_element(transform_proto.inputs.values())
    windowing_strategy_id = self.descriptor.pcollections[
        pcoll_id].windowing_strategy_id
    return self.context.windowing_strategies.get_by_id(windowing_strategy_id)

  # TODO(robertwb): Update all operations to take these in the constructor.
  @staticmethod
  def augment_oldstyle_op(
      op,  # type: OperationT
      step_name,
      consumers,
      tag_list=None):
    # type: (...) -> OperationT
    op.step_name = step_name
    for tag, op_consumers in consumers.items():
      for consumer in op_consumers:
        op.add_receiver(consumer, tag_list.index(tag) if tag_list else 0)
    return op


@BeamTransformFactory.register_urn(
    DATA_INPUT_URN, beam_fn_api_pb2.RemoteGrpcPort)
def create_source_runner(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    grpc_port,  # type: beam_fn_api_pb2.RemoteGrpcPort
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> DataInputOperation

  output_coder = factory.get_coder(grpc_port.coder_id)
  return DataInputOperation(
      common.NameContext(transform_proto.unique_name, transform_id),
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      output_coder,
      transform_id=transform_id,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(
    DATA_OUTPUT_URN, beam_fn_api_pb2.RemoteGrpcPort)
def create_sink_runner(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    grpc_port,  # type: beam_fn_api_pb2.RemoteGrpcPort
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> DataOutputOperation
  output_coder = factory.get_coder(grpc_port.coder_id)
  return DataOutputOperation(
      common.NameContext(transform_proto.unique_name, transform_id),
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      output_coder,
      transform_id=transform_id,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_READ_URN, None)
def create_source_java(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.ReadOperation
  # The Dataflow runner harness strips the base64 encoding.
  source = pickler.loads(base64.b64encode(parameter))
  spec = operation_specs.WorkerRead(
      iobase.SourceBundle(1.0, source, None, None),
      [factory.get_only_output_coder(transform_proto)])
  return factory.augment_oldstyle_op(
      operations.ReadOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.deprecated_primitives.READ.urn, beam_runner_api_pb2.ReadPayload)
def create_deprecated_read(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,  # type: beam_runner_api_pb2.ReadPayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.ReadOperation
  source = iobase.SourceBase.from_runner_api(parameter.source, factory.context)
  spec = operation_specs.WorkerRead(
      iobase.SourceBundle(1.0, source, None, None),
      [WindowedValueCoder(source.default_output_coder())])
  return factory.augment_oldstyle_op(
      operations.ReadOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    python_urns.IMPULSE_READ_TRANSFORM, beam_runner_api_pb2.ReadPayload)
def create_read_from_impulse_python(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,  # type: beam_runner_api_pb2.ReadPayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.ImpulseReadOperation
  return operations.ImpulseReadOperation(
      common.NameContext(transform_proto.unique_name, transform_id),
      factory.counter_factory,
      factory.state_sampler,
      consumers,
      iobase.SourceBase.from_runner_api(parameter.source, factory.context),
      factory.get_only_output_coder(transform_proto))


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN, None)
def create_dofn_javasdk(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    serialized_fn,
    consumers  # type: Dict[str, List[operations.Operation]]
):
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers, serialized_fn)


@BeamTransformFactory.register_urn(
    common_urns.sdf_components.PAIR_WITH_RESTRICTION.urn,
    beam_runner_api_pb2.ParDoPayload)
def create_pair_with_restriction(*args):
  class PairWithRestriction(beam.DoFn):
    def __init__(self, fn, restriction_provider, watermark_estimator_provider):
      self.restriction_provider = restriction_provider
      self.watermark_estimator_provider = watermark_estimator_provider

    def process(self, element, *args, **kwargs):
      # TODO(SDF): Do we want to allow mutation of the element?
      # (E.g. it could be nice to shift bulky description to the portion
      # that can be distributed.)
      initial_restriction = self.restriction_provider.initial_restriction(
          element)
      initial_estimator_state = (
          self.watermark_estimator_provider.initial_estimator_state(
              element, initial_restriction))
      yield (element, (initial_restriction, initial_estimator_state))

  return _create_sdf_operation(PairWithRestriction, *args)


@BeamTransformFactory.register_urn(
    common_urns.sdf_components.SPLIT_AND_SIZE_RESTRICTIONS.urn,
    beam_runner_api_pb2.ParDoPayload)
def create_split_and_size_restrictions(*args):
  class SplitAndSizeRestrictions(beam.DoFn):
    def __init__(self, fn, restriction_provider, watermark_estimator_provider):
      self.restriction_provider = restriction_provider
      self.watermark_estimator_provider = watermark_estimator_provider

    def process(self, element_restriction, *args, **kwargs):
      element, (restriction, _) = element_restriction
      for part, size in self.restriction_provider.split_and_size(
          element, restriction):
        estimator_state = (
            self.watermark_estimator_provider.initial_estimator_state(
                element, part))
        yield ((element, (part, estimator_state)), size)

  return _create_sdf_operation(SplitAndSizeRestrictions, *args)


@BeamTransformFactory.register_urn(
    common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn,
    beam_runner_api_pb2.ParDoPayload)
def create_truncate_sized_restriction(*args):
  class TruncateAndSizeRestriction(beam.DoFn):
    def __init__(self, fn, restriction_provider, watermark_estimator_provider):
      self.restriction_provider = restriction_provider

    def process(self, element_restriction, *args, **kwargs):
      ((element, (restriction, estimator_state)), _) = element_restriction
      truncated_restriction = self.restriction_provider.truncate(
          element, restriction)
      if truncated_restriction:
        truncated_restriction_size = (
            self.restriction_provider.restriction_size(
                element, truncated_restriction))
        yield ((element, (truncated_restriction, estimator_state)),
               truncated_restriction_size)

  return _create_sdf_operation(
      TruncateAndSizeRestriction,
      *args,
      operation_cls=operations.SdfTruncateSizedRestrictions)


@BeamTransformFactory.register_urn(
    common_urns.sdf_components.PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS.urn,
    beam_runner_api_pb2.ParDoPayload)
def create_process_sized_elements_and_restrictions(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,  # type: beam_runner_api_pb2.ParDoPayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  assert parameter.do_fn.urn == python_urns.PICKLED_DOFN_INFO
  serialized_fn = parameter.do_fn.payload
  return _create_pardo_operation(
      factory,
      transform_id,
      transform_proto,
      consumers,
      serialized_fn,
      parameter,
      operation_cls=operations.SdfProcessSizedElements)


def _create_sdf_operation(
    proxy_dofn,
    factory,
    transform_id,
    transform_proto,
    parameter,
    consumers,
    operation_cls=operations.DoOperation):

  dofn_data = pickler.loads(parameter.do_fn.payload)
  dofn = dofn_data[0]
  restriction_provider = common.DoFnSignature(dofn).get_restriction_provider()
  watermark_estimator_provider = (
      common.DoFnSignature(dofn).get_watermark_estimator_provider())
  serialized_fn = pickler.dumps(
      (proxy_dofn(dofn, restriction_provider, watermark_estimator_provider), ) +
      dofn_data[1:])
  return _create_pardo_operation(
      factory,
      transform_id,
      transform_proto,
      consumers,
      serialized_fn,
      parameter,
      operation_cls=operation_cls)


@BeamTransformFactory.register_urn(
    common_urns.primitives.PAR_DO.urn, beam_runner_api_pb2.ParDoPayload)
def create_par_do(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,  # type: beam_runner_api_pb2.ParDoPayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.DoOperation
  assert parameter.do_fn.urn == python_urns.PICKLED_DOFN_INFO
  serialized_fn = parameter.do_fn.payload
  return _create_pardo_operation(
      factory,
      transform_id,
      transform_proto,
      consumers,
      serialized_fn,
      parameter)


def _create_pardo_operation(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    consumers,
    serialized_fn,
    pardo_proto=None,  # type: Optional[beam_runner_api_pb2.ParDoPayload]
    operation_cls=operations.DoOperation
):

  if pardo_proto and pardo_proto.side_inputs:
    input_tags_to_coders = factory.get_input_coders(transform_proto)
    tagged_side_inputs = [
        (tag, beam.pvalue.SideInputData.from_runner_api(si, factory.context))
        for tag,
        si in pardo_proto.side_inputs.items()
    ]
    tagged_side_inputs.sort(
        key=lambda tag_si: sideinputs.get_sideinput_index(tag_si[0]))
    side_input_maps = [
        StateBackedSideInputMap(
            factory.state_handler,
            transform_id,
            tag,
            si,
            input_tags_to_coders[tag]) for tag,
        si in tagged_side_inputs
    ]
  else:
    side_input_maps = []

  output_tags = list(transform_proto.outputs.keys())

  dofn_data = pickler.loads(serialized_fn)
  if not dofn_data[-1]:
    # Windowing not set.
    if pardo_proto:
      other_input_tags = set.union(
          set(pardo_proto.side_inputs),
          set(pardo_proto.timer_family_specs))  # type: Container[str]
    else:
      other_input_tags = ()
    pcoll_id, = [pcoll for tag, pcoll in transform_proto.inputs.items()
                 if tag not in other_input_tags]
    windowing = factory.context.windowing_strategies.get_by_id(
        factory.descriptor.pcollections[pcoll_id].windowing_strategy_id)
    serialized_fn = pickler.dumps(dofn_data[:-1] + (windowing, ))

  if pardo_proto and (pardo_proto.timer_family_specs or pardo_proto.state_specs
                      or pardo_proto.restriction_coder_id):
    main_input_coder = None  # type: Optional[WindowedValueCoder]
    for tag, pcoll_id in transform_proto.inputs.items():
      if tag in pardo_proto.side_inputs:
        pass
      else:
        # Must be the main input
        assert main_input_coder is None
        main_input_tag = tag
        main_input_coder = factory.get_windowed_coder(pcoll_id)
    assert main_input_coder is not None

    if pardo_proto.timer_family_specs or pardo_proto.state_specs:
      user_state_context = FnApiUserStateContext(
          factory.state_handler,
          transform_id,
          main_input_coder.key_coder(),
          main_input_coder.window_coder
      )  # type: Optional[FnApiUserStateContext]
    else:
      user_state_context = None
  else:
    user_state_context = None

  output_coders = factory.get_output_coders(transform_proto)
  spec = operation_specs.WorkerDoFn(
      serialized_fn=serialized_fn,
      output_tags=output_tags,
      input=None,
      side_inputs=None,  # Fn API uses proto definitions and the Fn State API
      output_coders=[output_coders[tag] for tag in output_tags])

  result = factory.augment_oldstyle_op(
      operation_cls(
          common.NameContext(transform_proto.unique_name, transform_id),
          spec,
          factory.counter_factory,
          factory.state_sampler,
          side_input_maps,
          user_state_context),
      transform_proto.unique_name,
      consumers,
      output_tags)
  if pardo_proto and pardo_proto.restriction_coder_id:
    result.input_info = (
        transform_id,
        main_input_tag,
        main_input_coder,
        transform_proto.outputs.keys())
  return result


def _create_simple_pardo_operation(factory,  # type: BeamTransformFactory
                                   transform_id,
                                   transform_proto,
                                   consumers,
                                   dofn,  # type: beam.DoFn
                                  ):
  serialized_fn = pickler.dumps((dofn, (), {}, [], None))
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers, serialized_fn)


@BeamTransformFactory.register_urn(
    common_urns.primitives.ASSIGN_WINDOWS.urn,
    beam_runner_api_pb2.WindowingStrategy)
def create_assign_windows(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,  # type: beam_runner_api_pb2.WindowingStrategy
    consumers  # type: Dict[str, List[operations.Operation]]
):
  class WindowIntoDoFn(beam.DoFn):
    def __init__(self, windowing):
      self.windowing = windowing

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        window=beam.DoFn.WindowParam):
      new_windows = self.windowing.windowfn.assign(
          WindowFn.AssignContext(timestamp, element=element, window=window))
      yield WindowedValue(element, timestamp, new_windows)

  from apache_beam.transforms.core import Windowing
  from apache_beam.transforms.window import WindowFn, WindowedValue
  windowing = Windowing.from_runner_api(parameter, factory.context)
  return _create_simple_pardo_operation(
      factory,
      transform_id,
      transform_proto,
      consumers,
      WindowIntoDoFn(windowing))


@BeamTransformFactory.register_urn(IDENTITY_DOFN_URN, None)
def create_identity_dofn(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    parameter,
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.FlattenOperation
  return factory.augment_oldstyle_op(
      operations.FlattenOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          operation_specs.WorkerFlatten(
              None, [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_PRECOMBINE.urn,
    beam_runner_api_pb2.CombinePayload)
def create_combine_per_key_precombine(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,  # type: beam_runner_api_pb2.CombinePayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.PGBKCVOperation
  serialized_combine_fn = pickler.dumps((
      beam.CombineFn.from_runner_api(payload.combine_fn,
                                     factory.context), [], {}))
  return factory.augment_oldstyle_op(
      operations.PGBKCVOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          operation_specs.WorkerPartialGroupByKey(
              serialized_combine_fn,
              None, [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler,
          factory.get_input_windowing(transform_proto)),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_MERGE_ACCUMULATORS.urn,
    beam_runner_api_pb2.CombinePayload)
def create_combbine_per_key_merge_accumulators(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,  # type: beam_runner_api_pb2.CombinePayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  return _create_combine_phase_operation(
      factory, transform_id, transform_proto, payload, consumers, 'merge')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_EXTRACT_OUTPUTS.urn,
    beam_runner_api_pb2.CombinePayload)
def create_combine_per_key_extract_outputs(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,  # type: beam_runner_api_pb2.CombinePayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  return _create_combine_phase_operation(
      factory, transform_id, transform_proto, payload, consumers, 'extract')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS.urn,
    beam_runner_api_pb2.CombinePayload)
def create_combine_per_key_convert_to_accumulators(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,  # type: beam_runner_api_pb2.CombinePayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  return _create_combine_phase_operation(
      factory, transform_id, transform_proto, payload, consumers, 'convert')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
    beam_runner_api_pb2.CombinePayload)
def create_combine_grouped_values(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,  # type: beam_runner_api_pb2.CombinePayload
    consumers  # type: Dict[str, List[operations.Operation]]
):
  return _create_combine_phase_operation(
      factory, transform_id, transform_proto, payload, consumers, 'all')


def _create_combine_phase_operation(
    factory, transform_id, transform_proto, payload, consumers, phase):
  # type: (...) -> operations.CombineOperation
  serialized_combine_fn = pickler.dumps((
      beam.CombineFn.from_runner_api(payload.combine_fn,
                                     factory.context), [], {}))
  return factory.augment_oldstyle_op(
      operations.CombineOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          operation_specs.WorkerCombineFn(
              serialized_combine_fn,
              phase,
              None, [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(common_urns.primitives.FLATTEN.urn, None)
def create_flatten(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    payload,
    consumers  # type: Dict[str, List[operations.Operation]]
):
  # type: (...) -> operations.FlattenOperation
  return factory.augment_oldstyle_op(
      operations.FlattenOperation(
          common.NameContext(transform_proto.unique_name, transform_id),
          operation_specs.WorkerFlatten(
              None, [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.primitives.MAP_WINDOWS.urn, beam_runner_api_pb2.FunctionSpec)
def create_map_windows(
    factory,  # type: BeamTransformFactory
    transform_id,  # type: str
    transform_proto,  # type: beam_runner_api_pb2.PTransform
    mapping_fn_spec,  # type: beam_runner_api_pb2.FunctionSpec
    consumers  # type: Dict[str, List[operations.Operation]]
):
  assert mapping_fn_spec.urn == python_urns.PICKLED_WINDOW_MAPPING_FN
  window_mapping_fn = pickler.loads(mapping_fn_spec.payload)

  class MapWindows(beam.DoFn):
    def process(self, element):
      key, window = element
      return [(key, window_mapping_fn(window))]

  return _create_simple_pardo_operation(
      factory, transform_id, transform_proto, consumers, MapWindows())
