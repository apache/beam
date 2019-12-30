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
import uuid
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
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

import grpc

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
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import fn_api_runner_transforms
from apache_beam.runners.portability import portable_metrics
from apache_beam.runners.portability.fn_api_runner_transforms import create_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import only_element
from apache_beam.runners.portability.fn_api_runner_transforms import split_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import unique_name
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
from apache_beam.utils import windowed_value
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

if TYPE_CHECKING:
  from google.protobuf import message  # pylint: disable=ungrouped-imports
  from apache_beam.pipeline import Pipeline
  from apache_beam.coders.coder_impl import CoderImpl
  from apache_beam.coders.coder_impl import WindowedValueCoderImpl
  from apache_beam.portability.api import metrics_pb2
  from apache_beam.transforms.window import BoundedWindow

T = TypeVar('T')
ConstructorFn = Callable[
    [Union['message.Message', bytes],
     'FnApiRunner.StateServicer',
     Optional['ExtendedProvisionInfo'],
     'GrpcServer'],
    'WorkerHandler']
DataSideInput = Dict[Tuple[str, str],
                     Tuple[bytes, beam_runner_api_pb2.FunctionSpec]]
DataOutput = Dict[str, bytes]
BundleProcessResult = Tuple[beam_fn_api_pb2.InstructionResponse, List[beam_fn_api_pb2.ProcessBundleSplitResponse]]

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
    self._push_queue = queue.Queue()  # type: queue.Queue[beam_fn_api_pb2.InstructionRequest]
    self._input = None  # type: Optional[Iterable[beam_fn_api_pb2.InstructionResponse]]
    self._futures_by_id = dict()  # type: Dict[str, ControlFuture]
    self._read_thread = threading.Thread(
        name='beam_control_read', target=self._read)
    self._state = BeamFnControlServicer.UNSTARTED_STATE

  def _read(self):
    for data in self._input:
      self._futures_by_id.pop(data.instruction_id).set(data)

  def push(self, req):
    # type: (...) -> Optional[ControlFuture]
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

  _DONE_MARKER = object()

  def __init__(self):
    self._lock = threading.Lock()
    self._uid_counter = 0
    self._state = self.UNSTARTED_STATE
    # following self._req_* variables are used for debugging purpose, data is
    # added only when self._log_req is True.
    self._req_sent = collections.defaultdict(int)
    self._req_worker_mapping = {}
    self._log_req = logging.getLogger().getEffectiveLevel() <= logging.DEBUG
    self._connections_by_worker_id = collections.defaultdict(ControlConnection)  # type: DefaultDict[str, ControlConnection]

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
      raise RuntimeError('All workers communicate through gRPC should have '
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
    _LOGGER.debug('Runner: Requests sent by runner: %s',
                  [(str(req), cnt) for req, cnt in self._req_sent.items()])
    _LOGGER.debug('Runner: Requests multiplexing info: %s',
                  [(str(req), worker) for req, worker
                   in self._req_worker_mapping.items()])


class _ListBuffer(list):
  """Used to support parititioning of a list."""
  def partition(self, n):
    # type: (int) -> List[List[bytes]]
    return [self[k::n] for k in range(n)]


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
      self._table = None
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
  def __init__(self,
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


class FnApiRunner(runner.PipelineRunner):

  def __init__(
      self,
      default_environment=None,  # type: Optional[beam_runner_api_pb2.Environment]
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
          (for testing)
      provision_info: provisioning info to make available to workers, or None
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
    """
    super(FnApiRunner, self).__init__()
    self._last_uid = -1
    self._default_environment = (
        default_environment
        or environments.EmbeddedPythonEnvironment())
    self._bundle_repeat = bundle_repeat
    self._num_workers = 1
    self._progress_frequency = progress_request_frequency
    self._profiler_factory = None  # type: Optional[Callable[..., profiler.Profile]]
    self._use_state_iterables = use_state_iterables
    self._provision_info = provision_info or ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            job_id='unknown-job-id',
            job_name='unknown-job-name',
            retrieval_token='unused-retrieval-token'))

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def run_pipeline(self,
                   pipeline,  # type: Pipeline
                   options  # type: pipeline_options.PipelineOptions
                  ):
    # type: (...) -> RunnerResult
    RuntimeValueProvider.set_runtime_options({})

    # Setup "beam_fn_api" experiment options if lacked.
    experiments = (options.view_as(pipeline_options.DebugOptions).experiments
                   or [])
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
    self._profiler_factory = profiler.Profile.factory_from_options(
        options.view_as(pipeline_options.ProfilingOptions))

    self._latest_run_result = self.run_via_runner_api(pipeline.to_runner_api(
        default_environment=self._default_environment))
    return self._latest_run_result

  def run_via_runner_api(self, pipeline_proto):
    # type: (beam_runner_api_pb2.Pipeline) -> RunnerResult
    stage_context, stages = self.create_stages(pipeline_proto)
    # TODO(pabloem, BEAM-7514): Create a watermark manager (that has access to
    #   the teststream (if any), and all the stages).
    return self.run_stages(stage_context, stages)

  @contextlib.contextmanager
  def maybe_profile(self):
    if self._profiler_factory:
      try:
        profile_id = 'direct-' + subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
        ).decode(errors='ignore').strip()
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
        if not subprocess.call([
            sys.executable, '-m', 'gprof2dot',
            '-f', 'pstats', path, '-o', path + '.dot']):
          if not subprocess.call(
              ['dot', '-Tsvg', '-o', path + '.svg', path + '.dot']):
            print('CPU Profile rendering at file://%s.svg'
                  % os.path.abspath(path))
      except ImportError:
        # pylint: disable=superfluous-parens
        print('Please install gprof2dot and dot for profile renderings.')

    else:
      # Empty context.
      yield

  def create_stages(self,
                    pipeline_proto  # type: beam_runner_api_pb2.Pipeline
                   ):
    # type: (...) -> Tuple[fn_api_runner_transforms.TransformContext, List[fn_api_runner_transforms.Stage]]
    return fn_api_runner_transforms.create_and_optimize_stages(
        copy.deepcopy(pipeline_proto),
        phases=[fn_api_runner_transforms.annotate_downstream_side_inputs,
                fn_api_runner_transforms.fix_side_input_pcoll_coders,
                fn_api_runner_transforms.lift_combiners,
                fn_api_runner_transforms.expand_sdf,
                fn_api_runner_transforms.expand_gbk,
                fn_api_runner_transforms.sink_flattens,
                fn_api_runner_transforms.greedily_fuse,
                fn_api_runner_transforms.read_to_impulse,
                fn_api_runner_transforms.impulse_to_input,
                fn_api_runner_transforms.inject_timer_pcollections,
                fn_api_runner_transforms.sort_stages,
                fn_api_runner_transforms.window_pcollection_coders],
        known_runner_urns=frozenset([
            common_urns.primitives.FLATTEN.urn,
            common_urns.primitives.GROUP_BY_KEY.urn]),
        use_state_iterables=self._use_state_iterables)

  def run_stages(self,
                 stage_context,  # type: fn_api_runner_transforms.TransformContext
                 stages  # type: List[fn_api_runner_transforms.Stage]
                ):
    # type: (...) -> RunnerResult
    """Run a list of topologically-sorted stages in batch mode.

    Args:
      stage_context (fn_api_runner_transforms.TransformContext)
      stages (list[fn_api_runner_transforms.Stage])
    """
    worker_handler_manager = WorkerHandlerManager(
        stage_context.components.environments, self._provision_info)
    metrics_by_stage = {}
    monitoring_infos_by_stage = {}

    try:
      with self.maybe_profile():
        pcoll_buffers = collections.defaultdict(_ListBuffer)  # type: DefaultDict[bytes, _ListBuffer]
        for stage in stages:
          stage_results = self._run_stage(
              worker_handler_manager.get_worker_handlers,
              stage_context.components,
              stage,
              pcoll_buffers,
              stage_context.safe_coders)
          metrics_by_stage[stage.name] = stage_results.process_bundle.metrics
          monitoring_infos_by_stage[stage.name] = (
              stage_results.process_bundle.monitoring_infos)
    finally:
      worker_handler_manager.close_all()
    return RunnerResult(
        runner.PipelineState.DONE, monitoring_infos_by_stage, metrics_by_stage)

  def _store_side_inputs_in_state(self,
                                  worker_handler,  # type: WorkerHandler
                                  context,  # type: pipeline_context.PipelineContext
                                  pipeline_components,  # type: beam_runner_api_pb2.Components
                                  data_side_input,  # type: DataSideInput
                                  pcoll_buffers,  # type: Mapping[bytes, _ListBuffer]
                                  safe_coders):
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
            worker_handler_list, lambda pcoll_id: [],
            get_input_coder_callable, process_bundle_descriptor,
            self._progress_frequency, k,
            num_workers=self._num_workers,
            cache_token_generator=cache_token_generator
        )
        testing_bundle_manager.process_bundle(data_input, data_output)
      finally:
        worker_handler.state.restore()

  def _collect_written_timers_and_add_to_deferred_inputs(
      self,
      context,  # type: pipeline_context.PipelineContext
      pipeline_components,  # type: beam_runner_api_pb2.Components
      stage,  # type: fn_api_runner_transforms.Stage
      get_buffer_callable,
      deferred_inputs  # type: DefaultDict[str, _ListBuffer]
      ):

    for transform_id, timer_writes in stage.timer_pcollections:

      # Queue any set timers as new inputs.
      windowed_timer_coder_impl = context.coders[
          pipeline_components.pcollections[timer_writes].coder_id].get_impl()
      written_timers = get_buffer_callable(
          create_buffer_id(timer_writes, kind='timers'))
      if written_timers:
        # Keep only the "last" timer set per key and window.
        timers_by_key_and_window = {}
        for elements_data in written_timers:
          input_stream = create_InputStream(elements_data)
          while input_stream.size() > 0:
            windowed_key_timer = windowed_timer_coder_impl.decode_from_stream(
                input_stream, True)
            key, _ = windowed_key_timer.value
            # TODO: Explode and merge windows.
            assert len(windowed_key_timer.windows) == 1
            timers_by_key_and_window[
                key, windowed_key_timer.windows[0]] = windowed_key_timer
        out = create_OutputStream()
        for windowed_key_timer in timers_by_key_and_window.values():
          windowed_timer_coder_impl.encode_to_stream(
              windowed_key_timer, out, True)
        deferred_inputs[transform_id] = _ListBuffer([out.get()])
        written_timers[:] = []

  def _add_residuals_and_channel_splits_to_deferred_inputs(
      self,
      splits,  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
      get_input_coder_callable,
      input_for_callable,
      last_sent,
      deferred_inputs  # type: DefaultDict[str, _ListBuffer]
  ):

    prev_stops = {}  # type: Dict[str, int]
    for split in splits:
      for delayed_application in split.residual_roots:
        deferred_inputs[
            input_for_callable(
                delayed_application.application.transform_id,
                delayed_application.application.input_id)
        ].append(delayed_application.application.element)
      for channel_split in split.channel_splits:
        coder_impl = get_input_coder_callable(channel_split.transform_id)
        # TODO(SDF): This requires determanistic ordering of buffer iteration.
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
        all_elements = list(coder_impl.decode_all(b''.join(last_sent[
            channel_split.transform_id])))
        residual_elements = all_elements[
            channel_split.first_residual_element : prev_stops.get(
                channel_split.transform_id, len(all_elements)) + 1]
        if residual_elements:
          deferred_inputs[channel_split.transform_id].append(
              coder_impl.encode_all(residual_elements))
        prev_stops[
            channel_split.transform_id] = channel_split.last_primary_element

  @staticmethod
  def _extract_stage_data_endpoints(
      stage,  # type: fn_api_runner_transforms.Stage
      pipeline_components,  # type: beam_runner_api_pb2.Components
      data_api_service_descriptor,
      pcoll_buffers  # type: DefaultDict[bytes, _ListBuffer]
  ):
    # Returns maps of transform names to PCollection identifiers.
    # Also mutates IO stages to point to the data ApiServiceDescriptor.
    data_input = {}
    data_side_input = {}   # type: DataSideInput
    data_output = {}    # type: Dict[Tuple[str, str], bytes]
    for transform in stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          target = transform.unique_name, only_element(transform.outputs)
          if pcoll_id == fn_api_runner_transforms.IMPULSE_BUFFER:
            data_input[target] = _ListBuffer([ENCODED_IMPULSE_VALUE])
          else:
            data_input[target] = pcoll_buffers[pcoll_id]
          coder_id = pipeline_components.pcollections[
              only_element(transform.outputs.values())].coder_id
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          target = transform.unique_name, only_element(transform.inputs)
          data_output[target] = pcoll_id
          coder_id = pipeline_components.pcollections[
              only_element(transform.inputs.values())].coder_id
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in fn_api_runner_transforms.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for tag, si in payload.side_inputs.items():
          data_side_input[transform.unique_name, tag] = (
              create_buffer_id(transform.inputs[tag]), si.access_pattern)
    return data_input, data_side_input, data_output

  def _run_stage(self,
                 worker_handler_factory,  # type: Callable[[Optional[str], int], List[WorkerHandler]]
                 pipeline_components,  # type: beam_runner_api_pb2.Components
                 stage,  # type: fn_api_runner_transforms.Stage
                 pcoll_buffers,  # type: DefaultDict[bytes, _ListBuffer]
                 safe_coders
                ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    """Run an individual stage.

    Args:
      worker_handler_factory: A ``callable`` that takes in an environment id
        and a number of workers, and returns a list of ``WorkerHandler``s.
      pipeline_components (beam_runner_api_pb2.Components): TODO
      stage (fn_api_runner_transforms.Stage)
      pcoll_buffers (collections.defaultdict of str: list): Mapping of
        PCollection IDs to list that functions as buffer for the
        ``beam.PCollection``.
      safe_coders (dict): TODO
    """
    def iterable_state_write(values, element_coder_impl):
      # type: (...) -> bytes
      token = unique_name(None, 'iter').encode('ascii')
      out = create_OutputStream()
      for element in values:
        element_coder_impl.encode_to_stream(element, out, True)
      worker_handler.state.append_raw(
          beam_fn_api_pb2.StateKey(
              runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
          out.get())
      return token

    worker_handler_list = worker_handler_factory(
        stage.environment, self._num_workers)

    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    worker_handler = next(iter(worker_handler_list))
    context = pipeline_context.PipelineContext(
        pipeline_components, iterable_state_write=iterable_state_write)
    data_api_service_descriptor = worker_handler.data_api_service_descriptor()

    _LOGGER.info('Running %s', stage.name)
    data_input, data_side_input, data_output = self._extract_endpoints(
        stage, pipeline_components, data_api_service_descriptor, pcoll_buffers)

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms={transform.unique_name: transform
                    for transform in stage.transforms},
        pcollections=dict(pipeline_components.pcollections.items()),
        coders=dict(pipeline_components.coders.items()),
        windowing_strategies=dict(
            pipeline_components.windowing_strategies.items()),
        environments=dict(pipeline_components.environments.items()))

    if worker_handler.state_api_service_descriptor():
      process_bundle_descriptor.state_api_service_descriptor.url = (
          worker_handler.state_api_service_descriptor().url)

    # Store the required side inputs into state so it is accessible for the
    # worker when it runs this bundle.
    self._store_side_inputs_in_state(worker_handler,
                                     context,
                                     pipeline_components,
                                     data_side_input,
                                     pcoll_buffers,
                                     safe_coders)

    def get_buffer(buffer_id):
      """Returns the buffer for a given (operation_type, PCollection ID).

      For grouping-typed operations, we produce a ``_GroupingBuffer``. For
      others, we produce a ``_ListBuffer``.
      """
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
          pre_gbk_coder = context.coders[safe_coders[
              pipeline_components.pcollections[input_pcoll].coder_id]]
          post_gbk_coder = context.coders[safe_coders[
              pipeline_components.pcollections[output_pcoll].coder_id]]
          windowing_strategy = context.windowing_strategies[
              pipeline_components
              .pcollections[output_pcoll].windowing_strategy_id]
          pcoll_buffers[buffer_id] = _GroupingBuffer(
              pre_gbk_coder, post_gbk_coder, windowing_strategy)
      else:
        # These should be the only two identifiers we produce for now,
        # but special side input writes may go here.
        raise NotImplementedError(buffer_id)
      return pcoll_buffers[buffer_id]

    def get_input_coder_impl(transform_id):
      return context.coders[safe_coders[
          beam_fn_api_pb2.RemoteGrpcPort.FromString(
              process_bundle_descriptor.transforms[transform_id].spec.payload
          ).coder_id
      ]].get_impl()

    # Change cache token across bundle repeats
    cache_token_generator = FnApiRunner.get_cache_token_generator(static=False)

    self._run_bundle_multiple_times_for_testing(
        worker_handler_list, process_bundle_descriptor, data_input, data_output,
        get_input_coder_impl, cache_token_generator=cache_token_generator)

    bundle_manager = ParallelBundleManager(
        worker_handler_list, get_buffer, get_input_coder_impl,
        process_bundle_descriptor, self._progress_frequency,
        num_workers=self._num_workers,
        cache_token_generator=cache_token_generator)

    result, splits = bundle_manager.process_bundle(data_input, data_output)

    def input_for(transform_id, input_id):
      # type: (str, str) -> str
      input_pcoll = process_bundle_descriptor.transforms[
          transform_id].inputs[input_id]
      for read_id, proto in process_bundle_descriptor.transforms.items():
        if (proto.spec.urn == bundle_processor.DATA_INPUT_URN
            and input_pcoll in proto.outputs.values()):
          return read_id
      raise RuntimeError(
          'No IO transform feeds %s' % transform_id)

    last_result = result
    last_sent = data_input

    while True:
      deferred_inputs = collections.defaultdict(_ListBuffer)  # type: DefaultDict[str, _ListBuffer]

      self._collect_written_timers_and_add_to_deferred_inputs(
          context, pipeline_components, stage, get_buffer, deferred_inputs)

      # Queue any process-initiated delayed bundle applications.
      for delayed_application in last_result.process_bundle.residual_roots:
        deferred_inputs[
            input_for(
                delayed_application.application.transform_id,
                delayed_application.application.input_id)
        ].append(delayed_application.application.element)

      # Queue any runner-initiated delayed bundle applications.
      self._add_residuals_and_channel_splits_to_deferred_inputs(
          splits, get_input_coder_impl, input_for, last_sent, deferred_inputs)

      if deferred_inputs:
        # The worker will be waiting on these inputs as well.
        for other_input in data_input:
          if other_input not in deferred_inputs:
            deferred_inputs[other_input] = _ListBuffer([])
        # TODO(robertwb): merge results
        # We cannot split deferred_input until we include residual_roots to
        # merged results. Without residual_roots, pipeline stops earlier and we
        # may miss some data.
        bundle_manager._num_workers = 1
        bundle_manager._skip_registration = True
        last_result, splits = bundle_manager.process_bundle(
            deferred_inputs, data_output)
        last_sent = deferred_inputs
        result = beam_fn_api_pb2.InstructionResponse(
            process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                monitoring_infos=monitoring_infos.consolidate(
                    itertools.chain(
                        result.process_bundle.monitoring_infos,
                        last_result.process_bundle.monitoring_infos))),
            error=result.error or last_result.error)
      else:
        break

    return result

  @staticmethod
  def _extract_endpoints(stage,  # type: fn_api_runner_transforms.Stage
                         pipeline_components,  # type: beam_runner_api_pb2.Components
                         data_api_service_descriptor, # type: Optional[endpoints_pb2.ApiServiceDescriptor]
                         pcoll_buffers  # type: DefaultDict[bytes, _ListBuffer]
                        ):
    # type: (...) -> Tuple[Dict[str, _ListBuffer], DataSideInput, DataOutput]
    """Returns maps of transform names to PCollection identifiers.

    Also mutates IO stages to point to the data ApiServiceDescriptor.

    Args:
      stage (fn_api_runner_transforms.Stage): The stage to extract endpoints
        for.
      pipeline_components (beam_runner_api_pb2.Components): Components of the
        pipeline to include coders, transforms, PCollections, etc.
      data_api_service_descriptor: A GRPC endpoint descriptor for data plane.
      pcoll_buffers (dict): A dictionary containing buffers for PCollection
        elements.
    Returns:
      A tuple of (data_input, data_side_input, data_output) dictionaries.
        `data_input` is a dictionary mapping (transform_name, output_name) to a
        PCollection buffer; `data_output` is a dictionary mapping
        (transform_name, output_name) to a PCollection ID.
    """
    data_input = {}  # type: Dict[str, _ListBuffer]
    data_side_input = {}  # type: DataSideInput
    data_output = {}  # type: DataOutput
    for transform in stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          if pcoll_id == fn_api_runner_transforms.IMPULSE_BUFFER:
            data_input[transform.unique_name] = _ListBuffer(
                [ENCODED_IMPULSE_VALUE])
          else:
            data_input[transform.unique_name] = pcoll_buffers[pcoll_id]
          coder_id = pipeline_components.pcollections[
              only_element(transform.outputs.values())].coder_id
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          data_output[transform.unique_name] = pcoll_id
          coder_id = pipeline_components.pcollections[
              only_element(transform.inputs.values())].coder_id
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in fn_api_runner_transforms.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for tag, si in payload.side_inputs.items():
          data_side_input[transform.unique_name, tag] = (
              create_buffer_id(transform.inputs[tag]), si.access_pattern)
    return data_input, data_side_input, data_output

  # These classes are used to interact with the worker.

  class StateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer):

    class CopyOnWriteState(object):
      def __init__(self, underlying):
        self._underlying = underlying
        self._overlay = {}

      def __getitem__(self, key):
        if key in self._overlay:
          return self._overlay[key]
        else:
          return FnApiRunner.StateServicer.CopyOnWriteList(
              self._underlying, self._overlay, key)

      def __delitem__(self, key):
        # type: (bytes) -> None
        self._overlay[key] = []

      def commit(self):
        self._underlying.update(self._overlay)
        return self._underlying

    class CopyOnWriteList(object):
      def __init__(self, underlying, overlay, key):
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

    def __init__(self):
      self._lock = threading.Lock()
      self._state = collections.defaultdict(list)  # type: DefaultDict[bytes, List[bytes]]
      self._checkpoint = None
      self._use_continuation_tokens = False
      self._continuations = {}

    def checkpoint(self):
      assert self._checkpoint is None
      self._checkpoint = self._state
      self._state = FnApiRunner.StateServicer.CopyOnWriteState(self._state)

    def commit(self):
      self._state.commit()
      self._state = self._checkpoint.commit()
      self._checkpoint = None

    def restore(self):
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
            token_base = 'token_%x' % len(self._continuations)
            self._continuations[token_base] = tuple(full_state)
            return b'', '%s:0' % token_base
          else:
            token_base, index = continuation_token.split(':')
            ix = int(index)
            full_state = self._continuations[token_base]
            if ix == len(full_state):
              return b'', None
            else:
              return full_state[ix], '%s:%d' % (token_base, ix + 1)
        else:
          assert not continuation_token
          return b''.join(full_state), None

    def append_raw(self,
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
              id=request.id,
              append=beam_fn_api_pb2.StateAppendResponse())
        elif request_type == 'clear':
          self._state.clear(request.state_key)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              clear=beam_fn_api_pb2.StateClearResponse())
        else:
          raise NotImplementedError('Unknown state request: %s' % request_type)

  class SingletonStateHandlerFactory(sdk_worker.StateHandlerFactory):
    """A singleton cache for a StateServicer."""

    def __init__(self, state_handler):
      # type: (sdk_worker.StateHandler) -> None
      self._state_handler = state_handler

    def create_state_handler(self, api_service_descriptor):
      # type: (endpoints_pb2.ApiServiceDescriptor) -> sdk_worker.StateHandler
      """Returns the singleton state handler."""
      return self._state_handler

    def close(self):
      # type: (...) -> None
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
          user_state=beam_fn_api_pb2
          .ProcessBundleRequest.CacheToken.UserState(),
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
  def register_environment(cls,
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
               state,
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               unused_grpc_server=None
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
            self.data_plane_handler.inverse()),
        {})
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


class BasicProvisionService(
    beam_provision_api_pb2_grpc.ProvisionServiceServicer):

  def __init__(self, info):
    # type: (Optional[beam_provision_api_pb2.ProvisionInfo]) -> None
    self._info = info

  def GetProvisionInfo(self, request, context=None):
    # type: (...) -> beam_provision_api_pb2.GetProvisionInfoResponse
    return beam_provision_api_pb2.GetProvisionInfoResponse(
        info=self._info)


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
        UnboundedThreadPoolExecutor(),
        options=no_max_message_sizes)
    self.data_port = self.data_server.add_insecure_port('[::]:0')

    self.state_server = grpc.server(
        UnboundedThreadPoolExecutor(),
        options=no_max_message_sizes)
    self.state_port = self.state_server.add_insecure_port('[::]:0')

    self.control_handler = BeamFnControlServicer()
    beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
        self.control_handler, self.control_server)

    # If we have provision info, serve these off the control port as well.
    if self.provision_info:
      if self.provision_info.provision_info:
        provision_info = self.provision_info.provision_info
        if not provision_info.worker_id:
          provision_info = copy.copy(provision_info)
          provision_info.worker_id = str(uuid.uuid4())
        beam_provision_api_pb2_grpc.add_ProvisionServiceServicer_to_server(
            BasicProvisionService(self.provision_info.provision_info),
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
        FnApiRunner.GrpcStateServicer(state),
        self.state_server)

    self.logging_server = grpc.server(
        UnboundedThreadPoolExecutor(),
        options=no_max_message_sizes)
    self.logging_port = self.logging_server.add_insecure_port('[::]:0')
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        BasicLoggingService(),
        self.logging_server)

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
        self._grpc_server.control_handler, self._grpc_server.data_plane_handler,
        state, provision_info)
    self.state = state

    self.control_address = self.port_from_worker(self._grpc_server.control_port)
    self.control_conn = self._grpc_server.control_handler.get_conn_by_worker_id(
        self.worker_id)

    self.data_conn = self._grpc_server.data_plane_handler.get_conn_by_worker_id(
        self.worker_id)

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
    super(ExternalWorkerHandler, self).__init__(state, provision_info,
                                                grpc_server)
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
    super(EmbeddedGrpcWorkerHandler, self).__init__(state, provision_info,
                                                    grpc_server)

    from apache_beam.transforms.environments import EmbeddedPythonGrpcEnvironment
    config = EmbeddedPythonGrpcEnvironment.parse_config(
        payload.decode('utf-8'))
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
    super(SubprocessSdkWorkerHandler, self).__init__(state, provision_info,
                                                     grpc_server)
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


@WorkerHandler.register_environment(common_urns.environments.DOCKER.urn,
                                    beam_runner_api_pb2.DockerPayload)
class DockerSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               payload,  # type: beam_runner_api_pb2.DockerPayload
               state,  # type: FnApiRunner.StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(DockerSdkWorkerHandler, self).__init__(state, provision_info,
                                                 grpc_server)
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
      self._container_id = subprocess.check_output(
          ['docker',
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
      while True:
        status = subprocess.check_output([
            'docker',
            'inspect',
            '-f',
            '{{.State.Status}}',
            self._container_id]).strip()
        _LOGGER.info('Waiting for docker to start up.Current status is %s' %
                     status)
        if status == b'running':
          _LOGGER.info('Docker container is running. container_id = %s, '
                       'worker_id = %s', self._container_id, self.worker_id)
          break
        elif status in (b'dead', b'exited'):
          subprocess.call([
              'docker',
              'container',
              'logs',
              self._container_id])
          raise RuntimeError('SDK failed to start. Final status is %s' % status)
      time.sleep(1)

  def stop_worker(self):
    # type: () -> None
    if self._container_id:
      with SUBPROCESS_LOCK:
        subprocess.call([
            'docker',
            'kill',
            self._container_id])


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
    self._cached_handlers = collections.defaultdict(list)  # type: DefaultDict[str, List[WorkerHandler]]
    self._state = FnApiRunner.StateServicer() # rename?
    self._grpc_server = None  # type: Optional[GrpcServer]

  def get_worker_handlers(self,
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
      pass # no need for a gRPC server
    elif self._grpc_server is None:
      self._grpc_server = GrpcServer(self._state, self._job_provision_info)

    worker_handler_list = self._cached_handlers[environment_id]
    if len(worker_handler_list) < num_workers:
      for _ in range(len(worker_handler_list), num_workers):
        worker_handler = WorkerHandler.create(
            environment, self._state, self._job_provision_info,
            self._grpc_server)
        _LOGGER.info("Created Worker handler %s for environment %s",
                     worker_handler, environment)
        self._cached_handlers[environment_id].append(worker_handler)
        worker_handler.start_worker()
    return self._cached_handlers[environment_id][:num_workers]

  def close_all(self):
    for worker_handler_list in self._cached_handlers.values():
      for worker_handler in set(worker_handler_list):
        try:
          worker_handler.close()
        except Exception:
          _LOGGER.error("Error closing worker_handler %s" % worker_handler,
                        exc_info=True)
    self._cached_handlers = {}
    if self._grpc_server is not None:
      self._grpc_server.close()
      self._grpc_server = None


class ExtendedProvisionInfo(object):
  def __init__(self,
               provision_info=None,  # type: Optional[beam_provision_api_pb2.ProvisionInfo]
               artifact_staging_dir=None
              ):
    self.provision_info = (
        provision_info or beam_provision_api_pb2.ProvisionInfo())
    self.artifact_staging_dir = artifact_staging_dir


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
               get_buffer,  # type: Callable[[bytes], list]
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
      if (stage_name in unique_names
          or (stage_name + '/Process') in unique_names):
        split_manager = candidate
        break
    else:
      split_manager = None

    return split_manager

  def _generate_splits_for_testing(self,
                                   split_manager,
                                   inputs,  # type: Mapping[str, _ListBuffer]
                                   process_bundle_id):
    # type: (...) -> List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    read_transform_id, buffer_data = only_element(inputs.items())

    byte_stream = b''.join(buffer_data)
    num_elements = len(list(
        self._get_input_coder_impl(read_transform_id).decode_all(byte_stream)))

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

    # Execute the requested splits.
    while not done:
      if split_fraction is None:
        split_result = None
      else:
        split_request = beam_fn_api_pb2.InstructionRequest(
            process_bundle_split=
            beam_fn_api_pb2.ProcessBundleSplitRequest(
                instruction_id=process_bundle_id,
                desired_splits={
                    read_transform_id:
                    beam_fn_api_pb2.ProcessBundleSplitRequest.DesiredSplit(
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

  def process_bundle(self,
                     inputs,  # type: Mapping[str, _ListBuffer]
                     expected_outputs  # type: DataOutput
                    ):
    # type: (...) -> BundleProcessResult
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
        self._send_input_to_worker(
            process_bundle_id, transform_id, elements)

    # Actually start the bundle.
    process_bundle_req = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=self._bundle_descriptor.id,
            cache_tokens=[next(self._cache_token_generator)]))
    result_future = self._worker_handler.control_conn.push(process_bundle_req)

    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    with ProgressRequester(
        self._worker_handler, process_bundle_id, self._progress_frequency):

      if split_manager:
        split_results = self._generate_splits_for_testing(
            split_manager, inputs, process_bundle_id)

      # Gather all output data.
      for output in self._worker_handler.data_conn.input_elements(
          process_bundle_id,
          expected_outputs.keys(),
          abort_callback=lambda: (result_future.is_done()
                                  and result_future.get().error)):
        if output.transform_id in expected_outputs:
          with BundleManager._lock:
            self._get_buffer(
                expected_outputs[output.transform_id]).append(output.data)

      _LOGGER.debug('Wait for the bundle %s to finish.' % process_bundle_id)
      result = result_future.get()  # type: beam_fn_api_pb2.InstructionResponse

    if result.error:
      raise RuntimeError(result.error)

    if result.process_bundle.requires_finalization:
      finalize_request = beam_fn_api_pb2.InstructionRequest(
          finalize_bundle=
          beam_fn_api_pb2.FinalizeBundleRequest(
              instruction_id=process_bundle_id
          ))
      self._worker_handler.control_conn.push(finalize_request)

    return result, split_results


class ParallelBundleManager(BundleManager):

  def __init__(
      self,
      worker_handler_list,  # type: Sequence[WorkerHandler]
      get_buffer,  # type: Callable[[bytes], list]
      get_input_coder_impl,  # type: Callable[[str], CoderImpl]
      bundle_descriptor,  # type: beam_fn_api_pb2.ProcessBundleDescriptor
      progress_frequency=None,
      skip_registration=False,
      cache_token_generator=None,
      **kwargs):
    # type: (...) -> None
    super(ParallelBundleManager, self).__init__(
        worker_handler_list, get_buffer, get_input_coder_impl,
        bundle_descriptor, progress_frequency, skip_registration,
        cache_token_generator=cache_token_generator)
    self._num_workers = kwargs.pop('num_workers', 1)

  def process_bundle(self,
                     inputs,  # type: Mapping[str, _ListBuffer]
                     expected_outputs  # type: DataOutput
                    ):
    # type: (...) -> BundleProcessResult
    part_inputs = [{} for _ in range(self._num_workers)]  # type: List[Dict[str, List[bytes]]]
    for name, input in inputs.items():
      for ix, part in enumerate(input.partition(self._num_workers)):
        part_inputs[ix][name] = part

    merged_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]
    split_result_list = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    with UnboundedThreadPoolExecutor() as executor:
      for result, split_result in executor.map(lambda part: BundleManager(
          self._worker_handler_list, self._get_buffer,
          self._get_input_coder_impl, self._bundle_descriptor,
          self._progress_frequency, self._registered,
          cache_token_generator=self._cache_token_generator).process_bundle(
              part, expected_outputs), part_inputs):

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
                process_bundle_progress=
                beam_fn_api_pb2.ProcessBundleProgressRequest(
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
    counters = [MetricResult(k, v, v)
                for k, v in self._counters.items()
                if self.matches(filter, k)]
    distributions = [MetricResult(k, v, v)
                     for k, v in self._distributions.items()
                     if self.matches(filter, k)]
    gauges = [MetricResult(k, v, v)
              for k, v in self._gauges.items()
              if self.matches(filter, k)]

    return {self.COUNTERS: counters,
            self.DISTRIBUTIONS: distributions,
            self.GAUGES: gauges}

  def monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]
    return [item for sublist in self._monitoring_infos.values() for item in
            sublist]


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
