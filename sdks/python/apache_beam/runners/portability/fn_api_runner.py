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
from concurrent import futures

import grpc

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam import coders
from apache_beam import metrics
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
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
from apache_beam.runners.portability.fn_api_runner_transforms import create_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import only_element
from apache_beam.runners.portability.fn_api_runner_transforms import split_buffer_id
from apache_beam.runners.portability.fn_api_runner_transforms import unique_name
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import profiler
from apache_beam.utils import proto_utils

# This module is experimental. No backwards-compatibility guarantees.

ENCODED_IMPULSE_VALUE = beam.coders.WindowedValueCoder(
    beam.coders.BytesCoder(),
    beam.coders.coders.GlobalWindowCoder()).get_impl().encode_nested(
        beam.transforms.window.GlobalWindows.windowed_value(b''))


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):

  UNSTARTED_STATE = 'unstarted'
  STARTED_STATE = 'started'
  DONE_STATE = 'done'

  _DONE_MARKER = object()

  def __init__(self):
    self._push_queue = queue.Queue()
    self._futures_by_id = dict()
    self._read_thread = threading.Thread(
        name='beam_control_read', target=self._read)
    self._uid_counter = 0
    self._state = self.UNSTARTED_STATE
    self._lock = threading.Lock()

  def Control(self, iterator, context):
    with self._lock:
      if self._state == self.DONE_STATE:
        return
      else:
        self._state = self.STARTED_STATE
    self._inputs = iterator
    # Note: We only support one client for now.
    self._read_thread.start()
    while True:
      to_push = self._push_queue.get()
      if to_push is self._DONE_MARKER:
        return
      yield to_push

  def _read(self):
    for data in self._inputs:
      self._futures_by_id.pop(data.instruction_id).set(data)

  def push(self, item):
    if item is self._DONE_MARKER:
      future = None
    else:
      if not item.instruction_id:
        self._uid_counter += 1
        item.instruction_id = 'control_%s' % self._uid_counter
      future = ControlFuture(item.instruction_id)
      self._futures_by_id[item.instruction_id] = future
    self._push_queue.put(item)
    return future

  def done(self):
    with self._lock:
      if self._state == self.STARTED_STATE:
        self.push(self._DONE_MARKER)
        self._read_thread.join()
      self._state = self.DONE_STATE


class _GroupingBuffer(object):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(self, pre_grouped_coder, post_grouped_coder, windowing):
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table = collections.defaultdict(list)
    self._windowing = windowing
    self._grouped_output = None

  def append(self, elements_data):
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

  def __iter__(self):
    if not self._grouped_output:
      output_stream = create_OutputStream()
      if self._windowing.is_default():
        globally_window = GlobalWindows.windowed_value(None).with_value
        windowed_key_values = lambda key, values: [
            globally_window((key, values))]
      else:
        trigger_driver = trigger.create_trigger_driver(self._windowing, True)
        windowed_key_values = trigger_driver.process_entire_key
      coder_impl = self._post_grouped_coder.get_impl()
      key_coder_impl = self._key_coder.get_impl()
      for encoded_key, windowed_values in self._table.items():
        key = key_coder_impl.decode(encoded_key)
        for wkvs in windowed_key_values(key, windowed_values):
          coder_impl.encode_to_stream(wkvs, output_stream, True)
      self._grouped_output = [output_stream.get()]
      self._table = None
    return iter(self._grouped_output)


class _WindowGroupingBuffer(object):
  """Used to partition windowed side inputs."""
  def __init__(self, access_pattern, coder):
    # Here's where we would use a different type of partitioning
    # (e.g. also by key) for a different access pattern.
    if access_pattern.urn == common_urns.side_inputs.ITERABLE.urn:
      self._kv_extrator = lambda value: ('', value)
      self._key_coder = coders.SingletonCoder('')
      self._value_coder = coder.wrapped_value_coder
    elif access_pattern.urn == common_urns.side_inputs.MULTIMAP.urn:
      self._kv_extrator = lambda value: value
      self._key_coder = coder.wrapped_value_coder.key_coder()
      self._value_coder = (
          coder.wrapped_value_coder.value_coder())
    else:
      raise ValueError(
          "Unknown access pattern: '%s'" % access_pattern.urn)
    self._windowed_value_coder = coder
    self._window_coder = coder.window_coder
    self._values_by_window = collections.defaultdict(list)

  def append(self, elements_data):
    input_stream = create_InputStream(elements_data)
    while input_stream.size() > 0:
      windowed_value = self._windowed_value_coder.get_impl(
          ).decode_from_stream(input_stream, True)
      key, value = self._kv_extrator(windowed_value.value)
      for window in windowed_value.windows:
        self._values_by_window[key, window].append(value)

  def encoded_items(self):
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
      default_environment=None,
      bundle_repeat=0,
      use_state_iterables=False,
      provision_info=None):
    """Creates a new Fn API Runner.

    Args:
      default_environment: the default environment to use for UserFns.
      bundle_repeat: replay every bundle this many extra times, for profiling
          and debugging
      use_state_iterables: Intentionally split gbk iterables over state API
          (for testing)
      provision_info: provisioning info to make available to workers, or None
    """
    super(FnApiRunner, self).__init__()
    self._last_uid = -1
    self._default_environment = (
        default_environment
        or beam_runner_api_pb2.Environment(urn=python_urns.EMBEDDED_PYTHON))
    self._bundle_repeat = bundle_repeat
    self._progress_frequency = None
    self._profiler_factory = None
    self._use_state_iterables = use_state_iterables
    self._provision_info = provision_info

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def run_pipeline(self, pipeline, options):
    MetricsEnvironment.set_metrics_supported(False)
    RuntimeValueProvider.set_runtime_options({})
    # This is sometimes needed if type checking is disabled
    # to enforce that the inputs (and outputs) of GroupByKey operations
    # are known to be KVs.
    from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
    pipeline.visit(DataflowRunner.group_by_key_input_visitor())
    self._bundle_repeat = self._bundle_repeat or options.view_as(
        pipeline_options.DirectOptions).direct_runner_bundle_repeat
    self._profiler_factory = profiler.Profile.factory_from_options(
        options.view_as(pipeline_options.ProfilingOptions))
    self._latest_run_result = self.run_via_runner_api(pipeline.to_runner_api(
        default_environment=self._default_environment))
    return self._latest_run_result

  def run_via_runner_api(self, pipeline_proto):
    return self.run_stages(*self.create_stages(pipeline_proto))

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
        logging.warning(
            'The --direct_runner_bundle_repeat option is not set; '
            'a significant portion of the profile may be one-time overhead.')
      path = profiler.profile_output
      print('CPU Profile written to %s' % path)
      try:
        import gprof2dot  # pylint: disable=unused-variable
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

  def create_stages(self, pipeline_proto):
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

  def run_stages(self, stage_context, stages):
    worker_handler_manager = WorkerHandlerManager(
        stage_context.components.environments, self._provision_info)
    metrics_by_stage = {}
    monitoring_infos_by_stage = {}

    try:
      with self.maybe_profile():
        pcoll_buffers = collections.defaultdict(list)
        for stage in stages:
          stage_results = self.run_stage(
              worker_handler_manager.get_worker_handler,
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

  def run_stage(
      self,
      worker_handler_factory,
      pipeline_components,
      stage,
      pcoll_buffers,
      safe_coders):

    def iterable_state_write(values, element_coder_impl):
      token = unique_name(None, 'iter').encode('ascii')
      out = create_OutputStream()
      for element in values:
        element_coder_impl.encode_to_stream(element, out, True)
      controller.state.blocking_append(
          beam_fn_api_pb2.StateKey(
              runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
          out.get())
      return token

    controller = worker_handler_factory(stage.environment)
    context = pipeline_context.PipelineContext(
        pipeline_components, iterable_state_write=iterable_state_write)
    data_api_service_descriptor = controller.data_api_service_descriptor()

    def extract_endpoints(stage):
      # Returns maps of transform names to PCollection identifiers.
      # Also mutates IO stages to point to the data ApiServiceDescriptor.
      data_input = {}
      data_side_input = {}
      data_output = {}
      for transform in stage.transforms:
        if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                  bundle_processor.DATA_OUTPUT_URN):
          pcoll_id = transform.spec.payload
          if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
            target = transform.unique_name, only_element(transform.outputs)
            if pcoll_id == fn_api_runner_transforms.IMPULSE_BUFFER:
              data_input[target] = [ENCODED_IMPULSE_VALUE]
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

    logging.info('Running %s', stage.name)
    logging.debug('       %s', stage)
    data_input, data_side_input, data_output = extract_endpoints(stage)

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms={transform.unique_name: transform
                    for transform in stage.transforms},
        pcollections=dict(pipeline_components.pcollections.items()),
        coders=dict(pipeline_components.coders.items()),
        windowing_strategies=dict(
            pipeline_components.windowing_strategies.items()),
        environments=dict(pipeline_components.environments.items()))

    if controller.state_api_service_descriptor():
      process_bundle_descriptor.state_api_service_descriptor.url = (
          controller.state_api_service_descriptor().url)

    # Store the required side inputs into state.
    for (transform_id, tag), (buffer_id, si) in data_side_input.items():
      _, pcoll_id = split_buffer_id(buffer_id)
      value_coder = context.coders[safe_coders[
          pipeline_components.pcollections[pcoll_id].coder_id]]
      elements_by_window = _WindowGroupingBuffer(si, value_coder)
      for element_data in pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)
      for key, window, elements_data in elements_by_window.encoded_items():
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                ptransform_id=transform_id,
                side_input_id=tag,
                window=window,
                key=key))
        controller.state.blocking_append(state_key, elements_data)

    def get_buffer(buffer_id):
      kind, name = split_buffer_id(buffer_id)
      if kind in ('materialize', 'timers'):
        if buffer_id not in pcoll_buffers:
          # Just store the data chunks for replay.
          pcoll_buffers[buffer_id] = list()
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

    for k in range(self._bundle_repeat):
      try:
        controller.state.checkpoint()
        BundleManager(
            controller, lambda pcoll_id: [], get_input_coder_impl,
            process_bundle_descriptor, self._progress_frequency, k
        ).process_bundle(data_input, data_output)
      finally:
        controller.state.restore()

    result, splits = BundleManager(
        controller, get_buffer, get_input_coder_impl, process_bundle_descriptor,
        self._progress_frequency).process_bundle(
            data_input, data_output)

    def input_for(ptransform_id, input_id):
      input_pcoll = process_bundle_descriptor.transforms[
          ptransform_id].inputs[input_id]
      for read_id, proto in process_bundle_descriptor.transforms.items():
        if (proto.spec.urn == bundle_processor.DATA_INPUT_URN
            and input_pcoll in proto.outputs.values()):
          return read_id, 'out'
      raise RuntimeError(
          'No IO transform feeds %s' % ptransform_id)

    last_result = result
    last_sent = data_input

    while True:
      deferred_inputs = collections.defaultdict(list)
      for transform_id, timer_writes in stage.timer_pcollections:

        # Queue any set timers as new inputs.
        windowed_timer_coder_impl = context.coders[
            pipeline_components.pcollections[timer_writes].coder_id].get_impl()
        written_timers = get_buffer(
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
          deferred_inputs[transform_id, 'out'] = [out.get()]
          written_timers[:] = []

      # Queue any process-initiated delayed bundle applications.
      for delayed_application in last_result.process_bundle.residual_roots:
        deferred_inputs[
            input_for(
                delayed_application.application.ptransform_id,
                delayed_application.application.input_id)
        ].append(delayed_application.application.element)

      # Queue any runner-initiated delayed bundle applications.
      prev_stops = {}
      for split in splits:
        for delayed_application in split.residual_roots:
          deferred_inputs[
              input_for(
                  delayed_application.application.ptransform_id,
                  delayed_application.application.input_id)
          ].append(delayed_application.application.element)
        for channel_split in split.channel_splits:
          coder_impl = get_input_coder_impl(channel_split.ptransform_id)
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
              channel_split.ptransform_id, channel_split.input_id])))
          residual_elements = all_elements[
              channel_split.first_residual_element : prev_stops.get(
                  channel_split.ptransform_id, len(all_elements)) + 1]
          if residual_elements:
            deferred_inputs[
                channel_split.ptransform_id, channel_split.input_id].append(
                    coder_impl.encode_all(residual_elements))
          prev_stops[
              channel_split.ptransform_id] = channel_split.last_primary_element

      if deferred_inputs:
        # The worker will be waiting on these inputs as well.
        for other_input in data_input:
          if other_input not in deferred_inputs:
            deferred_inputs[other_input] = []
        # TODO(robertwb): merge results
        last_result, splits = BundleManager(
            controller,
            get_buffer,
            get_input_coder_impl,
            process_bundle_descriptor,
            self._progress_frequency,
            True).process_bundle(deferred_inputs, data_output)
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
        if self._key in self._overlay:
          return iter(self._overlay[self._key])
        else:
          return iter(self._underlying[self._key])

      def append(self, item):
        if self._key not in self._overlay:
          self._overlay[self._key] = list(self._underlying[self._key])
        self._overlay[self._key].append(item)

    def __init__(self):
      self._lock = threading.Lock()
      self._state = collections.defaultdict(list)
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

    def blocking_get(self, state_key, continuation_token=None):
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

    def blocking_append(self, state_key, data):
      with self._lock:
        self._state[self._to_key(state_key)].append(data)

    def blocking_clear(self, state_key):
      with self._lock:
        del self._state[self._to_key(state_key)]

    @staticmethod
    def _to_key(state_key):
      return state_key.SerializeToString()

  class GrpcStateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer):
    def __init__(self, state):
      self._state = state

    def State(self, request_stream, context=None):
      # Note that this eagerly mutates state, assuming any failures are fatal.
      # Thus it is safe to ignore instruction_reference.
      for request in request_stream:
        request_type = request.WhichOneof('request')
        if request_type == 'get':
          data, continuation_token = self._state.blocking_get(
              request.state_key, request.get.continuation_token)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              get=beam_fn_api_pb2.StateGetResponse(
                  data=data, continuation_token=continuation_token))
        elif request_type == 'append':
          self._state.blocking_append(request.state_key, request.append.data)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              append=beam_fn_api_pb2.StateAppendResponse())
        elif request_type == 'clear':
          self._state.blocking_clear(request.state_key)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              clear=beam_fn_api_pb2.StateClearResponse())
        else:
          raise NotImplementedError('Unknown state request: %s' % request_type)

  class SingletonStateHandlerFactory(sdk_worker.StateHandlerFactory):
    """A singleton cache for a StateServicer."""

    def __init__(self, state_handler):
      self._state_handler = state_handler

    def create_state_handler(self, api_service_descriptor):
      """Returns the singleton state handler."""
      return self._state_handler

    def close(self):
      """Does nothing."""
      pass


class WorkerHandler(object):

  _registered_environments = {}

  def __init__(
      self, control_handler, data_plane_handler, state, provision_info):
    self.control_handler = control_handler
    self.data_plane_handler = data_plane_handler
    self.state = state
    self.provision_info = provision_info

  def close(self):
    self.stop_worker()

  def start_worker(self):
    raise NotImplementedError

  def stop_worker(self):
    raise NotImplementedError

  def data_api_service_descriptor(self):
    raise NotImplementedError

  def state_api_service_descriptor(self):
    raise NotImplementedError

  def logging_api_service_descriptor(self):
    raise NotImplementedError

  @classmethod
  def register_environment(cls, urn, payload_type):
    def wrapper(constructor):
      cls._registered_environments[urn] = constructor, payload_type
      return constructor
    return wrapper

  @classmethod
  def create(cls, environment, state, provision_info):
    constructor, payload_type = cls._registered_environments[environment.urn]
    return constructor(
        proto_utils.parse_Bytes(environment.payload, payload_type),
        state,
        provision_info)


@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON, None)
class EmbeddedWorkerHandler(WorkerHandler):
  """An in-memory controller for fn API control, state and data planes."""

  def __init__(self, unused_payload, state, provision_info):
    super(EmbeddedWorkerHandler, self).__init__(
        self, data_plane.InMemoryDataChannel(), state, provision_info)
    self.worker = sdk_worker.SdkWorker(
        sdk_worker.BundleProcessorCache(
            FnApiRunner.SingletonStateHandlerFactory(self.state),
            data_plane.InMemoryDataChannelFactory(
                self.data_plane_handler.inverse()),
            {}))
    self._uid_counter = 0

  def push(self, request):
    if not request.instruction_id:
      self._uid_counter += 1
      request.instruction_id = 'control_%s' % self._uid_counter
    logging.debug('CONTROL REQUEST %s', request)
    response = self.worker.do_instruction(request)
    logging.debug('CONTROL RESPONSE %s', response)
    return ControlFuture(request.instruction_id, response)

  def start_worker(self):
    pass

  def stop_worker(self):
    pass

  def done(self):
    pass

  def data_api_service_descriptor(self):
    return None

  def state_api_service_descriptor(self):
    return None

  def logging_api_service_descriptor(self):
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
    self._info = info

  def GetProvisionInfo(self, request, context=None):
    return beam_provision_api_pb2.GetProvisionInfoResponse(
        info=self._info)


class GrpcWorkerHandler(WorkerHandler):
  """An grpc based controller for fn API control, state and data planes."""

  _DEFAULT_SHUTDOWN_TIMEOUT_SECS = 5

  def __init__(self, state, provision_info):
    self.state = state
    self.provision_info = provision_info
    self.control_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10))
    self.control_port = self.control_server.add_insecure_port('[::]:0')
    self.control_address = 'localhost:%s' % self.control_port

    # Options to have no limits (-1) on the size of the messages
    # received or sent over the data plane. The actual buffer size
    # is controlled in a layer above.
    no_max_message_sizes = [("grpc.max_receive_message_length", -1),
                            ("grpc.max_send_message_length", -1)]
    self.data_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=no_max_message_sizes)
    self.data_port = self.data_server.add_insecure_port('[::]:0')

    self.state_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
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
        m = beam_artifact_api_pb2_grpc
        m.add_ArtifactRetrievalServiceServicer_to_server(
            artifact_service.BeamFilesystemArtifactService(
                self.provision_info.artifact_staging_dir),
            self.control_server)

    self.data_plane_handler = data_plane.GrpcServerDataChannel()
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
        self.data_plane_handler, self.data_server)

    beam_fn_api_pb2_grpc.add_BeamFnStateServicer_to_server(
        FnApiRunner.GrpcStateServicer(state),
        self.state_server)

    self.logging_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2),
        options=no_max_message_sizes)
    self.logging_port = self.logging_server.add_insecure_port('[::]:0')
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        BasicLoggingService(),
        self.logging_server)

    logging.info('starting control server on port %s', self.control_port)
    logging.info('starting data server on port %s', self.data_port)
    logging.info('starting state server on port %s', self.state_port)
    logging.info('starting logging server on port %s', self.logging_port)
    self.logging_server.start()
    self.state_server.start()
    self.data_server.start()
    self.control_server.start()

  def data_api_service_descriptor(self):
    return endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%s' % self.data_port)

  def state_api_service_descriptor(self):
    return endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%s' % self.state_port)

  def logging_api_service_descriptor(self):
    return endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%s' % self.logging_port)

  def close(self):
    self.control_handler.done()
    self.data_plane_handler.close()
    to_wait = [
        self.control_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.data_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.state_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.logging_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS)
    ]
    for w in to_wait:
      w.wait()
    super(GrpcWorkerHandler, self).close()


@WorkerHandler.register_environment(
    common_urns.environments.EXTERNAL.urn, beam_runner_api_pb2.ExternalPayload)
class ExternalWorkerHandler(GrpcWorkerHandler):
  def __init__(self, external_payload, state, provision_info):
    super(ExternalWorkerHandler, self).__init__(state, provision_info)
    self._external_payload = external_payload

  def start_worker(self):
    stub = beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolStub(
        GRPCChannelFactory.insecure_channel(
            self._external_payload.endpoint.url))
    response = stub.NotifyRunnerAvailable(
        beam_fn_api_pb2.NotifyRunnerAvailableRequest(
            worker_id='worker_%s' % uuid.uuid4(),
            control_endpoint=endpoints_pb2.ApiServiceDescriptor(
                url=self.control_address),
            logging_endpoint=self.logging_api_service_descriptor(),
            params=self._external_payload.params))
    if response.error:
      raise RuntimeError("Error starting worker: %s" % response.error)

  def stop_worker(self):
    pass


@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON_GRPC, bytes)
class EmbeddedGrpcWorkerHandler(GrpcWorkerHandler):
  def __init__(self, num_workers_payload, state, provision_info):
    super(EmbeddedGrpcWorkerHandler, self).__init__(state, provision_info)
    self._num_threads = int(num_workers_payload) if num_workers_payload else 1

  def start_worker(self):
    self.worker = sdk_worker.SdkHarness(
        self.control_address, worker_count=self._num_threads)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.daemon = True
    self.worker_thread.start()

  def stop_worker(self):
    self.worker_thread.join()


@WorkerHandler.register_environment(python_urns.SUBPROCESS_SDK, bytes)
class SubprocessSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self, worker_command_line, state, provision_info):
    super(SubprocessSdkWorkerHandler, self).__init__(state, provision_info)
    self._worker_command_line = worker_command_line

  def start_worker(self):
    from apache_beam.runners.portability import local_job_service
    self.worker = local_job_service.SubprocessSdkWorker(
        self._worker_command_line, self.control_address)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.start()

  def stop_worker(self):
    self.worker_thread.join()


@WorkerHandler.register_environment(common_urns.environments.DOCKER.urn,
                                    beam_runner_api_pb2.DockerPayload)
class DockerSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self, payload, state, provision_info):
    super(DockerSdkWorkerHandler, self).__init__(state, provision_info)
    self._container_image = payload.container_image
    self._container_id = None

  def start_worker(self):
    try:
      subprocess.check_call(['docker', 'pull', self._container_image])
    except Exception:
      logging.info('Unable to pull image %s' % self._container_image)
    self._container_id = subprocess.check_output(
        ['docker',
         'run',
         '-d',
         # TODO:  credentials
         '--network=host',
         self._container_image,
         '--id=%s' % uuid.uuid4(),
         '--logging_endpoint=%s' % self.logging_api_service_descriptor().url,
         '--control_endpoint=%s' % self.control_address,
         '--artifact_endpoint=%s' % self.control_address,
         '--provision_endpoint=%s' % self.control_address,
        ]).strip()
    while True:
      logging.info('Waiting for docker to start up...')
      status = subprocess.check_output([
          'docker',
          'inspect',
          '-f',
          '{{.State.Status}}',
          self._container_id]).strip()
      if status == 'running':
        break
      elif status in ('dead', 'exited'):
        subprocess.call([
            'docker',
            'container',
            'logs',
            self._container_id])
        raise RuntimeError('SDK failed to start.')
      time.sleep(1)

  def stop_worker(self):
    if self._container_id:
      subprocess.call([
          'docker',
          'kill',
          self._container_id])


class WorkerHandlerManager(object):
  def __init__(self, environments, job_provision_info=None):
    self._environments = environments
    self._job_provision_info = job_provision_info
    self._cached_handlers = {}
    self._state = FnApiRunner.StateServicer() # rename?

  def get_worker_handler(self, environment_id):
    if environment_id is None:
      # Any environment will do, pick one arbitrarily.
      environment_id = next(iter(self._environments.keys()))
    environment = self._environments[environment_id]

    worker_handler = self._cached_handlers.get(environment_id)
    if worker_handler is None:
      worker_handler = self._cached_handlers[
          environment_id] = WorkerHandler.create(
              environment, self._state, self._job_provision_info)
      worker_handler.start_worker()
    return worker_handler

  def close_all(self):
    for controller in set(self._cached_handlers.values()):
      try:
        controller.close()
      except Exception:
        logging.info("Error closing controller %s" % controller, exc_info=True)
    self._cached_handlers = {}


class ExtendedProvisionInfo(object):
  def __init__(self, provision_info=None, artifact_staging_dir=None):
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

  _uid_counter = 0

  def __init__(
      self, controller, get_buffer, get_input_coder_impl, bundle_descriptor,
      progress_frequency=None, skip_registration=False):
    self._controller = controller
    self._get_buffer = get_buffer
    self._get_input_coder_impl = get_input_coder_impl
    self._bundle_descriptor = bundle_descriptor
    self._registered = skip_registration
    self._progress_frequency = progress_frequency

  def process_bundle(self, inputs, expected_outputs):
    # Unique id for the instruction processing this bundle.
    BundleManager._uid_counter += 1
    process_bundle_id = 'bundle_%s' % BundleManager._uid_counter

    # Register the bundle descriptor, if needed.
    if self._registered:
      registration_future = None
    else:
      process_bundle_registration = beam_fn_api_pb2.InstructionRequest(
          register=beam_fn_api_pb2.RegisterRequest(
              process_bundle_descriptor=[self._bundle_descriptor]))
      registration_future = self._controller.control_handler.push(
          process_bundle_registration)
      self._registered = True

    unique_names = set(
        t.unique_name for t in self._bundle_descriptor.transforms.values())
    for stage_name, candidate in reversed(_split_managers):
      if (stage_name in unique_names
          or (stage_name + '/Process') in unique_names):
        split_manager = candidate
        break
    else:
      split_manager = None

    if not split_manager:
      # Write all the input data to the channel immediately.
      for (transform_id, name), elements in inputs.items():
        data_out = self._controller.data_plane_handler.output_stream(
            process_bundle_id, beam_fn_api_pb2.Target(
                primitive_transform_reference=transform_id, name=name))
        for element_data in elements:
          data_out.write(element_data)
        data_out.close()

    split_results = []

    # Actually start the bundle.
    if registration_future and registration_future.get().error:
      raise RuntimeError(registration_future.get().error)
    process_bundle = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_reference=self._bundle_descriptor.id))
    result_future = self._controller.control_handler.push(process_bundle)

    with ProgressRequester(
        self._controller, process_bundle_id, self._progress_frequency):
      if split_manager:
        (read_transform_id, name), buffer_data = only_element(inputs.items())
        num_elements = len(list(
            self._get_input_coder_impl(read_transform_id).decode_all(
                b''.join(buffer_data))))

        # Start the split manager in case it wants to set any breakpoints.
        split_manager_generator = split_manager(num_elements)
        try:
          split_fraction = next(split_manager_generator)
          done = False
        except StopIteration:
          done = True

        # Send all the data.
        data_out = self._controller.data_plane_handler.output_stream(
            process_bundle_id,
            beam_fn_api_pb2.Target(
                primitive_transform_reference=read_transform_id, name=name))
        data_out.write(b''.join(buffer_data))
        data_out.close()

        # Execute the requested splits.
        while not done:
          if split_fraction is None:
            split_result = None
          else:
            split_request = beam_fn_api_pb2.InstructionRequest(
                process_bundle_split=
                beam_fn_api_pb2.ProcessBundleSplitRequest(
                    instruction_reference=process_bundle_id,
                    desired_splits={
                        read_transform_id:
                        beam_fn_api_pb2.ProcessBundleSplitRequest.DesiredSplit(
                            fraction_of_remainder=split_fraction,
                            estimated_input_elements=num_elements)
                    }))
            split_response = self._controller.control_handler.push(
                split_request).get()
            for t in (0.05, 0.1, 0.2):
              waiting = ('Instruction not running', 'not yet scheduled')
              if any(msg in split_response.error for msg in waiting):
                time.sleep(t)
                split_response = self._controller.control_handler.push(
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

      # Gather all output data.
      expected_targets = [
          beam_fn_api_pb2.Target(primitive_transform_reference=transform_id,
                                 name=output_name)
          for (transform_id, output_name), _ in expected_outputs.items()]
      logging.debug('Gather all output data from %s.', expected_targets)
      for output in self._controller.data_plane_handler.input_elements(
          process_bundle_id,
          expected_targets,
          abort_callback=lambda: (result_future.is_done()
                                  and result_future.get().error)):
        target_tuple = (
            output.target.primitive_transform_reference, output.target.name)
        if target_tuple in expected_outputs:
          self._get_buffer(expected_outputs[target_tuple]).append(output.data)

      logging.debug('Wait for the bundle to finish.')
      result = result_future.get()

    if result.error:
      raise RuntimeError(result.error)

    if result.process_bundle.requires_finalization:
      finalize_request = beam_fn_api_pb2.InstructionRequest(
          finalize_bundle=
          beam_fn_api_pb2.FinalizeBundleRequest(
              instruction_reference=process_bundle_id
          ))
      self._controller.control_handler.push(
          finalize_request)

    return result, split_results


class ProgressRequester(threading.Thread):
  def __init__(self, controller, instruction_id, frequency, callback=None):
    super(ProgressRequester, self).__init__()
    self._controller = controller
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
        progress_result = self._controller.control_handler.push(
            beam_fn_api_pb2.InstructionRequest(
                process_bundle_progress=
                beam_fn_api_pb2.ProcessBundleProgressRequest(
                    instruction_reference=self._instruction_id))).get()
        self._latest_progress = progress_result.process_bundle_progress
        if self._callback:
          self._callback(self._latest_progress)
      except Exception as exn:
        logging.error("Bad progress: %s", exn)
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


class FnApiMetrics(metrics.metric.MetricResults):
  def __init__(self, step_monitoring_infos, user_metrics_only=True):
    """Used for querying metrics from the PipelineResult object.

      step_monitoring_infos: Per step metrics specified as MonitoringInfos.
      use_monitoring_infos: If true, return the metrics based on the
          step_monitoring_infos.
    """
    self._counters = {}
    self._distributions = {}
    self._gauges = {}
    self._user_metrics_only = user_metrics_only
    self._init_metrics_from_monitoring_infos(step_monitoring_infos)
    self._monitoring_infos = step_monitoring_infos

  def _init_metrics_from_monitoring_infos(self, step_monitoring_infos):
    for smi in step_monitoring_infos.values():
      # Only include user metrics.
      for mi in smi:
        if (self._user_metrics_only and
            not monitoring_infos.is_user_monitoring_info(mi)):
          continue
        key = self._to_metric_key(mi)
        if monitoring_infos.is_counter(mi):
          self._counters[key] = (
              monitoring_infos.extract_metric_result_map_value(mi))
        elif monitoring_infos.is_distribution(mi):
          self._distributions[key] = (
              monitoring_infos.extract_metric_result_map_value(mi))
        elif monitoring_infos.is_gauge(mi):
          self._gauges[key] = (
              monitoring_infos.extract_metric_result_map_value(mi))

  def _to_metric_key(self, monitoring_info):
    # Right now this assumes that all metrics have a PTRANSFORM
    ptransform_id = monitoring_info.labels['PTRANSFORM']
    namespace, name = monitoring_infos.parse_namespace_and_name(monitoring_info)
    return MetricKey(ptransform_id, MetricName(namespace, name))

  def query(self, filter=None):
    counters = [metrics.execution.MetricResult(k, v, v)
                for k, v in self._counters.items()
                if self.matches(filter, k)]
    distributions = [metrics.execution.MetricResult(k, v, v)
                     for k, v in self._distributions.items()
                     if self.matches(filter, k)]
    gauges = [metrics.execution.MetricResult(k, v, v)
              for k, v in self._gauges.items()
              if self.matches(filter, k)]

    return {self.COUNTERS: counters,
            self.DISTRIBUTIONS: distributions,
            self.GAUGES: gauges}

  def monitoring_infos(self):
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
    """Returns a queryable oject including user metrics only."""
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
