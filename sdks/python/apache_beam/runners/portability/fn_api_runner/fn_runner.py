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

import contextlib
import copy
import itertools
import logging
import os
import subprocess
import sys
import threading
import time
from builtins import object
from typing import TYPE_CHECKING
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.metrics import metric
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricResult
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.portability import portable_metrics
from apache_beam.runners.portability.fn_api_runner import execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
from apache_beam.runners.portability.fn_api_runner.execution import WindowGroupingBuffer
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.portability.fn_api_runner.translations import split_buffer_id
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandlerManager
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import environments
from apache_beam.utils import profiler
from apache_beam.utils import proto_utils
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

if TYPE_CHECKING:
  from apache_beam.pipeline import Pipeline
  from apache_beam.coders.coder_impl import CoderImpl
  from apache_beam.portability.api import metrics_pb2

_LOGGER = logging.getLogger(__name__)

T = TypeVar('T')

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
          (for testing)
      provision_info: provisioning info to make available to workers, or None
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
    """
    super(FnApiRunner, self).__init__()
    self._default_environment = (
        default_environment or environments.EmbeddedPythonEnvironment())
    self._bundle_repeat = bundle_repeat
    self._num_workers = 1
    self._progress_frequency = progress_request_frequency
    self._profiler_factory = None  # type: Optional[Callable[..., profiler.Profile]]
    self._use_state_iterables = use_state_iterables
    self._provision_info = provision_info or ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            retrieval_token='unused-retrieval-token'))

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
        if (payload.state_specs or payload.timer_family_specs):
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
            translations.populate_data_channel_coders,
        ],
        known_runner_urns=frozenset([
            common_urns.primitives.FLATTEN.urn,
            common_urns.primitives.GROUP_BY_KEY.urn
        ]),
        use_state_iterables=self._use_state_iterables)

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
    monitoring_infos_by_stage = {}

    runner_execution_context = execution.FnApiRunnerExecutionContext(
        worker_handler_manager,
        stage_context.components,
        stage_context.safe_coders,
        stage_context.data_channel_coders)

    try:
      with self.maybe_profile():
        for stage in stages:
          bundle_context_manager = execution.BundleContextManager(
              runner_execution_context, stage, self._num_workers)
          stage_results = self._run_stage(
              runner_execution_context,
              bundle_context_manager,
          )
          monitoring_infos_by_stage[stage.name] = (
              stage_results.process_bundle.monitoring_infos)
    finally:
      worker_handler_manager.close_all()
    return RunnerResult(runner.PipelineState.DONE, monitoring_infos_by_stage)

  def _store_side_inputs_in_state(self,
                                  runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
                                  data_side_input,  # type: DataSideInput
                                 ):
    # type: (...) -> None
    for (transform_id, tag), (buffer_id, si) in data_side_input.items():
      _, pcoll_id = split_buffer_id(buffer_id)
      value_coder = runner_execution_context.pipeline_context.coders[
          runner_execution_context.safe_coders[
              runner_execution_context.data_channel_coders[pcoll_id]]]
      elements_by_window = WindowGroupingBuffer(si, value_coder)
      if buffer_id not in runner_execution_context.pcoll_buffers:
        runner_execution_context.pcoll_buffers[buffer_id] = ListBuffer(
            coder_impl=value_coder.get_impl())
      for element_data in runner_execution_context.pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)

      if si.urn == common_urns.side_inputs.ITERABLE.urn:
        for _, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                  transform_id=transform_id, side_input_id=tag, window=window))
          (
              runner_execution_context.worker_handler_manager.state_servicer.
              append_raw(state_key, elements_data))
      elif si.urn == common_urns.side_inputs.MULTIMAP.urn:
        for key, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                  transform_id=transform_id,
                  side_input_id=tag,
                  window=window,
                  key=key))
          (
              runner_execution_context.worker_handler_manager.state_servicer.
              append_raw(state_key, elements_data))
      else:
        raise ValueError("Unknown access pattern: '%s'" % si.urn)

  def _run_bundle_multiple_times_for_testing(
      self,
      runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
      bundle_context_manager,  # type: execution.BundleContextManager
      data_input,
      data_output,  # type: DataOutput
      cache_token_generator
  ):
    # type: (...) -> None

    """
    If bundle_repeat > 0, replay every bundle for profiling and debugging.
    """
    # all workers share state, so use any worker_handler.
    for k in range(self._bundle_repeat):
      try:
        runner_execution_context.state_servicer.checkpoint()
        testing_bundle_manager = ParallelBundleManager(
            bundle_context_manager.worker_handlers,
            lambda pcoll_id,
            transform_id: ListBuffer(
                coder_impl=bundle_context_manager.get_input_coder_impl),
            bundle_context_manager.get_input_coder_impl,
            bundle_context_manager.process_bundle_descriptor,
            self._progress_frequency,
            k,
            num_workers=self._num_workers,
            cache_token_generator=cache_token_generator)
        testing_bundle_manager.process_bundle(data_input, data_output)
      finally:
        runner_execution_context.state_servicer.restore()

  def _collect_written_timers_and_add_to_deferred_inputs(
      self,
      runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
      bundle_context_manager,  # type: execution.BundleContextManager
      deferred_inputs,  # type: MutableMapping[str, PartitionableBuffer]
  ):
    # type: (...) -> None

    for (transform_id,
         timer_writes) in bundle_context_manager.stage.timer_pcollections:

      # Queue any set timers as new inputs.
      windowed_timer_coder_impl = (
          runner_execution_context.pipeline_context.coders[
              runner_execution_context.data_channel_coders[timer_writes]].
          get_impl())
      written_timers = bundle_context_manager.get_buffer(
          create_buffer_id(timer_writes, kind='timers'), transform_id)
      if not written_timers.cleared:
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
        deferred_inputs[transform_id] = ListBuffer(
            coder_impl=windowed_timer_coder_impl)
        deferred_inputs[transform_id].append(out.get())
        written_timers.clear()

  def _add_residuals_and_channel_splits_to_deferred_inputs(
      self,
      splits,  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
      bundle_context_manager,  # type: execution.BundleContextManager
      last_sent,
      deferred_inputs  # type: MutableMapping[str, PartitionableBuffer]
  ):
    # type: (...) -> None

    prev_stops = {}  # type: Dict[str, int]
    for split in splits:
      for delayed_application in split.residual_roots:
        name = bundle_context_manager.input_for(
            delayed_application.application.transform_id,
            delayed_application.application.input_id)
        if name not in deferred_inputs:
          deferred_inputs[name] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(name))
        deferred_inputs[name].append(delayed_application.application.element)
      for channel_split in split.channel_splits:
        coder_impl = bundle_context_manager.get_input_coder_impl(
            channel_split.transform_id)
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
        all_elements = list(
            coder_impl.decode_all(
                b''.join(last_sent[channel_split.transform_id])))
        residual_elements = all_elements[
            channel_split.first_residual_element:prev_stops.
            get(channel_split.transform_id, len(all_elements)) + 1]
        if residual_elements:
          if channel_split.transform_id not in deferred_inputs:
            coder_impl = bundle_context_manager.get_input_coder_impl(
                channel_split.transform_id)
            deferred_inputs[channel_split.transform_id] = ListBuffer(
                coder_impl=coder_impl)
          deferred_inputs[channel_split.transform_id].append(
              coder_impl.encode_all(residual_elements))
        prev_stops[
            channel_split.transform_id] = channel_split.last_primary_element

  def _run_stage(self,
                 runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
                 bundle_context_manager,  # type: execution.BundleContextManager
                ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse

    """Run an individual stage.

    Args:
      runner_execution_context (execution.FnApiRunnerExecutionContext): An
        object containing execution information for the pipeline.
      stage (translations.Stage): A description of the stage to execute.
    """
    worker_handler_list = bundle_context_manager.worker_handlers
    worker_handler_manager = runner_execution_context.worker_handler_manager
    _LOGGER.info('Running %s', bundle_context_manager.stage.name)
    data_input, data_side_input, data_output = self._extract_endpoints(
        bundle_context_manager, runner_execution_context)
    worker_handler_manager.register_process_bundle_descriptor(
        bundle_context_manager.process_bundle_descriptor)

    # Store the required side inputs into state so it is accessible for the
    # worker when it runs this bundle.
    self._store_side_inputs_in_state(runner_execution_context, data_side_input)

    # Change cache token across bundle repeats
    cache_token_generator = FnApiRunner.get_cache_token_generator(static=False)

    self._run_bundle_multiple_times_for_testing(
        runner_execution_context,
        bundle_context_manager,
        data_input,
        data_output,
        cache_token_generator=cache_token_generator)

    bundle_manager = ParallelBundleManager(
        worker_handler_list,
        bundle_context_manager.get_buffer,
        bundle_context_manager.get_input_coder_impl,
        bundle_context_manager.process_bundle_descriptor,
        self._progress_frequency,
        num_workers=self._num_workers,
        cache_token_generator=cache_token_generator)

    result, splits = bundle_manager.process_bundle(data_input, data_output)

    last_result = result
    last_sent = data_input

    # We cannot split deferred_input until we include residual_roots to
    # merged results. Without residual_roots, pipeline stops earlier and we
    # may miss some data.
    bundle_manager._num_workers = 1
    while True:
      deferred_inputs = {}  # type: Dict[str, PartitionableBuffer]

      self._collect_written_timers_and_add_to_deferred_inputs(
          runner_execution_context, bundle_context_manager, deferred_inputs)
      # Queue any process-initiated delayed bundle applications.
      for delayed_application in last_result.process_bundle.residual_roots:
        name = bundle_context_manager.input_for(
            delayed_application.application.transform_id,
            delayed_application.application.input_id)
        if name not in deferred_inputs:
          deferred_inputs[name] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(name))
        deferred_inputs[name].append(delayed_application.application.element)
      # Queue any runner-initiated delayed bundle applications.
      self._add_residuals_and_channel_splits_to_deferred_inputs(
          splits, bundle_context_manager, last_sent, deferred_inputs)

      if deferred_inputs:
        # The worker will be waiting on these inputs as well.
        for other_input in data_input:
          if other_input not in deferred_inputs:
            deferred_inputs[other_input] = ListBuffer(
                coder_impl=bundle_context_manager.get_input_coder_impl(
                    other_input))
        # TODO(robertwb): merge results
        # TODO(BEAM-8486): this should be changed to _registered
        bundle_manager._skip_registration = True  # type: ignore[attr-defined]
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
  def _extract_endpoints(bundle_context_manager,  # type: execution.BundleContextManager
                         runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
                         ):
    # type: (...) -> Tuple[Dict[str, PartitionableBuffer], DataSideInput, DataOutput]

    """Returns maps of transform names to PCollection identifiers.

    Also mutates IO stages to point to the data ApiServiceDescriptor.

    Args:
      stage (translations.Stage): The stage to extract endpoints
        for.
      data_api_service_descriptor: A GRPC endpoint descriptor for data plane.
    Returns:
      A tuple of (data_input, data_side_input, data_output) dictionaries.
        `data_input` is a dictionary mapping (transform_name, output_name) to a
        PCollection buffer; `data_output` is a dictionary mapping
        (transform_name, output_name) to a PCollection ID.
    """
    data_input = {}  # type: Dict[str, PartitionableBuffer]
    data_side_input = {}  # type: DataSideInput
    data_output = {}  # type: DataOutput
    for transform in bundle_context_manager.stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = runner_execution_context.data_channel_coders[only_element(
              transform.outputs.values())]
          coder = runner_execution_context.pipeline_context.coders[
              runner_execution_context.safe_coders.get(coder_id, coder_id)]
          if pcoll_id == translations.IMPULSE_BUFFER:
            data_input[transform.unique_name] = ListBuffer(
                coder_impl=coder.get_impl())
            data_input[transform.unique_name].append(ENCODED_IMPULSE_VALUE)
          else:
            if pcoll_id not in runner_execution_context.pcoll_buffers:
              runner_execution_context.pcoll_buffers[pcoll_id] = ListBuffer(
                  coder_impl=coder.get_impl())
            data_input[transform.unique_name] = (
                runner_execution_context.pcoll_buffers[pcoll_id])
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          data_output[transform.unique_name] = pcoll_id
          coder_id = runner_execution_context.data_channel_coders[only_element(
              transform.inputs.values())]
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        data_api_service_descriptor = (
            bundle_context_manager.data_api_service_descriptor())
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for tag, si in payload.side_inputs.items():
          data_side_input[transform.unique_name, tag] = (
              create_buffer_id(transform.inputs[tag]), si.access_pattern)
    return data_input, data_side_input, data_output

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

  def process_bundle(self,
                     inputs,  # type: Mapping[str, PartitionableBuffer]
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
        self._send_input_to_worker(process_bundle_id, transform_id, elements)

    # Actually start the bundle.
    process_bundle_req = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=self._bundle_descriptor.id,
            cache_tokens=[next(self._cache_token_generator)]))
    result_future = self._worker_handler.control_conn.push(process_bundle_req)

    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    with ProgressRequester(self._worker_handler,
                           process_bundle_id,
                           self._progress_frequency):

      if split_manager:
        split_results = self._generate_splits_for_testing(
            split_manager, inputs, process_bundle_id)

      # Gather all output data.
      for output in self._worker_handler.data_conn.input_elements(
          process_bundle_id,
          expected_outputs.keys(),
          abort_callback=lambda:
          (result_future.is_done() and result_future.get().error)):
        if output.transform_id in expected_outputs:
          with BundleManager._lock:
            self._get_buffer(
                expected_outputs[output.transform_id],
                output.transform_id).append(output.data)

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
  def __init__(self, state, monitoring_infos_by_stage):
    super(RunnerResult, self).__init__(state)
    self._monitoring_infos_by_stage = monitoring_infos_by_stage
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
