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
# mypy: check-untyped-defs

import contextlib
import copy
import itertools
import logging
import multiprocessing
import os
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar
from typing import Union

from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.metrics import metric
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.common import group_by_key_input_visitor
from apache_beam.runners.portability import portable_metrics
from apache_beam.runners.portability.fn_api_runner import execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandlerManager
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import environments
from apache_beam.utils import proto_utils
from apache_beam.utils import thread_pool_executor
from apache_beam.utils import timestamp
from apache_beam.utils.profiler import Profile

if TYPE_CHECKING:
  from apache_beam.pipeline import Pipeline
  from apache_beam.portability.api import metrics_pb2
  from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandler

_LOGGER = logging.getLogger(__name__)

T = TypeVar('T')

DataSideInput = Dict[Tuple[str, str],
                     Tuple[bytes, beam_runner_api_pb2.FunctionSpec]]
DataOutput = Dict[str, bytes]
OutputTimers = Dict[Tuple[str, str], bytes]
BundleProcessResult = Tuple[beam_fn_api_pb2.InstructionResponse,
                            List[beam_fn_api_pb2.ProcessBundleSplitResponse]]

# This module is experimental. No backwards-compatibility guarantees.


class FnApiRunner(runner.PipelineRunner):

  NUM_FUSED_STAGES_COUNTER = "__num_fused_stages"

  def __init__(
      self,
      default_environment=None,  # type: Optional[environments.Environment]
      bundle_repeat=0,  # type: int
      use_state_iterables=False,  # type: bool
      provision_info=None,  # type: Optional[ExtendedProvisionInfo]
      progress_request_frequency=None,  # type: Optional[float]
      is_drain=False  # type: bool
  ):
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
      is_drain: identify whether expand the sdf graph in the drain mode.
    """
    super().__init__()
    self._default_environment = (
        default_environment or environments.EmbeddedPythonEnvironment.default())
    self._bundle_repeat = bundle_repeat
    self._num_workers = 1
    self._progress_frequency = progress_request_frequency
    self._profiler_factory = None  # type: Optional[Callable[..., Profile]]
    self._use_state_iterables = use_state_iterables
    self._is_drain = is_drain
    self._provision_info = provision_info or ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            retrieval_token='unused-retrieval-token'))

  @staticmethod
  def supported_requirements():
    # type: () -> Tuple[str, ...]
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
    pipeline.visit(
        group_by_key_input_visitor(
            not options.view_as(pipeline_options.TypeOptions).
            allow_non_deterministic_key_coders))
    self._bundle_repeat = self._bundle_repeat or options.view_as(
        pipeline_options.DirectOptions).direct_runner_bundle_repeat
    pipeline_direct_num_workers = options.view_as(
        pipeline_options.DirectOptions).direct_num_workers
    if pipeline_direct_num_workers == 0:
      self._num_workers = multiprocessing.cpu_count()
    else:
      self._num_workers = pipeline_direct_num_workers or self._num_workers

    # set direct workers running mode if it is defined with pipeline options.
    running_mode = \
      options.view_as(pipeline_options.DirectOptions).direct_running_mode
    if running_mode == 'multi_threading':
      self._default_environment = (
          environments.EmbeddedPythonGrpcEnvironment.default())
    elif running_mode == 'multi_processing':
      command_string = '%s -m apache_beam.runners.worker.sdk_worker_main' \
                    % sys.executable
      self._default_environment = (
          environments.SubprocessSDKEnvironment.from_command_string(
              command_string=command_string))

    if running_mode == 'in_memory' and self._num_workers != 1:
      _LOGGER.warning(
          'If direct_num_workers is not equal to 1, direct_running_mode '
          'should be `multi_processing` or `multi_threading` instead of '
          '`in_memory` in order for it to have the desired worker parallelism '
          'effect. direct_num_workers: %d ; running_mode: %s',
          self._num_workers,
          running_mode)

    self._profiler_factory = Profile.factory_from_options(
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
    # type: () -> Iterator[None]
    if self._profiler_factory:
      try:
        profile_id = 'direct-' + subprocess.check_output([
            'git', 'rev-parse', '--abbrev-ref', 'HEAD'
        ]).decode(errors='ignore').strip()
      except subprocess.CalledProcessError:
        profile_id = 'direct-unknown'
      profiler = self._profiler_factory(
          profile_id, time_prefix='')  # type: Optional[Profile]
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
    # type: (beam_runner_api_pb2.Pipeline) -> None

    """As a test runner, validate requirements were set correctly."""
    expected_requirements = set()

    def add_requirements(transform_id):
      # type: (str) -> None
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
    # type: (beam_runner_api_pb2.Pipeline) -> None

    """Check that this runner can satisfy all pipeline requirements."""
    supported_requirements = set(self.supported_requirements())
    for requirement in pipeline_proto.requirements:
      if requirement not in supported_requirements:
        raise ValueError(
            'Unable to run pipeline with requirement: %s' % requirement)
    for transform in pipeline_proto.components.transforms.values():
      if transform.spec.urn == common_urns.primitives.TEST_STREAM.urn:
        raise NotImplementedError(transform.spec.urn)
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for timer in payload.timer_family_specs.values():
          if timer.time_domain != beam_runner_api_pb2.TimeDomain.EVENT_TIME:
            raise NotImplementedError(timer.time_domain)

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
            translations.pack_combiners,
            translations.lift_combiners,
            translations.expand_sdf,
            translations.expand_gbk,
            translations.sink_flattens,
            translations.greedily_fuse,
            translations.read_to_impulse,
            translations.impulse_to_input,
            translations.sort_stages,
            translations.setup_timer_mapping,
            translations.populate_data_channel_coders,
        ],
        known_runner_urns=frozenset([
            common_urns.primitives.FLATTEN.urn,
            common_urns.primitives.GROUP_BY_KEY.urn,
        ]),
        use_state_iterables=self._use_state_iterables,
        is_drain=self._is_drain)

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
    pipeline_metrics = MetricsContainer('')
    pipeline_metrics.get_counter(
        MetricName(str(type(self)),
                   self.NUM_FUSED_STAGES_COUNTER)).update(len(stages))
    monitoring_infos_by_stage = {}

    runner_execution_context = execution.FnApiRunnerExecutionContext(
        stages,
        worker_handler_manager,
        stage_context.components,
        stage_context.safe_coders,
        stage_context.data_channel_coders)

    try:
      with self.maybe_profile():
        for stage in stages:
          bundle_context_manager = execution.BundleContextManager(
              runner_execution_context, stage, self._num_workers)

          assert (
              runner_execution_context.watermark_manager.get_stage_node(
                  bundle_context_manager.stage.name
              ).input_watermark() == timestamp.MAX_TIMESTAMP), (
              'wrong watermark for %s. Expected %s, but got %s.' % (
                  runner_execution_context.watermark_manager.get_stage_node(
                      bundle_context_manager.stage.name),
                  timestamp.MAX_TIMESTAMP,
                  runner_execution_context.watermark_manager.get_stage_node(
                      bundle_context_manager.stage.name
                  ).input_watermark()
              )
          )

          stage_results = self._run_stage(
              runner_execution_context, bundle_context_manager)

          assert (
              runner_execution_context.watermark_manager.get_stage_node(
                  bundle_context_manager.stage.name
              ).input_watermark() == timestamp.MAX_TIMESTAMP), (
              'wrong input watermark for %s. Expected %s, but got %s.' % (
              runner_execution_context.watermark_manager.get_stage_node(
                  bundle_context_manager.stage.name),
              timestamp.MAX_TIMESTAMP,
              runner_execution_context.watermark_manager.get_stage_node(
                  bundle_context_manager.stage.name
              ).output_watermark())
          )

          monitoring_infos_by_stage[stage.name] = (
              list(stage_results.process_bundle.monitoring_infos))

      monitoring_infos_by_stage[''] = list(
          pipeline_metrics.to_runner_api_monitoring_infos('').values())
    finally:
      worker_handler_manager.close_all()
    return RunnerResult(runner.PipelineState.DONE, monitoring_infos_by_stage)

  def _run_bundle_multiple_times_for_testing(
      self,
      runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
      bundle_manager,  # type: BundleManager
      data_input,  # type: Dict[str, execution.PartitionableBuffer]
      data_output,  # type: DataOutput
      fired_timers,  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]
      expected_output_timers,  # type: Dict[Tuple[str, str], bytes]
  ) -> None:
    """
    If bundle_repeat > 0, replay every bundle for profiling and debugging.
    """
    # all workers share state, so use any worker_handler.
    for _ in range(self._bundle_repeat):
      try:
        runner_execution_context.state_servicer.checkpoint()
        bundle_manager.process_bundle(
            data_input,
            data_output,
            fired_timers,
            expected_output_timers,
            dry_run=True)
      finally:
        runner_execution_context.state_servicer.restore()

  @staticmethod
  def _collect_written_timers(
      bundle_context_manager: execution.BundleContextManager,
  ) -> Tuple[Dict[translations.TimerFamilyId, timestamp.Timestamp],
             Dict[translations.TimerFamilyId, ListBuffer]]:
    """Review output buffers, and collect written timers.

    This function reviews a stage that has just been run. The stage will have
    written timers to its output buffers. The function then takes the timers,
    and adds them to the `newly_set_timers` dictionary, and the
    timer_watermark_data dictionary.

    The function then returns the following two elements in a tuple:
    - timer_watermark_data: A dictionary mapping timer family to upcoming
        timestamp to fire.
    - newly_set_timers: A dictionary mapping timer family to timer buffers
        to be passed to the SDK upon firing.
    """
    timer_watermark_data = {}
    newly_set_timers = {}
    for (transform_id, timer_family_id) in bundle_context_manager.stage.timers:
      written_timers = bundle_context_manager.get_buffer(
          create_buffer_id(timer_family_id, kind='timers'), transform_id)
      assert isinstance(written_timers, ListBuffer)
      timer_coder_impl = bundle_context_manager.get_timer_coder_impl(
          transform_id, timer_family_id)
      if not written_timers.cleared:
        timers_by_key_and_window = {}
        for elements_timers in written_timers:
          for decoded_timer in timer_coder_impl.decode_all(elements_timers):
            timers_by_key_and_window[decoded_timer.user_key,
                                     decoded_timer.windows[0]] = decoded_timer
        out = create_OutputStream()
        for decoded_timer in timers_by_key_and_window.values():
          # Only add not cleared timer to fired timers.
          if not decoded_timer.clear_bit:
            timer_coder_impl.encode_to_stream(decoded_timer, out, True)
            if (transform_id, timer_family_id) not in timer_watermark_data:
              timer_watermark_data[(transform_id,
                                    timer_family_id)] = timestamp.MAX_TIMESTAMP
            timer_watermark_data[(transform_id, timer_family_id)] = min(
                timer_watermark_data[(transform_id, timer_family_id)],
                decoded_timer.hold_timestamp)
        newly_set_timers[(transform_id, timer_family_id)] = ListBuffer(
            coder_impl=timer_coder_impl)
        newly_set_timers[(transform_id, timer_family_id)].append(out.get())
        written_timers.clear()

    return timer_watermark_data, newly_set_timers

  def _add_sdk_delayed_applications_to_deferred_inputs(
      self,
      bundle_context_manager,  # type: execution.BundleContextManager
      bundle_result,  # type: beam_fn_api_pb2.InstructionResponse
      deferred_inputs  # type: MutableMapping[str, execution.PartitionableBuffer]
  ):
    # type: (...) -> Set[str]

    """Returns a set of PCollection IDs of PColls having delayed applications.

    This transform inspects the bundle_context_manager, and bundle_result
    objects, and adds all deferred inputs to the deferred_inputs object.
    """
    pcolls_with_delayed_apps = set()
    for delayed_application in bundle_result.process_bundle.residual_roots:
      producer_name = bundle_context_manager.input_for(
          delayed_application.application.transform_id,
          delayed_application.application.input_id)
      if producer_name not in deferred_inputs:
        deferred_inputs[producer_name] = ListBuffer(
            coder_impl=bundle_context_manager.get_input_coder_impl(
                producer_name))
      deferred_inputs[producer_name].append(
          delayed_application.application.element)

      transform = bundle_context_manager.process_bundle_descriptor.transforms[
          producer_name]
      # We take the output with tag 'out' from the producer transform. The
      # producer transform is a GRPC read, and it has a single output.
      pcolls_with_delayed_apps.add(only_element(transform.outputs.values()))
    return pcolls_with_delayed_apps

  def _add_residuals_and_channel_splits_to_deferred_inputs(
      self,
      splits,  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
      bundle_context_manager,  # type: execution.BundleContextManager
      last_sent,  # type: Dict[str, execution.PartitionableBuffer]
      deferred_inputs  # type: MutableMapping[str, execution.PartitionableBuffer]
  ):
    # type: (...) -> Tuple[Set[str], Set[str]]

    """Returns a two sets representing PCollections with watermark holds.

    The first set represents PCollections with delayed root applications.
    The second set represents PTransforms with channel splits.
    """

    pcolls_with_delayed_apps = set()
    transforms_with_channel_splits = set()
    prev_stops = {}  # type: Dict[str, int]
    for split in splits:
      for delayed_application in split.residual_roots:
        producer_name = bundle_context_manager.input_for(
            delayed_application.application.transform_id,
            delayed_application.application.input_id)
        if producer_name not in deferred_inputs:
          deferred_inputs[producer_name] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(
                  producer_name))
        deferred_inputs[producer_name].append(
            delayed_application.application.element)
        # We take the output with tag 'out' from the producer transform. The
        # producer transform is a GRPC read, and it has a single output.
        pcolls_with_delayed_apps.add(
            bundle_context_manager.process_bundle_descriptor.
            transforms[producer_name].outputs['out'])
      for channel_split in split.channel_splits:
        coder_impl = bundle_context_manager.get_input_coder_impl(
            channel_split.transform_id)
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
            channel_split.first_residual_element:prev_stops.
            get(channel_split.transform_id, len(all_elements)) + 1]
        if residual_elements:
          transform = (
              bundle_context_manager.process_bundle_descriptor.transforms[
                  channel_split.transform_id])
          assert transform.spec.urn == bundle_processor.DATA_INPUT_URN
          transforms_with_channel_splits.add(transform.unique_name)

          if channel_split.transform_id not in deferred_inputs:
            coder_impl = bundle_context_manager.get_input_coder_impl(
                channel_split.transform_id)
            deferred_inputs[channel_split.transform_id] = ListBuffer(
                coder_impl=coder_impl)
          deferred_inputs[channel_split.transform_id].append(
              coder_impl.encode_all(residual_elements))
        prev_stops[
            channel_split.transform_id] = channel_split.last_primary_element
    return pcolls_with_delayed_apps, transforms_with_channel_splits

  def _run_stage(self,
                 runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
                 bundle_context_manager,  # type: execution.BundleContextManager
                ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse

    """Run an individual stage.

    Args:
      runner_execution_context (execution.FnApiRunnerExecutionContext): An
        object containing execution information for the pipeline.
      bundle_context_manager (execution.BundleContextManager): A description of
        the stage to execute, and its context.
    """
    data_input, data_output, expected_timer_output = (
        bundle_context_manager.extract_bundle_inputs_and_outputs())
    input_timers = {
    }  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]

    worker_handler_manager = runner_execution_context.worker_handler_manager
    _LOGGER.info('Running %s', bundle_context_manager.stage.name)
    worker_handler_manager.register_process_bundle_descriptor(
        bundle_context_manager.process_bundle_descriptor)

    # We create the bundle manager here, as it can be reused for bundles of the
    # same stage, but it may have to be created by-bundle later on.
    cache_token_generator = FnApiRunner.get_cache_token_generator(static=False)
    if bundle_context_manager.num_workers == 1:
      # Avoid thread/processor pools for increased performance and debugability.
      bundle_manager_type = BundleManager
    elif bundle_context_manager.stage.is_stateful():
      # State is keyed, and a single key cannot be processed concurrently.
      # Alternatively, we could arrange to partition work by key.
      bundle_manager_type = BundleManager
    else:
      bundle_manager_type = ParallelBundleManager
    bundle_manager = bundle_manager_type(
        bundle_context_manager,
        self._progress_frequency,
        cache_token_generator=cache_token_generator)

    final_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]

    def merge_results(last_result):
      # type: (beam_fn_api_pb2.InstructionResponse) -> beam_fn_api_pb2.InstructionResponse

      """ Merge the latest result with other accumulated results. """
      return (
          last_result
          if final_result is None else beam_fn_api_pb2.InstructionResponse(
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  monitoring_infos=monitoring_infos.consolidate(
                      itertools.chain(
                          final_result.process_bundle.monitoring_infos,
                          last_result.process_bundle.monitoring_infos))),
              error=final_result.error or last_result.error))

    while True:
      last_result, deferred_inputs, fired_timers, watermark_updates = (
          self._run_bundle(
              runner_execution_context,
              bundle_context_manager,
              data_input,
              data_output,
              input_timers,
              expected_timer_output,
              bundle_manager))

      for pc_name, watermark in watermark_updates.items():
        runner_execution_context.watermark_manager.set_pcoll_watermark(
            pc_name, watermark)

      final_result = merge_results(last_result)
      if not deferred_inputs and not fired_timers:
        break
      else:
        assert (runner_execution_context.watermark_manager.get_stage_node(
            bundle_context_manager.stage.name).output_watermark()
                < timestamp.MAX_TIMESTAMP), (
            'wrong timestamp for %s. '
            % runner_execution_context.watermark_manager.get_stage_node(
            bundle_context_manager.stage.name))
        data_input = deferred_inputs
        input_timers = fired_timers

    # Store the required downstream side inputs into state so it is accessible
    # for the worker when it runs bundles that consume this stage's output.
    data_side_input = (
        runner_execution_context.side_input_descriptors_by_stage.get(
            bundle_context_manager.stage.name, {}))
    runner_execution_context.commit_side_inputs_to_state(data_side_input)

    return final_result

  @staticmethod
  def _build_watermark_updates(
      runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
      stage_inputs,  # type: Iterable[str]
      expected_timers,  # type: Iterable[translations.TimerFamilyId]
      pcolls_with_da,  # type: Set[str]
      transforms_w_splits,  # type: Set[str]
      watermarks_by_transform_and_timer_family  # type: Dict[translations.TimerFamilyId, timestamp.Timestamp]
  ) -> Dict[Union[str, translations.TimerFamilyId], timestamp.Timestamp]:
    """Builds a dictionary of PCollection (or TimerFamilyId) to timestamp.

    Args:
      stage_inputs: represent the set of expected input PCollections for a
        stage. These do not include timers.
      expected_timers: represent the set of TimerFamilyIds that the stage can
        expect to receive as inputs.
      pcolls_with_da: represent the set of stage input PCollections that had
        delayed applications.
      transforms_w_splits: represent the set of transforms in the stage that had
        input splits.
      watermarks_by_transform_and_timer_family: represent the set of watermark
        holds to be added for each timer family.
    """
    updates = {
    }  # type: Dict[Union[str, translations.TimerFamilyId], timestamp.Timestamp]

    def get_pcoll_id(transform_id):
      buffer_id = runner_execution_context.input_transform_to_buffer_id[
          transform_id]
      # For IMPULSE-reading transforms, we use the transform name as buffer id.
      if buffer_id == translations.IMPULSE_BUFFER:
        pcollection_id = transform_id
      else:
        _, pcollection_id = translations.split_buffer_id(buffer_id)
      return pcollection_id

    # Any PCollections that have deferred applications should have their
    # watermark held back.
    for pcoll in pcolls_with_da:
      updates[pcoll] = timestamp.MIN_TIMESTAMP

    # Also any transforms with splits should have their input PCollection's
    # watermark held back.
    for tr in transforms_w_splits:
      pcoll_id = get_pcoll_id(tr)
      updates[pcoll_id] = timestamp.MIN_TIMESTAMP

    # For all expected stage timers, we have two possible outcomes:
    # 1) If the stage set a firing time for the timer, then we hold the
    #    watermark at that time
    # 2) If the stage did not set a firing time for the timer, then we
    #    advance the watermark for that timer to MAX_TIMESTAMP.
    for timer_pcoll_id in expected_timers:
      updates[timer_pcoll_id] = watermarks_by_transform_and_timer_family.get(
          timer_pcoll_id, timestamp.MAX_TIMESTAMP)

    # For any PCollection in the set of stage inputs, if its watermark was not
    # held back (i.e. there weren't splits in its consumer PTransform, and there
    # weren't delayed applications of the PCollection's elements), then the
    # watermark should be advanced to MAX_TIMESTAMP.
    for transform_id in stage_inputs:
      pcoll_id = get_pcoll_id(transform_id)
      if pcoll_id not in updates:
        updates[pcoll_id] = timestamp.MAX_TIMESTAMP
    return updates

  def _run_bundle(
      self,
      runner_execution_context,  # type: execution.FnApiRunnerExecutionContext
      bundle_context_manager,  # type: execution.BundleContextManager
      data_input,  # type: Dict[str, execution.PartitionableBuffer]
      data_output,  # type: DataOutput
      input_timers,  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]
      expected_timer_output,  # type: Dict[translations.TimerFamilyId, bytes]
      bundle_manager  # type: BundleManager
  ) -> Tuple[beam_fn_api_pb2.InstructionResponse,
             Dict[str, execution.PartitionableBuffer],
             Dict[translations.TimerFamilyId, ListBuffer],
             Dict[Union[str, translations.TimerFamilyId], timestamp.Timestamp]]:
    """Execute a bundle, and return a result object, and deferred inputs."""
    self._run_bundle_multiple_times_for_testing(
        runner_execution_context,
        bundle_manager,
        data_input,
        data_output,
        input_timers,
        expected_timer_output)

    result, splits = bundle_manager.process_bundle(
        data_input, data_output, input_timers, expected_timer_output)
    # Now we collect all the deferred inputs remaining from bundle execution.
    # Deferred inputs can be:
    # - timers
    # - SDK-initiated deferred applications of root elements
    # - Runner-initiated deferred applications of root elements
    deferred_inputs = {}  # type: Dict[str, execution.PartitionableBuffer]

    watermarks_by_transform_and_timer_family, newly_set_timers = (
        self._collect_written_timers(bundle_context_manager))

    sdk_pcolls_with_da = self._add_sdk_delayed_applications_to_deferred_inputs(
        bundle_context_manager, result, deferred_inputs)

    runner_pcolls_with_da, transforms_with_channel_splits = (
        self._add_residuals_and_channel_splits_to_deferred_inputs(
            splits, bundle_context_manager, data_input, deferred_inputs))

    watermark_updates = self._build_watermark_updates(
        runner_execution_context,
        data_input.keys(),
        expected_timer_output.keys(),
        runner_pcolls_with_da.union(sdk_pcolls_with_da),
        transforms_with_channel_splits,
        watermarks_by_transform_and_timer_family)

    # After collecting deferred inputs, we 'pad' the structure with empty
    # buffers for other expected inputs.
    if deferred_inputs or newly_set_timers:
      # The worker will be waiting on these inputs as well.
      for other_input in data_input:
        if other_input not in deferred_inputs:
          deferred_inputs[other_input] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(
                  other_input))

    return result, deferred_inputs, newly_set_timers, watermark_updates

  @staticmethod
  def get_cache_token_generator(static=True):
    # type: (bool) -> Iterator[beam_fn_api_pb2.ProcessBundleRequest.CacheToken]

    """A generator for cache tokens.
         :arg static If True, generator always returns the same cache token
                     If False, generator returns a new cache token each time
         :return A generator which returns a cache token on next(generator)
     """
    def generate_token(identifier):
      # type: (int) -> beam_fn_api_pb2.ProcessBundleRequest.CacheToken
      return beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
          user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState(
          ),
          token="cache_token_{}".format(identifier).encode("utf-8"))

    class StaticGenerator(object):
      def __init__(self):
        # type: () -> None
        self._token = generate_token(1)

      def __iter__(self):
        # type: () -> StaticGenerator
        # pylint: disable=non-iterator-returned
        return self

      def __next__(self):
        # type: () -> beam_fn_api_pb2.ProcessBundleRequest.CacheToken
        return self._token

    class DynamicGenerator(object):
      def __init__(self):
        # type: () -> None
        self._counter = 0
        self._lock = threading.Lock()

      def __iter__(self):
        # type: () -> DynamicGenerator
        # pylint: disable=non-iterator-returned
        return self

      def __next__(self):
        # type: () -> beam_fn_api_pb2.ProcessBundleRequest.CacheToken
        with self._lock:
          self._counter += 1
          return generate_token(self._counter)

    if static:
      return StaticGenerator()
    else:
      return DynamicGenerator()


class ExtendedProvisionInfo(object):
  def __init__(self,
               provision_info=None,  # type: Optional[beam_provision_api_pb2.ProvisionInfo]
               artifact_staging_dir=None,  # type: Optional[str]
               job_name=None,  # type: Optional[str]
              ):
    # type: (...) -> None
    self.provision_info = (
        provision_info or beam_provision_api_pb2.ProvisionInfo())
    self.artifact_staging_dir = artifact_staging_dir
    self.job_name = job_name

  def for_environment(self, env):
    # type: (...) -> ExtendedProvisionInfo
    if env.dependencies:
      provision_info_with_deps = copy.deepcopy(self.provision_info)
      provision_info_with_deps.dependencies.extend(env.dependencies)
      return ExtendedProvisionInfo(
          provision_info_with_deps, self.artifact_staging_dir, self.job_name)
    else:
      return self


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
               bundle_context_manager,  # type: execution.BundleContextManager
               progress_frequency=None,  # type: Optional[float]
               cache_token_generator=FnApiRunner.get_cache_token_generator()
              ):
    # type: (...) -> None

    """Set up a bundle manager.

    Args:
      progress_frequency
    """
    self.bundle_context_manager = bundle_context_manager  # type: execution.BundleContextManager
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

  def _send_timers_to_worker(
      self, process_bundle_id, transform_id, timer_family_id, timers):
    # type: (...) -> None
    assert self._worker_handler is not None
    timer_out = self._worker_handler.data_conn.output_timer_stream(
        process_bundle_id, transform_id, timer_family_id)
    for timer in timers:
      timer_out.write(timer)
    timer_out.close()

  def _select_split_manager(self):
    """TODO(pabloem) WHAT DOES THIS DO"""
    unique_names = set(
        t.unique_name for t in self.bundle_context_manager.
        process_bundle_descriptor.transforms.values())
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
                                   inputs,  # type: Mapping[str, execution.PartitionableBuffer]
                                   process_bundle_id
                                  ):
    # type: (...) -> List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    read_transform_id, buffer_data = only_element(inputs.items())
    byte_stream = b''.join(buffer_data)
    num_elements = len(
        list(
            self.bundle_context_manager.get_input_coder_impl(
                read_transform_id).decode_all(byte_stream)))

    # Start the split manager in case it wants to set any breakpoints.
    split_manager_generator = split_manager(num_elements)
    try:
      split_fraction = next(split_manager_generator)
      done = False
    except StopIteration:
      split_fraction = None
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
          if ('Unknown process bundle' in split_response.error or
              split_response.process_bundle_split ==
              beam_fn_api_pb2.ProcessBundleSplitResponse()):
            time.sleep(t)
            split_response = self._worker_handler.control_conn.push(
                split_request).get()
        if ('Unknown process bundle' in split_response.error or
            split_response.process_bundle_split ==
            beam_fn_api_pb2.ProcessBundleSplitResponse()):
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
                     inputs,  # type: Mapping[str, execution.PartitionableBuffer]
                     expected_outputs,  # type: DataOutput
                     fired_timers,  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]
                     expected_output_timers,  # type: OutputTimers
                     dry_run=False,  # type: bool
                    ):
    # type: (...) -> BundleProcessResult
    # Unique id for the instruction processing this bundle.
    with BundleManager._lock:
      BundleManager._uid_counter += 1
      process_bundle_id = 'bundle_%s' % BundleManager._uid_counter
      self._worker_handler = self.bundle_context_manager.worker_handlers[
          BundleManager._uid_counter %
          len(self.bundle_context_manager.worker_handlers)]

    split_manager = self._select_split_manager()
    if not split_manager:
      # Send timers.
      for transform_id, timer_family_id in expected_output_timers.keys():
        self._send_timers_to_worker(
            process_bundle_id,
            transform_id,
            timer_family_id,
            fired_timers.get((transform_id, timer_family_id), []))

      # If there is no split_manager, write all input data to the channel.
      for transform_id, elements in inputs.items():
        self._send_input_to_worker(process_bundle_id, transform_id, elements)

    # Actually start the bundle.
    process_bundle_req = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=self.bundle_context_manager.
            process_bundle_descriptor.id,
            cache_tokens=[next(self._cache_token_generator)]))
    result_future = self._worker_handler.control_conn.push(process_bundle_req)

    split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
    with ProgressRequester(self._worker_handler,
                           process_bundle_id,
                           self._progress_frequency):

      if split_manager:
        split_results = self._generate_splits_for_testing(
            split_manager, inputs, process_bundle_id)

      expect_reads = list(
          expected_outputs.keys())  # type: List[Union[str, Tuple[str, str]]]
      expect_reads.extend(list(expected_output_timers.keys()))

      # Gather all output data.
      for output in self._worker_handler.data_conn.input_elements(
          process_bundle_id,
          expect_reads,
          abort_callback=lambda:
          (result_future.is_done() and bool(result_future.get().error))):
        if isinstance(output, beam_fn_api_pb2.Elements.Timers) and not dry_run:
          with BundleManager._lock:
            timer_buffer = self.bundle_context_manager.get_buffer(
                expected_output_timers[(
                    output.transform_id, output.timer_family_id)],
                output.transform_id)
            if timer_buffer.cleared:
              timer_buffer.reset()
            timer_buffer.append(output.timers)
        if isinstance(output, beam_fn_api_pb2.Elements.Data) and not dry_run:
          with BundleManager._lock:
            self.bundle_context_manager.get_buffer(
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
      finalize_response = self._worker_handler.control_conn.push(
          finalize_request).get()
      if finalize_response.error:
        raise RuntimeError(finalize_response.error)

    return result, split_results


class ParallelBundleManager(BundleManager):

  def __init__(
      self,
      bundle_context_manager,  # type: execution.BundleContextManager
      progress_frequency=None,  # type: Optional[float]
      cache_token_generator=None,
      **kwargs):
    # type: (...) -> None
    super().__init__(
        bundle_context_manager,
        progress_frequency,
        cache_token_generator=cache_token_generator)
    self._num_workers = bundle_context_manager.num_workers

  def process_bundle(self,
                     inputs,  # type: Mapping[str, execution.PartitionableBuffer]
                     expected_outputs,  # type: DataOutput
                     fired_timers,  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]
                     expected_output_timers,  # type: OutputTimers
                     dry_run=False,  # type: bool
                    ):
    # type: (...) -> BundleProcessResult
    part_inputs = [{} for _ in range(self._num_workers)
                   ]  # type: List[Dict[str, List[bytes]]]
    # Timers are only executed on the first worker
    # TODO(BEAM-9741): Split timers to multiple workers
    timer_inputs = [
        fired_timers if i == 0 else {} for i in range(self._num_workers)
    ]
    for name, input in inputs.items():
      for ix, part in enumerate(input.partition(self._num_workers)):
        part_inputs[ix][name] = part

    merged_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]
    split_result_list = [
    ]  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]

    def execute(part_map_input_timers):
      # type: (...) -> BundleProcessResult
      part_map, input_timers = part_map_input_timers
      bundle_manager = BundleManager(
          self.bundle_context_manager,
          self._progress_frequency,
          cache_token_generator=self._cache_token_generator)
      return bundle_manager.process_bundle(
          part_map,
          expected_outputs,
          input_timers,
          expected_output_timers,
          dry_run)

    with thread_pool_executor.shared_unbounded_instance() as executor:
      for result, split_result in executor.map(execute, zip(part_inputs,  # pylint: disable=bad-option-value
                                                            timer_inputs)):
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
    super().__init__()
    self._worker_handler = worker_handler
    self._instruction_id = instruction_id
    self._frequency = frequency
    self._done = False
    self._latest_progress = None  # type: Optional[beam_fn_api_pb2.ProcessBundleProgressResponse]
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
    super().__init__(state)
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
