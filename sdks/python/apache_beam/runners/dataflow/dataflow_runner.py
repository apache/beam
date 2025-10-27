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

"""A runner implementation that submits a job for remote execution.

The runner will create a JSON description of the job graph and then submit it
to the Dataflow Service for remote execution by a worker.
"""
# pytype: skip-file

import logging
import os
import threading
import time
import warnings
from collections import defaultdict
from subprocess import DEVNULL
from typing import TYPE_CHECKING
from typing import List

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.pipeline_utils import group_by_key_input_visitor
from apache_beam.runners.pipeline_utils import merge_common_environments
from apache_beam.runners.pipeline_utils import merge_superset_dep_environments
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.transforms import environments
from apache_beam.typehints import typehints
from apache_beam.utils import processes
from apache_beam.utils.interactive_utils import is_in_notebook
from apache_beam.utils.plugin import BeamPlugin

if TYPE_CHECKING:
  from apache_beam.pipeline import PTransformOverride

__all__ = ['DataflowRunner']

_LOGGER = logging.getLogger(__name__)

BQ_SOURCE_UW_ERROR = (
    'The Read(BigQuerySource(...)) transform is not supported with newer stack '
    'features (Fn API, Dataflow Runner V2, etc). Please use the transform '
    'apache_beam.io.gcp.bigquery.ReadFromBigQuery instead.')


class DataflowRunner(PipelineRunner):
  """A runner that creates job graphs and submits them for remote execution.

  Every execution of the run() method will submit an independent job for
  remote execution that consists of the nodes reachable from the passed-in
  node argument or entire graph if the node is None. The run() method returns
  after the service creates the job, and the job status is reported as RUNNING.
  """

  # A list of PTransformOverride objects to be applied before running a pipeline
  # using DataflowRunner.
  # Currently this only works for overrides where the input and output types do
  # not change.
  # For internal SDK use only. This should not be updated by Beam pipeline
  # authors.

  # Imported here to avoid circular dependencies.
  # TODO: Remove the apache_beam.pipeline dependency in CreatePTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import NativeReadPTransformOverride

  # These overrides should be applied before the proto representation of the
  # graph is created.
  _PTRANSFORM_OVERRIDES = [
      NativeReadPTransformOverride(),
  ]  # type: List[PTransformOverride]

  def __init__(self, cache=None):
    self._default_environment = None

  def default_pickle_library_override(self):
    return 'cloudpickle'

  def is_fnapi_compatible(self):
    return False

  @staticmethod
  def poll_for_job_completion(
      runner, result, duration, state_update_callback=None):
    """Polls for the specified job to finish running (successfully or not).

    Updates the result with the new job information before returning.

    Args:
      runner: DataflowRunner instance to use for polling job state.
      result: DataflowPipelineResult instance used for job information.
      duration (int): The time to wait (in milliseconds) for job to finish.
        If it is set to :data:`None`, it will wait indefinitely until the job
        is finished.
    """
    if result.state == PipelineState.DONE:
      return

    last_message_time = None
    current_seen_messages = set()

    last_error_rank = float('-inf')
    last_error_msg = None
    last_job_state = None
    # How long to wait after pipeline failure for the error
    # message to show up giving the reason for the failure.
    # It typically takes about 30 seconds.
    final_countdown_timer_secs = 50.0
    sleep_secs = 5.0

    # Try to prioritize the user-level traceback, if any.
    def rank_error(msg):
      if 'work item was attempted' in msg:
        return -1
      elif 'Traceback' in msg:
        return 1
      return 0

    if duration:
      start_secs = time.time()
      duration_secs = duration // 1000

    job_id = result.job_id()
    while True:
      response = runner.dataflow_client.get_job(job_id)
      # If get() is called very soon after Create() the response may not contain
      # an initialized 'currentState' field.
      if response.currentState is not None:
        if response.currentState != last_job_state:
          if state_update_callback:
            state_update_callback(response.currentState)
          _LOGGER.info('Job %s is in state %s', job_id, response.currentState)
          last_job_state = response.currentState
        if str(response.currentState) != 'JOB_STATE_RUNNING':
          # Stop checking for new messages on timeout, explanatory
          # message received, success, or a terminal job state caused
          # by the user that therefore doesn't require explanation.
          if (final_countdown_timer_secs <= 0.0 or last_error_msg is not None or
              str(response.currentState) == 'JOB_STATE_DONE' or
              str(response.currentState) == 'JOB_STATE_CANCELLED' or
              str(response.currentState) == 'JOB_STATE_UPDATED' or
              str(response.currentState) == 'JOB_STATE_DRAINED'):
            break

          # Check that job is in a post-preparation state before starting the
          # final countdown.
          if (str(response.currentState)
              not in ('JOB_STATE_PENDING', 'JOB_STATE_QUEUED')):
            # The job has failed; ensure we see any final error messages.
            sleep_secs = 1.0  # poll faster during the final countdown
            final_countdown_timer_secs -= sleep_secs

      time.sleep(sleep_secs)

      # Get all messages since beginning of the job run or since last message.
      page_token = None
      while True:
        messages, page_token = runner.dataflow_client.list_messages(
            job_id, page_token=page_token, start_time=last_message_time)
        for m in messages:
          message = '%s: %s: %s' % (m.time, m.messageImportance, m.messageText)

          if not last_message_time or m.time > last_message_time:
            last_message_time = m.time
            current_seen_messages = set()

          if message in current_seen_messages:
            # Skip the message if it has already been seen at the current
            # time. This could be the case since the list_messages API is
            # queried starting at last_message_time.
            continue
          else:
            current_seen_messages.add(message)
          # Skip empty messages.
          if m.messageImportance is None:
            continue
          message_importance = str(m.messageImportance)
          if (message_importance == 'JOB_MESSAGE_DEBUG' or
              message_importance == 'JOB_MESSAGE_DETAILED'):
            _LOGGER.debug(message)
          elif message_importance == 'JOB_MESSAGE_BASIC':
            _LOGGER.info(message)
          elif message_importance == 'JOB_MESSAGE_WARNING':
            _LOGGER.warning(message)
          elif message_importance == 'JOB_MESSAGE_ERROR':
            _LOGGER.error(message)
            if rank_error(m.messageText) >= last_error_rank:
              last_error_rank = rank_error(m.messageText)
              last_error_msg = m.messageText
          else:
            _LOGGER.info(message)
        if not page_token:
          break

      if duration:
        passed_secs = time.time() - start_secs
        if passed_secs > duration_secs:
          _LOGGER.warning(
              'Timing out on waiting for job %s after %d seconds',
              job_id,
              passed_secs)
          break

    result._job = response
    runner.last_error_msg = last_error_msg

  @staticmethod
  def _only_element(iterable):
    # type: (Iterable[T]) -> T # noqa: F821
    element, = iterable
    return element

  @staticmethod
  def side_input_visitor(deterministic_key_coders=True):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.pipeline import PipelineVisitor
    from apache_beam.transforms.core import ParDo

    class SideInputVisitor(PipelineVisitor):
      """Ensures input `PCollection` used as a side inputs has a `KV` type.

      TODO(BEAM-115): Once Python SDK is compatible with the new Runner API,
      we could directly replace the coder instead of mutating the element type.
      """
      def visit_transform(self, transform_node):
        if isinstance(transform_node.transform, ParDo):
          new_side_inputs = []
          for side_input in transform_node.side_inputs:
            access_pattern = side_input._side_input_data().access_pattern
            if access_pattern == common_urns.side_inputs.ITERABLE.urn:
              # TODO(https://github.com/apache/beam/issues/20043): Stop
              # patching up the access pattern to appease Dataflow when
              # using the UW and hardcode the output type to be Any since
              # the Dataflow JSON and pipeline proto can differ in coders
              # which leads to encoding/decoding issues within the runner.
              side_input.pvalue.element_type = typehints.Any
              new_side_input = _DataflowIterableSideInput(side_input)
            elif access_pattern == common_urns.side_inputs.MULTIMAP.urn:
              # Ensure the input coder is a KV coder and patch up the
              # access pattern to appease Dataflow.
              side_input.pvalue.element_type = typehints.coerce_to_kv_type(
                  side_input.pvalue.element_type, transform_node.full_label)
              side_input.pvalue.requires_deterministic_key_coder = (
                  deterministic_key_coders and transform_node.full_label)
              new_side_input = _DataflowMultimapSideInput(side_input)
            else:
              raise ValueError(
                  'Unsupported access pattern for %r: %r' %
                  (transform_node.full_label, access_pattern))
            new_side_inputs.append(new_side_input)
          transform_node.side_inputs = new_side_inputs
          transform_node.transform.side_inputs = new_side_inputs

    return SideInputVisitor()

  @staticmethod
  def flatten_input_visitor():
    # Imported here to avoid circular dependencies.
    from apache_beam.pipeline import PipelineVisitor

    class FlattenInputVisitor(PipelineVisitor):
      """A visitor that replaces the element type for input ``PCollections``s of
       a ``Flatten`` transform with that of the output ``PCollection``.
      """
      def visit_transform(self, transform_node):
        # Imported here to avoid circular dependencies.
        # pylint: disable=wrong-import-order, wrong-import-position
        from apache_beam import Flatten
        if isinstance(transform_node.transform, Flatten):
          output_pcoll = DataflowRunner._only_element(
              transform_node.outputs.values())
          for input_pcoll in transform_node.inputs:
            input_pcoll.element_type = output_pcoll.element_type

    return FlattenInputVisitor()

  @staticmethod
  def combinefn_visitor():
    # Imported here to avoid circular dependencies.
    from apache_beam.pipeline import PipelineVisitor
    from apache_beam import core

    class CombineFnVisitor(PipelineVisitor):
      """Checks if `CombineFn` has non-default setup or teardown methods.
      If yes, raises `ValueError`.
      """
      def visit_transform(self, applied_transform):
        transform = applied_transform.transform
        if isinstance(transform, core.ParDo) and isinstance(
            transform.fn, core.CombineValuesDoFn):
          if self._overrides_setup_or_teardown(transform.fn.combinefn):
            raise ValueError(
                'CombineFn.setup and CombineFn.teardown are '
                'not supported with non-portable Dataflow '
                'runner. Please use Dataflow Runner V2 instead.')

      @staticmethod
      def _overrides_setup_or_teardown(combinefn):
        # TODO(https://github.com/apache/beam/issues/18716): provide an
        # implementation for this method
        return False

    return CombineFnVisitor()

  def _adjust_pipeline_for_dataflow_v2(self, pipeline):
    # Dataflow runner requires a KV type for GBK inputs, hence we enforce that
    # here.
    pipeline.visit(
        group_by_key_input_visitor(
            not pipeline._options.view_as(
                TypeOptions).allow_non_deterministic_key_coders))

  def run_pipeline(self, pipeline, options, pipeline_proto=None):
    """Remotely executes entire pipeline or parts reachable from node."""
    if _is_runner_v2_disabled(options):
      raise ValueError(
          'Disabling Runner V2 no longer supported '
          'using Beam Python %s.' % beam.version.__version__)

    # Label goog-dataflow-notebook if job is started from notebook.
    if is_in_notebook():
      notebook_version = (
          'goog-dataflow-notebook=' +
          beam.version.__version__.replace('.', '_'))
      if options.view_as(GoogleCloudOptions).labels:
        options.view_as(GoogleCloudOptions).labels.append(notebook_version)
      else:
        options.view_as(GoogleCloudOptions).labels = [notebook_version]

    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.runners.dataflow.internal import apiclient
    except ImportError:
      raise ImportError(
          'Google Cloud Dataflow runner not available, '
          'please install apache_beam[gcp]')

    _check_and_add_missing_options(options)

    # Convert all side inputs into a form acceptable to Dataflow.
    if pipeline:
      pipeline.visit(self.combinefn_visitor())

      pipeline.visit(
          self.side_input_visitor(
              deterministic_key_coders=not options.view_as(
                  TypeOptions).allow_non_deterministic_key_coders))

      # Performing configured PTransform overrides. Note that this is currently
      # done before Runner API serialization, since the new proto needs to
      # contain any added PTransforms.
      pipeline.replace_all(DataflowRunner._PTRANSFORM_OVERRIDES)

      # Apply DataflowRunner-specific overrides (e.g., streaming PubSub
      # optimizations)
      from apache_beam.runners.dataflow.ptransform_overrides import (
          get_dataflow_transform_overrides)
      dataflow_overrides = get_dataflow_transform_overrides(options)
      if dataflow_overrides:
        pipeline.replace_all(dataflow_overrides)

      if options.view_as(DebugOptions).lookup_experiment('use_legacy_bq_sink'):
        warnings.warn(
            "Native sinks no longer implemented; "
            "ignoring use_legacy_bq_sink.")

    if pipeline_proto:
      self.proto_pipeline = pipeline_proto

    else:
      if options.view_as(SetupOptions).prebuild_sdk_container_engine:
        # if prebuild_sdk_container_engine is specified we will build a new sdk
        # container image with dependencies pre-installed and use that image,
        # instead of using the inferred default container image.
        self._default_environment = (
            environments.DockerEnvironment.from_options(options))
        options.view_as(WorkerOptions).sdk_container_image = (
            self._default_environment.container_image)
      else:
        artifacts = environments.python_sdk_dependencies(options)
        if artifacts:
          _LOGGER.info(
              "Pipeline has additional dependencies to be installed "
              "in SDK worker container, consider using the SDK "
              "container image pre-building workflow to avoid "
              "repetitive installations. Learn more on "
              "https://cloud.google.com/dataflow/docs/guides/"
              "using-custom-containers#prebuild")
        self._default_environment = (
            environments.DockerEnvironment.from_container_image(
                apiclient.get_container_image_from_options(options),
                artifacts=artifacts,
                resource_hints=environments.resource_hints_from_options(
                    options)))

      # This has to be performed before pipeline proto is constructed to make
      # sure that the changes are reflected in the portable job submission path.
      self._adjust_pipeline_for_dataflow_v2(pipeline)

      # Snapshot the pipeline in a portable proto.
      self.proto_pipeline, self.proto_context = pipeline.to_runner_api(
          return_context=True, default_environment=self._default_environment)

    if any(pcoll.is_bounded == beam_runner_api_pb2.IsBounded.UNBOUNDED
           for pcoll in self.proto_pipeline.components.pcollections.values()):
      if (not options.view_as(StandardOptions).streaming and
          not options.view_as(DebugOptions).lookup_experiment(
              'unsafely_attempt_to_process_unbounded_data_in_batch_mode')):
        _LOGGER.info(
            'Automatically inferring streaming mode '
            'due to unbounded PCollections.')
        options.view_as(StandardOptions).streaming = True

    if options.view_as(StandardOptions).streaming:
      _check_and_add_missing_streaming_options(options)

    # Dataflow can only handle Docker environments.
    for env_id, env in self.proto_pipeline.components.environments.items():
      self.proto_pipeline.components.environments[env_id].CopyFrom(
          environments.resolve_anyof_environment(
              env, common_urns.environments.DOCKER.urn))
    self.proto_pipeline = merge_common_environments(
        merge_superset_dep_environments(self.proto_pipeline))

    # Optimize the pipeline if it not streaming and the pre_optimize
    # experiment is set.
    if not options.view_as(StandardOptions).streaming:
      pre_optimize = options.view_as(DebugOptions).lookup_experiment(
          'pre_optimize', 'default').lower()
      from apache_beam.runners.portability.fn_api_runner import translations
      if pre_optimize == 'none':
        phases = []
      elif pre_optimize == 'default' or pre_optimize == 'all':
        phases = [translations.pack_combiners, translations.sort_stages]
      else:
        phases = []
        for phase_name in pre_optimize.split(','):
          # For now, these are all we allow.
          if phase_name in ('pack_combiners', ):
            phases.append(getattr(translations, phase_name))
          else:
            raise ValueError(
                'Unknown or inapplicable phase for pre_optimize: %s' %
                phase_name)
        phases.append(translations.sort_stages)

      if phases:
        self.proto_pipeline = translations.optimize_pipeline(
            self.proto_pipeline,
            phases=phases,
            known_runner_urns=frozenset(),
            partial=True)

    # Add setup_options for all the BeamPlugin imports
    setup_options = options.view_as(SetupOptions)
    plugins = BeamPlugin.get_all_plugin_paths()
    if setup_options.beam_plugins is not None:
      plugins = list(set(plugins + setup_options.beam_plugins))
    setup_options.beam_plugins = plugins

    # Elevate "min_cpu_platform" to pipeline option, but using the existing
    # experiment.
    debug_options = options.view_as(DebugOptions)
    worker_options = options.view_as(WorkerOptions)
    if worker_options.min_cpu_platform:
      debug_options.add_experiment(
          'min_cpu_platform=' + worker_options.min_cpu_platform)

    self.job = apiclient.Job(options, self.proto_pipeline)

    test_options = options.view_as(TestOptions)
    # If it is a dry run, return without submitting the job.
    if test_options.dry_run:
      result = PipelineResult(PipelineState.DONE)
      result.wait_until_finish = lambda duration=None: None
      result.job = self.job
      return result

    # Get a Dataflow API client and set its options
    self.dataflow_client = apiclient.DataflowApplicationClient(
        options, self.job.root_staging_location)

    # Create the job description and send a request to the service. The result
    # can be None if there is no need to send a request to the service (e.g.
    # template creation). If a request was sent and failed then the call will
    # raise an exception.
    result = DataflowPipelineResult(
        self.dataflow_client.create_job(self.job), self, options)

    # TODO(BEAM-4274): Circular import runners-metrics. Requires refactoring.
    from apache_beam.runners.dataflow.dataflow_metrics import DataflowMetrics
    self._metrics = DataflowMetrics(self.dataflow_client, result, self.job)
    result.metric_results = self._metrics
    return result

  @staticmethod
  def _get_coder(typehint, window_coder):
    """Returns a coder based on a typehint object."""
    if window_coder:
      return coders.WindowedValueCoder(
          coders.registry.get_coder(typehint), window_coder=window_coder)
    return coders.registry.get_coder(typehint)

  def _verify_gbk_coders(self, transform, pcoll):
    # Infer coder of parent.
    #
    # TODO(ccy): make Coder inference and checking less specialized and more
    # comprehensive.

    parent = pcoll.producer
    if parent:
      coder = parent.transform._infer_output_coder()  # pylint: disable=protected-access
    if not coder:
      coder = self._get_coder(pcoll.element_type or typehints.Any, None)
    if not coder.is_kv_coder():
      raise ValueError((
          'Coder for the GroupByKey operation "%s" is not a '
          'key-value coder: %s.') % (transform.label, coder))
    # TODO(robertwb): Update the coder itself if it changed.
    coders.registry.verify_deterministic(
        coder.key_coder(), 'GroupByKey operation "%s"' % transform.label)

  def get_default_gcp_region(self):
    """Get a default value for Google Cloud region according to
    https://cloud.google.com/compute/docs/gcloud-compute/#default-properties.
    If no default can be found, returns None.
    """
    environment_region = os.environ.get('CLOUDSDK_COMPUTE_REGION')
    if environment_region:
      _LOGGER.info(
          'Using default GCP region %s from $CLOUDSDK_COMPUTE_REGION',
          environment_region)
      return environment_region
    try:
      cmd = ['gcloud', 'config', 'get-value', 'compute/region']
      raw_output = processes.check_output(cmd, stderr=DEVNULL)
      formatted_output = raw_output.decode('utf-8').strip()
      if formatted_output:
        _LOGGER.info(
            'Using default GCP region %s from `%s`',
            formatted_output,
            ' '.join(cmd))
        return formatted_output
    except RuntimeError:
      pass
    return None


class _DataflowSideInput(beam.pvalue.AsSideInput):
  """Wraps a side input as a dataflow-compatible side input."""
  def _view_options(self):
    return {
        'data': self._data,
    }

  def _side_input_data(self):
    return self._data


def _add_runner_v2_missing_options(options):
  debug_options = options.view_as(DebugOptions)
  debug_options.add_experiment('beam_fn_api')
  debug_options.add_experiment('use_unified_worker')
  debug_options.add_experiment('use_runner_v2')
  debug_options.add_experiment('use_portable_job_submission')


def _check_and_add_missing_options(options):
  # Type: (PipelineOptions) -> None

  """Validates and adds missing pipeline options depending on options set.

  :param options: PipelineOptions for this pipeline.
  """
  debug_options = options.view_as(DebugOptions)
  dataflow_service_options = options.view_as(
      GoogleCloudOptions).dataflow_service_options or []

  # Add use_gbek to dataflow_service_options if gbek is set.
  if options.view_as(SetupOptions).gbek:
    if 'use_gbek' not in dataflow_service_options:
      dataflow_service_options.append('use_gbek')
  elif 'use_gbek' in dataflow_service_options:
    raise ValueError(
        'Do not set use_gbek directly, pass in the --gbek pipeline option '
        'with a valid secret instead.')

  _add_runner_v2_missing_options(options)

  # Ensure that prime is specified as an experiment if specified as a dataflow
  # service option
  if 'enable_prime' in dataflow_service_options:
    debug_options.add_experiment('enable_prime')
  elif debug_options.lookup_experiment('enable_prime'):
    dataflow_service_options.append('enable_prime')

  options.view_as(
      GoogleCloudOptions).dataflow_service_options = dataflow_service_options

  sdk_location = options.view_as(SetupOptions).sdk_location
  if 'dev' in beam.version.__version__ and sdk_location == 'default':
    raise ValueError(
        "You are submitting a pipeline with Apache Beam Python SDK "
        f"{beam.version.__version__}. "
        "When launching Dataflow jobs with an unreleased (dev) SDK, "
        "please provide an SDK distribution in the --sdk_location option "
        "to use a consistent SDK version at "
        "pipeline submission and runtime. To ignore this error and use "
        "an SDK preinstalled in the default Dataflow dev runtime environment "
        "or in a custom container image, use --sdk_location=container.")


def _check_and_add_missing_streaming_options(options):
  # Type: (PipelineOptions) -> None

  """Validates and adds missing pipeline options depending on options set.

  Must be called after it has been determined whether we're running in
  streaming mode.

  :param options: PipelineOptions for this pipeline.
  """
  # Streaming only supports using runner v2 (aka unified worker).
  # Runner v2 only supports using streaming engine (aka windmill service)
  if options.view_as(StandardOptions).streaming:
    debug_options = options.view_as(DebugOptions)
    debug_options.add_experiment('enable_streaming_engine')
    debug_options.add_experiment('enable_windmill_service')


def _is_runner_v2_disabled(options):
  # Type: (PipelineOptions) -> bool

  """Returns true if runner v2 is disabled."""
  debug_options = options.view_as(DebugOptions)
  return (
      debug_options.lookup_experiment('disable_runner_v2') or
      debug_options.lookup_experiment('disable_runner_v2_until_2023') or
      debug_options.lookup_experiment('disable_runner_v2_until_v2.50') or
      debug_options.lookup_experiment('disable_prime_runner_v2'))


class _DataflowIterableSideInput(_DataflowSideInput):
  """Wraps an iterable side input as dataflow-compatible side input."""
  def __init__(self, side_input):
    # pylint: disable=protected-access
    self.pvalue = side_input.pvalue
    side_input_data = side_input._side_input_data()
    assert (
        side_input_data.access_pattern == common_urns.side_inputs.ITERABLE.urn)
    self._data = beam.pvalue.SideInputData(
        common_urns.side_inputs.ITERABLE.urn,
        side_input_data.window_mapping_fn,
        side_input_data.view_fn)


class _DataflowMultimapSideInput(_DataflowSideInput):
  """Wraps a multimap side input as dataflow-compatible side input."""
  def __init__(self, side_input):
    # pylint: disable=protected-access
    self.pvalue = side_input.pvalue
    side_input_data = side_input._side_input_data()
    assert (
        side_input_data.access_pattern == common_urns.side_inputs.MULTIMAP.urn)
    self._data = beam.pvalue.SideInputData(
        common_urns.side_inputs.MULTIMAP.urn,
        side_input_data.window_mapping_fn,
        side_input_data.view_fn)


class DataflowPipelineResult(PipelineResult):
  """Represents the state of a pipeline run on the Dataflow service."""
  def __init__(self, job, runner, options=None):
    """Initialize a new DataflowPipelineResult instance.

    Args:
      job: Job message from the Dataflow API. Could be :data:`None` if a job
        request was not sent to Dataflow service (e.g. template jobs).
      runner: DataflowRunner instance.
    """
    self._job = job
    self._runner = runner
    self._options = options
    self.metric_results = None

  def _update_job(self):
    # We need the job id to be able to update job information. There is no need
    # to update the job if we are in a known terminal state.
    if self.has_job and not self.is_in_terminal_state():
      self._job = self._runner.dataflow_client.get_job(self.job_id())

  def job_id(self):
    return self._job.id

  def metrics(self):
    return self.metric_results

  def monitoring_infos(self):
    logging.warning('Monitoring infos not yet supported for Dataflow runner.')
    return []

  @property
  def has_job(self):
    return self._job is not None

  @staticmethod
  def api_jobstate_to_pipeline_state(api_jobstate):
    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    # Ordered by the enum values. Values that may be introduced in
    # future versions of Dataflow API are considered UNRECOGNIZED by this SDK.
    api_jobstate_map = defaultdict(
        lambda: PipelineState.UNRECOGNIZED,
        {
            values_enum.JOB_STATE_UNKNOWN: PipelineState.UNKNOWN,
            values_enum.JOB_STATE_STOPPED: PipelineState.STOPPED,
            values_enum.JOB_STATE_RUNNING: PipelineState.RUNNING,
            values_enum.JOB_STATE_DONE: PipelineState.DONE,
            values_enum.JOB_STATE_FAILED: PipelineState.FAILED,
            values_enum.JOB_STATE_CANCELLED: PipelineState.CANCELLED,
            values_enum.JOB_STATE_UPDATED: PipelineState.UPDATED,
            values_enum.JOB_STATE_DRAINING: PipelineState.DRAINING,
            values_enum.JOB_STATE_DRAINED: PipelineState.DRAINED,
            values_enum.JOB_STATE_PENDING: PipelineState.PENDING,
            values_enum.JOB_STATE_CANCELLING: PipelineState.CANCELLING,
            values_enum.JOB_STATE_RESOURCE_CLEANING_UP: PipelineState.
            RESOURCE_CLEANING_UP,
        })

    return (
        api_jobstate_map[api_jobstate]
        if api_jobstate else PipelineState.UNKNOWN)

  def _get_job_state(self):
    return self.api_jobstate_to_pipeline_state(self._job.currentState)

  @property
  def state(self):
    """Return the current state of the remote job.

    Returns:
      A PipelineState object.
    """
    if not self.has_job:
      # https://github.com/apache/beam/blob/8f71dc41b30a978095ca0e0699009e4f4445a618/sdks/python/apache_beam/runners/dataflow/dataflow_runner.py#L867-L870
      return PipelineState.DONE

    self._update_job()

    return self._get_job_state()

  def is_in_terminal_state(self):
    if not self.has_job:
      return True

    return PipelineState.is_terminal(self._get_job_state())

  def wait_until_finish(self, duration=None):
    if not self.is_in_terminal_state():
      if not self.has_job:
        raise IOError('Failed to get the Dataflow job id.')
      gcp_options = self._options.view_as(GoogleCloudOptions)
      consoleUrl = (
          "Console URL: https://console.cloud.google.com/"
          f"dataflow/jobs/{gcp_options.region}/{self.job_id()}"
          f"?project={gcp_options.project}")
      thread = threading.Thread(
          target=DataflowRunner.poll_for_job_completion,
          args=(self._runner, self, duration))

      # Mark the thread as a daemon thread so a keyboard interrupt on the main
      # thread will terminate everything. This is also the reason we will not
      # use thread.join() to wait for the polling thread.
      thread.daemon = True
      thread.start()
      while thread.is_alive():
        time.sleep(5.0)

      # TODO: Merge the termination code in poll_for_job_completion and
      # is_in_terminal_state.
      terminated = self.is_in_terminal_state()
      assert duration or terminated, (
          'Job did not reach to a terminal state after waiting indefinitely. '
          '{}'.format(consoleUrl))

      if terminated and self.state != PipelineState.DONE:
        # TODO(BEAM-1290): Consider converting this to an error log based on
        # theresolution of the issue.
        _LOGGER.error(consoleUrl)
        raise DataflowRuntimeException(
            'Dataflow pipeline failed. State: %s, Error:\n%s' %
            (self.state, getattr(self._runner, 'last_error_msg', None)),
            self)
    elif PipelineState.is_terminal(
        self.state) and self.state == PipelineState.FAILED and self._runner:
      raise DataflowRuntimeException(
          'Dataflow pipeline failed. State: %s, Error:\n%s' %
          (self.state, getattr(self._runner, 'last_error_msg', None)),
          self)

    return self.state

  def cancel(self):
    if not self.has_job:
      raise IOError('Failed to get the Dataflow job id.')

    self._update_job()

    if self.is_in_terminal_state():
      _LOGGER.warning(
          'Cancel failed because job %s is already terminated in state %s.',
          self.job_id(),
          self.state)
    else:
      if not self._runner.dataflow_client.modify_job_state(
          self.job_id(), 'JOB_STATE_CANCELLED'):
        cancel_failed_message = (
            'Failed to cancel job %s, please go to the Developers Console to '
            'cancel it manually.') % self.job_id()
        _LOGGER.error(cancel_failed_message)
        raise DataflowRuntimeException(cancel_failed_message, self)

    return self.state

  def __str__(self):
    return '<%s %s %s>' % (self.__class__.__name__, self.job_id(), self.state)

  def __repr__(self):
    return '<%s %s at %s>' % (self.__class__.__name__, self._job, hex(id(self)))


class DataflowRuntimeException(Exception):
  """Indicates an error has occurred in running this pipeline."""
  def __init__(self, msg, result):
    super().__init__(msg)
    self.result = result
