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

import base64
import logging
import os
import threading
import time
import traceback
import warnings
from collections import defaultdict
from subprocess import DEVNULL
from typing import TYPE_CHECKING
from typing import List
from urllib.parse import quote
from urllib.parse import quote_from_bytes
from urllib.parse import unquote_to_bytes

import apache_beam as beam
from apache_beam import coders
from apache_beam import error
from apache_beam.internal import pickler
from apache_beam.internal.gcp import json_value
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.pvalue import AsSideInput
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import group_by_key_input_visitor
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.dataflow.internal.names import TransformNames
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PValueCache
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.sideinputs import SIDE_INPUT_PREFIX
from apache_beam.typehints import typehints
from apache_beam.utils import processes
from apache_beam.utils import proto_utils
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
  remote execution that consists of the nodes reachable from the passed in
  node argument or entire graph if node is None. The run() method returns
  after the service created the job and  will not wait for the job to finish
  if blocking is set to False.
  """

  # A list of PTransformOverride objects to be applied before running a pipeline
  # using DataflowRunner.
  # Currently this only works for overrides where the input and output types do
  # not change.
  # For internal SDK use only. This should not be updated by Beam pipeline
  # authors.

  # Imported here to avoid circular dependencies.
  # TODO: Remove the apache_beam.pipeline dependency in CreatePTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import CombineValuesPTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import CreatePTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import ReadPTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import NativeReadPTransformOverride

  # These overrides should be applied before the proto representation of the
  # graph is created.
  _PTRANSFORM_OVERRIDES = [
      NativeReadPTransformOverride(),
  ]  # type: List[PTransformOverride]

  # These overrides should be applied after the proto representation of the
  # graph is created.
  _NON_PORTABLE_PTRANSFORM_OVERRIDES = [
      CombineValuesPTransformOverride(),
      CreatePTransformOverride(),
      ReadPTransformOverride(),
  ]  # type: List[PTransformOverride]

  def __init__(self, cache=None):
    # Cache of CloudWorkflowStep protos generated while the runner
    # "executes" a pipeline.
    self._cache = cache if cache is not None else PValueCache()
    self._unique_step_id = 0
    self._default_environment = None

  def is_fnapi_compatible(self):
    return False

  def apply(self, transform, input, options):
    _check_and_add_missing_options(options)
    return super().apply(transform, input, options)

  def _get_unique_step_name(self):
    self._unique_step_id += 1
    return 's%s' % self._unique_step_id

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
          if (str(response.currentState) not in ('JOB_STATE_PENDING',
                                                 'JOB_STATE_QUEUED')):
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
          _LOGGER.info(message)
          if str(m.messageImportance) == 'JOB_MESSAGE_ERROR':
            if rank_error(m.messageText) >= last_error_rank:
              last_error_rank = rank_error(m.messageText)
              last_error_msg = m.messageText
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
  def side_input_visitor(is_runner_v2=False, deterministic_key_coders=True):
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
          if is_runner_v2:
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

  def _check_for_unsupported_features_on_non_portable_worker(self, pipeline):
    pipeline.visit(self.combinefn_visitor())

  def run_pipeline(self, pipeline, options, pipeline_proto=None):
    """Remotely executes entire pipeline or parts reachable from node."""
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

    if pipeline_proto or pipeline.contains_external_transforms:
      if _is_runner_v2_disabled(options):
        raise ValueError(
            'This pipeline contains cross language transforms, '
            'which requires Runner V2.')
      if not _is_runner_v2(options):
        _LOGGER.info(
            'Automatically enabling Dataflow Runner V2 since the '
            'pipeline used cross-language transforms.')
        _add_runner_v2_missing_options(options)

    is_runner_v2 = _is_runner_v2(options)
    if not is_runner_v2:
      self._check_for_unsupported_features_on_non_portable_worker(pipeline)

    # Convert all side inputs into a form acceptable to Dataflow.
    if pipeline:
      pipeline.visit(
          self.side_input_visitor(
              _is_runner_v2(options),
              deterministic_key_coders=not options.view_as(
                  TypeOptions).allow_non_deterministic_key_coders))

      # Performing configured PTransform overrides. Note that this is currently
      # done before Runner API serialization, since the new proto needs to
      # contain any added PTransforms.
      pipeline.replace_all(DataflowRunner._PTRANSFORM_OVERRIDES)

      if options.view_as(DebugOptions).lookup_experiment('use_legacy_bq_sink'):
        warnings.warn(
            "Native sinks no longer implemented; "
            "ignoring use_legacy_bq_sink.")

      from apache_beam.runners.dataflow.ptransform_overrides import GroupIntoBatchesWithShardedKeyPTransformOverride
      pipeline.replace_all(
          [GroupIntoBatchesWithShardedKeyPTransformOverride(self, options)])

    if pipeline_proto:
      self.proto_pipeline = pipeline_proto

    else:
      from apache_beam.transforms import environments
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
        if artifacts and _is_runner_v2(options):
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

    if not is_runner_v2:
      # Performing configured PTransform overrides which should not be reflected
      # in the proto representation of the graph.
      pipeline.replace_all(DataflowRunner._NON_PORTABLE_PTRANSFORM_OVERRIDES)

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

    # TODO: Consider skipping these for all use_portable_job_submission jobs.
    if pipeline:
      # Dataflow Runner v1 requires output type of the Flatten to be the same as
      # the inputs, hence we enforce that here. Dataflow Runner v2 does not
      # require this.
      pipeline.visit(self.flatten_input_visitor())

      # Trigger a traversal of all reachable nodes.
      self.visit_transforms(pipeline, options)

    test_options = options.view_as(TestOptions)
    # If it is a dry run, return without submitting the job.
    if test_options.dry_run:
      result = PipelineResult(PipelineState.DONE)
      result.wait_until_finish = lambda duration=None: None
      return result

    # Get a Dataflow API client and set its options
    self.dataflow_client = apiclient.DataflowApplicationClient(
        options, self.job.root_staging_location)

    # Create the job description and send a request to the service. The result
    # can be None if there is no need to send a request to the service (e.g.
    # template creation). If a request was sent and failed then the call will
    # raise an exception.
    result = DataflowPipelineResult(
        self.dataflow_client.create_job(self.job), self)

    # TODO(BEAM-4274): Circular import runners-metrics. Requires refactoring.
    from apache_beam.runners.dataflow.dataflow_metrics import DataflowMetrics
    self._metrics = DataflowMetrics(self.dataflow_client, result, self.job)
    result.metric_results = self._metrics
    return result

  def _get_typehint_based_encoding(self, typehint, window_coder):
    """Returns an encoding based on a typehint object."""
    return self._get_cloud_encoding(
        self._get_coder(typehint, window_coder=window_coder))

  @staticmethod
  def _get_coder(typehint, window_coder):
    """Returns a coder based on a typehint object."""
    if window_coder:
      return coders.WindowedValueCoder(
          coders.registry.get_coder(typehint), window_coder=window_coder)
    return coders.registry.get_coder(typehint)

  def _get_cloud_encoding(self, coder, unused=None):
    """Returns an encoding based on a coder object."""
    if not isinstance(coder, coders.Coder):
      raise TypeError(
          'Coder object must inherit from coders.Coder: %s.' % str(coder))
    return coder.as_cloud_object(self.proto_context.coders)

  def _get_side_input_encoding(self, input_encoding):
    """Returns an encoding for the output of a view transform.

    Args:
      input_encoding: encoding of current transform's input. Side inputs need
        this because the service will check that input and output types match.

    Returns:
      An encoding that matches the output and input encoding. This is essential
      for the View transforms introduced to produce side inputs to a ParDo.
    """
    return {
        '@type': 'kind:stream',
        'component_encodings': [input_encoding],
        'is_stream_like': {
            'value': True
        },
    }

  def _get_encoded_output_coder(
      self, transform_node, window_value=True, output_tag=None):
    """Returns the cloud encoding of the coder for the output of a transform."""

    if output_tag in transform_node.outputs:
      element_type = transform_node.outputs[output_tag].element_type
    elif len(transform_node.outputs) == 1:
      output_tag = DataflowRunner._only_element(transform_node.outputs.keys())
      # TODO(robertwb): Handle type hints for multi-output transforms.
      element_type = transform_node.outputs[output_tag].element_type

    else:
      # TODO(silviuc): Remove this branch (and assert) when typehints are
      # propagated everywhere. Returning an 'Any' as type hint will trigger
      # usage of the fallback coder (i.e., cPickler).
      element_type = typehints.Any
    if window_value:
      # All outputs have the same windowing. So getting the coder from an
      # arbitrary window is fine.
      output_tag = next(iter(transform_node.outputs.keys()))
      window_coder = (
          transform_node.outputs[output_tag].windowing.windowfn.
          get_window_coder())
    else:
      window_coder = None
    return self._get_typehint_based_encoding(element_type, window_coder)

  def get_pcoll_with_auto_sharding(self):
    if not hasattr(self, '_pcoll_with_auto_sharding'):
      return set()
    return self._pcoll_with_auto_sharding

  def add_pcoll_with_auto_sharding(self, applied_ptransform):
    if not hasattr(self, '_pcoll_with_auto_sharding'):
      self.__setattr__('_pcoll_with_auto_sharding', set())
    output = DataflowRunner._only_element(applied_ptransform.outputs.keys())
    self._pcoll_with_auto_sharding.add(
        applied_ptransform.outputs[output]._unique_name())

  def _add_step(self, step_kind, step_label, transform_node, side_tags=()):
    """Creates a Step object and adds it to the cache."""
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.internal import apiclient
    step = apiclient.Step(step_kind, self._get_unique_step_name())
    self.job.proto.steps.append(step.proto)
    step.add_property(PropertyNames.USER_NAME, step_label)
    # Cache the node/step association for the main output of the transform node.

    # External transforms may not use 'None' as an output tag.
    output_tags = ([None] +
                   list(side_tags) if None in transform_node.outputs.keys() else
                   list(transform_node.outputs.keys()))

    # We have to cache output for all tags since some transforms may produce
    # multiple outputs.
    for output_tag in output_tags:
      self._cache.cache_output(transform_node, output_tag, step)

    # Finally, we add the display data items to the pipeline step.
    # If the transform contains no display data then an empty list is added.
    step.add_property(
        PropertyNames.DISPLAY_DATA,
        [
            item.get_dict()
            for item in DisplayData.create_from(transform_node.transform).items
        ])

    if transform_node.resource_hints:
      step.add_property(
          PropertyNames.RESOURCE_HINTS,
          {
              hint: quote_from_bytes(value)
              for (hint, value) in transform_node.resource_hints.items()
          })

    return step

  def _add_singleton_step(
      self,
      label,
      full_label,
      tag,
      input_step,
      windowing_strategy,
      access_pattern):
    """Creates a CollectionToSingleton step used to handle ParDo side inputs."""
    # Import here to avoid adding the dependency for local running scenarios.
    from apache_beam.runners.dataflow.internal import apiclient
    step = apiclient.Step(TransformNames.COLLECTION_TO_SINGLETON, label)
    self.job.proto.steps.append(step.proto)
    step.add_property(PropertyNames.USER_NAME, full_label)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {
            '@type': 'OutputReference',
            PropertyNames.STEP_NAME: input_step.proto.name,
            PropertyNames.OUTPUT_NAME: input_step.get_output(tag)
        })
    step.encoding = self._get_side_input_encoding(input_step.encoding)

    output_info = {
        PropertyNames.USER_NAME: '%s.%s' % (full_label, PropertyNames.OUTPUT),
        PropertyNames.ENCODING: step.encoding,
        PropertyNames.OUTPUT_NAME: PropertyNames.OUT
    }
    if common_urns.side_inputs.MULTIMAP.urn == access_pattern:
      output_info[PropertyNames.USE_INDEXED_FORMAT] = True
    step.add_property(PropertyNames.OUTPUT_INFO, [output_info])

    step.add_property(
        PropertyNames.WINDOWING_STRATEGY,
        self.serialize_windowing_strategy(
            windowing_strategy, self._default_environment))
    return step

  def run_Impulse(self, transform_node, options):
    step = self._add_step(
        TransformNames.READ, transform_node.full_label, transform_node)
    step.add_property(PropertyNames.FORMAT, 'impulse')
    encoded_impulse_element = coders.WindowedValueCoder(
        coders.BytesCoder(),
        coders.coders.GlobalWindowCoder()).get_impl().encode_nested(
            window.GlobalWindows.windowed_value(b''))
    if _is_runner_v2(options):
      encoded_impulse_as_str = self.byte_array_to_json_string(
          encoded_impulse_element)
    else:
      encoded_impulse_as_str = base64.b64encode(encoded_impulse_element).decode(
          'ascii')

    step.add_property(PropertyNames.IMPULSE_ELEMENT, encoded_impulse_as_str)

    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{
            PropertyNames.USER_NAME: (
                '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT
        }])

  def run_Flatten(self, transform_node, options):
    step = self._add_step(
        TransformNames.FLATTEN, transform_node.full_label, transform_node)
    inputs = []
    for one_input in transform_node.inputs:
      input_step = self._cache.get_pvalue(one_input)
      inputs.append({
          '@type': 'OutputReference',
          PropertyNames.STEP_NAME: input_step.proto.name,
          PropertyNames.OUTPUT_NAME: input_step.get_output(one_input.tag)
      })
    step.add_property(PropertyNames.INPUTS, inputs)
    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{
            PropertyNames.USER_NAME: (
                '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT
        }])

  # TODO(srohde): Remove this after internal usages have been removed.
  def apply_GroupByKey(self, transform, pcoll, options):
    return transform.expand(pcoll)

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

  def run_GroupByKey(self, transform_node, options):
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])

    # Verify that the GBK's parent has a KV coder.
    self._verify_gbk_coders(transform_node.transform, transform_node.inputs[0])

    step = self._add_step(
        TransformNames.GROUP, transform_node.full_label, transform_node)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {
            '@type': 'OutputReference',
            PropertyNames.STEP_NAME: input_step.proto.name,
            PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)
        })
    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{
            PropertyNames.USER_NAME: (
                '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT
        }])
    windowing = transform_node.transform.get_windowing(transform_node.inputs)
    step.add_property(
        PropertyNames.SERIALIZED_FN,
        self.serialize_windowing_strategy(windowing, self._default_environment))

  def run_ExternalTransform(self, transform_node, options):
    # Adds a dummy step to the Dataflow job description so that inputs and
    # outputs are mapped correctly in the presence of external transforms.
    #
    # Note that Dataflow Python multi-language pipelines use Portable Job
    # Submission by default, hence this step and rest of the Dataflow step
    # definitions defined here are not used at Dataflow service but we have to
    # maintain the mapping correctly till we can fully drop the Dataflow step
    # definitions from the SDK.

    # AppliedTransform node outputs have to be updated to correctly map the
    # outputs for external transforms.
    transform_node.outputs = ({
        output.tag: output
        for output in transform_node.outputs.values()
    })

    self.run_Impulse(transform_node, options)

  def run_ParDo(self, transform_node, options):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])

    # Attach side inputs.
    si_dict = {}
    si_labels = {}
    full_label_counts = defaultdict(int)
    lookup_label = lambda side_pval: si_labels[side_pval]
    named_inputs = transform_node.named_inputs()
    label_renames = {}
    for ix, side_pval in enumerate(transform_node.side_inputs):
      assert isinstance(side_pval, AsSideInput)
      step_name = 'SideInput-' + self._get_unique_step_name()
      si_label = ((SIDE_INPUT_PREFIX + '%d-%s') %
                  (ix, transform_node.full_label))
      old_label = (SIDE_INPUT_PREFIX + '%d') % ix

      label_renames[old_label] = si_label

      assert old_label in named_inputs
      pcollection_label = '%s.%s' % (
          side_pval.pvalue.producer.full_label.split('/')[-1],
          side_pval.pvalue.tag if side_pval.pvalue.tag else 'out')
      si_full_label = '%s/%s(%s.%s)' % (
          transform_node.full_label,
          side_pval.__class__.__name__,
          pcollection_label,
          full_label_counts[pcollection_label])

      # Count the number of times the same PCollection is a side input
      # to the same ParDo.
      full_label_counts[pcollection_label] += 1

      self._add_singleton_step(
          step_name,
          si_full_label,
          side_pval.pvalue.tag,
          self._cache.get_pvalue(side_pval.pvalue),
          side_pval.pvalue.windowing,
          side_pval._side_input_data().access_pattern)
      si_dict[si_label] = {
          '@type': 'OutputReference',
          PropertyNames.STEP_NAME: step_name,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT
      }
      si_labels[side_pval] = si_label

    # Now create the step for the ParDo transform being handled.
    transform_name = transform_node.full_label.rsplit('/', 1)[-1]
    step = self._add_step(
        TransformNames.DO,
        transform_node.full_label +
        ('/{}'.format(transform_name) if transform_node.side_inputs else ''),
        transform_node,
        transform_node.transform.output_tags)
    transform_proto = self.proto_context.transforms.get_proto(transform_node)
    transform_id = self.proto_context.transforms.get_id(transform_node)
    is_runner_v2 = _is_runner_v2(options)
    # Patch side input ids to be unique across a given pipeline.
    if (label_renames and
        transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn):
      # Patch PTransform proto.
      for old, new in label_renames.items():
        transform_proto.inputs[new] = transform_proto.inputs[old]
        del transform_proto.inputs[old]

      # Patch ParDo proto.
      proto_type, _ = beam.PTransform._known_urns[transform_proto.spec.urn]
      proto = proto_utils.parse_Bytes(transform_proto.spec.payload, proto_type)
      for old, new in label_renames.items():
        proto.side_inputs[new].CopyFrom(proto.side_inputs[old])
        del proto.side_inputs[old]
      transform_proto.spec.payload = proto.SerializeToString()
      # We need to update the pipeline proto.
      del self.proto_pipeline.components.transforms[transform_id]
      (
          self.proto_pipeline.components.transforms[transform_id].CopyFrom(
              transform_proto))
    # The data transmitted in SERIALIZED_FN is different depending on whether
    # this is a runner v2 pipeline or not.
    if is_runner_v2:
      serialized_data = transform_id
    else:
      serialized_data = pickler.dumps(
          self._pardo_fn_data(transform_node, lookup_label))
    step.add_property(PropertyNames.SERIALIZED_FN, serialized_data)
    # TODO(BEAM-8882): Enable once dataflow service doesn't reject this.
    # step.add_property(PropertyNames.PIPELINE_PROTO_TRANSFORM_ID, transform_id)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {
            '@type': 'OutputReference',
            PropertyNames.STEP_NAME: input_step.proto.name,
            PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)
        })
    # Add side inputs if any.
    step.add_property(PropertyNames.NON_PARALLEL_INPUTS, si_dict)

    # Generate description for the outputs. The output names
    # will be 'None' for main output and '<tag>' for a tagged output.
    outputs = []

    all_output_tags = list(transform_proto.outputs.keys())

    # Some external transforms require output tags to not be modified.
    # So we randomly select one of the output tags as the main output and
    # leave others as side outputs. Transform execution should not change
    # dependending on which output tag we choose as the main output here.
    # Also, some SDKs do not work correctly if output tags are modified. So for
    # external transforms, we leave tags unmodified.
    #
    # Python SDK uses 'None' as the tag of the main output.
    main_output_tag = 'None'

    step.encoding = self._get_encoded_output_coder(
        transform_node, output_tag=main_output_tag)

    side_output_tags = set(all_output_tags).difference({main_output_tag})

    # Add the main output to the description.
    outputs.append({
        PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
        PropertyNames.ENCODING: step.encoding,
        PropertyNames.OUTPUT_NAME: main_output_tag
    })
    for side_tag in side_output_tags:
      # The assumption here is that all outputs will have the same typehint
      # and coder as the main output. This is certainly the case right now
      # but conceivably it could change in the future.
      encoding = self._get_encoded_output_coder(
          transform_node, output_tag=side_tag)
      outputs.append({
          PropertyNames.USER_NAME: (
              '%s.%s' % (transform_node.full_label, side_tag)),
          PropertyNames.ENCODING: encoding,
          PropertyNames.OUTPUT_NAME: side_tag
      })

    step.add_property(PropertyNames.OUTPUT_INFO, outputs)

    # Add the restriction encoding if we are a splittable DoFn
    restriction_coder = transform.get_restriction_coder()
    if restriction_coder:
      step.add_property(
          PropertyNames.RESTRICTION_ENCODING,
          self._get_cloud_encoding(restriction_coder))

    if options.view_as(StandardOptions).streaming:
      is_stateful_dofn = (DoFnSignature(transform.dofn).is_stateful_dofn())
      if is_stateful_dofn:
        step.add_property(PropertyNames.USES_KEYED_STATE, 'true')

        # Also checks whether the step allows shardable keyed states.
        # TODO(BEAM-11360): remove this when migrated to portable job
        #  submission since we only consider supporting the property in runner
        #  v2.
        for pcoll in transform_node.outputs.values():
          if pcoll._unique_name() in self.get_pcoll_with_auto_sharding():
            step.add_property(PropertyNames.ALLOWS_SHARDABLE_STATE, 'true')
            # Currently we only allow auto-sharding to be enabled through the
            # GroupIntoBatches transform. So we also add the following property
            # which GroupIntoBatchesDoFn has, to allow the backend to perform
            # graph optimization.
            step.add_property(PropertyNames.PRESERVES_KEYS, 'true')
            break

  @staticmethod
  def _pardo_fn_data(transform_node, get_label):
    transform = transform_node.transform
    si_tags_and_types = [  # pylint: disable=protected-access
        (get_label(side_pval), side_pval.__class__, side_pval._view_options())
        for side_pval in transform_node.side_inputs]
    return (
        transform.fn,
        transform.args,
        transform.kwargs,
        si_tags_and_types,
        transform_node.inputs[0].windowing)

  def run_CombineValuesReplacement(self, transform_node, options):
    transform = transform_node.transform.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step = self._add_step(
        TransformNames.COMBINE, transform_node.full_label, transform_node)
    transform_id = self.proto_context.transforms.get_id(transform_node.parent)

    # The data transmitted in SERIALIZED_FN is different depending on whether
    # this is a runner v2 pipeline or not.
    if _is_runner_v2(options):
      # Fnapi pipelines send the transform ID of the CombineValues transform's
      # parent composite because Dataflow expects the ID of a CombinePerKey
      # transform.
      serialized_data = transform_id
    else:
      # Combiner functions do not take deferred side-inputs (i.e. PValues) and
      # therefore the code to handle extra args/kwargs is simpler than for the
      # DoFn's of the ParDo transform. In the last, empty argument is where
      # side inputs information would go.
      serialized_data = pickler.dumps(
          (transform.fn, transform.args, transform.kwargs, ()))
    step.add_property(PropertyNames.SERIALIZED_FN, serialized_data)
    # TODO(BEAM-8882): Enable once dataflow service doesn't reject this.
    # step.add_property(PropertyNames.PIPELINE_PROTO_TRANSFORM_ID, transform_id)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {
            '@type': 'OutputReference',
            PropertyNames.STEP_NAME: input_step.proto.name,
            PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)
        })
    # Note that the accumulator must not have a WindowedValue encoding, while
    # the output of this step does in fact have a WindowedValue encoding.
    accumulator_encoding = self._get_cloud_encoding(
        transform.fn.get_accumulator_coder())
    output_encoding = self._get_encoded_output_coder(transform_node)

    step.encoding = output_encoding
    step.add_property(PropertyNames.ENCODING, accumulator_encoding)
    # Generate description for main output 'out.'
    outputs = []
    # Add the main output to the description.
    outputs.append({
        PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
        PropertyNames.ENCODING: step.encoding,
        PropertyNames.OUTPUT_NAME: PropertyNames.OUT
    })
    step.add_property(PropertyNames.OUTPUT_INFO, outputs)

  def run_Read(self, transform_node, options):
    transform = transform_node.transform
    step = self._add_step(
        TransformNames.READ, transform_node.full_label, transform_node)
    # TODO(mairbek): refactor if-else tree to use registerable functions.
    # Initialize the source specific properties.

    standard_options = options.view_as(StandardOptions)
    if not hasattr(transform.source, 'format'):
      # If a format is not set, we assume the source to be a custom source.
      source_dict = {}

      source_dict['spec'] = {
          '@type': names.SOURCE_TYPE,
          names.SERIALIZED_SOURCE_KEY: pickler.dumps(transform.source)
      }

      try:
        source_dict['metadata'] = {
            'estimated_size_bytes': json_value.get_typed_value_descriptor(
                transform.source.estimate_size())
        }
      except error.RuntimeValueProviderError:
        # Size estimation is best effort, and this error is by value provider.
        _LOGGER.info(
            'Could not estimate size of source %r due to ' + \
            'RuntimeValueProviderError', transform.source)
      except Exception:  # pylint: disable=broad-except
        # Size estimation is best effort. So we log the error and continue.
        _LOGGER.info(
            'Could not estimate size of source %r due to an exception: %s',
            transform.source,
            traceback.format_exc())

      step.add_property(PropertyNames.SOURCE_STEP_INPUT, source_dict)
    elif transform.source.format == 'pubsub':
      if not standard_options.streaming:
        raise ValueError(
            'Cloud Pub/Sub is currently available for use '
            'only in streaming pipelines.')
      # Only one of topic or subscription should be set.
      if transform.source.full_subscription:
        step.add_property(
            PropertyNames.PUBSUB_SUBSCRIPTION,
            transform.source.full_subscription)
      elif transform.source.full_topic:
        step.add_property(
            PropertyNames.PUBSUB_TOPIC, transform.source.full_topic)
      if transform.source.id_label:
        step.add_property(
            PropertyNames.PUBSUB_ID_LABEL, transform.source.id_label)
      if transform.source.with_attributes:
        # Setting this property signals Dataflow runner to return full
        # PubsubMessages instead of just the data part of the payload.
        step.add_property(PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, '')

      if transform.source.timestamp_attribute is not None:
        step.add_property(
            PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE,
            transform.source.timestamp_attribute)
    else:
      raise ValueError(
          'Source %r has unexpected format %s.' %
          (transform.source, transform.source.format))

    if not hasattr(transform.source, 'format'):
      step.add_property(PropertyNames.FORMAT, names.SOURCE_FORMAT)
    else:
      step.add_property(PropertyNames.FORMAT, transform.source.format)

    # Wrap coder in WindowedValueCoder: this is necessary as the encoding of a
    # step should be the type of value outputted by each step.  Read steps
    # automatically wrap output values in a WindowedValue wrapper, if necessary.
    # This is also necessary for proper encoding for size estimation.
    # Using a GlobalWindowCoder as a place holder instead of the default
    # PickleCoder because GlobalWindowCoder is known coder.
    # TODO(robertwb): Query the collection for the windowfn to extract the
    # correct coder.
    coder = coders.WindowedValueCoder(
        coders.registry.get_coder(transform_node.outputs[None].element_type),
        coders.coders.GlobalWindowCoder())

    step.encoding = self._get_cloud_encoding(coder)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{
            PropertyNames.USER_NAME: (
                '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT
        }])

  def run__NativeWrite(self, transform_node, options):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step = self._add_step(
        TransformNames.WRITE, transform_node.full_label, transform_node)
    # TODO(mairbek): refactor if-else tree to use registerable functions.
    # Initialize the sink specific properties.
    if transform.sink.format == 'pubsub':
      standard_options = options.view_as(StandardOptions)
      if not standard_options.streaming:
        raise ValueError(
            'Cloud Pub/Sub is currently available for use '
            'only in streaming pipelines.')
      step.add_property(PropertyNames.PUBSUB_TOPIC, transform.sink.full_topic)
      if transform.sink.id_label:
        step.add_property(
            PropertyNames.PUBSUB_ID_LABEL, transform.sink.id_label)
      # Setting this property signals Dataflow runner that the PCollection
      # contains PubsubMessage objects instead of just raw data.
      step.add_property(PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, '')
      if transform.sink.timestamp_attribute is not None:
        step.add_property(
            PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE,
            transform.sink.timestamp_attribute)
    else:
      raise ValueError(
          'Sink %r has unexpected format %s.' %
          (transform.sink, transform.sink.format))
    step.add_property(PropertyNames.FORMAT, transform.sink.format)

    # Wrap coder in WindowedValueCoder: this is necessary for proper encoding
    # for size estimation. Using a GlobalWindowCoder as a place holder instead
    # of the default PickleCoder because GlobalWindowCoder is known coder.
    # TODO(robertwb): Query the collection for the windowfn to extract the
    # correct coder.
    coder = coders.WindowedValueCoder(
        transform.sink.coder, coders.coders.GlobalWindowCoder())
    step.encoding = self._get_cloud_encoding(coder)
    step.add_property(PropertyNames.ENCODING, step.encoding)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {
            '@type': 'OutputReference',
            PropertyNames.STEP_NAME: input_step.proto.name,
            PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)
        })

  def run_TestStream(self, transform_node, options):
    from apache_beam.testing.test_stream import ElementEvent
    from apache_beam.testing.test_stream import ProcessingTimeEvent
    from apache_beam.testing.test_stream import WatermarkEvent
    standard_options = options.view_as(StandardOptions)
    if not standard_options.streaming:
      raise ValueError(
          'TestStream is currently available for use '
          'only in streaming pipelines.')

    transform = transform_node.transform
    step = self._add_step(
        TransformNames.READ, transform_node.full_label, transform_node)
    step.add_property(
        PropertyNames.SERIALIZED_FN,
        self.proto_context.transforms.get_id(transform_node))
    step.add_property(PropertyNames.FORMAT, 'test_stream')
    test_stream_payload = beam_runner_api_pb2.TestStreamPayload()
    # TestStream source doesn't do any decoding of elements,
    # so we won't set test_stream_payload.coder_id.
    output_coder = transform._infer_output_coder()  # pylint: disable=protected-access
    for event in transform._events:
      new_event = test_stream_payload.events.add()
      if isinstance(event, ElementEvent):
        for tv in event.timestamped_values:
          element = new_event.element_event.elements.add()
          element.encoded_element = output_coder.encode(tv.value)
          element.timestamp = tv.timestamp.micros
      elif isinstance(event, ProcessingTimeEvent):
        new_event.processing_time_event.advance_duration = (
            event.advance_by.micros)
      elif isinstance(event, WatermarkEvent):
        new_event.watermark_event.new_watermark = event.new_watermark.micros
    serialized_payload = self.byte_array_to_json_string(
        test_stream_payload.SerializeToString())
    step.add_property(PropertyNames.SERIALIZED_TEST_STREAM, serialized_payload)

    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{
            PropertyNames.USER_NAME: (
                '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT
        }])

  # We must mark this method as not a test or else its name is a matcher for
  # nosetest tests.
  run_TestStream.__test__ = False  # type: ignore[attr-defined]

  @classmethod
  def serialize_windowing_strategy(cls, windowing, default_environment):
    from apache_beam.runners import pipeline_context
    context = pipeline_context.PipelineContext(
        default_environment=default_environment)
    windowing_proto = windowing.to_runner_api(context)
    return cls.byte_array_to_json_string(
        beam_runner_api_pb2.MessageWithComponents(
            components=context.to_runner_api(),
            windowing_strategy=windowing_proto).SerializeToString())

  @classmethod
  def deserialize_windowing_strategy(cls, serialized_data):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners import pipeline_context
    from apache_beam.transforms.core import Windowing
    proto = beam_runner_api_pb2.MessageWithComponents()
    proto.ParseFromString(cls.json_string_to_byte_array(serialized_data))
    return Windowing.from_runner_api(
        proto.windowing_strategy,
        pipeline_context.PipelineContext(proto.components))

  @staticmethod
  def byte_array_to_json_string(raw_bytes):
    """Implements org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString."""
    return quote(raw_bytes)

  @staticmethod
  def json_string_to_byte_array(encoded_string):
    """Implements org.apache.beam.sdk.util.StringUtils.jsonStringToByteArray."""
    return unquote_to_bytes(encoded_string)

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
  options.view_as(
      GoogleCloudOptions).dataflow_service_options = dataflow_service_options

  # Ensure that prime is specified as an experiment if specified as a dataflow
  # service option
  if 'enable_prime' in dataflow_service_options:
    debug_options.add_experiment('enable_prime')
  elif debug_options.lookup_experiment('enable_prime'):
    dataflow_service_options.append('enable_prime')

  # Streaming only supports using runner v2 (aka unified worker).
  # Runner v2 only supports using streaming engine (aka windmill service)
  if options.view_as(StandardOptions).streaming:
    google_cloud_options = options.view_as(GoogleCloudOptions)
    if _is_runner_v2_disabled(options):
      raise ValueError(
          'Disabling Runner V2 no longer supported for streaming pipeline '
          'using Beam Python %s.' % beam.version.__version__)

    if (not google_cloud_options.enable_streaming_engine and
        (debug_options.lookup_experiment("enable_windmill_service") or
         debug_options.lookup_experiment("enable_streaming_engine"))):
      raise ValueError(
          """Streaming engine both disabled and enabled:
          --enable_streaming_engine flag is not set, but
          enable_windmill_service and/or enable_streaming_engine experiments
          are present. It is recommended you only set the
          --enable_streaming_engine flag.""")

    # Ensure that if we detected a streaming pipeline that streaming specific
    # options and experiments.
    options.view_as(StandardOptions).streaming = True
    google_cloud_options.enable_streaming_engine = True
    debug_options.add_experiment("enable_streaming_engine")
    debug_options.add_experiment("enable_windmill_service")
    _add_runner_v2_missing_options(debug_options)
  elif (debug_options.lookup_experiment('enable_prime') or
        debug_options.lookup_experiment('beam_fn_api') or
        debug_options.lookup_experiment('use_unified_worker') or
        debug_options.lookup_experiment('use_runner_v2') or
        debug_options.lookup_experiment('use_portable_job_submission')):
    if _is_runner_v2_disabled(options):
      raise ValueError(
          """Runner V2 both disabled and enabled: at least one of
          ['enable_prime', 'beam_fn_api', 'use_unified_worker', 'use_runner_v2',
          'use_portable_job_submission'] is set and also one of
          ['disable_runner_v2', 'disable_runner_v2_until_2023',
          'disable_prime_runner_v2'] is set.""")
    _add_runner_v2_missing_options(debug_options)


def _is_runner_v2(options):
  # Type: (PipelineOptions) -> bool

  """Returns true if runner v2 is enabled."""
  _check_and_add_missing_options(options)
  return options.view_as(DebugOptions).lookup_experiment(
      'use_runner_v2', default=False)


def _is_runner_v2_disabled(options):
  # Type: (PipelineOptions) -> bool

  """Returns true if runner v2 is disabled."""
  debug_options = options.view_as(DebugOptions)
  return (
      debug_options.lookup_experiment('disable_runner_v2') or
      debug_options.lookup_experiment('disable_runner_v2_until_2023') or
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
  def __init__(self, job, runner):
    """Initialize a new DataflowPipelineResult instance.

    Args:
      job: Job message from the Dataflow API. Could be :data:`None` if a job
        request was not sent to Dataflow service (e.g. template jobs).
      runner: DataflowRunner instance.
    """
    self._job = job
    self._runner = runner
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
      return PipelineState.UNKNOWN

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
      consoleUrl = (
          "Console URL: https://console.cloud.google.com/"
          f"dataflow/jobs/<RegionId>/{self.job_id()}"
          "?project=<ProjectId>")
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

      # TODO(https://github.com/apache/beam/issues/21695): Also run this check
      # if wait_until_finish was called after the pipeline completed.
      if terminated and self.state != PipelineState.DONE:
        # TODO(BEAM-1290): Consider converting this to an error log based on
        # theresolution of the issue.
        _LOGGER.error(consoleUrl)
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
