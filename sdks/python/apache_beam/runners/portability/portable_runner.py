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

from __future__ import absolute_import

import functools
import itertools
import logging
import threading
import time
from typing import TYPE_CHECKING
from typing import Optional

import grpc

from apache_beam.metrics import metric
from apache_beam.metrics.execution import MetricResult
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.job import utils as job_utils
from apache_beam.runners.portability import fn_api_runner_transforms
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_metrics
from apache_beam.runners.portability import portable_stager
from apache_beam.runners.worker import sdk_worker_main
from apache_beam.runners.worker import worker_pool_main
from apache_beam.transforms import environments

if TYPE_CHECKING:
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.pipeline import Pipeline

__all__ = ['PortableRunner']

MESSAGE_LOG_LEVELS = {
    beam_job_api_pb2.JobMessage.MESSAGE_IMPORTANCE_UNSPECIFIED: logging.INFO,
    beam_job_api_pb2.JobMessage.JOB_MESSAGE_DEBUG: logging.DEBUG,
    beam_job_api_pb2.JobMessage.JOB_MESSAGE_DETAILED: logging.DEBUG,
    beam_job_api_pb2.JobMessage.JOB_MESSAGE_BASIC: logging.INFO,
    beam_job_api_pb2.JobMessage.JOB_MESSAGE_WARNING: logging.WARNING,
    beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR: logging.ERROR,
}

TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.DRAINED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]

ENV_TYPE_ALIASES = {'LOOPBACK': 'EXTERNAL'}

_LOGGER = logging.getLogger(__name__)


class PortableRunner(runner.PipelineRunner):
  """
    Experimental: No backward compatibility guaranteed.
    A BeamRunner that executes Python pipelines via the Beam Job API.

    This runner is a stub and does not run the actual job.
    This runner schedules the job on a job service. The responsibility of
    running and managing the job lies with the job service used.
  """
  def __init__(self):
    self._dockerized_job_server = None  # type: Optional[job_server.JobServer]

  @staticmethod
  def _create_environment(options):
    # type: (PipelineOptions) -> beam_runner_api_pb2.Environment
    portable_options = options.view_as(PortableOptions)
    # Do not set a Runner. Otherwise this can cause problems in Java's
    # PipelineOptions, i.e. ClassNotFoundException, if the corresponding Runner
    # does not exist in the Java SDK. In portability, the entry point is clearly
    # defined via the JobService.
    portable_options.view_as(StandardOptions).runner = None
    environment_type = portable_options.environment_type
    if not environment_type:
      environment_urn = common_urns.environments.DOCKER.urn
    elif environment_type.startswith('beam:env:'):
      environment_urn = environment_type
    else:
      # e.g. handle LOOPBACK -> EXTERNAL
      environment_type = ENV_TYPE_ALIASES.get(environment_type,
                                              environment_type)
      try:
        environment_urn = getattr(common_urns.environments,
                                  environment_type).urn
      except AttributeError:
        raise ValueError(
            'Unknown environment type: %s' % environment_type)

    env_class = environments.Environment.get_env_cls_from_urn(environment_urn)
    return env_class.from_options(portable_options)

  def default_job_server(self, portable_options):
    # type: (...) -> job_server.JobServer
    # TODO Provide a way to specify a container Docker URL
    # https://issues.apache.org/jira/browse/BEAM-6328
    if not self._dockerized_job_server:
      self._dockerized_job_server = job_server.StopOnExitJobServer(
          job_server.DockerizedJobServer())
    return self._dockerized_job_server

  def create_job_service(self, options):
    job_endpoint = options.view_as(PortableOptions).job_endpoint
    if job_endpoint:
      if job_endpoint == 'embed':
        server = job_server.EmbeddedJobServer()
      else:
        job_server_timeout = options.view_as(PortableOptions).job_server_timeout
        server = job_server.ExternalJobServer(job_endpoint, job_server_timeout)
    else:
      server = self.default_job_server(options)
    return server.start()

  def run_pipeline(self, pipeline, options):
    # type: (Pipeline, PipelineOptions) -> PipelineResult
    portable_options = options.view_as(PortableOptions)

    # TODO: https://issues.apache.org/jira/browse/BEAM-5525
    # portable runner specific default
    if options.view_as(SetupOptions).sdk_location == 'default':
      options.view_as(SetupOptions).sdk_location = 'container'

    # This is needed as we start a worker server if one is requested
    # but none is provided.
    if portable_options.environment_type == 'LOOPBACK':
      use_loopback_process_worker = options.view_as(
          DebugOptions).lookup_experiment(
              'use_loopback_process_worker', False)
      portable_options.environment_config, server = (
          worker_pool_main.BeamFnExternalWorkerPoolServicer.start(
              state_cache_size=sdk_worker_main._get_state_cache_size(options),
              data_buffer_time_limit_ms=
              sdk_worker_main._get_data_buffer_time_limit_ms(options),
              use_process=use_loopback_process_worker))
      cleanup_callbacks = [functools.partial(server.stop, 1)]
    else:
      cleanup_callbacks = []

    proto_pipeline = pipeline.to_runner_api(
        default_environment=PortableRunner._create_environment(
            portable_options))

    # Some runners won't detect the GroupByKey transform unless it has no
    # subtransforms.  Remove all sub-transforms until BEAM-4605 is resolved.
    for _, transform_proto in list(
        proto_pipeline.components.transforms.items()):
      if transform_proto.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
        for sub_transform in transform_proto.subtransforms:
          del proto_pipeline.components.transforms[sub_transform]
        del transform_proto.subtransforms[:]

    # Preemptively apply combiner lifting, until all runners support it.
    # These optimizations commute and are idempotent.
    pre_optimize = options.view_as(DebugOptions).lookup_experiment(
        'pre_optimize', 'lift_combiners').lower()
    if not options.view_as(StandardOptions).streaming:
      flink_known_urns = frozenset([
          common_urns.composites.RESHUFFLE.urn,
          common_urns.primitives.IMPULSE.urn,
          common_urns.primitives.FLATTEN.urn,
          common_urns.primitives.GROUP_BY_KEY.urn])
      if pre_optimize == 'none':
        pass
      elif pre_optimize == 'all':
        proto_pipeline = fn_api_runner_transforms.optimize_pipeline(
            proto_pipeline,
            phases=[fn_api_runner_transforms.annotate_downstream_side_inputs,
                    fn_api_runner_transforms.annotate_stateful_dofns_as_roots,
                    fn_api_runner_transforms.fix_side_input_pcoll_coders,
                    fn_api_runner_transforms.lift_combiners,
                    fn_api_runner_transforms.expand_sdf,
                    fn_api_runner_transforms.fix_flatten_coders,
                    # fn_api_runner_transforms.sink_flattens,
                    fn_api_runner_transforms.greedily_fuse,
                    fn_api_runner_transforms.read_to_impulse,
                    fn_api_runner_transforms.extract_impulse_stages,
                    fn_api_runner_transforms.remove_data_plane_ops,
                    fn_api_runner_transforms.sort_stages],
            known_runner_urns=flink_known_urns)
      else:
        phases = []
        for phase_name in pre_optimize.split(','):
          # For now, these are all we allow.
          if phase_name in 'lift_combiners':
            phases.append(getattr(fn_api_runner_transforms, phase_name))
          else:
            raise ValueError(
                'Unknown or inapplicable phase for pre_optimize: %s'
                % phase_name)
        proto_pipeline = fn_api_runner_transforms.optimize_pipeline(
            proto_pipeline,
            phases=phases,
            known_runner_urns=flink_known_urns,
            partial=True)

    job_service = self.create_job_service(options)

    # fetch runner options from job service
    # retries in case the channel is not ready
    def send_options_request(max_retries=5):
      # type: (int) -> beam_job_api_pb2.DescribePipelineOptionsResponse
      num_retries = 0
      while True:
        try:
          # This reports channel is READY but connections may fail
          # Seems to be only an issue on Mac with port forwardings
          return job_service.DescribePipelineOptions(
              beam_job_api_pb2.DescribePipelineOptionsRequest(),
              timeout=portable_options.job_server_timeout)
        except grpc.FutureTimeoutError:
          # no retry for timeout errors
          raise
        except grpc._channel._Rendezvous as e:
          num_retries += 1
          if num_retries > max_retries:
            raise e
          time.sleep(1)

    options_response = send_options_request()

    def add_runner_options(parser):
      for option in options_response.options:
        try:
          # no default values - we don't want runner options
          # added unless they were specified by the user
          add_arg_args = {'action' : 'store', 'help' : option.description}
          if option.type == beam_job_api_pb2.PipelineOptionType.BOOLEAN:
            add_arg_args['action'] = 'store_true'\
              if option.default_value != 'true' else 'store_false'
          elif option.type == beam_job_api_pb2.PipelineOptionType.INTEGER:
            add_arg_args['type'] = int
          elif option.type == beam_job_api_pb2.PipelineOptionType.ARRAY:
            add_arg_args['action'] = 'append'
          parser.add_argument("--%s" % option.name, **add_arg_args)
        except Exception as e:
          # ignore runner options that are already present
          # only in this case is duplicate not treated as error
          if 'conflicting option string' not in str(e):
            raise
          _LOGGER.debug("Runner option '%s' was already added" % option.name)

    all_options = options.get_all_options(add_extra_args_fn=add_runner_options)
    # TODO: Define URNs for options.
    # convert int values: https://issues.apache.org/jira/browse/BEAM-5509
    p_options = {'beam:option:' + k + ':v1': (str(v) if type(v) == int else v)
                 for k, v in all_options.items()
                 if v is not None}

    prepare_request = beam_job_api_pb2.PrepareJobRequest(
        job_name='job', pipeline=proto_pipeline,
        pipeline_options=job_utils.dict_to_struct(p_options))
    _LOGGER.debug('PrepareJobRequest: %s', prepare_request)
    prepare_response = job_service.Prepare(
        prepare_request,
        timeout=portable_options.job_server_timeout)
    artifact_endpoint = (portable_options.artifact_endpoint
                         if portable_options.artifact_endpoint
                         else prepare_response.artifact_staging_endpoint.url)
    if artifact_endpoint:
      stager = portable_stager.PortableStager(
          grpc.insecure_channel(artifact_endpoint),
          prepare_response.staging_session_token)
      retrieval_token, _ = stager.stage_job_resources(
          options,
          staging_location='')
    else:
      retrieval_token = None

    try:
      state_stream = job_service.GetStateStream(
          beam_job_api_pb2.GetJobStateRequest(
              job_id=prepare_response.preparation_id),
          timeout=portable_options.job_server_timeout)
      # If there's an error, we don't always get it until we try to read.
      # Fortunately, there's always an immediate current state published.
      state_stream = itertools.chain(
          [next(state_stream)],
          state_stream)
      message_stream = job_service.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(
              job_id=prepare_response.preparation_id),
          timeout=portable_options.job_server_timeout)
    except Exception:
      # TODO(BEAM-6442): Unify preparation_id and job_id for all runners.
      state_stream = message_stream = None

    # Run the job and wait for a result, we don't set a timeout here because
    # it may take a long time for a job to complete and streaming
    # jobs currently never return a response.
    run_response = job_service.Run(
        beam_job_api_pb2.RunJobRequest(
            preparation_id=prepare_response.preparation_id,
            retrieval_token=retrieval_token))

    if state_stream is None:
      state_stream = job_service.GetStateStream(
          beam_job_api_pb2.GetJobStateRequest(
              job_id=run_response.job_id))
      message_stream = job_service.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(
              job_id=run_response.job_id))

    result = PipelineResult(job_service, run_response.job_id, message_stream,
                            state_stream, cleanup_callbacks)
    if cleanup_callbacks:
      # We wait here to ensure that we run the cleanup callbacks.
      logging.info('Waiting until the pipeline has finished because the '
                   'environment "%s" has started a component necessary for the '
                   'execution.', portable_options.environment_type)
      result.wait_until_finish()
    return result


class PortableMetrics(metric.MetricResults):
  def __init__(self, job_metrics_response):
    metrics = job_metrics_response.metrics
    self.attempted = portable_metrics.from_monitoring_infos(metrics.attempted)
    self.committed = portable_metrics.from_monitoring_infos(metrics.committed)

  @staticmethod
  def _combine(committed, attempted, filter):
    all_keys = set(committed.keys()) | set(attempted.keys())
    return [
        MetricResult(key, committed.get(key), attempted.get(key))
        for key in all_keys
        if metric.MetricResults.matches(filter, key)
    ]

  def query(self, filter=None):
    counters, distributions, gauges = [
        self._combine(x, y, filter)
        for x, y in zip(self.committed, self.attempted)
    ]

    return {self.COUNTERS: counters,
            self.DISTRIBUTIONS: distributions,
            self.GAUGES: gauges}


class PipelineResult(runner.PipelineResult):

  def __init__(self, job_service, job_id, message_stream, state_stream,
               cleanup_callbacks=()):
    super(PipelineResult, self).__init__(beam_job_api_pb2.JobState.UNSPECIFIED)
    self._job_service = job_service
    self._job_id = job_id
    self._messages = []
    self._message_stream = message_stream
    self._state_stream = state_stream
    self._cleanup_callbacks = cleanup_callbacks
    self._metrics = None

  def cancel(self):
    try:
      self._job_service.Cancel(beam_job_api_pb2.CancelJobRequest(
          job_id=self._job_id))
    finally:
      self._cleanup()

  @property
  def state(self):
    runner_api_state = self._job_service.GetState(
        beam_job_api_pb2.GetJobStateRequest(job_id=self._job_id)).state
    self._state = self._runner_api_state_to_pipeline_state(runner_api_state)
    return self._state

  @staticmethod
  def _runner_api_state_to_pipeline_state(runner_api_state):
    return getattr(runner.PipelineState,
                   beam_job_api_pb2.JobState.Enum.Name(runner_api_state))

  @staticmethod
  def _pipeline_state_to_runner_api_state(pipeline_state):
    return beam_job_api_pb2.JobState.Enum.Value(pipeline_state)

  def metrics(self):
    if not self._metrics:

      job_metrics_response = self._job_service.GetJobMetrics(
          beam_job_api_pb2.GetJobMetricsRequest(job_id=self._job_id))

      self._metrics = PortableMetrics(job_metrics_response)
    return self._metrics

  def _last_error_message(self):
    # Filter only messages with the "message_response" and error messages.
    messages = [m.message_response for m in self._messages
                if m.HasField('message_response')]
    error_messages = [m for m in messages
                      if m.importance ==
                      beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR]
    if error_messages:
      return error_messages[-1].message_text
    else:
      return 'unknown error'

  def wait_until_finish(self):

    def read_messages():
      for message in self._message_stream:
        if message.HasField('message_response'):
          logging.log(
              MESSAGE_LOG_LEVELS[message.message_response.importance],
              "%s",
              message.message_response.message_text)
        else:
          _LOGGER.info(
              "Job state changed to %s",
              self._runner_api_state_to_pipeline_state(
                  message.state_response.state))
        self._messages.append(message)

    t = threading.Thread(target=read_messages, name='wait_until_finish_read')
    t.daemon = True
    t.start()

    try:
      for state_response in self._state_stream:
        self._state = self._runner_api_state_to_pipeline_state(
            state_response.state)
        if state_response.state in TERMINAL_STATES:
          # Wait for any last messages.
          t.join(10)
          break
      if self._state != runner.PipelineState.DONE:
        raise RuntimeError(
            'Pipeline %s failed in state %s: %s' % (
                self._job_id, self._state, self._last_error_message()))
      return self._state
    finally:
      self._cleanup()

  def _cleanup(self):
    has_exception = None
    for callback in self._cleanup_callbacks:
      try:
        callback()
      except Exception:
        has_exception = True
    self._cleanup_callbacks = ()
    if has_exception:
      raise
