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
import json
import logging
import os
import sys
import threading
import time

import grpc

from apache_beam import metrics
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners import runner
from apache_beam.runners.job import utils as job_utils
from apache_beam.runners.portability import fn_api_runner_transforms
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_stager
from apache_beam.runners.worker import sdk_worker_main
from apache_beam.runners.worker import worker_pool_main

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
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class PortableRunner(runner.PipelineRunner):
  """
    Experimental: No backward compatibility guaranteed.
    A BeamRunner that executes Python pipelines via the Beam Job API.

    This runner is a stub and does not run the actual job.
    This runner schedules the job on a job service. The responsibility of
    running and managing the job lies with the job service used.
  """
  def __init__(self):
    self._dockerized_job_server = None

  @staticmethod
  def default_docker_image():
    if 'USER' in os.environ:
      if sys.version_info[0] == 2:
        version_suffix = ''
      elif sys.version_info[0:2] == (3, 5):
        version_suffix = '3'
      else:
        version_suffix = '3'
        # TODO(BEAM-7474): Use an image which has correct Python minor version.
        logging.warning('Make sure that locally built Python SDK docker image '
                        'has Python %d.%d interpreter. See also: BEAM-7474.' % (
                            sys.version_info[0], sys.version_info[1]))

      # Perhaps also test if this was built?
      image = ('{user}-docker-apache.bintray.io/beam/python'
               '{version_suffix}:latest'.format(
                   user=os.environ['USER'],
                   version_suffix=version_suffix))
      logging.info(
          'Using latest locally built Python SDK docker image: %s.' % image)
      return image

    else:
      logging.warning('Could not find a Python SDK docker image.')
      return 'unknown'

  @staticmethod
  def _create_environment(options):
    portable_options = options.view_as(PortableOptions)
    environment_urn = common_urns.environments.DOCKER.urn
    if portable_options.environment_type == 'DOCKER':
      environment_urn = common_urns.environments.DOCKER.urn
    elif portable_options.environment_type == 'PROCESS':
      environment_urn = common_urns.environments.PROCESS.urn
    elif portable_options.environment_type in ('EXTERNAL', 'LOOPBACK'):
      environment_urn = common_urns.environments.EXTERNAL.urn
    elif portable_options.environment_type:
      if portable_options.environment_type.startswith('beam:env:'):
        environment_urn = portable_options.environment_type
      else:
        raise ValueError(
            'Unknown environment type: %s' % portable_options.environment_type)

    if environment_urn == common_urns.environments.DOCKER.urn:
      docker_image = (
          portable_options.environment_config
          or PortableRunner.default_docker_image())
      return beam_runner_api_pb2.Environment(
          urn=common_urns.environments.DOCKER.urn,
          payload=beam_runner_api_pb2.DockerPayload(
              container_image=docker_image
          ).SerializeToString())
    elif environment_urn == common_urns.environments.PROCESS.urn:
      config = json.loads(portable_options.environment_config)
      return beam_runner_api_pb2.Environment(
          urn=common_urns.environments.PROCESS.urn,
          payload=beam_runner_api_pb2.ProcessPayload(
              os=(config.get('os') or ''),
              arch=(config.get('arch') or ''),
              command=config.get('command'),
              env=(config.get('env') or '')
          ).SerializeToString())
    elif environment_urn == common_urns.environments.EXTERNAL.urn:
      return beam_runner_api_pb2.Environment(
          urn=common_urns.environments.EXTERNAL.urn,
          payload=beam_runner_api_pb2.ExternalPayload(
              endpoint=endpoints_pb2.ApiServiceDescriptor(
                  url=portable_options.environment_config)
          ).SerializeToString())
    else:
      return beam_runner_api_pb2.Environment(
          urn=environment_urn,
          payload=(portable_options.environment_config.encode('ascii')
                   if portable_options.environment_config else None))

  def default_job_server(self, options):
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
        server = job_server.ExternalJobServer(job_endpoint)
    else:
      server = self.default_job_server(options)
    return server.start()

  def run_pipeline(self, pipeline, options):
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
              sdk_worker_main._get_worker_count(options),
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
    # Also apply sdf expansion.
    # These optimizations commute and are idempotent.
    pre_optimize = options.view_as(DebugOptions).lookup_experiment(
        'pre_optimize', 'lift_combiners,expand_sdf').lower()
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
          if phase_name in ('lift_combiners', 'expand_sdf'):
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
      num_retries = 0
      while True:
        try:
          # This reports channel is READY but connections may fail
          # Seems to be only an issue on Mac with port forwardings
          return job_service.DescribePipelineOptions(
              beam_job_api_pb2.DescribePipelineOptionsRequest())
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
          logging.debug("Runner option '%s' was already added" % option.name)

    all_options = options.get_all_options(add_extra_args_fn=add_runner_options)
    # TODO: Define URNs for options.
    # convert int values: https://issues.apache.org/jira/browse/BEAM-5509
    p_options = {'beam:option:' + k + ':v1': (str(v) if type(v) == int else v)
                 for k, v in all_options.items()
                 if v is not None}

    prepare_response = job_service.Prepare(
        beam_job_api_pb2.PrepareJobRequest(
            job_name='job', pipeline=proto_pipeline,
            pipeline_options=job_utils.dict_to_struct(p_options)))
    if prepare_response.artifact_staging_endpoint.url:
      stager = portable_stager.PortableStager(
          grpc.insecure_channel(prepare_response.artifact_staging_endpoint.url),
          prepare_response.staging_session_token)
      retrieval_token, _ = stager.stage_job_resources(
          options,
          staging_location='')
    else:
      retrieval_token = None

    try:
      state_stream = job_service.GetStateStream(
          beam_job_api_pb2.GetJobStateRequest(
              job_id=prepare_response.preparation_id))
      # If there's an error, we don't always get it until we try to read.
      # Fortunately, there's always an immediate current state published.
      state_stream = itertools.chain(
          [next(state_stream)],
          state_stream)
      message_stream = job_service.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(
              job_id=prepare_response.preparation_id))
    except Exception:
      # TODO(BEAM-6442): Unify preparation_id and job_id for all runners.
      state_stream = message_stream = None

    # Run the job and wait for a result.
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

    return PipelineResult(job_service, run_response.job_id, message_stream,
                          state_stream, cleanup_callbacks)


class PortableMetrics(metrics.metric.MetricResults):
  def __init__(self, job_metrics_response):
    # TODO(lgajowy): Convert portable metrics to MetricResults
    # and allow querying them (BEAM-4775)
    pass

  def query(self, filter=None):
    return {'counters': [],
            'distributions': [],
            'gauges': []}


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
          logging.info(
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
