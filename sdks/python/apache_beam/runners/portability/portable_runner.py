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

import json
import logging
import os
import threading

import grpc

from apache_beam import coders
from apache_beam import metrics
from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner
from apache_beam.runners.job import utils as job_utils
from apache_beam.runners.portability import portable_stager
from apache_beam.runners.portability.job_server import DockerizedJobServer

__all__ = ['PortableRunner']

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

  def __init__(self, is_embedded_fnapi_runner=False):
    self.is_embedded_fnapi_runner = is_embedded_fnapi_runner

  @staticmethod
  def default_docker_image():
    if 'USER' in os.environ:
      # Perhaps also test if this was built?
      logging.info('Using latest locally built Python SDK docker image.')
      return os.environ['USER'] + '-docker-apache.bintray.io/beam/python:latest'
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

    if environment_urn == common_urns.environments.DOCKER.urn:
      docker_image = (
          portable_options.environment_config
          or PortableRunner.default_docker_image())
      return beam_runner_api_pb2.Environment(
          url=docker_image,
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

  def run_pipeline(self, pipeline):
    portable_options = pipeline.options.view_as(PortableOptions)
    job_endpoint = portable_options.job_endpoint

    # TODO: https://issues.apache.org/jira/browse/BEAM-5525
    # portable runner specific default
    if pipeline.options.view_as(SetupOptions).sdk_location == 'default':
      pipeline.options.view_as(SetupOptions).sdk_location = 'container'

    if not job_endpoint:
      docker = DockerizedJobServer()
      job_endpoint = docker.start()

    proto_context = pipeline_context.PipelineContext(
        default_environment=PortableRunner._create_environment(
            portable_options))
    proto_pipeline = pipeline.to_runner_api(context=proto_context)

    if not self.is_embedded_fnapi_runner:
      # Java has different expectations about coders
      # (windowed in Fn API, but *un*windowed in runner API), whereas the
      # embedded FnApiRunner treats them consistently, so we must guard this
      # for now, until FnApiRunner is fixed.
      # See also BEAM-2717.
      for pcoll in proto_pipeline.components.pcollections.values():
        if pcoll.coder_id not in proto_context.coders:
          # This is not really a coder id, but a pickled coder.
          coder = coders.registry.get_coder(pickler.loads(pcoll.coder_id))
          pcoll.coder_id = proto_context.coders.get_id(coder)
      proto_context.coders.populate_map(proto_pipeline.components.coders)

    # Some runners won't detect the GroupByKey transform unless it has no
    # subtransforms.  Remove all sub-transforms until BEAM-4605 is resolved.
    for _, transform_proto in list(
        proto_pipeline.components.transforms.items()):
      if transform_proto.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
        for sub_transform in transform_proto.subtransforms:
          del proto_pipeline.components.transforms[sub_transform]
        del transform_proto.subtransforms[:]

    # TODO: Define URNs for options.
    # convert int values: https://issues.apache.org/jira/browse/BEAM-5509
    options = {'beam:option:' + k + ':v1': (str(v) if type(v) == int else v)
               for k, v in pipeline._options.get_all_options().items()
               if v is not None}

    channel = grpc.insecure_channel(job_endpoint)
    grpc.channel_ready_future(channel).result()
    job_service = beam_job_api_pb2_grpc.JobServiceStub(channel)

    # Sends the PrepareRequest but retries in case the channel is not ready
    def send_prepare_request(max_retries=5):
      num_retries = 0
      while True:
        try:
          # This reports channel is READY but connections may fail
          # Seems to be only an issue on Mac with port forwardings
          grpc.channel_ready_future(channel).result()
          return job_service.Prepare(
              beam_job_api_pb2.PrepareJobRequest(
                  job_name='job', pipeline=proto_pipeline,
                  pipeline_options=job_utils.dict_to_struct(options)))
        except grpc._channel._Rendezvous as e:
          num_retries += 1
          if num_retries > max_retries:
            raise e

    prepare_response = send_prepare_request()
    if prepare_response.artifact_staging_endpoint.url:
      stager = portable_stager.PortableStager(
          grpc.insecure_channel(prepare_response.artifact_staging_endpoint.url),
          prepare_response.staging_session_token)
      retrieval_token, _ = stager.stage_job_resources(
          pipeline._options,
          staging_location='')
    else:
      retrieval_token = None
    run_response = job_service.Run(
        beam_job_api_pb2.RunJobRequest(
            preparation_id=prepare_response.preparation_id,
            retrieval_token=retrieval_token))
    return PipelineResult(job_service, run_response.job_id)


class PortableMetrics(metrics.metric.MetricResults):
  def __init__(self):
    pass

  def query(self, filter=None):
    return {'counters': [],
            'distributions': [],
            'gauges': []}


class PipelineResult(runner.PipelineResult):

  def __init__(self, job_service, job_id):
    super(PipelineResult, self).__init__(beam_job_api_pb2.JobState.UNSPECIFIED)
    self._job_service = job_service
    self._job_id = job_id
    self._messages = []

  def cancel(self):
    self._job_service.Cancel()

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
    return PortableMetrics()

  def wait_until_finish(self):

    def read_messages():
      for message in self._job_service.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(job_id=self._job_id)):
        self._messages.append(message)

    t = threading.Thread(target=read_messages, name='wait_until_finish_read')
    t.daemon = True
    t.start()

    for state_response in self._job_service.GetStateStream(
        beam_job_api_pb2.GetJobStateRequest(job_id=self._job_id)):
      self._state = self._runner_api_state_to_pipeline_state(
          state_response.state)
      if state_response.state in TERMINAL_STATES:
        break
    if self._state != runner.PipelineState.DONE:
      raise RuntimeError(
          'Pipeline %s failed in state %s.' % (self._job_id, self._state))
    return self._state
