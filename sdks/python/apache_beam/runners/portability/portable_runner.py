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

import logging
import os
import threading

import grpc

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner

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

  # TODO(angoenka): Read all init parameters from pipeline_options.
  def __init__(self,
               runner_api_address=None,
               job_service_address=None,
               docker_image=None):
    super(PortableRunner, self).__init__()

    self._subprocess = None
    self._runner_api_address = runner_api_address
    if not job_service_address:
      raise ValueError(
          'job_service_address should be provided while creating runner.')
    self._job_service_address = job_service_address
    self._docker_image = docker_image or self.default_docker_image()

  @staticmethod
  def default_docker_image():
    if 'USER' in os.environ:
      # Perhaps also test if this was built?
      logging.info('Using latest locally built Python SDK docker image.')
      return os.environ['USER'] + '-docker.apache.bintray.io/beam/python:latest'
    else:
      logging.warning('Could not find a Python SDK docker image.')
      return 'unknown'

  def _create_job_service(self):
    return beam_job_api_pb2_grpc.JobServiceStub(
        grpc.insecure_channel(self._job_service_address))

  def run_pipeline(self, pipeline):
    # Java has different expectations about coders
    # (windowed in Fn API, but *un*windowed in runner API), whereas the
    # FnApiRunner treats them consistently, so we must guard this.
    # See also BEAM-2717.
    proto_context = pipeline_context.PipelineContext(
        default_environment_url=self._docker_image)
    proto_pipeline = pipeline.to_runner_api(context=proto_context)
    if self._runner_api_address:
      for pcoll in proto_pipeline.components.pcollections.values():
        if pcoll.coder_id not in proto_context.coders:
          coder = coders.registry.get_coder(pickler.loads(pcoll.coder_id))
          pcoll.coder_id = proto_context.coders.get_id(coder)
      proto_context.coders.populate_map(proto_pipeline.components.coders)

    job_service = self._create_job_service()
    prepare_response = job_service.Prepare(
        beam_job_api_pb2.PrepareJobRequest(
            job_name='job', pipeline=proto_pipeline))
    run_response = job_service.Run(
        beam_job_api_pb2.RunJobRequest(
            preparation_id=prepare_response.preparation_id))
    return PipelineResult(job_service, run_response.job_id)


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
