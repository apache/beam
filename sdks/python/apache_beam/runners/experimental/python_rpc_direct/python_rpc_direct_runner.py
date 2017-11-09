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
"""

import logging
import random
import string

import grpc

from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.runners.job import utils as job_utils
from apache_beam.runners.job.manager import DockerRPCManager
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner

__all__ = ['PythonRPCDirectRunner']


class PythonRPCDirectRunner(PipelineRunner):
  """Executes a single pipeline on the local machine inside a container."""

  # A list of PTransformOverride objects to be applied before running a pipeline
  # using DirectRunner.
  # Currently this only works for overrides where the input and output types do
  # not change.
  # For internal SDK use only. This should not be updated by Beam pipeline
  # authors.
  _PTRANSFORM_OVERRIDES = []

  def __init__(self):
    self._cache = None

  def run(self, pipeline):
    """Remotely executes entire pipeline or parts reachable from node."""

    # Performing configured PTransform overrides.
    pipeline.replace_all(PythonRPCDirectRunner._PTRANSFORM_OVERRIDES)

    # Start the RPC co-process
    manager = DockerRPCManager()

    # Submit the job to the RPC co-process
    jobName = ('Job-' +
               ''.join(random.choice(string.ascii_uppercase) for _ in range(6)))
    options = {k: v for k, v in pipeline._options.get_all_options().iteritems()
               if v is not None}

    try:
      response = manager.service.run(beam_job_api_pb2.SubmitJobRequest(
          pipeline=pipeline.to_runner_api(),
          pipelineOptions=job_utils.dict_to_struct(options),
          jobName=jobName))

      logging.info('Submitted a job with id: %s', response.jobId)

      # Return the result object that references the manager instance
      result = PythonRPCDirectPipelineResult(response.jobId, manager)
      return result
    except grpc.RpcError:
      logging.error('Failed to run the job with name: %s', jobName)
      raise


class PythonRPCDirectPipelineResult(PipelineResult):
  """Represents the state of a pipeline run on the Dataflow service."""

  def __init__(self, job_id, job_manager):
    self.job_id = job_id
    self.manager = job_manager

  @property
  def state(self):
    return self.manager.service.getState(
        beam_job_api_pb2.GetJobStateRequest(jobId=self.job_id))

  def wait_until_finish(self):
    messages_request = beam_job_api_pb2.JobMessagesRequest(jobId=self.job_id)
    for message in self.manager.service.getMessageStream(messages_request):
      if message.HasField('stateResponse'):
        logging.info(
            'Current state of job: %s',
            beam_job_api_pb2.JobState.Enum.Name(
                message.stateResponse.state))
      else:
        logging.info('Message %s', message.messageResponse)
    logging.info('Job with id: %s in terminal state now.', self.job_id)

  def cancel(self):
    return self.manager.service.cancel(
        beam_job_api_pb2.CancelJobRequest(jobId=self.job_id))

  def metrics(self):
    raise NotImplementedError
