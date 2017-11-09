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
import time
import uuid
from concurrent import futures

import grpc

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners.runner import PipelineState

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class JobService(beam_job_api_pb2_grpc.JobServiceServicer):

  def __init__(self):
    self.jobs = {}

  def run(self, request, context):
    job_id = uuid.uuid4().get_hex()
    pipeline_result = Pipeline.from_runner_api(
        request.pipeline,
        'DirectRunner',
        PipelineOptions()).run()
    self.jobs[job_id] = pipeline_result
    return beam_job_api_pb2.SubmitJobResponse(jobId=job_id)

  def getState(self, request, context):
    pipeline_result = self.jobs[request.jobId]
    return beam_job_api_pb2.GetJobStateResponse(
        state=self._map_state_to_jobState(pipeline_result.state))

  def cancel(self, request, context):
    pipeline_result = self.jobs[request.jobId]
    pipeline_result.cancel()
    return beam_job_api_pb2.CancelJobResponse(
        state=self._map_state_to_jobState(pipeline_result.state))

  def getMessageStream(self, request, context):
    pipeline_result = self.jobs[request.jobId]
    pipeline_result.wait_until_finish()
    yield beam_job_api_pb2.JobMessagesResponse(
        stateResponse=beam_job_api_pb2.GetJobStateResponse(
            state=self._map_state_to_jobState(pipeline_result.state)))

  def getStateStream(self, request, context):
    context.set_details('Not Implemented for direct runner!')
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    return

  @staticmethod
  def _map_state_to_jobState(state):
    if state == PipelineState.UNKNOWN:
      return beam_job_api_pb2.JobState.UNSPECIFIED
    elif state == PipelineState.STOPPED:
      return beam_job_api_pb2.JobState.STOPPED
    elif state == PipelineState.RUNNING:
      return beam_job_api_pb2.JobState.RUNNING
    elif state == PipelineState.DONE:
      return beam_job_api_pb2.JobState.DONE
    elif state == PipelineState.FAILED:
      return beam_job_api_pb2.JobState.FAILED
    elif state == PipelineState.CANCELLED:
      return beam_job_api_pb2.JobState.CANCELLED
    elif state == PipelineState.UPDATED:
      return beam_job_api_pb2.JobState.UPDATED
    elif state == PipelineState.DRAINING:
      return beam_job_api_pb2.JobState.DRAINING
    elif state == PipelineState.DRAINED:
      return beam_job_api_pb2.JobState.DRAINED
    else:
      raise ValueError('Unknown pipeline state')


def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(JobService(), server)

  server.add_insecure_port('[::]:50051')
  server.start()

  try:
    while True:
      time.sleep(_ONE_DAY_IN_SECONDS)
  except KeyboardInterrupt:
    server.stop(0)


if __name__ == '__main__':
  serve()
