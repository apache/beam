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

import logging
import uuid
from builtins import object
from typing import TYPE_CHECKING
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Union

from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc

if TYPE_CHECKING:
  from google.protobuf import struct_pb2  # pylint: disable=ungrouped-imports
  from apache_beam.portability.api import beam_runner_api_pb2
  from apache_beam.portability.api import endpoints_pb2

TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class AbstractJobServiceServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """Manages one or more pipelines, possibly concurrently.
  Experimental: No backward compatibility guaranteed.
  Servicer for the Beam Job API.
  """
  def __init__(self):
    self._jobs = {}  # type: Dict[str, AbstractBeamJob]

  def create_beam_job(self,
                      preparation_id,  # stype: str
                      job_name,  # type: str
                      pipeline,  # type: beam_runner_api_pb2.Pipeline
                      options  # type: struct_pb2.Struct
                     ):
    # type: (...) -> AbstractBeamJob
    """Returns an instance of AbstractBeamJob specific to this servicer."""
    raise NotImplementedError(type(self))

  def Prepare(self,
              request,  # type: beam_job_api_pb2.PrepareJobRequest
              context=None,
              timeout=None
             ):
    # type: (...) -> beam_job_api_pb2.PrepareJobResponse
    logging.debug('Got Prepare request.')
    preparation_id = '%s-%s' % (request.job_name, uuid.uuid4())
    self._jobs[preparation_id] = self.create_beam_job(
        preparation_id,
        request.job_name,
        request.pipeline,
        request.pipeline_options)
    self._jobs[preparation_id].prepare()
    logging.debug("Prepared job '%s' as '%s'", request.job_name, preparation_id)
    return beam_job_api_pb2.PrepareJobResponse(
        preparation_id=preparation_id,
        artifact_staging_endpoint=self._jobs[
            preparation_id].artifact_staging_endpoint(),
        staging_session_token=preparation_id)

  def Run(self,
          request,  # type: beam_job_api_pb2.RunJobRequest
          context=None,
          timeout=None
         ):
    # type: (...) -> beam_job_api_pb2.RunJobResponse
    # For now, just use the preparation id as the job id.
    job_id = request.preparation_id
    logging.info("Running job '%s'", job_id)
    self._jobs[job_id].run()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetJobs(self,
              request,  # type: beam_job_api_pb2.GetJobsRequest
              context=None,
              timeout=None
             ):
    # type: (...) -> beam_job_api_pb2.GetJobsResponse
    return beam_job_api_pb2.GetJobsResponse(
        job_info=[job.to_runner_api() for job in self._jobs.values()])

  def GetState(self,
               request,  # type: beam_job_api_pb2.GetJobStateRequest
               context=None
              ):
    # type: (...) -> beam_job_api_pb2.GetJobStateResponse
    return beam_job_api_pb2.GetJobStateResponse(
        state=self._jobs[request.job_id].get_state())

  def GetPipeline(self,
                  request,  # type: beam_job_api_pb2.GetJobPipelineRequest
                  context=None,
                  timeout=None
                 ):
    # type: (...) -> beam_job_api_pb2.GetJobPipelineResponse
    return beam_job_api_pb2.GetJobPipelineResponse(
        pipeline=self._jobs[request.job_id].get_pipeline())

  def Cancel(self,
             request,  # type: beam_job_api_pb2.CancelJobRequest
             context=None,
             timeout=None
            ):
    # type: (...) -> beam_job_api_pb2.CancelJobResponse
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobResponse(
        state=self._jobs[request.job_id].get_state())

  def GetStateStream(self, request, context=None, timeout=None):
    # type: (...) -> Iterator[beam_job_api_pb2.GetJobStateResponse]
    """Yields state transitions since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for state in job.get_state_stream():
      yield beam_job_api_pb2.GetJobStateResponse(state=state)

  def GetMessageStream(self, request, context=None, timeout=None):
    # type: (...) -> Iterator[beam_job_api_pb2.JobMessagesResponse]
    """Yields messages since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for msg in job.get_message_stream():
      if isinstance(msg, int):
        resp = beam_job_api_pb2.JobMessagesResponse(
            state_response=beam_job_api_pb2.GetJobStateResponse(state=msg))
      else:
        resp = beam_job_api_pb2.JobMessagesResponse(message_response=msg)
      yield resp

  def DescribePipelineOptions(self, request, context=None, timeout=None):
    # type: (...) -> beam_job_api_pb2.DescribePipelineOptionsResponse
    return beam_job_api_pb2.DescribePipelineOptionsResponse()


class AbstractBeamJob(object):
  """Abstract baseclass for managing a single Beam job."""

  def __init__(self,
               job_id,  # type: str
               job_name,  # type: str
               pipeline,  # type: beam_runner_api_pb2.Pipeline
               options  # type: struct_pb2.Struct
              ):
    self._job_id = job_id
    self._job_name = job_name
    self._pipeline_proto = pipeline
    self._pipeline_options = options

  def prepare(self):
    # type: () -> None
    """Called immediately after this class is instantiated"""
    raise NotImplementedError(self)

  def run(self):
    # type: () -> None
    raise NotImplementedError(self)

  def cancel(self):
    # type: () -> Optional[beam_job_api_pb2.JobState.Enum]
    raise NotImplementedError(self)

  def artifact_staging_endpoint(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError(self)

  def get_state(self):
    # type: () -> Optional[beam_job_api_pb2.JobState.Enum]
    raise NotImplementedError(self)

  def get_state_stream(self):
    # type: () -> Iterator[Optional[beam_job_api_pb2.JobState.Enum]]
    raise NotImplementedError(self)

  def get_message_stream(self):
    # type: () -> Iterator[Union[int, Optional[beam_job_api_pb2.JobMessage]]]
    raise NotImplementedError(self)

  def get_pipeline(self):
    # type: () -> beam_runner_api_pb2.Pipeline
    return self._pipeline_proto

  def to_runner_api(self):
    # type: () -> beam_job_api_pb2.JobInfo
    return beam_job_api_pb2.JobInfo(
        job_id=self._job_id,
        job_name=self._job_name,
        pipeline_options=self._pipeline_options,
        state=self.get_state())
