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

import itertools
import logging
import uuid
from builtins import object

from google.protobuf import timestamp_pb2

from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger()


def make_state_event(state, timestamp):
  if isinstance(timestamp, Timestamp):
    proto_timestamp = timestamp.to_proto()
  elif isinstance(timestamp, timestamp_pb2.Timestamp):
    proto_timestamp = timestamp
  else:
    raise ValueError("Expected apache_beam.utils.timestamp.Timestamp, "
                     "or google.protobuf.timestamp_pb2.Timestamp. "
                     "Got %s" % type(timestamp))

  return beam_job_api_pb2.JobStateEvent(
      state=state, timestamp=proto_timestamp)


class AbstractJobServiceServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """Manages one or more pipelines, possibly concurrently.
  Experimental: No backward compatibility guaranteed.
  Servicer for the Beam Job API.
  """
  def __init__(self):
    self._jobs = {}

  def create_beam_job(self, preparation_id, job_name, pipeline, options):
    """Returns an instance of AbstractBeamJob specific to this servicer."""
    raise NotImplementedError(type(self))

  def Prepare(self, request, context=None, timeout=None):
    _LOGGER.debug('Got Prepare request.')
    preparation_id = '%s-%s' % (request.job_name, uuid.uuid4())
    self._jobs[preparation_id] = self.create_beam_job(
        preparation_id,
        request.job_name,
        request.pipeline,
        request.pipeline_options)
    self._jobs[preparation_id].prepare()
    _LOGGER.debug("Prepared job '%s' as '%s'", request.job_name, preparation_id)
    return beam_job_api_pb2.PrepareJobResponse(
        preparation_id=preparation_id,
        artifact_staging_endpoint=self._jobs[
            preparation_id].artifact_staging_endpoint(),
        staging_session_token=preparation_id)

  def Run(self, request, context=None, timeout=None):
    # For now, just use the preparation id as the job id.
    job_id = request.preparation_id
    _LOGGER.info("Running job '%s'", job_id)
    self._jobs[job_id].run()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetJobs(self, request, context=None, timeout=None):
    return beam_job_api_pb2.GetJobsResponse(
        job_info=[job.to_runner_api() for job in self._jobs.values()])

  def GetState(self, request, context=None):
    return beam_job_api_pb2.JobStateEvent(
        state=self._jobs[request.job_id].get_state())

  def GetPipeline(self, request, context=None, timeout=None):
    return beam_job_api_pb2.GetJobPipelineResponse(
        pipeline=self._jobs[request.job_id].get_pipeline())

  def Cancel(self, request, context=None, timeout=None):
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobResponse(
        state=self._jobs[request.job_id].get_state())

  def GetStateStream(self, request, context=None, timeout=None):
    """Yields state transitions since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for state, timestamp in job.get_state_stream():
      yield make_state_event(state, timestamp)

  def GetMessageStream(self, request, context=None, timeout=None):
    """Yields messages since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for msg in job.get_message_stream():
      if isinstance(msg, tuple):
        resp = beam_job_api_pb2.JobMessagesResponse(
            state_response=make_state_event(*msg))
      else:
        resp = beam_job_api_pb2.JobMessagesResponse(message_response=msg)
      yield resp

  def DescribePipelineOptions(self, request, context=None, timeout=None):
    return beam_job_api_pb2.DescribePipelineOptionsResponse()


class AbstractBeamJob(object):
  """Abstract baseclass for managing a single Beam job."""

  def __init__(self, job_id, job_name, pipeline, options):
    self._job_id = job_id
    self._job_name = job_name
    self._pipeline_proto = pipeline
    self._pipeline_options = options
    self._state_history = [(beam_job_api_pb2.JobState.STOPPED,
                            Timestamp.now())]

  def _to_implement(self):
    raise NotImplementedError(self)

  prepare = run = cancel = _to_implement
  artifact_staging_endpoint = _to_implement
  get_state_stream = get_message_stream = _to_implement

  @property
  def state(self):
    """Get the latest state enum."""
    return self.get_state()[0]

  def get_state(self):
    """Get a tuple of the latest state and its timestamp."""
    # this is safe: initial state is set in __init__
    return self._state_history[-1]

  def set_state(self, new_state):
    """Set the latest state as an int enum and update the state history.

    :param new_state: int
      latest state enum
    :return: Timestamp or None
      the new timestamp if the state has not changed, else None
    """
    if new_state != self._state_history[-1][0]:
      timestamp = Timestamp.now()
      self._state_history.append((new_state, timestamp))
      return timestamp
    else:
      return None

  def with_state_history(self, state_stream):
    """Utility to prepend recorded state history to an active state stream"""
    return itertools.chain(self._state_history[:], state_stream)

  def get_pipeline(self):
    return self._pipeline_proto

  @staticmethod
  def is_terminal_state(state):
    from apache_beam.runners.portability import portable_runner
    return state in portable_runner.TERMINAL_STATES

  def to_runner_api(self):
    return beam_job_api_pb2.JobInfo(
        job_id=self._job_id,
        job_name=self._job_name,
        pipeline_options=self._pipeline_options,
        state=self.state)
