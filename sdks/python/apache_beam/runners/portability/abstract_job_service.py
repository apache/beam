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
# pytype: skip-file

from __future__ import absolute_import

import copy
import itertools
import json
import logging
import shutil
import tempfile
import uuid
import zipfile
from builtins import object
from concurrent import futures
from typing import TYPE_CHECKING
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import Union

import grpc
from google.protobuf import json_format
from google.protobuf import timestamp_pb2

from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import artifact_service
from apache_beam.utils.timestamp import Timestamp

if TYPE_CHECKING:
  # pylint: disable=ungrouped-imports
  from typing import BinaryIO
  from google.protobuf import struct_pb2
  from apache_beam.portability.api import beam_runner_api_pb2

_LOGGER = logging.getLogger(__name__)

StateEvent = Tuple[int, Union[timestamp_pb2.Timestamp, Timestamp]]


def make_state_event(state, timestamp):
  if isinstance(timestamp, Timestamp):
    proto_timestamp = timestamp.to_proto()
  elif isinstance(timestamp, timestamp_pb2.Timestamp):
    proto_timestamp = timestamp
  else:
    raise ValueError(
        "Expected apache_beam.utils.timestamp.Timestamp, "
        "or google.protobuf.timestamp_pb2.Timestamp. "
        "Got %s" % type(timestamp))

  return beam_job_api_pb2.JobStateEvent(state=state, timestamp=proto_timestamp)


class AbstractJobServiceServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """Manages one or more pipelines, possibly concurrently.
  Experimental: No backward compatibility guaranteed.
  Servicer for the Beam Job API.
  """
  def __init__(self):
    self._jobs: Dict[str, AbstractBeamJob] = {}

  def create_beam_job(self,
                      preparation_id,  # stype: str
                      job_name: str,
                      pipeline: beam_runner_api_pb2.Pipeline,
                      options: struct_pb2.Struct
                     ) -> AbstractBeamJob:
    """Returns an instance of AbstractBeamJob specific to this servicer."""
    raise NotImplementedError(type(self))

  def Prepare(
      self,
      request: beam_job_api_pb2.PrepareJobRequest,
      context=None,
      timeout=None) -> beam_job_api_pb2.PrepareJobResponse:
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
        artifact_staging_endpoint=self._jobs[preparation_id].
        artifact_staging_endpoint(),
        staging_session_token=preparation_id)

  def Run(
      self,
      request: beam_job_api_pb2.RunJobRequest,
      context=None,
      timeout=None) -> beam_job_api_pb2.RunJobResponse:
    # For now, just use the preparation id as the job id.
    job_id = request.preparation_id
    _LOGGER.info("Running job '%s'", job_id)
    self._jobs[job_id].run()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetJobs(
      self,
      request: beam_job_api_pb2.GetJobsRequest,
      context=None,
      timeout=None) -> beam_job_api_pb2.GetJobsResponse:
    return beam_job_api_pb2.GetJobsResponse(
        job_info=[job.to_runner_api() for job in self._jobs.values()])

  def GetState(
      self,
      request: beam_job_api_pb2.GetJobStateRequest,
      context=None) -> beam_job_api_pb2.JobStateEvent:
    return make_state_event(*self._jobs[request.job_id].get_state())

  def GetPipeline(
      self,
      request: beam_job_api_pb2.GetJobPipelineRequest,
      context=None,
      timeout=None) -> beam_job_api_pb2.GetJobPipelineResponse:
    return beam_job_api_pb2.GetJobPipelineResponse(
        pipeline=self._jobs[request.job_id].get_pipeline())

  def Cancel(
      self,
      request: beam_job_api_pb2.CancelJobRequest,
      context=None,
      timeout=None) -> beam_job_api_pb2.CancelJobResponse:
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobResponse(
        state=self._jobs[request.job_id].get_state()[0])

  def GetStateStream(self,
                     request,
                     context=None,
                     timeout=None) -> Iterator[beam_job_api_pb2.JobStateEvent]:
    """Yields state transitions since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for state, timestamp in job.get_state_stream():
      yield make_state_event(state, timestamp)

  def GetMessageStream(
      self,
      request,
      context=None,
      timeout=None) -> Iterator[beam_job_api_pb2.JobMessagesResponse]:
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

  def DescribePipelineOptions(
      self,
      request,
      context=None,
      timeout=None) -> beam_job_api_pb2.DescribePipelineOptionsResponse:
    return beam_job_api_pb2.DescribePipelineOptionsResponse()


class AbstractBeamJob(object):
  """Abstract baseclass for managing a single Beam job."""
  def __init__(
      self,
      job_id: str,
      job_name: Optional[str],
      pipeline: beam_runner_api_pb2.Pipeline,
      options: struct_pb2.Struct):
    self._job_id = job_id
    self._job_name = job_name
    self._pipeline_proto = pipeline
    self._pipeline_options = options
    self._state_history = [(beam_job_api_pb2.JobState.STOPPED, Timestamp.now())]

  def prepare(self) -> None:
    """Called immediately after this class is instantiated"""
    raise NotImplementedError(self)

  def run(self) -> None:
    raise NotImplementedError(self)

  def cancel(self) -> Optional[beam_job_api_pb2.JobState.Enum]:
    raise NotImplementedError(self)

  def artifact_staging_endpoint(
      self) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    raise NotImplementedError(self)

  def get_state_stream(self) -> Iterator[StateEvent]:
    raise NotImplementedError(self)

  def get_message_stream(
      self
  ) -> Iterator[Union[StateEvent, Optional[beam_job_api_pb2.JobMessage]]]:
    raise NotImplementedError(self)

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

  def get_pipeline(self) -> beam_runner_api_pb2.Pipeline:
    return self._pipeline_proto

  @staticmethod
  def is_terminal_state(state):
    from apache_beam.runners.portability import portable_runner
    return state in portable_runner.TERMINAL_STATES

  def to_runner_api(self) -> beam_job_api_pb2.JobInfo:
    return beam_job_api_pb2.JobInfo(
        job_id=self._job_id,
        job_name=self._job_name,
        pipeline_options=self._pipeline_options,
        state=self.state)


class JarArtifactManager(object):
  def __init__(self, jar_path, root):
    self._root = root
    self._zipfile_handle = zipfile.ZipFile(jar_path, 'a')

  def close(self):
    self._zipfile_handle.close()

  def file_writer(self, path: str) -> Tuple[BinaryIO, str]:
    """Given a relative path, returns an open handle that can be written to
    and an reference that can later be used to read this file."""
    full_path = '%s/%s' % (self._root, path)
    return self._zipfile_handle.open(
        full_path, 'w', force_zip64=True), 'classpath://%s' % full_path

  def zipfile_handle(self):
    return self._zipfile_handle


class UberJarBeamJob(AbstractBeamJob):
  """Abstract baseclass for creating a Beam job. The resulting job will be
  packaged and run in an executable uber jar."""

  # These must agree with those defined in PortablePipelineJarUtils.java.
  PIPELINE_FOLDER = 'BEAM-PIPELINE'
  PIPELINE_MANIFEST = PIPELINE_FOLDER + '/pipeline-manifest.json'

  # We only stage a single pipeline in the jar.
  PIPELINE_NAME = 'pipeline'
  PIPELINE_PATH = '/'.join([PIPELINE_FOLDER, PIPELINE_NAME, "pipeline.json"])
  PIPELINE_OPTIONS_PATH = '/'.join(
      [PIPELINE_FOLDER, PIPELINE_NAME, 'pipeline-options.json'])
  ARTIFACT_FOLDER = '/'.join([PIPELINE_FOLDER, PIPELINE_NAME, 'artifacts'])

  def __init__(
      self,
      executable_jar,
      job_id,
      job_name,
      pipeline,
      options,
      artifact_port=0):
    super(UberJarBeamJob, self).__init__(job_id, job_name, pipeline, options)
    self._executable_jar = executable_jar
    self._jar_uploaded = False
    self._artifact_port = artifact_port

  def prepare(self):
    # Copy the executable jar, injecting the pipeline and options as resources.
    with tempfile.NamedTemporaryFile(suffix='.jar') as tout:
      self._jar = tout.name
    shutil.copy(self._executable_jar, self._jar)
    self._start_artifact_service(self._jar, self._artifact_port)

  def _start_artifact_service(self, jar, requested_port):
    self._artifact_manager = JarArtifactManager(self._jar, self.ARTIFACT_FOLDER)
    self._artifact_staging_service = artifact_service.ArtifactStagingService(
        self._artifact_manager.file_writer)
    self._artifact_staging_service.register_job(
        self._job_id,
        {
            env_id: env.dependencies
            for (env_id,
                 env) in self._pipeline_proto.components.environments.items()
        })
    self._artifact_staging_server = grpc.server(futures.ThreadPoolExecutor())
    port = self._artifact_staging_server.add_insecure_port(
        '[::]:%s' % requested_port)
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        self._artifact_staging_service, self._artifact_staging_server)
    self._artifact_staging_endpoint = endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%d' % port)
    self._artifact_staging_server.start()
    _LOGGER.info('Artifact server started on port %s', port)
    return port

  def _stop_artifact_service(self):
    self._artifact_staging_server.stop(1)

    # Update dependencies to point to staged files.
    pipeline = copy.copy(self._pipeline_proto)
    if any(env.dependencies
           for env in pipeline.components.environments.values()):
      for env_id, deps in self._artifact_staging_service.resolved_deps(
          self._job_id).items():
        # Slice assignment not supported for repeated fields.
        env = self._pipeline_proto.components.environments[env_id]
        del env.dependencies[:]
        env.dependencies.extend(deps)

    # Copy the pipeline definition and metadata into the jar.
    z = self._artifact_manager.zipfile_handle()
    with z.open(self.PIPELINE_PATH, 'w') as fout:
      fout.write(
          json_format.MessageToJson(self._pipeline_proto).encode('utf-8'))
    with z.open(self.PIPELINE_OPTIONS_PATH, 'w') as fout:
      fout.write(
          json_format.MessageToJson(self._pipeline_options).encode('utf-8'))
    with z.open(self.PIPELINE_MANIFEST, 'w') as fout:
      fout.write(
          json.dumps({
              'defaultJobName': self.PIPELINE_NAME
          }).encode('utf-8'))

    # Closes the jar file.
    self._artifact_manager.close()

  def artifact_staging_endpoint(self):
    return self._artifact_staging_endpoint
