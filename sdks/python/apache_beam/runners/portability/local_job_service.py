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
import os
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import traceback
import uuid
from builtins import object
from concurrent import futures

import grpc
from google.protobuf import text_format

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import fn_api_runner

TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class LocalJobServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """Manages one or more pipelines, possibly concurrently.
    Experimental: No backward compatibility guaranteed.
    Servicer for the Beam Job API.

    This JobService uses a basic local implementation of runner to run the job.
    This JobService is not capable of managing job on remote clusters.

    By default, this JobService executes the job in process but still uses GRPC
    to communicate pipeline and worker state.  It can also be configured to use
    inline calls rather than GRPC (for speed) or launch completely separate
    subprocesses for the runner and worker(s).
    """

  def __init__(self, staging_dir=None):
    self._jobs = {}
    self._cleanup_staging_dir = staging_dir is None
    self._staging_dir = staging_dir or tempfile.mkdtemp()
    self._artifact_service = artifact_service.BeamFilesystemArtifactService(
        self._staging_dir)
    self._artifact_staging_endpoint = None

  def start_grpc_server(self, port=0):
    self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    port = self._server.add_insecure_port('localhost:%d' % port)
    beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(self, self._server)
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        self._artifact_service, self._server)
    self._artifact_staging_endpoint = endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%d' % port)
    self._server.start()
    logging.info('Grpc server started on port %s', port)
    return port

  def stop(self, timeout=1):
    self._server.stop(timeout)
    if os.path.exists(self._staging_dir) and self._cleanup_staging_dir:
      shutil.rmtree(self._staging_dir, ignore_errors=True)

  def Prepare(self, request, context=None):
    # For now, just use the job name as the job id.
    logging.debug('Got Prepare request.')
    preparation_id = '%s-%s' % (request.job_name, uuid.uuid4())
    provision_info = fn_api_runner.ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            job_id=preparation_id,
            job_name=request.job_name,
            pipeline_options=request.pipeline_options,
            retrieval_token=self._artifact_service.retrieval_token(
                preparation_id)),
        self._staging_dir)
    self._jobs[preparation_id] = BeamJob(
        preparation_id,
        request.pipeline_options,
        request.pipeline,
        provision_info)
    logging.debug("Prepared job '%s' as '%s'", request.job_name, preparation_id)
    # TODO(angoenka): Pass an appropriate staging_session_token. The token can
    # be obtained in PutArtifactResponse from JobService
    if not self._artifact_staging_endpoint:
      # The front-end didn't try to stage anything, but the worker may
      # request what's here so we should at least store an empty manifest.
      self._artifact_service.CommitManifest(
          beam_artifact_api_pb2.CommitManifestRequest(
              staging_session_token=preparation_id,
              manifest=beam_artifact_api_pb2.Manifest()))
    return beam_job_api_pb2.PrepareJobResponse(
        preparation_id=preparation_id,
        artifact_staging_endpoint=self._artifact_staging_endpoint,
        staging_session_token=preparation_id)

  def Run(self, request, context=None):
    job_id = request.preparation_id
    logging.info("Runing job '%s'", job_id)
    self._jobs[job_id].start()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetState(self, request, context=None):
    return beam_job_api_pb2.GetJobStateResponse(
        state=self._jobs[request.job_id].state)

  def Cancel(self, request, context=None):
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobRequest(
        state=self._jobs[request.job_id].state)

  def GetStateStream(self, request, context=None):
    """Yields state transitions since the stream started.
      """
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    job = self._jobs[request.job_id]
    for state in job.get_state_stream():
      yield beam_job_api_pb2.GetJobStateResponse(state=state)

  def GetMessageStream(self, request, context=None):
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

  def DescribePipelineOptions(self, request, context=None):
    return beam_job_api_pb2.DescribePipelineOptionsResponse()


class SubprocessSdkWorker(object):
  """Manages a SDK worker implemented as a subprocess communicating over grpc.
    """

  def __init__(self, worker_command_line, control_address):
    self._worker_command_line = worker_command_line
    self._control_address = control_address

  def run(self):
    logging_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_port = logging_server.add_insecure_port('[::]:0')
    logging_server.start()
    logging_servicer = BeamFnLoggingServicer()
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        logging_servicer, logging_server)
    logging_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url='localhost:%s' % logging_port))

    control_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url=self._control_address))

    p = subprocess.Popen(
        self._worker_command_line,
        shell=True,
        env=dict(
            os.environ,
            CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
            LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor))
    try:
      p.wait()
      if p.returncode:
        raise RuntimeError(
            'Worker subprocess exited with return code %s' % p.returncode)
    finally:
      if p.poll() is None:
        p.kill()
      logging_server.stop(0)


class BeamJob(threading.Thread):
  """This class handles running and managing a single pipeline.

    The current state of the pipeline is available as self.state.
    """

  def __init__(self,
               job_id,
               pipeline_options,
               pipeline_proto,
               provision_info):
    super(BeamJob, self).__init__()
    self._job_id = job_id
    self._pipeline_options = pipeline_options
    self._pipeline_proto = pipeline_proto
    self._provision_info = provision_info
    self._state = None
    self._state_queues = []
    self._log_queues = []
    self.state = beam_job_api_pb2.JobState.STARTING
    self.daemon = True

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, new_state):
    # Inform consumers of the new state.
    for queue in self._state_queues:
      queue.put(new_state)
    self._state = new_state

  def run(self):
    with JobLogHandler(self._log_queues):
      try:
        fn_api_runner.FnApiRunner(
            provision_info=self._provision_info).run_via_runner_api(
                self._pipeline_proto)
        logging.info('Successfully completed job.')
        self.state = beam_job_api_pb2.JobState.DONE
      except:  # pylint: disable=bare-except
        logging.exception('Error running pipeline.')
        logging.exception(traceback)
        self.state = beam_job_api_pb2.JobState.FAILED
        raise

  def cancel(self):
    if self.state not in TERMINAL_STATES:
      self.state = beam_job_api_pb2.JobState.CANCELLING
      # TODO(robertwb): Actually cancel...
      self.state = beam_job_api_pb2.JobState.CANCELLED

  def get_state_stream(self):
    # Register for any new state changes.
    state_queue = queue.Queue()
    self._state_queues.append(state_queue)

    yield self.state
    while True:
      current_state = state_queue.get(block=True)
      yield current_state
      if current_state in TERMINAL_STATES:
        break

  def get_message_stream(self):
    # Register for any new messages.
    log_queue = queue.Queue()
    self._log_queues.append(log_queue)
    self._state_queues.append(log_queue)

    current_state = self.state
    yield current_state
    while current_state not in TERMINAL_STATES:
      msg = log_queue.get(block=True)
      yield msg
      if isinstance(msg, int):
        current_state = msg


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):

  def Logging(self, log_bundles, context=None):
    for log_bundle in log_bundles:
      for log_entry in log_bundle.log_entries:
        logging.info('Worker: %s', str(log_entry).replace('\n', ' '))
    return iter([])


class JobLogHandler(logging.Handler):
  """Captures logs to be returned via the Beam Job API.

    Enabled via the with statement."""

  # Mapping from logging levels to LogEntry levels.
  LOG_LEVEL_MAP = {
      logging.FATAL: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.CRITICAL: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.ERROR: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.WARNING: beam_job_api_pb2.JobMessage.JOB_MESSAGE_WARNING,
      logging.INFO: beam_job_api_pb2.JobMessage.JOB_MESSAGE_BASIC,
      logging.DEBUG: beam_job_api_pb2.JobMessage.JOB_MESSAGE_DEBUG,
  }

  def __init__(self, log_queues):
    super(JobLogHandler, self).__init__()
    self._last_id = 0
    self._logged_thread = None
    self._log_queues = log_queues

  def __enter__(self):
    # Remember the current thread to demultiplex the logs of concurrently
    # running pipelines (as Python log handlers are global).
    self._logged_thread = threading.current_thread()
    logging.getLogger().addHandler(self)

  def __exit__(self, *args):
    self._logged_thread = None
    self.close()

  def _next_id(self):
    self._last_id += 1
    return str(self._last_id)

  def emit(self, record):
    if self._logged_thread is threading.current_thread():
      msg = beam_job_api_pb2.JobMessage(
          message_id=self._next_id(),
          time=time.strftime('%Y-%m-%d %H:%M:%S.',
                             time.localtime(record.created)),
          importance=self.LOG_LEVEL_MAP[record.levelno],
          message_text=self.format(record))

      # Inform all message consumers.
      for queue in self._log_queues:
        queue.put(msg)
