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
from builtins import object

import grpc
from google.protobuf import text_format

from apache_beam.metrics import monitoring_infos
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import abstract_job_service
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import fn_api_runner
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

_LOGGER = logging.getLogger(__name__)


class LocalJobServicer(abstract_job_service.AbstractJobServiceServicer):
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
    super(LocalJobServicer, self).__init__()
    self._cleanup_staging_dir = staging_dir is None
    self._staging_dir = staging_dir or tempfile.mkdtemp()
    self._artifact_service = artifact_service.BeamFilesystemArtifactService(
        self._staging_dir)
    self._artifact_staging_endpoint = None

  def create_beam_job(self, preparation_id, job_name, pipeline, options):
    # TODO(angoenka): Pass an appropriate staging_session_token. The token can
    # be obtained in PutArtifactResponse from JobService
    if not self._artifact_staging_endpoint:
      # The front-end didn't try to stage anything, but the worker may
      # request what's here so we should at least store an empty manifest.
      self._artifact_service.CommitManifest(
          beam_artifact_api_pb2.CommitManifestRequest(
              staging_session_token=preparation_id,
              manifest=beam_artifact_api_pb2.Manifest()))
    provision_info = fn_api_runner.ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            job_id=preparation_id,
            job_name=job_name,
            pipeline_options=options,
            retrieval_token=self._artifact_service.retrieval_token(
                preparation_id)),
        self._staging_dir)
    return BeamJob(
        preparation_id,
        pipeline,
        options,
        provision_info,
        self._artifact_staging_endpoint)

  def start_grpc_server(self, port=0):
    self._server = grpc.server(UnboundedThreadPoolExecutor())
    port = self._server.add_insecure_port('localhost:%d' % port)
    beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(self, self._server)
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        self._artifact_service, self._server)
    self._artifact_staging_endpoint = endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%d' % port)
    self._server.start()
    _LOGGER.info('Grpc server started on port %s', port)
    return port

  def stop(self, timeout=1):
    self._server.stop(timeout)
    if os.path.exists(self._staging_dir) and self._cleanup_staging_dir:
      shutil.rmtree(self._staging_dir, ignore_errors=True)

  def GetJobMetrics(self, request, context=None):
    if request.job_id not in self._jobs:
      raise LookupError("Job {} does not exist".format(request.job_id))

    result = self._jobs[request.job_id].result
    monitoring_info_list = []
    for mi in result._monitoring_infos_by_stage.values():
      monitoring_info_list.extend(mi)

    # Filter out system metrics
    user_monitoring_info_list = [
        x for x in monitoring_info_list
        if monitoring_infos._is_user_monitoring_info(x) or
        monitoring_infos._is_user_distribution_monitoring_info(x)
    ]

    return beam_job_api_pb2.GetJobMetricsResponse(
        metrics=beam_job_api_pb2.MetricResults(
            committed=user_monitoring_info_list))


class SubprocessSdkWorker(object):
  """Manages a SDK worker implemented as a subprocess communicating over grpc.
    """

  def __init__(self, worker_command_line, control_address, worker_id=None):
    self._worker_command_line = worker_command_line
    self._control_address = control_address
    self._worker_id = worker_id

  def run(self):
    logging_server = grpc.server(UnboundedThreadPoolExecutor())
    logging_port = logging_server.add_insecure_port('[::]:0')
    logging_server.start()
    logging_servicer = BeamFnLoggingServicer()
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        logging_servicer, logging_server)
    logging_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url='localhost:%s' % logging_port))

    control_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url=self._control_address))

    env_dict = dict(
        os.environ,
        CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
        LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor
    )
    # only add worker_id when it is set.
    if self._worker_id:
      env_dict['WORKER_ID'] = self._worker_id

    with fn_api_runner.SUBPROCESS_LOCK:
      p = subprocess.Popen(
          self._worker_command_line,
          shell=True,
          env=env_dict)
    try:
      p.wait()
      if p.returncode:
        raise RuntimeError(
            'Worker subprocess exited with return code %s' % p.returncode)
    finally:
      if p.poll() is None:
        p.kill()
      logging_server.stop(0)


class BeamJob(abstract_job_service.AbstractBeamJob):
  """This class handles running and managing a single pipeline.

    The current state of the pipeline is available as self.state.
    """

  def __init__(self,
               job_id,
               pipeline,
               options,
               provision_info,
               artifact_staging_endpoint):
    super(BeamJob, self).__init__(
        job_id, provision_info.provision_info.job_name, pipeline, options)
    self._provision_info = provision_info
    self._artifact_staging_endpoint = artifact_staging_endpoint
    self._state = None
    self._state_queues = []
    self._log_queues = []
    self.state = beam_job_api_pb2.JobState.STOPPED
    self.daemon = True
    self.result = None

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, new_state):
    # Inform consumers of the new state.
    for queue in self._state_queues:
      queue.put(new_state)
    self._state = new_state

  def get_state(self):
    return self.state

  def prepare(self):
    pass

  def artifact_staging_endpoint(self):
    return self._artifact_staging_endpoint

  def run(self):
    self.state = beam_job_api_pb2.JobState.STARTING
    self._run_thread = threading.Thread(target=self._run_job)
    self._run_thread.start()

  def _run_job(self):
    self.state = beam_job_api_pb2.JobState.RUNNING
    with JobLogHandler(self._log_queues):
      try:
        result = fn_api_runner.FnApiRunner(
            provision_info=self._provision_info).run_via_runner_api(
                self._pipeline_proto)
        _LOGGER.info('Successfully completed job.')
        self.state = beam_job_api_pb2.JobState.DONE
        self.result = result
      except:  # pylint: disable=bare-except
        _LOGGER.exception('Error running pipeline.')
        _LOGGER.exception(traceback)
        self.state = beam_job_api_pb2.JobState.FAILED
        raise

  def cancel(self):
    if not self.is_terminal_state(self.state):
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
      if self.is_terminal_state(current_state):
        break

  def get_message_stream(self):
    # Register for any new messages.
    log_queue = queue.Queue()
    self._log_queues.append(log_queue)
    self._state_queues.append(log_queue)

    current_state = self.state
    yield current_state
    while not self.is_terminal_state(current_state):
      msg = log_queue.get(block=True)
      yield msg
      if isinstance(msg, int):
        current_state = msg


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):

  def Logging(self, log_bundles, context=None):
    for log_bundle in log_bundles:
      for log_entry in log_bundle.log_entries:
        _LOGGER.info('Worker: %s', str(log_entry).replace('\n', ' '))
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
