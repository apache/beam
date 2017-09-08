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

from concurrent import futures
import logging
import socket
import subprocess
import sys
import time
import threading
import traceback
import uuid

import grpc

from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners import runner
from apache_beam.runners.portability import fn_api_runner


TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class UniversalLocalRunner(runner.PipelineRunner):

  def __init__(self, timeout=None, use_grpc=True, use_subprocesses=True):
    super(UniversalLocalRunner, self).__init__()
    self._timeout = use_grpc
    self._use_grpc = use_grpc
    self._use_subprocesses = use_subprocesses

    self._handle = None
    self._subprocess = None

  def __del__(self):
    if self._subprocess:
      self._subprocess.kill()

  def _get_handle(self):
    if not self._handle:
      if self._use_subprocesses:
        if self._subprocess:
          # Kill the old one if it exists.
          self._subprocess.kill()
        # TODO(robertwb): Consider letting the subprocess pick one and
        # communicate it back...
        port = _pick_unused_port()
        logging.info("Starting server on port %d.", port)
        self._subprocess = subprocess.Popen([
            sys.executable,
            '-m',
            'apache_beam.runners.portability.universal_local_runner_main',
            '-p',
            str(port)])
        handle = beam_job_api_pb2_grpc.JobServiceStub(
            grpc.insecure_channel('localhost:%d' % port))
        logging.info("Waiting for server to be ready...")
        start = time.time()
        timeout = 30
        while True:
          time.sleep(0.1)
          if self._subprocess.poll() is not None:
            raise RuntimeError(
                "Subprocess terminated unexpectedly with exit code %d." %
                self._subprocess.returncode)
          elif time.time() - start > timeout:
            raise RuntimeError(
                "Pipeline timed out waiting for job service subprocess.")
          else:
            try:
              handle.GetState(
                  beam_job_api_pb2.GetJobStateRequest(job_id='[fake]'))
              break
            except grpc.RpcError as exn:
              if exn.code != grpc.StatusCode.UNAVAILABLE:
                break
        logging.info("Server ready.")
        self._handle = handle

      elif self._use_grpc:
        self._servicer = JobServicer()
        self._handle = beam_job_api_pb2_grpc.JobServiceStub(
            grpc.insecure_channel('localhost:%d' % self._servicer.start_grpc()))

      else:
        self._handle = JobServicer()

    return self._handle

  def run(self, pipeline):
    handle = self._get_handle()
    prepare_response = handle.Prepare(
        beam_job_api_pb2.PrepareJobRequest(
            job_name='job',
            pipeline=pipeline.to_runner_api()))
    run_response = handle.Run(beam_job_api_pb2.RunJobRequest(
        preparation_id=prepare_response.preparation_id))
    return PipelineResult(handle, run_response.job_id, self._timeout)


class PipelineResult(runner.PipelineResult):
  def __init__(self, handle, job_id, timeout):
    super(PipelineResult, self).__init__(beam_job_api_pb2.JobState.UNKNOWN)
    self._handle = handle
    self._job_id = job_id
    self._timeout = timeout

  def cancel(self):
    self._handle.Cancel()

  @property
  def state(self):
    runner_api_state = self._handle.GetState(
        beam_job_api_pb2.GetJobStateRequest(job_id=self._job_id)).state
    self._state = self._runner_api_state_to_pipeline_state(runner_api_state)
    return self._state

  @staticmethod
  def _runner_api_state_to_pipeline_state(runner_api_state):
    return getattr(
        runner.PipelineState,
        beam_job_api_pb2.JobState.JobStateType.Name(runner_api_state))

  @staticmethod
  def _pipeline_state_to_runner_api_state(pipeline_state):
    return beam_job_api_pb2.JobState.JobStateType.Value(pipeline_state)

  def wait_until_finish(self):
    start = time.time()
    sleep_interval = 0.01
    while self._pipeline_state_to_runner_api_state(
        self.state) not in TERMINAL_STATES:
      if self._timeout and time.time() - start > self._timeout:
        raise RuntimeError(
            "Pipeline %s timed out in state %s." % (self._job_id, self._state))
      time.sleep(sleep_interval)
    if self._state != runner.PipelineState.DONE:
      raise RuntimeError(
          "Pipeline %s failed in state %s." % (self._job_id, self._state))


class BeamJob(threading.Thread):
  def __init__(self, job_id, pipeline_options, pipeline_proto):
    super(BeamJob, self).__init__()
    self._job_id = job_id
    self._pipeline_options = pipeline_options
    self._pipeline_proto = pipeline_proto
    self.state = beam_job_api_pb2.JobState.STARTING
    self.daemon = True

  def run(self):
    try:
      fn_api_runner.FnApiRunner().run_via_runner_api(self._pipeline_proto)
      self.state = beam_job_api_pb2.JobState.DONE
    except:  # pylint: disable=bare-except
      traceback.print_exc()
      self.state = beam_job_api_pb2.JobState.FAILED

  def cancel(self):
    if self.state not in TERMINAL_STATES:
      self.state = beam_job_api_pb2.JobState.CANCELLING
      # TODO(robertwb): Actually cancel...
      self.state = beam_job_api_pb2.JobState.CANCELLED


class JobServicer(beam_job_api_pb2.JobServiceServicer):

  def __init__(self, worker_command_line=None):
    self._worker_command_line = worker_command_line
    self._jobs = {}

  def start_grpc(self, port=0):
    self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    port = self._server.add_insecure_port('localhost:%d' % port)
    beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(self, self._server)
    self._server.start()
    return port

  def Prepare(self, request, context=None):
    # For now, just use the job name as the job id.
    preparation_id = "%s-%s" % (request.job_name, uuid.uuid4())
    self._jobs[preparation_id] = BeamJob(
        preparation_id, request.pipeline_options, request.pipeline)
    return beam_job_api_pb2.PrepareJobResponse(preparation_id=preparation_id)

  def Run(self, request, context=None):
    job_id = request.preparation_id
    self._jobs[job_id].start()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetState(self, request, context=None):
    return beam_job_api_pb2.GetJobStateResponse(
        state=self._jobs[request.job_id].state)

  def Cancel(self, request, context=None):
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobRequest(
        state=self._jobs[request.job_id].state)


def _pick_unused_port():
  """Not perfect, but we have to provide a port to the subprocess."""
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  _, port = s.getsockname()
  s.close()
  return port
