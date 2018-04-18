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

import functools
import logging
import os
import Queue as queue
import socket
import subprocess
import sys
import threading
import time
import traceback
import uuid
from concurrent import futures

import grpc
from google.protobuf import text_format

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner
from apache_beam.runners.portability import fn_api_runner

TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class UniversalLocalRunner(runner.PipelineRunner):
  """A BeamRunner that executes Python pipelines via the Beam Job API.

  By default, this runner executes in process but still uses GRPC to communicate
  pipeline and worker state.  It can also be configured to use inline calls
  rather than GRPC (for speed) or launch completely separate subprocesses for
  the runner and worker(s).
  """

  def __init__(
      self,
      use_grpc=True,
      use_subprocesses=False,
      runner_api_address=None,
      docker_image=None):
    if use_subprocesses and not use_grpc:
      raise ValueError("GRPC must be used with subprocesses")
    super(UniversalLocalRunner, self).__init__()
    self._use_grpc = use_grpc
    self._use_subprocesses = use_subprocesses

    self._job_service = None
    self._job_service_lock = threading.Lock()
    self._subprocess = None
    self._runner_api_address = runner_api_address
    self._docker_image = (
      docker_image
      or os.environ['USER'] + '-docker.apache.bintray.io/beam/python:latest')

  def __del__(self):
    # Best effort to not leave any dangling processes around.
    self.cleanup()

  def cleanup(self):
    if self._subprocess:
      self._subprocess.kill()
      time.sleep(0.1)
    self._subprocess = None

  def _get_job_service(self):
    with self._job_service_lock:
      if not self._job_service:
        if self._runner_api_address:
          self._job_service = beam_job_api_pb2_grpc.JobServiceStub(
              grpc.insecure_channel(self._runner_api_address))
        elif self._use_subprocesses:
          self._job_service = self._start_local_runner_subprocess_job_service()

        elif self._use_grpc:
          self._servicer = JobServicer(use_grpc=True)
          self._job_service = beam_job_api_pb2_grpc.JobServiceStub(
              grpc.insecure_channel(
                  'localhost:%d' % self._servicer.start_grpc()))

        else:
          self._job_service = JobServicer(use_grpc=False)

    return self._job_service

  def _start_local_runner_subprocess_job_service(self):
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
        str(port),
        '--worker_command_line',
        '%s -m apache_beam.runners.worker.sdk_worker_main' % sys.executable
    ])
    job_service = beam_job_api_pb2_grpc.JobServiceStub(
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
          job_service.GetState(
              beam_job_api_pb2.GetJobStateRequest(job_id='[fake]'))
          break
        except grpc.RpcError as exn:
          if exn.code != grpc.StatusCode.UNAVAILABLE:
            # We were able to contact the service for our fake state request.
            break
    logging.info("Server ready.")
    return job_service

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

    job_service = self._get_job_service()
    prepare_response = job_service.Prepare(
        beam_job_api_pb2.PrepareJobRequest(
            job_name='job',
            pipeline=proto_pipeline))
    run_response = job_service.Run(beam_job_api_pb2.RunJobRequest(
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
    return getattr(
        runner.PipelineState,
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
          "Pipeline %s failed in state %s." % (self._job_id, self._state))
    return self._state


class BeamJob(threading.Thread):
  """This class handles running and managing a single pipeline.

  The current state of the pipeline is available as self.state.
  """
  def __init__(self, job_id, pipeline_options, pipeline_proto,
               use_grpc=True, sdk_harness_factory=None):
    super(BeamJob, self).__init__()
    self._job_id = job_id
    self._pipeline_options = pipeline_options
    self._pipeline_proto = pipeline_proto
    self._use_grpc = use_grpc
    self._sdk_harness_factory = sdk_harness_factory
    self._log_queue = queue.Queue()
    self._state_change_callbacks = [
        lambda new_state: self._log_queue.put(
            beam_job_api_pb2.JobMessagesResponse(
                state_response=
                beam_job_api_pb2.GetJobStateResponse(state=new_state)))
    ]
    self._state = None
    self.state = beam_job_api_pb2.JobState.STARTING
    self.daemon = True

  def add_state_change_callback(self, f):
    self._state_change_callbacks.append(f)

  @property
  def log_queue(self):
    return self._log_queue

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, new_state):
    for state_change_callback in self._state_change_callbacks:
      state_change_callback(new_state)
    self._state = new_state

  def run(self):
    with JobLogHandler(self._log_queue):
      try:
        fn_api_runner.FnApiRunner(
            use_grpc=self._use_grpc,
            sdk_harness_factory=self._sdk_harness_factory
        ).run_via_runner_api(self._pipeline_proto)
        logging.info("Successfully completed job.")
        self.state = beam_job_api_pb2.JobState.DONE
      except:  # pylint: disable=bare-except
        logging.exception("Error running pipeline.")
        traceback.print_exc()
        self.state = beam_job_api_pb2.JobState.FAILED
        raise

  def cancel(self):
    if self.state not in TERMINAL_STATES:
      self.state = beam_job_api_pb2.JobState.CANCELLING
      # TODO(robertwb): Actually cancel...
      self.state = beam_job_api_pb2.JobState.CANCELLED


class JobServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """Servicer for the Beam Job API.

  Manages one or more pipelines, possibly concurrently.
  """
  def __init__(
      self, worker_command_line=None, use_grpc=True):
    self._worker_command_line = worker_command_line
    self._use_grpc = use_grpc or bool(worker_command_line)
    self._jobs = {}

  def start_grpc(self, port=0):
    self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    port = self._server.add_insecure_port('localhost:%d' % port)
    beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(self, self._server)
    self._server.start()
    logging.info("Grpc server started on port %s", port)
    return port

  def Prepare(self, request, context=None):
    # For now, just use the job name as the job id.
    logging.debug("Got Prepare request.")
    preparation_id = "%s-%s" % (request.job_name, uuid.uuid4())
    if self._worker_command_line:
      sdk_harness_factory = functools.partial(
          SubprocessSdkWorker, self._worker_command_line)
    else:
      sdk_harness_factory = None
    self._jobs[preparation_id] = BeamJob(
        preparation_id, request.pipeline_options, request.pipeline,
        use_grpc=self._use_grpc, sdk_harness_factory=sdk_harness_factory)
    logging.debug("Prepared job '%s' as '%s'", request.job_name, preparation_id)
    return beam_job_api_pb2.PrepareJobResponse(preparation_id=preparation_id)

  def Run(self, request, context=None):
    job_id = request.preparation_id
    logging.debug("Runing job '%s'", job_id)
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
    job = self._jobs[request.job_id]
    state_queue = queue.Queue()
    job.add_state_change_callback(lambda state: state_queue.put(state))
    try:
      current_state = state_queue.get()
    except queue.Empty:
      current_state = job.state
    yield beam_job_api_pb2.GetJobStateResponse(
        state=current_state)
    while current_state not in TERMINAL_STATES:
      current_state = state_queue.get(block=True)
      yield beam_job_api_pb2.GetJobStateResponse(
          state=current_state)

  def GetMessageStream(self, request, context=None):
    job = self._jobs[request.job_id]
    current_state = job.state
    while current_state not in TERMINAL_STATES:
      msg = job.log_queue.get(block=True)
      yield msg
      if msg.HasField('state_response'):
        current_state = msg.state_response.state
    try:
      while True:
        yield job.log_queue.get(block=False)
    except queue.Empty:
      pass


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):
  def Logging(self, log_bundles, context=None):
    for log_bundle in log_bundles:
      for log_entry in log_bundle.log_entries:
        logging.info('Worker: %s', str(log_entry).replace('\n', ' '))
    return iter([])


class SubprocessSdkWorker(object):
  """Manages a SDK worker implemented as a subprocess communicating over grpc.
  """

  def __init__(self, worker_command_line, control_address):
    self._worker_command_line = worker_command_line
    self._control_address = control_address

  def run(self):
    logging_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10))
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
        env=dict(os.environ,
                 CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
                 LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor))
    try:
      p.wait()
      if p.returncode:
        raise RuntimeError(
            "Worker subprocess exited with return code %s" % p.returncode)
    finally:
      if p.poll() is None:
        p.kill()
      logging_server.stop(0)


class JobLogHandler(logging.Handler):
  """Captures logs to be returned via the Beam Job API.

  Enabled via the with statement."""

  # Mapping from logging levels to LogEntry levels.
  LOG_LEVEL_MAP = {
      logging.FATAL: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.ERROR: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.WARNING: beam_job_api_pb2.JobMessage.JOB_MESSAGE_WARNING,
      logging.INFO: beam_job_api_pb2.JobMessage.JOB_MESSAGE_BASIC,
      logging.DEBUG: beam_job_api_pb2.JobMessage.JOB_MESSAGE_DEBUG,
  }

  def __init__(self, message_queue):
    super(JobLogHandler, self).__init__()
    self._message_queue = message_queue
    self._last_id = 0
    self._logged_thread = None

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
      self._message_queue.put(beam_job_api_pb2.JobMessagesResponse(
          message_response=beam_job_api_pb2.JobMessage(
              message_id=self._next_id(),
              time=time.strftime(
                  '%Y-%m-%d %H:%M:%S.', time.localtime(record.created)),
              importance=self.LOG_LEVEL_MAP[record.levelno],
              message_text=self.format(record))))


def _pick_unused_port():
  """Not perfect, but we have to provide a port to the subprocess."""
  # TODO(robertwb): Consider letting the subprocess communicate a choice of
  # port back.
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  _, port = s.getsockname()
  s.close()
  return port
