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

import atexit
import shutil
import signal
import tempfile
import threading

import grpc

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners.portability import local_job_service
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version


class JobServer(object):
  def start(self):
    """Starts this JobServer, returning a grpc service to which to submit jobs.
    """
    raise NotImplementedError(type(self))

  def stop(self):
    """Stops this job server."""
    raise NotImplementedError(type(self))


class ExternalJobServer(JobServer):
  def __init__(self, endpoint, timeout=None):
    self._endpoint = endpoint
    self._timeout = timeout

  def start(self) -> beam_job_api_pb2_grpc.JobServiceStub:
    channel = grpc.insecure_channel(self._endpoint)
    grpc.channel_ready_future(channel).result(timeout=self._timeout)
    return beam_job_api_pb2_grpc.JobServiceStub(channel)

  def stop(self):
    pass


class EmbeddedJobServer(JobServer):
  def start(self) -> 'local_job_service.LocalJobServicer':
    return local_job_service.LocalJobServicer()

  def stop(self):
    pass


class StopOnExitJobServer(JobServer):
  """Wraps a JobServer such that its stop will automatically be called on exit.
  """
  def __init__(self, job_server):
    self._lock = threading.Lock()
    self._job_server = job_server
    self._started = False

  def start(self):
    with self._lock:
      if not self._started:
        self._endpoint = self._job_server.start()
        self._started = True
        atexit.register(self.stop)
        signal.signal(signal.SIGINT, self.stop)
    return self._endpoint

  def stop(self):
    with self._lock:
      if self._started:
        self._job_server.stop()
        self._started = False


class SubprocessJobServer(JobServer):
  """An abstract base class for JobServers run as an external process."""
  def __init__(self):
    self._local_temp_root = None
    self._server = None

  def subprocess_cmd_and_endpoint(self):
    raise NotImplementedError(type(self))

  def start(self):
    if self._server is None:
      self._local_temp_root = tempfile.mkdtemp(prefix='beam-temp')
      cmd, endpoint = self.subprocess_cmd_and_endpoint()
      port = int(endpoint.split(':')[-1])
      self._server = subprocess_server.SubprocessServer(
          beam_job_api_pb2_grpc.JobServiceStub, cmd, port=port)
    return self._server.start()

  def stop(self):
    if self._local_temp_root:
      shutil.rmtree(self._local_temp_root)
      self._local_temp_root = None
    return self._server.stop()

  def local_temp_dir(self, **kwargs):
    return tempfile.mkdtemp(dir=self._local_temp_root, **kwargs)


class JavaJarJobServer(SubprocessJobServer):
  def __init__(self, options):
    super().__init__()
    options = options.view_as(pipeline_options.JobServerOptions)
    self._job_port = options.job_port
    self._artifact_port = options.artifact_port
    self._expansion_port = options.expansion_port
    self._artifacts_dir = options.artifacts_dir
    self._java_launcher = options.job_server_java_launcher
    self._jvm_properties = options.job_server_jvm_properties
    self._jar_cache_dir = options.jar_cache_dir

  def java_arguments(
      self,
      job_port,
      artifact_port,
      expansion_port,
      artifacts_dir):
    raise NotImplementedError(type(self))

  def path_to_jar(self):
    raise NotImplementedError(type(self))

  @staticmethod
  def path_to_beam_jar(gradle_target, artifact_id=None):
    return subprocess_server.JavaJarServer.path_to_beam_jar(
        gradle_target, artifact_id=artifact_id)

  @staticmethod
  def local_jar(url, jar_cache_dir=None):
    return subprocess_server.JavaJarServer.local_jar(url, jar_cache_dir)

  def subprocess_cmd_and_endpoint(self):
    jar_path = self.local_jar(self.path_to_jar(), self._jar_cache_dir)
    artifacts_dir = (
        self._artifacts_dir if self._artifacts_dir else self.local_temp_dir(
            prefix='artifacts'))
    job_port, = subprocess_server.pick_port(self._job_port)
    subprocess_cmd = [self._java_launcher, '-jar'
                      ] + self._jvm_properties + [jar_path] + list(
                          self.java_arguments(
                              job_port,
                              self._artifact_port,
                              self._expansion_port,
                              artifacts_dir))
    return (subprocess_cmd, 'localhost:%s' % job_port)
