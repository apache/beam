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

import atexit
import os
import shutil
import signal
import subprocess
import sys
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

  def start(self):
    # type: () -> beam_job_api_pb2_grpc.JobServiceStub
    channel = grpc.insecure_channel(self._endpoint)
    grpc.channel_ready_future(channel).result(timeout=self._timeout)
    return beam_job_api_pb2_grpc.JobServiceStub(channel)

  def stop(self):
    pass


class EmbeddedJobServer(JobServer):
  def start(self):
    # type: () -> local_job_service.LocalJobServicer
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

  MAVEN_REPOSITORY = 'https://repo.maven.apache.org/maven2/org/apache/beam'
  JAR_CACHE = os.path.expanduser("~/.apache_beam/cache")

  def __init__(self, options):
    super(JavaJarJobServer, self).__init__()
    options = options.view_as(pipeline_options.JobServerOptions)
    self._job_port = options.job_port
    self._artifact_port = options.artifact_port
    self._expansion_port = options.expansion_port
    self._artifacts_dir = options.artifacts_dir

  def java_arguments(
      self, job_port, artifact_port, expansion_port, artifacts_dir):
    raise NotImplementedError(type(self))

  def path_to_jar(self):
    raise NotImplementedError(type(self))

  @staticmethod
  def path_to_beam_jar(gradle_target):
    return subprocess_server.JavaJarServer.path_to_beam_jar(gradle_target)

  @staticmethod
  def local_jar(url):
    return subprocess_server.JavaJarServer.local_jar(url)

  def subprocess_cmd_and_endpoint(self):
    jar_path = self.local_jar(self.path_to_jar())
    artifacts_dir = (
        self._artifacts_dir if self._artifacts_dir else self.local_temp_dir(
            prefix='artifacts'))
    job_port, = subprocess_server.pick_port(self._job_port)
    return (['java', '-jar', jar_path] + list(
        self.java_arguments(
            job_port, self._artifact_port, self._expansion_port,
            artifacts_dir)),
            'localhost:%s' % job_port)


class DockerizedJobServer(SubprocessJobServer):
  """
  Spins up the JobServer in a docker container for local execution.
  """
  def __init__(
      self,
      job_host="localhost",
      job_port=None,
      artifact_port=None,
      expansion_port=None,
      harness_port_range=(8100, 8200),
      max_connection_retries=5):
    super(DockerizedJobServer, self).__init__()
    self.job_host = job_host
    self.job_port = job_port
    self.expansion_port = expansion_port
    self.artifact_port = artifact_port
    self.harness_port_range = harness_port_range
    self.max_connection_retries = max_connection_retries

  def subprocess_cmd_and_endpoint(self):
    # TODO This is hardcoded to Flink at the moment but should be changed
    job_server_image_name = "apachebeam/flink1.9_job_server:latest"
    docker_path = subprocess.check_output(['which',
                                           'docker']).strip().decode('utf-8')
    cmd = [
        "docker",
        "run",
        # We mount the docker binary and socket to be able to spin up
        # "sibling" containers for the SDK harness.
        "-v",
        ':'.join([docker_path, "/bin/docker"]),
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock"
    ]

    self.job_port, self.artifact_port, self.expansion_port = (
        subprocess_server.pick_port(
            self.job_port, self.artifact_port, self.expansion_port))

    args = [
        '--job-host',
        self.job_host,
        '--job-port',
        str(self.job_port),
        '--artifact-port',
        str(self.artifact_port),
        '--expansion-port',
        str(self.expansion_port)
    ]

    if sys.platform == "darwin":
      # Docker-for-Mac doesn't support host networking, so we need to explictly
      # publish ports from the Docker container to be able to connect to it.
      # Also, all other containers need to be aware that they run Docker-on-Mac
      # to connect against the internal Docker-for-Mac address.
      cmd += ["-e", "DOCKER_MAC_CONTAINER=1"]
      cmd += ["-p", "{}:{}".format(self.job_port, self.job_port)]
      cmd += ["-p", "{}:{}".format(self.artifact_port, self.artifact_port)]
      cmd += ["-p", "{}:{}".format(self.expansion_port, self.expansion_port)]
      cmd += [
          "-p",
          "{0}-{1}:{0}-{1}".format(
              self.harness_port_range[0], self.harness_port_range[1])
      ]
    else:
      # This shouldn't be set for MacOS because it detroys port forwardings,
      # even though host networking is not supported on MacOS.
      cmd.append("--network=host")

    cmd.append(job_server_image_name)

    return cmd + args, '%s:%s' % (self.job_host, self.job_port)
