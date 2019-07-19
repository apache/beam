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

import atexit
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

import grpc
from future.moves.urllib.error import URLError
from future.moves.urllib.request import urlopen

from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners.portability import local_job_service
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
  def __init__(self, endpoint):
    self._endpoint = endpoint

  def start(self):
    channel = grpc.insecure_channel(self._endpoint)
    grpc.channel_ready_future(channel).result()
    return beam_job_api_pb2_grpc.JobServiceStub(channel)

  def stop(self):
    pass


class EmbeddedJobServer(JobServer):
  def start(self):
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
    self._process_lock = threading.RLock()
    self._process = None
    self._local_temp_root = None

  def subprocess_cmd_and_endpoint(self):
    raise NotImplementedError(type(self))

  def start(self):
    with self._process_lock:
      if self._process:
        self.stop()
      cmd, endpoint = self.subprocess_cmd_and_endpoint()
      logging.debug("Starting job service with %s", cmd)
      try:
        self._process = subprocess.Popen([str(arg) for arg in cmd])
        self._local_temp_root = tempfile.mkdtemp(prefix='beam-temp')
        wait_secs = .1
        channel = grpc.insecure_channel(endpoint)
        channel_ready = grpc.channel_ready_future(channel)
        while True:
          if self._process.poll() is not None:
            logging.error("Starting job service with %s", cmd)
            raise RuntimeError(
                'Job service failed to start up with error %s' %
                self._process.poll())
          try:
            channel_ready.result(timeout=wait_secs)
            break
          except (grpc.FutureTimeoutError, grpc._channel._Rendezvous):
            wait_secs *= 1.2
            logging.log(logging.WARNING if wait_secs > 1 else logging.DEBUG,
                        'Waiting for jobs grpc channel to be ready at %s.',
                        endpoint)
        return beam_job_api_pb2_grpc.JobServiceStub(channel)
      except:  # pylint: disable=bare-except
        logging.exception("Error bringing up job service")
        self.stop()
        raise

  def stop(self):
    with self._process_lock:
      if not self._process:
        return
      for _ in range(5):
        if self._process.poll() is not None:
          break
        logging.debug("Sending SIGINT to job_server")
        self._process.send_signal(signal.SIGINT)
        time.sleep(1)
      if self._process.poll() is None:
        self._process.kill()
      self._process = None
      if self._local_temp_root:
        shutil.rmtree(self._local_temp_root)
        self._local_temp_root = None

  def local_temp_dir(self, **kwargs):
    return tempfile.mkdtemp(dir=self._local_temp_root, **kwargs)


class JavaJarJobServer(SubprocessJobServer):

  MAVEN_REPOSITORY = 'https://repo.maven.apache.org/maven2/org/apache/beam'
  JAR_CACHE = os.path.expanduser("~/.apache_beam/cache")

  def java_arguments(self, job_port, artifacts_dir):
    raise NotImplementedError(type(self))

  def path_to_jar(self):
    raise NotImplementedError(type(self))

  @classmethod
  def path_to_gradle_target_jar(cls, target):
    gradle_package = target[:target.rindex(':')]
    jar_name = '-'.join([
        'beam', gradle_package.replace(':', '-'), beam_version + '.jar'])

    if beam_version.endswith('.dev'):
      # TODO: Attempt to use nightly snapshots?
      project_root = os.path.sep.join(__file__.split(os.path.sep)[:-6])
      dev_path = os.path.join(
          project_root,
          gradle_package.replace(':', os.path.sep),
          'build',
          'libs',
          jar_name.replace('.dev', '').replace('.jar', '-SNAPSHOT.jar'))
      if os.path.exists(dev_path):
        logging.warning(
            'Using pre-built job server snapshot at %s', dev_path)
        return dev_path
      else:
        raise RuntimeError(
            'Please build the job server with \n  cd %s; ./gradlew %s' % (
                os.path.abspath(project_root), target))
    else:
      return '/'.join([
          cls.MAVEN_REPOSITORY,
          'beam-' + gradle_package.replace(':', '-'),
          beam_version,
          jar_name])

  def subprocess_cmd_and_endpoint(self):
    jar_path = self.local_jar(self.path_to_jar())
    artifacts_dir = self.local_temp_dir(prefix='artifacts')
    job_port, = _pick_port(None)
    return (
        ['java', '-jar', jar_path] + list(
            self.java_arguments(job_port, artifacts_dir)),
        'localhost:%s' % job_port)

  def local_jar(self, url):
    # TODO: Verify checksum?
    if os.path.exists(url):
      return url
    else:
      logging.warning('Downloading job server jar from %s' % url)
      cached_jar = os.path.join(self.JAR_CACHE, os.path.basename(url))
      if not os.path.exists(cached_jar):
        if not os.path.exists(self.JAR_CACHE):
          os.makedirs(self.JAR_CACHE)
          # TODO: Clean up this cache according to some policy.
        try:
          url_read = urlopen(url)
          with open(cached_jar + '.tmp', 'wb') as jar_write:
            shutil.copyfileobj(url_read, jar_write, length=1 << 20)
          os.rename(cached_jar + '.tmp', cached_jar)
        except URLError as e:
          raise RuntimeError(
              'Unable to fetch remote job server jar at %s: %s' % (url, e))
      return cached_jar


class DockerizedJobServer(SubprocessJobServer):
  """
  Spins up the JobServer in a docker container for local execution.
  """

  def __init__(self, job_host="localhost",
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
    job_server_image_name = os.environ['USER'] + \
        "-docker-apache.bintray.io/beam/flink-job-server:latest"
    docker_path = subprocess.check_output(
        ['which', 'docker']).strip().decode('utf-8')
    cmd = ["docker", "run",
           # We mount the docker binary and socket to be able to spin up
           # "sibling" containers for the SDK harness.
           "-v", ':'.join([docker_path, "/bin/docker"]),
           "-v", "/var/run/docker.sock:/var/run/docker.sock"]

    self.job_port, self.artifact_port, self.expansion_port = _pick_port(
        self.job_port, self.artifact_port, self.expansion_port)

    args = ['--job-host', self.job_host,
            '--job-port', str(self.job_port),
            '--artifact-port', str(self.artifact_port),
            '--expansion-port', str(self.expansion_port)]

    if sys.platform == "darwin":
      # Docker-for-Mac doesn't support host networking, so we need to explictly
      # publish ports from the Docker container to be able to connect to it.
      # Also, all other containers need to be aware that they run Docker-on-Mac
      # to connect against the internal Docker-for-Mac address.
      cmd += ["-e", "DOCKER_MAC_CONTAINER=1"]
      cmd += ["-p", "{}:{}".format(self.job_port, self.job_port)]
      cmd += ["-p", "{}:{}".format(self.artifact_port, self.artifact_port)]
      cmd += ["-p", "{}:{}".format(self.expansion_port, self.expansion_port)]
      cmd += ["-p", "{0}-{1}:{0}-{1}".format(
          self.harness_port_range[0], self.harness_port_range[1])]
    else:
      # This shouldn't be set for MacOS because it detroys port forwardings,
      # even though host networking is not supported on MacOS.
      cmd.append("--network=host")

    cmd.append(job_server_image_name)

    return cmd + args, '%s:%s' % (self.job_host, self.job_port)


def _pick_port(*ports):
  """
  Returns a list of ports, same length as input ports list, but replaces
  all None or 0 ports with a random free port.
  """
  sockets = []

  def find_free_port(port):
    if port:
      return port
    else:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sockets.append(s)
      s.bind(('localhost', 0))
      _, free_port = s.getsockname()
      return free_port

  ports = list(map(find_free_port, ports))
  # Close sockets only now to avoid the same port to be chosen twice
  for s in sockets:
    s.close()
  return ports
