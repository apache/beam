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

import contextlib
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time

import grpc
from future.moves.urllib.error import URLError
from future.moves.urllib.request import urlopen

from apache_beam.version import __version__ as beam_version

_LOGGER = logging.getLogger(__name__)


class SubprocessServer(object):
  """An abstract base class for running GRPC Servers as an external process.

  This class acts as a context which will start up a server, provides a stub
  to connect to it, and then shuts the server down.  For example::

      with SubprocessServer(GrpcStubClass, [executable, arg, ...]) as stub:
          stub.CallService(...)
  """
  def __init__(self, stub_class, cmd, port=None):
    """Creates the server object.

    :param stub_class: the auto-generated GRPC client stub class used for
        connecting to the GRPC service
    :param cmd: command (including arguments) for starting up the server,
        suitable for passing to `subprocess.POpen`.
    :param port: (optional) the port at which the subprocess will serve its
        service.  If not given, one will be randomly chosen and the special
        string "{{PORT}}" will be substituted in the command line arguments
        with the chosen port.
    """
    self._process_lock = threading.RLock()
    self._process = None
    self._stub_class = stub_class
    self._cmd = [str(arg) for arg in cmd]
    self._port = port

  def __enter__(self):
    return self.start()

  def __exit__(self, *unused_args):
    self.stop()

  def start(self):
    try:
      endpoint = self.start_process()
      wait_secs = .1
      channel_options = [("grpc.max_receive_message_length", -1),
                         ("grpc.max_send_message_length", -1)]
      channel = grpc.insecure_channel(endpoint, options=channel_options)
      channel_ready = grpc.channel_ready_future(channel)
      while True:
        if self._process is not None and self._process.poll() is not None:
          _LOGGER.error("Starting job service with %s", self._process.args)
          raise RuntimeError(
              'Service failed to start up with error %s' % self._process.poll())
        try:
          channel_ready.result(timeout=wait_secs)
          break
        except (grpc.FutureTimeoutError, grpc.RpcError):
          wait_secs *= 1.2
          logging.log(
              logging.WARNING if wait_secs > 1 else logging.DEBUG,
              'Waiting for grpc channel to be ready at %s.',
              endpoint)
      return self._stub_class(channel)
    except:  # pylint: disable=bare-except
      _LOGGER.exception("Error bringing up service")
      self.stop()
      raise

  def start_process(self):
    with self._process_lock:
      if self._process:
        self.stop()
      if self._port:
        port = self._port
        cmd = self._cmd
      else:
        port, = pick_port(None)
        cmd = [arg.replace('{{PORT}}', str(port)) for arg in self._cmd]
      endpoint = 'localhost:%s' % port
      _LOGGER.info("Starting service with %s", str(cmd).replace("',", "'"))
      self._process = subprocess.Popen(
          cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

      # Emit the output of this command as info level logging.
      def log_stdout():
        line = self._process.stdout.readline()
        while line:
          # Remove newline via rstrip() to not print an empty line
          _LOGGER.info(line.rstrip())
          line = self._process.stdout.readline()

      t = threading.Thread(target=log_stdout)
      t.daemon = True
      t.start()
      return endpoint

  def stop(self):
    self.stop_process()

  def stop_process(self):
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

  def local_temp_dir(self, **kwargs):
    return tempfile.mkdtemp(dir=self._local_temp_root, **kwargs)


class JavaJarServer(SubprocessServer):

  APACHE_REPOSITORY = 'https://repo.maven.apache.org/maven2'
  BEAM_GROUP_ID = 'org.apache.beam'
  JAR_CACHE = os.path.expanduser("~/.apache_beam/cache/jars")

  _BEAM_SERVICES = type(
      'local', (threading.local, ),
      dict(__init__=lambda self: setattr(self, 'replacements', {})))()

  def __init__(self, stub_class, path_to_jar, java_arguments):
    super(JavaJarServer, self).__init__(
        stub_class, ['java', '-jar', path_to_jar] + list(java_arguments))
    self._existing_service = path_to_jar if _is_service_endpoint(
        path_to_jar) else None

  def start_process(self):
    if self._existing_service:
      return self._existing_service
    else:
      return super(JavaJarServer, self).start_process()

  def stop_process(self):
    if self._existing_service:
      pass
    else:
      return super(JavaJarServer, self).stop_process()

  @classmethod
  def jar_name(cls, artifact_id, version, classifier=None, appendix=None):
    return '-'.join(
        filter(None, [artifact_id, appendix, version, classifier])) + '.jar'

  @classmethod
  def path_to_maven_jar(
      cls,
      artifact_id,
      group_id,
      version,
      repository=APACHE_REPOSITORY,
      classifier=None,
      appendix=None):
    return '/'.join([
        repository,
        group_id.replace('.', '/'),
        artifact_id,
        version,
        cls.jar_name(artifact_id, version, classifier, appendix)
    ])

  @classmethod
  def path_to_beam_jar(cls, gradle_target, appendix=None, version=beam_version):
    if gradle_target in cls._BEAM_SERVICES.replacements:
      return cls._BEAM_SERVICES.replacements[gradle_target]

    gradle_package = gradle_target.strip(':').rsplit(':', 1)[0]
    artifact_id = 'beam-' + gradle_package.replace(':', '-')
    project_root = os.path.sep.join(
        os.path.abspath(__file__).split(os.path.sep)[:-5])
    local_path = os.path.join(
        project_root,
        gradle_package.replace(':', os.path.sep),
        'build',
        'libs',
        cls.jar_name(
            artifact_id,
            version.replace('.dev', ''),
            classifier='SNAPSHOT',
            appendix=appendix))
    if os.path.exists(local_path):
      _LOGGER.info('Using pre-built snapshot at %s', local_path)
      return local_path
    elif '.dev' in version:
      # TODO: Attempt to use nightly snapshots?
      raise RuntimeError(
          (
              '%s not found. '
              'Please build the server with \n  cd %s; ./gradlew %s') %
          (local_path, os.path.abspath(project_root), gradle_target))
    else:
      return cls.path_to_maven_jar(
          artifact_id,
          cls.BEAM_GROUP_ID,
          version,
          cls.APACHE_REPOSITORY,
          appendix=appendix)

  @classmethod
  def local_jar(cls, url, cache_dir=None):
    if cache_dir is None:
      cache_dir = cls.JAR_CACHE
    # TODO: Verify checksum?
    if _is_service_endpoint(url):
      return url
    elif os.path.exists(url):
      return url
    else:
      cached_jar = os.path.join(cache_dir, os.path.basename(url))
      if os.path.exists(cached_jar):
        _LOGGER.info('Using cached job server jar from %s' % url)
      else:
        _LOGGER.info('Downloading job server jar from %s' % url)
        if not os.path.exists(cache_dir):
          os.makedirs(cache_dir)
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

  @classmethod
  @contextlib.contextmanager
  def beam_services(cls, replacements):
    try:
      old = cls._BEAM_SERVICES.replacements
      cls._BEAM_SERVICES.replacements = dict(old, **replacements)
      yield
    finally:
      cls._BEAM_SERVICES.replacements = old


def _is_service_endpoint(path):
  return re.match(r'^[a-zA-Z0-9.-]+:\d+$', path)


def pick_port(*ports):
  """
  Returns a list of ports, same length as input ports list, but replaces
  all None or 0 ports with a random free port.
  """
  sockets = []

  def find_free_port(port):
    if port:
      return port
    else:
      s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      sockets.append(s)
      s.bind(('localhost', 0))
      _, free_port, _, _ = s.getsockname()
      return free_port

  ports = list(map(find_free_port, ports))
  # Close sockets only now to avoid the same port to be chosen twice
  for s in sockets:
    s.close()
  return ports
