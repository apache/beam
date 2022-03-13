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

import contextlib
import glob
import hashlib
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
import zipfile
from urllib.error import URLError
from urllib.request import urlopen

import grpc

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

  MAVEN_CENTRAL_REPOSITORY = 'https://repo.maven.apache.org/maven2'
  BEAM_GROUP_ID = 'org.apache.beam'
  JAR_CACHE = os.path.expanduser("~/.apache_beam/cache/jars")

  _BEAM_SERVICES = type(
      'local', (threading.local, ),
      dict(__init__=lambda self: setattr(self, 'replacements', {})))()

  def __init__(self, stub_class, path_to_jar, java_arguments, classpath=None):
    if classpath:
      # java -jar ignores the classpath, so we make a new jar that embeds
      # the requested classpath.
      path_to_jar = self.make_classpath_jar(path_to_jar, classpath)
    super().__init__(
        stub_class, ['java', '-jar', path_to_jar] + list(java_arguments))
    self._existing_service = path_to_jar if _is_service_endpoint(
        path_to_jar) else None

  def start_process(self):
    if self._existing_service:
      return self._existing_service
    else:
      if not shutil.which('java'):
        raise RuntimeError(
            'Java must be installed on this system to use this '
            'transform/runner.')
      return super().start_process()

  def stop_process(self):
    if self._existing_service:
      pass
    else:
      return super().stop_process()

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
      repository=MAVEN_CENTRAL_REPOSITORY,
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
  def path_to_beam_jar(
      cls,
      gradle_target,
      appendix=None,
      version=beam_version,
      artifact_id=None):
    if gradle_target in cls._BEAM_SERVICES.replacements:
      return cls._BEAM_SERVICES.replacements[gradle_target]

    gradle_package = gradle_target.strip(':').rsplit(':', 1)[0]
    if not artifact_id:
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
          cls.MAVEN_CENTRAL_REPOSITORY,
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

  @classmethod
  def make_classpath_jar(cls, main_jar, extra_jars, cache_dir=None):
    if cache_dir is None:
      cache_dir = cls.JAR_CACHE
    composite_jar_dir = os.path.join(cache_dir, 'composite-jars')
    os.makedirs(composite_jar_dir, exist_ok=True)
    classpath = []
    # Class-Path references from a jar must be relative, so we create
    # a relatively-addressable subdirectory with symlinks to all the
    # required jars.
    for pattern in [main_jar] + list(extra_jars):
      for path in glob.glob(pattern) or [pattern]:
        path = os.path.abspath(path)
        rel_path = hashlib.sha256(
            path.encode('utf-8')).hexdigest() + os.path.splitext(path)[1]
        classpath.append(rel_path)
        if not os.path.lexists(os.path.join(composite_jar_dir, rel_path)):
          os.symlink(path, os.path.join(composite_jar_dir, rel_path))
    # Now create a single jar that simply references the rest and has the same
    # main class as main_jar.
    composite_jar = os.path.join(
        composite_jar_dir,
        hashlib.sha256(' '.join(sorted(classpath)).encode('ascii')).hexdigest()
        + '.jar')
    if not os.path.exists(composite_jar):
      with zipfile.ZipFile(main_jar) as main:
        with main.open('META-INF/MANIFEST.MF') as manifest:
          main_class = next(
              filter(lambda line: line.startswith(b'Main-Class: '), manifest))
      with zipfile.ZipFile(composite_jar + '.tmp', 'w') as composite:
        with composite.open('META-INF/MANIFEST.MF', 'w') as manifest:
          manifest.write(b'Manifest-Version: 1.0\n')
          manifest.write(main_class)
          manifest.write(
              b'Class-Path: ' + ' '.join(classpath).encode('ascii') + b'\n')
      os.rename(composite_jar + '.tmp', composite_jar)
    return composite_jar


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
      try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      except OSError as e:
        # [Errno 97] Address family not supported by protocol
        # Likely indicates we are in an IPv6-only environment (BEAM-10618). Try
        # again with AF_INET6.
        if e.errno == 97:
          s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
          raise e

      sockets.append(s)
      s.bind(('localhost', 0))
      return s.getsockname()[1]

  ports = list(map(find_free_port, ports))
  # Close sockets only now to avoid the same port to be chosen twice
  for s in sockets:
    s.close()
  return ports
