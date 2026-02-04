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

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path

import grpc

from apache_beam.utils import subprocess_server

_LOGGER = logging.getLogger(__name__)

_COMMAND_POSSIBLE_VALUES = ['up', 'down', 'ps']

_EXPANSION_SERVICE_LAUNCHER_JAR = ':sdks:java:transform-service:app:build'


def is_docker_compose_v2_available():
  cmd = ['docker', 'compose', 'version']

  try:
    subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  except:  # pylint: disable=bare-except
    return False

  return True


class TransformServiceLauncher(object):
  _DEFAULT_PROJECT_NAME = 'apache.beam.transform.service'
  _DEFAULT_START_WAIT_TIMEOUT = 50000

  _launchers = {}  # type: ignore

  # Maintaining a static list of launchers to prevent temporary resources
  # from being created unnecessarily.
  def __new__(cls, project_name, port, beam_version=None, user_agent=None):
    if project_name not in TransformServiceLauncher._launchers:
      TransformServiceLauncher._launchers[project_name] = super(
          TransformServiceLauncher, cls).__new__(cls)
    return TransformServiceLauncher._launchers[project_name]

  def __init__(self, project_name, port, beam_version=None, user_agent=None):
    logging.info('Initializing the Beam Transform Service %s.' % project_name)

    self._project_name = project_name
    self._port = port
    self._address = 'localhost:' + str(self._port)

    self._launcher_lock = threading.RLock()

    # Setting up Docker Compose configuration.

    # We use Docker Compose project name as the name of the temporary directory
    # to isolate different transform service instances that may be running in
    # the same machine.

    temp_dir = os.path.join(tempfile.gettempdir(), project_name)
    if not os.path.exists(temp_dir):
      os.mkdir(temp_dir)

    # Get the jar with configs
    path_to_local_jar = subprocess_server.JavaJarServer.local_jar(
        subprocess_server.JavaJarServer.path_to_beam_jar(
            _EXPANSION_SERVICE_LAUNCHER_JAR),
        user_agent=user_agent)

    with zipfile.ZipFile(path_to_local_jar) as launcher_jar:
      launcher_jar.extract('docker-compose.yml', path=temp_dir)
      launcher_jar.extract('.env', path=temp_dir)

    compose_file = os.path.join(temp_dir, 'docker-compose.yml')

    # Creating the credentials volume.
    credentials_dir = os.path.join(temp_dir, 'credentials_dir')
    if not os.path.exists(credentials_dir):
      os.mkdir(credentials_dir)

    logging.info('Copying the Google Application Default Credentials file.')

    is_windows = 'windows' in os.name.lower()
    application_default_path_suffix = (
        '\\gcloud\\application_default_credentials.json' if is_windows else
        '.config/gcloud/application_default_credentials.json')
    application_default_path_file = os.path.join(
        str(Path.home()), application_default_path_suffix)
    application_default_path_copied = os.path.join(
        credentials_dir, 'application_default_credentials.json')

    if os.path.exists(application_default_path_file):
      shutil.copyfile(
          application_default_path_file, application_default_path_copied)
    else:
      logging.info(
          'GCP credentials will not be available for the transform service '
          'since could not find the Google Cloud application default '
          'credentials file at the expected location %s.' %
          application_default_path_file)

    # Creating the dependencies volume.
    dependencies_dir = os.path.join(temp_dir, 'dependencies_dir')
    if not os.path.exists(dependencies_dir):
      os.mkdir(dependencies_dir)

    self._environmental_variables = {}
    self._environmental_variables['CREDENTIALS_VOLUME'] = credentials_dir
    self._environmental_variables['DEPENDENCIES_VOLUME'] = dependencies_dir
    self._environmental_variables['TRANSFORM_SERVICE_PORT'] = str(port)
    self._environmental_variables['BEAM_VERSION'] = beam_version

    # Setting an empty requirements file
    requirements_file_name = os.path.join(dependencies_dir, 'requirements.txt')
    with open(requirements_file_name, 'w') as _:
      pass
    self._environmental_variables['PYTHON_REQUIREMENTS_FILE_NAME'] = (
        'requirements.txt')

    # Building docker compose start command
    if is_docker_compose_v2_available():
      self._docker_compose_start_command_prefix = ['docker', 'compose']
    else:
      self._docker_compose_start_command_prefix = ['docker-compose']

    self._docker_compose_start_command_prefix.append('-p')
    self._docker_compose_start_command_prefix.append(project_name)
    self._docker_compose_start_command_prefix.append('-f')
    self._docker_compose_start_command_prefix.append(compose_file)

  def _get_channel(self):
    channel_options = [("grpc.max_receive_message_length", -1),
                       ("grpc.max_send_message_length", -1)]
    if hasattr(grpc, 'local_channel_credentials'):
      # TODO: update this to support secure non-local channels.
      return grpc.secure_channel(
          self._address,
          grpc.local_channel_credentials(),
          options=channel_options)
    else:
      return grpc.insecure_channel(self._address, options=channel_options)

  def __enter__(self):
    self.start()
    self.wait_till_up(-1)

    self._channel = self._get_channel()

    from apache_beam import external
    return external.ExpansionAndArtifactRetrievalStub(self._channel.__enter__())

  def __exit__(self, *args):
    self.shutdown()
    self._channel.__exit__(*args)

  def _run_docker_compose_command(self, command, output_override=None):
    cmd = []
    cmd.extend(self._docker_compose_start_command_prefix)
    cmd.extend(command)

    myenv = os.environ.copy()
    myenv.update(self._environmental_variables)

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=myenv)
    std_out, _ = process.communicate()

    if output_override:
      output_override.write(std_out)
    else:
      print(std_out.decode(errors='backslashreplace'))

  def start(self):
    with self._launcher_lock:
      self._run_docker_compose_command(['up', '-d'])

  def shutdown(self):
    with self._launcher_lock:
      self._run_docker_compose_command(['down'])

  def status(self):
    with self._launcher_lock:
      self._run_docker_compose_command(['ps'])

  def wait_till_up(self, timeout_ms):
    channel = self._get_channel()

    timeout_ms = (
        TransformServiceLauncher._DEFAULT_START_WAIT_TIMEOUT
        if timeout_ms <= 0 else timeout_ms)

    # Waiting till the service is up.
    channel_ready = grpc.channel_ready_future(channel)
    wait_secs = .1
    start_time = time.time()
    while True:
      if (time.time() - start_time) * 1000 > timeout_ms > 0:
        raise ValueError(
            'Transform service did not start in %s seconds.' %
            (timeout_ms / 1000))
      try:
        channel_ready.result(timeout=wait_secs)
        break
      except (grpc.FutureTimeoutError, grpc.RpcError):
        wait_secs *= 1.2
        logging.log(
            logging.WARNING if wait_secs > 1 else logging.DEBUG,
            'Waiting for the transform service to be ready at %s.',
            self._address)

    logging.info('Transform service ' + self._project_name + ' started.')

  def _get_status(self):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    self._run_docker_compose_command(['ps'], tmp)
    tmp.close()
    return tmp.name


def main(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('--project_name', help='Docker Compose project name.')
  parser.add_argument(
      '--command',
      required=True,
      choices=_COMMAND_POSSIBLE_VALUES,
      help='Command to run. Possible values are ' +
      ', '.join(_COMMAND_POSSIBLE_VALUES))
  parser.add_argument(
      '--port',
      type=int,
      default=-1,
      help='External visible port of the transform service.')
  parser.add_argument(
      '--beam_version',
      required=True,
      help='Beam version of the expansion service containers to be used.')

  known_args, _ = parser.parse_known_args(argv)

  project_name = (
      TransformServiceLauncher._DEFAULT_PROJECT_NAME
      if known_args.project_name is None else known_args.project_name)
  logging.info(
      'Starting the Beam Transform Service at %s.' % (
          'the default port' if known_args.port < 0 else
          (' port ' + str(known_args.port))))
  launcher = TransformServiceLauncher(
      project_name, known_args.port, known_args.beam_version)

  if known_args.command == 'up':
    launcher.start()
    launcher.wait_till_up(-1)
  elif known_args.command == 'down':
    launcher.shutdown()
  elif known_args.command == 'ps':
    launcher.status()
  else:
    raise ValueError(
        'Unknown command %s possible values are %s' %
        (known_args.command, ', '.join(_COMMAND_POSSIBLE_VALUES)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv)
