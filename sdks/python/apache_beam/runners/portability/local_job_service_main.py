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

"""Starts a service for running portable beam pipelines.

The basic usage is simply

    python -m apache_beam.runners.portability.local_job_service_main

Many other options are also supported, such as starting in the background or
passing in a lockfile to ensure that only one copy of the service is running
at a time.  Pass --help to see them all.
"""

import argparse
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time

from apache_beam.runners.portability import local_job_service

_LOGGER = logging.getLogger(__name__)


def run(argv):
  if argv[0] == __file__:
    argv = argv[1:]
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-p',
      '--port',
      '--job_port',
      type=int,
      default=0,
      help='port on which to serve the job api')
  parser.add_argument('--staging_dir')
  parser.add_argument(
      '--pid_file', help='File in which to store the process id of the server.')
  parser.add_argument(
      '--port_file', help='File in which to store the port of the server.')
  parser.add_argument(
      '--background',
      action='store_true',
      help='Start the server up as a background process.'
      ' Will fail if pid_file already exists, unless --stop is also specified.')
  parser.add_argument(
      '--stderr_file',
      help='Where to write stderr (if not specified, merged with stdout).')
  parser.add_argument(
      '--stdout_file', help='Where to write stdout for background job service.')
  parser.add_argument(
      '--stop',
      action='store_true',
      help='Stop the existing process, if any, specified in pid_file.'
      ' Will not start up a new service unless --background is specified.')
  options = parser.parse_args(argv)

  if options.stop:
    if not options.pid_file:
      raise RuntimeError('--pid_file must be specified with --stop')
    if os.path.exists(options.pid_file):
      with open(options.pid_file) as fin:
        pid = int(fin.read())
      print('Killing process at', pid)
      try:
        os.kill(pid, signal.SIGTERM)
      except Exception:
        print('Process', pid, 'already killed.')
      os.unlink(options.pid_file)
    else:
      print('Process id file', options.pid_file, 'already removed.')
    if not options.background:
      return

  if options.background:
    if not options.pid_file:
      raise RuntimeError('--pid_file must be specified with --start')
    if options.stop:
      argv.remove('--stop')
    argv.remove('--background')
    if not options.port_file:
      options.port_file = os.path.splitext(options.pid_file)[0] + '.port'
      argv.append('--port_file')
      argv.append(options.port_file)

    if not options.stdout_file:
      raise RuntimeError('--stdout_file must be specified with --background')
    os.makedirs(pathlib.PurePath(options.stdout_file).parent, exist_ok=True)
    stdout_dest = open(options.stdout_file, mode='w')

    if options.stderr_file:
      os.makedirs(pathlib.PurePath(options.stderr_file).parent, exist_ok=True)
      stderr_dest = open(options.stderr_file, mode='w')
    else:
      stderr_dest = subprocess.STDOUT

    subprocess.Popen([
        sys.executable,
        '-m',
        'apache_beam.runners.portability.local_job_service_main'
    ] + argv,
                     stderr=stderr_dest,
                     stdout=stdout_dest)
    print('Waiting for server to start up...')
    while not os.path.exists(options.port_file):
      time.sleep(.1)
    with open(options.port_file) as fin:
      port = fin.read()
    print('Server started at port', port)
    return

  if options.pid_file:
    print('Writing process id to', options.pid_file)
    os.makedirs(pathlib.PurePath(options.pid_file).parent, exist_ok=True)
    fd = os.open(options.pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
    with os.fdopen(fd, 'w') as fout:
      fout.write(str(os.getpid()))
  try:
    job_servicer = local_job_service.LocalJobServicer(options.staging_dir)
    port = job_servicer.start_grpc_server(options.port)
    try:
      if options.port_file:
        print('Writing port to', options.port_file)
        os.makedirs(pathlib.PurePath(options.port_file).parent, exist_ok=True)
        with open(options.port_file + '.tmp', 'w') as fout:
          fout.write(str(port))
        os.rename(options.port_file + '.tmp', options.port_file)
      serve("Listening for beam jobs on port %d." % port)
    finally:
      job_servicer.stop()
  finally:
    if options.pid_file and os.path.exists(options.pid_file):
      os.unlink(options.pid_file)
    if options.port_file and os.path.exists(options.port_file):
      os.unlink(options.port_file)


def serve(msg):
  logging_delay = 30
  while True:
    _LOGGER.info(msg)
    time.sleep(logging_delay)
    logging_delay *= 1.25


if __name__ == '__main__':
  signal.signal(signal.SIGTERM, lambda *args: sys.exit(0))
  logging.basicConfig()
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
