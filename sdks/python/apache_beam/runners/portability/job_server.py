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
import signal
import socket
import sys
import time
from subprocess import Popen
from subprocess import check_output
from threading import Lock


class DockerizedJobServer(object):
  """
   Spins up the JobServer in a docker container for local execution
  """

  def __init__(self, job_host="localhost",
               job_port=None,
               artifact_port=None,
               expansion_port=None,
               harness_port_range=(8100, 8200),
               max_connection_retries=5):
    self.job_host = job_host
    self.job_port = job_port
    self.expansion_port = expansion_port
    self.artifact_port = artifact_port
    self.harness_port_range = harness_port_range
    self.max_connection_retries = max_connection_retries
    self.docker_process = None
    self.process_lock = Lock()

  def start(self):
    # TODO This is hardcoded to Flink at the moment but should be changed
    job_server_image_name = os.environ['USER'] + \
        "-docker-apache.bintray.io/beam/flink-job-server:latest"
    docker_path = check_output(['which', 'docker']).strip()
    cmd = ["docker", "run",
           # We mount the docker binary and socket to be able to spin up
           # "sibling" containers for the SDK harness.
           "-v", ':'.join([docker_path, "/bin/docker"]),
           "-v", "/var/run/docker.sock:/var/run/docker.sock"]

    self.job_port, self.artifact_port, self.expansion_port = \
      DockerizedJobServer._pick_port(self.job_port,
                                     self.artifact_port,
                                     self.expansion_port)

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
    cmd += args

    logging.debug("Starting container with %s", cmd)
    try:
      self.docker_process = Popen(cmd)
      atexit.register(self.stop)
      signal.signal(signal.SIGINT, self.stop)
    except:  # pylint:disable=bare-except
      logging.exception("Error bringing up container")
      self.stop()

    return "{}:{}".format(self.job_host, self.job_port)

  def stop(self):
    with self.process_lock:
      if not self.docker_process:
        return
      num_retries = 0
      while self.docker_process.poll() is None and \
              num_retries < self.max_connection_retries:
        logging.debug("Sending SIGINT to job_server container")
        self.docker_process.send_signal(signal.SIGINT)
        num_retries += 1
        time.sleep(1)
      if self.docker_process.poll is None:
        self.docker_process.kill()

  @staticmethod
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
