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

"""A object to control to the Job API Co-Process
"""

# pytype: skip-file

import logging
import subprocess
import time

import grpc

from apache_beam.portability.api import beam_job_api_pb2_grpc

_LOGGER = logging.getLogger(__name__)


class DockerRPCManager(object):
  """A native co-process to start a contianer that speaks the JobApi
  """
  def __init__(self, run_command=None):
    # TODO(BEAM-2431): Change this to a docker container from a command.
    self.process = subprocess.Popen([
        'python',
        '-m',
        'apache_beam.runners.experimental.python_rpc_direct.server'
    ])

    self.channel = grpc.insecure_channel('localhost:50051')
    self.service = beam_job_api_pb2_grpc.JobServiceStub(self.channel)

    # Sleep for 2 seconds for process to start completely
    # This is just for the co-process and would be removed
    # once we migrate to docker.
    time.sleep(2)

  def __del__(self):
    """Terminate the co-process when the manager is GC'ed
    """
    _LOGGER.info('Shutting the co-process')
    self.process.terminate()
