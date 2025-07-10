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
import logging
import unittest

from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.runners.portability.fn_api_runner.fn_runner import ExtendedProvisionInfo
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandlerManager
from apache_beam.transforms import environments

_LOGGER = logging.getLogger(__name__)


class WorkerHandlerManagerTest(unittest.TestCase):
  def test_close_all(self):
    inprocess_env = environments.EmbeddedPythonEnvironment(
        capabilities=environments.python_sdk_capabilities(),
        artifacts=(),
        resource_hints={}).to_runner_api(None)
    envs = {
        'inprocess': inprocess_env,
    }
    prov_info = ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            retrieval_token='unused-retrieval-token'))

    manager = WorkerHandlerManager(envs, job_provision_info=prov_info)
    first_workers = manager.get_worker_handlers('inprocess', 1)
    manager.close_all()
    second_workers = manager.get_worker_handlers('inprocess', 1)
    assert len(first_workers) == len(second_workers) == 1
    assert first_workers != second_workers


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
