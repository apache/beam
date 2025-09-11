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
from unittest import mock

from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability.fn_api_runner.fn_runner import ExtendedProvisionInfo
from apache_beam.runners.portability.fn_api_runner.worker_handlers import ExternalWorkerHandler
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandlerManager
from apache_beam.transforms import environments

_LOGGER = logging.getLogger(__name__)


class ExternalWorkerHandlerTest(unittest.TestCase):
  def setUp(self):
    self.prov_info = ExtendedProvisionInfo(
        beam_provision_api_pb2.ProvisionInfo(
            retrieval_token='unused-retrieval-token'))

  def test_host_from_worker_with_control_host_param(self):
    """Test that control_host parameter is used when specified."""
    # Create external payload with control_host parameter
    external_payload = beam_runner_api_pb2.ExternalPayload(
        endpoint=endpoints_pb2.ApiServiceDescriptor(url='worker:50000'),
        params={'control_host': 'custom-host'})

    # Mock the grpc_server
    mock_grpc_server = mock.MagicMock()
    mock_grpc_server.control_port = 8080
    mock_grpc_server.control_handler = mock.MagicMock()
    mock_grpc_server.data_plane_handler = mock.MagicMock()

    # Create ExternalWorkerHandler
    handler = ExternalWorkerHandler(
        external_payload=external_payload,
        state=mock.MagicMock(),
        provision_info=self.prov_info,
        grpc_server=mock_grpc_server)

    # Test that host_from_worker returns the custom host
    self.assertEqual(handler.host_from_worker(), 'custom-host')

  def test_host_from_worker_without_control_host_param(self):
    """Test that default behavior is preserved when control_host
    is not specified."""
    # Create external payload without control_host parameter
    external_payload = beam_runner_api_pb2.ExternalPayload(
        endpoint=endpoints_pb2.ApiServiceDescriptor(url='worker:50000'),
        params={})

    # Mock the grpc_server
    mock_grpc_server = mock.MagicMock()
    mock_grpc_server.control_port = 8080
    mock_grpc_server.control_handler = mock.MagicMock()
    mock_grpc_server.data_plane_handler = mock.MagicMock()

    # Create ExternalWorkerHandler
    handler = ExternalWorkerHandler(
        external_payload=external_payload,
        state=mock.MagicMock(),
        provision_info=self.prov_info,
        grpc_server=mock_grpc_server)

    # Test that host_from_worker returns default behavior (localhost or fqdn)
    result = handler.host_from_worker()
    # Should be either 'localhost' (on win32/darwin) or socket.getfqdn() result
    self.assertIsInstance(result, str)
    self.assertNotEqual(result, '')

  def test_host_from_worker_with_empty_params(self):
    """Test that default behavior is preserved when params is None."""
    # Create external payload with None params
    external_payload = beam_runner_api_pb2.ExternalPayload(
        endpoint=endpoints_pb2.ApiServiceDescriptor(url='worker:50000'),
        params=None)

    # Mock the grpc_server
    mock_grpc_server = mock.MagicMock()
    mock_grpc_server.control_port = 8080
    mock_grpc_server.control_handler = mock.MagicMock()
    mock_grpc_server.data_plane_handler = mock.MagicMock()

    # Create ExternalWorkerHandler
    handler = ExternalWorkerHandler(
        external_payload=external_payload,
        state=mock.MagicMock(),
        provision_info=self.prov_info,
        grpc_server=mock_grpc_server)

    # Test that host_from_worker returns default behavior
    result = handler.host_from_worker()
    self.assertIsInstance(result, str)
    self.assertNotEqual(result, '')

  def test_control_address_with_custom_host(self):
    """Test that control_address uses the custom host when specified."""
    # Create external payload with control_host parameter
    external_payload = beam_runner_api_pb2.ExternalPayload(
        endpoint=endpoints_pb2.ApiServiceDescriptor(url='worker:50000'),
        params={'control_host': 'docker-host'})

    # Mock the grpc_server
    mock_grpc_server = mock.MagicMock()
    mock_grpc_server.control_port = 8080
    mock_grpc_server.control_handler = mock.MagicMock()
    mock_grpc_server.data_plane_handler = mock.MagicMock()

    # Create ExternalWorkerHandler
    handler = ExternalWorkerHandler(
        external_payload=external_payload,
        state=mock.MagicMock(),
        provision_info=self.prov_info,
        grpc_server=mock_grpc_server)

    # Test that control_address includes the custom host
    self.assertEqual(handler.control_address, 'docker-host:8080')


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
