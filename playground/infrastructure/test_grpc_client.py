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

import uuid
from unittest.mock import AsyncMock

import pytest

from api.v1 import api_pb2
from grpc_client import GRPCClient


@pytest.fixture()
def mock_run_code(mocker):
    async_mock = AsyncMock(return_value=str(uuid.uuid4()))
    mocker.patch("grpc_client.GRPCClient.run_code", side_effect=async_mock)
    return async_mock


@pytest.fixture()
def mock_check_status(mocker):
    async_mock = AsyncMock(return_value=api_pb2.STATUS_FINISHED)
    mocker.patch("grpc_client.GRPCClient.check_status", side_effect=async_mock)
    return async_mock


@pytest.fixture()
def mock_get_run_error(mocker):
    async_mock = AsyncMock(return_value="MOCK_ERROR")
    mocker.patch("grpc_client.GRPCClient.get_run_error", side_effect=async_mock)
    return async_mock


@pytest.fixture()
def mock_get_run_output(mocker):
    async_mock = AsyncMock(return_value="MOCK_RUN_OUTPUT")
    mocker.patch("grpc_client.GRPCClient.get_run_output", side_effect=async_mock)
    return async_mock


@pytest.fixture()
def mock_get_compile_output(mocker):
    async_mock = AsyncMock(return_value="MOCK_COMPILE_OUTPUT")
    mocker.patch(
        "grpc_client.GRPCClient.get_compile_output", side_effect=async_mock)
    return async_mock


class TestGRPCClient:

    @pytest.mark.asyncio
    async def test_run_code(self, mock_run_code):
        result = await GRPCClient().run_code("", api_pb2.SDK_GO, "", [])
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_check_status(self, mock_check_status):
        result = await GRPCClient().check_status(str(uuid.uuid4()))
        assert result == api_pb2.STATUS_FINISHED

    @pytest.mark.asyncio
    async def test_get_run_error(self, mock_get_run_error):
        result = await GRPCClient().get_run_error(str(uuid.uuid4()))
        assert result == "MOCK_ERROR"

    @pytest.mark.asyncio
    async def test_get_run_output(self, mock_get_run_output):
        result = await GRPCClient().get_run_output(str(uuid.uuid4()))
        assert result == "MOCK_RUN_OUTPUT"

    @pytest.mark.asyncio
    async def test_get_compile_output(self, mock_get_compile_output):
        result = await GRPCClient().get_compile_output(str(uuid.uuid4()))
        assert result == "MOCK_COMPILE_OUTPUT"
