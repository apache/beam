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

"""
Module contains the client to communicate with GRPC test Playground server
"""
import logging
import os
import uuid
from typing import List

import grpc
import sonora.aio

from api.v1 import api_pb2_grpc, api_pb2
from config import Config
from constants import BEAM_USE_WEBGRPC_ENV_VAR_KEY, GRPC_TIMEOUT_ENV_VAR_KEY
from models import SdkEnum


class GRPCClient:
    """GRPCClient is gRPC client for sending a request to the backend."""

    def __init__(self, wait_for_ready=True):
        use_webgrpc = os.getenv(BEAM_USE_WEBGRPC_ENV_VAR_KEY, False)
        timeout = int(os.getenv(GRPC_TIMEOUT_ENV_VAR_KEY, 30))
        logging.info("grpc timeout: %d", timeout)
        if use_webgrpc:
            self._channel = sonora.aio.insecure_web_channel(Config.SERVER_ADDRESS)
        else:
            self._channel = grpc.aio.insecure_channel(Config.SERVER_ADDRESS)

        self._stub = api_pb2_grpc.PlaygroundServiceStub(self._channel)

        self._kwargs = dict(timeout=timeout)
        if wait_for_ready and not use_webgrpc:
            self._kwargs["wait_for_ready"] = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    async def run_code(self, 
        code: str,
        sdk: SdkEnum,
        pipeline_options: str,
        datasets: List[api_pb2.Dataset],
        files: List[api_pb2.SnippetFile],
        ) -> str:
        """
        Run example by his code and SDK

        Args:
            code: code of the example.
            sdk: SDK of the example.
            pipeline_options: pipeline options of the example.
            datasets: datasets of the example.

        Returns:
            pipeline_uuid: uuid of the pipeline
        """
        if sdk not in api_pb2.Sdk.values():
            sdks = api_pb2.Sdk.keys()
            sdks.remove(api_pb2.Sdk.Name(0))  # del SDK_UNSPECIFIED
            raise Exception(
                f'Incorrect sdk: must be from this pool: {", ".join(sdks)}')
        request = api_pb2.RunCodeRequest(
            code=code, sdk=sdk, pipeline_options=pipeline_options, datasets=datasets, files=files)
        response = await self._stub.RunCode(request, **self._kwargs)
        return response.pipeline_uuid

    async def check_status(self, pipeline_uuid: str) -> api_pb2.Status:
        """
        Get status of the pipeline by his pipeline

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            status: status of the pipeline
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.CheckStatusRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.CheckStatus(request, **self._kwargs)
        return response.status

    async def get_run_error(self, pipeline_uuid: str) -> str:
        """
        Get the error of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            output: contain an error of pipeline execution
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.GetRunErrorRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetRunError(request, **self._kwargs)
        return response.output

    async def get_run_output(self, pipeline_uuid: str, example_filepath: str) -> str:
        """
        Get the result of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline
            example_filepath: path to the file of the example

        Returns:
            output: contain the result of pipeline execution
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.GetRunOutputRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetRunOutput(request, **self._kwargs)
        if response.output == "":
            logging.info("Run output for %s is empty", example_filepath)
        return response.output

    async def get_log(self, pipeline_uuid: str, example_filepath: str) -> str:
        """
        Get the result of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline
            example_filepath: path to the file of the example

        Returns:
            output: contain the result of pipeline execution
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.GetLogsRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetLogs(request, **self._kwargs)
        if response.output == "":
            logging.info("Log for %s is empty", example_filepath)

        return response.output

    async def get_compile_output(self, pipeline_uuid: str) -> str:
        """
        Get the result of pipeline compilation.

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            output: contain the result of pipeline compilation
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.GetCompileOutputRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetCompileOutput(request, **self._kwargs)

        return response.output

    async def get_graph(self, pipeline_uuid: str, example_filepath: str) -> str:
        """
        Get the graph of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline
            example_filepath: path to the file of the example

        Returns:
            graph: contain the graph of pipeline execution as a string
        """
        self._verify_pipeline_uuid(pipeline_uuid)
        request = api_pb2.GetGraphRequest(pipeline_uuid=pipeline_uuid)
        try:
            response = await self._stub.GetGraph(request, **self._kwargs)
            if response.graph == "":
                logging.warning("Graph for %s wasn't generated", example_filepath)
            return response.graph
        except grpc.RpcError:
            logging.warning("Graph for %s wasn't generated", example_filepath)
            return ""

    def _verify_pipeline_uuid(self, pipeline_uuid):
        """
        Verify the received pipeline_uuid format

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            If pipeline ID is not verified, will raise an exception
        """
        try:
            uuid.UUID(pipeline_uuid)
        except ValueError as ve:
            raise ValueError(f"Incorrect pipeline uuid: '{pipeline_uuid}'") from ve
