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

import grpc

from api import api_pb2_grpc, api_pb2
from config import SERVER_ADDRESS


class GRPCClient:
    """GRPCClient is gRPC client for sending a request to the backend."""

    def __init__(self):
        self._channel = grpc.aio.insecure_channel(SERVER_ADDRESS)
        self._stub = api_pb2_grpc.PlaygroundServiceStub(self._channel)

    async def run_code(self, code: str, sdk: api_pb2.Sdk) -> str:
        """Run example by his code and SDK

            Args:
                code: code of the example.
                sdk: SDK of the example.

            Returns:
                pipeline_uuid: uuid of the pipeline
            """
        request = api_pb2.RunCodeRequest(code=code, sdk=sdk)
        response = await self._stub.RunCode(request)
        return response.pipeline_uuid

    async def check_status(self, pipeline_uuid: str) -> api_pb2.Status:
        """Get status of the pipeline by his pipeline

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            status: status of the pipeline
        """
        request = api_pb2.CheckStatusRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.CheckStatus(request)
        return response.status

    async def get_run_error(self, pipeline_uuid: str) -> str:
        """Get the error of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            output: contain an error of pipeline execution
        """
        request = api_pb2.GetRunErrorRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetRunError(request)
        return response.output

    async def get_run_output(self, pipeline_uuid: str) -> str:
        """Get the result of pipeline execution.

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            output: contain the result of pipeline execution
        """
        request = api_pb2.GetRunOutputRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetRunOutput(request)
        return response.output

    async def get_compile_output(self, pipeline_uuid: str) -> str:
        """Get the result of pipeline compilation.

        Args:
            pipeline_uuid: uuid of the pipeline

        Returns:
            output: contain the result of pipeline compilation
        """
        request = api_pb2.GetCompileOutputRequest(pipeline_uuid=pipeline_uuid)
        response = await self._stub.GetCompileOutput(request)
        return response.output
