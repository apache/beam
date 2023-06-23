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

import asyncio
import logging
import os
from pathlib import Path
from typing import List

from tqdm.asyncio import tqdm

from api.v1.api_pb2 import Sdk, SDK_PYTHON, SDK_JAVA
from api.v1.api_pb2 import (
    STATUS_COMPILE_ERROR,
    STATUS_ERROR,
    STATUS_RUN_ERROR,
    STATUS_RUN_TIMEOUT,
    STATUS_VALIDATION_ERROR,
    STATUS_PREPARATION_ERROR,
)
from config import Origin, Config
from grpc_client import GRPCClient
from helper import update_example_status
from models import Example, SdkEnum


class VerifyException(Exception):
    pass


class Verifier:
    """Run examples and verify the results, enrich examples with produced artifacts"""

    LOGS_FILENAME = "logs.log"
    OUTPUT_FILENAME = "output.log"
    GRAPH_FILENAME = "graph.log"
    COMPILE_OUTPUT_FILENAME = "compile_output.log"

    _sdk: SdkEnum
    _origin: Origin

    def __init__(self, sdk: SdkEnum, origin: Origin):
        self._sdk = sdk
        self._origin = origin

    def run_verify(self, examples: List[Example]):
        """
        Save beam examples and their output in the Google Cloud Datastore.

        """
        logging.info("Start of executing Playground examples ...")
        asyncio.run(self._run_and_verify(examples))
        logging.info("Finish of executing Playground examples")

    async def _get_statuses(
            self,
            client: GRPCClient,
            examples: List[Example],
            concurrency: int = 10
    ):
        """
        Receive status and update example.status and example.pipeline_id for
        each example

        Args:
            examples: beam examples for processing and updating statuses and
            pipeline_id values.
        """
        tasks = []
        try:
            concurrency = int(os.environ["BEAM_CONCURRENCY"])
            logging.info("override default concurrency: %d", concurrency)
        except (KeyError, ValueError):
            pass

        semaphore = asyncio.Semaphore(concurrency)

        async def _semaphored_task(example):
            await semaphore.acquire()
            try:
                await update_example_status(example, client)
                await self._populate_fields(example, client)
            finally:
                semaphore.release()

        for example in examples:
            if example.tag.never_run:
                logging.info("skipping non runnable example %s", example.filepath)
            else:
                tasks.append(_semaphored_task(example))
        await tqdm.gather(*tasks)

    async def _populate_fields(self, example: Example, client: GRPCClient):
        """
        Populate fields of the example reading them from the backend or from the repository.
        Args:
            example: beam example that should be verified
        """
        if example.tag.never_run:
            logging.info("populating example fields from provided files %s", example.filepath)
            self._populate_from_repo(example)
        else:
            await self._populate_from_runner(example, client)

    def _populate_from_repo(self, example: Example):
        """
        Populate fields of the example reading them from the repository.
        Args:
            example: beam example that should be verified
        """
        path = Path(example.filepath)
        example_folder = path.parent

        log_file_path = example_folder / self.LOGS_FILENAME
        # Check if the file exists and read its content
        if log_file_path.exists():
            example.logs = log_file_path.read_text()
        graph_file_path = example_folder / self.GRAPH_FILENAME
        # Check if the file exists and read its content
        if graph_file_path.exists():
            example.graph = graph_file_path.read_text()
        output_file_path = example_folder / self.OUTPUT_FILENAME
        # Check if the file exists and read its content
        if output_file_path.exists():
            example.output = output_file_path.read_text()
        compile_output_file_path = example_folder / self.COMPILE_OUTPUT_FILENAME
        # Check if the file exists and read its content
        if compile_output_file_path.exists():
            example.compile_output = compile_output_file_path.read_text()

    async def _populate_from_runner(self, example: Example, client: GRPCClient):
        try:
            example.compile_output = await client.get_compile_output(
                example.pipeline_id
            )
            example.output = await client.get_run_output(example.pipeline_id, example.filepath)
            example.logs = await client.get_log(example.pipeline_id, example.filepath)
            if example.sdk in [SDK_JAVA, SDK_PYTHON]:
                example.graph = await client.get_graph(
                    example.pipeline_id, example.filepath
                )
        except Exception as e:
            logging.error(example.url_vcs)
            logging.error(example.compile_output)
            raise RuntimeError(f"error in {example.tag.name}") from e

    async def _run_and_verify(self, examples: List[Example]):
        """
        Run beam examples and keep their output.

        Call the backend to start code processing for the examples.
        Then receive code output.

        Args:
            examples: beam examples that should be run
        """

        async with GRPCClient() as client:
            await self._get_statuses(
                client, examples
            )  # run examples code and wait until all are executed
            await self._verify_examples(client, examples, self._origin)

    async def _verify_examples(
        self, client: GRPCClient, examples: List[Example], origin: Origin
    ):
        """
        Verify statuses of beam examples and the number of found default examples.

        Check example.status for each examples. If the status of the example is:
        - STATUS_VALIDATION_ERROR/STATUS_PREPARATION_ERROR
          /STATUS_ERROR/STATUS_RUN_TIMEOUT: log error
        - STATUS_COMPILE_ERROR: get logs using GetCompileOutput request and
          log them with error.
        - STATUS_RUN_ERROR: get logs using GetRunError request and
          log them with error.

        Args:
            examples: beam examples that should be verified
        """
        count_of_verified = 0
        verify_status_failed = False
        default_examples = []

        for example in examples:
            if example.tag.default_example:
                default_examples.append(example)
            if example.status not in Config.ERROR_STATUSES:
                count_of_verified += 1
                continue
            if example.status == STATUS_VALIDATION_ERROR:
                logging.error("Example: %s has validation error", example.filepath)
            elif example.status == STATUS_PREPARATION_ERROR:
                logging.error("Example: %s has preparation error", example.filepath)
            elif example.status == STATUS_ERROR:
                logging.error(
                    "Example: %s has error during setup run builder", example.filepath
                )
            elif example.status == STATUS_RUN_TIMEOUT:
                logging.error("Example: %s failed because of timeout", example.filepath)
            elif example.status == STATUS_COMPILE_ERROR:
                err = await client.get_compile_output(example.pipeline_id)
                logging.error(
                    "Example: %s has compilation error: %s", example.filepath, err
                )
            elif example.status == STATUS_RUN_ERROR:
                err = await client.get_run_error(example.pipeline_id)
                logging.error(
                    "Example: %s has execution error: %s", example.filepath, err
                )
            verify_status_failed = True

        logging.info(
            "Number of verified Playground examples: %s / %s",
            count_of_verified,
            len(examples),
        )
        logging.info(
            "Number of Playground examples with some error: %s / %s",
            len(examples) - count_of_verified,
            len(examples),
        )

        if origin == Origin.PG_EXAMPLES:
            if len(default_examples) == 0:
                logging.error("Default example not found")
                raise VerifyException(
                    "CI step failed due to finding an incorrect number "
                    "of default examples. Default example not found"
                )
            if len(default_examples) > 1:
                logging.error("Many default examples found")
                logging.error("Examples where the default_example field is true:")
                for example in default_examples:
                    logging.error(example.filepath)
                raise VerifyException(
                    "CI step failed due to finding an incorrect number "
                    "of default examples. Many default examples found"
                )

        if verify_status_failed:
            raise VerifyException("CI step failed due to errors in the examples")
