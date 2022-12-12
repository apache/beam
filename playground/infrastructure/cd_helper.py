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
  Helper for CD step.

  It is used to save beam examples/katas/tests and their output on the GCS.
"""
import asyncio
import logging
from typing import List

from api.v1.api_pb2 import Sdk, SDK_PYTHON, SDK_JAVA
from config import Origin
from datastore_client import DatastoreClient
from grpc_client import GRPCClient
from helper import Example, get_statuses
from repository import set_dataset_path_for_examples


class CDHelper:
    """
    Helper for CD step.

    It is used to save beam examples/katas/tests and their output on the GCD.
    """
    _sdk: Sdk
    _origin: Origin

    def __init__(self, sdk: Sdk, origin: Origin):
        self._sdk = sdk
        self._origin = origin

    def save_examples(self, examples: List[Example]):
        """
        Save beam examples and their output in the Google Cloud Datastore.

        Outputs for multifile examples are left empty.
        """
        single_file_examples = list(filter(
            lambda example: example.tag.multifile is False, examples))
        set_dataset_path_for_examples(single_file_examples)
        logging.info("Start of executing only single-file Playground examples ...")
        asyncio.run(self._get_outputs(single_file_examples))
        logging.info("Finish of executing single-file Playground examples")

        logging.info("Start of sending Playground examples to the Cloud Datastore ...")
        self._save_to_datastore(single_file_examples)
        logging.info("Finish of sending Playground examples to the Cloud Datastore")

    def _save_to_datastore(self, examples: List[Example]):
        """
        Save beam examples to the Google Cloud Datastore
        :param examples: beam examples from the repository
        """
        datastore_client = DatastoreClient()
        datastore_client.save_catalogs()
        datastore_client.save_to_cloud_datastore(examples, self._sdk, self._origin)

    async def _get_outputs(self, examples: List[Example]):
        """
        Run beam examples and keep their output.

        Call the backend to start code processing for the examples.
        Then receive code output.

        Args:
            examples: beam examples that should be run
        """

        async def _populate_fields(example: Example):
            try:
                example.compile_output = await client.get_compile_output(example.pipeline_id)
                example.output = await client.get_run_output(example.pipeline_id)
                example.logs = await client.get_log(example.pipeline_id)
                if example.sdk in [SDK_JAVA, SDK_PYTHON]:
                    example.graph = await client.get_graph(example.pipeline_id, example.filepath)
            except Exception as e:
                logging.error(example.link)
                logging.error(example.compile_output)
                raise RuntimeError(f"error in {example.name}") from e

        async with GRPCClient() as client:
            await get_statuses(client,
                               examples)  # run examples code and wait until all are executed
            tasks = [_populate_fields(example) for example in examples]
            await asyncio.gather(*tasks)
