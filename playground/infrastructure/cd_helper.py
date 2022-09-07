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


class CDHelper:
    """
    Helper for CD step.

    It is used to save beam examples/katas/tests and their output on the GCD.
    """
    _examples: List[Example]
    _sdk: Sdk
    _origin: Origin

    def __init__(self, examples: List[Example], sdk: Sdk, origin: Origin):
        _examples = examples
        _sdk = sdk
        _origin = origin

    def save_examples(self):
        """
        Save beam examples and their output in the Google Cloud Datastore.

        Outputs for multifile examples are left empty.
        """
        single_file_examples = list(filter(
            lambda example: example.tag.multifile is False, self._examples))
        logging.info("Start of executing only single-file Playground examples ...")
        asyncio.run(self._get_outputs(single_file_examples))
        logging.info("Finish of executing single-file Playground examples")

        logging.info("Start of sending Playground examples to the Cloud Datastore ...")
        self._save_to_datastore(single_file_examples)
        logging.info("Finish of sending Playground examples to the Cloud Datastore")

    def _save_to_datastore(self):
        """
        Save beam examples to the Google Cloud Datastore
        :param examples: beam examples from the repository
        :param sdk: specific sdk that needs to filter examples
        """
        datastore_client = DatastoreClient()
        datastore_client.save_catalogs()
        datastore_client.save_to_cloud_datastore()

    async def _get_outputs(self):
        """
        Run beam examples and keep their output.

        Call the backend to start code processing for the examples.
        Then receive code output.

        Args:
            examples: beam examples that should be run
        """
        await get_statuses(
            self._examples)  # run examples code and wait until all are executed
        client = GRPCClient()
        tasks = [client.get_run_output(example.pipeline_id) for example in self._examples]
        outputs = await asyncio.gather(*tasks)

        tasks = [client.get_log(example.pipeline_id) for example in self._examples]
        logs = await asyncio.gather(*tasks)

        if self._examples and self._examples[0].sdk in [SDK_PYTHON, SDK_JAVA]:
            tasks = [
                client.get_graph(example.pipeline_id, example.filepath)
                for example in self._examples
            ]
            graphs = await asyncio.gather(*tasks)

            for graph, example in zip(graphs, self._examples):
                example.graph = graph

        for output, example in zip(outputs, self._examples):
            example.output = output

        for log, example in zip(logs, self._examples):
            example.logs = log
