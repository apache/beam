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
from datastore_client import DatastoreClient
from grpc_client import GRPCClient
from helper import Example, get_statuses


class CDHelper:
    """
    Helper for CD step.

    It is used to save beam examples/katas/tests and their output on the GCD.
    """

    def save_examples(self, examples: List[Example], sdk: Sdk):
        """
        Save beam examples and their output in the Google Cloud Datastore.

        Outputs for multifile examples are left empty.
        """
        single_file_examples = list(filter(
            lambda example: example.tag.multifile is False, examples))
        logging.info("Start of executing only single-file Playground examples ...")
        asyncio.run(self._get_outputs(single_file_examples))
        logging.info("Finish of executing single-file Playground examples")

        logging.info("Start of sending Playground examples to the Cloud Datastore ...")
        self._save_to_datastore(single_file_examples, sdk)
        logging.info("Finish of sending Playground examples to the Cloud Datastore")

    def _save_to_datastore(self, examples: List[Example], sdk: Sdk):
        """
        Save beam examples to the Google Cloud Datastore
        :param examples: beam examples from the repository
        :param sdk: specific sdk that needs to filter examples
        """
        datastore_client = DatastoreClient()
        datastore_client.save_catalogs()
        datastore_client.save_to_cloud_datastore(examples, sdk)

    async def _get_outputs(self, examples: List[Example]):
        """
        Run beam examples and keep their output.

        Call the backend to start code processing for the examples.
        Then receive code output.

        Args:
            examples: beam examples that should be run
        """
        await get_statuses(
            examples)  # run examples code and wait until all are executed
        client = GRPCClient()
        tasks = [client.get_run_output(example.pipeline_id) for example in examples]
        outputs = await asyncio.gather(*tasks)

        tasks = [client.get_log(example.pipeline_id) for example in examples]
        logs = await asyncio.gather(*tasks)

        if len(examples) > 0 and (examples[0].sdk is SDK_PYTHON or
                                  examples[0].sdk is SDK_JAVA):
            tasks = [
                client.get_graph(example.pipeline_id, example.filepath)
                for example in examples
            ]
            graphs = await asyncio.gather(*tasks)

            for graph, example in zip(graphs, examples):
                example.graph = graph

        for output, example in zip(outputs, examples):
            example.output = output

        for log, example in zip(logs, examples):
            example.logs = log
