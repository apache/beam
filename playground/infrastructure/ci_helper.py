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
Module contains the helper for CI step.

It is used to find and verify correctness if beam examples/katas/tests.
"""

import logging
from typing import List

from api.v1.api_pb2 import STATUS_COMPILE_ERROR, STATUS_ERROR, STATUS_RUN_ERROR, \
    STATUS_RUN_TIMEOUT, \
    STATUS_VALIDATION_ERROR, STATUS_PREPARATION_ERROR
from config import Config
from grpc_client import GRPCClient
from helper import Example, get_statuses


class VerifyException(Exception):
    def __init__(self, error: str):
        super().__init__()
        self.msg = error

    def __str__(self):
        return self.msg


class CIHelper:
    """
    Helper for CI step.

    It is used to find and verify correctness if beam examples/katas/tests.
    """
    async def verify_examples(self, examples: List[Example]):
        """
        Verify correctness of beam examples.

        1. Find all beam examples starting from directory os.getenv("BEAM_ROOT_DIR")
        2. Group code of examples by their SDK.
        3. Run processing for single-file examples to verify examples' code.
        """
        single_file_examples = list(filter(
            lambda example: example.tag.multifile is False, examples))
        await get_statuses(single_file_examples)
        await self._verify_examples(single_file_examples)

    async def _verify_examples(self, examples: List[Example]):
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
        client = GRPCClient()
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
                    "Example: %s has error during setup run builder", example.filepath)
            elif example.status == STATUS_RUN_TIMEOUT:
                logging.error("Example: %s failed because of timeout", example.filepath)
            elif example.status == STATUS_COMPILE_ERROR:
                err = await client.get_compile_output(example.pipeline_id)
                logging.error(
                    "Example: %s has compilation error: %s", example.filepath, err)
            elif example.status == STATUS_RUN_ERROR:
                err = await client.get_run_error(example.pipeline_id)
                logging.error(
                    "Example: %s has execution error: %s", example.filepath, err)
            verify_status_failed = True

        logging.info(
            "Number of verified Playground examples: %s / %s",
            count_of_verified,
            len(examples))
        logging.info(
            "Number of Playground examples with some error: %s / %s",
            len(examples) - count_of_verified,
            len(examples))

        if len(default_examples) == 0:
            logging.error("Default example not found")
            raise VerifyException(
                "CI step failed due to finding an incorrect number "
                "of default examples. Default example not found")
        if len(default_examples) > 1:
            logging.error("Many default examples found")
            logging.error("Examples where the default_example field is true:")
            for example in default_examples:
                logging.error(example.filepath)
            raise VerifyException(
                "CI step failed due to finding an incorrect number "
                "of default examples. Many default examples found")

        if verify_status_failed:
            raise VerifyException("CI step failed due to errors in the examples")
