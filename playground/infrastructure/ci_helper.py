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

from typing import List
from helper import Example, get_statuses


class CIHelper:
    """
    Helper for CI step.

    It is used to find and verify correctness if beam examples/katas/tests.
    """

    def verify_examples(self, examples: List[Example]):
        """
        Verify correctness of beam examples.

        1. Find all beam examples starting from directory os.getenv("BEAM_ROOT_DIR").
        2. Group code of examples by their SDK.
        3. Run processing for all examples to verify examples' code.
        """
        get_statuses(examples)
        self._verify_examples_status(examples)

    def _verify_examples_status(self, examples: List[Example]):
        """
        Verify statuses of beam examples.

        Check example.status for each examples. If the status of the example is:
        - STATUS_VALIDATION_ERROR/STATUS_PREPARATION_ERROR/STATUS_ERROR/STATUS_RUN_TIMEOUT: log error
        - STATUS_COMPILE_ERROR: get logs using GetCompileOutput request and log them with error.
        - STATUS_RUN_ERROR: get logs using GetRunError request and log them with error.

        Args:
            examples: beam examples that should be verified
        """
        # TODO [BEAM-13256] Implement
        pass
