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

from dataclasses import dataclass
from typing import List
from api.v1.api_pb2 import SDK_UNSPECIFIED, STATUS_UNSPECIFIED


@dataclass
class Example:
    """
    Class which contains all information about beam example
    """
    name: str
    pipeline_id: str
    sdk: SDK_UNSPECIFIED
    filepath: str
    code: str
    output: str
    status: STATUS_UNSPECIFIED


def find_examples(work_dir: str) -> List[Example]:
    """
    Find and return beam examples.

    Search throws all child files of work_dir directory files with beam tag:
    Beam-playground:
        name: NameOfExample
        description: Description of NameOfExample.
        multifile: false
        categories:
            - category-1
            - category-2

    Args:
        work_dir: directory where to search examples.

    Returns:
        List of Examples.
    """
    # TODO [BEAM-13135] Implement
    pass


def get_statuses(examples: List[Example]):
    """
    Receive statuses for examples and update example.status and example.pipelineId

    Use client to send requests to the backend:
    1. Start code processing.
    2. Ping the backend while status is STATUS_VALIDATING/STATUS_PREPARING/STATUS_COMPILING/STATUS_EXECUTING
    Update example.pipelineId with resulting pipelineId.
    Update example.status with resulting status.

    Args:
        examples: beam examples for processing and updating statuses.
    """
    # TODO [BEAM-13267] Implement
    pass
