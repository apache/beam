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

from api.v1.api_pb2 import SDK_JAVA, STATUS_UNSPECIFIED
from helper import Example, Tag


def _get_examples(number_of_examples: int) -> List[Example]:
    examples = []
    for number in range(number_of_examples):
        object_meta = {
            "name": f"MOCK_NAME_{number}",
            "description": f"MOCK_DESCRIPTION_{number}",
            "multifile": False,
            "categories": ["MOCK_CATEGORY_1", "MOCK_CATEGORY_2"],
            "pipeline_options": "--MOCK_OPTION MOCK_OPTION_VALUE"
        }
        example = Example(
            name=f"MOCK_NAME_{number}",
            complexity="MEDIUM",
            pipeline_id=f"MOCK_PIPELINE_ID_{number}",
            sdk=SDK_JAVA,
            filepath=f"MOCK_FILEPATH_{number}",
            code=f"MOCK_CODE_{number}",
            output=f"MOCK_OUTPUT_{number}",
            status=STATUS_UNSPECIFIED,
            tag=Tag(**object_meta),
            link=f"MOCK_LINK_{number}")
        examples.append(example)
    return examples
