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
import pytest
from typing import Optional, List, Dict, Any

from models import Example, SdkEnum, Tag

from helper import (
    load_supported_categories,
)


@pytest.fixture(autouse=True, scope="session")
def supported_categories():
    load_supported_categories("../categories.yaml")


@pytest.fixture(autouse=True)
def create_test_example(create_test_tag):
    def _create_test_example(
        tag_meta: Optional[Dict[str, Any]] = None, **example_meta
    ) -> Example:
        if tag_meta is None:
            tag_meta = {}
        meta: Dict[str, Any] = dict(
            sdk=SdkEnum.JAVA,
            pipeline_id="MOCK_PIPELINE_ID",
            filepath="MOCK_FILEPATH",
            code="MOCK_CODE",
            output="MOCK_OUTPUT",
            url_vcs="https://github.com/proj/MOCK_LINK",
            context_line=132,
        )
        meta.update(**example_meta)
        return Example(
            tag=create_test_tag(**tag_meta),
            **meta,
        )

    return _create_test_example


@pytest.fixture(autouse=True)
def create_test_tag():
    def _create_test_tag(**tag_meta) -> Tag:
        meta = {
            "name": "MOCK_NAME",
            "description": "MOCK_DESCRIPTION",
            "complexity": "ADVANCED",
            "multifile": False,
            "categories": ["Testing", "Schemas"],
            "pipeline_options": "--MOCK_OPTION MOCK_OPTION_VALUE",
        }
        for k, v in tag_meta.items():
            if v is None:
                meta.pop(k, None)
            else:
                meta[k] = v
        return Tag(
            line_start=10,
            line_finish=20,
            context_line=30,
            **meta,
        )

    return _create_test_tag
