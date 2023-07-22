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
import os.path
import pytest
from pytest_mock import MockerFixture
from typing import Optional, List, Dict, Any

from models import Example, SdkEnum, Tag

from helper import (
    load_supported_categories,
)


@pytest.fixture(autouse=True, scope="session")
def supported_categories():
    load_supported_categories("../categories.yaml")


@pytest.fixture(autouse=True)
def mock_files(mocker: MockerFixture):
    def _mock_isfile(filepath):
        if filepath in [
            # mock examples & imports
            "MOCK_FILEPATH_0",
            "../../examples/MOCK_EXAMPLE/main.java",
            "../../examples/MOCK_EXAMPLE/utils.java",
            "../../examples/MOCK_EXAMPLE/schema.java",
            # datasets
            "../backend/datasets/dataset_id_1.json",
            "../backend/datasets/dataset_id_1.avro",
            "../backend/datasets/dataset_id_2.json",
            "../backend/datasets/dataset_id_2.avro",
            "../backend/datasets/dataset_id_3.json",
            "../backend/datasets/dataset_id_3.avro",
        ]:
            return True
        raise FileNotFoundError(filepath)

    mocker.patch("os.path.isfile", side_effect=_mock_isfile)
    mocker.patch("builtins.open", mocker.mock_open(read_data="file content"))


@pytest.fixture
def create_test_example(create_test_tag):
    def _create_test_example(
            is_multifile=False, with_kafka=False, tag_meta: Optional[Dict[str, Any]] = None, **example_meta
    ) -> Example:
        if tag_meta is None:
            tag_meta = {}
        meta: Dict[str, Any] = dict(
            sdk=SdkEnum.JAVA,
            pipeline_id="MOCK_PIPELINE_ID",
            filepath="MOCK_FILEPATH",
            code="MOCK_CODE",
            output="MOCK_OUTPUT",
            logs="MOCK_LOGS",
            graph="MOCK_GRAPH",
            url_vcs="https://github.com/proj/MOCK_LINK",
            context_line=132,
        )
        meta.update(**example_meta)
        return Example(
            tag=create_test_tag(is_multifile=is_multifile, with_kafka=with_kafka, **tag_meta),
            **meta,
        )

    return _create_test_example


@pytest.fixture
def create_test_tag():
    def _create_test_tag(with_kafka=False, kafka_datasets=None, is_multifile=False, context_line=30, line_start=10,
                         line_finish=20, **tag_meta) -> Tag:
        meta = {
            "name": "MOCK_NAME",
            "description": "MOCK_DESCRIPTION",
            "complexity": "ADVANCED",
            "multifile": False,
            "categories": ["Testing", "Schemas"],
            "pipeline_options": "--MOCK_OPTION MOCK_OPTION_VALUE",
        }
        if with_kafka:
            if kafka_datasets is None:
                kafka_datasets = {"dataset_id_1": {"format": "avro", "location": "local"}}
            emulators = [
                {
                    "type": "kafka",
                    "topic": {"id": "topic1", "source_dataset": k},
                } for k in kafka_datasets.keys()]
            meta.update(
                emulators=emulators,
                datasets=kafka_datasets
            )
        if is_multifile:
            meta.update(
                multifile=True,
                files=[
                    {"name": "utils.java"},
                    {"name": "schema.java"}
                ]
            )
        for k, v in tag_meta.items():
            if v is None:
                meta.pop(k, None)
            else:
                meta[k] = v
        return Tag(
            filepath="../../examples/MOCK_EXAMPLE/main.java",
            line_start=line_start,
            line_finish=line_finish,
            context_line=context_line,
            **meta,
        )

    return _create_test_tag
