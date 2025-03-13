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
from typing import Dict, List, Any, Optional
from unittest.mock import mock_open

import mock
import pytest
import pydantic

from api.v1 import api_pb2
from api.v1.api_pb2 import (
    SDK_UNSPECIFIED,
    STATUS_UNSPECIFIED,
    STATUS_VALIDATING,
    STATUS_FINISHED,
    PRECOMPILED_OBJECT_TYPE_EXAMPLE,
    PRECOMPILED_OBJECT_TYPE_KATA,
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST,
)
from grpc_client import GRPCClient
from models import (
    ComplexityEnum,
    SdkEnum,
    Emulator,
    Topic,
    EmulatorType,
    Dataset,
    DatasetFormat,
    DatasetLocation,
)
from helper import (
    find_examples,
    Example,
    _load_example,
    get_tag,
    Tag,
    _check_no_nested,
    update_example_status,
    _get_object_type,
    validate_examples_for_duplicates_by_name,
    validate_examples_for_conflicting_datasets,
    DuplicatesError,
    ConflictingDatasetsError,
)


def test_check_for_nested():
    _check_no_nested([])
    _check_no_nested(["sub"])
    _check_no_nested(["sub", "subsub"])
    _check_no_nested(["sub1", "sub2"])
    with pytest.raises(ValueError, match="sub1/sub2 is a subdirectory of sub1"):
        _check_no_nested(["sub3", "sub1", "sub1/sub2"])
    with pytest.raises(ValueError):
        _check_no_nested([".", "sub"])
    with pytest.raises(ValueError):
        _check_no_nested(["./sub1", "sub1"])


@pytest.mark.parametrize("is_valid", [True, False])
@mock.patch("helper._check_no_nested")
@mock.patch("helper._load_example")
@mock.patch("helper.os.walk")
def test_find_examples(
    mock_os_walk, mock_load_example, mock_check_no_nested, is_valid, create_test_example
):
    mock_os_walk.return_value = [
        ("/root/sub1", (), ("file.java",)),
        ("/root/sub2", (), ("file2.java",)),
    ]
    if is_valid:
        mock_load_example.return_value = create_test_example()
        assert (
            find_examples(root_dir="/root", subdirs=["sub1", "sub2"], sdk=SdkEnum.JAVA)
            == [create_test_example()] * 4
        )
    else:
        mock_load_example.side_effect = Exception("MOCK_ERROR")
        with pytest.raises(
            ValueError,
            match="Some of the beam examples contain beam playground tag with an incorrect format",
        ):
            find_examples(root_dir="/root", subdirs=["sub1", "sub2"], sdk=SdkEnum.JAVA)

    mock_check_no_nested.assert_called_once_with(["sub1", "sub2"])
    mock_os_walk.assert_has_calls(
        [
            mock.call("/root/sub1"),
            mock.call("/root/sub2"),
        ]
    )
    mock_load_example.assert_has_calls(
        [
            mock.call(
                filename="file.java",
                filepath="/root/sub1/file.java",
                sdk=SdkEnum.JAVA,
            ),
            mock.call(
                filename="file2.java",
                filepath="/root/sub2/file2.java",
                sdk=SdkEnum.JAVA,
            ),
        ]
    )


@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""// license line 1
// license line 2
//
// beam-playground:
//   name: KafkaWordCount
//   description: Test example with Apache Kafka
//   multifile: false
//   context_line: 28
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator
//   emulators:
//    - type: kafka
//      topic:
//        id: topic_1
//        source_dataset: dataset_id_1
//   datasets:
//      dataset_id_1:
//          location: local
//          format: json

code line 1
code line 2

"""
    ),
)
def test_load_example():
    example = _load_example(
        "kafka.java", "../../examples/MOCK_EXAMPLE/main.java", SdkEnum.JAVA
    )
    assert example == Example(
        sdk=SdkEnum.JAVA,
        type=PRECOMPILED_OBJECT_TYPE_EXAMPLE,
        filepath="../../examples/MOCK_EXAMPLE/main.java",
        code="""// license line 1
// license line 2
//

code line 1
code line 2

""",
        url_vcs="https://github.com/apache/beam/blob/master/examples/MOCK_EXAMPLE/main.java",  # type: ignore
        context_line=5,
        tag=Tag(
            filepath="../../examples/MOCK_EXAMPLE/main.java",
            line_start=3,
            line_finish=26,
            name="KafkaWordCount",
            description="Test example with Apache Kafka",
            multifile=False,
            context_line=28,
            categories=["Filtering", "Options", "Quickstart"],
            complexity=ComplexityEnum.MEDIUM,
            tags=["filter", "strings", "emulator"],
            emulators=[
                Emulator(
                    type=EmulatorType.KAFKA,
                    topic=Topic(id="topic_1", source_dataset="dataset_id_1"),
                )
            ],
            datasets={
                "dataset_id_1": Dataset(
                    location=DatasetLocation.LOCAL, format=DatasetFormat.JSON
                )
            },
        ),
    )


@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""// license line 1
// license line 2
//
// beam-playground:
//   name: KafkaWordCount
//   description: Test example with Apache Kafka
//   multifile: false
//   context_line: 27
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator
//   emulators:
//    - type: kafka
//      topic:
//        id: topic_1
//        source_dataset: dataset_id_1
//   datasets:
//      dataset_id_1:
//          location: local
//          format: json

code line 1
code line 2

"""
    ),
)
def test_load_example_context_at_the_end_of_tag():
    example = _load_example(
        "kafka.java", "../../examples/MOCK_EXAMPLE/main.java", SdkEnum.JAVA
    )
    assert example == Example(
        sdk=SdkEnum.JAVA,
        type=PRECOMPILED_OBJECT_TYPE_EXAMPLE,
        filepath="../../examples/MOCK_EXAMPLE/main.java",
        code="""// license line 1
// license line 2
//

code line 1
code line 2

""",
        url_vcs="https://github.com/apache/beam/blob/master/examples/MOCK_EXAMPLE/main.java",  # type: ignore
        context_line=4,
        tag=Tag(
            filepath="../../examples/MOCK_EXAMPLE/main.java",
            line_start=3,
            line_finish=26,
            name="KafkaWordCount",
            description="Test example with Apache Kafka",
            multifile=False,
            context_line=27,
            categories=["Filtering", "Options", "Quickstart"],
            complexity=ComplexityEnum.MEDIUM,
            tags=["filter", "strings", "emulator"],
            emulators=[
                Emulator(
                    type=EmulatorType.KAFKA,
                    topic=Topic(id="topic_1", source_dataset="dataset_id_1"),
                )
            ],
            datasets={
                "dataset_id_1": Dataset(
                    location=DatasetLocation.LOCAL, format=DatasetFormat.JSON
                )
            },
        ),
    )

@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""// license line 1
// license line 2
//
// beam-playground:
//   name: KafkaWordCount
//   description: Test example with Apache Kafka
//   multifile: false
//   context_line: 3
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator
//   emulators:
//    - type: kafka
//      topic:
//        id: topic_1
//        source_dataset: dataset_id_1
//   datasets:
//      dataset_id_1:
//          location: local
//          format: json

code line 1
code line 2

"""
    ),
)
def test_load_example_context_before_of_tag():
    example = _load_example(
        "kafka.java", "../../examples/MOCK_EXAMPLE/main.java", SdkEnum.JAVA
    )
    assert example == Example(
        sdk=SdkEnum.JAVA,
        type=PRECOMPILED_OBJECT_TYPE_EXAMPLE,
        filepath="../../examples/MOCK_EXAMPLE/main.java",
        code="""// license line 1
// license line 2
//

code line 1
code line 2

""",
        url_vcs="https://github.com/apache/beam/blob/master/examples/MOCK_EXAMPLE/main.java",  # type: ignore
        context_line=3,
        tag=Tag(
            filepath="../../examples/MOCK_EXAMPLE/main.java",
            line_start=3,
            line_finish=26,
            name="KafkaWordCount",
            description="Test example with Apache Kafka",
            multifile=False,
            context_line=3,
            categories=["Filtering", "Options", "Quickstart"],
            complexity=ComplexityEnum.MEDIUM,
            tags=["filter", "strings", "emulator"],
            emulators=[
                Emulator(
                    type=EmulatorType.KAFKA,
                    topic=Topic(id="topic_1", source_dataset="dataset_id_1"),
                )
            ],
            datasets={
                "dataset_id_1": Dataset(
                    location=DatasetLocation.LOCAL, format=DatasetFormat.JSON
                )
            },
        ),
    )


def test__validate_context_line_at_beggining_of_tag(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="line ordering error",
    ):
        create_test_tag(context_line=4, line_start=3, line_finish=27)


def test__validate_context_line_at_end_of_tag(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="line ordering error",
    ):
        create_test_tag(context_line=27, line_start=4, line_finish=27)


def test__validate_without_name_field(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="field required",
    ):
        create_test_tag(name=None)


def test__validate_without_description_field(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="field required",
    ):
        create_test_tag(description=None)


def test__validate_with_incorrect_multifile_field(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value could not be parsed to a boolean",
    ):
        create_test_tag(multifile="multifile")


def test__validate_with_incorrect_categories_field(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid list",
    ):
        create_test_tag(categories="MOCK_CATEGORY_1")


def test__validate_with_not_supported_category(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="Category MOCK_CATEGORY_1 not in",
    ):
        create_test_tag(categories=["MOCK_CATEGORY_1"])


@pytest.mark.asyncio
@mock.patch("grpc_client.GRPCClient.check_status")
@mock.patch("grpc_client.GRPCClient.run_code")
async def test__update_example_status(
    mock_grpc_client_run_code, mock_grpc_client_check_status
):
    example = Example(
        tag=Tag(
            filepath="../../examples/MOCK_EXAMPLE/main.java",
            line_start=10,
            line_finish=20,
            context_line=100,
            name="file",
            description="MOCK_DESCRIPTION",
            complexity=ComplexityEnum.MEDIUM,
            pipeline_options="--key value",
            categories=["Testing"],
        ),
        context_line=100,
        pipeline_id="pipeline_id",
        sdk=SdkEnum.JAVA,
        filepath="root/file.extension",
        code="code",
        output="output",
        status=STATUS_UNSPECIFIED,
        url_vcs="https://github.com/link",  # type: ignore
    )

    mock_grpc_client_run_code.return_value = "pipeline_id"
    mock_grpc_client_check_status.side_effect = [STATUS_VALIDATING, STATUS_FINISHED]

    await update_example_status(example, GRPCClient())

    assert example.pipeline_id == "pipeline_id"
    assert example.status == STATUS_FINISHED
    mock_grpc_client_run_code.assert_called_once_with(
        example.code, example.sdk, "--key value", [], files=[api_pb2.SnippetFile(
            name="root/file.extension",
           content="code",
           is_main=True,
    )]
    )
    mock_grpc_client_check_status.assert_has_calls([mock.call("pipeline_id")])


def test__get_object_type():
    result_example = _get_object_type(
        "filename.extension", "filepath/examples/filename.extension"
    )
    result_kata = _get_object_type(
        "filename.extension", "filepath/katas/filename.extension"
    )
    result_test = _get_object_type(
        "filename_test.extension", "filepath/examples/filename_test.extension"
    )

    assert result_example == PRECOMPILED_OBJECT_TYPE_EXAMPLE
    assert result_kata == PRECOMPILED_OBJECT_TYPE_KATA
    assert result_test == PRECOMPILED_OBJECT_TYPE_UNIT_TEST


def test_validate_examples_for_duplicates_by_name_in_the_usual_case(
    create_test_example,
):
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"]
    examples = list(
        map(lambda name: create_test_example(tag_meta=dict(name=name)), examples_names)
    )
    try:
        validate_examples_for_duplicates_by_name(examples)
    except DuplicatesError:
        pytest.fail("Unexpected ValidationException")


def test_validate_examples_for_duplicates_by_name_when_examples_have_duplicates(
    create_test_example,
):
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_1", "MOCK_NAME_3"]
    examples = list(
        map(lambda name: create_test_example(tag_meta=dict(name=name)), examples_names)
    )
    with pytest.raises(
        DuplicatesError,
        match="Examples have duplicate names.\nDuplicates: \n - path #1: MOCK_FILEPATH \n - path #2: MOCK_FILEPATH",
    ):
        validate_examples_for_duplicates_by_name(examples)


def test_validate_examples_for_conflicting_datasets_same_datasets_no_conflicts(
    create_test_example,
):
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"]
    examples = list(
        map(lambda name: create_test_example(tag_meta=dict(name=name,
                                                           kafka_datasets={"dataset_id_1": {"format": "avro", "location": "local"}}),
                                             with_kafka=True),
            examples_names)
    )
    try:
        validate_examples_for_conflicting_datasets(examples)
    except ConflictingDatasetsError:
        pytest.fail("Unexpected ConflictingDatasetsError")


def test_validate_examples_for_conflicting_datasets_different_datasets_have_conflict(
    create_test_example,
):
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"]
    datasets = [{"dataset_id_1": {"format": "avro", "location": "local"}},
                {"dataset_id_1": {"format": "json", "location": "local"}},
                {"dataset_id_3": {"format": "avro", "location": "local"}}]
    examples = list(
        map(lambda p: create_test_example(tag_meta=dict(name=p[0],
                                                        kafka_datasets=p[1]),
                                          with_kafka=True),
            zip(examples_names, datasets))
    )
    with pytest.raises(ConflictingDatasetsError):
        validate_examples_for_conflicting_datasets(examples)


def test_validate_examples_for_conflicting_datasets_different_datasets_no_conflicts(
    create_test_example,
):
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"]
    datasets = [{"dataset_id_1": {"format": "avro", "location": "local"}},
                {"dataset_id_2": {"format": "json", "location": "local"}},
                {"dataset_id_3": {"format": "avro", "location": "local"}}]
    examples = list(
        map(lambda p: create_test_example(tag_meta=dict(name=p[0],
                                                        kafka_datasets=p[1]),
                                          with_kafka=True),
            zip(examples_names, datasets))
    )
    try:
        validate_examples_for_conflicting_datasets(examples)
    except ConflictingDatasetsError:
        pytest.fail("Unexpected ConflictingDatasetsError")


def test_validate_example_fields_when_filepath_is_invalid(create_test_example):
    with pytest.raises(
        pydantic.ValidationError,
        match="ensure this value has at least 1 characters",
    ):
        create_test_example(filepath="")


def test_validate_example_fields_when_sdk_is_invalid(create_test_example):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid enumeration member",
    ):
        create_test_example(sdk=SDK_UNSPECIFIED)


def test_validate_example_fields_when_code_is_invalid(create_test_example):
    with pytest.raises(
        pydantic.ValidationError,
        match="ensure this value has at least 1 characters",
    ):
        create_test_example(code="")


def test_validate_example_fields_when_url_vcs_is_invalid(create_test_example):
    with pytest.raises(
        pydantic.ValidationError,
        match="ensure this value has at least 1 characters",
    ):
        create_test_example(url_vcs="")


def test_validate_example_fields_when_name_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="ensure this value has at least 1 characters",
    ):
        create_test_tag(name="")


def test_validate_example_fields_when_complexity_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid enumeration member",
    ):
        create_test_tag(complexity="")


def test_validate_example_fields_when_emulator_not_set_but_dataset_set(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="datasets w/o emulators",
    ):
        create_test_tag(
            datasets={"dataset_id_1": {"format": "avro", "location": "local"}}
        )


def test_validate_example_fields_when_emulator_type_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid enumeration member",
    ):
        create_test_tag(
            emulators=[
                {
                    "type": "MOCK_TYPE",
                    "topic": {"id": "topic1", "source_dataset": "dataset_id_1"},
                }
            ],
            datasets={"dataset_id_1": {"format": "json", "location": "local"}},
        )


def test_validate_example_fields_when_dataset_format_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid enumeration member",
    ):
        create_test_tag(
            emulators=[
                {"type": "kafka", "topic": {"id": "topic1", "source_dataset": "src"}}
            ],
            datasets={"src": {"format": "MOCK_FORMAT", "location": "local"}},
        )


def test_validate_example_fields_when_dataset_location_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="value is not a valid enumeration member",
    ):
        create_test_tag(
            emulators=[
                {"type": "kafka", "topic": {"id": "topic1", "source_dataset": "src"}}
            ],
            datasets={"src": {"format": "avro", "location": "MOCK_LOCATION"}},
        )


def test_validate_example_fields_when_dataset_name_is_invalid(create_test_tag):
    with pytest.raises(
        pydantic.ValidationError,
        match="mulator topic topic1 has undefined dataset src",
    ):
        create_test_tag(
            emulators=[
                {"type": "kafka", "topic": {"id": "topic1", "source_dataset": "src"}}
            ]
        )


@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""

// beam-playground:
//   name: KafkaWordCount
//   description: Test example with Apache Kafka
//   multifile: false
//   context_line: 55
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator
//   emulators:
//    - type: kafka
//      topic:
//        id: topic_1
//        source_dataset: dataset_id_1
//   datasets:
//      dataset_id_1:
//          location: local
//          format: json

"""
    ),
)
def test_get_tag_with_datasets():
    tag = get_tag("../../examples/MOCK_EXAMPLE/main.java")
    assert tag == Tag(
        **{
            "filepath": "../../examples/MOCK_EXAMPLE/main.java",
            "line_start": 2,
            "line_finish": 25,
            "name": "KafkaWordCount",
            "description": "Test example with Apache Kafka",
            "multifile": False,
            "context_line": 55,
            "categories": ["Filtering", "Options", "Quickstart"],
            "complexity": "MEDIUM",
            "tags": ["filter", "strings", "emulator"],
            "emulators": [
                {
                    "type": "kafka",
                    "topic": {"id": "topic_1", "source_dataset": "dataset_id_1"},
                }
            ],
            "datasets": {"dataset_id_1": {"location": "local", "format": "json"}},
        },
    )


@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""

// beam-playground:
//   name: MultifileExample
//   description: Test example with imports
//   multifile: true
//   files:
//     - name: utils.java
//       context_line: 51
//     - name: schema.java
//       context_line: 52
//   context_line: 55
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator

"""
    ),
)
def test_get_tag_multifile():
    tag = get_tag("../../examples/MOCK_EXAMPLE/main.java")
    assert tag == Tag(
        **{
            "filepath": "../../examples/MOCK_EXAMPLE/main.java",
            "line_start": 2,
            "line_finish": 21,
            "name": "MultifileExample",
            "description": "Test example with imports",
            "multifile": True,
            "context_line": 55,
            "categories": ["Filtering", "Options", "Quickstart"],
            "complexity": "MEDIUM",
            "tags": ["filter", "strings", "emulator"],
            "files": [
                {
                    "name": "utils.java",
                    "context_line": 51,
                },
                {
                    "name": "schema.java",
                    "context_line": 52,
                },
            ],
        },
    )

@mock.patch(
    "builtins.open",
    mock_open(
        read_data="""

// beam-playground:
//   name: MultifileExample
//   description: Test example with imports
//   multifile: true
//   context_line: 55
//   categories:
//     - Filtering
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - filter
//     - strings
//     - emulator

"""
    ),
)

def test_get_tag_multifile_incomplete():
    tag = get_tag("../../examples/MOCK_EXAMPLE/main.java")
    assert tag is None

@mock.patch("os.path.isfile", return_value=True)
def test_dataset_path_ok(mock_file_check, create_test_example):
    example = create_test_example(with_kafka=True)
    assert len(example.tag.datasets) > 0
    assert example.tag.datasets.popitem()[1].file_name == "dataset_id_1.avro"


@mock.patch("os.path.isfile", return_value=False)
def test_dataset_path_notfound(mock_file_check, create_test_example):
    with pytest.raises(FileNotFoundError):
        create_test_example(with_kafka=True)
