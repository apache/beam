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

from unittest.mock import mock_open

import mock
import pytest

from api.v1.api_pb2 import SDK_UNSPECIFIED, STATUS_UNSPECIFIED, \
    STATUS_VALIDATING, \
    STATUS_FINISHED, SDK_JAVA, \
    PRECOMPILED_OBJECT_TYPE_EXAMPLE, PRECOMPILED_OBJECT_TYPE_KATA, \
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST
from grpc_client import GRPCClient
from helper import find_examples, Example, _get_example, _get_name, get_tag, \
    _validate, Tag, get_statuses, \
    _update_example_status, get_supported_categories, _check_file, \
    _get_object_type, ExampleTag, validate_examples_for_duplicates_by_name, ValidationException, validate_example_fields


@mock.patch("helper._check_file")
@mock.patch("helper.os.walk")
def test_find_examples_with_valid_tag(mock_os_walk, mock_check_file):
    mock_os_walk.return_value = [("/root", (), ("file.java",))]
    mock_check_file.return_value = False
    sdk = SDK_UNSPECIFIED
    result = find_examples(work_dir="", supported_categories=[], sdk=sdk)

    assert not result
    mock_os_walk.assert_called_once_with("")
    mock_check_file.assert_called_once_with(
        examples=[],
        filename="file.java",
        filepath="/root/file.java",
        supported_categories=[],
        sdk=sdk)


@mock.patch("helper._check_file")
@mock.patch("helper.os.walk")
def test_find_examples_with_invalid_tag(mock_os_walk, mock_check_file):
    mock_os_walk.return_value = [("/root", (), ("file.java",))]
    mock_check_file.return_value = True
    sdk = SDK_UNSPECIFIED
    with pytest.raises(
          ValueError,
          match="Some of the beam examples contain beam playground tag with "
                "an incorrect format"):
        find_examples("", [], sdk=sdk)

    mock_os_walk.assert_called_once_with("")
    mock_check_file.assert_called_once_with(
        examples=[],
        filename="file.java",
        filepath="/root/file.java",
        supported_categories=[],
        sdk=sdk)


@pytest.mark.asyncio
@mock.patch("helper.GRPCClient")
@mock.patch("helper._update_example_status")
async def test_get_statuses(mock_update_example_status, mock_grpc_client):
    example = Example(
        name="file",
        complexity="MEDIUM",
        pipeline_id="pipeline_id",
        sdk=SDK_UNSPECIFIED,
        filepath="root/file.extension",
        code="code",
        output="output",
        status=STATUS_UNSPECIFIED,
        tag={"name": "Name"},
        link="link")
    client = None

    mock_grpc_client.return_value = client

    await get_statuses([example])

    mock_update_example_status.assert_called_once_with(example, client)


@mock.patch(
    "builtins.open",
    mock_open(
        read_data="...\n# beam-playground:\n#     name: Name\n\nimport ..."))
def test_get_tag_when_tag_is_exists():
    result = get_tag("")

    assert result.tag_as_dict.get("name") == "Name"
    assert result.tag_as_string == "# beam-playground:\n#     name: Name\n\n"


@mock.patch("builtins.open", mock_open(read_data="...\n..."))
def test_get_tag_when_tag_does_not_exist():
    result = get_tag("")

    assert result is None


@mock.patch("helper._get_example")
@mock.patch("helper._validate")
@mock.patch("helper.get_tag")
def test__check_file_with_correct_tag(
      mock_get_tag, mock_validate, mock_get_example):
    tag = ExampleTag({"name": "Name"}, "")
    example = Example(
        name="filename",
        complexity="MEDIUM",
        sdk=SDK_JAVA,
        filepath="/root/filename.java",
        code="data",
        status=STATUS_UNSPECIFIED,
        tag=Tag("Name", "Description", False, [], '--option option'),
        link="link")
    examples = []

    mock_get_tag.return_value = tag
    mock_validate.return_value = True
    mock_get_example.return_value = example

    result = _check_file(
        examples, "filename.java", "/root/filename.java", [], sdk=SDK_JAVA)

    assert result is False
    assert len(examples) == 1
    assert examples[0] == example
    mock_get_tag.assert_called_once_with("/root/filename.java")
    mock_validate.assert_called_once_with(tag.tag_as_dict, [])
    mock_get_example.assert_called_once_with(
        "/root/filename.java", "filename.java", tag)


@mock.patch("helper._validate")
@mock.patch("helper.get_tag")
def test__check_file_with_incorrect_tag(mock_get_tag, mock_validate):
    tag = ExampleTag({"name": "Name"}, "")
    examples = []
    sdk = SDK_JAVA
    mock_get_tag.return_value = tag
    mock_validate.return_value = False

    result = _check_file(
        examples, "filename.java", "/root/filename.java", [], sdk)

    assert result is True
    assert len(examples) == 0
    mock_get_tag.assert_called_once_with("/root/filename.java")
    mock_validate.assert_called_once_with(tag.tag_as_dict, [])


@mock.patch("builtins.open", mock_open(read_data="categories:\n    - category"))
def test_get_supported_categories():
    result = get_supported_categories("")

    assert len(result) == 1
    assert result[0] == "category"


@mock.patch("builtins.open", mock_open(read_data="data"))
def test__get_example():
    tag = ExampleTag({
        "name": "Name",
        "description": "Description",
        "multifile": "False",
        "categories": [""],
        "pipeline_options": "--option option",
        "context_line": 1,
        "complexity": "MEDIUM"
    },
        "")

    result = _get_example("/root/filepath.java", "filepath.java", tag)

    assert result == Example(
        name="Name",
        sdk=SDK_JAVA,
        filepath="/root/filepath.java",
        code="data",
        status=STATUS_UNSPECIFIED,
        tag=Tag(
            "Name", "MEDIUM", "Description", "False", [""], "--option option", False, 1),
        link="https://github.com/apache/beam/blob/master/root/filepath.java",
        complexity="MEDIUM")


def test__validate_without_name_field():
    tag = {}
    assert _validate(tag, []) is False


def test__validate_without_description_field():
    tag = {"name": "Name"}
    assert _validate(tag, []) is False


def test__validate_without_multifile_field():
    tag = {"name": "Name", "description": "Description"}
    assert _validate(tag, []) is False


def test__validate_with_incorrect_multifile_field():
    tag = {"name": "Name", "description": "Description", "multifile": "Multifile"}
    assert _validate(tag, []) is False


def test__validate_without_categories_field():
    tag = {"name": "Name", "description": "Description", "multifile": "true"}
    assert _validate(tag, []) is False


def test__validate_without_incorrect_categories_field():
    tag = {
        "name": "Name",
        "description": "Description",
        "multifile": "true",
        "categories": "Categories"
    }
    assert _validate(tag, []) is False


def test__validate_with_not_supported_category():
    tag = {
        "name": "Name",
        "description": "Description",
        "multifile": "true",
        "categories": ["category1"]
    }
    assert _validate(tag, ["category"]) is False


def test__validate_with_all_fields():
    tag = {
        "name": "Name",
        "description": "Description",
        "multifile": "true",
        "categories": ["category"],
        "pipeline_options": "--option option",
        "context_line": 1,
        "complexity": "MEDIUM",
        "tags": ["tag"]
    }
    assert _validate(tag, ["category"]) is True


def test__get_name():
    result = _get_name("filepath.extension")

    assert result == "filepath"


@pytest.mark.asyncio
@mock.patch("grpc_client.GRPCClient.check_status")
@mock.patch("grpc_client.GRPCClient.run_code")
async def test__update_example_status(
      mock_grpc_client_run_code, mock_grpc_client_check_status):
    example = Example(
        name="file",
        complexity="MEDIUM",
        pipeline_id="pipeline_id",
        sdk=SDK_UNSPECIFIED,
        filepath="root/file.extension",
        code="code",
        output="output",
        status=STATUS_UNSPECIFIED,
        tag=Tag(**{"pipeline_options": "--key value"}),
        link="link")

    mock_grpc_client_run_code.return_value = "pipeline_id"
    mock_grpc_client_check_status.side_effect = [
        STATUS_VALIDATING, STATUS_FINISHED
    ]

    await _update_example_status(example, GRPCClient())

    assert example.pipeline_id == "pipeline_id"
    assert example.status == STATUS_FINISHED
    mock_grpc_client_run_code.assert_called_once_with(
        example.code, example.sdk, "--key value")
    mock_grpc_client_check_status.assert_has_calls([mock.call("pipeline_id")])


def test__get_object_type():
    result_example = _get_object_type(
        "filename.extension", "filepath/examples/filename.extension")
    result_kata = _get_object_type(
        "filename.extension", "filepath/katas/filename.extension")
    result_test = _get_object_type(
        "filename_test.extension", "filepath/examples/filename_test.extension")

    assert result_example == PRECOMPILED_OBJECT_TYPE_EXAMPLE
    assert result_kata == PRECOMPILED_OBJECT_TYPE_KATA
    assert result_test == PRECOMPILED_OBJECT_TYPE_UNIT_TEST


def test_validate_examples_for_duplicates_by_name_in_the_usual_case():
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"]
    examples = list(map(lambda name: _create_example(name), examples_names))
    try:
        validate_examples_for_duplicates_by_name(examples)
    except ValidationException:
        pytest.fail("Unexpected ValidationException")


def test_validate_examples_for_duplicates_by_name_when_examples_have_duplicates():
    examples_names = ["MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_1", "MOCK_NAME_3"]
    examples = list(map(lambda name: _create_example(name), examples_names))
    with pytest.raises(ValidationException, match="Examples have duplicate names.\nDuplicates: \n - path #1: MOCK_FILEPATH \n - path #2: MOCK_FILEPATH"):
        validate_examples_for_duplicates_by_name(examples)


def test_validate_example_fields_when_filepath_is_invalid():
    example = _create_example("MOCK_NAME")
    example.filepath = ""
    with pytest.raises(ValidationException, match="Example doesn't have a file path field. Example: "):
        validate_example_fields(example)


def test_validate_example_fields_when_name_is_invalid():
    example = _create_example("")
    with pytest.raises(ValidationException, match="Example doesn't have a name field. Path: MOCK_FILEPATH"):
        validate_example_fields(example)


def test_validate_example_fields_when_sdk_is_invalid():
    example = _create_example("MOCK_NAME")
    example.sdk = SDK_UNSPECIFIED
    with pytest.raises(ValidationException, match="Example doesn't have a sdk field. Path: MOCK_FILEPATH"):
        validate_example_fields(example)


def test_validate_example_fields_when_code_is_invalid():
    example = _create_example("MOCK_NAME")
    example.code = ""
    with pytest.raises(ValidationException, match="Example doesn't have a code field. Path: MOCK_FILEPATH"):
        validate_example_fields(example)


def test_validate_example_fields_when_link_is_invalid():
    example = _create_example("MOCK_NAME")
    example.link = ""
    with pytest.raises(ValidationException, match="Example doesn't have a link field. Path: MOCK_FILEPATH"):
        validate_example_fields(example)


def test_validate_example_fields_when_complexity_is_invalid():
    example = _create_example("MOCK_NAME")
    example.complexity = ""
    with pytest.raises(ValidationException, match="Example doesn't have a complexity field. Path: MOCK_FILEPATH"):
        validate_example_fields(example)


def _create_example(name: str) -> Example:
    object_meta = {
        "name": "MOCK_NAME",
        "description": "MOCK_DESCRIPTION",
        "multifile": False,
        "categories": ["MOCK_CATEGORY_1", "MOCK_CATEGORY_2"],
        "pipeline_options": "--MOCK_OPTION MOCK_OPTION_VALUE"
    }
    example = Example(
        name=name,
        pipeline_id="MOCK_PIPELINE_ID",
        sdk=SDK_JAVA,
        filepath="MOCK_FILEPATH",
        code="MOCK_CODE",
        output="MOCK_OUTPUT",
        status=STATUS_UNSPECIFIED,
        tag=Tag(**object_meta),
        link="MOCK_LINK",
        complexity="MOCK_COMPLEXITY")
    return example
