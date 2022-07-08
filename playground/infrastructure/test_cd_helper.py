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

import os
import pathlib
import shutil

import pytest

from api.v1.api_pb2 import Sdk, SDK_JAVA, SDK_GO, STATUS_UNSPECIFIED, \
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST
from cd_helper import CDHelper
from config import Config
from helper import Example, Tag


@pytest.fixture
def delete_temp_folder():
    """
    Create temp folder for tests with storing files
    """
    yield delete_temp_folder
    if os.path.exists(Config.TEMP_FOLDER):
        shutil.rmtree(Config.TEMP_FOLDER)


@pytest.fixture
def upload_blob():
    """
    Fake method for mocking
    Returns: None
    """
    pass


def test__get_gcs_object_name():
    """
      Test getting the path where file will be stored at the bucket
    """
    expected_path = "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_UNIT_TEST/base_folder"
    expected_result = "%s/%s" % (expected_path, "file.java")
    expected_result_with_extension = "%s/%s" % (expected_path, "file.output")
    assert CDHelper()._get_gcs_object_name(
        SDK_JAVA, PRECOMPILED_OBJECT_TYPE_UNIT_TEST, "base_folder",
        "file") == expected_result
    assert CDHelper()._get_gcs_object_name(
        SDK_JAVA,
        PRECOMPILED_OBJECT_TYPE_UNIT_TEST,
        "base_folder",
        "file",
        "output") == expected_result_with_extension


def test__write_to_local_fs(delete_temp_folder):
    """
    Test writing code of an example, output and meta info to
    the filesystem (in temp folder)
    Args:
        delete_temp_folder: python fixture to clean up temp folder
        after method execution
    """
    object_meta = {
        "name": "name",
        "description": "description",
        "multifile": False,
        "categories": ["category-1", "category-2"],
        "pipeline_options": "--option option"
    }
    example = Example(
        name="name",
        pipeline_id="pipeline_id",
        sdk=SDK_JAVA,
        filepath="filepath",
        code="code_of_example",
        output="output_of_example",
        status=STATUS_UNSPECIFIED,
        tag=Tag(**object_meta),
        link="link")
    result_filepath = "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_UNSPECIFIED/name"
    expected_result = {
        "%s/%s" % (result_filepath, "name.java"): "%s/%s/%s" %
                                                  ("temp/pipeline_id", result_filepath, "name.java"),
        "%s/%s" % (result_filepath, "name.output"): "%s/%s/%s" %
                                                    ("temp/pipeline_id", result_filepath, "name.output"),
        "%s/%s" % (result_filepath, "name.log"): "%s/%s/%s" %
                                                 ("temp/pipeline_id", result_filepath, "name.log"),
        "%s/%s" % (result_filepath, "name.graph"): "%s/%s/%s" %
                                                   ("temp/pipeline_id", result_filepath, "name.graph"),
        "%s/%s" % (result_filepath, "meta.info"): "%s/%s/%s" %
                                                  ("temp/pipeline_id", result_filepath, "meta.info"),
    }
    assert CDHelper()._write_to_local_fs(example) == expected_result


def test__save_to_cloud_storage(mocker):
    """
    Test saving examples, outputs and meta to bucket
    Args:
        mocker: mocker fixture from pytest-mocker
    """
    expected_cloud_path = "SDK_JAVA/Example/example.java"
    object_meta = {
        "name": "name",
        "description": "description",
        "multifile": False,
        "categories": ["category-1", "category-2"],
        "pipeline_options": "--option option",
        "default_example": True
    }
    upload_blob_mock = mocker.patch(
        "cd_helper.CDHelper._upload_blob", return_value=upload_blob)
    write_to_os_mock = mocker.patch(
        "cd_helper.CDHelper._write_to_local_fs",
        return_value={expected_cloud_path: ""})
    example = Example(
        name="name",
        pipeline_id="pipeline_id",
        sdk=SDK_JAVA,
        filepath="filepath",
        code="code_of_example",
        output="output_of_example",
        status=STATUS_UNSPECIFIED,
        tag=Tag(**object_meta),
        link="link")

    CDHelper()._save_to_cloud_storage([example])
    write_to_os_mock.assert_called_with(example)
    upload_blob_mock.assert_called_with(
        source_file="", destination_blob_name=expected_cloud_path)


def test__write_default_example_path_to_local_fs(delete_temp_folder):
    """
      Test writing default example link of sdk to
      the filesystem (in temp folder)
      Args:
          delete_temp_folder: python fixture to clean up temp folder
          after method execution
    """
    sdk = Sdk.Name(SDK_GO)
    default_example_path = "SDK_GO/PRECOMPILED_OBJECT_TYPE_EXAMPLE/WordCount"
    expected_result = str(pathlib.Path(sdk, Config.DEFAULT_PRECOMPILED_OBJECT))
    cloud_path = CDHelper()._write_default_example_path_to_local_fs(
        default_example_path)
    assert cloud_path == expected_result
    assert os.path.exists(os.path.join("temp", cloud_path))


def test__clear_temp_folder():
    if not os.path.exists(Config.TEMP_FOLDER):
        os.mkdir(Config.TEMP_FOLDER)
    CDHelper()._clear_temp_folder()
    assert os.path.exists(Config.TEMP_FOLDER) is False
