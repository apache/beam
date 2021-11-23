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
import shutil

import pytest

from cd_helper import CDHelper
from api.v1.api_pb2 import SDK_JAVA, STATUS_UNSPECIFIED
from config import Config
from helper import Example, Tag


@pytest.fixture
def delete_temp_folder():
    """Create temp folder for tests with storing files"""
    yield delete_temp_folder
    if os.path.exists(Config.TEMP_FOLDER):
        shutil.rmtree(Config.TEMP_FOLDER)


def upload_blob():
    """
    Fake method for mocking
    Returns: None

    """
    return None


def test__get_cloud_file_name():
    """
    Test getting the path where file will be stored at the bucket
    Returns:

    """
    expected_result = "SDK_JAVA/base_folder/file.java"
    expected_result_with_extension = "SDK_JAVA/base_folder/file.output"
    assert CDHelper()._get_cloud_file_name(SDK_JAVA, "base_folder", "file") == expected_result
    assert CDHelper()._get_cloud_file_name(SDK_JAVA, "base_folder", "file", "output") == expected_result_with_extension


def test__write_to_os(delete_temp_folder):
    """
    Test writing code of an example, output and meta info to the filesystem (in temp folder)
    Args:
        delete_temp_folder:

    Returns:

    """
    object_meta = {'name': 'name', 'description': 'description', 'multifile': False,
                   'categories': ['category-1', 'category-2']}
    example = Example("name", "pipeline_id", SDK_JAVA, "filepath", "code_of_example",
                      "output_of_example", STATUS_UNSPECIFIED, Tag(**object_meta))
    expected_result = {'SDK_JAVA/name/name.java': 'temp/pipeline_id/SDK_JAVA/name/name.java',
                       'SDK_JAVA/name/name.output': 'temp/pipeline_id/SDK_JAVA/name/name.output',
                       'SDK_JAVA/name/meta.info': 'temp/pipeline_id/SDK_JAVA/name/meta.info'}
    assert CDHelper()._write_to_os(example) == expected_result

    example.tag.name = None
    with pytest.raises(KeyError):
        CDHelper()._write_to_os(example)  # "tag should contain 'name' field"


def test__save_to_cloud_storage(mocker):
    """
    Test saving examples, outputs and meta to bucket
    Args:
        mocker:

    Returns:

    """
    mocker.patch(
        'cd_helper.CDHelper._upload_blob',
        return_value=upload_blob
    )
    mocker.patch(
        'cd_helper.CDHelper._write_to_os',
        return_value={"": ""}
    )
    example = Example("name", "pipeline_id", SDK_JAVA, "filepath", "code_of_example",
                      "output_of_example", STATUS_UNSPECIFIED, None)

    CDHelper()._save_to_cloud_storage([example])
