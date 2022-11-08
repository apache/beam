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
from unittest.mock import call

import mock
import pytest

from config import Dataset
from storage_client import StorageClient
from test_utils import _get_examples

"""
Unit tests for the Cloud Storage client
"""


@mock.patch("google.cloud.storage.Client")
@mock.patch.dict(os.environ, {"DATASET_BUCKET_NAME": "MOCK_BUCKET"}, clear=True)
@mock.patch("os.path.isfile", return_value=True)
def test_set_dataset_path_for_examples_when_datasets_are_empty(mock_file_check, mock_storage_client):
    """
    Test setting a dataset path for examples when datasets are empty
    """
    examples = _get_examples(2)
    client = StorageClient()
    client.set_dataset_path_for_examples(examples)
    mock_storage_client.bucket.assert_not_called()


@mock.patch("google.cloud.storage.Client")
@mock.patch.dict(os.environ, {"DATASET_BUCKET_NAME": "MOCK_BUCKET"}, clear=True)
def test_set_dataset_path_for_examples_when_dataset_path_is_bad(mock_storage_client):
    """
    Test setting a dataset path for examples when dataset path is wrong
    """
    with pytest.raises(FileNotFoundError):
        examples = _get_examples_with_datasets(1)
        client = StorageClient()
        client.set_dataset_path_for_examples(examples)


@mock.patch("google.cloud.storage.Client")
@mock.patch.dict(os.environ, {"DATASET_BUCKET_NAME": "MOCK_BUCKET"}, clear=True)
@mock.patch("os.path.isfile", return_value=True)
def test_set_dataset_path_for_examples_in_the_usual_case(mock_file_check, mock_storage_client):
    """
    Test setting a dataset path for examples in the usual case
    """
    examples = _get_examples_with_datasets(1)
    client = StorageClient()
    client.set_dataset_path_for_examples(examples)
    calls = [call().bucket("MOCK_BUCKET")]
    mock_storage_client.assert_has_calls(calls, any_order=False)


def _get_examples_with_datasets(number_of_examples: int):
    examples = _get_examples(number_of_examples)
    for example in examples:
        datasets = []
        dataset = Dataset(
            format="MOCK_FORMAT",
            location="MOCK_LOCATION",
            name="MOCK_NAME"
        )
        datasets.append(dataset)
        example.datasets = datasets
    return examples
