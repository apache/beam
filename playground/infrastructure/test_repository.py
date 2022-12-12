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
import mock
import pytest

from models import Dataset, DatasetFormat, DatasetLocation
from repository import set_dataset_file_name
from test_utils import _get_examples

"""
Unit tests for the Cloud Storage client
"""


@mock.patch("os.path.isfile", return_value=True)
def test_set_dataset_path_for_examples(mock_file_check):
    examples = _get_examples_with_datasets(3)
    set_dataset_file_name(examples)
    for example in examples:
        assert len(example.tag.datasets) > 0
        assert example.tag.datasets.popitem()[1].file_name == "MOCK_NAME.json"


@mock.patch("os.path.isfile", return_value=False)
def test_set_dataset_path_for_examples_when_path_is_invalid(mock_file_check):
    with pytest.raises(FileNotFoundError):
        examples = _get_examples_with_datasets(1)
        set_dataset_file_name(examples)


def _get_examples_with_datasets(number_of_examples: int):
    examples = _get_examples(number_of_examples)
    for example in examples:
        dataset = Dataset(
            format=DatasetFormat.JSON,
            location=DatasetLocation.LOCAL,
        )
        example.tag.datasets["MOCK_NAME"] = dataset
    return examples
