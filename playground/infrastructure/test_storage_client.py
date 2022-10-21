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
from unittest.mock import MagicMock, call

import pytest
import mock

from config import StorageProps
from storage_client import StorageClient

"""
Unit tests for the Cloud Storage client
"""


def test_upload_dataset_when_dataset_bucket_name_not_set():
    """
    Test uploading a dataset to the cloud storage when the Dataset bucket name is not set
    """
    with pytest.raises(KeyError, match="DATASET_BUCKET_NAME environment variable should be specified in os"):
        StorageClient()


@mock.patch("google.cloud.storage.Client")
@mock.patch("config.StorageProps.DATASET_BUCKET_NAME")
def test_upload_dataset_in_the_usual_case(mock_dataset_bucket_name, mock_storage_client):
    """
    Test uploading a dataset to the cloud storage in the usual case
    """
    mock_dataset_bucket_name.return_value = "MOCK_BUCKET"

    client = StorageClient()
    url = client.upload_dataset("MOCK_FILE_NAME")
    assert url is not None
    calls = [call().bucket(StorageProps.DATASET_BUCKET_NAME)]
    mock_storage_client.assert_has_calls(calls, any_order=False)
