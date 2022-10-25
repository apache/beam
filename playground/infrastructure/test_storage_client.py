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

import pytest
import mock

from storage_client import StorageClient

"""
Unit tests for the Cloud Storage client
"""


@mock.patch("google.cloud.storage.Client")
def test_upload_dataset_when_dataset_bucket_name_not_set(mock_storage_client):
    """
    Test uploading a dataset to the cloud storage when the Dataset bucket name is not set
    """
    with pytest.raises(KeyError, match="DATASET_BUCKET_NAME environment variable should be specified in os"):
        client = StorageClient()
        client.upload_dataset("MOCK_FILE_NAME")


@mock.patch.dict(os.environ, {"DATASET_BUCKET_NAME": "MOCK_BUCKET"}, clear=True)
@mock.patch("google.cloud.storage.Client")
def test_upload_dataset_in_the_usual_case(mock_storage_client):
    """
    Test uploading a dataset to the cloud storage in the usual case
    """

    client = StorageClient()
    url = client.upload_dataset("MOCK_FILE_NAME")
    assert url is not None
    calls = [call().bucket("MOCK_BUCKET")]
    mock_storage_client.assert_has_calls(calls, any_order=False)
