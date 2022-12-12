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

from unittest.mock import MagicMock, ANY

import mock
import pytest
from mock.mock import call
from config import Origin

from datastore_client import DatastoreClient, DatastoreException
from api.v1.api_pb2 import SDK_JAVA
from test_utils import _get_examples

"""
Unit tests for the Cloud Datastore client
"""


@mock.patch("config.Config.GOOGLE_CLOUD_PROJECT")
@mock.patch("google.cloud.datastore.Client")
def test_save_to_cloud_datastore_when_schema_version_not_found(
    mock_datastore_client, mock_config_project
):
    """
    Test saving examples to the cloud datastore when the schema version not found
    """
    mock_config_project.return_value = "MOCK_PROJECT_ID"
    with pytest.raises(
        DatastoreException,
        match="Schema versions not found. Schema versions must be downloaded during application startup",
    ):
        examples = _get_examples(1)
        client = DatastoreClient()
        client.save_to_cloud_datastore(examples, SDK_JAVA, Origin.PG_EXAMPLES)


def test_save_to_cloud_datastore_when_google_cloud_project_id_not_set():
    """
    Test saving examples to the cloud datastore when the Google Cloud Project ID is not set
    """
    with pytest.raises(
        KeyError,
        match="GOOGLE_CLOUD_PROJECT environment variable should be specified in os",
    ):
        DatastoreClient()


@pytest.mark.parametrize(
    "origin, key_prefix",
    [
        pytest.param(Origin.PG_EXAMPLES, "", id="PG_EXAMPLES"),
        pytest.param(Origin.TB_EXAMPLES, "TB_EXAMPLES_", id="TB_EXAMPLES"),
    ],
)
@mock.patch("datastore_client.DatastoreClient._get_all_examples")
@mock.patch("datastore_client.DatastoreClient._get_actual_schema_version_key")
@mock.patch("config.Config.GOOGLE_CLOUD_PROJECT")
@mock.patch("google.cloud.datastore.Client")
def test_save_to_cloud_datastore_in_the_usual_case(
    mock_client,
    mock_config_project,
    mock_get_schema,
    mock_get_examples,
    origin,
    key_prefix,
):
    """
    Test saving examples to the cloud datastore in the usual case
    """
    mock_schema_key = MagicMock()
    mock_get_schema.return_value = mock_schema_key
    mock_examples = MagicMock()
    mock_get_examples.return_value = mock_examples
    mock_config_project.return_value = "MOCK_PROJECT_ID"

    examples = _get_examples(1)
    client = DatastoreClient()
    client.save_to_cloud_datastore(examples, SDK_JAVA, origin)
    mock_client.assert_called_once()
    mock_get_schema.assert_called_once()
    mock_get_examples.assert_called_once()
    calls = [
        call().transaction(),
        call().transaction().__enter__(),
        call().key("pg_sdks", "SDK_JAVA"),
        call().key("pg_examples", key_prefix + "SDK_JAVA_MOCK_NAME_0"),
        call().put(ANY),
        call().key("pg_snippets", key_prefix + "SDK_JAVA_MOCK_NAME_0"),
        call().put(ANY),
        call().key("pg_pc_objects", key_prefix + "SDK_JAVA_MOCK_NAME_0_OUTPUT"),
        call().put_multi([ANY]),
        call().key("pg_files", key_prefix + "SDK_JAVA_MOCK_NAME_0_0"),
        call().put(ANY),
        call().transaction().__exit__(None, None, None),
    ]
    mock_client.assert_has_calls(calls, any_order=False)
    mock_client.delete_multi.assert_not_called()
