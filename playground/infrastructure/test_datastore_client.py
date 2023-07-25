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
from google.cloud import datastore

from config import Origin, Config
from datastore_client import DatastoreClient, DatastoreException
from models import SdkEnum
from test_utils import _get_examples

"""
Unit tests for the Cloud Datastore client
"""


@mock.patch("google.cloud.datastore.Client")
def test_save_to_cloud_datastore_when_schema_version_not_found(
    mock_datastore_client
):
    """
    Test saving examples to the cloud datastore when the schema version not found
    """
    with pytest.raises(
        DatastoreException,
        match="Schema versions not found. Schema versions must be downloaded during application startup",
    ):
        examples = _get_examples(1)
        client = DatastoreClient("MOCK_PROJECT_ID", Config.DEFAULT_NAMESPACE)
        client.save_to_cloud_datastore(examples, SdkEnum.JAVA, Origin.PG_EXAMPLES)


@pytest.mark.parametrize("is_multifile", [False, True])
@pytest.mark.parametrize("with_kafka", [False, True])
@pytest.mark.parametrize(
    "origin, key_prefix",
    [
        pytest.param(Origin.PG_EXAMPLES, "", id="PG_EXAMPLES"),
        pytest.param(Origin.PG_BEAMDOC, "PG_BEAMDOC_", id="PG_BEAMDOC"),
        pytest.param(Origin.TB_EXAMPLES, "TB_EXAMPLES_", id="TB_EXAMPLES"),
    ],
)
@pytest.mark.parametrize("namespace", [Config.DEFAULT_NAMESPACE, "Staging"])
@mock.patch("datastore_client.DatastoreClient._get_all_examples")
@mock.patch("datastore_client.DatastoreClient._get_actual_schema_version_key")
@mock.patch("google.cloud.datastore.Client")
def test_save_to_cloud_datastore_in_the_usual_case(
    mock_client,
    mock_get_schema,
    mock_get_examples,
    create_test_example,
    origin,
    key_prefix,
    with_kafka,
    is_multifile,
    namespace,
):
    """
    Test saving examples to the cloud datastore in the usual case
    """
    mock_schema_key = MagicMock()
    mock_get_schema.return_value = mock_schema_key
    mock_examples = MagicMock()
    mock_get_examples.return_value = mock_examples

    project_id = "MOCK_PROJECT_ID"

    examples = [create_test_example(is_multifile=is_multifile, with_kafka=with_kafka)]
    client = DatastoreClient(project_id, namespace)
    client.save_to_cloud_datastore(examples, SdkEnum.JAVA, origin)
    mock_client.assert_called_once_with(namespace=namespace, project=project_id)
    mock_client.assert_called_once()
    mock_get_schema.assert_called_once()
    mock_get_examples.assert_called_once()
    calls = [
        call().transaction(),
        call().transaction().__enter__(),
        call().key("pg_sdks", "SDK_JAVA"),
        call().key("pg_examples", key_prefix + "SDK_JAVA_MOCK_NAME"),
        call().put(ANY),
        call().key("pg_snippets", key_prefix + "SDK_JAVA_MOCK_NAME"),
    ]
    if with_kafka:
        calls.append(
            call().key(
                "pg_datasets", "dataset_id_1"
            ),  # used in the nested datasets construction
        )
    calls.extend(
        [
            call().put(ANY),
            call().key("pg_pc_objects", key_prefix + "SDK_JAVA_MOCK_NAME_GRAPH"),
            call().key("pg_pc_objects", key_prefix + "SDK_JAVA_MOCK_NAME_OUTPUT"),
            call().key("pg_pc_objects", key_prefix + "SDK_JAVA_MOCK_NAME_LOG"),
            call().put_multi([ANY, ANY, ANY]),
            call().key("pg_files", key_prefix + "SDK_JAVA_MOCK_NAME_0"),
            call().put(ANY),
        ]
    )
    if is_multifile:
        calls.extend(
            [
                call().key("pg_files", key_prefix + "SDK_JAVA_MOCK_NAME_1"),
                call().key("pg_files", key_prefix + "SDK_JAVA_MOCK_NAME_2"),
                call().put_multi([ANY, ANY]),
            ]
        )
    if with_kafka:
        calls.extend(
            [
                call().key("pg_datasets", "dataset_id_1"),
                call().put_multi([ANY]),
            ]
        )
    calls.append(
        call().transaction().__exit__(None, None, None),
    )

    mock_client.assert_has_calls(calls, any_order=False)
    mock_client.delete_multi.assert_not_called()
