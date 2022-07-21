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

import unittest
from unittest.mock import MagicMock, ANY

import mock
import pytest
from mock.mock import call

from datastore_client import DatastoreClient, DatastoreException
from api.v1.api_pb2 import SDK_JAVA
from test_utils import _get_examples

"""
Unit tests for the Cloud Datastore client
"""


class TestDatastoreClient(unittest.TestCase):

    @mock.patch("config.Config.GOOGLE_CLOUD_PROJECT")
    @mock.patch("google.cloud.datastore.Client")
    def test_save_to_cloud_datastore_when_schema_version_not_found(self, _, mock_config_project):
        """
        Test saving examples to the cloud datastore when the schema version not found
        """
        mock_config_project.return_value = "MOCK_PROJECT_ID"
        with pytest.raises(DatastoreException, match="Schema versions not found. Schema versions must be downloaded during application startup"):
            examples = _get_examples(1)
            client = DatastoreClient()
            client.save_to_cloud_datastore(examples, SDK_JAVA)

    def test_save_to_cloud_datastore_when_google_cloud_project_id_not_set(self):
        """
        Test saving examples to the cloud datastore when the Google Cloud Project ID is not set
        """
        with pytest.raises(KeyError, match="GOOGLE_CLOUD_PROJECT environment variable should be specified in os"):
            DatastoreClient()

    @mock.patch("datastore_client.DatastoreClient._get_all_examples")
    @mock.patch("datastore_client.DatastoreClient._get_actual_schema_version_key")
    @mock.patch("config.Config.GOOGLE_CLOUD_PROJECT")
    @mock.patch("google.cloud.datastore.Client")
    def test_save_to_cloud_datastore_in_the_usual_case(self, mock_client, mock_config_project, mock_get_schema, mock_get_examples):
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
        client.save_to_cloud_datastore(examples, SDK_JAVA)
        mock_client.assert_called_once()
        mock_get_schema.assert_called_once()
        mock_get_examples.assert_called_once()
        calls = [call().key('pg_sdks', 'SDK_JAVA'),
                 call().key('pg_examples', 'SDK_JAVA_MOCK_NAME_0'),
                 call().key('pg_snippets', 'SDK_JAVA_MOCK_NAME_0'),
                 call().key('pg_pc_objects', 'SDK_JAVA_MOCK_NAME_0_OUTPUT'),
                 call().key('pg_files', 'SDK_JAVA_MOCK_NAME_0_0'),
                 call().put_multi(ANY),
                 call().put_multi(ANY),
                 call().put_multi(ANY),
                 call().put_multi(ANY)]
        mock_client.assert_has_calls(calls, any_order=False)
        mock_client.delete_multi.assert_not_called()
