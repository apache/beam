#
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
#

import contextlib
import unittest
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

try:
  from apache_beam.io.gcp.internal.clients import bigquery
  from apache_beam.yaml import integration_tests
except ImportError as exc:
  raise unittest.SkipTest(f"Integration test dependencies not installed: {exc}")


class BigQueryFixtureCleanupTest(unittest.TestCase):
  def assert_dataset_deleted(self, bigquery_wrapper, project):
    bigquery_wrapper.get_or_create_dataset.assert_called_once()
    dataset_id = bigquery_wrapper.get_or_create_dataset.call_args.args[1]
    delete_call = bigquery_wrapper.client.datasets.Delete.call_args
    self.assertIsNotNone(delete_call)
    request, = delete_call.args
    self.assertIsInstance(request, bigquery.BigqueryDatasetsDeleteRequest)
    self.assertEqual(project, request.projectId)
    self.assertEqual(dataset_id, request.datasetId)
    self.assertTrue(request.deleteContents)

  def test_later_fixture_failure_deletes_dataset(self):
    project = "beam-test-project"
    failure = RuntimeError("Bigtable setup failed")
    bigquery_wrapper = Mock()
    fail_later_fixture = MagicMock()
    fail_later_fixture.return_value.__enter__.side_effect = failure

    with patch.object(integration_tests.BigQueryWrapper,
                      "from_pipeline_options",
                      return_value=bigquery_wrapper):
      with self.assertRaises(RuntimeError) as raised:
        with contextlib.ExitStack() as stack:
          stack.enter_context(integration_tests.temp_bigquery_table(project))
          stack.enter_context(fail_later_fixture())

    self.assertIs(failure, raised.exception)
    self.assert_dataset_deleted(bigquery_wrapper, project)

  def test_table_setup_failure_deletes_dataset(self):
    project = "beam-test-project"
    failure = RuntimeError("table creation failed")
    bigquery_wrapper = Mock()
    bigquery_wrapper.get_or_create_table.side_effect = failure

    with patch.object(integration_tests.BigQueryWrapper,
                      "from_pipeline_options",
                      return_value=bigquery_wrapper):
      with self.assertRaises(RuntimeError) as raised:
        with integration_tests.temp_bigquery_table(project,
                                                   table_schema="id:INTEGER"):
          self.fail("Table setup must fail before yielding the fixture")

    self.assertIs(failure, raised.exception)
    self.assert_dataset_deleted(bigquery_wrapper, project)


class BigtableFixtureZoneFallbackTest(unittest.TestCase):
  def test_falls_back_to_next_zone_on_stockout(self):
    stockout_error = RuntimeError(
        "499 Operation successfully rolled back : Not enough servers to "
        "allocate the requested nodes. We have temporarily sold out of "
        "Cloud Bigtable in this zone.")
    mock_client = Mock()
    first_instance = Mock()
    first_op = Mock()
    first_op.result.side_effect = stockout_error
    first_instance.create.return_value = first_op

    second_instance = Mock()
    second_op = Mock()
    second_instance.create.return_value = second_op

    mock_client.instance.side_effect = [first_instance, second_instance]

    with patch.object(integration_tests.client,
                      "Client",
                      return_value=mock_client):
      with integration_tests.temp_bigtable_table("beam-test-project") as bt_id:
        self.assertTrue(bt_id.startswith("bt-write-tests-"))

    first_instance.cluster.assert_called_once_with(
        "test-cluster", "us-central1-a")
    second_instance.cluster.assert_called_once_with(
        "test-cluster", "us-central1-b")
    second_instance.delete.assert_called_once()


if __name__ == '__main__':
  unittest.main()
