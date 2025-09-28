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

"""Unit tests for BigQuery BigLake configuration."""

import unittest
from unittest import mock

from apache_beam.io.gcp import bigquery


@mock.patch('apache_beam.io.gcp.bigquery.BeamJarExpansionService')
class BigQueryBigLakeTest(unittest.TestCase):
  """Test BigLake configuration support in BigQuery Storage Write API."""
  def test_storage_write_to_bigquery_with_biglake_config(
      self, mock_expansion_service):
    """Test that StorageWriteToBigQuery accepts bigLakeConfiguration."""
    big_lake_config = {
        'connectionId': (
            'projects/test-project/locations/us/connections/test-connection'),
        'storageUri': 'gs://test-bucket/test-path',
        'fileFormat': 'parquet',
        'tableFormat': 'iceberg'
    }

    # Test that the constructor accepts the bigLakeConfiguration parameter
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=big_lake_config)

    # Verify the configuration is stored
    self.assertEqual(transform._big_lake_configuration, big_lake_config)

  def test_storage_write_to_bigquery_without_biglake_config(
      self, mock_expansion_service):
    """Test that StorageWriteToBigQuery works without bigLakeConfiguration."""
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table')

    # Verify the configuration is None by default
    self.assertIsNone(transform._big_lake_configuration)

  def test_biglake_config_passed_to_external_transform(
      self, mock_expansion_service):
    """Test that StorageWriteToBigQuery accepts bigLakeConfiguration."""
    big_lake_config = {
        'connection_id': 'projects/my-project/locations/us/connections/my-conn',
        'table_format': 'ICEBERG'
    }

    # Mock the expansion service to avoid JAR dependency
    mock_expansion_service.return_value = mock.MagicMock()

    # Create the transform
    transform = bigquery.StorageWriteToBigQuery(
        table='my-project:my_dataset.my_table',
        big_lake_configuration=big_lake_config)

    # Verify the big_lake_configuration is stored correctly
    self.assertEqual(transform._big_lake_configuration, big_lake_config)

    # Verify that the transform has the expected identifier
    self.assertEqual(
        transform.IDENTIFIER,
        "beam:schematransform:org.apache.beam:bigquery_storage_write:v2")

    # Verify that the expansion service was created (mocked)
    mock_expansion_service.assert_called_once_with(
        'sdks:java:io:google-cloud-platform:expansion-service:build')

  def test_biglake_config_validation(self, mock_expansion_service):
    """Test validation of bigLakeConfiguration parameters."""
    # Test with minimal required configuration
    minimal_config = {
        'connectionId': (
            'projects/test-project/locations/us/connections/test-connection'),
        'storageUri': 'gs://test-bucket/test-path'
    }

    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=minimal_config)

    self.assertEqual(transform._big_lake_configuration, minimal_config)

    # Test with full configuration
    full_config = {
        'connectionId': (
            'projects/test-project/locations/us/connections/test-connection'),
        'storageUri': 'gs://test-bucket/test-path',
        'fileFormat': 'parquet',
        'tableFormat': 'iceberg'
    }

    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=full_config)

    self.assertEqual(transform._big_lake_configuration, full_config)


if __name__ == '__main__':
  unittest.main()
