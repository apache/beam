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
import mock

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.testing.test_pipeline import TestPipeline


class BigQueryBigLakeTest(unittest.TestCase):
  """Test BigLake configuration support in BigQuery Storage Write API."""

  def test_storage_write_to_bigquery_with_biglake_config(self):
    """Test that StorageWriteToBigQuery accepts bigLakeConfiguration parameter."""
    big_lake_config = {
        'connectionId': 'projects/test-project/locations/us/connections/test-connection',
        'storageUri': 'gs://test-bucket/test-path',
        'fileFormat': 'parquet',
        'tableFormat': 'iceberg'
    }
    
    # Test that the constructor accepts the bigLakeConfiguration parameter
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=big_lake_config
    )
    
    # Verify the configuration is stored
    self.assertEqual(transform._big_lake_configuration, big_lake_config)

  def test_storage_write_to_bigquery_without_biglake_config(self):
    """Test that StorageWriteToBigQuery works without bigLakeConfiguration parameter."""
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table'
    )
    
    # Verify the configuration is None by default
    self.assertIsNone(transform._big_lake_configuration)

  @mock.patch('apache_beam.io.gcp.bigquery.SchemaAwareExternalTransform')
  def test_biglake_config_passed_to_external_transform(self, mock_external_transform):
    """Test that bigLakeConfiguration is passed to the external transform."""
    big_lake_config = {
        'connectionId': 'projects/test-project/locations/us/connections/test-connection',
        'storageUri': 'gs://test-bucket/test-path'
    }
    
    with TestPipeline() as p:
      input_data = p | 'Create' >> beam.Create([
          beam.Row(name='Alice', age=30),
          beam.Row(name='Bob', age=25)
      ])
      
      transform = bigquery.StorageWriteToBigQuery(
          table='test-project:test_dataset.test_table',
          big_lake_configuration=big_lake_config
      )
      
      # Apply the transform
      _ = input_data | transform
    
    # Verify that SchemaAwareExternalTransform was called with bigLakeConfiguration
    mock_external_transform.assert_called_once()
    call_kwargs = mock_external_transform.call_args[1]
    self.assertEqual(call_kwargs['big_lake_configuration'], big_lake_config)

  def test_biglake_config_validation(self):
    """Test validation of bigLakeConfiguration parameters."""
    # Test with minimal required configuration
    minimal_config = {
        'connectionId': 'projects/test-project/locations/us/connections/test-connection',
        'storageUri': 'gs://test-bucket/test-path'
    }
    
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=minimal_config
    )
    
    self.assertEqual(transform._big_lake_configuration, minimal_config)
    
    # Test with full configuration
    full_config = {
        'connectionId': 'projects/test-project/locations/us/connections/test-connection',
        'storageUri': 'gs://test-bucket/test-path',
        'fileFormat': 'parquet',
        'tableFormat': 'iceberg'
    }
    
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        big_lake_configuration=full_config
    )
    
    self.assertEqual(transform._big_lake_configuration, full_config)


if __name__ == '__main__':
  unittest.main()