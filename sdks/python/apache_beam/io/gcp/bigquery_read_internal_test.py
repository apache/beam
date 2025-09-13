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

"""Unit tests for BigQuery read internal module."""

import unittest
from unittest import mock

from apache_beam.io.gcp import bigquery_read_internal
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider

try:
  from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
except ImportError:
  DatasetReference = None


class BigQueryReadSplitTest(unittest.TestCase):
  """Tests for _BigQueryReadSplit DoFn."""
  def setUp(self):
    if DatasetReference is None:
      self.skipTest('BigQuery dependencies are not installed')
    self.options = PipelineOptions()
    self.gcp_options = self.options.view_as(GoogleCloudOptions)
    self.gcp_options.project = 'test-project'

  def test_get_temp_dataset_project_with_string_temp_dataset(self):
    """Test _get_temp_dataset_project with string temp_dataset."""
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset='temp_dataset_id')

    # Should return the pipeline project when temp_dataset is a string
    self.assertEqual(split._get_temp_dataset_project(), 'test-project')

  def test_get_temp_dataset_project_with_dataset_reference(self):
    """Test _get_temp_dataset_project with DatasetReference temp_dataset."""
    dataset_ref = DatasetReference(
        projectId='custom-project', datasetId='temp_dataset_id')
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset=dataset_ref)

    # Should return the project from DatasetReference
    self.assertEqual(split._get_temp_dataset_project(), 'custom-project')

  def test_get_temp_dataset_project_with_none_temp_dataset(self):
    """Test _get_temp_dataset_project with None temp_dataset."""
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset=None)

    # Should return the pipeline project when temp_dataset is None
    self.assertEqual(split._get_temp_dataset_project(), 'test-project')

  def test_get_temp_dataset_project_with_value_provider_project(self):
    """Test _get_temp_dataset_project with ValueProvider project."""
    self.gcp_options.project = StaticValueProvider(str, 'vp-project')
    dataset_ref = DatasetReference(
        projectId='custom-project', datasetId='temp_dataset_id')
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset=dataset_ref)

    # Should still return the project from DatasetReference
    self.assertEqual(split._get_temp_dataset_project(), 'custom-project')

  @mock.patch('apache_beam.io.gcp.bigquery_tools.BigQueryWrapper')
  def test_setup_temporary_dataset_uses_correct_project(self, mock_bq_wrapper):
    """Test that _setup_temporary_dataset uses the correct project."""
    dataset_ref = DatasetReference(
        projectId='custom-project', datasetId='temp_dataset_id')
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset=dataset_ref)

    # Mock the BigQueryWrapper instance
    mock_bq = mock.Mock()
    mock_bq.get_query_location.return_value = 'US'

    # Mock ReadFromBigQueryRequest
    mock_element = mock.Mock()
    mock_element.query = 'SELECT * FROM table'
    mock_element.use_standard_sql = True

    # Call _setup_temporary_dataset
    split._setup_temporary_dataset(mock_bq, mock_element)

    # Verify that create_temporary_dataset was called with the custom project
    mock_bq.create_temporary_dataset.assert_called_once_with(
        'custom-project', 'US', kms_key=None)
    # Verify that get_query_location was called with the pipeline project
    mock_bq.get_query_location.assert_called_once_with(
        'test-project', 'SELECT * FROM table', False)

  @mock.patch('apache_beam.io.gcp.bigquery_tools.BigQueryWrapper')
  def test_finish_bundle_uses_correct_project(self, mock_bq_wrapper):
    """Test that finish_bundle uses the correct project for cleanup."""
    dataset_ref = DatasetReference(
        projectId='custom-project', datasetId='temp_dataset_id')
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset=dataset_ref)

    # Mock the BigQueryWrapper instance
    mock_bq = mock.Mock()
    mock_bq.created_temp_dataset = True
    split.bq = mock_bq

    # Call finish_bundle
    split.finish_bundle()

    # Verify that clean_up_temporary_dataset was called with the custom project
    mock_bq.clean_up_temporary_dataset.assert_called_once_with('custom-project')

  @mock.patch('apache_beam.io.gcp.bigquery_tools.BigQueryWrapper')
  def test_setup_temporary_dataset_with_string_temp_dataset(
      self, mock_bq_wrapper):
    """Test _setup_temporary_dataset with string temp_dataset uses pipeline
    project."""
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset='temp_dataset_id')

    # Mock the BigQueryWrapper instance
    mock_bq = mock.Mock()
    mock_bq.get_query_location.return_value = 'US'

    # Mock ReadFromBigQueryRequest
    mock_element = mock.Mock()
    mock_element.query = 'SELECT * FROM table'
    mock_element.use_standard_sql = True

    # Call _setup_temporary_dataset
    split._setup_temporary_dataset(mock_bq, mock_element)

    # Verify that create_temporary_dataset was called with the pipeline project
    mock_bq.create_temporary_dataset.assert_called_once_with(
        'test-project', 'US', kms_key=None)

  @mock.patch('apache_beam.io.gcp.bigquery_tools.BigQueryWrapper')
  def test_finish_bundle_with_string_temp_dataset(self, mock_bq_wrapper):
    """Test finish_bundle with string temp_dataset uses pipeline project."""
    split = bigquery_read_internal._BigQueryReadSplit(
        options=self.options, temp_dataset='temp_dataset_id')

    # Mock the BigQueryWrapper instance
    mock_bq = mock.Mock()
    mock_bq.created_temp_dataset = True
    split.bq = mock_bq

    # Call finish_bundle
    split.finish_bundle()

    # Verify that clean_up_temporary_dataset was called with the pipeline
    # project
    mock_bq.clean_up_temporary_dataset.assert_called_once_with('test-project')


if __name__ == '__main__':
  unittest.main()
