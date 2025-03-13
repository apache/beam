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

"""Tests for apache_beam.runners.interactive.dataproc.
dataproc_cluster_manager."""
# pytype: skip-file

import unittest
from unittest.mock import patch

from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import DataprocClusterManager
from apache_beam.runners.interactive.dataproc.types import ClusterMetadata

try:
  from google.cloud import dataproc_v1  # pylint: disable=unused-import
except ImportError:
  _dataproc_imported = False
else:
  _dataproc_imported = True


class MockProperty:
  def __init__(self, property, value):
    object.__setattr__(self, property, value)


class MockException(Exception):
  def __init__(self, code=-1):
    self.code = code


class MockCluster:
  def __init__(self, config_bucket=None):
    self.config = MockProperty('config_bucket', config_bucket)
    self.status = MockProperty('state', MockProperty('name', None))


class MockFileSystem:
  def _list(self, dir=None):
    return [
        MockProperty(
            'path', 'test-path/dataproc-initialization-script-0_output')
    ]

  def open(self, dir=None):
    return MockFileIO('test-line Found Web Interface test-master-url' \
    ' of application \'test-app-id\'.\n')


class MockFileIO:
  def __init__(self, contents):
    self.contents = contents

  def readlines(self):
    return [self.contents.encode('utf-8')]


@unittest.skipIf(not _dataproc_imported, 'dataproc package was not imported.')
class DataprocClusterManagerTest(unittest.TestCase):
  """Unit test for DataprocClusterManager"""
  def setUp(self):
    self.patcher = patch(
        'apache_beam.runners.interactive.interactive_environment.current_env')
    self.m_env = self.patcher.start()
    self.m_env().clusters = ib.Clusters()
    self.m_env().options.cache_root = 'gs://fake'

  def tearDown(self):
    self.m_env().options.cache_root = None
    self.patcher.stop()

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(409))
  def test_create_cluster_default_already_exists(self, mock_cluster_client):
    """
    Tests that no exception is thrown when a cluster already exists,
    but is using ie.current_env().clusters.default_cluster_name.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='INFO') as context_manager:
      cluster_manager.create_cluster({})
      self.assertTrue('already exists' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(403))
  def test_create_cluster_permission_denied(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a user is trying to write to
    a project while having insufficient permissions.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.create_cluster, {})
      self.assertTrue(
          'Due to insufficient project permissions' in
          context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(501))
  def test_create_cluster_region_does_not_exist(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a user specifies a region
    that does not exist.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.create_cluster, {})
      self.assertTrue('Invalid region provided' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException())
  def test_create_cluster_other_exception(self, mock_cluster_client):
    """
    Tests that an exception is thrown when the exception is not handled by
    any other case under _create_cluster.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(MockException, cluster_manager.create_cluster, {})
      self.assertTrue('Unable to create cluster' in context_manager.output[0])

  @patch(
      'apache_beam.runners.interactive.dataproc.dataproc_cluster_manager.'
      'DataprocClusterManager.cleanup_staging_files',
      return_value=None)
  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException(403))
  def test_cleanup_permission_denied(self, mock_cluster_client, mock_cleanup):
    """
    Tests that an exception is thrown when a user is trying to delete
    a project that they have insufficient permissions for.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.cleanup)
      self.assertTrue(
          'Due to insufficient project permissions' in
          context_manager.output[0])

  @patch(
      'apache_beam.runners.interactive.dataproc.dataproc_cluster_manager.'
      'DataprocClusterManager.cleanup_staging_files',
      return_value=None)
  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException(404))
  def test_cleanup_does_not_exist(self, mock_cluster_client, mock_cleanup):
    """
    Tests that an exception is thrown when cleanup attempts to delete
    a cluster that does not exist.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.cleanup)
      self.assertTrue('Cluster does not exist' in context_manager.output[0])

  @patch(
      'apache_beam.runners.interactive.dataproc.dataproc_cluster_manager.'
      'DataprocClusterManager.cleanup_staging_files',
      return_value=None)
  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException())
  def test_cleanup_other_exception(self, mock_cluster_client, mock_cleanup):
    """
    Tests that an exception is thrown when the exception is not handled by
    any other case under cleanup.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(MockException, cluster_manager.cleanup)
      self.assertTrue('Failed to delete cluster' in context_manager.output[0])

  @patch(
      'apache_beam.io.gcp.gcsfilesystem.GCSFileSystem._list',
      return_value=[
          MockProperty(
              'path',
              'gs://test-bucket/google-cloud-dataproc-metainfo'
              '/test-cluster/item')
      ])
  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.get_cluster',
      return_value=MockCluster('test-bucket'))
  def test_get_staging_location(self, mock_cluster_client, mock_list):
    """
    Test to receive a mock staging location successfully under
    get_staging_location.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project',
        region='test-region',
        cluster_name='test-cluster')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    self.assertEqual(
        cluster_manager.get_staging_location(),
        'gs://test-bucket/google-cloud-dataproc-metainfo/')

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.get_cluster',
      side_effect=MockException())
  def test_get_staging_location_exception(self, mock_cluster_client):
    """
    Test to catch when an error is raised inside get_staging_location.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project',
        region='test-region',
        cluster_name='test-cluster')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    with self.assertRaises(MockException):
      cluster_manager.get_staging_location()

  @patch(
      'apache_beam.runners.interactive.dataproc.dataproc_cluster_manager.'
      'DataprocClusterManager.get_cluster_details',
      return_value=MockProperty(
          'config',
          MockProperty(
              'endpoint_config',
              MockProperty(
                  'http_ports',
                  {'YARN ResourceManager': 'test-resource-manager/yarn/'}))))
  def test_parse_master_url_and_dashboard(self, mock_cluster_details):
    """
    Tests that parse_master_url_and_dashboard properly parses the input
    string and produces a mock master_url and mock dashboard link.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    line = 'test-line Found Web Interface test-master-url' \
    ' of application \'test-app-id\'.\n'
    master_url, dashboard = cluster_manager.parse_master_url_and_dashboard(line)
    self.assertEqual('test-master-url', master_url)
    self.assertEqual(
        'test-resource-manager/gateway/default/yarn/proxy/test-app-id/',
        dashboard)

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.get_cluster',
      side_effect=MockException(403))
  def test_get_cluster_details_permission_denied(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a user is trying to get information
    for a project without sufficient permissions to do so.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(
        _LOGGER,
        level='ERROR') as context_manager, self.assertRaises(ValueError):
      cluster_manager.get_cluster_details()
      self.assertTrue(
          'Due to insufficient project permissions' in
          context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.get_cluster',
      side_effect=MockException(404))
  def test_get_cluster_details_does_not_exist(self, mock_cluster_client):
    """
    Tests that an exception is thrown when cleanup attempts to get information
    for a cluster that does not exist.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(
        _LOGGER,
        level='ERROR') as context_manager, self.assertRaises(ValueError):
      cluster_manager.get_cluster_details()
      self.assertTrue('Cluster does not exist' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.get_cluster',
      side_effect=MockException())
  def test_get_cluster_details_other_exception(self, mock_cluster_client):
    """
    Tests that an exception is thrown when the exception is not handled by
    any other case under get_cluster_details.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(
        _LOGGER,
        level='ERROR') as context_manager, self.assertRaises(MockException):
      cluster_manager.get_cluster_details()
      self.assertTrue(
          'Failed to get information for cluster' in context_manager.output[0])

  @patch(
      'apache_beam.runners.interactive.dataproc.dataproc_cluster_manager.'
      'DataprocClusterManager.parse_master_url_and_dashboard',
      return_value=('test-master-url', 'test-dashboard-link'))
  def test_get_master_url_and_dashboard(self, mock_parse_method):
    """
    Tests that get_master_url_and_dashboard detect the line containing the
    unique substring which identifies the location of the master_url and
    application id of the Flink master.
    """
    cluster_metadata = ClusterMetadata(
        project_id='test-project', region='test-region')
    cluster_manager = DataprocClusterManager(cluster_metadata)
    cluster_manager._fs = MockFileSystem()
    cluster_metadata._staging_directory = 'test-staging-bucket'
    master_url, dashboard = cluster_manager.get_master_url_and_dashboard()
    self.assertEqual(master_url, 'test-master-url')
    self.assertEqual(dashboard, 'test-dashboard-link')


if __name__ == '__main__':
  unittest.main()
