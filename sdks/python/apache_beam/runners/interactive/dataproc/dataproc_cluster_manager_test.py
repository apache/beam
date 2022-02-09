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

# pytype: skip-file

import unittest
from unittest.mock import patch

try:
  from google.cloud import dataproc_v1
  from apache_beam.runners.interactive.dataproc import dataproc_cluster_manager
except ImportError:
  _dataproc_imported = False
else:
  _dataproc_imported = True


class MockException(Exception):
  def __init__(self, code=-1):
    self.code = code


@unittest.skipIf(not _dataproc_imported, 'dataproc package was not imported.')
class DataprocClusterManagerTest(unittest.TestCase):
  """Unit test for DataprocClusterManager"""
  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(409))
  def test_create_cluster_default_already_exists(self, mock_cluster_client):
    """
    Tests that no exception is thrown when a cluster already exists,
    but is using DataprocClusterManager.DEFAULT_NAME.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='INFO') as context_manager:
      cluster_manager.create_cluster({})
      self.assertTrue(
          'Cluster {} already exists'.format(cluster_manager.DEFAULT_NAME) in
          context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(409))
  def test_create_cluster_custom_already_exists(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a cluster already exists,
    but is using a user-specified name.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager(
        cluster_name='test-cluster')
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.create_cluster, {})
      self.assertTrue('Cluster already exists' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.create_cluster',
      side_effect=MockException(403))
  def test_create_cluster_permission_denied(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a user is trying to write to
    a project while having insufficient permissions.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
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
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
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
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(MockException, cluster_manager.create_cluster, {})
      self.assertTrue('Unable to create cluster' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException(403))
  def test_cleanup_permission_denied(self, mock_cluster_client):
    """
    Tests that an exception is thrown when a user is trying to delete
    a project that they have insufficient permissions for.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.cleanup)
      self.assertTrue(
          'Due to insufficient project permissions' in
          context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException(404))
  def test_cleanup_does_not_exist(self, mock_cluster_client):
    """
    Tests that an exception is thrown when cleanup attempts to delete
    a cluster that does not exist.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(ValueError, cluster_manager.cleanup)
      self.assertTrue('Cluster does not exist' in context_manager.output[0])

  @patch(
      'google.cloud.dataproc_v1.ClusterControllerClient.delete_cluster',
      side_effect=MockException())
  def test_cleanup_other_exception(self, mock_cluster_client):
    """
    Tests that an exception is thrown when the exception is not handled by
    any other case under cleanup.
    """
    cluster_manager = dataproc_cluster_manager.DataprocClusterManager()
    from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import _LOGGER
    with self.assertLogs(_LOGGER, level='ERROR') as context_manager:
      self.assertRaises(MockException, cluster_manager.cleanup)
      self.assertTrue('Failed to delete cluster' in context_manager.output[0])


if __name__ == '__main__':
  unittest.main()
