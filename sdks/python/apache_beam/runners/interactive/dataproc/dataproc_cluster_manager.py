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

import logging
from typing import Optional

try:
  from google.cloud import dataproc_v1
except ImportError:
  raise ImportError(
      'Google Cloud Dataproc not supported for this execution environment.')

_LOGGER = logging.getLogger(__name__)


class DataprocClusterManager:
  """The DataprocClusterManager object simplifies the operations
  required for creating and deleting Dataproc clusters for use
  under Interactive Beam.
  """
  DEFAULT_NAME = 'interactive-beam-cluster'

  def __init__(
      self,
      project_id: Optional[str] = None,
      region: Optional[str] = None,
      cluster_name: Optional[str] = None) -> None:
    """Initializes the DataprocClusterManager with properties required
    to interface with the Dataproc ClusterControllerClient.
    """

    self._project_id = project_id
    if region == 'global':
      # The global region is unsupported as it will be eventually deprecated.
      raise ValueError('Clusters in the global region are not supported.')
    elif region:
      self._region = region
    else:
      _LOGGER.warning(
          'No region information was detected, defaulting Dataproc cluster '
          'region to: us-central1.')
      self._region = 'us-central1'

    if cluster_name:
      _LOGGER.warning(
          'A user-specified cluster_name has been detected. '
          'Please note that you will have to manually delete the Dataproc '
          'cluster that will be created under the name: %s',
          cluster_name)
      self._cluster_name = cluster_name
    else:
      self._cluster_name = self.DEFAULT_NAME

    self._cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={
            'api_endpoint': f'{self._region}-dataproc.googleapis.com:443'
        })

  def create_cluster(self, cluster: dict) -> None:
    """Attempts to create a cluster using attributes that were
    initialized with the DataprocClusterManager instance.

    Args:
      cluster: Dictionary representing Dataproc cluster. Read more about the
      schema for clusters here:
      https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Cluster
    """
    try:
      self._cluster_client.create_cluster(
          request={
              'project_id': self._project_id,
              'region': self._region,
              'cluster': cluster
          })
      _LOGGER.info('Cluster created successfully: %s', self._cluster_name)
    except Exception as e:
      if e.code == 409:
        if self._cluster_name == self.DEFAULT_NAME:
          _LOGGER.info(
              'Cluster %s already exists. Continuing...', self.DEFAULT_NAME)
        else:
          _LOGGER.error(
              'Cluster already exists - unable to create cluster: %s',
              self._cluster_name)
          raise ValueError(
              'Cluster {} already exists!'.format(self._cluster_name))
      elif e.code == 403:
        _LOGGER.error(
            'Due to insufficient project permissions, '
            'unable to create cluster: %s',
            self._cluster_name)
        raise ValueError(
            'You cannot create a cluster in project: {}'.format(
                self._project_id))
      elif e.code == 501:
        _LOGGER.error('Invalid region provided: %s', self._region)
        raise ValueError('Region {} does not exist!'.format(self._region))
      else:
        _LOGGER.error('Unable to create cluster: %s', self._cluster_name)
        raise e

  # TODO(victorhc): Add support for user-specified pip packages
  def create_flink_cluster(self) -> None:
    """Calls _create_cluster with a configuration that enables FlinkRunner."""
    cluster = {
        'project_id': self._project_id,
        'cluster_name': self._cluster_name,
        'config': {
            'software_config': {
                'optional_components': ['DOCKER', 'FLINK']
            }
        }
    }
    self.create_cluster(cluster)

  def cleanup(self) -> None:
    """Deletes the cluster that uses the attributes initialized
    with the DataprocClusterManager instance."""
    try:
      self._cluster_client.delete_cluster(
          request={
              'project_id': self._project_id,
              'region': self._region,
              'cluster_name': self._cluster_name,
          })
    except Exception as e:
      if e.code == 403:
        _LOGGER.error(
            'Due to insufficient project permissions, '
            'unable to clean up the default cluster: %s',
            self._cluster_name)
        raise ValueError(
            'You cannot delete a cluster in project: {}'.format(
                self._project_id))
      elif e.code == 404:
        _LOGGER.error('Cluster does not exist: %s', self._cluster_name)
        raise ValueError('Cluster was not found: {}'.format(self._cluster_name))
      else:
        _LOGGER.error('Failed to delete cluster: %s', self._cluster_name)
        raise e

  def cleanup_if_default(self) -> None:
    """Checks if the cluster_name initialized with the
    DataprocClusterManager instance is the default
    cluster_name. If it is, then the cleanup() method
    is invoked."""
    if self._cluster_name == self.DEFAULT_NAME:
      self.cleanup()
