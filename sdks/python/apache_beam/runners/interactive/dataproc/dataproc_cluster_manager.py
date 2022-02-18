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
from dataclasses import dataclass
from typing import Optional

from apache_beam.runners.interactive import interactive_environment as ie

_LOGGER = logging.getLogger(__name__)


@dataclass
class MasterURLIdentifier:
  project_id: Optional[str] = None
  region: Optional[str] = None
  cluster_name: Optional[str] = None

  def __key(self):
    return (self.project_id, self.region, self.cluster_name)

  def __hash__(self):
    return hash(self.__key())

  def __eq__(self, other):
    if isinstance(other, MasterURLIdentifier):
      return self.__key() == other.__key()
    raise NotImplementedError(
        'Comparisons are only supported between '
        'instances of MasterURLIdentifier.')


class DataprocClusterManager:
  """The DataprocClusterManager object simplifies the operations
  required for creating and deleting Dataproc clusters for use
  under Interactive Beam.
  """
  def __init__(self, cluster_metadata: MasterURLIdentifier) -> None:
    """Initializes the DataprocClusterManager with properties required
    to interface with the Dataproc ClusterControllerClient.
    """
    self.cluster_metadata = cluster_metadata
    if self.cluster_metadata.region == 'global':
      # The global region is unsupported as it will be eventually deprecated.
      raise ValueError('Clusters in the global region are not supported.')
    elif not self.cluster_metadata.region:
      _LOGGER.warning(
          'No region information was detected, defaulting Dataproc cluster '
          'region to: us-central1.')
      self.cluster_metadata.region = 'us-central1'

    if not self.cluster_metadata.cluster_name:
      self.cluster_metadata.cluster_name = ie.current_env(
      ).clusters.default_cluster_name

    from google.cloud import dataproc_v1
    self._cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={
            'api_endpoint': \
            f'{self.cluster_metadata.region}-dataproc.googleapis.com:443'
        })

    if self.cluster_metadata in ie.current_env().clusters.master_urls.inverse:
      self.master_url = ie.current_env().clusters.master_urls.inverse[
          self.cluster_metadata]
    else:
      self.master_url = None

  def create_cluster(self, cluster: dict) -> None:
    """Attempts to create a cluster using attributes that were
    initialized with the DataprocClusterManager instance.

    Args:
      cluster: Dictionary representing Dataproc cluster. Read more about the
          schema for clusters here:
          https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Cluster
    """
    if self.master_url:
      return
    try:
      self._cluster_client.create_cluster(
          request={
              'project_id': self.cluster_metadata.project_id,
              'region': self.cluster_metadata.region,
              'cluster': cluster
          })
      _LOGGER.info(
          'Cluster created successfully: %s',
          self.cluster_metadata.cluster_name)
      self.master_url = self.get_master_url(self.cluster_metadata)
    except Exception as e:
      if e.code == 409:
        _LOGGER.info(
            'Cluster %s already exists. Continuing...',
            ie.current_env().clusters.default_cluster_name)
      elif e.code == 403:
        _LOGGER.error(
            'Due to insufficient project permissions, '
            'unable to create cluster: %s',
            self.cluster_metadata.cluster_name)
        raise ValueError(
            'You cannot create a cluster in project: {}'.format(
                self.cluster_metadata.project_id))
      elif e.code == 501:
        _LOGGER.error(
            'Invalid region provided: %s', self.cluster_metadata.region)
        raise ValueError(
            'Region {} does not exist!'.format(self.cluster_metadata.region))
      else:
        _LOGGER.error(
            'Unable to create cluster: %s', self.cluster_metadata.cluster_name)
        raise e

  # TODO(victorhc): Add support for user-specified pip packages
  def create_flink_cluster(self) -> None:
    """Calls _create_cluster with a configuration that enables FlinkRunner."""
    cluster = {
        'project_id': self.cluster_metadata.project_id,
        'cluster_name': self.cluster_metadata.cluster_name,
        'config': {
            'software_config': {
                'optional_components': ['DOCKER', 'FLINK']
            },
            'gce_cluster_config': {
                'metadata': {
                    'flink-start-yarn-session': 'true'
                },
                'service_account_scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            },
            'endpoint_config': {
                'enable_http_port_access': True
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
              'project_id': self.cluster_metadata.project_id,
              'region': self.cluster_metadata.region,
              'cluster_name': self.cluster_metadata.cluster_name,
          })
    except Exception as e:
      if e.code == 403:
        _LOGGER.error(
            'Due to insufficient project permissions, '
            'unable to clean up the default cluster: %s',
            self.cluster_metadata.cluster_name)
        raise ValueError(
            'You cannot delete a cluster in project: {}'.format(
                self.cluster_metadata.project_id))
      elif e.code == 404:
        _LOGGER.error(
            'Cluster does not exist: %s', self.cluster_metadata.cluster_name)
        raise ValueError(
            'Cluster was not found: {}'.format(
                self.cluster_metadata.cluster_name))
      else:
        _LOGGER.error(
            'Failed to delete cluster: %s', self.cluster_metadata.cluster_name)
        raise e

  def describe(self) -> None:
    """Returns a dictionary describing the cluster."""
    return {
        'cluster_metadata': self.cluster_metadata,
        'master_url': self.master_url
    }

  def get_master_url(self, identifier) -> None:
    """Returns the master_url of the current cluster."""
    # TODO(victorhc): Implement the following method to fetch the cluster
    # master_url from Dataproc.
    return '.'.join([
        self.cluster_metadata.project_id,
        self.cluster_metadata.region,
        self.cluster_metadata.cluster_name
    ])
