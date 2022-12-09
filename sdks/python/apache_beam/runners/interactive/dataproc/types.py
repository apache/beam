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

import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Optional
from typing import Union

from apache_beam.pipeline import Pipeline


def _generate_unique_cluster_name():
  return f'interactive-beam-{uuid.uuid4().hex}'


@dataclass
class ClusterMetadata:
  """Metadata of a provisioned worker cluster that executes Beam pipelines.

  Apache Beam supports running Beam pipelines on different runners provisioned
  in different setups based on the runner and pipeline options associated with
  each pipeline. To provide similar portability features, Interactive Beam
  automatically extracts such ClusterMetadata information from pipeline options
  of a pipeline in the REPL context and provision suitable clusters to execute
  the pipeline. The lifecyle of the clusters is managed by Interactive Beam
  and the user doesn not need to interact with it.

  It's not recommended to build this ClusterMetadata from raw values nor use it
  to interact with the cluster management logic directly.

  Interactive Beam now supports::

    1. Runner: FlinkRunner; Setup: on Google Cloud with Flink on Dataproc.

  """
  project_id: Optional[str] = None
  region: Optional[str] = 'us-central1'
  cluster_name: Optional[str] = field(
      default_factory=_generate_unique_cluster_name)
  # From WorkerOptions.
  subnetwork: Optional[str] = None
  num_workers: Optional[int] = None
  machine_type: Optional[int] = None

  # Derivative fields do not affect hash or comparison.
  master_url: Optional[str] = None
  dashboard: Optional[str] = None

  def __key(self):
    return (
        self.project_id,
        self.region,
        self.cluster_name,
        self.subnetwork,
        self.num_workers,
        self.machine_type)

  def __hash__(self):
    return hash(self.__key())

  def __eq__(self, other):
    if isinstance(other, ClusterMetadata):
      return self.__key() == other.__key()
    return False

  def reset_name(self):
    self.cluster_name = _generate_unique_cluster_name()


ClusterIdentifier = Union[str, Pipeline, ClusterMetadata]
