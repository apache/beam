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

"""Module of mocks to isolated the test environment for each Interactive Beam
test.
"""

import unittest
import uuid
from typing import Type
from unittest.mock import patch

from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import DataprocClusterManager
from apache_beam.runners.interactive.interactive_environment import InteractiveEnvironment
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython


def isolated_env(cls: Type[unittest.TestCase]):
  """A class decorator for unittest.TestCase to set up an isolated test
  environment for Interactive Beam."""
  class IsolatedInteractiveEnvironmentTest(cls):
    def setUp(self):
      self.env_patchers = []
      interactive_path = 'apache_beam.runners.interactive'

      if ie.current_env().is_interactive_ready:
        # Group lines of code into a notebook/IPython cell:
        # with self.cell:
        #   arbitrary python code
        #   ...
        #   arbitrary python code
        self.ipython_patcher = patch(
            'IPython.get_ipython', new_callable=mock_get_ipython)
        self.cell = self.ipython_patcher.start()
        self.env_patchers.append(self.ipython_patcher)

      # self.current_env IS interactive_environment.current_env() in tests.
      self.ie_patcher = patch(
          f'{interactive_path}.interactive_environment.current_env')
      self.m_current_env = self.ie_patcher.start()
      self.current_env = InteractiveEnvironment()
      self.m_current_env.return_value = self.current_env
      self.env_patchers.append(self.ie_patcher)

      # Patches dataproc cluster creation and deletion.
      self.create_cluster_patcher = patch.object(
          DataprocClusterManager, 'create_flink_cluster', mock_create_cluster)
      self.alt_create_cluster = self.create_cluster_patcher.start()
      self.env_patchers.append(self.create_cluster_patcher)
      self.delete_cluster_patcher = patch.object(
          DataprocClusterManager, 'cleanup')
      self.m_delete_cluster = self.delete_cluster_patcher.start()
      self.env_patchers.append(self.delete_cluster_patcher)

      super().setUp()

    def tearDown(self):
      super().tearDown()
      # Explicitly calls the cleanup instead of letting it be called at exit
      # when the mocks and isolated env instance are out of scope.
      self.current_env.cleanup()
      for patcher in reversed(self.env_patchers):
        patcher.stop()

  return IsolatedInteractiveEnvironmentTest


def mock_create_cluster(self):
  """Mocks a cluster creation and populates derived fields."""
  self.cluster_metadata.master_url = 'test-url-' + uuid.uuid4().hex
  self.cluster_metadata.dashboard = 'test-dashboard'


# This file contains no tests. Below lines are purely for passing lint.
if __name__ == '__main__':
  unittest.main()
