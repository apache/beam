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

"""Integration test for the juliaset example."""

# pytype: skip-file

import logging
import os
import unittest
import uuid

import pytest
from hamcrest.core.core.allof import all_of

from apache_beam.examples.complete.juliaset.juliaset import juliaset
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


@pytest.mark.it_postcommit
class JuliaSetTestIT(unittest.TestCase):
  GRID_SIZE = 1000

  def test_run_example_with_setup_file(self):
    pipeline = TestPipeline(is_integration_test=True)
    coordinate_output = FileSystems.join(
        pipeline.get_option('output'),
        'juliaset-{}'.format(str(uuid.uuid4())),
        'coordinates.txt')
    extra_args = {
        'coordinate_output': coordinate_output,
        'grid_size': self.GRID_SIZE,
        'setup_file': os.path.normpath(
            os.path.join(os.path.dirname(__file__), '..', 'setup.py')),
        'on_success_matcher': all_of(PipelineStateMatcher(PipelineState.DONE)),
    }
    args = pipeline.get_full_options_as_args(**extra_args)

    juliaset.run(args)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
