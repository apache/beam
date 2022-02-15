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

"""Integration test for the BigQuery side input example."""

# pytype: skip-file

import logging
import unittest
import uuid

import pytest
from hamcrest.core.core.allof import all_of

from apache_beam.examples.cookbook import bigquery_side_input
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class BigQuerySideInputIT(unittest.TestCase):
  DEFAULT_OUTPUT_FILE = \
      'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.uuid = str(uuid.uuid4())

    self.output = '/'.join([self.DEFAULT_OUTPUT_FILE, self.uuid, 'results'])

  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_bigquery_side_input_it(self):
    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    NUM_GROUPS = 3

    extra_opts = {
        'output': self.output,
        'num_groups': str(NUM_GROUPS),
        'on_success_matcher': all_of(state_verifier)
    }

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [self.output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    bigquery_side_input.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
