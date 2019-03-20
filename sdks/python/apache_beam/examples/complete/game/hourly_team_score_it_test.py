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

"""End-to-end test for the hourly team score example.

Code: beam/sdks/python/apache_beam/examples/complete/game/hourly_team_score.py
Usage:

  python setup.py nosetests --test-pipeline-options=" \
      --runner=TestDataflowRunner \
      --project=... \
      --staging_location=gs://... \
      --temp_location=gs://... \
      --output=gs://... \
      --sdk_location=... \

"""

from __future__ import absolute_import

import logging
import os
import sys
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples.complete.game import hourly_team_score
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-6870')
class HourlyTeamScoreIT(unittest.TestCase):

  DEFAULT_INPUT_FILE = 'gs://dataflow-samples/game/gaming_data*'
  # SHA-1 hash generated from sorted rows reading from BigQuery table
  DEFAULT_EXPECTED_CHECKSUM = '4fa761fb5c3341ec573d5d12c6ab75e3b2957a25'
  OUTPUT_DATASET = 'hourly_team_score_it_dataset'
  OUTPUT_TABLE = 'leader_board'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')

    # Set up BigQuery environment
    self.dataset_ref = utils.create_bq_dataset(self.project,
                                               self.OUTPUT_DATASET)

  @attr('IT')
  def test_hourly_team_score_it(self):
    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    query = ('SELECT COUNT(*) FROM `%s.%s.%s`' % (self.project,
                                                  self.dataset_ref.dataset_id,
                                                  self.OUTPUT_TABLE))

    bigquery_verifier = BigqueryMatcher(self.project,
                                        query,
                                        self.DEFAULT_EXPECTED_CHECKSUM)

    extra_opts = {'input': self.DEFAULT_INPUT_FILE,
                  'dataset': self.dataset_ref.dataset_id,
                  'window_duration': 1,
                  'on_success_matcher': all_of(state_verifier,
                                               bigquery_verifier)}

    # Register clean up before pipeline execution
    # Note that actual execution happens in reverse order.
    self.addCleanup(utils.delete_bq_dataset, self.project, self.dataset_ref)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    hourly_team_score.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
