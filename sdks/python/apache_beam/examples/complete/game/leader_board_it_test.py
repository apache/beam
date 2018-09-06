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

"""End-to-end test for the leader board example.

Code: beam/sdks/python/apache_beam/examples/complete/game/leader_board.py
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
import time
import unittest
import uuid
from builtins import range

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples.complete.game import leader_board
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class LeaderBoardIT(unittest.TestCase):

  # Input event containing user, team, score, processing time, window start.
  INPUT_EVENT = 'user1,teamA,10,%d,2015-11-02 09:09:28.224'
  INPUT_TOPIC = 'leader_board_it_input_topic'
  INPUT_SUB = 'leader_board_it_input_subscription'

  # SHA-1 hash generated from sorted rows reading from BigQuery table
  DEFAULT_EXPECTED_CHECKSUM = 'de00231fe6730b972c0ff60a99988438911cda53'
  OUTPUT_DATASET = 'leader_board_it_dataset'
  OUTPUT_TABLE_USERS = 'leader_board_users'
  OUTPUT_TABLE_TEAMS = 'leader_board_teams'
  DEFAULT_INPUT_COUNT = 500

  WAIT_UNTIL_FINISH_DURATION = 10 * 60 * 1000   # in milliseconds

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')
    _unique_id = str(uuid.uuid4())

    # Set up PubSub environment.
    from google.cloud import pubsub
    self.pubsub_client = pubsub.Client(project=self.project)
    unique_topic_name = self.INPUT_TOPIC + _unique_id
    unique_subscrition_name = self.INPUT_SUB + _unique_id
    self.input_topic = self.pubsub_client.topic(unique_topic_name)
    self.input_sub = self.input_topic.subscription(unique_subscrition_name)

    self.input_topic.create()
    test_utils.wait_for_topics_created([self.input_topic])
    self.input_sub.create()

    # Set up BigQuery environment
    from google.cloud import bigquery
    client = bigquery.Client()
    unique_dataset_name = self.OUTPUT_DATASET + str(int(time.time()))
    self.dataset = client.dataset(unique_dataset_name, project=self.project)
    self.dataset.create()

    self._test_timestamp = int(time.time() * 1000)

  def _inject_pubsub_game_events(self, topic, message_count):
    """Inject game events as test data to PubSub."""

    logging.debug('Injecting %d game events to topic %s',
                  message_count, topic.full_name)

    for _ in range(message_count):
      topic.publish(self.INPUT_EVENT % self._test_timestamp)

  def _cleanup_pubsub(self):
    test_utils.cleanup_subscriptions([self.input_sub])
    test_utils.cleanup_topics([self.input_topic])

  def _cleanup_dataset(self):
    self.dataset.delete()

  @attr('IT')
  def test_leader_board_it(self):
    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)

    success_condition = 'total_score=5000 LIMIT 1'
    users_query = ('SELECT total_score FROM [%s:%s.%s] '
                   'WHERE %s' % (self.project,
                                 self.dataset.name,
                                 self.OUTPUT_TABLE_USERS,
                                 success_condition))
    bq_users_verifier = BigqueryMatcher(self.project,
                                        users_query,
                                        self.DEFAULT_EXPECTED_CHECKSUM)

    teams_query = ('SELECT total_score FROM [%s:%s.%s] '
                   'WHERE %s' % (self.project,
                                 self.dataset.name,
                                 self.OUTPUT_TABLE_TEAMS,
                                 success_condition))
    bq_teams_verifier = BigqueryMatcher(self.project,
                                        teams_query,
                                        self.DEFAULT_EXPECTED_CHECKSUM)

    extra_opts = {'subscription': self.input_sub.full_name,
                  'dataset': self.dataset.name,
                  'topic': self.input_topic.full_name,
                  'team_window_duration': 1,
                  'wait_until_finish_duration':
                      self.WAIT_UNTIL_FINISH_DURATION,
                  'on_success_matcher': all_of(state_verifier,
                                               bq_users_verifier,
                                               bq_teams_verifier)}

    # Register cleanup before pipeline execution.
    # Note that actual execution happens in reverse order.
    self.addCleanup(self._cleanup_pubsub)
    self.addCleanup(self._cleanup_dataset)
    self.addCleanup(utils.delete_bq_table, self.project,
                    self.dataset.name, self.OUTPUT_TABLE_USERS)
    self.addCleanup(utils.delete_bq_table, self.project,
                    self.dataset.name, self.OUTPUT_TABLE_TEAMS)

    # Generate input data and inject to PubSub.
    test_utils.wait_for_subscriptions_created([self.input_topic,
                                               self.input_sub])
    self._inject_pubsub_game_events(self.input_topic, self.DEFAULT_INPUT_COUNT)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    leader_board.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
