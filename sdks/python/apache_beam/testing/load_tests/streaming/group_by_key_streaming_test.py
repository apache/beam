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

"""
Performance GBK streaming test that uses PubSub SyntheticSources
messages.

Test requires --test-pipeline-options with following options:
* --pubsub_topic_name=name - name of PubSub topic which is
    already created
* --project=project-name
* --num_of_records=1000 - expected number of records
* --runner=TestDataflowRunner or TestDirectRunner - only
    test runners supports matchers

Optional pipeline options:
* --timeout=1000 max time that test will be run and wait
    for all messages.
* --publish_to_big_query=true/false
* --metrics_dataset=python_load_tests
* --metrics_table=gbk_stream
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import uuid

from hamcrest.core.core.allof import all_of

from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.testing import test_utils
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.streaming import group_by_key_streaming_pipeline

DEFAULT_TIMEOUT = 800
WAIT_UNTIL_FINISH_DURATION = 1 * 60 * 1000


class GroupByKeyStreamingTest(LoadTest):
  ID_LABEL = 'id'

  def __init__(self):
    super(GroupByKeyStreamingTest, self).__init__()
    self.topic_short_name = self.pipeline.get_option('pubsub_topic_name')
    self.setup_pubsub()

    timeout = self.pipeline.get_option('timeout') or DEFAULT_TIMEOUT
    expected_num_of_records = self.pipeline.get_option('num_of_records')
    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project_id,
        self.output_sub.name,
        expected_msg_len=int(expected_num_of_records),
        timeout=int(timeout))

    self.extra_opts = {
        'input_subscription': self.input_sub.name,
        'output_topic': self.output_topic.name,
        'metrics_namespace': self.metrics_namespace,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION,
        'on_success_matcher': all_of(pubsub_msg_verifier)
    }

  def setup_pubsub(self):
    self.uuid = str(uuid.uuid4())

    # Set up PubSub environment.
    from google.cloud import pubsub
    self.pub_client = pubsub.PublisherClient()
    input_topic_full_name = "projects/{}/topics/{}" \
      .format(self.project_id, self.topic_short_name)
    self.input_topic = self.pub_client.get_topic(input_topic_full_name)
    self.output_topic_name = self.topic_short_name + '_out_' + self.uuid
    self.output_topic = self.pub_client.create_topic(
        self.pub_client.topic_path(self.project_id, self.output_topic_name))

    self.sub_client = pubsub.SubscriberClient()
    self.input_sub_name = self.topic_short_name + '_sub_in_' + self.uuid
    self.input_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(self.project_id, self.input_sub_name),
        self.input_topic.name)
    self.output_sub_name = self.topic_short_name + '_sub_out_' + self.uuid
    self.output_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(
            self.project_id, self.output_sub_name),
        self.output_topic.name,
        ack_deadline_seconds=60)

  def test(self):
    args = self.pipeline.get_full_options_as_args(**self.extra_opts)
    self.result = group_by_key_streaming_pipeline.run(args)

  # If the test timeouts on Dataflow it may not cleanup pubsub after
  def cleanup(self):
    test_utils.cleanup_subscriptions(
        self.sub_client, [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(
        self.pub_client, [self.input_topic, self.output_topic])


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  GroupByKeyStreamingTest().run()
