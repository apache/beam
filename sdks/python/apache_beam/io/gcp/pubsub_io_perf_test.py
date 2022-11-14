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
Performance PubsubIO streaming test for Write/Read operations.

Caution: only test runners (e.g. TestDataflowRunner) support matchers

Example for TestDataflowRunner:

python -m apache_beam.io.gcp.pubsub_io_perf_test \
    --test-pipeline-options="
    --runner=TestDataflowRunner
    --sdk_location=.../dist/apache-beam-x.x.x.dev0.tar.gz
    --project=<GCP_PROJECT_ID>
    --temp_location=gs://<BUCKET_NAME>/tmp
    --staging_location=gs://<BUCKET_NAME>/staging
    --wait_until_finish_duration=<TIME_IN_MS>
    --pubsub_namespace_prefix=<PUBSUB_NAMESPACE_PREFIX>
    --publish_to_big_query=<OPTIONAL><true/false>
    --metrics_dataset=<OPTIONAL>
    --metrics_table=<OPTIONAL>
    --dataflow_worker_jar=<OPTIONAL>
    --input_options='{
      \"num_records\": <SIZE_OF_INPUT>
      \"key_size\": 1
      \"value_size\": <SIZE_OF_EACH_MESSAGE>
    }'"
"""

# pytype: skip-file

import logging
import sys

from hamcrest import all_of

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import trigger
from apache_beam.transforms import window

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position

WRITE_METRICS_NAMESPACE = 'pubsub_io_perf_write'
READ_METRICS_NAMESPACE = 'pubsub_io_perf_read'
MATCHER_TIMEOUT = 60 * 15
MATCHER_PULL_TIMEOUT = 60 * 5


class PubsubIOPerfTest(LoadTest):
  def _setup_env(self):
    if not self.pipeline.get_option('pubsub_namespace_prefix'):
      logging.error('--pubsub_namespace_prefix argument is required.')
      sys.exit(1)
    if not self.pipeline.get_option('wait_until_finish_duration'):
      logging.error('--wait_until_finish_duration argument is required.')
      sys.exit(1)

    self.num_of_messages = int(self.input_options.get('num_records'))
    pubsub_namespace_prefix = self.pipeline.get_option(
        'pubsub_namespace_prefix')
    self.pubsub_namespace = pubsub_namespace_prefix + unique_id

  def _setup_pubsub(self):
    self.pub_client = pubsub.PublisherClient()
    self.topic_name = self.pub_client.topic_path(
        self.project_id, self.pubsub_namespace)

    self.matcher_topic_name = self.pub_client.topic_path(
        self.project_id, self.pubsub_namespace + '_matcher')

    self.sub_client = pubsub.SubscriberClient()
    self.read_sub_name = self.sub_client.subscription_path(
        self.project_id,
        self.pubsub_namespace + '_read',
    )
    self.read_matcher_sub_name = self.sub_client.subscription_path(
        self.project_id,
        self.pubsub_namespace + '_read_matcher',
    )


class PubsubWritePerfTest(PubsubIOPerfTest):
  def __init__(self):
    super().__init__(WRITE_METRICS_NAMESPACE)
    self._setup_env()
    self._setup_pubsub()
    self._setup_pipeline()

  def test(self):
    def to_pubsub_message(element):
      import uuid
      from apache_beam.io import PubsubMessage
      return PubsubMessage(
          data=element[1],
          attributes={'id': str(uuid.uuid1()).encode('utf-8')},
      )

    _ = (
        self.pipeline
        | 'Create input' >> Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Format to pubsub message in bytes' >> beam.Map(to_pubsub_message)
        | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace))
        | 'Write to Pubsub' >> beam.io.WriteToPubSub(
            self.topic_name,
            with_attributes=True,
            id_label='id',
        ))

  def _setup_pipeline(self):
    options = PipelineOptions(self.pipeline.get_full_options_as_args())
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True
    self.pipeline = TestPipeline(options=options)

  def _setup_pubsub(self):
    super()._setup_pubsub()
    _ = self.pub_client.create_topic(name=self.topic_name)

    _ = self.sub_client.create_subscription(
        name=self.read_sub_name,
        topic=self.topic_name,
    )


class PubsubReadPerfTest(PubsubIOPerfTest):
  def __init__(self):
    super().__init__(READ_METRICS_NAMESPACE)
    self._setup_env()
    self._setup_pubsub()
    self._setup_pipeline()

  def test(self):
    _ = (
        self.pipeline
        | 'Read from pubsub' >> ReadFromPubSub(
            subscription=self.read_sub_name,
            with_attributes=True,
            id_label='id',
        )
        | beam.Map(lambda x: bytes(1)).with_output_types(bytes)
        | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace))
        | 'Window' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(
                trigger.AfterCount(self.num_of_messages)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'Count messages' >> beam.CombineGlobally(
            beam.combiners.CountCombineFn()).without_defaults().
        with_output_types(int)
        | 'Convert to bytes' >>
        beam.Map(lambda count: str(count).encode('utf-8'))
        | 'Write to Pubsub' >> beam.io.WriteToPubSub(self.matcher_topic_name))

  def _setup_pubsub(self):
    super()._setup_pubsub()
    _ = self.pub_client.create_topic(name=self.matcher_topic_name)

    _ = self.sub_client.create_subscription(
        name=self.read_matcher_sub_name,
        topic=self.matcher_topic_name,
    )

  def _setup_pipeline(self):
    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project_id,
        self.read_matcher_sub_name,
        expected_msg=[str(self.num_of_messages).encode('utf-8')],
        timeout=MATCHER_TIMEOUT,
        pull_timeout=MATCHER_PULL_TIMEOUT,
    )
    extra_opts = {
        'on_success_matcher': all_of(pubsub_msg_verifier),
        'streaming': True,
        'save_main_session': True
    }
    args = self.pipeline.get_full_options_as_args(**extra_opts)
    self.pipeline = TestPipeline(options=PipelineOptions(args))

  def cleanup(self):
    self.sub_client.delete_subscription(subscription=self.read_sub_name)
    self.sub_client.delete_subscription(subscription=self.read_matcher_sub_name)
    self.pub_client.delete_topic(topic=self.topic_name)
    self.pub_client.delete_topic(topic=self.matcher_topic_name)


if __name__ == '__main__':
  import uuid
  unique_id = str(uuid.uuid4())

  logging.basicConfig(level=logging.INFO)
  PubsubWritePerfTest().run()
  PubsubReadPerfTest().run()
