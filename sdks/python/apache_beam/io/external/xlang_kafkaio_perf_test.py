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

import logging
import sys
import typing

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io import kafka
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test import LoadTestOptions
from apache_beam.testing.load_tests.load_test_metrics_utils import CountMessages
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.util import Reshuffle

WRITE_NAMESPACE = 'write'
READ_NAMESPACE = 'read'

_LOGGER = logging.getLogger(__name__)


class KafkaIOTestOptions(LoadTestOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--test_class', required=True, help='Test class to run.')

    parser.add_argument('--kafka_topic', required=True, help='Kafka topic.')

    parser.add_argument(
        '--bootstrap_servers', help='URL TO Kafka Bootstrap service.')


class KafkaIOBatchPerfTest:
  """Performance test for cross-language Kafka IO batch pipeline."""
  def run(self):
    write_test = _KafkaIOBatchWritePerfTest()
    read_test = _KafkaIOBatchReadPerfTest()
    write_test.run()
    read_test.run()


class _KafkaIOBatchWritePerfTest(LoadTest):
  def __init__(self):
    super().__init__(WRITE_NAMESPACE)
    self.test_options = self.pipeline.get_pipeline_options().view_as(
        KafkaIOTestOptions)
    self.kafka_topic = self.test_options.kafka_topic
    # otherwise see 'ValueError: Unexpected DoFn type: beam:dofn:javasdk:0.1'
    self.pipeline.not_use_test_runner_api = True

  def test(self):
    _ = (
        self.pipeline
        | 'Generate records' >> iobase.Read(
            SyntheticSource(self.parse_synthetic_source_options())) \
            .with_output_types(typing.Tuple[bytes, bytes])
        | 'Count records' >> beam.ParDo(CountMessages(self.metrics_namespace))
        | 'Avoid Fusion' >> Reshuffle()
        | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace))
        | 'WriteToKafka' >> kafka.WriteToKafka(
            producer_config={
                'bootstrap.servers': self.test_options.bootstrap_servers
            },
            topic=self.kafka_topic))

  def cleanup(self):
    pass


class _KafkaIOBatchReadPerfTest(LoadTest):
  def __init__(self):
    super().__init__(READ_NAMESPACE)
    self.test_options = self.pipeline.get_pipeline_options().view_as(
        KafkaIOTestOptions)
    self.kafka_topic = self.test_options.kafka_topic
    # otherwise see 'ValueError: Unexpected DoFn type: beam:dofn:javasdk:0.1'
    self.pipeline.not_use_test_runner_api = True

  def test(self):
    output = (
        self.pipeline
        | 'ReadFromKafka' >> kafka.ReadFromKafka(
            consumer_config={
                'bootstrap.servers': self.test_options.bootstrap_servers,
                'auto.offset.reset': 'earliest'
            },
            topics=[self.kafka_topic],
            max_num_records=self.input_options['num_records'])
        | 'Count records' >> beam.ParDo(CountMessages(self.metrics_namespace))
        | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace))
        | 'Count' >> beam.combiners.Count.Globally())
    assert_that(output, equal_to([self.input_options['num_records']]))

  def cleanup(self):
    pass


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  test_options = TestPipeline().get_pipeline_options().view_as(
      KafkaIOTestOptions)
  supported_test_classes = list(
      filter(
          lambda s: s.endswith('PerfTest') and not s.startswith('_'),
          dir(sys.modules[__name__])))

  if test_options.test_class not in supported_test_classes:
    raise RuntimeError(
        f'Test {test_options.test_class} not found. '
        'Supported tests are {supported_test_classes}')

  getattr(sys.modules[__name__], test_options.test_class)().run()
