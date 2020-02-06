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
Streaming pipeline which reads, GBK and ungroup data from PubSub.
Pipeline is cancelled when matcher achieves expected elements
amount or reaches timeout. To cancel pipeline by matcher it is
required to use TestRunner (TestDataflowRunner or TestDirectRunner).
Data from pubsub is parsed into pair of strings so it
would be possible group it by key.

Values have to be reparsed again to bytes
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.load_tests.load_test_metrics_utils import CountMessages
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.transforms import window


def run(argv=None):
  class MessageParser(beam.DoFn):
    # It is required to parse messages for GBK operation.
    # Otherwise there are encoding problems.
    def process(self, item):
      if item.attributes:
        k, v = item.attributes.popitem()
        yield (str(k), str(v))

  class ParserToBytes(beam.DoFn):
    # Parsing to bytes is required for saving in PubSub.
    def process(self, item):
      _, v = item
      yield bytes(v, encoding='utf8')

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topic/<TOPIC>".'))
  parser.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  parser.add_argument(
      '--metrics_namespace', help=('Namespace of metrics '
                                   '"string".'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # pylint: disable=expression-not-assigned
  (
      p
      | ReadFromPubSub(
          subscription=known_args.input_subscription, with_attributes=True)
      | 'Window' >> beam.WindowInto(window.FixedWindows(1000, 0))
      | 'Measure time: Start' >> beam.ParDo(
          MeasureTime(known_args.metrics_namespace))
      | 'Count messages' >> beam.ParDo(
          CountMessages(known_args.metrics_namespace))
      | 'Parse' >> beam.ParDo(MessageParser())
      | 'GroupByKey' >> beam.GroupByKey()
      | 'Ungroup' >> beam.FlatMap(lambda elm: [(elm[0], v) for v in elm[1]])
      | 'Measure time: End' >> beam.ParDo(
          MeasureTime(known_args.metrics_namespace))
      | 'Parse to bytes' >> beam.ParDo(ParserToBytes())
      | 'Write' >> beam.io.WriteToPubSub(topic=known_args.output_topic))

  result = p.run()
  result.wait_until_finish()
  logging.error(result)
  return result
