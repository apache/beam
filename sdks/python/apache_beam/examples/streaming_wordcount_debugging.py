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

"""A streaming wordcount example with debugging capabilities.

It demonstrate the use of logging and assert_that in streaming mode.

This workflow only works with the DirectRunner (BEAM-3377).

Usage:
python streaming_wordcount_debugging.py
--input_topic projects/$PROJECT_ID/topics/$PUBSUB_INPUT_TOPIC
--output_topic projects/$PROJECT_ID/topics/$PUBSUB_OUTPUT_TOPIC
--streaming

To publish messages:
gcloud alpha pubsub topics publish $PUBSUB_INPUT_TOPIC --message '210 213 151'

"""

# pytype: skip-file

import argparse
import logging
import re
import time

import apache_beam as beam
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import window
from apache_beam.transforms.core import ParDo


class PrintFn(beam.DoFn):
  """A DoFn that prints label, element, its window, and its timstamp. """
  def __init__(self, label):
    self.label = label

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam):
    # Log at INFO level each element processed.
    logging.info('[%s]: %s %s %s', self.label, element, window, timestamp)
    yield element


class AddTimestampFn(beam.DoFn):
  """A DoFn that attaches timestamps to its elements.

  It takes an element and attaches a timestamp of its same value for integer
  and current timestamp in other cases.

  For example, 120 and Sometext will result in:
  (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
  """
  def process(self, element):
    logging.info('Adding timestamp to: %s', element)
    try:
      timestamp = int(element)
    except ValueError:
      timestamp = int(time.time())
    yield beam.window.TimestampedValue(element, timestamp)


def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=(
          'Input PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
      messages = p | beam.io.ReadFromPubSub(
          subscription=known_args.input_subscription)
    else:
      messages = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)

    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

    # Count the occurrences of each word.
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'AddTimestampFn' >> beam.ParDo(AddTimestampFn())
        | 'After AddTimestampFn' >> ParDo(PrintFn('After AddTimestampFn'))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | beam.WindowInto(window.FixedWindows(5, 0))
        | 'GroupByKey' >> beam.GroupByKey()
        | 'CountOnes' >> beam.Map(count_ones))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %d' % (word, count)

    output = (
        counts
        | 'format' >> beam.Map(format_result)
        | 'encode' >>
        beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

    # Write to PubSub.
    # pylint: disable=expression-not-assigned
    output | beam.io.WriteToPubSub(known_args.output_topic)

    def check_gbk_format():
      # A matcher that checks that the output of GBK is of the form word: count.
      def matcher(elements):
        # pylint: disable=unused-variable
        actual_elements_in_window, window = elements
        for elm in actual_elements_in_window:
          assert re.match(r'\S+:\s+\d+', elm.decode('utf-8')) is not None

      return matcher

    # Check that the format of the output is correct.
    assert_that(
        output,
        check_gbk_format(),
        use_global_window=False,
        label='Assert word:count format.')

    # Check also that elements are ouput in the right window.
    # This expects exactly 1 occurrence of any subset of the elements
    # 150, 151, 152, 153, 154 in the window [150, 155)
    # or exactly 1 occurrence of any subset of the elements
    # 210, 211, 212, 213, 214 in the window [210, 215).
    first_window_val = [
        '150: 1',
        '151: 1',
        '152: 1',
        '153: 1',
        '154: 1',
    ]
    second_window_val = [
        '210: 1',
        '211: 1',
        '212: 1',
        '213: 1',
        '214: 1',
    ]
    expected_window_to_elements = {
        window.IntervalWindow(150, 155): [
            x.encode('utf-8') for x in first_window_val
        ],
        window.IntervalWindow(210, 215): [
            x.encode('utf-8') for x in second_window_val
        ],
    }

    # To pass, publish numbers in [150-155) or [210-215) with no repeats.
    # To fail, publish a repeated number in the range above range.
    # For example: '210 213 151 213'
    assert_that(
        output,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='Assert correct streaming windowing.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
