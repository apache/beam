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
"""A streaming word-group workflow.

In this somewhat contrived example, we emit a group of words that we accumulate
in every window.

This example is based on streaming_wordcount.py.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.combiners as combiners
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class FormatPubsubMessage(beam.DoFn):

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam):
    import json
    from apache_beam.io.gcp.pubsub import PubsubMessage

    data = {
        "elements": sorted(element),
        "window_start_ts": window.start.micros // 1000,
        "window_end_ts": window.end.micros // 1000
    }
    message = PubsubMessage(json.dumps(data).encode("utf-8"), {})
    yield message


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=('Output PubSub topic of the form '
            '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    messages = (
        p | beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription,
            with_attributes=True,
            timestamp_attribute="ts",
        ))
  else:
    messages = (
        p | beam.io.ReadFromPubSub(
            topic=known_args.input_topic,
            with_attributes=True,
            timestamp_attribute="ts",
        ))

  lines = messages | 'decode' >> beam.Map(lambda x: x.data.decode('utf-8'))

  word_groups = (
      lines
      | 'split' >> beam.ParDo(WordExtractingDoFn()).with_output_types(unicode)
      | beam.WindowInto(window.FixedWindows(1, 0))
      | 'group' >> beam.CombineGlobally(
          combiners.ToListCombineFn()).without_defaults())

  output = word_groups | 'encode' >> beam.ParDo(FormatPubsubMessage())

  # Write to PubSub.
  _ = output | beam.io.WriteToPubSub(
      known_args.output_topic, with_attributes=True)

  result = p.run()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
