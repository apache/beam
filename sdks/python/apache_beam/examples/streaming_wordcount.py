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

"""A streaming word-counting workflow.

Important: streaming pipeline support in Python Dataflow is in development
and is not yet available for use.
"""

from __future__ import absolute_import

import argparse
import logging

import six

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


def split_fn(lines):
  import re
  return re.findall(r'[A-Za-z\']+', lines)


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic', required=True,
      help=('Output PubSub topic of the form '
            '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_sub',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)
  options = PipelineOptions(pipeline_args)
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:

    # Read from PubSub into a PCollection.
    if known_args.input_sub:
      lines = p | beam.io.ReadStringsFromPubSub(subscription=known_args.input_sub)
    else:
      lines = p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic)

    # Capitalize the characters in each line.
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    transformed = (lines
                   # Use a pre-defined function that imports the re package.
                   | 'Split' >> (
                       beam.FlatMap(split_fn).with_output_types(six.text_type))
                   | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                   | beam.WindowInto(window.FixedWindows(15, 0))
                   | 'Group' >> beam.GroupByKey()
                   | 'Count' >> beam.Map(count_ones)
                   | 'Format' >> beam.Map(lambda tup: '%s: %d' % tup))

    # Write to PubSub.
    # pylint: disable=expression-not-assigned
    transformed | beam.io.WriteStringsToPubSub(known_args.output_topic)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
