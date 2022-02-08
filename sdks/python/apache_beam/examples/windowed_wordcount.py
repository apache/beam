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

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.transforms import window

TABLE_SCHEMA = (
    'word:STRING, count:INTEGER, '
    'window_start:TIMESTAMP, window_end:TIMESTAMP')


def find_words(element):
  import re
  return re.findall(r'[A-Za-z\']+', element)


class FormatDoFn(beam.DoFn):
  def process(self, element, window=beam.DoFn.WindowParam):
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    window_start = window.start.to_utc_datetime().strftime(ts_format)
    window_end = window.end.to_utc_datetime().strftime(ts_format)
    return [{
        'word': element[0],
        'count': element[1],
        'window_start': window_start,
        'window_end': window_end
    }]


def main(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic',
      required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
  parser.add_argument(
      '--output_table',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    # Read the text from PubSub messages.
    lines = p | beam.io.ReadFromPubSub(known_args.input_topic)

    # Get the number of appearances of a word.
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    transformed = (
        lines
        | 'Split' >> (beam.FlatMap(find_words).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | beam.WindowInto(window.FixedWindows(2 * 60, 0))
        | 'Group' >> beam.GroupByKey()
        | 'Count' >> beam.Map(count_ones)
        | 'Format' >> beam.ParDo(FormatDoFn()))

    # Write to BigQuery.
    # pylint: disable=expression-not-assigned
    transformed | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output_table,
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
