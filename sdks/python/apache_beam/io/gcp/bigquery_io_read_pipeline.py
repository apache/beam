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

"""A Dataflow job that counts the number of rows in a BQ table.

   Can be configured to simulate slow reading for a given number of rows.
"""

# pytype: skip-file

import argparse
import logging
import random
import time

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class RowToStringWithSlowDown(beam.DoFn):
  def process(self, element, num_slow=0, *args, **kwargs):

    if num_slow == 0:
      yield ['row']
    else:
      rand = random.random() * 100
      if rand < num_slow:
        time.sleep(0.01)
        yield ['slow_row']
      else:
        yield ['row']


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_table', required=True, help='Input table to process.')
  parser.add_argument(
      '--num_records',
      required=True,
      help='The expected number of records',
      type=int)
  parser.add_argument(
      '--num_slow',
      default=0,
      help=(
          'Percentage of rows that will be slow. '
          'Must be in the range [0, 100)'))
  parser.add_argument(
      '--beam_bq_source',
      default=False,
      type=bool,
      help=(
          'Whether to use the new ReadFromBigQuery'
          ' transform, or the BigQuerySource.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  with TestPipeline(options=options) as p:
    if known_args.beam_bq_source:
      reader = ReadFromBigQuery(
          table='%s:%s' %
          (options.view_as(GoogleCloudOptions).project, known_args.input_table))
    else:
      reader = beam.io.Read(beam.io.BigQuerySource(known_args.input_table))

    # pylint: disable=expression-not-assigned
    count = (
        p | 'read' >> reader
        | 'row to string' >> beam.ParDo(
            RowToStringWithSlowDown(), num_slow=known_args.num_slow)
        | 'count' >> beam.combiners.Count.Globally())

    assert_that(count, equal_to([known_args.num_records]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
