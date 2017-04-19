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

"""First in a series of four pipelines that tell a story in a 'gaming' domain.
Concepts: batch processing; reading input from Google Cloud Storage and writing
output to BigQuery; using standalone DoFns; use of the sum by key transform.

In this gaming scenario, many users play, as members of different teams, over
the course of a day, and their actions are logged for processing. Some of the
logged game events may be late-arriving, if users play on mobile devices and go
transiently offline for a period of time.

This pipeline does batch processing of data collected from gaming events. It
calculates the sum of scores per user, over an entire batch of gaming data
(collected, say, for each day). The batch processing will not include any late
data that arrives after the day's cutoff point.

To execute this pipeline using the static example input data, specify the
`--dataset=YOUR-DATASET` flag along with other runner specific flags. Note:
The BigQuery dataset you specify must already exist. You can simply create a new
empty BigQuery dataset if you don't have an existing one.

Optionally include the `--input` argument to specify a batch input file. See the
`--input` default value for an example batch data file.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam import typehints
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.utils.pipeline_options import GoogleCloudOptions
from apache_beam.utils.pipeline_options import PipelineOptions


class ParseEventFn(beam.DoFn):
  """Parses the raw game event info into GameActionInfo tuples.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    super(ParseEventFn, self).__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, element):
    components = element.split(',')
    try:
      user = components[0].strip()
      team = components[1].strip()
      score = int(components[2].strip())
      timestamp = int(components[3].strip())
      yield {'user': user, 'team': team, 'score': score, 'timestamp': timestamp}
    except:  # pylint: disable=bare-except
      # Log and count parse errors.
      self.num_parse_errors.inc()
      logging.info('Parse error on %s.', element)


@with_input_types(ints=typehints.Iterable[int])
@with_output_types(int)
def sum_ints(ints):
  return sum(ints)


class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.

  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):
    super(ExtractAndSumScore, self).__init__()
    self.field = field

  def expand(self, pcoll):
    return (pcoll
            | beam.Map(lambda info: (info[self.field], info['score']))
            | beam.CombinePerKey(sum_ints))


def configure_bigquery_write():
  return [
      ('user', 'STRING', lambda e: e[0]),
      ('total_score', 'INTEGER', lambda e: e[1]),
  ]


class WriteToBigQuery(beam.PTransform):
  """Generate, format, and write BigQuery table row information.

  Use provided information about the field names and types, as well as lambda
  functions that describe how to generate their values.
  """

  def __init__(self, table_name, dataset, field_info):
    """Initializes the transform.

    Args:
      table_name: Name of the BigQuery table to use.
      dataset: Name of the dataset to use.
      field_info: List of tuples that holds information about output table field
                  definitions. The tuples are in the
                  (field_name, field_type, field_fn) format, where field_name is
                  the name of the field, field_type is the BigQuery type of the
                  field and field_fn is a lambda function to generate the field
                  value from the element.
    """
    super(WriteToBigQuery, self).__init__()
    self.table_name = table_name
    self.dataset = dataset
    self.field_info = field_info

  def get_schema(self):
    """Build the output table schema."""
    return ', '.join(
        '%s:%s' % (entry[0], entry[1]) for entry in self.field_info)

  def get_table(self, pipeline):
    """Utility to construct an output table reference."""
    project = pipeline.options.view_as(GoogleCloudOptions).project
    return '%s:%s.%s' % (project, self.dataset, self.table_name)

  class BuildRowFn(beam.DoFn):
    """Convert each key/score pair into a BigQuery TableRow as specified."""
    def __init__(self, field_info):
      super(WriteToBigQuery.BuildRowFn, self).__init__()
      self.field_info = field_info

    def process(self, element):
      row = {}
      for entry in self.field_info:
        row[entry[0]] = entry[2](element)
      yield row

  def expand(self, pcoll):
    table = self.get_table(pcoll.pipeline)
    return (
        pcoll
        | 'ConvertToRow' >> beam.ParDo(
            WriteToBigQuery.BuildRowFn(self.field_info))
        | beam.io.Write(beam.io.BigQuerySink(
            table,
            schema=self.get_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))


class UserScore(beam.PTransform):
  def expand(self, pcoll):
    return (pcoll
            | 'ParseGameEvent' >> beam.ParDo(ParseEventFn())
            # Extract and sum username/score pairs from the event data.
            | 'ExtractUserScore' >> ExtractAndSumScore('user'))


def run(argv=None):
  """Main entry point; defines and runs the user_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/game/gaming_data*.csv',
                      help='Path to the data file(s) containing game data.')
  parser.add_argument('--dataset',
                      dest='dataset',
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                           'Must already exist.')
  parser.add_argument('--table_name',
                      dest='table_name',
                      default='user_score',
                      help='The BigQuery table name. Should not already exist.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)

  (p  # pylint: disable=expression-not-assigned
   | ReadFromText(known_args.input) # Read events from a file and parse them.
   | UserScore()
   | WriteToBigQuery(
       known_args.table_name, known_args.dataset, configure_bigquery_write()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
