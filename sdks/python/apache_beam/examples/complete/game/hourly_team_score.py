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

"""Second in a series of four pipelines that tell a story in a 'gaming' domain.

In addition to the concepts introduced in `user_score`, new concepts include:
windowing and element timestamps; use of `Filter`.

This pipeline processes data collected from gaming events in batch, building on
`user_score` but using fixed windows. It calculates the sum of scores per team,
for each window, optionally allowing specification of two timestamps before and
after which data is filtered out. This allows a model where late data collected
after the intended analysis window can be included, and any late-arriving data
prior to the beginning of the analysis window can be removed as well. By using
windowing and adding element timestamps, we can do finer-grained analysis than
with the `user_score` pipeline. However, our batch processing is high-latency,
in that we don't get results from plays at the beginning of the batch's time
period until the batch is processed.

To execute this pipeline using the static example input data, specify the
`--dataset=YOUR-DATASET` flag along with other runner specific flags. (Note:
BigQuery dataset you specify must already exist.)

Optionally include the `--input` argument to specify a batch input file. To
indicate a time after which the data should be filtered out, include the
`--stop_min` arg. E.g., `--stop_min=2015-10-18-23-59` indicates that any data
timestamped after 23:59 PST on 2015-10-18 should not be included in the
analysis. To indicate a time before which data should be filtered out, include
the `--start_min` arg. If you're using the default input
"gs://dataflow-samples/game/gaming_data*.csv", then
`--start_min=2015-11-16-16-10 --stop_min=2015-11-17-16-10` are good values.
"""

from __future__ import absolute_import

import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam import typehints
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.utils.pipeline_options import GoogleCloudOptions
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.pipeline_options import SetupOptions


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

  def window_start_format(element, window):
    dt = datetime.datetime.fromtimestamp(int(window.start))
    return dt.strftime('%Y-%m-%d %H:%M:%S')

  return [
      ('team', 'STRING', lambda e, w: e[0]),
      ('total_score', 'INTEGER', lambda e, w: e[1]),
      ('window_start', 'STRING', window_start_format),
  ]


class WriteWindowedToBigQuery(beam.PTransform):
  """Generate, format, and write BigQuery table row information.

  This class may be used for writes that require access to the window
  information.
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
    super(WriteWindowedToBigQuery, self).__init__()
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
      super(WriteWindowedToBigQuery.BuildRowFn, self).__init__()
      self.field_info = field_info

    def process(self, element, window=beam.DoFn.WindowParam):
      row = {}
      for entry in self.field_info:
        row[entry[0]] = entry[2](element, window)
      yield row

  def expand(self, pcoll):
    table = self.get_table(pcoll.pipeline)
    return (
        pcoll
        | 'ConvertToRow' >> beam.ParDo(
            WriteWindowedToBigQuery.BuildRowFn(self.field_info))
        | beam.io.Write(beam.io.BigQuerySink(
            table,
            schema=self.get_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))


def string_to_timestamp(datetime_str):
  dt = datetime.datetime.strptime(datetime_str, '%Y-%m-%d-%H-%M')
  epoch = datetime.datetime.utcfromtimestamp(0)
  return (dt - epoch).total_seconds() * 1000.0


class HourlyTeamScore(beam.PTransform):
  def __init__(self, start_min, stop_min, window_duration):
    super(HourlyTeamScore, self).__init__()
    self.start_min = start_min
    self.stop_min = stop_min
    self.window_duration = window_duration

  def expand(self, pcoll):
    start_min_filter = string_to_timestamp(self.start_min)
    end_min_filter = string_to_timestamp(self.stop_min)

    return (
        pcoll
        | 'ParseGameEvent' >> beam.ParDo(ParseEventFn())
        # Filter out data before and after the given times so that it is not
        # included in the calculations. As we collect data in batches (say, by
        # day), the batch for the day that we want to analyze could potentially
        # include some late-arriving data from the previous day. If so, we want
        # to weed it out. Similarly, if we include data from the following day
        # (to scoop up late-arriving events from the day we're analyzing), we
        # need to weed out events that fall after the time period we want to
        # analyze.
        | 'FilterStartTime' >> beam.Filter(
            lambda element: element['timestamp'] > start_min_filter)
        | 'FilterEndTime' >> beam.Filter(
            lambda element: element['timestamp'] < end_min_filter)
        # Add an element timestamp based on the event log, and apply fixed
        # windowing.
        # Convert element['timestamp'] into seconds as expected by
        # TimestampedValue.
        | 'AddEventTimestamps' >> beam.Map(
            lambda element: TimestampedValue(
                element, element['timestamp'] / 1000.0))
        # Convert window_duration into seconds as expected by FixedWindows.
        | 'FixedWindowsTeam' >> beam.WindowInto(FixedWindows(
            size=self.window_duration * 60))
        # Extract and sum teamname/score pairs from the event data.
        | 'ExtractTeamScore' >> ExtractAndSumScore('team'))


def run(argv=None):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
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
                      default='hourly_team_score',
                      help='The BigQuery table name. Should not already exist.')
  parser.add_argument('--window_duration',
                      type=int,
                      default=60,
                      help='Numeric value of fixed window duration, in minutes')
  parser.add_argument('--start_min',
                      dest='start_min',
                      default='1970-01-01-00-00',
                      help='String representation of the first minute after '
                           'which to generate results in the format: '
                           'yyyy-MM-dd-HH-mm. Any input data timestamped '
                           'prior to that minute won\'t be included in the '
                           'sums.')
  parser.add_argument('--stop_min',
                      dest='stop_min',
                      default='2100-01-01-00-00',
                      help='String representation of the first minute for '
                           'which to generate results in the format: '
                           'yyyy-MM-dd-HH-mm. Any input data timestamped '
                           'after to that minute won\'t be included in the '
                           'sums.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  (p  # pylint: disable=expression-not-assigned
   | ReadFromText(known_args.input)
   | HourlyTeamScore(
       known_args.start_min, known_args.stop_min, known_args.window_duration)
   | WriteWindowedToBigQuery(
       known_args.table_name, known_args.dataset, configure_bigquery_write()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
