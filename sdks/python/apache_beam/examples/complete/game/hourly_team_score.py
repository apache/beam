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
windowing and element timestamps; use of `Filter`; using standalone DoFns.

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

Optionally include the `--input` argument to specify a batch input file. To
indicate a time after which the data should be filtered out, include the
`--stop_min` arg. E.g., `--stop_min=2015-10-18-23-59` indicates that any data
timestamped after 23:59 PST on 2015-10-18 should not be included in the
analysis. To indicate a time before which data should be filtered out, include
the `--start_min` arg. If you're using the default input
"gs://dataflow-samples/game/gaming_data*.csv", then
`--start_min=2015-11-16-16-10 --stop_min=2015-11-17-16-10` are good values.

For a description of the usage and options, use -h or --help.

To specify a different runner:
  --runner YOUR_RUNNER

NOTE: When specifying a different runner, additional runner-specific options
      may have to be passed in as well
NOTE: With DataflowRunner, the --setup_file flag must be specified to handle the
      'util' module

EXAMPLES
--------

# DirectRunner
python hourly_team_score.py \
    --project $PROJECT_ID \
    --dataset $BIGQUERY_DATASET

# DataflowRunner
python hourly_team_score.py \
    --project $PROJECT_ID \
    --dataset $BIGQUERY_DATASET \
    --runner DataflowRunner \
    --setup_file ./setup.py \
    --staging_location gs://$BUCKET/user_score/staging \
    --temp_location gs://$BUCKET/user_score/temp
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.examples.complete.game.util import util
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class HourlyTeamScore(beam.PTransform):
  def __init__(self, start_min, stop_min, window_duration):
    super(HourlyTeamScore, self).__init__()
    self.start_timestamp = util.str2timestamp(start_min)
    self.stop_timestamp = util.str2timestamp(stop_min)
    self.window_duration_in_seconds = window_duration * 60

  def expand(self, pcoll):
    return (
        pcoll
        | 'ParseGameEventFn' >> beam.ParDo(util.ParseGameEventFn())

        # Filter out data before and after the given times so that it is not
        # included in the calculations. As we collect data in batches (say, by
        # day), the batch for the day that we want to analyze could potentially
        # include some late-arriving data from the previous day. If so, we want
        # to weed it out. Similarly, if we include data from the following day
        # (to scoop up late-arriving events from the day we're analyzing), we
        # need to weed out events that fall after the time period we want to
        # analyze.
        | 'FilterStartTime' >> beam.Filter(
            lambda elem: elem['timestamp'] > self.start_timestamp)
        | 'FilterEndTime' >> beam.Filter(
            lambda elem: elem['timestamp'] < self.stop_timestamp)

        # Add an element timestamp based on the event log, and apply fixed
        # windowing.
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
        | 'FixedWindowsTeam' >> beam.WindowInto(
            beam.window.FixedWindows(self.window_duration_in_seconds))

        # Extract and sum teamname/score pairs from the event data.
        | 'ExtractAndSumScore' >> util.ExtractAndSumScore('team')
    )


def run(argv=None):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument('--input',
                      type=str,
                      default='gs://apache-beam-samples/game/gaming_data*.csv',
                      help='Path to the data file(s) containing game data.')
  parser.add_argument('--project',
                      type=str,
                      required=True,
                      help='GCP Project ID for the output BigQuery Dataset.')
  parser.add_argument('--dataset',
                      type=str,
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                      'Must already exist.')
  parser.add_argument('--table_name',
                      default='leader_board',
                      help='The BigQuery table name. Should not already exist.')
  parser.add_argument('--window_duration',
                      type=int,
                      default=60,
                      help='Numeric value of fixed window duration, in minutes')
  parser.add_argument('--start_min',
                      type=str,
                      default='1970-01-01-00-00',
                      help='String representation of the first minute after '
                           'which to generate results in the format: '
                           'yyyy-MM-dd-HH-mm. Any input data timestamped '
                           'prior to that minute won\'t be included in the '
                           'sums.')
  parser.add_argument('--stop_min',
                      type=str,
                      default='2100-01-01-00-00',
                      help='String representation of the first minute for '
                           'which to generate results in the format: '
                           'yyyy-MM-dd-HH-mm. Any input data timestamped '
                           'after to that minute won\'t be included in the '
                           'sums.')

  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  options.view_as(SetupOptions).save_main_session = True

  # The GoogleCloudOptions require the project
  options.view_as(GoogleCloudOptions).project = args.project

  schema = {
      'team': 'STRING',
      'total_score': 'INTEGER',
      'window_start': 'STRING',
  }
  with beam.Pipeline(options=options) as p:
    (p  # pylint: disable=expression-not-assigned
     | 'ReadInputText' >> beam.io.ReadFromText(args.input)
     | 'HourlyTeamScore' >> HourlyTeamScore(
         args.start_min, args.stop_min, args.window_duration)
     | 'TeamScoresDict' >> beam.ParDo(util.TeamScoresDict())
     | 'WriteTeamScoreSums' >> util.WriteToBigQuery(
         args.table_name, args.dataset, schema)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
