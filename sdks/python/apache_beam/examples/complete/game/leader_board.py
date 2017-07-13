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

"""Third in a series of four pipelines that tell a story in a 'gaming' domain.

Concepts include: processing unbounded data using fixed windows; use of custom
timestamps and event-time processing; generation of early/speculative results;
using AccumulationMode.ACCUMULATING to do cumulative processing of late-arriving
data.

This pipeline processes an unbounded stream of 'game events'. The calculation of
the team scores uses fixed windowing based on event time (the time of the game
play event), not processing time (the time that an event is processed by the
pipeline). The pipeline calculates the sum of scores per team, for each window.
By default, the team scores are calculated using one-hour windows.

In contrast-- to demo another windowing option-- the user scores are calculated
using a global window, which periodically (every ten minutes) emits cumulative
user score sums.

In contrast to the previous pipelines in the series, which used static, finite
input data, here we're using an unbounded data source, which lets us provide
speculative results, and allows handling of late data, at much lower latency.
We can use the early/speculative results to keep a 'leaderboard' updated in
near-realtime. Our handling of late data lets us generate correct results,
e.g. for 'team prizes'. We're now outputting window results as they're
calculated, giving us much lower latency than with the previous batch examples.

Run injector.Injector to generate pubsub data for this pipeline. The Injector
documentation provides more detail on how to do this. The injector is currently
implemented in Java only, it can be used from the Java SDK.

The PubSub topic you specify should be the same topic to which the Injector is
publishing.

To run the Java injector:
<beam_root>/examples/java8$ mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
    -Dexec.args="$PROJECT_ID $PUBSUB_TOPIC none"

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
python leader_board.py \
    --project $PROJECT_ID \
    --topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC \
    --dataset $BIGQUERY_DATASET

# DataflowRunner
python leader_board.py \
    --project $PROJECT_ID \
    --topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC \
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
from apache_beam.transforms import trigger

from apache_beam.examples.complete.game.util import util


class CalculateTeamScores(beam.PTransform):
  """Calculates scores for each team within the configured window duration.

  Extract team/score pairs from the event stream, using hour-long windows by
  default.
  """
  def __init__(self, team_window_duration, allowed_lateness):
    super(CalculateTeamScores, self).__init__()
    self.team_window_duration = team_window_duration * 60
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    # NOTE: the behavior does not exactly match the Java example
    # TODO: allowed_lateness not implemented yet in FixedWindows
    # TODO: AfterProcessingTime not implemented yet, replace AfterCount
    return (
        pcoll
        # We will get early (speculative) results as well as cumulative
        # processing of late data.
        | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(self.team_window_duration),
            trigger=trigger.AfterWatermark(trigger.AfterCount(10),
                                           trigger.AfterCount(20)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        # Extract and sum teamname/score pairs from the event data.
        | 'ExtractTeamScores' >> beam.Map(
            lambda elem: (elem['team'], elem['score']))
        | 'SumTeamScores' >> beam.CombinePerKey(sum)
    )


class CalculateUserScores(beam.PTransform):
  """Extract user/score pairs from the event stream using processing time, via
  global windowing. Get periodic updates on all users' running scores.
  """
  def __init__(self, allowed_lateness):
    super(CalculateUserScores, self).__init__()
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    # NOTE: the behavior does not exactly match the Java example
    # TODO: allowed_lateness not implemented yet in FixedWindows
    # TODO: AfterProcessingTime not implemented yet, replace AfterCount
    return (
        pcoll
        # Get periodic results every ten events.
        | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        # Extract and sum username/score pairs from the event data.
        | 'ExtractUserScores' >> beam.Map(
            lambda elem: (elem['user'], elem['score']))
        | 'SumUserScores' >> beam.CombinePerKey(sum)
    )


def run(argv=None):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument('--project',
                      type=str,
                      required=True,
                      help='GCP Project ID for the output BigQuery Dataset.')
  parser.add_argument('--topic',
                      type=str,
                      required=True,
                      help='Pub/Sub topic to read from')
  parser.add_argument('--dataset',
                      type=str,
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                      'Must already exist.')
  parser.add_argument('--table_name',
                      default='leader_board',
                      help='The BigQuery table name. Should not already exist.')
  parser.add_argument('--team_window_duration',
                      type=int,
                      default=60,
                      help='Numeric value of fixed window duration for team '
                           'analysis, in minutes')
  parser.add_argument('--allowed_lateness',
                      type=int,
                      default=120,
                      help='Numeric value of allowed data lateness, in minutes')

  args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_args += ['--save_main_session']

  # The pipeline_args validator also requires --project
  pipeline_args += ['--project', args.project]

  # Enforce that this pipeline is always run in streaming mode
  pipeline_args += ['--streaming']

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read game events from Pub/Sub using custom timestamps, which are extracted
    # from the pubsub data elements, and parse the data.
    events = (
        p
        | 'ReadPubSub' >> beam.io.gcp.pubsub.ReadStringsFromPubSub(args.topic)
        | 'ParseGameEventFn' >> beam.ParDo(util.ParseGameEventFn())
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
    )

    # Get team scores and write the results to BigQuery
    teams_schema = {
        'team': 'STRING',
        'total_score': 'INTEGER',
        'window_start': 'STRING',
        'processing_time': 'STRING',
    }
    (events  # pylint: disable=expression-not-assigned
     | 'CalculateTeamScores' >> CalculateTeamScores(
         args.team_window_duration, args.allowed_lateness)
     | 'TeamScoresDict' >> beam.ParDo(util.TeamScoresDict())
     # Write the results to BigQuery.
     | 'WriteTeamScoreSums' >> util.WriteToBigQuery(
         args.table_name + '_teams', args.dataset, teams_schema)
    )

    # Get user scores and write the results to BigQuery
    users_schema = {
        'user': 'STRING',
        'total_score': 'INTEGER',
    }
    (events  # pylint: disable=expression-not-assigned
     | 'CalculateUserScores' >> CalculateUserScores(args.allowed_lateness)
     | 'FormatUserScoreSums' >> beam.Map(
         lambda (user, score): {'user': user, 'total_score': score})
     # Write the results to BigQuery.
     | 'WriteUserScoreSums' >> util.WriteToBigQuery(
         args.table_name + '_users', args.dataset, users_schema)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
