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

"""Fourth in a series of four pipelines that tell a story in a 'gaming' domain.

New concepts: session windows and finding session duration; use of both
singleton and non-singleton side inputs.

This pipeline builds on the {@link LeaderBoard} functionality, and adds some
"business intelligence" analysis: abuse detection and usage patterns. The
pipeline derives the Mean user score sum for a window, and uses that information
to identify likely spammers/robots. (The robots have a higher click rate than
the human users). The 'robot' users are then filtered out when calculating the
team scores.

Additionally, user sessions are tracked: that is, we find bursts of user
activity using session windows. Then, the mean session duration information is
recorded in the context of subsequent fixed windowing. (This could be used to
tell us what games are giving us greater user retention).

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
python game_stats.py \
    --project $PROJECT_ID \
    --topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC \
    --dataset $BIGQUERY_DATASET

# DataflowRunner
python game_stats.py \
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
from apache_beam.transforms import combiners

from apache_beam.examples.complete.game.util import util


class CalculateSpammyUsers(beam.PTransform):
  """Filter out all but those users with a high clickrate, which we will
  consider as 'spammy' uesrs.

  We do this by finding the mean total score per user, then using that
  information as a side input to filter out all but those user scores that are
  larger than (mean * SCORE_WEIGHT).
  """
  SCORE_WEIGHT = 2.5

  def expand(self, user_scores):
    # Get the sum of scores for each user.
    sum_scores = (
        user_scores
        | 'SumUsersScores' >> beam.CombinePerKey(sum)
    )

    # Extract the score from each element, and use it to find the global mean.
    global_mean_score = (
        sum_scores
        | beam.Values()
        # NOTE: DataflowRunner currently does not support combiners
        | beam.CombineGlobally(combiners.MeanCombineFn()).as_singleton_view()
    )

    # Filter the user sums using the global mean.
    filtered = (
        sum_scores
        # Use the derived mean total score (global_mean_score) as a side input.
        | 'ProcessAndFilter' >> beam.Filter(
            lambda (_, score), global_mean:\
                score > global_mean * self.SCORE_WEIGHT,
            global_mean_score)
    )
    return filtered


class UserSessionActivity(beam.DoFn):
  """Calculate and output an element's session duration, in seconds."""
  def process(self, elem, window=beam.DoFn.WindowParam):
    yield (window.end.micros - window.start.micros) / 1000000


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
                      type=str,
                      default='game_stats',
                      help='The BigQuery table name. Should not already exist.')
  parser.add_argument('--fixed_window_duration',
                      type=int,
                      default=60,
                      help='Numeric value of fixed window duration for user '
                           'analysis, in minutes')
  parser.add_argument('--session_gap',
                      type=int,
                      default=5,
                      help='Numeric value of gap between user sessions, '
                           'in minutes')
  parser.add_argument('--user_activity_window_duration',
                      type=int,
                      default=30,
                      help='Numeric value of fixed window for finding mean of '
                           'user session duration, in minutes')

  args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_args += ['--save_main_session']

  # The pipeline_args validator also requires --project
  pipeline_args += ['--project', args.project]

  # Enforce that this pipeline is always run in streaming mode
  pipeline_args += ['--streaming']

  fixed_window_duration = args.fixed_window_duration * 60
  session_gap = args.session_gap * 60
  user_activity_window_duration = args.user_activity_window_duration * 60

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read events from Pub/Sub using custom timestamps
    raw_events = (
        p
        | 'ReadPubSub' >> beam.io.gcp.pubsub.ReadStringsFromPubSub(args.topic)
        | 'ParseGameEventFn' >> beam.ParDo(util.ParseGameEventFn())
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
    )

    # Extract username/score pairs from the event stream
    user_events = (
        raw_events
        | 'ExtractUserScores' >> beam.Map(
            lambda elem: (elem['user'], elem['score']))
    )

    # Calculate the total score per user over fixed windows, and cumulative
    # updates for late data
    spammers_view = (
        user_events
        | 'UserFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(fixed_window_duration))

        # Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
        # These might be robots/spammers.
        | 'CalculateSpammyUsers' >> CalculateSpammyUsers()

        # Derive a view from the collection of spammer users. It will be used as
        # a side input in calculating the team score sums, below
        # NOTE: DataflowRunner currently does not support combiners
        | 'CreateSpammersView' >> beam.CombineGlobally(
            combiners.ToDictCombineFn()).as_singleton_view()
    )

    # Calculate the total score per team over fixed windows, and emit cumulative
    # updates for late data. Uses the side input derived above-- the set of
    # suspected robots-- to filter out scores from those users from the sum.
    # Write the results to BigQuery.
    teams_schema = {
        'team': 'STRING',
        'total_score': 'INTEGER',
        'window_start': 'STRING',
        'processing_time': 'STRING',
    }
    (raw_events  # pylint: disable=expression-not-assigned
     | 'WindowIntoFixedWindows' >> beam.WindowInto(
         beam.window.FixedWindows(fixed_window_duration))

     # Filter out the detected spammer users, using the side input derived above
     | 'FilterOutSpammers' >> beam.Filter(
         lambda elem, spammers: elem['user'] not in spammers,
         spammers_view)
     # Extract and sum teamname/score pairs from the event data.
     | 'ExtractTeamScores' >> beam.Map(
         lambda elem: (elem['team'], elem['score']))
     | 'SumTeamScores' >> beam.CombinePerKey(sum)
     | 'TeamScoresDict' >> beam.ParDo(util.TeamScoresDict())
     # Write the result to BigQuery
     | 'WriteTeamScoreSums' >> util.WriteToBigQuery(
         args.table_name + '_teams', args.dataset, teams_schema)
    )

    # Detect user sessions-- that is, a burst of activity separated by a gap
    # from further activity. Find and record the mean session lengths.
    # This information could help the game designers track the changing user
    # engagement as their set of game changes.
    sessions_schema = {
        #'window_start': 'STRING',
        'mean_duration': 'FLOAT',
    }
    (user_events  # pylint: disable=expression-not-assigned
     | 'WindowIntoSessions' >> beam.WindowInto(
         beam.window.Sessions(session_gap),
         timestamp_combiner=beam.window.TimestampCombiner.OUTPUT_AT_EOW)

     # For this use, we care only about the existence of the session, not any
     # particular information aggregated over it, so we can just group by key
     # and assign a "dummy value" of None.
     | beam.CombinePerKey(lambda _: None)

     # Get the duration of the session
     | 'UserSessionActivity' >> beam.ParDo(UserSessionActivity())

     # Re-window to process groups of session sums according to when the
     # sessions complete
     | 'WindowToExtractSessionMean' >> beam.WindowInto(
         beam.window.FixedWindows(user_activity_window_duration))

     # Find the mean session duration in each window
     # NOTE: DataflowRunner currently does not support combiners
     | beam.CombineGlobally(combiners.MeanCombineFn()).without_defaults()
     | 'FormatAvgSessionLength' >> beam.Map(
         lambda elem: {'mean_duration': float(elem)})
     | 'WriteAvgSessionLength' >> util.WriteToBigQuery(
         args.table_name + '_sessions', args.dataset, sessions_schema)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
