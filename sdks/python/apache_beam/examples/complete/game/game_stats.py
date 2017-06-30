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
documentation provides more detail on how to do this.

The PubSub topic you specify should be the same topic to which the Injector is
publishing.

For a description of the usage and options, use -h or --help.

Required options:
  --output=OUTPUT_PATH

Options with default values:
  --input='gs://apache-beam-samples/game/gaming_data*.csv'
  --team_window_duration=60
  --allowed_lateness=120

To specify a different runner:
  --runner=YOUR_RUNNER

NOTE: When specifying a different runner, additional runner-specific options
      may have to be passed in as well
"""

from __future__ import absolute_import
from __future__ import division

import argparse
import csv
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.transforms import combiners


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)


def parse_game_event(elem):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  try:
    row = list(csv.reader([elem]))[0]
    yield {
        'user': row[0],
        'team': row[1],
        'score': int(row[2]),
        'timestamp': int(row[3]) / 1000.0,
    }
  except:  # pylint: disable=bare-except
    logging.error('Parse error on "%s"', elem)


class FormatTeamScores(beam.DoFn):
  """Formats the data into the output format

  Receives a (team, score) pair, extracts the window start timestamp, and
  formats everything together into a string. Each string will be a line in the
  output file.
  """
  def process(self, elem, window=beam.DoFn.WindowParam):
    team, score = elem
    window = timestamp2str(int(window.start))
    yield 'window_start: %s, total_score: %s, team: %s' % (window, score, team)


class CalculateSpammyUsers(beam.PTransform):
  """Filter out all but those users with a high clickrate, which we will
  consider as 'spammy' uesrs.

  We do this by finding the mean total score per user, then using that
  information as a side input to filter out all but those user scores that are
  larger than (mean * SCORE_WEIGHT).
  """
  SCORE_WEIGHT = 2.5

  def expand(self, user_scores):
    sum_scores = (
        user_scores
        | 'SumUsersScores' >> beam.CombinePerKey(sum)
    )

    global_mean_score = (
        sum_scores
        | beam.Values()
        | beam.CombineGlobally(combiners.MeanCombineFn()).as_singleton_view()
    )

    filtered = (
        sum_scores
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


def main():
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument('--topic',
                      type=str,
                      required=True,
                      help='Pub/Sub topic to read from')
  parser.add_argument('--output',
                      type=str,
                      required=True,
                      help='Path to the output file(s).')
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
  args, pipeline_args = parser.parse_known_args()

  # Enforce that this pipeline is always run in streaming mode
  pipeline_args += ['--streaming']

  fixed_window_duration = args.fixed_window_duration * 60
  session_gap = args.session_gap * 60
  user_activity_window_duration = args.user_activity_window_duration * 60

  with beam.Pipeline(argv=pipeline_args) as p:
    raw_events = (
        p
        | 'ReadPubSub' >> beam.io.gcp.pubsub.ReadStringsFromPubSub(args.topic)
        | 'ParseGameEvent' >> beam.FlatMap(parse_game_event)
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
    )

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
        | 'CalculateSpammyUsers' >> CalculateSpammyUsers()

        # Derive a view from the collection of spammer users. It will be used as
        # a side input in calculating the team score sums, below
        | 'CreateSpammersView' >> beam.CombineGlobally(
            combiners.ToDictCombineFn()).as_singleton_view()
    )

    # Calculate the total score per team over fixed windows, and emit cumulative
    # updates for late data. Uses the side input derived above-- the set of
    # suspected robots-- to filter out scores from those users from the sum.
    (raw_events  # pylint: disable=expression-not-assigned
     | 'WindowIntoFixedWindows' >> beam.WindowInto(
         beam.window.FixedWindows(fixed_window_duration))

     # Filter out the detected spammer users, using the side input derived above
     | 'FilterOutSpammers' >> beam.Filter(
         lambda elem, spammers: elem['user'] not in spammers,
         spammers_view)
     | 'ExtractTeamScores' >> beam.Map(
         lambda elem: (elem['team'], elem['score']))
     | 'SumTeamScores' >> beam.CombinePerKey(sum)
     | 'FormatTeamScores' >> beam.ParDo(FormatTeamScores())
     | 'WriteTeamScoreSums' >> beam.io.WriteToText(args.output)
    )

    # Detect user sessions-- that is, a burst of activity separated by a gap
    # from further activity. Find and record the mean session lengths.
    # This information could help the game designers track the changing user
    # engagement as their set of game changes.
    (user_events  # pylint: disable=expression-not-assigned
     | 'WindowIntoSessions' >> beam.WindowInto(
         beam.window.Sessions(session_gap),
         timestamp_combiner=beam.window.TimestampCombiner.OUTPUT_AT_EOW)

     # For this use, we care only about the existence of the session, not any
     # particular information aggregated over it, so we can just group by key
     # and assign a "dummy score" of 0.
     | beam.CombinePerKey(lambda _: 0)

     # Get the duration of the session
     | 'UserSessionActivity' >> beam.ParDo(UserSessionActivity())

     # Re-window to process groups of session sums according to when the
     # sessions complete
     | 'WindowToExtractSessionMean' >> beam.WindowInto(
         beam.window.FixedWindows(user_activity_window_duration))

     # Find the mean session duration in each window
     | beam.CombineGlobally(combiners.MeanCombineFn()).without_defaults()
     | 'WriteAvgSessionLength' >> beam.io.WriteToText(args.output+'-sessions')
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
