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
from __future__ import print_function

import argparse
import csv
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.transforms import trigger

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
  def process(self, team_score, window=beam.DoFn.WindowParam):
    team, score = team_score
    start = timestamp2str(int(window.start))
    yield 'window_start: %s, total_score: %s, team: %s' % (start, score, team)

class CalculateTeamScoreSums(beam.PTransform):
  """Calculates scores for each team within the configured window duration.

  Extract team/score pairs from the event stream, using hour-long windows by
  default.
  """
  def __init__(self, team_window_duration, allowed_lateness):
    super(CalculateTeamScoreSums, self).__init__()
    self.team_window_duration = team_window_duration * 60
    self.allowed_lateness = allowed_lateness * 60

  def expand(self, pcoll):
    return (
        # TODO: allowed_lateness not implemented yet in FixedWindows
        # TODO: AfterProcessingTime not implemented yet, replace AfterCount
        pcoll
        | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(self.team_window_duration),
            trigger=trigger.AfterWatermark(trigger.AfterCount(10),
                                           trigger.AfterCount(20)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        | 'ExtractTeamScores' >> beam.Map(
            lambda elem: (elem['team'], elem['score']))
        | 'SumTeamScores' >> beam.CombinePerKey(sum)
    )

class CalculateUserScoreSums(beam.PTransform):
  """Extract user/score pairs from the event stream using processing time, via
  global windowing.

  Get periodic updates on all users' running scores.
  """
  def __init__(self, allowed_lateness):
    super(CalculateUserScoreSums, self).__init__()
    self.allowed_lateness = allowed_lateness * 60

  def expand(self, pcoll):
    return (
        # TODO: allowed_lateness not implemented yet in FixedWindows
        # TODO: AfterProcessingTime not implemented yet, replace AfterCount
        pcoll
        | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        | 'ExtractUserScores' >> beam.Map(
            lambda elem: (elem['user'], elem['score']))
        | 'SumUserScores' >> beam.CombinePerKey(sum)
    )

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
  parser.add_argument('--team_window_duration',
                      type=int,
                      default=60,
                      help='Numeric value of fixed window duration for team '
                           'analysis, in minutes')
  parser.add_argument('--allowed_lateness',
                      type=int,
                      default=120,
                      help='Numeric value of allowed data lateness, in minutes')

  args, pipeline_args = parser.parse_known_args()

  # Enforce that this pipeline is always run in streaming mode
  pipeline_args += ['--streaming']

  with beam.Pipeline(argv=pipeline_args) as p:
    events = (
        p
        | 'ReadPubSub' >> beam.io.gcp.pubsub.ReadStringsFromPubSub(args.topic)
        | 'ParseGameEvent' >> beam.FlatMap(parse_game_event)
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
    )

    (events  # pylint: disable=expression-not-assigned
     | 'CalculateTeamScoreSums' >> CalculateTeamScoreSums(
         args.team_window_duration, args.allowed_lateness)
     | 'FormatTeamScores' >> beam.ParDo(FormatTeamScores())
     | 'WriteTeamScoreSums' >> beam.io.WriteToText(args.output+'-team_scores')
    )

    (events  # pylint: disable=expression-not-assigned
     | 'CalculateUserScoreSums' >> CalculateUserScoreSums(allowed_lateness)
     | 'FormatUserScoreSums' >> beam.Map(
         lambda (user, score): 'total_score: %s, user: %s' % (score, user))
     | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output+'-user_scores')
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
