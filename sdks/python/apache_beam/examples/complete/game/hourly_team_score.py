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

Required options:
  --output=OUTPUT_PATH

Options with default values:
  --input='gs://apache-beam-samples/game/gaming_data*.csv'
  --window_duration=60
  --start_min='1970-01-01-00-00'
  --stop_min='2100-01-01-00-00'

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


def str2timestamp(s, fmt='%Y-%m-%d-%H-%M'):
  """Converts a string into a unix timestamp."""
  dt = datetime.strptime(s, fmt)
  epoch = datetime.utcfromtimestamp(0)
  return (dt - epoch).total_seconds()


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


class HourlyTeamScore(beam.PTransform):
  def __init__(self, start_min, stop_min, window_duration):
    super(HourlyTeamScore, self).__init__()
    self.start_timestamp = str2timestamp(start_min)
    self.stop_timestamp = str2timestamp(stop_min)
    self.window_duration = window_duration * 60

  def expand(self, pcoll):
    return (
        pcoll
        | 'ParseGameEvent' >> beam.FlatMap(parse_game_event)
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
        # Convert element['timestamp'] into seconds as expected by
        # TimestampedValue.
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
        | 'FixedWindowsTeam' >> beam.WindowInto(
            beam.window.FixedWindows(self.window_duration))
        | 'ExtractTeamScores' >> beam.Map(
            lambda elem: (elem['team'], elem['score']))
        | 'SumTeamScores' >> beam.CombinePerKey(sum)
    )


def main():
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument('--input',
                      type=str,
                      default='gs://apache-beam-samples/game/gaming_data*.csv',
                      help='Path to the data file(s) containing game data.')
  parser.add_argument('--output',
                      type=str,
                      required=True,
                      help='Path to the output file(s).')
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

  args, pipeline_args = parser.parse_known_args()

  with beam.Pipeline(argv=pipeline_args) as p:
    (p  # pylint: disable=expression-not-assigned
     | 'ReadInputText' >> beam.io.ReadFromText(args.input)
     | 'HourlyTeamScore' >> HourlyTeamScore(
         args.start_min, args.end_min, args.window_duration)
     | 'FormatTeamScores' >> beam.ParDo(FormatTeamScores())
     | 'WriteTeamScoreSums' >> beam.io.WriteToText(args.output)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
