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
Concepts: batch processing; reading input from Google Cloud Storage or a from a
local text file, and writing output to a text file; use of the CombinePerKey
transform.

In this gaming scenario, many users play, as members of different teams, over
the course of a day, and their actions are logged for processing. Some of the
logged game events may be late-arriving, if users play on mobile devices and go
transiently offline for a period of time.

This pipeline does batch processing of data collected from gaming events. It
calculates the sum of scores per user, over an entire batch of gaming data
(collected, say, for each day). The batch processing will not include any late
data that arrives after the day's cutoff point.

For a description of the usage and options, use -h or --help.

Required options:
  --output=OUTPUT_PATH

Options with default values:
  --input='gs://apache-beam-samples/game/gaming_data*.csv'

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

import apache_beam as beam


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


class UserScore(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll
        | 'ParseGameEvent' >> beam.FlatMap(parse_game_event)
        # Extract and sum username/score pairs from the event data.
        | 'ExtractUserScores' >> beam.Map(
            lambda elem: (elem['user'], elem['score']))
        | 'SumUserScores' >> beam.CombinePerKey(sum)
    )


def main():
  """Main entry point; defines and runs the user_score pipeline."""
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

  args, pipeline_args = parser.parse_known_args()

  with beam.Pipeline(argv=pipeline_args) as p:
    (p  # pylint: disable=expression-not-assigned
     | 'ReadInputText' >> beam.io.ReadFromText(args.input)
     | 'UserScore' >> UserScore()
     | 'FormatUserScoreSums' >> beam.Map(
         lambda (user, score): 'total_score: %s, user: %s' % (score, user))
     | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
