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

To specify a different runner:
  --runner YOUR_RUNNER

NOTE: When specifying a different runner, additional runner-specific options
      may have to be passed in as well
NOTE: With DataflowRunner, the --setup_file flag must be specified to handle the
      'util' module

EXAMPLES
--------

# DirectRunner
python user_score.py \
    --output /local/path/user_score/output

# DataflowRunner
python user_score.py \
    --output gs://$BUCKET/user_score/output \
    --runner DataflowRunner \
    --setup_file ./setup.py \
    --project $PROJECT_ID \
    --staging_location gs://$BUCKET/user_score/staging \
    --temp_location gs://$BUCKET/user_score/temp
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.examples.complete.game.util import util


class UserScore(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll
        | 'ParseGameEventFn' >> beam.ParDo(util.ParseGameEventFn())
        # Extract and sum username/score pairs from the event data.
        | 'ExtractUserScores' >> beam.Map(
            lambda elem: (elem['user'], elem['score']))
        | 'SumUserScores' >> beam.CombinePerKey(sum)
    )


def run(argv=None):
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

  args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
    (p  # pylint: disable=expression-not-assigned
     | 'ReadInputText' >> beam.io.ReadFromText(args.input)
     | 'UserScore' >> UserScore()
     | 'FormatUserScoreSums' >> beam.Map(
         lambda (user, score): 'user: %s, total_score: %s' % (user, score))
     | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
