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
local text file, and writing output to a text file; using standalone DoFns; use
of the CombinePerKey transform.

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

EXAMPLES
--------

# DirectRunner
python user_score.py \
    --output /local/path/user_score/output

# DataflowRunner
python user_score.py \
    --output gs://$BUCKET/user_score/output \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --region $REGION_ID \
    --temp_location gs://$BUCKET/user_score/temp
"""

# pytype: skip-file

# beam-playground:
#   name: UserScore
#   description: batch processing; reading input from Google Cloud Storage or a
#     from a local text file, and writing output to a text file; using
#     standalone DoFns; use of the CombinePerKey transform.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 174
#   categories:
#     - Batch
#     - Combiners
#     - Options
#   complexity: ADVANCED
#   tags:
#     - batch
#     - combine
#     - io
#     - strings

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.DoFn.__init__(self)
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)


# [START extract_and_sum_score]
class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.
  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda elem: (elem[self.field], elem['score']))
        | beam.CombinePerKey(sum))


# [END extract_and_sum_score]


class UserScore(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll
        | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn())
        # Extract and sum username/score pairs from the event data.
        | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))


# [START main]
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the user_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument(
      '--input',
      type=str,
      default='gs://apache-beam-samples/game/small/gaming_data.csv',
      help='Path to the data file(s) containing game data.')
  parser.add_argument(
      '--output', type=str, required=True, help='Path to the output file(s).')

  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=options) as p:

    def format_user_score_sums(user_score):
      (user, score) = user_score
      return 'user: %s, total_score: %s' % (user, score)


    (  # pylint: disable=expression-not-assigned
        p
        | 'ReadInputText' >> beam.io.ReadFromText(args.input)
        | 'UserScore' >> UserScore()
        | 'FormatUserScoreSums' >> beam.Map(format_user_score_sums)
        | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output))


# [END main]

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
