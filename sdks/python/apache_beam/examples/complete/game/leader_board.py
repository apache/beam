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
<beam_root>/examples/java$ mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
    -Dexec.args="$PROJECT_ID $PUBSUB_TOPIC none"

For a description of the usage and options, use -h or --help.

To specify a different runner:
  --runner YOUR_RUNNER

NOTE: When specifying a different runner, additional runner-specific options
      may have to be passed in as well

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
    --temp_location gs://$BUCKET/user_score/temp
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)


class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    super(ParseGameEventFn, self).__init__()
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
            | beam.Map(lambda elem: (elem[self.field], elem['score']))
            | beam.CombinePerKey(sum))


class TeamScoresDict(beam.DoFn):
  """Formats the data into a dictionary of BigQuery columns with their values

  Receives a (team, score) pair, extracts the window start timestamp, and
  formats everything together into a dictionary. The dictionary is in the format
  {'bigquery_column': value}
  """
  def process(self, team_score, window=beam.DoFn.WindowParam):
    team, score = team_score
    start = timestamp2str(int(window.start))
    yield {
        'team': team,
        'total_score': score,
        'window_start': start,
        'processing_time': timestamp2str(int(time.time()))
    }


class WriteToBigQuery(beam.PTransform):
  """Generate, format, and write BigQuery table row information."""
  def __init__(self, table_name, dataset, schema, project):
    """Initializes the transform.
    Args:
      table_name: Name of the BigQuery table to use.
      dataset: Name of the dataset to use.
      schema: Dictionary in the format {'column_name': 'bigquery_type'}
      project: Name of the Cloud project containing BigQuery table.
    """
    super(WriteToBigQuery, self).__init__()
    self.table_name = table_name
    self.dataset = dataset
    self.schema = schema
    self.project = project

  def get_schema(self):
    """Build the output table schema."""
    return ', '.join(
        '%s:%s' % (col, self.schema[col]) for col in self.schema)

  def expand(self, pcoll):
    return (
        pcoll
        | 'ConvertToRow' >> beam.Map(
            lambda elem: {col: elem[col] for col in self.schema})
        | beam.io.WriteToBigQuery(
            self.table_name, self.dataset, self.project, self.get_schema()))


# [START window_and_trigger]
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
        | 'ExtractAndSumScore' >> ExtractAndSumScore('team'))
# [END window_and_trigger]


# [START processing_time_trigger]
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
        | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))
# [END processing_time_trigger]


def run(argv=None):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument('--topic',
                      type=str,
                      help='Pub/Sub topic to read from')
  parser.add_argument('--subscription',
                      type=str,
                      help='Pub/Sub subscription to read from')
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

  if args.topic is None and args.subscription is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: one of --topic or --subscription is required')
    sys.exit(1)

  options = PipelineOptions(pipeline_args)

  # We also require the --project option to access --dataset
  if options.view_as(GoogleCloudOptions).project is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --project is required')
    sys.exit(1)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  options.view_as(SetupOptions).save_main_session = True

  # Enforce that this pipeline is always run in streaming mode
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    # Read game events from Pub/Sub using custom timestamps, which are extracted
    # from the pubsub data elements, and parse the data.

    # Read from PubSub into a PCollection.
    if args.subscription:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
          subscription=args.subscription)
    else:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
          topic=args.topic)

    events = (
        scores
        | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn())
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])))

    # Get team scores and write the results to BigQuery
    (events  # pylint: disable=expression-not-assigned
     | 'CalculateTeamScores' >> CalculateTeamScores(
         args.team_window_duration, args.allowed_lateness)
     | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
     | 'WriteTeamScoreSums' >> WriteToBigQuery(
         args.table_name + '_teams', args.dataset, {
             'team': 'STRING',
             'total_score': 'INTEGER',
             'window_start': 'STRING',
             'processing_time': 'STRING',
         }, options.view_as(GoogleCloudOptions).project))

    def format_user_score_sums(user_score):
      (user, score) = user_score
      return {'user': user, 'total_score': score}

    # Get user scores and write the results to BigQuery
    (events  # pylint: disable=expression-not-assigned
     | 'CalculateUserScores' >> CalculateUserScores(args.allowed_lateness)
     | 'FormatUserScoreSums' >> beam.Map(format_user_score_sums)
     | 'WriteUserScoreSums' >> WriteToBigQuery(
         args.table_name + '_users', args.dataset, {
             'user': 'STRING',
             'total_score': 'INTEGER',
         }, options.view_as(GoogleCloudOptions).project))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
