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

"""An example that reads Wikipedia edit data and computes strings of edits.

An example that reads Wikipedia edit data from Cloud Storage and computes the
user with the longest string of edits separated by no more than an hour within
each 30 day period.

To execute this pipeline locally using the DirectPipelineRunner, specify an
output prefix on GCS:
  --output gs://YOUR_OUTPUT_PREFIX

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration in addition to the above:
  --job_name NAME_FOR_YOUR_JOB
  --project YOUR_PROJECT_ID
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMPORARY_DIRECTORY
  --runner BlockingDataflowPipelineRunner

The default input is gs://dataflow-samples/wikipedia_edits/*.json and can be
overridden with --input.
"""

from __future__ import absolute_import

import argparse
import json
import logging

import apache_beam as beam
from apache_beam import combiners
from apache_beam import window
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


ONE_HOUR_IN_SECONDS = 3600
THIRTY_DAYS_IN_SECONDS = 30 * 24 * ONE_HOUR_IN_SECONDS
MAX_TIMESTAMP = 0x7fffffffffffffff


class ExtractUserAndTimestampDoFn(beam.DoFn):
  """Extracts user and timestamp representing a Wikipedia edit."""

  def process(self, context):
    table_row = json.loads(context.element)
    if 'contributor_username' in table_row:
      user_name = table_row['contributor_username']
      timestamp = table_row['timestamp']
      yield window.TimestampedValue(user_name, timestamp)


class ComputeSessions(beam.PTransform):
  """Computes the number of edits in each user session.

  A session is defined as a string of edits where each is separated from the
  next by less than an hour.
  """

  def __init__(self):
    super(ComputeSessions, self).__init__()

  def apply(self, pcoll):
    return (pcoll
            | beam.WindowInto('ComputeSessionsWindow',
                              window.Sessions(gap_size=ONE_HOUR_IN_SECONDS))
            | combiners.Count.PerElement())


class TopPerMonth(beam.PTransform):
  """Computes the longest session ending in each month."""

  def __init__(self):
    super(TopPerMonth, self).__init__()

  def apply(self, pcoll):
    return (pcoll
            | beam.WindowInto('TopPerMonthWindow',
                              window.FixedWindows(
                                  size=THIRTY_DAYS_IN_SECONDS))
            | combiners.core.CombineGlobally(
                'Top',
                combiners.TopCombineFn(
                    10, lambda first, second: first[1] < second[1]))
            .without_defaults())


class SessionsToStringsDoFn(beam.DoFn):
  """Adds the session information to be part of the key."""

  def process(self, context):
    yield (context.element[0] + ' : ' +
           ', '.join([str(w) for w in context.windows]), context.element[1])


class FormatOutputDoFn(beam.DoFn):
  """Formats a string containing the user, count, and session."""

  def process(self, context):
    for kv in context.element:
      session = kv[0]
      count = kv[1]
      yield (session + ' : ' + str(count) + ' : '
             + ', '.join([str(w) for w in context.windows]))


class ComputeTopSessions(beam.PTransform):
  """Computes the top user sessions for each month."""

  def __init__(self, sampling_threshold):
    super(ComputeTopSessions, self).__init__()
    self.sampling_threshold = sampling_threshold

  def apply(self, pcoll):
    return (pcoll
            | beam.ParDo('ExtractUserAndTimestamp',
                         ExtractUserAndTimestampDoFn())
            | beam.Filter(lambda x: (abs(hash(x)) <=
                                     MAX_TIMESTAMP * self.sampling_threshold))
            | ComputeSessions()
            | 'SessionsToStrings' >> beam.ParDo(SessionsToStringsDoFn())
            | TopPerMonth()
            | 'FormatOutput' >> beam.ParDo(FormatOutputDoFn()))


def run(argv=None):
  """Runs the Wikipedia top edits pipeline.

  Args:
    argv: Pipeline options as a list of arguments.
  """

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/wikipedia_edits/*.json',
      help='Input specified as a GCS path containing a BigQuery table exported '
      'as json.')
  parser.add_argument('--output',
                      required=True,
                      help='Output file to write results to.')
  parser.add_argument('--sampling_threshold',
                      type=float,
                      default=0.1,
                      help='Fraction of entries used for session tracking')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  (p  # pylint: disable=expression-not-assigned
   | beam.Read(beam.io.TextFileSource(known_args.input))
   | ComputeTopSessions(known_args.sampling_threshold)
   | beam.io.Write(beam.io.TextFileSink(known_args.output)))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
