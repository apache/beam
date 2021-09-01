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

"""A workflow using custom JSON-based coders for text sources and sinks.

The input file contains a JSON string on each line describing a match
record using the following schema:

  {'guest': [TEAM_NAME, GOALS], 'host': [TEAM_NAME, GOALS]}

The output file will contain the computed points for each team with one team
per line in the following format:

  [TEAM_NAME, POINTS]
"""

# pytype: skip-file

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""
  def encode(self, x):
    return json.dumps(x).encode('utf-8')

  def decode(self, x):
    return json.loads(x)


def compute_points(record):
  """Compute points based on the record containing the match result.

  The function assigns 3 points for a win, 1 point for a draw, and 0 points for
  a loss (see http://en.wikipedia.org/wiki/Three_points_for_a_win).
  """
  host_name, host_goals = record['host']
  guest_name, guest_goals = record['guest']
  if host_goals == guest_goals:
    yield host_name, 1
    yield guest_name, 1
  elif host_goals > guest_goals:
    yield host_name, 3
    yield guest_name, 0
  else:
    yield host_name, 0
    yield guest_name, 3


def run(argv=None):
  """Runs the workflow computing total points from a collection of matches."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='Input file to process.')
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    (  # pylint: disable=expression-not-assigned
        p
        | 'read' >> ReadFromText(known_args.input, coder=JsonCoder())
        | 'points' >> beam.FlatMap(compute_points)
        | beam.CombinePerKey(sum)
        | 'write' >> WriteToText(known_args.output, coder=JsonCoder()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
