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

"""A word-counting workflow that uses the SQL transform.

A Java version supported by Beam must be installed locally to run this pipeline.
Additionally, Docker must also be available to run this pipeline locally.
"""

import argparse
import logging
import re
from typing import NamedTuple

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.transforms.sql import SqlTransform

# The input to SqlTransform must be a PCollection(s) of known schema.
# One way to create such a PCollection is to produce a PCollection of
# NamedTuple registered with the RowCoder.
#
# Here we create and register a simple NamedTuple with a single str typed
# field named 'word' which we will use below.
MyRow = NamedTuple('MyRow', [('word', str)])
coders.registry.register_coder(MyRow, coders.RowCoder)


def run(p, input_file, output_file):
  #pylint: disable=expression-not-assigned
  (
      p
      # Read the lines from a text file.
      | 'Read' >> ReadFromText(input_file)
      # Split the line into individual words.
      | 'Split' >> beam.FlatMap(lambda line: re.split(r'\W+', line))
      # Map each word to an instance of MyRow.
      | 'ToRow' >> beam.Map(MyRow).with_output_types(MyRow)
      # SqlTransform yields a PCollection containing elements with attributes
      # based on the output of the query.
      | 'Sql!!' >> SqlTransform(
          """
                   SELECT
                     word as key,
                     COUNT(*) as `count`
                   FROM PCOLLECTION
                   GROUP BY word""")
      | 'Format' >> beam.Map(lambda row: '{}: {}'.format(row.key, row.count))
      | 'Write' >> WriteToText(output_file))


def main():
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')

  known_args, pipeline_args = parser.parse_known_args()

  pipeline_options = PipelineOptions(pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    if isinstance(p.runner, portable_runner.PortableRunner):
      # Preemptively start due to BEAM-6666.
      p.runner.create_job_service(pipeline_options)

    run(p, known_args.input, known_args.output)


# Some more fun queries:
# ------
# SELECT
#   word as key,
#   COUNT(*) as `count`
# FROM PCOLLECTION
# GROUP BY word
# ORDER BY `count` DESC
# LIMIT 100
# ------
# SELECT
#   len as key,
#   COUNT(*) as `count`
# FROM (
#   SELECT
#     LENGTH(word) AS len
#   FROM PCOLLECTION
# )
# GROUP BY len

if __name__ == '__main__':
  main()
