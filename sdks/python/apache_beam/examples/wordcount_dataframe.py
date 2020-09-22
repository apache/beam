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

"""A word-counting workflow using dataframes."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
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
  known_args, pipeline_args = parser.parse_known_args(argv)

  # Import this here to avoid pickling the main session.
  import re

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    words = (
        lines
        | 'Split' >> beam.FlatMap(
            lambda line: re.findall(r'[\w]+', line)).with_output_types(str)
        # Map to Row objects to generate a schema suitable for conversion
        # to a dataframe.
        | 'ToRows' >> beam.Map(lambda word: beam.Row(word=word)))

    df = to_dataframe(words)
    df['count'] = 1
    counted = df.groupby('word').sum()
    counted.to_csv(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
