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

"""A cross-language word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging
import re
import subprocess

import grpc

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

EXPANSION_SERVICE_PORT = '8097'
EXPANSION_SERVICE_ADDR = 'localhost:%s' % EXPANSION_SERVICE_PORT


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    text_line = element.strip()
    words = [bytes(x) for x in re.findall(r'[\w\']+', text_line)]
    return words


def run(pipeline_args, input_file, output_file):

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(input_file)

  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(bytes))
            | 'count' >> beam.ExternalTransform(
                'pytest:beam:transforms:count', None, EXPANSION_SERVICE_ADDR))

  # Format the counts into a PCollection of strings.
  def format_result(word_count):
    (word, count) = word_count
    return '%s: %d' % (word, count)

  output = counts | 'format' >> beam.Map(format_result)

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'write' >> WriteToText(output_file)

  result = p.run()
  result.wait_until_finish()


def wait_for_ready():
  with grpc.insecure_channel(EXPANSION_SERVICE_ADDR) as channel:
    grpc.channel_ready_future(channel).result()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  parser.add_argument('--expansion_service_jar',
                      dest='expansion_service_jar',
                      required=True,
                      help='Jar file for expansion service')

  known_args, pipeline_args = parser.parse_known_args()
  try:
    server = subprocess.Popen([
        'java', '-jar', known_args.expansion_service_jar,
        EXPANSION_SERVICE_PORT])
    wait_for_ready()

    run(pipeline_args, known_args.input, known_args.output)

  finally:
    server.kill()
