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

"""A word-counting workflow."""

# pytype: skip-file

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


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
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True) -> PipelineResult:
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

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  pipeline = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = pipeline | 'Read' >> ReadFromText(known_args.input)

  counts = (
      lines
      | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
      | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
      | 'GroupAndSum' >> beam.CombinePerKey(sum))

  # Format the counts into a PCollection of strings.
  def format_result(word, count):
    return '%s: %d' % (word, count)

  output = counts | 'Format' >> beam.MapTuple(format_result)

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'Write' >> WriteToText(known_args.output)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
