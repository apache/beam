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

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


empty_line_aggregator = beam.Aggregator('emptyLines')
average_word_size_aggregator = beam.Aggregator('averageWordLength',
                                               beam.combiners.MeanCombineFn(),
                                               float)


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, context):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      context: the call-specific context: data and aggregator.

    Returns:
      The processed element.
    """
    text_line = context.element.strip()
    if not text_line:
      context.aggregate_to(empty_line_aggregator, 1)
    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      context.aggregate_to(average_word_size_aggregator, len(w))
    return words


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> beam.io.Read(beam.io.TextFileSource(known_args.input))

  # Count the occurrences of each word.
  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones))))

  # Format the counts into a PCollection of strings.
  output = counts | 'format' >> beam.Map(lambda (word, c): '%s: %s' % (word, c))

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'write' >> beam.io.Write(beam.io.TextFileSink(known_args.output))

  # Actually run the pipeline (all operations above are deferred).
  result = p.run()
  empty_line_values = result.aggregated_values(empty_line_aggregator)
  logging.info('number of empty lines: %d', sum(empty_line_values.values()))
  word_length_values = result.aggregated_values(average_word_size_aggregator)
  logging.info('average word lengths: %s', word_length_values.values())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
