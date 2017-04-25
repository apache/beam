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

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def __init__(self):
    super(WordExtractingDoFn, self).__init__()
    self.words_counter = Metrics.counter(self.__class__, 'words')
    self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
    self.word_lengths_dist = Metrics.distribution(
        self.__class__, 'word_len_dist')
    self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    text_line = element.strip()
    if not text_line:
      self.empty_line_counter.inc(1)
    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      self.words_counter.inc()
      self.word_lengths_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
    return words


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  class WordcountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument(
          '--input',
          dest='input',
          default='gs://dataflow-samples/shakespeare/kinglear.txt',
          help='Input file to process.')
      parser.add_value_provider_argument(
          '--output',
          dest='output',
          required=True,
          help='Output file to write results to.')
  pipeline_options = PipelineOptions(argv)
  wordcount_options = pipeline_options.view_as(WordcountOptions)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(wordcount_options.input)

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
  output | 'write' >> WriteToText(wordcount_options.output)

  # Actually run the pipeline (all operations above are deferred).
  result = p.run()
  result.wait_until_finish()

  # Do not query metrics when creating a template which doesn't run
  if (not hasattr(result, 'has_job')    # direct runner
      or result.has_job):               # not just a template creation
    empty_lines_filter = MetricsFilter().with_name('empty_lines')
    query_result = result.metrics().query(empty_lines_filter)
    if query_result['counters']:
      empty_lines_counter = query_result['counters'][0]
      logging.info('number of empty lines: %d', empty_lines_counter.committed)
    # TODO(pabloem)(BEAM-1366): Add querying of MEAN metrics.


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
