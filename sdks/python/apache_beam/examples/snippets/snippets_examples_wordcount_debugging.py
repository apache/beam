# coding=utf-8
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

# pytype: skip-file

# Quiet some pylint warnings that happen because of the somewhat special
# format for the code snippets.
# pylint:disable=invalid-name
# pylint:disable=expression-not-assigned
# pylint:disable=redefined-outer-name
# pylint:disable=reimported
# pylint:disable=unused-variable
# pylint:disable=wrong-import-order, wrong-import-position

# beam-playground:
#   name: WordCountDebuggingSnippet
#   description: An example that counts words in Shakespeare's works.
#     includes regex filter("Flourish|stomach").
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 57
#   categories:
#     - Flatten
#     - Debugging
#     - Options
#     - Combiners
#     - Filtering
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - count
#     - debug
#     - string

import apache_beam as beam
from apache_beam.examples.snippets.snippets import SnippetUtils
from apache_beam.metrics import Metrics
from apache_beam.testing.test_pipeline import TestPipeline


def examples_wordcount_debugging(renames):
  """DebuggingWordCount example snippets."""
  import re

  # [START example_wordcount_debugging_logging]
  # [START example_wordcount_debugging_aggregators]
  import logging

  class FilterTextFn(beam.DoFn):
    """A DoFn that filters for a specific key based on a regular expression."""
    def __init__(self, pattern):
      self.pattern = pattern
      # A custom metric can track values in your pipeline as it runs. Create
      # custom metrics matched_word and unmatched_words.
      self.matched_words = Metrics.counter(self.__class__, 'matched_words')
      self.umatched_words = Metrics.counter(self.__class__, 'umatched_words')

    def process(self, element):
      word, _ = element
      if re.match(self.pattern, word):
        # Log at INFO level each element we match. When executing this pipeline
        # using the Dataflow service, these log lines will appear in the Cloud
        # Logging UI.
        logging.info('Matched %s', word)

        # Add 1 to the custom metric counter matched_words
        self.matched_words.inc()
        yield element
      else:
        # Log at the "DEBUG" level each element that is not matched. Different
        # log levels can be used to control the verbosity of logging providing
        # an effective mechanism to filter less important information. Note
        # currently only "INFO" and higher level logs are emitted to the Cloud
        # Logger. This log message will not be visible in the Cloud Logger.
        logging.debug('Did not match %s', word)

        # Add 1 to the custom metric counter umatched_words
        self.umatched_words.inc()

  # [END example_wordcount_debugging_logging]
  # [END example_wordcount_debugging_aggregators]

  with TestPipeline() as pipeline:  # Use TestPipeline for testing.
    # assert False
    filtered_words = (
        pipeline
        |
        beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
        |
        'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | beam.combiners.Count.PerElement()
        | 'FilterText' >> beam.ParDo(FilterTextFn('Flourish|stomach')))

    # [START example_wordcount_debugging_assert]
    beam.testing.util.assert_that(
        filtered_words,
        beam.testing.util.equal_to([('Flourish', 3), ('stomach', 1)]))

    # [END example_wordcount_debugging_assert]

    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = (
        filtered_words
        | 'format' >> beam.Map(format_result)
        | 'Write' >> beam.io.WriteToText('output.txt'))

    pipeline.visit(SnippetUtils.RenameFiles(renames))


if __name__ == '__main__':
  import glob
  examples_wordcount_debugging(None)
  for file_name in glob.glob('output.txt*'):
    with open(file_name) as f:
      print(f.read())
