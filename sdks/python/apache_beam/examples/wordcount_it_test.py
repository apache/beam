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

"""End-to-end test for the wordcount example."""

from __future__ import absolute_import

import argparse
import logging
import re
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr
from past.builtins import unicode

import apache_beam as beam
from apache_beam.examples import wordcount
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


def run_wordcount_without_save_main_session(argv):
  """Defines and runs a simple version of wordcount pipeline.

  This pipeline is the same as wordcount example except replace customized
  DoFn class with transform function and disable save_main_session option
  due to BEAM-6158."""
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

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  # Count the occurrences of each word.
  def count_ones(word_ones):
    (word, ones) = word_ones
    return (word, sum(ones))

  # Parse each line of input text into words.
  def extract_words(line):
    return re.findall(r'[\w\']+', line, re.UNICODE)

  # Format the counts into a PCollection of strings.
  def format_result(word_count):
    (word, count) = word_count
    return '%s: %d' % (word, count)

  # pylint: disable=expression-not-assigned
  (p | 'read' >> ReadFromText(known_args.input)
   | 'split' >> (beam.ParDo(extract_words).with_output_types(unicode))
   | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
   | 'group' >> beam.GroupByKey()
   | 'count' >> beam.Map(count_ones)
   | 'format' >> beam.Map(format_result)
   | 'write' >> WriteToText(known_args.output))

  result = p.run()
  result.wait_until_finish()


class WordCountIT(unittest.TestCase):

  # Enable nose tests running in parallel
  _multiprocess_can_split_ = True

  # The default checksum is a SHA-1 hash generated from a sorted list of
  # lines read from expected output.
  DEFAULT_CHECKSUM = '33535a832b7db6d78389759577d4ff495980b9c0'

  @attr('IT')
  def test_wordcount_it(self):
    self._run_wordcount_it(wordcount.run)

  @attr('IT', 'ValidatesContainer')
  def test_wordcount_fnapi_it(self):
    self._run_wordcount_it(wordcount.run, experiment='beam_fn_api')

  @attr('Py3IT')
  # TODO: Delete this test and use test_wordcount_fnapi_it instead
  # once BEAM-6158 is fixed.
  def test_wordcount_without_save_main_session(self):
    self._run_wordcount_it(run_wordcount_without_save_main_session,
                           experiment='beam_fn_api')

  def _run_wordcount_it(self, run_wordcount, **opts):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set extra options to the pipeline for test purpose
    output = '/'.join([test_pipeline.get_option('output'),
                       str(int(time.time() * 1000)),
                       'results'])
    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    pipeline_verifiers = [PipelineStateMatcher(),
                          FileChecksumMatcher(output + '*-of-*',
                                              self.DEFAULT_CHECKSUM,
                                              sleep_secs)]
    extra_opts = {'output': output,
                  'on_success_matcher': all_of(*pipeline_verifiers)}
    extra_opts.update(opts)

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    run_wordcount(test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
