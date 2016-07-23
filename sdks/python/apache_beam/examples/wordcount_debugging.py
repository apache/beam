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

"""An example that verifies the counts and includes Dataflow best practices.

On top of the basic concepts in the wordcount example, this workflow introduces
logging to Cloud Logging, and using assertions in a Dataflow pipeline.

To execute this pipeline locally, specify a local output file or output prefix
on GCS::

  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration::

  --project YOUR_PROJECT_ID
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMP_DIRECTORY
  --job_name YOUR_JOB_NAME
  --runner BlockingDataflowPipelineRunner

and an output prefix on GCS::

  --output gs://YOUR_OUTPUT_PREFIX
"""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


class FilterTextFn(beam.DoFn):
  """A DoFn that filters for a specific key based on a regular expression."""

  # A custom aggregator can track values in your pipeline as it runs. Those
  # values will be displayed in the Dataflow Monitoring UI when this pipeline is
  # run using the Dataflow service. These aggregators below track the number of
  # matched and unmatched words. Learn more at
  # https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf about
  # the Dataflow Monitoring UI.
  matched_words = beam.Aggregator('matched_words')
  umatched_words = beam.Aggregator('umatched_words')

  def __init__(self, pattern):
    super(FilterTextFn, self).__init__()
    self.pattern = pattern

  def process(self, context):
    word, _ = context.element
    if re.match(self.pattern, word):
      # Log at INFO level each element we match. When executing this pipeline
      # using the Dataflow service, these log lines will appear in the Cloud
      # Logging UI.
      logging.info('Matched %s', word)
      context.aggregate_to(self.matched_words, 1)
      yield context.element
    else:
      # Log at the "DEBUG" level each element that is not matched. Different log
      # levels can be used to control the verbosity of logging providing an
      # effective mechanism to filter less important information.
      # Note currently only "INFO" and higher level logs are emitted to the
      # Cloud Logger. This log message will not be visible in the Cloud Logger.
      logging.debug('Did not match %s', word)
      context.aggregate_to(self.umatched_words, 1)


class CountWords(beam.PTransform):
  """A transform to count the occurrences of each word.

  A PTransform that converts a PCollection containing lines of text into a
  PCollection of (word, count) tuples.
  """

  def __init__(self):
    super(CountWords, self).__init__()

  def apply(self, pcoll):
    return (pcoll
            | 'split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones))))


def run(argv=None):
  """Runs the debugging wordcount pipeline."""

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

  # Read the text file[pattern] into a PCollection, count the occurrences of
  # each word and filter by a list of words.
  filtered_words = (
      p | 'read' >> beam.io.Read(beam.io.TextFileSource(known_args.input))
      | CountWords()
      | 'FilterText' >> beam.ParDo(FilterTextFn('Flourish|stomach')))

  # assert_that is a convenient PTransform that checks a PCollection has an
  # expected value. Asserts are best used in unit tests with small data sets but
  # is demonstrated here as a teaching tool.
  #
  # Note assert_that does not provide any output and that successful completion
  # of the Pipeline implies that the expectations were  met. Learn more at
  # https://cloud.google.com/dataflow/pipelines/testing-your-pipeline on how to
  # test your pipeline.
  beam.assert_that(
      filtered_words, beam.equal_to([('Flourish', 3), ('stomach', 1)]))

  # Format the counts into a PCollection of strings and write the output using a
  # "Write" transform that has side effects.
  # pylint: disable=unused-variable
  output = (filtered_words
            | 'format' >> beam.Map(lambda (word, c): '%s: %s' % (word, c))
            | 'write' >> beam.io.Write(beam.io.TextFileSink(known_args.output)))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


if __name__ == '__main__':
  # Cloud Logging would contain only logging.INFO and higher level logs logged
  # by the root logger. All log statements emitted by the root logger will be
  # visible in the Cloud Logging UI. Learn more at
  # https://cloud.google.com/logging about the Cloud Logging UI.
  #
  # You can set the default logging level to a different level when running
  # locally.
  logging.getLogger().setLevel(logging.INFO)
  run()
