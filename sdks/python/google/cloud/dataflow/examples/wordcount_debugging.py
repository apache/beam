# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example that verifies the counts and includes Dataflow best practices.

On top of the basic concepts in the wordcount example, this workflow introduces
logging to Cloud Logging, and using assertions in a Dataflow pipeline.

To execute this pipeline locally, specify a local output file or output prefix
on GCS:
  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration:
  --project YOUR_PROJECT_ID
  --stagingLocation gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMP_DIRECTORY
  --job_name YOUR_JOB_NAME
  --runner BlockingDataflowPipelineRunner

and an output prefix on GCS:
  --output gs://YOUR_OUTPUT_PREFIX
"""

from __future__ import absolute_import

import logging
import re

import google.cloud.dataflow as df
from google.cloud.dataflow.utils.options import add_option
from google.cloud.dataflow.utils.options import get_options


class FilterTextFn(df.DoFn):
  """A DoFn that filters for a specific key based on a regular expression."""

  # A custom aggregator can track values in your pipeline as it runs. Those
  # values will be displayed in the Dataflow Monitoring UI when this pipeline is
  # run using the Dataflow service. These aggregators below track the number of
  # matched and unmatched words. Learn more at
  # https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf about
  # the Dataflow Monitoring UI.
  matched_words = df.Aggregator('matched_words')
  umatched_words = df.Aggregator('umatched_words')

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


class AssertEqualsIgnoringOrderFn(df.DoFn):
  """A DoFn that asserts that its input is the same as the expected value.

  This DoFn is useful only for testing purposes with small data sets. It will
  materialize all of its input and assumes that its input is a singleton.
  """

  def __init__(self, expected_elements):
    super(AssertEqualsIgnoringOrderFn, self).__init__()
    self.expected_elements = expected_elements

  def process(self, context):
    assert sorted(context.element) == sorted(self.expected_elements), (
        'AssertEqualsIgnoringOrderFn input does not match expected value.'
        '%s != %s' % (context.element, self.expected_elements))


class CountWords(df.PTransform):
  """A transform to count the occurrences of each word.

  A PTransform that converts a PCollection containing lines of text into a
  PCollection of (word, count) tuples.
  """

  def __init__(self):
    super(CountWords, self).__init__()

  def apply(self, pcoll):
    return (pcoll
            | (df.FlatMap('split', lambda x: re.findall(r'[A-Za-z\']+', x))
               .with_output_types(unicode))
            | df.Map('pair_with_one', lambda x: (x, 1))
            | df.GroupByKey('group')
            | df.Map('count', lambda (word, ones): (word, sum(ones))))


def run(options=None):
  """Runs the debugging wordcount pipeline."""
  p = df.Pipeline(options=get_options(options))

  # Read the text file[pattern] into a PCollection, count the occurrences of
  # each word and filter by a list of words.
  filtered_words = (p
                    | df.io.Read('read', df.io.TextFileSource(p.options.input))
                    | CountWords()
                    | df.ParDo('FilterText', FilterTextFn('Flourish|stomach')))

  # AssertEqualsIgnoringOrderFn is a convenient DoFn to validate its input.
  # Asserts are best used in unit tests with small data sets but is demonstrated
  # here as a teaching tool.
  #
  # Note AssertEqualsIgnoringOrderFn does not provide any output and that
  # successful completion of the Pipeline implies that the expectations were
  # met. Learn more at
  # https://cloud.google.com/dataflow/pipelines/testing-your-pipeline on how to
  # test your pipeline.
  # pylint: disable=expression-not-assigned
  (filtered_words
   | df.transforms.combiners.ToList('ToList')
   | df.ParDo(AssertEqualsIgnoringOrderFn([('Flourish', 3), ('stomach', 1)])))

  # Format the counts into a PCollection of strings and write the output using a
  # "Write" transform that has side effects.
  # pylint: disable=unused-variable
  output = (filtered_words
            | df.Map('format', lambda (word, c): '%s: %s' % (word, c))
            | df.io.Write('write', df.io.TextFileSink(p.options.output)))

  # Actually run the pipeline (all operations above are deferred).
  p.run()

add_option(
    '--input', dest='input',
    default='gs://dataflow-samples/shakespeare/kinglear.txt',
    help='Input file to process.')
add_option(
    '--output', dest='output', required=True,
    help='Output file to write results to.')


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
