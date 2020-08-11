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

"""Code snippets used in webdocs.

The examples here are written specifically to read well with the accompanying
web docs. Do not rewrite them until you make sure the webdocs still read well
and the rewritten code supports the concept being described. For example, there
are snippets that could be shorter but they are written like this to make a
specific point in the docs.

The code snippets are all organized as self contained functions. Parts of the
function body delimited by [START tag] and [END tag] will be included
automatically in the web docs. The naming convention for the tags is to have as
prefix the PATH_TO_HTML where they are included followed by a descriptive
string. The tags can contain only letters, digits and _.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import base64
import json
from builtins import object
from builtins import range
from decimal import Decimal

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import PTransform

# Protect against environments where Google Cloud Natural Language client is
# not available.
try:
  from apache_beam.ml.gcp import naturallanguageml as nlp
except ImportError:
  nlp = None

# Quiet some pylint warnings that happen because of the somewhat special
# format for the code snippets.
# pylint:disable=invalid-name
# pylint:disable=expression-not-assigned
# pylint:disable=redefined-outer-name
# pylint:disable=reimported
# pylint:disable=unused-variable
# pylint:disable=wrong-import-order, wrong-import-position


class SnippetUtils(object):
  from apache_beam.pipeline import PipelineVisitor

  class RenameFiles(PipelineVisitor):
    """RenameFiles will rewire read/write paths for unit testing.

    RenameFiles will replace the GCS files specified in the read and
    write transforms to local files so the pipeline can be run as a
    unit test. This assumes that read and write transforms defined in snippets
    have already been replaced by transforms 'DummyReadForTesting' and
    'DummyReadForTesting' (see snippets_test.py).

    This is as close as we can get to have code snippets that are
    executed and are also ready to presented in webdocs.
    """
    def __init__(self, renames):
      self.renames = renames

    def visit_transform(self, transform_node):
      if transform_node.full_label.find('DummyReadForTesting') >= 0:
        transform_node.transform.fn.file_to_read = self.renames['read']
      elif transform_node.full_label.find('DummyWriteForTesting') >= 0:
        transform_node.transform.fn.file_to_write = self.renames['write']


def construct_pipeline(renames):
  """A reverse words snippet as an example for constructing a pipeline."""
  import re

  # This is duplicate of the import statement in
  # pipelines_constructing_creating tag below, but required to avoid
  # Unresolved reference in ReverseWords class
  import apache_beam as beam

  class ReverseWords(beam.PTransform):
    """A PTransform that reverses individual elements in a PCollection."""
    def expand(self, pcoll):
      return pcoll | beam.Map(lambda e: e[::-1])

  def filter_words(unused_x):
    """Pass through filter to select everything."""
    return True

  # [START pipelines_constructing_creating]
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  with beam.Pipeline(options=PipelineOptions()) as p:
    pass  # build your pipeline here
    # [END pipelines_constructing_creating]

    with TestPipeline() as p:  # Use TestPipeline for testing.
      # pylint: disable=line-too-long

      # [START pipelines_constructing_reading]
      lines = p | 'ReadMyFile' >> beam.io.ReadFromText(
          'gs://some/inputData.txt')
      # [END pipelines_constructing_reading]

      # [START pipelines_constructing_applying]
      words = lines | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
      reversed_words = words | ReverseWords()
      # [END pipelines_constructing_applying]

      # [START pipelines_constructing_writing]
      filtered_words = reversed_words | 'FilterWords' >> beam.Filter(
          filter_words)
      filtered_words | 'WriteMyFile' >> beam.io.WriteToText(
          'gs://some/outputData.txt')
      # [END pipelines_constructing_writing]

      p.visit(SnippetUtils.RenameFiles(renames))


def model_pipelines(argv):
  """A wordcount snippet as a simple pipeline example."""
  # [START model_pipelines]
  import re

  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          '--input',
          dest='input',
          default='gs://dataflow-samples/shakespeare/kinglear'
          '.txt',
          help='Input file to process.')
      parser.add_argument(
          '--output',
          dest='output',
          required=True,
          help='Output file to write results to.')

  pipeline_options = PipelineOptions(argv)
  my_options = pipeline_options.view_as(MyOptions)

  with beam.Pipeline(options=pipeline_options) as p:

    (
        p
        | beam.io.ReadFromText(my_options.input)
        | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | beam.Map(lambda x: (x, 1))
        | beam.combiners.Count.PerKey()
        | beam.io.WriteToText(my_options.output))
  # [END model_pipelines]


def model_pcollection(argv):
  """Creating a PCollection from data in local memory."""
  # [START model_pcollection]
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # argv = None  # if None, uses sys.argv
  pipeline_options = PipelineOptions(argv)
  with beam.Pipeline(options=pipeline_options) as pipeline:
    lines = (
        pipeline
        | beam.Create([
            'To be, or not to be: that is the question: ',
            "Whether 'tis nobler in the mind to suffer ",
            'The slings and arrows of outrageous fortune, ',
            'Or to take arms against a sea of troubles, ',
        ]))

    # [END model_pcollection]

    class MyOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--output',
            dest='output',
            required=True,
            help='Output file to write results to.')

    my_options = pipeline_options.view_as(MyOptions)
    lines | beam.io.WriteToText(my_options.output)


def pipeline_options_remote(argv):
  """Creating a Pipeline using a PipelineOptions object for remote execution."""

  # [START pipeline_options_create]
  from apache_beam.options.pipeline_options import PipelineOptions

  options = PipelineOptions(flags=argv)

  # [END pipeline_options_create]

  # [START pipeline_options_define_custom]
  from apache_beam.options.pipeline_options import PipelineOptions

  class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--input')
      parser.add_argument('--output')

  # [END pipeline_options_define_custom]

  # [START pipeline_options_dataflow_service]
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # Create and set your PipelineOptions.
  # For Cloud execution, specify DataflowRunner and set the Cloud Platform
  # project, job name, temporary files location, and region.
  # For more information about regions, check:
  # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
  options = PipelineOptions(
      flags=argv,
      runner='DataflowRunner',
      project='my-project-id',
      job_name='unique-job-name',
      temp_location='gs://my-bucket/temp',
      region='us-central1')

  # Create the Pipeline with the specified options.
  # with beam.Pipeline(options=options) as pipeline:
  #   pass  # build your pipeline here.
  # [END pipeline_options_dataflow_service]

  my_options = options.view_as(MyOptions)

  with TestPipeline() as p:  # Use TestPipeline for testing.
    lines = p | beam.io.ReadFromText(my_options.input)
    lines | beam.io.WriteToText(my_options.output)


def pipeline_options_local(argv):
  """Creating a Pipeline using a PipelineOptions object for local execution."""

  from apache_beam import Pipeline
  from apache_beam.options.pipeline_options import PipelineOptions

  # [START pipeline_options_define_custom_with_help_and_default]
  from apache_beam.options.pipeline_options import PipelineOptions

  class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          '--input',
          help='Input for the pipeline',
          default='gs://my-bucket/input')
      parser.add_argument(
          '--output',
          help='Output for the pipeline',
          default='gs://my-bucket/output')

  # [END pipeline_options_define_custom_with_help_and_default]

  # [START pipeline_options_local]
  # Create and set your Pipeline Options.
  options = PipelineOptions(flags=argv)
  my_options = options.view_as(MyOptions)

  with Pipeline(options=options) as pipeline:
    pass  # build your pipeline here.
    # [END pipeline_options_local]

    with TestPipeline() as p:  # Use TestPipeline for testing.
      lines = p | beam.io.ReadFromText(my_options.input)
      lines | beam.io.WriteToText(my_options.output)


def pipeline_options_command_line(argv):
  """Creating a Pipeline by passing a list of arguments."""

  # [START pipeline_options_command_line]
  # Use Python argparse module to parse custom arguments
  import argparse

  import apache_beam as beam

  parser = argparse.ArgumentParser()
  parser.add_argument('--input')
  parser.add_argument('--output')
  args, beam_args = parser.parse_known_args(argv)

  # Create the Pipeline with remaining arguments.
  with beam.Pipeline(argv=beam_args) as pipeline:
    lines = pipeline | 'Read files' >> beam.io.ReadFromText(args.input)
    lines | 'Write files' >> beam.io.WriteToText(args.output)
    # [END pipeline_options_command_line]


def pipeline_logging(lines, output):
  """Logging Pipeline Messages."""

  import re
  import apache_beam as beam

  # [START pipeline_logging]
  # import Python logging module.
  import logging

  class ExtractWordsFn(beam.DoFn):
    def process(self, element):
      words = re.findall(r'[A-Za-z\']+', element)
      for word in words:
        yield word

        if word.lower() == 'love':
          # Log using the root logger at info or higher levels
          logging.info('Found : %s', word.lower())

  # Remaining WordCount example code ...
  # [END pipeline_logging]

  with TestPipeline() as p:  # Use TestPipeline for testing.
    (
        p
        | beam.Create(lines)
        | beam.ParDo(ExtractWordsFn())
        | beam.io.WriteToText(output))


def pipeline_monitoring(renames):
  """Using monitoring interface snippets."""

  import re
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  class WordCountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          '--input',
          help='Input for the pipeline',
          default='gs://my-bucket/input')
      parser.add_argument(
          '--output',
          help='output for the pipeline',
          default='gs://my-bucket/output')

  class ExtractWordsFn(beam.DoFn):
    def process(self, element):
      words = re.findall(r'[A-Za-z\']+', element)
      for word in words:
        yield word

  class FormatCountsFn(beam.DoFn):
    def process(self, element):
      word, count = element
      yield '%s: %s' % (word, count)

  # [START pipeline_monitoring_composite]
  # The CountWords Composite Transform inside the WordCount pipeline.
  class CountWords(beam.PTransform):
    def expand(self, pcoll):
      return (
          pcoll
          # Convert lines of text into individual words.
          | 'ExtractWords' >> beam.ParDo(ExtractWordsFn())
          # Count the number of times each word occurs.
          | beam.combiners.Count.PerElement()
          # Format each word and count into a printable string.
          | 'FormatCounts' >> beam.ParDo(FormatCountsFn()))

  # [END pipeline_monitoring_composite]

  pipeline_options = PipelineOptions()
  options = pipeline_options.view_as(WordCountOptions)
  with TestPipeline() as p:  # Use TestPipeline for testing.

    # [START pipeline_monitoring_execution]
    (
        p
        # Read the lines of the input text.
        | 'ReadLines' >> beam.io.ReadFromText(options.input)
        # Count the words.
        | CountWords()
        # Write the formatted word counts to output.
        | 'WriteCounts' >> beam.io.WriteToText(options.output))
    # [END pipeline_monitoring_execution]

    p.visit(SnippetUtils.RenameFiles(renames))


def examples_wordcount_minimal(renames):
  """MinimalWordCount example snippets."""
  import re

  import apache_beam as beam

  from apache_beam.options.pipeline_options import GoogleCloudOptions
  from apache_beam.options.pipeline_options import StandardOptions
  from apache_beam.options.pipeline_options import PipelineOptions

  # [START examples_wordcount_minimal_options]
  options = PipelineOptions()
  google_cloud_options = options.view_as(GoogleCloudOptions)
  google_cloud_options.project = 'my-project-id'
  google_cloud_options.job_name = 'myjob'
  google_cloud_options.staging_location = 'gs://your-bucket-name-here/staging'
  google_cloud_options.temp_location = 'gs://your-bucket-name-here/temp'
  options.view_as(StandardOptions).runner = 'DataflowRunner'
  # [END examples_wordcount_minimal_options]

  # Run it locally for testing.
  options = PipelineOptions()

  # [START examples_wordcount_minimal_create]
  p = beam.Pipeline(options=options)
  # [END examples_wordcount_minimal_create]

  (
      # [START examples_wordcount_minimal_read]
      p
      | beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
      # [END examples_wordcount_minimal_read]

      # [START examples_wordcount_minimal_pardo]
      | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
      # [END examples_wordcount_minimal_pardo]

      # [START examples_wordcount_minimal_count]
      | beam.combiners.Count.PerElement()
      # [END examples_wordcount_minimal_count]

      # [START examples_wordcount_minimal_map]
      | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
      # [END examples_wordcount_minimal_map]

      # [START examples_wordcount_minimal_write]
      | beam.io.WriteToText('gs://my-bucket/counts.txt')
      # [END examples_wordcount_minimal_write]
  )

  p.visit(SnippetUtils.RenameFiles(renames))

  # [START examples_wordcount_minimal_run]
  result = p.run()
  # [END examples_wordcount_minimal_run]
  result.wait_until_finish()


def examples_wordcount_wordcount(renames):
  """WordCount example snippets."""
  import re

  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  argv = []

  # [START examples_wordcount_wordcount_options]
  class WordCountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          '--input',
          help='Input for the pipeline',
          default='gs://my-bucket/input')

  options = PipelineOptions(argv)
  word_count_options = options.view_as(WordCountOptions)
  with beam.Pipeline(options=options) as p:
    lines = p | beam.io.ReadFromText(word_count_options.input)

    # [END examples_wordcount_wordcount_options]

    # [START examples_wordcount_wordcount_composite]
    class CountWords(beam.PTransform):
      def expand(self, pcoll):
        return (
            pcoll
            # Convert lines of text into individual words.
            | 'ExtractWords' >>
            beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))

            # Count the number of times each word occurs.
            | beam.combiners.Count.PerElement())

    counts = lines | CountWords()

    # [END examples_wordcount_wordcount_composite]

    # [START examples_wordcount_wordcount_dofn]
    class FormatAsTextFn(beam.DoFn):
      def process(self, element):
        word, count = element
        yield '%s: %s' % (word, count)

    formatted = counts | beam.ParDo(FormatAsTextFn())
    # [END examples_wordcount_wordcount_dofn]

    formatted | beam.io.WriteToText('gs://my-bucket/counts.txt')
    p.visit(SnippetUtils.RenameFiles(renames))


def examples_wordcount_templated(renames):
  """Templated WordCount example snippet."""
  import re

  import apache_beam as beam
  from apache_beam.io import ReadFromText
  from apache_beam.io import WriteToText
  from apache_beam.options.pipeline_options import PipelineOptions

  # [START example_wordcount_templated]
  class WordcountTemplatedOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input', help='Path of the file to read from')
      parser.add_argument(
          '--output', required=True, help='Output file to write results to.')

  pipeline_options = PipelineOptions(['--output', 'some/output_path'])
  with beam.Pipeline(options=pipeline_options) as p:

    wordcount_options = pipeline_options.view_as(WordcountTemplatedOptions)
    lines = p | 'Read' >> ReadFromText(wordcount_options.input)

    # [END example_wordcount_templated]

    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    (
        lines
        |
        'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | 'PairWithOnes' >> beam.Map(lambda x: (x, 1))
        | 'Group' >> beam.GroupByKey()
        |
        'Sum' >> beam.Map(lambda word_ones: (word_ones[0], sum(word_ones[1])))
        | 'Format' >> beam.Map(format_result)
        | 'Write' >> WriteToText(wordcount_options.output))

    p.visit(SnippetUtils.RenameFiles(renames))


def examples_wordcount_debugging(renames):
  """DebuggingWordCount example snippets."""
  import re

  import apache_beam as beam

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

  with TestPipeline() as p:  # Use TestPipeline for testing.
    filtered_words = (
        p
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
        | 'Write' >> beam.io.WriteToText('gs://my-bucket/counts.txt'))

    p.visit(SnippetUtils.RenameFiles(renames))


def examples_wordcount_streaming(argv):
  import apache_beam as beam
  from apache_beam import window
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import StandardOptions

  # Parse out arguments.
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=(
          'Input PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(StandardOptions).streaming = True

  with TestPipeline(options=pipeline_options) as p:
    # [START example_wordcount_streaming_read]
    # Read from Pub/Sub into a PCollection.
    if known_args.input_subscription:
      lines = p | beam.io.ReadFromPubSub(
          subscription=known_args.input_subscription)
    else:
      lines = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)
    # [END example_wordcount_streaming_read]

    output = (
        lines
        | 'DecodeUnicode' >> beam.FlatMap(
            lambda encoded:
            (encoded if isinstance(encoded, list) else encoded.decode('utf-8')))
        | 'ExtractWords' >>
        beam.FlatMap(lambda x: __import__('re').findall(r'[A-Za-z\']+', x))
        | 'PairWithOnes' >> beam.Map(lambda x: (x, 1))
        | beam.WindowInto(window.FixedWindows(15, 0))
        | 'Group' >> beam.GroupByKey()
        |
        'Sum' >> beam.Map(lambda word_ones: (word_ones[0], sum(word_ones[1])))
        |
        'Format' >> beam.Map(lambda word_and_count: '%s: %d' % word_and_count))

    # [START example_wordcount_streaming_write]
    # Write to Pub/Sub
    output | beam.io.WriteStringsToPubSub(known_args.output_topic)
    # [END example_wordcount_streaming_write]


def examples_ptransforms_templated(renames):
  # [START examples_ptransforms_templated]
  import apache_beam as beam
  from apache_beam.io import WriteToText
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.value_provider import StaticValueProvider

  class TemplatedUserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--templated_int', type=int)

  class MySumFn(beam.DoFn):
    def __init__(self, templated_int):
      self.templated_int = templated_int

    def process(self, an_int):
      yield self.templated_int.get() + an_int

  pipeline_options = PipelineOptions()
  with beam.Pipeline(options=pipeline_options) as p:

    user_options = pipeline_options.view_as(TemplatedUserOptions)
    my_sum_fn = MySumFn(user_options.templated_int)
    sum = (
        p
        | 'ReadCollection' >>
        beam.io.ReadFromText('gs://some/integer_collection')
        | 'StringToInt' >> beam.Map(lambda w: int(w))
        | 'AddGivenInt' >> beam.ParDo(my_sum_fn)
        | 'WriteResultingCollection' >> WriteToText('some/output_path'))
    # [END examples_ptransforms_templated]

    # Templates are not supported by DirectRunner (only by DataflowRunner)
    # so a value must be provided at graph-construction time
    my_sum_fn.templated_int = StaticValueProvider(int, 10)

    p.visit(SnippetUtils.RenameFiles(renames))


# Defining a new source.
# [START model_custom_source_new_source]
class CountingSource(iobase.BoundedSource):
  def __init__(self, count):
    self.records_read = Metrics.counter(self.__class__, 'recordsRead')
    self._count = count

  def estimate_size(self):
    return self._count

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._count

    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    for i in range(range_tracker.start_position(),
                   range_tracker.stop_position()):
      if not range_tracker.try_claim(i):
        return
      self.records_read.inc()
      yield i

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._count

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
      yield iobase.SourceBundle(
          weight=(bundle_stop - bundle_start),
          source=self,
          start_position=bundle_start,
          stop_position=bundle_stop)
      bundle_start = bundle_stop


# [END model_custom_source_new_source]


# We recommend users to start Source classes with an underscore to discourage
# using the Source class directly when a PTransform for the source is
# available. We simulate that here by simply extending the previous Source
# class.
class _CountingSource(CountingSource):
  pass


# [START model_custom_source_new_ptransform]
class ReadFromCountingSource(PTransform):
  def __init__(self, count):
    super(ReadFromCountingSource, self).__init__()
    self._count = count

  def expand(self, pcoll):
    return pcoll | iobase.Read(_CountingSource(self._count))


# [END model_custom_source_new_ptransform]


def model_custom_source(count):
  """Demonstrates creating a new custom source and using it in a pipeline.

  Defines a new source ``CountingSource`` that produces integers starting from 0
  up to a given size.

  Uses the new source in an example pipeline.

  Additionally demonstrates how a source should be implemented using a
  ``PTransform``. This is the recommended way to develop sources that are to
  distributed to a large number of end users.

  This method runs two pipelines.

  (1) A pipeline that uses ``CountingSource`` directly using the ``df.Read``
      transform.
  (2) A pipeline that uses a custom ``PTransform`` that wraps
      ``CountingSource``.

  Args:
    count: the size of the counting source to be used in the pipeline
           demonstrated in this method.

  """

  # Using the source in an example pipeline.
  # [START model_custom_source_use_new_source]
  with beam.Pipeline(options=PipelineOptions()) as p:
    numbers = p | 'ProduceNumbers' >> beam.io.Read(CountingSource(count))
    # [END model_custom_source_use_new_source]

    lines = numbers | beam.core.Map(lambda number: 'line %d' % number)
    assert_that(
        lines, equal_to(['line ' + str(number) for number in range(0, count)]))

  # [START model_custom_source_use_ptransform]
  with beam.Pipeline(options=PipelineOptions()) as p:
    numbers = p | 'ProduceNumbers' >> ReadFromCountingSource(count)
    # [END model_custom_source_use_ptransform]

    lines = numbers | beam.core.Map(lambda number: 'line %d' % number)
    assert_that(
        lines, equal_to(['line ' + str(number) for number in range(0, count)]))


# Defining the new sink.
#
# Defines a new sink ``SimpleKVSink`` that demonstrates writing to a simple
# key-value based storage system which has following API.
#
#   simplekv.connect(url) -
#       connects to the storage system and returns an access token which can be
#       used to perform further operations
#   simplekv.open_table(access_token, table_name) -
#       creates a table named 'table_name'. Returns a table object.
#   simplekv.write_to_table(access_token, table, key, value) -
#       writes a key-value pair to the given table.
#   simplekv.rename_table(access_token, old_name, new_name) -
#       renames the table named 'old_name' to 'new_name'.
#
# [START model_custom_sink_new_sink]
class SimpleKVSink(iobase.Sink):
  def __init__(self, simplekv, url, final_table_name):
    self._simplekv = simplekv
    self._url = url
    self._final_table_name = final_table_name

  def initialize_write(self):
    access_token = self._simplekv.connect(self._url)
    return access_token

  def open_writer(self, access_token, uid):
    table_name = 'table' + uid
    return SimpleKVWriter(self._simplekv, access_token, table_name)

  def pre_finalize(self, init_result, writer_results):
    pass

  def finalize_write(self, access_token, table_names, pre_finalize_result):
    for i, table_name in enumerate(table_names):
      self._simplekv.rename_table(
          access_token, table_name, self._final_table_name + str(i))


# [END model_custom_sink_new_sink]


# Defining a writer for the new sink.
# [START model_custom_sink_new_writer]
class SimpleKVWriter(iobase.Writer):
  def __init__(self, simplekv, access_token, table_name):
    self._simplekv = simplekv
    self._access_token = access_token
    self._table_name = table_name
    self._table = self._simplekv.open_table(access_token, table_name)

  def write(self, record):
    key, value = record

    self._simplekv.write_to_table(self._access_token, self._table, key, value)

  def close(self):
    return self._table_name


# [END model_custom_sink_new_writer]


# [START model_custom_sink_new_ptransform]
class WriteToKVSink(PTransform):
  def __init__(self, simplekv, url, final_table_name):
    self._simplekv = simplekv
    super(WriteToKVSink, self).__init__()
    self._url = url
    self._final_table_name = final_table_name

  def expand(self, pcoll):
    return pcoll | iobase.Write(
        _SimpleKVSink(self._simplekv, self._url, self._final_table_name))


# [END model_custom_sink_new_ptransform]


# We recommend users to start Sink class names with an underscore to
# discourage using the Sink class directly when a PTransform for the sink is
# available. We simulate that here by simply extending the previous Sink
# class.
class _SimpleKVSink(SimpleKVSink):
  pass


def model_custom_sink(
    simplekv,
    KVs,
    final_table_name_no_ptransform,
    final_table_name_with_ptransform):
  """Demonstrates creating a new custom sink and using it in a pipeline.

  Uses the new sink in an example pipeline.

  Additionally demonstrates how a sink should be implemented using a
  ``PTransform``. This is the recommended way to develop sinks that are to be
  distributed to a large number of end users.

  This method runs two pipelines.

  (1) A pipeline that uses ``SimpleKVSink`` directly using the ``df.Write``
      transform.
  (2) A pipeline that uses a custom ``PTransform`` that wraps
      ``SimpleKVSink``.

  Args:
    simplekv: an object that mocks the key-value storage.

    KVs: the set of key-value pairs to be written in the example pipeline.

    final_table_name_no_ptransform: the prefix of final set of tables to be
                                    created by the example pipeline that uses
                                    ``SimpleKVSink`` directly.

    final_table_name_with_ptransform: the prefix of final set of tables to be
                                      created by the example pipeline that uses
                                      a ``PTransform`` that wraps
                                      ``SimpleKVSink``.
  """

  final_table_name = final_table_name_no_ptransform

  # Using the new sink in an example pipeline.
  # [START model_custom_sink_use_new_sink]
  with beam.Pipeline(options=PipelineOptions()) as p:
    kvs = p | 'CreateKVs' >> beam.Create(KVs)

    kvs | 'WriteToSimpleKV' >> beam.io.Write(
        SimpleKVSink(simplekv, 'http://url_to_simple_kv/', final_table_name))
    # [END model_custom_sink_use_new_sink]

  final_table_name = final_table_name_with_ptransform

  # [START model_custom_sink_use_ptransform]
  with beam.Pipeline(options=PipelineOptions()) as p:
    kvs = p | 'CreateKVs' >> beam.core.Create(KVs)
    kvs | 'WriteToSimpleKV' >> WriteToKVSink(
        simplekv, 'http://url_to_simple_kv/', final_table_name)
    # [END model_custom_sink_use_ptransform]


def model_textio(renames):
  """Using a Read and Write transform to read/write text files."""
  def filter_words(x):
    import re
    return re.findall(r'[A-Za-z\']+', x)

  # [START model_textio_read]
  with beam.Pipeline(options=PipelineOptions()) as p:
    # [START model_pipelineio_read]
    lines = p | 'ReadFromText' >> beam.io.ReadFromText('path/to/input-*.csv')
    # [END model_pipelineio_read]
    # [END model_textio_read]

    # [START model_textio_write]
    filtered_words = lines | 'FilterWords' >> beam.FlatMap(filter_words)
    # [START model_pipelineio_write]
    filtered_words | 'WriteToText' >> beam.io.WriteToText(
        '/path/to/numbers', file_name_suffix='.csv')
    # [END model_pipelineio_write]
    # [END model_textio_write]

    p.visit(SnippetUtils.RenameFiles(renames))


def model_textio_compressed(renames, expected):
  """Using a Read Transform to read compressed text files."""
  with TestPipeline() as p:

    # [START model_textio_write_compressed]
    lines = p | 'ReadFromText' >> beam.io.ReadFromText(
        '/path/to/input-*.csv.gz',
        compression_type=beam.io.filesystem.CompressionTypes.GZIP)
    # [END model_textio_write_compressed]

    assert_that(lines, equal_to(expected))
    p.visit(SnippetUtils.RenameFiles(renames))


def model_datastoreio():
  """Using a Read and Write transform to read/write to Cloud Datastore."""

  import uuid
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
  from apache_beam.io.gcp.datastore.v1new.types import Entity
  from apache_beam.io.gcp.datastore.v1new.types import Key
  from apache_beam.io.gcp.datastore.v1new.types import Query

  project = 'my_project'
  kind = 'my_kind'
  query = Query(kind, project)

  # [START model_datastoreio_read]
  p = beam.Pipeline(options=PipelineOptions())
  entities = p | 'Read From Datastore' >> ReadFromDatastore(query)
  # [END model_datastoreio_read]

  # [START model_datastoreio_write]
  p = beam.Pipeline(options=PipelineOptions())
  musicians = p | 'Musicians' >> beam.Create(
      ['Mozart', 'Chopin', 'Beethoven', 'Vivaldi'])

  def to_entity(content):
    key = Key([kind, str(uuid.uuid4())])
    entity = Entity(key)
    entity.set_properties({'content': content})
    return entity

  entities = musicians | 'To Entity' >> beam.Map(to_entity)
  entities | 'Write To Datastore' >> WriteToDatastore(project)
  # [END model_datastoreio_write]


def model_bigqueryio(p, write_project='', write_dataset='', write_table=''):
  """Using a Read and Write transform to read/write from/to BigQuery."""

  # [START model_bigqueryio_table_spec]
  # project-id:dataset_id.table_id
  table_spec = 'clouddataflow-readonly:samples.weather_stations'
  # [END model_bigqueryio_table_spec]

  # [START model_bigqueryio_table_spec_without_project]
  # dataset_id.table_id
  table_spec = 'samples.weather_stations'
  # [END model_bigqueryio_table_spec_without_project]

  # [START model_bigqueryio_table_spec_object]
  from apache_beam.io.gcp.internal.clients import bigquery

  table_spec = bigquery.TableReference(
      projectId='clouddataflow-readonly',
      datasetId='samples',
      tableId='weather_stations')
  # [END model_bigqueryio_table_spec_object]

  # [START model_bigqueryio_data_types]
  bigquery_data = [{
      'string': 'abc',
      'bytes': base64.b64encode(b'\xab\xac'),
      'integer': 5,
      'float': 0.5,
      'numeric': Decimal('5'),
      'boolean': True,
      'timestamp': '2018-12-31 12:44:31.744957 UTC',
      'date': '2018-12-31',
      'time': '12:44:31',
      'datetime': '2018-12-31T12:44:31',
      'geography': 'POINT(30 10)'
  }]
  # [END model_bigqueryio_data_types]

  # [START model_bigqueryio_read_table]
  max_temperatures = (
      p
      | 'ReadTable' >> beam.io.Read(beam.io.BigQuerySource(table_spec))
      # Each row is a dictionary where the keys are the BigQuery columns
      | beam.Map(lambda elem: elem['max_temperature']))
  # [END model_bigqueryio_read_table]

  # [START model_bigqueryio_read_query]
  max_temperatures = (
      p
      | 'QueryTable' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT max_temperature FROM '\
                '[clouddataflow-readonly:samples.weather_stations]'))
      # Each row is a dictionary where the keys are the BigQuery columns
      | beam.Map(lambda elem: elem['max_temperature']))
  # [END model_bigqueryio_read_query]

  # [START model_bigqueryio_read_query_std_sql]
  max_temperatures = (
      p
      | 'QueryTableStdSQL' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT max_temperature FROM '\
                '`clouddataflow-readonly.samples.weather_stations`',
          use_standard_sql=True))
      # Each row is a dictionary where the keys are the BigQuery columns
      | beam.Map(lambda elem: elem['max_temperature']))
  # [END model_bigqueryio_read_query_std_sql]

  # [START model_bigqueryio_schema]
  # column_name:BIGQUERY_TYPE, ...
  table_schema = 'source:STRING, quote:STRING'
  # [END model_bigqueryio_schema]

  # [START model_bigqueryio_schema_object]
  table_schema = {
      'fields': [{
          'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'
      }, {
          'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'
      }]
  }
  # [END model_bigqueryio_schema_object]

  if write_project and write_dataset and write_table:
    table_spec = '{}:{}.{}'.format(write_project, write_dataset, write_table)

  # [START model_bigqueryio_write_input]
  quotes = p | beam.Create([
      {
          'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
      },
      {
          'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."
      },
  ])
  # [END model_bigqueryio_write_input]

  # [START model_bigqueryio_write]
  quotes | beam.io.WriteToBigQuery(
      table_spec,
      schema=table_schema,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  # [END model_bigqueryio_write]


def model_composite_transform_example(contents, output_path):
  """Example of a composite transform.

  To declare a composite transform, define a subclass of PTransform.

  To override the apply method, define a method "apply" that
  takes a PCollection as its only parameter and returns a PCollection.
  """
  import re

  import apache_beam as beam

  # [START composite_transform_example]
  # [START composite_ptransform_apply_method]
  # [START composite_ptransform_declare]
  class CountWords(beam.PTransform):
    # [END composite_ptransform_declare]

    def expand(self, pcoll):
      return (
          pcoll
          | beam.FlatMap(lambda x: re.findall(r'\w+', x))
          | beam.combiners.Count.PerElement()
          | beam.Map(lambda word_c: '%s: %s' % (word_c[0], word_c[1])))

  # [END composite_ptransform_apply_method]
  # [END composite_transform_example]

  with TestPipeline() as p:  # Use TestPipeline for testing.
    (
        p
        | beam.Create(contents)
        | CountWords()
        | beam.io.WriteToText(output_path))


def model_multiple_pcollections_flatten(contents, output_path):
  """Merging a PCollection with Flatten."""
  some_hash_fn = lambda s: ord(s[0])
  partition_fn = lambda element, partitions: some_hash_fn(element) % partitions
  import apache_beam as beam
  with TestPipeline() as p:  # Use TestPipeline for testing.

    # Partition into deciles
    partitioned = p | beam.Create(contents) | beam.Partition(partition_fn, 3)
    pcoll1 = partitioned[0]
    pcoll2 = partitioned[1]
    pcoll3 = partitioned[2]

    # Flatten them back into 1

    # A collection of PCollection objects can be represented simply
    # as a tuple (or list) of PCollections.
    # (The SDK for Python has no separate type to store multiple
    # PCollection objects, whether containing the same or different
    # types.)
    # [START model_multiple_pcollections_flatten]
    merged = (
        (pcoll1, pcoll2, pcoll3)
        # A list of tuples can be "piped" directly into a Flatten transform.
        | beam.Flatten())
    # [END model_multiple_pcollections_flatten]
    merged | beam.io.WriteToText(output_path)


def model_multiple_pcollections_partition(contents, output_path):
  """Splitting a PCollection with Partition."""
  some_hash_fn = lambda s: ord(s[0])

  def get_percentile(i):
    """Assume i in [0,100)."""
    return i

  import apache_beam as beam
  with TestPipeline() as p:  # Use TestPipeline for testing.

    students = p | beam.Create(contents)

    # [START model_multiple_pcollections_partition]
    def partition_fn(student, num_partitions):
      return int(get_percentile(student) * num_partitions / 100)

    by_decile = students | beam.Partition(partition_fn, 10)
    # [END model_multiple_pcollections_partition]
    # [START model_multiple_pcollections_partition_40th]
    fortieth_percentile = by_decile[4]
    # [END model_multiple_pcollections_partition_40th]

    ([by_decile[d] for d in range(10) if d != 4] + [fortieth_percentile]
     | beam.Flatten()
     | beam.io.WriteToText(output_path))


def model_group_by_key(contents, output_path):
  """Applying a GroupByKey Transform."""
  import re

  import apache_beam as beam
  with TestPipeline() as p:  # Use TestPipeline for testing.

    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    words_and_counts = (
        p
        | beam.Create(contents)
        | beam.FlatMap(lambda x: re.findall(r'\w+', x))
        | 'one word' >> beam.Map(lambda w: (w, 1)))
    # GroupByKey accepts a PCollection of (w, 1) and
    # outputs a PCollection of (w, (1, 1, ...)).
    # (A key/value pair is just a tuple in Python.)
    # This is a somewhat forced example, since one could
    # simply use beam.combiners.Count.PerElement here.
    # [START model_group_by_key_transform]
    grouped_words = words_and_counts | beam.GroupByKey()
    # [END model_group_by_key_transform]
    (
        grouped_words
        | 'count words' >> beam.Map(count_ones)
        | beam.io.WriteToText(output_path))


def model_co_group_by_key_tuple(emails, phones, output_path):
  """Applying a CoGroupByKey Transform to a tuple."""
  import apache_beam as beam
  # [START model_group_by_key_cogroupbykey_tuple]
  # The result PCollection contains one key-value element for each key in the
  # input PCollections. The key of the pair will be the key from the input and
  # the value will be a dictionary with two entries: 'emails' - an iterable of
  # all values for the current key in the emails PCollection and 'phones': an
  # iterable of all values for the current key in the phones PCollection.
  results = ({'emails': emails, 'phones': phones} | beam.CoGroupByKey())

  def join_info(name_info):
    (name, info) = name_info
    return '%s; %s; %s' %\
        (name, sorted(info['emails']), sorted(info['phones']))

  contact_lines = results | beam.Map(join_info)
  # [END model_group_by_key_cogroupbykey_tuple]
  contact_lines | beam.io.WriteToText(output_path)


def model_join_using_side_inputs(
    name_list, email_list, phone_list, output_path):
  """Joining PCollections using side inputs."""

  import apache_beam as beam
  from apache_beam.pvalue import AsIter

  with TestPipeline() as p:  # Use TestPipeline for testing.
    # [START model_join_using_side_inputs]
    # This code performs a join by receiving the set of names as an input and
    # passing PCollections that contain emails and phone numbers as side inputs
    # instead of using CoGroupByKey.
    names = p | 'names' >> beam.Create(name_list)
    emails = p | 'email' >> beam.Create(email_list)
    phones = p | 'phone' >> beam.Create(phone_list)

    def join_info(name, emails, phone_numbers):
      filtered_emails = []
      for name_in_list, email in emails:
        if name_in_list == name:
          filtered_emails.append(email)

      filtered_phone_numbers = []
      for name_in_list, phone_number in phone_numbers:
        if name_in_list == name:
          filtered_phone_numbers.append(phone_number)

      return '; '.join([
          '%s' % name,
          '%s' % ','.join(filtered_emails),
          '%s' % ','.join(filtered_phone_numbers)
      ])

    contact_lines = names | 'CreateContacts' >> beam.core.Map(
        join_info, AsIter(emails), AsIter(phones))
    # [END model_join_using_side_inputs]
    contact_lines | beam.io.WriteToText(output_path)


# [START model_library_transforms_keys]
class Keys(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | 'Keys' >> beam.Map(lambda k_v: k_v[0])


# [END model_library_transforms_keys]
# pylint: enable=invalid-name


# [START model_library_transforms_count]
class Count(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll
        | 'PairWithOne' >> beam.Map(lambda v: (v, 1))
        | beam.CombinePerKey(sum))


# [END model_library_transforms_count]


def file_process_pattern_access_metadata():

  import apache_beam as beam
  from apache_beam.io import fileio

  # [START FileProcessPatternAccessMetadataSnip1]
  with beam.Pipeline() as p:
    readable_files = (
        p
        | fileio.MatchFiles('hdfs://path/to/*.txt')
        | fileio.ReadMatches()
        | beam.Reshuffle())
    files_and_contents = (
        readable_files
        | beam.Map(lambda x: (x.metadata.path, x.read_utf8())))
  # [END FileProcessPatternAccessMetadataSnip1]


def accessing_valueprovider_info_after_run():
  # [START AccessingValueProviderInfoAfterRunSnip1]
  import logging

  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.utils.value_provider import RuntimeValueProvider

  class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--string_value', type=str)

  class LogValueProvidersFn(beam.DoFn):
    def __init__(self, string_vp):
      self.string_vp = string_vp

    # Define the DoFn that logs the ValueProvider value.
    # The DoFn is called when creating the pipeline branch.
    # This example logs the ValueProvider value, but
    # you could store it by pushing it to an external database.
    def process(self, an_int):
      logging.info('The string_value is %s' % self.string_vp.get())
      # Another option (where you don't need to pass the value at all) is:
      logging.info(
          'The string value is %s' %
          RuntimeValueProvider.get_value('string_value', str, ''))

  pipeline_options = PipelineOptions()
  # Create pipeline.
  with beam.Pipeline(options=pipeline_options) as p:

    my_options = pipeline_options.view_as(MyOptions)
    # Add a branch for logging the ValueProvider value.
    _ = (
        p
        | beam.Create([None])
        | 'LogValueProvs' >> beam.ParDo(
            LogValueProvidersFn(my_options.string_value)))

    # The main pipeline.
    result_pc = (
        p
        | "main_pc" >> beam.Create([1, 2, 3])
        | beam.combiners.Sum.Globally())

  # [END AccessingValueProviderInfoAfterRunSnip1]


def side_input_slow_update(
    src_file_pattern,
    first_timestamp,
    last_timestamp,
    interval,
    sample_main_input_elements,
    main_input_windowing_interval):
  # [START SideInputSlowUpdateSnip1]
  from apache_beam.transforms.periodicsequence import PeriodicImpulse
  from apache_beam.transforms.window import TimestampedValue
  from apache_beam.transforms import window

  # from apache_beam.utils.timestamp import MAX_TIMESTAMP
  # last_timestamp = MAX_TIMESTAMP to go on indefninitely

  # Any user-defined function.
  # cross join is used as an example.
  def cross_join(left, rights):
    for x in rights:
      yield (left, x)

  # Create pipeline.
  pipeline_options = PipelineOptions()
  p = beam.Pipeline(options=pipeline_options)
  side_input = (
      p
      | 'PeriodicImpulse' >> PeriodicImpulse(
          first_timestamp, last_timestamp, interval, True)
      | 'MapToFileName' >> beam.Map(lambda x: src_file_pattern + str(x))
      | 'ReadFromFile' >> beam.io.ReadAllFromText())

  main_input = (
      p
      | 'MpImpulse' >> beam.Create(sample_main_input_elements)
      |
      'MapMpToTimestamped' >> beam.Map(lambda src: TimestampedValue(src, src))
      | 'WindowMpInto' >> beam.WindowInto(
          window.FixedWindows(main_input_windowing_interval)))

  result = (
      main_input
      | 'ApplyCrossJoin' >> beam.FlatMap(
          cross_join, rights=beam.pvalue.AsIter(side_input)))
  # [END SideInputSlowUpdateSnip1]

  return p, result


def bigqueryio_deadletter():
  # [START BigQueryIODeadLetter]

  # Create pipeline.
  schema = ({'fields': [{'name': 'a', 'type': 'STRING', 'mode': 'REQUIRED'}]})

  p = beam.Pipeline()

  errors = (
      p | 'Data' >> beam.Create([1, 2])
      | 'CreateBrokenData' >>
      beam.Map(lambda src: {'a': src} if src == 2 else {'a': None})
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          "<Your Project:Test.dummy_a_table",
          schema=schema,
          insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
          create_disposition='CREATE_IF_NEEDED',
          write_disposition='WRITE_APPEND'))
  result = (
      errors['FailedRows']
      | 'PrintErrors' >>
      beam.FlatMap(lambda err: print("Error Found {}".format(err))))
  # [END BigQueryIODeadLetter]

  return result


def extract_sentiments(response):
  # [START nlp_extract_sentiments]
  return {
      'sentences': [{
          sentence.text.content: sentence.sentiment.score
      } for sentence in response.sentences],
      'document_sentiment': response.document_sentiment.score,
  }
  # [END nlp_extract_sentiments]


def extract_entities(response):
  # [START nlp_extract_entities]
  return [{
      'name': entity.name,
      'type': nlp.enums.Entity.Type(entity.type).name,
  } for entity in response.entities]
  # [END nlp_extract_entities]


def analyze_dependency_tree(response):
  # [START analyze_dependency_tree]
  from collections import defaultdict
  adjacency_lists = []

  index = 0
  for sentence in response.sentences:
    adjacency_list = defaultdict(list)
    sentence_begin = sentence.text.begin_offset
    sentence_end = sentence_begin + len(sentence.text.content) - 1

    while index < len(response.tokens) and \
        response.tokens[index].text.begin_offset <= sentence_end:
      token = response.tokens[index]
      head_token_index = token.dependency_edge.head_token_index
      head_token_text = response.tokens[head_token_index].text.content
      adjacency_list[head_token_text].append(token.text.content)
      index += 1
    adjacency_lists.append(adjacency_list)
  # [END analyze_dependency_tree]

  return adjacency_lists


def nlp_analyze_text():
  # [START nlp_analyze_text]
  features = nlp.types.AnnotateTextRequest.Features(
      extract_entities=True,
      extract_document_sentiment=True,
      extract_entity_sentiment=True,
      extract_syntax=True,
  )

  with beam.Pipeline() as p:
    responses = (
        p
        | beam.Create([
            'My experience so far has been fantastic! '
            'I\'d really recommend this product.'
        ])
        | beam.Map(lambda x: nlp.Document(x, type='PLAIN_TEXT'))
        | nlp.AnnotateText(features))

    _ = (
        responses
        | beam.Map(extract_sentiments)
        | 'Parse sentiments to JSON' >> beam.Map(json.dumps)
        | 'Write sentiments' >> beam.io.WriteToText('sentiments.txt'))

    _ = (
        responses
        | beam.Map(extract_entities)
        | 'Parse entities to JSON' >> beam.Map(json.dumps)
        | 'Write entities' >> beam.io.WriteToText('entities.txt'))

    _ = (
        responses
        | beam.Map(analyze_dependency_tree)
        | 'Parse adjacency list to JSON' >> beam.Map(json.dumps)
        | 'Write adjacency list' >> beam.io.WriteToText('adjancency_list.txt'))
  # [END nlp_analyze_text]
