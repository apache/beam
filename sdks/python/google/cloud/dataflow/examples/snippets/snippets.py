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

"""Code snippets used in Cloud Dataflow webdocs.

The examples here are written specifically to read well with the accompanying
web docs from https://cloud.google.com/dataflow. Do not rewrite them until you
make sure the webdocs still read well and the rewritten code supports the
concept being described. For example, there are snippets that could be shorter
but they are written like this to make a specific point in the docs.

The code snippets are all organized as self contained functions. Parts of the
function body delimited by [START tag] and [END tag] will be included
automatically in the web docs. The naming convention for the tags is to have as
prefix the PATH_TO_HTML where they are included followed by a descriptive
string. For instance a code snippet that will be used as a code example
at https://cloud.google.com/dataflow/model/pipelines will have the tag
model_pipelines_DESCRIPTION. The tags can contain only letters, digits and _.
"""

import google.cloud.dataflow as df

# Quiet some pylint warnings that happen because of the somewhat special
# format for the code snippets.
# pylint:disable=invalid-name
# pylint:disable=expression-not-assigned
# pylint:disable=redefined-outer-name
# pylint:disable=unused-variable
# pylint:disable=g-doc-args
# pylint:disable=g-import-not-at-top


class SnippetUtils(object):
  from google.cloud.dataflow.pipeline import PipelineVisitor

  class RenameFiles(PipelineVisitor):
    """RenameFiles will rewire source and sink for unit testing.

    RenameFiles will rewire the GCS files specified in the source and
    sink in the snippet pipeline to local files so the pipeline can be run as a
    unit test. This is as close as we can get to have code snippets that are
    executed and are also ready to presented in webdocs.
    """

    def __init__(self, renames):
      self.renames = renames

    def visit_transform(self, transform_node):
      if hasattr(transform_node.transform, 'source'):
        source = transform_node.transform.source
        source.file_path = self.renames['read']
        source.is_gcs_source = False
      elif hasattr(transform_node.transform, 'sink'):
        sink = transform_node.transform.sink
        sink.file_path = self.renames['write']
        sink.is_gcs_sink = False


def construct_pipeline(renames):
  """A reverse words snippet as an example for constructing a pipeline.

  URL: https://cloud.google.com/dataflow/pipelines/constructing-your-pipeline
  """
  import re

  class ReverseWords(df.PTransform):
    """A PTransform that reverses individual elements in a PCollection."""

    def apply(self, pcoll):
      return pcoll | df.Map(lambda e: e[::-1])

  def filter_words(unused_x):
    """Pass through filter to select everything."""
    return True

  # [START pipelines_constructing_creating]
  from google.cloud.dataflow.utils.options import PipelineOptions

  p = df.Pipeline(options=PipelineOptions())
  # [END pipelines_constructing_creating]

  # [START pipelines_constructing_reading]
  lines = p | df.io.Read('ReadMyFile',
                      df.io.TextFileSource('gs://some/inputData.txt'))
  # [END pipelines_constructing_reading]

  # [START pipelines_constructing_applying]
  words = lines | df.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
  reversed_words = words | ReverseWords()
  # [END pipelines_constructing_applying]

  # [START pipelines_constructing_writing]
  filtered_words = reversed_words | df.Filter('FilterWords', filter_words)
  filtered_words | df.io.Write('WriteMyFile',
                               df.io.TextFileSink('gs://some/outputData.txt'))
  # [END pipelines_constructing_writing]

  p.visit(SnippetUtils.RenameFiles(renames))

  # [START pipelines_constructing_running]
  p.run()
  # [END pipelines_constructing_running]


def model_pipelines(argv):
  """A wordcount snippet as a simple pipeline example.

  URL: https://cloud.google.com/dataflow/model/pipelines
  """
  # [START model_pipelines]
  import re

  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions

  class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--input',
                          dest='input',
                          default='gs://dataflow-samples/shakespeare/kinglear'
                          '.txt',
                          help='Input file to process.')
      parser.add_argument('--output',
                          dest='output',
                          required=True,
                          help='Output file to write results to.')

  pipeline_options = PipelineOptions(argv)
  my_options = pipeline_options.view_as(MyOptions)

  p = df.Pipeline(options=pipeline_options)

  (p
   | df.io.Read(df.io.TextFileSource(my_options.input))
   | df.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
   | df.Map(lambda x: (x, 1)) | df.combiners.Count.PerKey()
   | df.io.Write(df.io.TextFileSink(my_options.output)))

  p.run()
  # [END model_pipelines]


def model_pcollection(argv):
  """Creating a PCollection from data in local memory.

  URL: https://cloud.google.com/dataflow/model/pcollection
  """
  from google.cloud.dataflow.utils.options import PipelineOptions

  class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--output',
                          dest='output',
                          required=True,
                          help='Output file to write results to.')

  pipeline_options = PipelineOptions(argv)
  my_options = pipeline_options.view_as(MyOptions)

  # [START model_pcollection]
  p = df.Pipeline(options=pipeline_options)

  (p
   | df.Create([
       'To be, or not to be: that is the question: ',
       'Whether \'tis nobler in the mind to suffer ',
       'The slings and arrows of outrageous fortune, ',
       'Or to take arms against a sea of troubles, '])
   | df.io.Write(df.io.TextFileSink(my_options.output)))

  p.run()
  # [END model_pcollection]


def pipeline_options_remote(argv):
  """"Creating a Pipeline using a PipelineOptions object for remote execution.

  URL: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
  """

  from google.cloud.dataflow import Pipeline
  from google.cloud.dataflow.utils.options import PipelineOptions

  # [START pipeline_options_create]
  options = PipelineOptions(flags=argv)
  # [END pipeline_options_create]

  # [START pipeline_options_define_custom]
  class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--input')
      parser.add_argument('--output')
  # [END pipeline_options_define_custom]

  from google.cloud.dataflow.utils.options import GoogleCloudOptions
  from google.cloud.dataflow.utils.options import StandardOptions

  # [START pipeline_options_dataflow_service]
  # Create and set your PipelineOptions.
  options = PipelineOptions(flags=argv)

  # For Cloud execution, set the Cloud Platform project, staging location,
  # and specify DataflowPipelineRunner or BlockingDataflowPipelineRunner.
  google_cloud_options = options.view_as(GoogleCloudOptions)
  google_cloud_options.project = 'my-project-id'
  google_cloud_options.staging_location = 'gs://my-bucket/binaries'
  options.view_as(StandardOptions).runner = 'DataflowPipelineRunner'

  # Create the Pipeline with the specified options.
  p = Pipeline(options=options)
  # [END pipeline_options_dataflow_service]

  my_options = options.view_as(MyOptions)
  my_input = my_options.input
  my_output = my_options.output

  # Overriding the runner for tests.
  options.view_as(StandardOptions).runner = 'DirectPipelineRunner'
  p = Pipeline(options=options)

  lines = p | df.io.Read('ReadFromText', df.io.TextFileSource(my_input))
  lines | df.io.Write('WriteToText', df.io.TextFileSink(my_output))

  p.run()


def pipeline_options_local(argv):
  """"Creating a Pipeline using a PipelineOptions object for local execution.

  URL: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
  """

  from google.cloud.dataflow import Pipeline
  from google.cloud.dataflow.utils.options import PipelineOptions

  options = PipelineOptions(flags=argv)

  # [START pipeline_options_define_custom_with_help_and_default]
  class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--input',
                          help='Input for the dataflow pipeline',
                          default='gs://my-bucket/input')
      parser.add_argument('--output',
                          help='Output for the dataflow pipeline',
                          default='gs://my-bucket/output')
  # [END pipeline_options_define_custom_with_help_and_default]

  my_options = options.view_as(MyOptions)

  my_input = my_options.input
  my_output = my_options.output

  # [START pipeline_options_local]
  # Create and set your Pipeline Options.
  options = PipelineOptions()
  p = Pipeline(options=options)
  # [END pipeline_options_local]

  lines = p | df.io.Read('ReadFromText', df.io.TextFileSource(my_input))
  lines | df.io.Write('WriteToText', df.io.TextFileSink(my_output))
  p.run()


def pipeline_options_command_line(argv):
  """"Creating a Pipeline by passing a list of arguments.

  URL: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
  """

  # [START pipeline_options_command_line]
  # Use Python argparse module to parse custom arguments
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--input')
  parser.add_argument('--output')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # Create the Pipeline with remaining arguments.
  p = df.Pipeline(argv=pipeline_args)
  lines = p | df.io.Read('ReadFromText', df.io.TextFileSource(known_args.input))
  lines | df.io.Write('WriteToText', df.io.TextFileSink(known_args.output))
  # [END pipeline_options_command_line]

  p.run()


def model_textio(renames):
  """Using a Read and Write transform to read/write text files.

  URLs:
    https://cloud.google.com/dataflow/model/pipeline-io
    https://cloud.google.com/dataflow/model/text-io
  """
  def filter_words(x):
    import re
    return re.findall(r'[A-Za-z\']+', x)

  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions

  # [START model_textio_read]
  p = df.Pipeline(options=PipelineOptions())
  # [START model_pipelineio_read]
  lines = p | df.io.Read(
      'ReadFromText',
      df.io.TextFileSource('gs://my_bucket/path/to/input-*.csv'))
  # [END model_pipelineio_read]
  # [END model_textio_read]

  # [START model_textio_write]
  filtered_words = lines | df.FlatMap('FilterWords', filter_words)
  # [START model_pipelineio_write]
  filtered_words | df.io.Write(
      'WriteToText', df.io.TextFileSink('gs://my_bucket/path/to/numbers',
                                        file_name_suffix='.csv'))
  # [END model_pipelineio_write]
  # [END model_textio_write]

  p.visit(SnippetUtils.RenameFiles(renames))
  p.run()


def model_bigqueryio():
  """Using a Read and Write transform to read/write to BigQuery.

  URL: https://cloud.google.com/dataflow/model/bigquery-io
  """
  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions

  # [START model_bigqueryio_read]
  p = df.Pipeline(options=PipelineOptions())
  weather_data = p | df.io.Read(
      'ReadWeatherStations',
      df.io.BigQuerySource(
          'clouddataflow-readonly:samples.weather_stations'))
  # [END model_bigqueryio_read]

  # [START model_bigqueryio_query]
  p = df.Pipeline(options=PipelineOptions())
  weather_data = p | df.io.Read(
      'ReadYearAndTemp',
      df.io.BigQuerySource(
          query='SELECT year, mean_temp FROM samples.weather_stations'))
  # [END model_bigqueryio_query]

  # [START model_bigqueryio_schema]
  schema = 'source:STRING, quote:STRING'
  # [END model_bigqueryio_schema]

  # [START model_bigqueryio_write]
  quotes = p | df.Create(
      [{'source': 'Mahatma Ghandi', 'quote': 'My life is my message.'}])
  quotes | df.io.Write(
      'Write', df.io.BigQuerySink(
          'my-project:output.output_table',
          schema=schema,
          write_disposition=df.io.BigQueryDisposition.WRITE_TRUNCATE,
          create_disposition=df.io.BigQueryDisposition.CREATE_IF_NEEDED))
  # [END model_bigqueryio_write]


def model_composite_transform_example(contents, output_path):
  """Example of a composite transform.

  To declare a composite transform, define a subclass of PTransform.

  To override the apply method, define a method "apply" that
  takes a PCollection as its only parameter and returns a PCollection.

  URL: https://cloud.google.com/dataflow/model/composite-transforms
  """
  import re

  import google.cloud.dataflow as df

  # [START composite_transform_example]
  # [START composite_ptransform_apply_method]
  # [START composite_ptransform_declare]
  class CountWords(df.PTransform):
    # [END composite_ptransform_declare]

    def apply(self, pcoll):
      return (pcoll
              | df.FlatMap(lambda x: re.findall(r'\w+', x))
              | df.combiners.Count.PerElement()
              | df.Map(lambda (word, c): '%s: %s' % (word, c)))
  # [END composite_ptransform_apply_method]
  # [END composite_transform_example]

  from google.cloud.dataflow.utils.options import PipelineOptions
  p = df.Pipeline(options=PipelineOptions())
  (p
   | df.Create(contents)
   | CountWords()
   | df.io.Write(df.io.TextFileSink(output_path)))
  p.run()


def model_multiple_pcollections_flatten(contents, output_path):
  """Merging a PCollection with Flatten.

  URL: https://cloud.google.com/dataflow/model/multiple-pcollections
  """
  some_hash_fn = lambda s: ord(s[0])
  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions
  p = df.Pipeline(options=PipelineOptions())
  partition_fn = lambda element, partitions: some_hash_fn(element) % partitions

  # Partition into deciles
  partitioned = p | df.Create(contents) | df.Partition(partition_fn, 3)
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
      # [START model_multiple_pcollections_tuple]
      (pcoll1, pcoll2, pcoll3)
      # [END model_multiple_pcollections_tuple]
      # A list of tuples can be "piped" directly into a Flatten transform.
      | df.Flatten())
  # [END model_multiple_pcollections_flatten]
  merged | df.io.Write(df.io.TextFileSink(output_path))

  p.run()


def model_multiple_pcollections_partition(contents, output_path):
  """Splitting a PCollection with Partition.

  URL: https://cloud.google.com/dataflow/model/multiple-pcollections
  """
  some_hash_fn = lambda s: ord(s[0])
  def get_percentile(i):
    """Assume i in [0,100)."""
    return i
  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions
  p = df.Pipeline(options=PipelineOptions())

  students = p | df.Create(contents)
  # [START model_multiple_pcollections_partition]
  def partition_fn(student, num_partitions):
    return int(get_percentile(student) * num_partitions / 100)

  by_decile = students | df.Partition(partition_fn, 10)
  # [END model_multiple_pcollections_partition]
  # [START model_multiple_pcollections_partition_40th]
  fortieth_percentile = by_decile[4]
  # [END model_multiple_pcollections_partition_40th]

  ([by_decile[d] for d in xrange(10) if d != 4] + [fortieth_percentile]
   | df.Flatten()
   | df.io.Write(df.io.TextFileSink(output_path)))

  p.run()


def model_group_by_key(contents, output_path):
  """Applying a GroupByKey Transform.

  URL: https://cloud.google.com/dataflow/model/group-by-key
  """
  import re

  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions
  p = df.Pipeline(options=PipelineOptions())
  words_and_counts = (
      p
      | df.Create(contents)
      | df.FlatMap(lambda x: re.findall(r'\w+', x))
      | df.Map('one word', lambda w: (w, 1)))
  # GroupByKey accepts a PCollection of (w, 1) and
  # outputs a PCollection of (w, (1, 1, ...)).
  # (A key/value pair is just a tuple in Python.)
  # This is a somewhat forced example, since one could
  # simply use df.combiners.Count.PerElement here.
  # [START model_group_by_key_transform]
  grouped_words = words_and_counts | df.GroupByKey()
  # [END model_group_by_key_transform]
  (grouped_words
   | df.Map('count words', lambda (word, counts): (word, len(counts)))
   | df.io.Write(df.io.TextFileSink(output_path)))
  p.run()


def model_co_group_by_key_tuple(email_list, phone_list, output_path):
  """Applying a CoGroupByKey Transform to a tuple.

  URL: https://cloud.google.com/dataflow/model/group-by-key
  """
  import google.cloud.dataflow as df
  from google.cloud.dataflow.utils.options import PipelineOptions
  p = df.Pipeline(options=PipelineOptions())
  # [START model_group_by_key_cogroupbykey_tuple]
  # Each data set is represented by key-value pairs in separate PCollections.
  # Both data sets share a common key type (in this example str).
  # The email_list contains values such as: ('joe', 'joe@example.com') with
  # multiple possible values for each key.
  # The phone_list contains values such as: ('mary': '111-222-3333') with
  # multiple possible values for each key.
  emails = p | df.Create('email', email_list)
  phones = p | df.Create('phone', phone_list)
  # The result PCollection contains one key-value element for each key in the
  # input PCollections. The key of the pair will be the key from the input and
  # the value will be a dictionary with two entries: 'emails' - an iterable of
  # all values for the current key in the emails PCollection and 'phones': an
  # iterable of all values for the current key in the phones PCollection.
  # For instance, if 'emails' contained ('joe', 'joe@example.com') and
  # ('joe', 'joe@gmail.com'), then 'result' will contain the element
  # ('joe', {'emails': ['joe@example.com', 'joe@gmail.com'], 'phones': ...})
  result = {'emails': emails, 'phones': phones} | df.CoGroupByKey()

  def join_info((name, info)):
    return '; '.join(['%s' % name,
                      '%s' % ','.join(info['emails']),
                      '%s' % ','.join(info['phones'])])

  contact_lines = result | df.Map(join_info)
  # [END model_group_by_key_cogroupbykey_tuple]
  contact_lines | df.io.Write(df.io.TextFileSink(output_path))
  p.run()


# [START model_library_transforms_keys]
class Keys(df.PTransform):

  def apply(self, pcoll):
    return pcoll | df.Map('Keys', lambda (k, v): k)
# [END model_library_transforms_keys]
# pylint: enable=invalid-name


# [START model_library_transforms_count]
class Count(df.PTransform):

  def apply(self, pcoll):
    return (
        pcoll
        | df.Map('Init', lambda v: (v, 1))
        | df.CombinePerKey(sum))
# [END model_library_transforms_count]
# pylint: enable=g-wrong-blank-lines
