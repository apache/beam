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
#   name: WordCountSnippet
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output-path output.txt
#   context_line: 50
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings


def examples_wordcount_wordcount():
  """WordCount example snippets."""
  import re

  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # [START examples_wordcount_wordcount_options]
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input-file',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='The file path for the input text to process.')
  parser.add_argument(
      '--output-path', required=True, help='The path prefix for output files.')
  args, beam_args = parser.parse_known_args()

  beam_options = PipelineOptions(beam_args)
  with beam.Pipeline(options=beam_options) as pipeline:
    lines = pipeline | beam.io.ReadFromText(args.input_file)

    # [END examples_wordcount_wordcount_options]

    # [START examples_wordcount_wordcount_composite]
    @beam.ptransform_fn
    def CountWords(pcoll):
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

    formatted | beam.io.WriteToText(args.output_path)


if __name__ == '__main__':
  examples_wordcount_wordcount()
