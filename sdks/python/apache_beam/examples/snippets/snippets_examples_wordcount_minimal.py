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
# pylint:disable=line-too-long

# beam-playground:
#   name: WordCountMinimalSnippet
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --input-file gs://dataflow-samples/shakespeare/kinglear.txt --output-path output.txt
#   context_line: 85
#   categories:
#     - IO
#     - Core Transforms
#     - Flatten
#     - Options
#     - Combiners
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - count
#     - strings
#     - hellobeam


def examples_wordcount_minimal():
  """MinimalWordCount example snippets."""
  import re

  import apache_beam as beam

  # [START examples_wordcount_minimal_options]
  from apache_beam.options.pipeline_options import PipelineOptions

  input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
  output_path = 'gs://my-bucket/counts.txt'

  beam_options = PipelineOptions(
      runner='DataflowRunner',
      project='my-project-id',
      job_name='unique-job-name',
      temp_location='gs://my-bucket/temp',
  )
  # [END examples_wordcount_minimal_options]

  # Run it locally for testing.
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--input-file')
  parser.add_argument('--output-path')
  args, beam_args = parser.parse_known_args()

  input_file = args.input_file
  output_path = args.output_path

  beam_options = PipelineOptions(beam_args)

  # [START examples_wordcount_minimal_create]
  pipeline = beam.Pipeline(options=beam_options)
  # [END examples_wordcount_minimal_create]

  (
      # [START examples_wordcount_minimal_read]
      pipeline
      | beam.io.ReadFromText(input_file)
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
      | beam.io.WriteToText(output_path)
      # [END examples_wordcount_minimal_write]
  )

  # [START examples_wordcount_minimal_run]
  result = pipeline.run()
  # [END examples_wordcount_minimal_run]
  result.wait_until_finish()


if __name__ == '__main__':
  examples_wordcount_minimal()
