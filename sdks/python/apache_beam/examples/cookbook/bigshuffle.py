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

"""A BigShuffle workflow."""

from __future__ import absolute_import

import argparse
import binascii
import logging


import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


def crc32line(line):
  return binascii.crc32(line) & 0xffffffff


def run(argv=None):
  # pylint: disable=expression-not-assigned

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      required=True,
                      help='Input file pattern to process.')
  parser.add_argument('--output',
                      required=True,
                      help='Output file pattern to write results to.')
  parser.add_argument('--checksum_output',
                      help='Checksum output file pattern.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | beam.io.Read(
      beam.io.TextFileSource(known_args.input,
                             coder=beam.coders.BytesCoder()))

  # Count the occurrences of each word.
  output = (lines
            | 'split' >> beam.Map(
                lambda x: (x[:10], x[10:99]))
            .with_output_types(beam.typehints.KV[str, str])
            | 'group' >> beam.GroupByKey()
            | beam.FlatMap(
                'format',
                lambda (key, vals): ['%s%s' % (key, val) for val in vals]))

  # Write the output using a "Write" transform that has side effects.
  output | beam.io.Write(beam.io.TextFileSink(known_args.output))

  # Optionally write the input and output checksums.
  if known_args.checksum_output:
    input_csum = (lines
                  | 'input-csum' >> beam.Map(crc32line)
                  | 'combine-input-csum' >> beam.CombineGlobally(sum)
                  | 'hex-format' >> beam.Map(lambda x: '%x' % x))
    input_csum | 'write-input-csum' >> beam.io.Write(
        beam.io.TextFileSink(known_args.checksum_output + '-input'))

    output_csum = (output
                   | 'output-csum' >> beam.Map(crc32line)
                   | 'combine-output-csum' >> beam.CombineGlobally(sum)
                   | 'hex-format-output' >> beam.Map(lambda x: '%x' % x))
    output_csum | 'write-output-csum' >> beam.io.Write(
        beam.io.TextFileSink(known_args.checksum_output + '-output'))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
