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
                      required=True,
                      help='Checksum output file pattern.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = beam.Pipeline(argv=pipeline_args)

  # Read the text file[pattern] into a PCollection.
  lines = p | beam.io.Read('read', beam.io.TextFileSource(known_args.input))

  # Count the occurrences of each word.
  output = (lines
            | beam.Map('split', lambda x: (x[:10], x[10:99]))
            | beam.GroupByKey('group')
            | beam.FlatMap(
                'format',
                lambda (key, vals): ['%s%s' % (key, val) for val in vals]))

  input_csum = (lines
                | beam.Map('input-csum', crc32line)
                | beam.CombineGlobally('combine-input-csum', sum)
                | beam.Map('hex-format', lambda x: '%x' % x))
  input_csum | beam.io.Write(
      'write-input-csum',
      beam.io.TextFileSink(known_args.checksum_output + '-input'))

  # Write the output using a "Write" transform that has side effects.
  output | beam.io.Write('write', beam.io.TextFileSink(known_args.output))
  # Write the output checksum
  output_csum = (output
                 | beam.Map('output-csum', crc32line)
                 | beam.CombineGlobally('combine-output-csum', sum)
                 | beam.Map('hex-format-output', lambda x: '%x' % x))
  output_csum | beam.io.Write(
      'write-output-csum',
      beam.io.TextFileSink(known_args.checksum_output + '-output'))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
