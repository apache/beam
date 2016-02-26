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

"""A BigShuffle workflow."""

from __future__ import absolute_import

import argparse
import binascii
import logging


import google.cloud.dataflow as df


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

  p = df.Pipeline(argv=pipeline_args)

  # Read the text file[pattern] into a PCollection.
  lines = p | df.io.Read('read', df.io.TextFileSource(known_args.input))

  # Count the occurrences of each word.
  output = (lines
            | df.Map('split', lambda x: (x[:10], x[10:99]))
            | df.GroupByKey('group')
            | df.FlatMap(
                'format',
                lambda (key, vals): ['%s%s' % (key, val) for val in vals]))

  input_csum = (lines
                | df.Map('input-csum', crc32line)
                | df.CombineGlobally('combine-input-csum', sum)
                | df.Map('hex-format', lambda x: '%x' % x))
  input_csum | df.io.Write(
      'write-input-csum',
      df.io.TextFileSink(known_args.checksum_output + '-input'))

  # Write the output using a "Write" transform that has side effects.
  output | df.io.Write('write', df.io.TextFileSink(known_args.output))
  # Write the output checksum
  output_csum = (output
                 | df.Map('output-csum', crc32line)
                 | df.CombineGlobally('combine-output-csum', sum)
                 | df.Map('hex-format-output', lambda x: '%x' % x))
  output_csum | df.io.Write(
      'write-output-csum',
      df.io.TextFileSink(known_args.checksum_output + '-output'))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
