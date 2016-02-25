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

import binascii
import logging

import google.cloud.dataflow as df
from google.cloud.dataflow.utils.options import add_option
from google.cloud.dataflow.utils.options import get_options


def crc32line(line):
  return binascii.crc32(line) & 0xffffffff


def run(options=None):
  # pylint: disable=expression-not-assigned

  p = df.Pipeline(options=get_options(options))

  # Read the text file[pattern] into a PCollection.
  lines = p | df.io.Read('read', df.io.TextFileSource(p.options.input))

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
      df.io.TextFileSink(p.options.checksum_output + '-input'))

  # Write the output using a "Write" transform that has side effects.
  output | df.io.Write('write', df.io.TextFileSink(p.options.output))
  # Write the output checksum
  output_csum = (output
                 | df.Map('output-csum', crc32line)
                 | df.CombineGlobally('combine-output-csum', sum)
                 | df.Map('hex-format-output', lambda x: '%x' % x))
  output_csum | df.io.Write(
      'write-output-csum',
      df.io.TextFileSink(p.options.checksum_output + '-output'))

  # Actually run the pipeline (all operations above are deferred).
  p.run()

add_option(
    '--input', dest='input', required=True,
    help='Input file pattern to process.')
add_option(
    '--output', dest='output', required=True,
    help='Output file pattern to write results to.')
add_option(
    '--checksum_output', dest='checksum_output', required=True,
    help='Checksum output file pattern.')

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
