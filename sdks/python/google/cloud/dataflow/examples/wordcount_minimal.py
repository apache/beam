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

"""A minimalist word-counting workflow."""

from __future__ import absolute_import

import logging
import re

import google.cloud.dataflow as df
from google.cloud.dataflow.utils.options import add_option
from google.cloud.dataflow.utils.options import get_options


def run(options=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  p = df.Pipeline(options=get_options(options))

  # Read the text file[pattern] into a PCollection.
  lines = p | df.io.Read('read', df.io.TextFileSource(p.options.input))

  # Count the occurrences of each word.
  counts = (lines
            | (df.FlatMap('split', lambda x: re.findall(r'[A-Za-z\']+', x))
               .with_output_types(unicode))
            | df.Map('pair_with_one', lambda x: (x, 1))
            | df.GroupByKey('group')
            | df.Map('count', lambda (word, ones): (word, sum(ones))))

  # Format the counts into a PCollection of strings.
  output = counts | df.Map('format', lambda (word, c): '%s: %s' % (word, c))

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | df.io.Write('write', df.io.TextFileSink(p.options.output))

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
  logging.getLogger().setLevel(logging.INFO)
  run()
