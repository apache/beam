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

"""A workflow emitting the top k most common words for each prefix."""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      required=True,
                      help='Input file to process.')
  parser.add_argument('--output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  (p  # pylint: disable=expression-not-assigned
   | 'read' >> beam.io.Read(beam.io.TextFileSource(known_args.input))
   | 'split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
   | 'TopPerPrefix' >> TopPerPrefix(5)
   | 'format' >> beam.Map(
       lambda (prefix, candidates): '%s: %s' % (prefix, candidates))
   | 'write' >> beam.io.Write(beam.io.TextFileSink(known_args.output)))
  p.run()


class TopPerPrefix(beam.PTransform):

  def __init__(self, count):
    super(TopPerPrefix, self).__init__()
    self._count = count

  def apply(self, words):
    """Compute the most common words for each possible prefixes.

    Args:
      words: a PCollection of strings

    Returns:
      A PCollection of most common words with each prefix, in the form
          (prefix, [(count, word), (count, word), ...])
    """
    return (words
            | beam.combiners.Count.PerElement()
            | beam.FlatMap(extract_prefixes)
            | beam.combiners.Top.LargestPerKey(self._count))


def extract_prefixes((word, count)):
  for k in range(1, len(word) + 1):
    prefix = word[:k]
    yield prefix, (count, word)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
