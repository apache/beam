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

# pytype: skip-file

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='Input file to process.')
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    def format_result(prefix_candidates):
      (prefix, candidates) = prefix_candidates
      return '%s: %s' % (prefix, candidates)

    (  # pylint: disable=expression-not-assigned
        p
        | 'read' >> ReadFromText(known_args.input)
        | 'split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | 'TopPerPrefix' >> TopPerPrefix(5)
        | 'format' >> beam.Map(format_result)
        | 'write' >> WriteToText(known_args.output))


class TopPerPrefix(beam.PTransform):
  def __init__(self, count):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.PTransform.__init__(self)
    self._count = count

  def expand(self, words):
    """Compute the most common words for each possible prefixes.

    Args:
      words: a PCollection of strings

    Returns:
      A PCollection of most common words with each prefix, in the form
          (prefix, [(count, word), (count, word), ...])
    """
    return (
        words
        | beam.combiners.Count.PerElement()
        | beam.FlatMap(extract_prefixes)
        | beam.combiners.Top.LargestPerKey(self._count))


def extract_prefixes(element):
  word, count = element
  for k in range(1, len(word) + 1):
    prefix = word[:k]
    yield prefix, (count, word)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
