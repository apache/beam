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

"""A workflow emitting the top k most common words for each prefix."""

from __future__ import absolute_import

import argparse
import logging
import re

import google.cloud.dataflow as df


def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      required=True,
                      help='Input file to process.')
  parser.add_argument('--output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = df.Pipeline(argv=pipeline_args)

  (p  # pylint: disable=expression-not-assigned
   | df.io.Read('read', df.io.TextFileSource(known_args.input))
   | df.FlatMap('split', lambda x: re.findall(r'[A-Za-z\']+', x))
   | TopPerPrefix('TopPerPrefix', 5)
   | df.Map('format',
            lambda (prefix, candidates): '%s: %s' % (prefix, candidates))
   | df.io.Write('write', df.io.TextFileSink(known_args.output)))
  p.run()


class TopPerPrefix(df.PTransform):

  def __init__(self, label, count):
    super(TopPerPrefix, self).__init__(label)
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
            | df.combiners.Count.PerElement()
            | df.FlatMap(extract_prefixes)
            | df.combiners.Top.LargestPerKey(self._count))


def extract_prefixes((word, count)):
  for k in range(1, len(word) + 1):
    prefix = word[:k]
    yield prefix, (count, word)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
