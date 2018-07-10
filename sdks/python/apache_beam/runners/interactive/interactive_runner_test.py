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

"""Tests for google3.pipeline.dataflow.python.interactive.interactive_runner.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import apache_beam as beam
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import interactive_runner


def print_with_message(msg):

  def printer(elem):
    print(msg, elem)
    return elem

  return printer


class InteractiveRunnerTest(unittest.TestCase):

  def test_basic(self):
    # TODO(qinyeli, BEAM-4755) remove explicitly overriding underlying runner
    # once interactive_runner works with FnAPI mode
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.BundleBasedDirectRunner()))
    p.run().wait_until_finish()
    pc0 = (
        p | 'read' >> beam.Create([1, 2, 3])
        | 'Print1.1' >> beam.Map(print_with_message('Run1.1')))
    pc = pc0 | 'Print1.2' >> beam.Map(print_with_message('Run1.2'))
    p.run().wait_until_finish()
    _ = pc | 'Print2' >> beam.Map(print_with_message('Run2'))
    p.run().wait_until_finish()
    _ = pc0 | 'Print3' >> beam.Map(print_with_message('Run3'))
    p.run().wait_until_finish()

  def test_wordcount(self):

    class WordExtractingDoFn(beam.DoFn):

      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    # TODO(qinyeli, BEAM-4755) remove explicitly overriding underlying runner
    # once interactive_runner works with FnAPI mode
    p = beam.Pipeline(
        runner=interactive_runner.InteractiveRunner(
            direct_runner.BundleBasedDirectRunner()))

    # Count the occurrences of each word.
    counts = (
        p
        | beam.Create(['to be or not to be that is the question'])
        | 'split' >> beam.ParDo(WordExtractingDoFn())
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda wordones: (wordones[0], sum(wordones[1]))))

    result = p.run()
    result.wait_until_finish()

    actual = dict(result.get(counts))
    self.assertDictEqual(
        actual, {
            'to': 2,
            'be': 2,
            'or': 1,
            'not': 1,
            'that': 1,
            'is': 1,
            'the': 1,
            'question': 1
        })


if __name__ == '__main__':
  unittest.main()
