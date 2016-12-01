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

"""Test for the BigQuery side input example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import bigquery_side_input


class BigQuerySideInputTest(unittest.TestCase):

  def test_create_groups(self):
    p = beam.Pipeline('DirectPipelineRunner')

    group_ids_pcoll = p | 'create_group_ids' >> beam.Create(['A', 'B', 'C'])
    corpus_pcoll = p | beam.Create('create_corpus',
                                   [{'f': 'corpus1'},
                                    {'f': 'corpus2'},
                                    {'f': 'corpus3'}])
    words_pcoll = p | 'create_words' >> beam.Create([{'f': 'word1'},
                                                     {'f': 'word2'},
                                                     {'f': 'word3'}])
    ignore_corpus_pcoll = p | 'create_ignore_corpus' >> beam.Create(['corpus1'])
    ignore_word_pcoll = p | 'create_ignore_word' >> beam.Create(['word1'])

    groups = bigquery_side_input.create_groups(group_ids_pcoll, corpus_pcoll,
                                               words_pcoll, ignore_corpus_pcoll,
                                               ignore_word_pcoll)

    beam.assert_that(groups, beam.equal_to(
        [('A', 'corpus2', 'word2'),
         ('B', 'corpus2', 'word2'),
         ('C', 'corpus2', 'word2')]))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
